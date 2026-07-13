//! CLOG (`pg_xact`) visibility resolution — v2-scope P2: real xmin/xmax
//! visibility for catalog-from-pages, replacing the `xmax == 0` spike
//! heuristic in `catalog.rs`.
//!
//! The CLOG page format is standard Postgres SLRU (`clog.c`), unrelated to
//! Neon: 2 bits per xid packed 4-per-byte LSB-first (`TransactionIdToPage`,
//! `TransactionIdToPgIndex`), one page = 32768 xids, one segment file = 32
//! pages named as a zero-padded 4-hex-digit segment number ("0000", "0001",
//! ...; CLOG never needs "long" 16-digit segment names — its xid space maxes
//! out at 4096 segments). Verified byte-for-byte against a live PG17 instance
//! (`openltap-pg`): a committed xid 760 and an aborted xid 762 landed at
//! byte 190 of segment `0000` as `0x65 = 0b01100101`, decoding to
//! (760:committed, 761:committed, 762:aborted, 763:committed) — exactly
//! matching what was run (see the `grounded_against_live_pg17_bytes` test,
//! which pins that literal byte).
//!
//! [`ClogSource`] is page-source-agnostic, like `catalog::PageSource`. The
//! one implementation here, [`FileClogSource`], reads a live `pg_xact/`
//! directory — verified against both the dev-compose vanilla Postgres
//! (`openltap-pg`) *and* a real Neon compute (`neon-compose`'s `compute1`
//! maintains its own local `pg_xact/` cache, same on-disk format). **This is
//! a "current state" read, not CLOG-at-an-arbitrary-past-LSN** — the true
//! v2-scope P2 answer for V2b (CLOG served from the pageserver's own SLRU
//! keyspace, pinned to an image layer's exact LSN) needs fork-side plumbing
//! (a pagestream SLRU-segment request, or a native `Timeline` SLRU read) that
//! does not exist anywhere in this repo yet and is out of scope for this
//! session (the fork is off-limits — see CLAUDE.md). Add a second
//! `ClogSource` impl there when that plumbing lands; nothing in this module
//! or in `tuple_visible`'s callers needs to change — the trait boundary is
//! the seam.
//!
//! Multixact xmax (the P2 sub-case named in `docs/v2-scope.md`): a lock-only
//! multixact (no updater, just concurrent lockers) is resolved for free —
//! `HEAP_XMAX_LOCK_ONLY` means xmax never deletes the row, regardless of what
//! it points at. A multixact *with* an updater member needs the
//! members/offsets SLRU (a different, more involved format) to find that
//! member's xid before CLOG can even be consulted; that's genuinely
//! unimplemented, so [`tuple_visible`] returns a typed error for that case
//! rather than guessing.

use anyhow::{Context, Result, bail};
use std::path::PathBuf;

/// `CLOG_XACTS_PER_BYTE` (clog.c): 2 bits per xid, 4 xids per byte.
pub const CLOG_XACTS_PER_BYTE: u32 = 4;
/// `CLOG_XACTS_PER_PAGE` = `BLCKSZ` * `CLOG_XACTS_PER_BYTE`.
pub const CLOG_XACTS_PER_PAGE: u32 = 8192 * CLOG_XACTS_PER_BYTE;
/// `SLRU_PAGES_PER_SEGMENT` (slru.c), unchanged since introduced.
pub const SLRU_PAGES_PER_SEGMENT: u32 = 32;
const PAGE_SZ: usize = 8192;

pub const INVALID_XID: u32 = 0;
pub const BOOTSTRAP_XID: u32 = 1;
pub const FROZEN_XID: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    InProgress,
    Committed,
    Aborted,
    SubCommitted,
}

/// `TransactionIdToPage`.
pub fn xid_to_page(xid: u32) -> u32 {
    xid / CLOG_XACTS_PER_PAGE
}

/// (byte offset within page, bit shift) for `xid`'s 2-bit status field.
fn byte_bit(xid: u32) -> (usize, u32) {
    let idx = xid % CLOG_XACTS_PER_PAGE;
    ((idx / CLOG_XACTS_PER_BYTE) as usize, (idx % CLOG_XACTS_PER_BYTE) * 2)
}

/// Decode `xid`'s status from its already-fetched CLOG page (must be the
/// page `xid_to_page(xid)` names). A page that was never written (xid never
/// assigned, or not yet flushed) reads as all-zero bytes, which decodes to
/// `InProgress` — the correct answer for "never happened as far as this read
/// can tell", same as a genuinely in-progress xid.
pub fn status_in_page(page: &[u8], xid: u32) -> Result<TxnStatus> {
    let (byteno, bshift) = byte_bit(xid);
    let byte = *page.get(byteno).context("clog page too short for this xid")?;
    Ok(match (byte >> bshift) & 0x3 {
        0 => TxnStatus::InProgress,
        1 => TxnStatus::Committed,
        2 => TxnStatus::Aborted,
        3 => TxnStatus::SubCommitted,
        _ => unreachable!("2 bits"),
    })
}

/// A source of CLOG pages, addressed by the global page number
/// (`xid_to_page`). Page-source-agnostic like `catalog::PageSource` — the
/// pageserver-keyspace / native-Timeline impl slots in here for true
/// CLOG-at-LSN reads (see the module doc).
pub trait ClogSource {
    async fn clog_page(&mut self, pageno: u32) -> Result<Vec<u8>>;
}

/// Resolve one xid's final status. `BootstrapTransactionId`/
/// `FrozenTransactionId` are special-cased exactly as
/// `TransactionIdDidCommit` does in Postgres — they predate CLOG and are
/// always committed.
pub async fn resolve<S: ClogSource>(src: &mut S, xid: u32) -> Result<TxnStatus> {
    if xid == BOOTSTRAP_XID || xid == FROZEN_XID {
        return Ok(TxnStatus::Committed);
    }
    if xid == INVALID_XID {
        bail!("resolve() called on InvalidTransactionId — check for xmax == 0 before calling");
    }
    let page = src.clog_page(xid_to_page(xid)).await?;
    status_in_page(&page, xid)
}

// htup_details.h infomask bits relevant to visibility. HEAP_XMIN_COMMITTED /
// HEAP_XMIN_INVALID / HEAP_XMAX_COMMITTED / HEAP_XMAX_INVALID are hint bits —
// opportunistically set post-hoc by any backend that resolves visibility,
// not WAL-logged — so per docs/v2-scope.md P2 ("hint bits can't be trusted")
// this module never reads them. HEAP_XMAX_LOCK_ONLY and HEAP_XMAX_IS_MULTI
// are structural (set when the tuple version is written, WAL-logged with
// it) and are trusted.
const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080;
const HEAP_XMAX_IS_MULTI: u16 = 0x1000;

/// Is the row version `(xmin, xmax)` visible, given CLOG resolved via `src`?
/// This is the core of `HeapTupleSatisfiesMVCC` minus the in-progress
/// snapshot list — there is no live snapshot here, and per
/// docs/v2-scope.md P2/V2b that's by design: "Emit 'committed as of LSN X';
/// in-progress txns are simply not yet in the fragment and arrive via the
/// tail." A still-in-progress xmin or xmax therefore resolves the same way
/// an aborted one would for xmin (not yet visible) or the same way an
/// invalid one would for xmax (not yet deleted) — "not committed as of this
/// read" either way.
pub async fn tuple_visible<S: ClogSource>(src: &mut S, xmin: u32, xmax: u32, infomask: u16) -> Result<bool> {
    let xmin_committed = matches!(resolve(src, xmin).await?, TxnStatus::Committed);
    if !xmin_committed {
        return Ok(false); // insert never (yet) took effect
    }
    if xmax == INVALID_XID {
        return Ok(true);
    }
    if infomask & HEAP_XMAX_IS_MULTI != 0 {
        if infomask & HEAP_XMAX_LOCK_ONLY != 0 {
            return Ok(true); // lock-only multixact: no updater, never deletes
        }
        bail!(
            "xmax {xmax} is a multixact with an updater member — resolving it needs \
             the members/offsets SLRU, not implemented (docs/v2-scope.md P2 sub-case)"
        );
    }
    if infomask & HEAP_XMAX_LOCK_ONLY != 0 {
        return Ok(true); // plain-xid row lock (FOR UPDATE/SHARE), not a deleter
    }
    let xmax_committed = matches!(resolve(src, xmax).await?, TxnStatus::Committed);
    Ok(!xmax_committed)
}

/// [`ClogSource`] over a live `pg_xact/` directory — a running compute's
/// PGDATA (vanilla Postgres or a Neon compute's local SLRU cache both use
/// this exact on-disk format). Reads *current* on-disk state, not state
/// pinned to a past LSN — see the module doc for the true CLOG-at-LSN path.
pub struct FileClogSource {
    dir: PathBuf,
}

impl FileClogSource {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        FileClogSource { dir: dir.into() }
    }
}

impl ClogSource for FileClogSource {
    async fn clog_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
        let segno = pageno / SLRU_PAGES_PER_SEGMENT;
        let page_in_seg = (pageno % SLRU_PAGES_PER_SEGMENT) as usize;
        let path = self.dir.join(format!("{segno:04X}"));
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("reading clog segment {}", path.display()))?;
        let off = page_in_seg * PAGE_SZ;
        bytes
            .get(off..off + PAGE_SZ)
            .map(|s| s.to_vec())
            .with_context(|| format!("clog segment {} has no page {page_in_seg}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct OnePage(Vec<u8>);
    impl ClogSource for OnePage {
        async fn clog_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
            assert_eq!(pageno, 0, "test xids all fit on page 0");
            Ok(self.0.clone())
        }
    }

    fn page_with(byteno: usize, byte: u8) -> Vec<u8> {
        let mut p = vec![0u8; PAGE_SZ];
        p[byteno] = byte;
        p
    }

    #[tokio::test]
    async fn grounded_against_live_pg17_bytes() {
        // Captured live from openltap-pg (dev-compose PG17): a transaction
        // that committed got xid 760, a subxid at 761 was folded into that
        // same commit, xid 762 was rolled back, and the immediately
        // following implicit read-only statement auto-committed as 763.
        // Byte 190 = 760/4 held 0x65 = 0b01100101 after CHECKPOINT flushed
        // the page to disk.
        let page = page_with(190, 0x65);
        assert_eq!(status_in_page(&page, 760).unwrap(), TxnStatus::Committed);
        assert_eq!(status_in_page(&page, 761).unwrap(), TxnStatus::Committed);
        assert_eq!(status_in_page(&page, 762).unwrap(), TxnStatus::Aborted);
        assert_eq!(status_in_page(&page, 763).unwrap(), TxnStatus::Committed);
    }

    #[test]
    fn page_addressing_matches_clog_c() {
        assert_eq!(xid_to_page(0), 0);
        assert_eq!(xid_to_page(32767), 0);
        assert_eq!(xid_to_page(32768), 1);
        assert_eq!(byte_bit(0), (0, 0));
        assert_eq!(byte_bit(1), (0, 2));
        assert_eq!(byte_bit(3), (0, 6));
        assert_eq!(byte_bit(4), (1, 0));
    }

    #[tokio::test]
    async fn special_xids_are_always_committed() {
        let mut src = OnePage(vec![0u8; PAGE_SZ]); // an all-zero page would read as in-progress
        assert_eq!(resolve(&mut src, BOOTSTRAP_XID).await.unwrap(), TxnStatus::Committed);
        assert_eq!(resolve(&mut src, FROZEN_XID).await.unwrap(), TxnStatus::Committed);
    }

    #[tokio::test]
    async fn invalid_xid_is_an_error() {
        let mut src = OnePage(vec![0u8; PAGE_SZ]);
        assert!(resolve(&mut src, INVALID_XID).await.is_err());
    }

    #[tokio::test]
    async fn unwritten_xid_reads_as_in_progress() {
        let mut src = OnePage(vec![0u8; PAGE_SZ]);
        assert_eq!(resolve(&mut src, 100).await.unwrap(), TxnStatus::InProgress);
    }

    // ---- tuple_visible: the scenarios the old xmax==0 heuristic got wrong ----

    fn page_status(xid: u32, status: u8) -> Vec<u8> {
        let (byteno, bshift) = byte_bit(xid);
        page_with(byteno, status << bshift)
    }

    #[tokio::test]
    async fn committed_insert_no_delete_is_visible() {
        let mut src = OnePage(page_status(10, 1)); // xmin 10 committed
        assert!(tuple_visible(&mut src, 10, 0, 0).await.unwrap());
    }

    #[tokio::test]
    async fn aborted_insert_is_invisible_even_with_xmax_zero() {
        // The old xmax==0 heuristic kept this row (xmax==0 -> kept). Wrong:
        // the insert never committed.
        let mut src = OnePage(page_status(10, 2)); // xmin 10 aborted
        assert!(!tuple_visible(&mut src, 10, 0, 0).await.unwrap());
    }

    #[tokio::test]
    async fn committed_delete_is_invisible() {
        let mut page = page_status(10, 1); // xmin 10 committed
        let (byteno, bshift) = byte_bit(20);
        page[byteno] |= 1 << bshift; // xmax 20 committed
        let mut src = OnePage(page);
        assert!(!tuple_visible(&mut src, 10, 20, 0).await.unwrap());
    }

    #[tokio::test]
    async fn aborted_delete_is_still_visible() {
        // The old xmax==0 heuristic dropped this row (xmax!=0 -> dropped).
        // Wrong: the deleting transaction rolled back.
        let mut page = page_status(10, 1); // xmin 10 committed
        let (byteno, bshift) = byte_bit(20);
        page[byteno] |= 2 << bshift; // xmax 20 aborted
        let mut src = OnePage(page);
        assert!(tuple_visible(&mut src, 10, 20, 0).await.unwrap());
    }

    #[tokio::test]
    async fn in_progress_delete_is_still_visible() {
        let mut src = OnePage(page_status(10, 1)); // xmin 10 committed, xmax 20 untouched (in progress)
        assert!(tuple_visible(&mut src, 10, 20, 0).await.unwrap());
    }

    #[tokio::test]
    async fn lock_only_xmax_is_visible_regardless_of_its_clog_status() {
        // The old heuristic dropped this row too (xmax != 0). A row lock is
        // not a deleter, committed or not.
        let mut page = page_status(10, 1); // xmin 10 committed
        let (byteno, bshift) = byte_bit(20);
        page[byteno] |= 1 << bshift; // xmax 20 committed (irrelevant: lock-only)
        let mut src = OnePage(page);
        assert!(tuple_visible(&mut src, 10, 20, HEAP_XMAX_LOCK_ONLY).await.unwrap());
    }

    #[tokio::test]
    async fn lock_only_multixact_is_visible() {
        let mut src = OnePage(page_status(10, 1));
        let infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY;
        assert!(tuple_visible(&mut src, 10, 4_000_000, infomask).await.unwrap());
    }

    #[tokio::test]
    async fn updater_multixact_errors_instead_of_guessing() {
        let mut src = OnePage(page_status(10, 1));
        let err = tuple_visible(&mut src, 10, 4_000_000, HEAP_XMAX_IS_MULTI).await.unwrap_err();
        assert!(err.to_string().contains("multixact"));
    }
}
