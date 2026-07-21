//! V2b fragment emit (P2 + P3) — the forward half of the V2c round trip.
//! Decode a materialized heap page into the rows a columnar fragment carries,
//! each tagged with its index-addressable `(block, offnum)`, with visibility
//! resolved against CLOG *at the page's LSN* (P2) and HOT chains collapsed to
//! their single visible version while preserving the chain root's line pointer
//! as the row's address (P3). The exact inverse direction is
//! [`crate::reconstruct`], so the two validate each other offline:
//! `reconstruct::build_page` builds a page from rows + chain shapes, and
//! `emit_page` decodes it back to the visible rows at their root offnums.
//!
//! In production this is teed off `create_image_layer_for_rel_blocks` in the
//! pageserver fork (`docs/v2-scope.md` §V2b); here it is a pure async function
//! over a page + a [`ClogSource`], so every visibility and HOT-chain case is
//! pinned without a running stack.
//!
//! **Scope.** Rows are decoded semantically (the same `Row` shape the engine
//! writes to Delta today). The bit-exact-datum question (P6) — storing raw
//! datums for the types the Arrow mapping can't prove round-trippable — is
//! orthogonal and not addressed here. On-page TOAST resolves through the
//! supplied `ToastCache` exactly as elsewhere.

use anyhow::{Result, bail};

use crate::clog::{ClogSource, tuple_visible};
use crate::reconstruct::{PAGE_SIZE, line_pointer};
use crate::schema::TableDesc;
use crate::wal::heap::{Row, ToastCache, decode_tuple_from_page};

const HEAP_TUPLE_HEADER_SIZE: usize = 23;
// t_infomask2 bits.
const HEAP_HOT_UPDATED: u16 = 0x4000;
const HEAP_ONLY_TUPLE: u16 = 0x8000;
// ItemIdData lp_flags.
const LP_NORMAL: u8 = 1;
const LP_REDIRECT: u8 = 2;
/// Upper bound on HOT-chain length before we declare a cycle (a page holds at
/// most `MaxHeapTuplesPerPage` ~291 line pointers, so any real chain is short).
const MAX_CHAIN: usize = 1024;

/// One row of a columnar fragment: a decoded row plus the `(block, offnum)` an
/// index entry resolves to (the HOT chain's root line pointer).
#[derive(Debug, Clone, PartialEq)]
pub struct FragmentRow {
    pub block: u32,
    pub offnum: u16,
    pub row: Row,
}

/// The visibility-relevant + chain-linking fields of a HeapTupleHeader.
struct TupleHdr {
    xmin: u32,
    xmax: u32,
    infomask: u16,
    infomask2: u16,
    ctid_block: u32,
    ctid_off: u16,
}

fn read_header(page: &[u8], off: usize) -> Result<TupleHdr> {
    let t = page
        .get(off..off + HEAP_TUPLE_HEADER_SIZE)
        .ok_or_else(|| anyhow::anyhow!("tuple header at {off} beyond page"))?;
    let bi_hi = u16::from_le_bytes([t[12], t[13]]) as u32;
    let bi_lo = u16::from_le_bytes([t[14], t[15]]) as u32;
    Ok(TupleHdr {
        xmin: u32::from_le_bytes(t[0..4].try_into().unwrap()),
        xmax: u32::from_le_bytes(t[4..8].try_into().unwrap()),
        ctid_block: (bi_hi << 16) | bi_lo,
        ctid_off: u16::from_le_bytes([t[16], t[17]]),
        infomask2: u16::from_le_bytes(t[18..20].try_into().unwrap()),
        infomask: u16::from_le_bytes(t[20..22].try_into().unwrap()),
    })
}

/// Decode `page` (at heap block `block`) into its committed-visible rows,
/// addressed by their index line pointer. Tuple versions whose xmin isn't
/// committed as of the CLOG `clog` reflects, and versions superseded by a
/// committed deleter/updater, are excluded — "committed as of this LSN", the
/// V2b fragment semantics (in-progress writers arrive later via the tail).
pub async fn emit_page<C: ClogSource>(
    page: &[u8],
    block: u32,
    desc: &TableDesc,
    toast: &ToastCache,
    clog: &mut C,
) -> Result<Vec<FragmentRow>> {
    if page.len() != PAGE_SIZE {
        bail!("emit_page: expected an {PAGE_SIZE}-byte page, got {}", page.len());
    }
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    let n_line_pointers = pd_lower.saturating_sub(24) / 4;

    let mut out = Vec::new();
    for offnum in 1..=n_line_pointers as u16 {
        let lp = line_pointer(page, offnum)?;
        // Determine where this index-addressable slot's HOT chain starts.
        let chain_start = match lp.flags {
            LP_REDIRECT => lp.off, // a HOT root redirect → follow to the target
            LP_NORMAL => {
                // A heap-only tuple is reachable only through its chain root,
                // never directly from an index — skip it as a starting point;
                // it is visited when we walk its root's chain.
                let hdr = read_header(page, lp.off as usize)?;
                if hdr.infomask2 & HEAP_ONLY_TUPLE != 0 {
                    continue;
                }
                offnum
            }
            _ => continue, // LP_UNUSED / LP_DEAD
        };
        if let Some(row) = walk_chain(page, block, chain_start, desc, toast, clog).await? {
            out.push(FragmentRow { block, offnum, row });
        }
    }
    Ok(out)
}

/// Walk a HOT chain from `start` to its visible version, returning that row (or
/// `None` if the whole chain is invisible: an aborted insert, or a deletion
/// with no live successor).
async fn walk_chain<C: ClogSource>(
    page: &[u8],
    block: u32,
    start: u16,
    desc: &TableDesc,
    toast: &ToastCache,
    clog: &mut C,
) -> Result<Option<Row>> {
    let mut cur = start;
    for _ in 0..MAX_CHAIN {
        let lp = line_pointer(page, cur)?;
        if lp.flags != LP_NORMAL {
            return Ok(None); // chain ran into a dead/unused pointer
        }
        let hdr = read_header(page, lp.off as usize)?;
        if tuple_visible(clog, hdr.xmin, hdr.xmax, hdr.infomask).await? {
            let (row, _) = decode_tuple_from_page(page, cur, desc, toast)?;
            return Ok(Some(row));
        }
        // Not visible. Only a *HOT update* to a newer version on the same page
        // continues the chain; anything else (aborted insert, plain delete) is
        // a dead end.
        let hot_updated = hdr.infomask2 & HEAP_HOT_UPDATED != 0;
        if hot_updated && hdr.ctid_block == block && hdr.ctid_off != 0 && hdr.ctid_off != cur {
            cur = hdr.ctid_off;
            continue;
        }
        return Ok(None);
    }
    bail!("emit_page: HOT chain from offnum {start} exceeds {MAX_CHAIN} (cycle?)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{CLOG_XACTS_PER_BYTE, CLOG_XACTS_PER_PAGE, xid_to_page};
    use crate::reconstruct::{Slot, TupleSpec, build_page};
    use crate::schema::{Col, PgType, PhysCol};
    use crate::wal::heap::Value;
    use std::collections::HashMap;

    const BLOCK: u32 = 7;
    const COMMITTED: u32 = 100;
    const UPDATER: u32 = 101;
    const ABORTED: u32 = 200;

    struct FakeClog {
        status: HashMap<u32, u8>, // 1 = committed, 2 = aborted
    }
    impl ClogSource for FakeClog {
        async fn clog_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
            let mut page = vec![0u8; 8192];
            for (&xid, &st) in &self.status {
                if xid_to_page(xid) != pageno {
                    continue;
                }
                let idx = xid % CLOG_XACTS_PER_PAGE;
                page[(idx / CLOG_XACTS_PER_BYTE) as usize] |= st << ((idx % CLOG_XACTS_PER_BYTE) * 2);
            }
            Ok(page)
        }
    }
    fn clog() -> FakeClog {
        FakeClog { status: HashMap::from([(COMMITTED, 1), (UPDATER, 1), (ABORTED, 2)]) }
    }

    fn desc() -> TableDesc {
        let cols = vec![
            Col { name: "id".into(), ty: PgType::Int4 },
            Col { name: "txt".into(), ty: PgType::Text },
        ];
        TableDesc {
            name: "t".into(),
            oid: 40000,
            db_oid: 5,
            rel_node: 40000,
            toast_rel_node: None,
            phys: cols.iter().cloned().map(PhysCol::Live).collect(),
            cols,
            has_fast_defaults: false,
            pk: vec!["id".into()],
        }
    }
    fn row(id: i32, txt: &str) -> Row {
        vec![Some(Value::I32(id)), Some(Value::Text(txt.into()))]
    }
    fn spec(xmin: u32, xmax: u32, row: Row) -> TupleSpec {
        TupleSpec { row, xmin, xmax, ctid: None, heap_only: false, hot_updated: false }
    }

    async fn emit(slots: &[Slot]) -> Vec<FragmentRow> {
        let page = build_page(&desc(), BLOCK, 0, slots).unwrap();
        emit_page(&page, BLOCK, &desc(), &ToastCache::default(), &mut clog()).await.unwrap()
    }

    #[tokio::test]
    async fn frozen_rows_emit_at_their_offnums() {
        let got = emit(&[Slot::live(row(1, "a")), Slot::live(row(2, "b"))]).await;
        assert_eq!(
            got,
            vec![
                FragmentRow { block: BLOCK, offnum: 1, row: row(1, "a") },
                FragmentRow { block: BLOCK, offnum: 2, row: row(2, "b") },
            ]
        );
    }

    #[tokio::test]
    async fn aborted_insert_is_excluded() {
        // Row 1 committed, row 2 an aborted insert.
        let got = emit(&[
            Slot::Tuple(spec(COMMITTED, 0, row(1, "live"))),
            Slot::Tuple(spec(ABORTED, 0, row(2, "ghost"))),
        ])
        .await;
        assert_eq!(got, vec![FragmentRow { block: BLOCK, offnum: 1, row: row(1, "live") }]);
    }

    #[tokio::test]
    async fn committed_delete_is_excluded() {
        // xmax committed, not HOT-updated → deleted, no successor.
        let got = emit(&[Slot::Tuple(spec(COMMITTED, UPDATER, row(1, "gone")))]).await;
        assert_eq!(got, vec![]);
    }

    #[tokio::test]
    async fn hot_chain_collapses_to_the_live_version_at_the_root_offnum() {
        // offnum 1: root, xmin committed, xmax = updater (committed), HOT-updated
        //           to offnum 2. offnum 2: the new heap-only version, live.
        let mut root = spec(COMMITTED, UPDATER, row(1, "v1"));
        root.ctid = Some((BLOCK, 2));
        root.hot_updated = true;
        let mut child = spec(UPDATER, 0, row(1, "v2"));
        child.heap_only = true;

        let got = emit(&[Slot::Tuple(root), Slot::Tuple(child)]).await;
        // One row: the live version's data, addressed by the ROOT offnum 1.
        assert_eq!(got, vec![FragmentRow { block: BLOCK, offnum: 1, row: row(1, "v2") }]);
    }

    #[tokio::test]
    async fn redirect_root_follows_to_the_heap_only_version() {
        // offnum 1 is an LP_REDIRECT root → offnum 2, a live heap-only tuple.
        let mut child = spec(COMMITTED, 0, row(9, "hot"));
        child.heap_only = true;
        let got = emit(&[Slot::Redirect(2), Slot::Tuple(child)]).await;
        assert_eq!(got, vec![FragmentRow { block: BLOCK, offnum: 1, row: row(9, "hot") }]);
    }

    #[tokio::test]
    async fn unused_and_dead_pointers_are_skipped() {
        let got = emit(&[Slot::live(row(1, "a")), Slot::Unused, Slot::Dead, Slot::live(row(4, "d"))]).await;
        assert_eq!(
            got,
            vec![
                FragmentRow { block: BLOCK, offnum: 1, row: row(1, "a") },
                FragmentRow { block: BLOCK, offnum: 4, row: row(4, "d") },
            ]
        );
    }

    #[tokio::test]
    async fn round_trips_with_reconstruct() {
        // The pairing: rows -> page (reconstruct) -> rows (fragment). Frozen
        // rows are always visible, so emit returns exactly what we built.
        let rows = [row(10, "x"), row(20, "yy"), row(30, "zzz")];
        let slots: Vec<Slot> = rows.iter().cloned().map(Slot::live).collect();
        let got = emit(&slots).await;
        let got_rows: Vec<Row> = got.into_iter().map(|f| f.row).collect();
        assert_eq!(got_rows, rows);
    }
}
