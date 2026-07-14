//! MultiXact (`pg_multixact`) updater resolution — closes the P2 sub-case
//! `docs/v2-scope.md` names ("multixact xmax (members SLRU is also in the
//! keyspace)") and `clog.rs` deliberately left unresolved: given a
//! MultiXactId with an updater member (`HEAP_XMAX_IS_MULTI` set,
//! `HEAP_XMAX_LOCK_ONLY` clear), find that member's xid so
//! [`crate::clog::resolve`] can decide whether it actually committed. A
//! lock-only multixact (every member just a locker, no updater) never
//! reaches this module — `HEAP_XMAX_LOCK_ONLY` already answers that for
//! free, without touching either SLRU (see `clog::tuple_visible`).
//!
//! Two SLRUs, standard Postgres format (`multixact.c`), unrelated to Neon:
//! - `pg_multixact/offsets`: one 4-byte `MultiXactOffset` per `MultiXactId`,
//!   naming where that multixact's member list starts in the members SLRU.
//!   A multixact's member *count* isn't stored directly — it's
//!   `offsets[multi+1] - offsets[multi]` — so the single most-recently
//!   -created multixact cluster-wide can't be resolved from disk alone (no
//!   next entry exists yet to bound it). That's a real, inherent limitation
//!   of a disk-only read, not a shortcut: surfaced as a typed error, never
//!   guessed.
//! - `pg_multixact/members`: packed into 20-byte groups of 4 members each —
//!   4 flag bytes (one per member, low bits = `MultiXactStatus`), then the
//!   4 corresponding 4-byte xids (`multixact.c`'s own comment: "store four
//!   bytes of flags, and then the corresponding 4 Xids").
//!
//! Both SLRUs use short (4-hex-digit) segment names, same convention as
//! CLOG (`crate::clog::read_slru_segment_page`, shared).
//!
//! Grounded against `postgres/postgres` REL_17_STABLE `multixact.c`/
//! `multixact.h` (layout constants, `ISUPDATE_from_mxstatus`), then verified
//! byte-for-byte against a live multixact captured from `openltap-pg`: a
//! `FOR KEY SHARE` locker left open while a concurrent non-key `UPDATE`
//! proceeded produced multixact 1 = {768: ForKeyShare, 769: NoKeyUpdate},
//! cross-checked against Postgres's own `pg_get_multixact_members('1')` —
//! see the `grounded_against_live_bytes` test, which pins those literal
//! bytes and expects exactly that answer.

use anyhow::{Context, Result, bail};
use std::path::PathBuf;

use crate::clog::{PAGE_SZ, read_slru_segment_page};

const OFFSET_SZ: usize = 4;
/// `MULTIXACT_OFFSETS_PER_PAGE` = `BLCKSZ` / `sizeof(MultiXactOffset)`.
const OFFSETS_PER_PAGE: u32 = (PAGE_SZ / OFFSET_SZ) as u32;

/// `MULTIXACT_FLAGBYTES_PER_GROUP` (one flag byte per member).
const FLAGBYTES_PER_GROUP: usize = 4;
/// `MULTIXACT_MEMBERS_PER_MEMBERGROUP`.
const MEMBERS_PER_MEMBERGROUP: usize = FLAGBYTES_PER_GROUP;
/// `MULTIXACT_MEMBERGROUP_SIZE`: 4 flag bytes + 4 xids (4 bytes each) = 20.
const MEMBERGROUP_SIZE: usize = FLAGBYTES_PER_GROUP + MEMBERS_PER_MEMBERGROUP * 4;
/// `MULTIXACT_MEMBERGROUPS_PER_PAGE`.
const MEMBERGROUPS_PER_PAGE: usize = PAGE_SZ / MEMBERGROUP_SIZE;
/// `MULTIXACT_MEMBERS_PER_PAGE`.
const MEMBERS_PER_PAGE: u32 = (MEMBERGROUPS_PER_PAGE * MEMBERS_PER_MEMBERGROUP) as u32;

/// `MultiXactStatus` values greater than this are updaters/deleters
/// (`ISUPDATE_from_mxstatus`: `status > MultiXactStatusForUpdate`); values
/// at or below are lockers only (ForKeyShare=0, ForShare=1,
/// ForNoKeyUpdate=2, ForUpdate=3; NoKeyUpdate=4, Update=5 are updaters).
const STATUS_FOR_UPDATE: u8 = 0x03;

/// A source of MultiXact offsets/members pages, addressed by global page
/// number. Page-source-agnostic like `catalog::PageSource` /
/// `clog::ClogSource` — a pageserver-keyspace impl slots in here for true
/// MultiXact-at-LSN reads (same honest gap as `clog::ClogSource`; see that
/// module's doc).
pub trait MultiXactSource {
    async fn offsets_page(&mut self, pageno: u32) -> Result<Vec<u8>>;
    async fn members_page(&mut self, pageno: u32) -> Result<Vec<u8>>;
}

fn offset_page_entry(multi: u32) -> (u32, usize) {
    (multi / OFFSETS_PER_PAGE, (multi % OFFSETS_PER_PAGE) as usize * OFFSET_SZ)
}

async fn read_offset<M: MultiXactSource>(src: &mut M, multi: u32) -> Result<u32> {
    let (pageno, byteoff) = offset_page_entry(multi);
    let page = src.offsets_page(pageno).await?;
    let b = page.get(byteoff..byteoff + OFFSET_SZ).context("multixact offsets page too short")?;
    Ok(u32::from_le_bytes(b.try_into().unwrap()))
}

/// (page, byte offset of that member's group within the page, member's
/// index within its group).
fn member_location(offset: u32) -> (u32, usize, usize) {
    let page = offset / MEMBERS_PER_PAGE;
    let member_in_page = (offset % MEMBERS_PER_PAGE) as usize;
    let group_on_page = (member_in_page / MEMBERS_PER_MEMBERGROUP) % MEMBERGROUPS_PER_PAGE;
    let member_in_group = member_in_page % MEMBERS_PER_MEMBERGROUP;
    (page, group_on_page * MEMBERGROUP_SIZE, member_in_group)
}

async fn read_member<M: MultiXactSource>(src: &mut M, offset: u32) -> Result<(u32, u8)> {
    let (pageno, group_off, member_in_group) = member_location(offset);
    let page = src.members_page(pageno).await?;
    let status = *page.get(group_off + member_in_group).context("multixact members page too short (flags)")?;
    let xid_off = group_off + FLAGBYTES_PER_GROUP + member_in_group * 4;
    let xid_bytes = page.get(xid_off..xid_off + 4).context("multixact members page too short (xid)")?;
    Ok((u32::from_le_bytes(xid_bytes.try_into().unwrap()), status))
}

/// Find the updater/deleter member of multixact `multi`, if any. `Ok(None)`
/// means every member is a locker — a real answer, not the common path
/// (callers normally already know this from `HEAP_XMAX_LOCK_ONLY` without
/// calling this function at all; see `clog::tuple_visible_with_multixact`).
pub async fn resolve_updater<M: MultiXactSource>(src: &mut M, multi: u32) -> Result<Option<u32>> {
    let start = read_offset(src, multi).await?;
    let end = read_offset(src, multi.wrapping_add(1)).await?;
    if end <= start {
        bail!(
            "multixact {multi}'s member count can't be determined from disk alone — it looks \
             like the most recently created multixact cluster-wide, so no newer offsets entry \
             exists yet to bound its member list (would need live nextMXact/nextOffset state)"
        );
    }
    for off in start..end {
        let (xid, status) = read_member(src, off).await?;
        if status > STATUS_FOR_UPDATE {
            return Ok(Some(xid));
        }
    }
    Ok(None)
}

/// [`MultiXactSource`] over a live `pg_multixact/` directory (a running
/// compute's PGDATA). Reads *current* on-disk state — same "not pinned to a
/// past LSN" caveat as `clog::FileClogSource`.
pub struct FileMultiXactSource {
    offsets_dir: PathBuf,
    members_dir: PathBuf,
}

impl FileMultiXactSource {
    pub fn new(pg_multixact_dir: impl Into<PathBuf>) -> Self {
        let dir = pg_multixact_dir.into();
        FileMultiXactSource { offsets_dir: dir.join("offsets"), members_dir: dir.join("members") }
    }
}

impl MultiXactSource for FileMultiXactSource {
    async fn offsets_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
        read_slru_segment_page(&self.offsets_dir, pageno).await
    }
    async fn members_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
        read_slru_segment_page(&self.members_dir, pageno).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Pages { offsets: Vec<u8>, members: Vec<u8> }
    impl MultiXactSource for Pages {
        async fn offsets_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
            assert_eq!(pageno, 0, "test multixacts all fit on page 0");
            Ok(self.offsets.clone())
        }
        async fn members_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
            assert_eq!(pageno, 0, "test members all fit on page 0");
            Ok(self.members.clone())
        }
    }

    #[test]
    fn layout_constants_match_multixact_c() {
        assert_eq!(OFFSETS_PER_PAGE, 2048);
        assert_eq!(MEMBERGROUP_SIZE, 20);
        assert_eq!(MEMBERGROUPS_PER_PAGE, 409);
        assert_eq!(MEMBERS_PER_PAGE, 1636);
    }

    #[tokio::test]
    async fn grounded_against_live_bytes() {
        // Captured live from openltap-pg: FOR KEY SHARE held open by xid 768
        // while a concurrent non-key UPDATE (xid 769) proceeded produced
        // multixact 1, cross-checked via Postgres's own
        // pg_get_multixact_members('1') = {768: ForKeyShare, 769:
        // NoKeyUpdate}. offsets/0000 bytes 0..12: entry0(multi0)=0,
        // entry1(multi1)=1, entry2(multi2)=3 (2 members: offsets 1 and 2).
        // members/0000 bytes 0..20 (group 0): flags [00 00 04 00], then
        // xids [00000000, 00000300, 00000301, 00000000] LE.
        let mut offsets = vec![0u8; PAGE_SZ];
        offsets[4..8].copy_from_slice(&1u32.to_le_bytes());
        offsets[8..12].copy_from_slice(&3u32.to_le_bytes());

        let mut members = vec![0u8; PAGE_SZ];
        members[0..4].copy_from_slice(&[0x00, 0x00, 0x04, 0x00]); // flags for member-offsets 0..3
        members[8..12].copy_from_slice(&768u32.to_le_bytes()); // member-offset 1's xid
        members[12..16].copy_from_slice(&769u32.to_le_bytes()); // member-offset 2's xid

        let mut src = Pages { offsets, members };
        assert_eq!(resolve_updater(&mut src, 1).await.unwrap(), Some(769));
    }

    #[tokio::test]
    async fn all_lockers_returns_none() {
        let mut offsets = vec![0u8; PAGE_SZ];
        offsets[4..8].copy_from_slice(&1u32.to_le_bytes());
        offsets[8..12].copy_from_slice(&3u32.to_le_bytes());
        let mut members = vec![0u8; PAGE_SZ];
        members[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0x00]); // member 1 ForKeyShare, member 2 ForShare
        members[8..12].copy_from_slice(&100u32.to_le_bytes());
        members[12..16].copy_from_slice(&101u32.to_le_bytes());
        let mut src = Pages { offsets, members };
        assert_eq!(resolve_updater(&mut src, 1).await.unwrap(), None);
    }

    #[tokio::test]
    async fn most_recent_multixact_is_a_typed_error() {
        let offsets = vec![0u8; PAGE_SZ]; // multi=1 and multi=2 both unwritten (0)
        let members = vec![0u8; PAGE_SZ];
        let mut src = Pages { offsets, members };
        let err = resolve_updater(&mut src, 1).await.unwrap_err();
        assert!(err.to_string().contains("can't be determined"));
    }
}
