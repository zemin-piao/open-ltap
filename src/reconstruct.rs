//! V2c reverse path (P5) — rebuild an 8 KB Postgres heap page from decoded
//! rows. This is the research core of V2c (`docs/v2-scope.md` P5): once heap
//! pages are demoted, a `GetPage@LSN` miss below the fragment horizon must be
//! answered by *reconstructing* the page from the columnar fragments (+ the
//! delta-layer tail), such that every index-visible `(block, offnum)` resolves
//! to the right tuple. Nobody outside Databricks has shipped this.
//!
//! **Scope of this prototype.** It is deliberately a pure function
//! `(rows, catalog) -> [u8; 8192]` so it can be validated offline long before
//! it serves a live GetPage — the P5 plan's "prototype as a pure function
//! validated against pg_filedump/amcheck". What it does today:
//!   * exact line-pointer placement (LP_NORMAL / LP_UNUSED / LP_DEAD /
//!     LP_REDIRECT), so index pointers to any offnum resolve correctly (P3);
//!   * MAXALIGN'd tuple packing downward from pd_special, matching heapam;
//!   * frozen xmin by default (`FrozenTransactionId` — legal and simplest
//!     below the horizon), with per-tuple xmin/xmax/ctid/HOT flags available
//!     for HOT-chain reconstruction;
//!   * a recomputed page checksum (`pg_checksum_page`, checksum_impl.h).
//!
//! **Not yet:** dropped-column descriptors, on-page TOAST pointers /
//! compressed or >126-byte varlenas (they'd need the fragment's overflow-text
//! side channel — P6), and full HOT-chain *inference* (the caller supplies the
//! chain shape; we place it faithfully). These are the next increments.
//!
//! **Validation.** The strongest offline check is the round-trip: a page built
//! here decoded back through [`crate::wal::heap::decode_tuple_from_page`]
//! yields the exact input rows (see the tests). The checksum is spec-faithful
//! but has NOT yet been cross-checked against a real data-checksums cluster —
//! that (and pg_filedump/amcheck) is the live-gauntlet step.

use anyhow::{Result, bail};

use crate::schema::{PhysCol, TableDesc};
use crate::wal::heap::{Row, encode_attrs};

pub const PAGE_SIZE: usize = 8192;
const PAGE_HEADER_SIZE: usize = 24;
const ITEM_ID_SIZE: usize = 4;
/// `sizeof(HeapTupleHeaderData)` up to (not including) the null bitmap.
const HEAP_TUPLE_HEADER_SIZE: usize = 23;
/// `PG_PAGE_LAYOUT_VERSION` (bufpage.h), stable across the majors we target.
const PD_LAYOUT_VERSION: u16 = 4;
/// `FrozenTransactionId` — always-visible xmin, no CLOG lookup needed.
pub const FROZEN_XID: u32 = 2;

// t_infomask bits (htup_details.h).
const HEAP_HASNULL: u16 = 0x0001;
const HEAP_HASVARWIDTH: u16 = 0x0002;
const HEAP_XMAX_INVALID: u16 = 0x0800;
/// `HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID` — the "frozen" xmin hint.
const HEAP_XMIN_FROZEN: u16 = 0x0300;

// t_infomask2 bits.
const HEAP_NATTS_MASK: u16 = 0x07FF;
const HEAP_HOT_UPDATED: u16 = 0x4000;
const HEAP_ONLY_TUPLE: u16 = 0x8000;

// ItemIdData lp_flags (itemid.h).
const LP_UNUSED: u32 = 0;
const LP_NORMAL: u32 = 1;
const LP_REDIRECT: u32 = 2;
const LP_DEAD: u32 = 3;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// A tuple to place on the page. `xmin` defaults to [`FROZEN_XID`]; `ctid`
/// `None` means the line pointer points at itself (a non-updated tuple).
#[derive(Debug, Clone)]
pub struct TupleSpec {
    pub row: Row,
    pub xmin: u32,
    pub xmax: u32,
    /// t_ctid: `None` = self-pointer `(this_block, this_offnum)`; `Some` = the
    /// next version in a HOT/update chain.
    pub ctid: Option<(u32, u16)>,
    /// HEAP_ONLY_TUPLE — this version is reachable only via the HOT chain, not
    /// directly from an index.
    pub heap_only: bool,
    /// HEAP_HOT_UPDATED — this version was HOT-updated (a newer one exists in
    /// the chain).
    pub hot_updated: bool,
}

impl TupleSpec {
    /// A frozen, self-pointing tuple — the common fragment-materialized case.
    pub fn frozen(row: Row) -> Self {
        TupleSpec { row, xmin: FROZEN_XID, xmax: 0, ctid: None, heap_only: false, hot_updated: false }
    }
}

/// One line-pointer slot; slot index `i` is offnum `i + 1`.
#[derive(Debug, Clone)]
pub enum Slot {
    /// A live tuple (LP_NORMAL).
    Tuple(TupleSpec),
    /// LP_UNUSED — a reserved-but-empty slot.
    Unused,
    /// LP_DEAD — a dead line pointer (an index may still point here; the
    /// tuple's storage is gone).
    Dead,
    /// LP_REDIRECT — a HOT root redirecting to another offnum.
    Redirect(u16),
}

impl Slot {
    /// A frozen live tuple from a decoded row — the usual case.
    pub fn live(row: Row) -> Self {
        Slot::Tuple(TupleSpec::frozen(row))
    }
}

/// Rebuild the heap page at `block` holding `slots` (offnum = index + 1),
/// stamping page LSN `lsn`. Returns the 8192-byte page.
pub fn build_page(desc: &TableDesc, block: u32, lsn: u64, slots: &[Slot]) -> Result<Vec<u8>> {
    let mut page = vec![0u8; PAGE_SIZE];
    let pd_lower = PAGE_HEADER_SIZE + slots.len() * ITEM_ID_SIZE;
    if pd_lower > PAGE_SIZE {
        bail!("reconstruct: {} line pointers overflow the page", slots.len());
    }

    let mut pd_upper = PAGE_SIZE;
    for (i, slot) in slots.iter().enumerate() {
        let offnum = (i + 1) as u16;
        let itemid: u32 = match slot {
            Slot::Unused => LP_UNUSED << 15,
            Slot::Dead => LP_DEAD << 15,
            Slot::Redirect(target) => {
                if *target == 0 {
                    bail!("reconstruct: LP_REDIRECT target offnum must be > 0");
                }
                (*target as u32) | (LP_REDIRECT << 15)
            }
            Slot::Tuple(spec) => {
                let tup = encode_tuple(desc, block, offnum, spec)?;
                let slot_size = maxalign(tup.len());
                if pd_upper < pd_lower + slot_size {
                    bail!("reconstruct: page overflow placing tuple at offnum {offnum}");
                }
                pd_upper -= slot_size;
                page[pd_upper..pd_upper + tup.len()].copy_from_slice(&tup);
                // [pd_upper + tup.len() .. pd_upper + slot_size) is alignment
                // padding and stays zero, exactly as heapam leaves it.
                (pd_upper as u32) | (LP_NORMAL << 15) | ((tup.len() as u32) << 17)
            }
        };
        let idx = PAGE_HEADER_SIZE + i * ITEM_ID_SIZE;
        page[idx..idx + 4].copy_from_slice(&itemid.to_le_bytes());
    }

    write_page_header(&mut page, lsn, pd_lower as u16, pd_upper as u16);
    let checksum = pg_checksum_page(&page, block);
    page[8..10].copy_from_slice(&checksum.to_le_bytes());
    Ok(page)
}

/// Encode one on-disk heap tuple: HeapTupleHeaderData + null bitmap + padding
/// to t_hoff + the attribute bytes (the inverse of `decode_tuple_payload`).
fn encode_tuple(desc: &TableDesc, block: u32, offnum: u16, spec: &TupleSpec) -> Result<Vec<u8>> {
    let n_live = desc.cols.len();
    if spec.row.len() != n_live {
        bail!("reconstruct: row has {} values, table has {n_live} live columns", spec.row.len());
    }
    let attrs = encode_attrs(&spec.row, desc).ok_or_else(|| {
        anyhow::anyhow!(
            "reconstruct: row not encodable on-page (type mismatch, or a varlena \
             that would need TOAST/compression — P6, not yet supported)"
        )
    })?;

    // natts counts *physical* slots (heapam stores dropped columns as slots),
    // and a dropped column is always NULL in a freshly written tuple — so a
    // descriptor with any dropped column forces a null bitmap.
    let natts = desc.phys.len();
    let has_dropped = desc.phys.iter().any(|p| matches!(p, PhysCol::Dropped { .. }));
    let has_null_live = spec.row.iter().any(|v| v.is_none());
    let has_nulls = has_dropped || has_null_live;
    let bitmap_len = if has_nulls { natts.div_ceil(8) } else { 0 };
    let t_hoff = maxalign(HEAP_TUPLE_HEADER_SIZE + bitmap_len);

    let mut tup = vec![0u8; t_hoff];
    tup[0..4].copy_from_slice(&spec.xmin.to_le_bytes()); // t_xmin
    tup[4..8].copy_from_slice(&spec.xmax.to_le_bytes()); // t_xmax
    // t_field3 (t_cid / t_xvac) @8..12 stays 0.
    // t_ctid @12..18: BlockIdData { bi_hi u16, bi_lo u16 } + ip_posid u16.
    let (cblk, coff) = spec.ctid.unwrap_or((block, offnum));
    tup[12..14].copy_from_slice(&((cblk >> 16) as u16).to_le_bytes()); // bi_hi
    tup[14..16].copy_from_slice(&((cblk & 0xffff) as u16).to_le_bytes()); // bi_lo
    tup[16..18].copy_from_slice(&coff.to_le_bytes()); // ip_posid

    let mut t_infomask2 = (natts as u16) & HEAP_NATTS_MASK;
    if spec.heap_only {
        t_infomask2 |= HEAP_ONLY_TUPLE;
    }
    if spec.hot_updated {
        t_infomask2 |= HEAP_HOT_UPDATED;
    }
    tup[18..20].copy_from_slice(&t_infomask2.to_le_bytes());

    let mut t_infomask = 0u16;
    if spec.xmax == 0 {
        t_infomask |= HEAP_XMAX_INVALID;
    }
    if spec.xmin == FROZEN_XID {
        t_infomask |= HEAP_XMIN_FROZEN;
    }
    if has_nulls {
        t_infomask |= HEAP_HASNULL;
    }
    let has_varwidth = desc
        .cols
        .iter()
        .zip(&spec.row)
        .any(|(c, v)| v.is_some() && matches!(c.ty, crate::schema::PgType::Text | crate::schema::PgType::Bytea));
    if has_varwidth {
        t_infomask |= HEAP_HASVARWIDTH;
    }
    tup[20..22].copy_from_slice(&t_infomask.to_le_bytes());
    tup[22] = t_hoff as u8;

    // Null bitmap over *physical* slots: a set bit means present. A live slot
    // is present iff its row value is Some; a dropped slot is always NULL
    // (bit clear), which is how heapam writes new tuples of an altered table.
    if has_nulls {
        let mut live_i = 0usize;
        for (i, pc) in desc.phys.iter().enumerate() {
            if let PhysCol::Live(_) = pc {
                if spec.row[live_i].is_some() {
                    tup[HEAP_TUPLE_HEADER_SIZE + i / 8] |= 1 << (i % 8);
                }
                live_i += 1;
            }
        }
    }

    tup.extend_from_slice(&attrs);
    Ok(tup)
}

fn write_page_header(page: &mut [u8], lsn: u64, pd_lower: u16, pd_upper: u16) {
    // pd_lsn: PageXLogRecPtr { xlogid (high 32), xrecoff (low 32) }.
    page[0..4].copy_from_slice(&((lsn >> 32) as u32).to_le_bytes());
    page[4..8].copy_from_slice(&(lsn as u32).to_le_bytes());
    // pd_checksum @8..10 left 0 (filled after the block checksum is computed).
    // pd_flags @10..12 = 0.
    page[12..14].copy_from_slice(&pd_lower.to_le_bytes());
    page[14..16].copy_from_slice(&pd_upper.to_le_bytes());
    page[16..18].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes()); // pd_special: no special space
    page[18..20].copy_from_slice(&((PAGE_SIZE as u16) | PD_LAYOUT_VERSION).to_le_bytes());
    // pd_prune_xid @20..24 = 0.
}

/// The 32 base offsets `pg_checksum_block` seeds its sums with (checksum_impl.h).
const CHECKSUM_BASE_OFFSETS: [u32; 32] = [
    0x5B1F_36E9, 0xB852_5960, 0x02AB_50AA, 0x1DE6_6D2A, 0x79FF_467A, 0x9BB9_F8A3, 0x217E_7CD2,
    0x83E1_3D2C, 0xF8D4_474F, 0xE39E_B970, 0x42C6_AE16, 0x9932_16FA, 0x7B09_3B5D, 0x98DA_FF3C,
    0xF718_902A, 0x0B1C_9CDB, 0xE58F_764B, 0x1876_36BC, 0x5D7B_3BB1, 0xE73D_E7DE, 0x92BE_C979,
    0xCCA6_C0B2, 0x304A_0979, 0x85AA_43D4, 0x7831_25BB, 0x6CA8_EAA2, 0xE407_EAC6, 0x4B5C_FC3E,
    0x9FBF_8C76, 0x15CA_20BE, 0xF2CA_9FD3, 0x959B_D756,
];

const FNV_PRIME: u32 = 16_777_619;

/// `pg_checksum_block` (checksum_impl.h): 32 interleaved FNV-1a-ish sums over
/// the page's 2048 uint32 words, folded to one uint32.
fn pg_checksum_block(page: &[u8]) -> u32 {
    const N_SUMS: usize = 32;
    let mut sums = CHECKSUM_BASE_OFFSETS;
    for i in 0..(PAGE_SIZE / (4 * N_SUMS)) {
        for (j, sum) in sums.iter_mut().enumerate() {
            let idx = (i * N_SUMS + j) * 4;
            let value = u32::from_le_bytes(page[idx..idx + 4].try_into().unwrap());
            let tmp = *sum ^ value;
            *sum = tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17);
        }
    }
    sums.iter().fold(0u32, |acc, &s| acc ^ s)
}

/// `pg_checksum_page`: the block checksum computed with pd_checksum treated as
/// zero, mixed with the block number, folded to a non-zero 16-bit value. Also
/// serves as the verifier for an already-stamped page.
pub fn pg_checksum_page(page: &[u8], blkno: u32) -> u16 {
    let mut buf = page.to_vec();
    buf[8] = 0;
    buf[9] = 0;
    let checksum = pg_checksum_block(&buf) ^ blkno;
    ((checksum % 65535) + 1) as u16
}

/// A decoded ItemIdData (for tests / offline inspection).
#[derive(Debug, PartialEq, Eq)]
pub struct LinePointer {
    pub flags: u8,
    pub off: u16,
    pub len: u16,
}

/// Read the line pointer for `offnum` (1-based) out of a page.
pub fn line_pointer(page: &[u8], offnum: u16) -> Result<LinePointer> {
    if offnum == 0 {
        bail!("invalid offnum 0");
    }
    let idx = PAGE_HEADER_SIZE + (offnum as usize - 1) * ITEM_ID_SIZE;
    let raw = page.get(idx..idx + 4).ok_or_else(|| anyhow::anyhow!("offnum {offnum} beyond pd_lower"))?;
    let itemid = u32::from_le_bytes(raw.try_into().unwrap());
    Ok(LinePointer {
        off: (itemid & 0x7FFF) as u16,
        flags: ((itemid >> 15) & 0x3) as u8,
        len: (itemid >> 17) as u16,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::heap::{ToastCache, Value, decode_tuple_from_page};

    fn desc() -> TableDesc {
        // (id int4, txt text) — no dropped columns.
        use crate::schema::{Col, PgType};
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

    fn read(page: &[u8], offnum: u16) -> Row {
        decode_tuple_from_page(page, offnum, &desc(), &ToastCache::default()).unwrap().0
    }

    #[test]
    fn single_row_round_trips() {
        let page = build_page(&desc(), 5, 0, &[Slot::live(row(1, "hello"))]).unwrap();
        assert_eq!(page.len(), PAGE_SIZE);
        assert_eq!(read(&page, 1), row(1, "hello"));
    }

    #[test]
    fn multiple_rows_and_a_gap() {
        let slots = vec![
            Slot::live(row(10, "a")),
            Slot::Unused,
            Slot::live(row(30, "ccc")),
        ];
        let page = build_page(&desc(), 7, 0, &slots).unwrap();
        assert_eq!(read(&page, 1), row(10, "a"));
        assert_eq!(read(&page, 3), row(30, "ccc"));
        // The gap is LP_UNUSED and decodes to nothing.
        assert_eq!(line_pointer(&page, 2).unwrap().flags, LP_UNUSED as u8);
        assert!(decode_tuple_from_page(&page, 2, &desc(), &ToastCache::default()).is_err());
    }

    #[test]
    fn nulls_round_trip_in_both_positions() {
        let null_txt: Row = vec![Some(Value::I32(1)), None];
        let null_id: Row = vec![None, Some(Value::Text("x".into()))];
        let page =
            build_page(&desc(), 0, 0, &[Slot::live(null_txt.clone()), Slot::live(null_id.clone())]).unwrap();
        assert_eq!(read(&page, 1), null_txt);
        assert_eq!(read(&page, 2), null_id);
    }

    #[test]
    fn tuples_are_maxaligned_and_do_not_overlap() {
        // An odd-length text (2 bytes payload → 3-byte short varlena) forces
        // MAXALIGN padding before the next tuple.
        let page = build_page(&desc(), 1, 0, &[Slot::live(row(1, "ab")), Slot::live(row(2, "cd"))]).unwrap();
        let lp1 = line_pointer(&page, 1).unwrap();
        let lp2 = line_pointer(&page, 2).unwrap();
        assert_eq!(lp1.off % 8, 0, "tuple starts are MAXALIGN(8)'d");
        assert_eq!(lp2.off % 8, 0);
        // Distinct, non-overlapping storage.
        let (a, b) = (lp1.off as usize, lp2.off as usize);
        let (a_end, b_end) = (a + lp1.len as usize, b + lp2.len as usize);
        assert!(a_end <= b || b_end <= a, "tuple storage overlaps");
        assert_eq!(read(&page, 1), row(1, "ab"));
        assert_eq!(read(&page, 2), row(2, "cd"));
    }

    #[test]
    fn header_geometry_is_consistent() {
        let page = build_page(&desc(), 3, 0, &[Slot::live(row(1, "z")), Slot::Unused]).unwrap();
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        let pd_upper = u16::from_le_bytes([page[14], page[15]]) as usize;
        let pd_special = u16::from_le_bytes([page[16], page[17]]) as usize;
        assert_eq!(pd_lower, PAGE_HEADER_SIZE + 2 * ITEM_ID_SIZE);
        assert!(pd_lower <= pd_upper && pd_upper <= pd_special);
        assert_eq!(pd_special, PAGE_SIZE);
        assert_eq!(u16::from_le_bytes([page[18], page[19]]), (PAGE_SIZE as u16) | PD_LAYOUT_VERSION);
    }

    #[test]
    fn checksum_verifies_and_is_sensitive() {
        let page = build_page(&desc(), 42, 0, &[Slot::live(row(1, "hello"))]).unwrap();
        let stored = u16::from_le_bytes([page[8], page[9]]);
        assert_ne!(stored, 0, "pd_checksum is never zero");
        assert_eq!(pg_checksum_page(&page, 42), stored, "recomputes to the stored value");

        // Sensitive to block number...
        assert_ne!(pg_checksum_page(&page, 43), stored);
        // ...and to content.
        let mut tampered = page.clone();
        tampered[PAGE_SIZE - 1] ^= 0xFF;
        assert_ne!(pg_checksum_page(&tampered, 42), stored);
    }

    #[test]
    fn redirect_and_dead_line_pointers() {
        let slots = vec![Slot::Redirect(3), Slot::Dead, Slot::live(row(9, "root"))];
        let page = build_page(&desc(), 0, 0, &slots).unwrap();
        let lp1 = line_pointer(&page, 1).unwrap();
        assert_eq!(lp1.flags, LP_REDIRECT as u8);
        assert_eq!(lp1.off, 3, "redirect target offnum is stored in lp_off");
        assert_eq!(line_pointer(&page, 2).unwrap().flags, LP_DEAD as u8);
        assert_eq!(read(&page, 3), row(9, "root"));
    }

    #[test]
    fn hot_chain_ctids_and_flags_are_placed() {
        // Offnum 1 is a HOT root updated to offnum 2 (heap-only).
        let mut root = TupleSpec::frozen(row(1, "v1"));
        root.ctid = Some((0, 2));
        root.hot_updated = true;
        let mut child = TupleSpec::frozen(row(1, "v2"));
        child.heap_only = true;
        let page = build_page(&desc(), 0, 0, &[Slot::Tuple(root), Slot::Tuple(child)]).unwrap();

        // Both versions decode; the chain link is in the root's t_ctid.
        assert_eq!(read(&page, 1), row(1, "v1"));
        assert_eq!(read(&page, 2), row(1, "v2"));
        let lp1 = line_pointer(&page, 1).unwrap();
        let tup1 = &page[lp1.off as usize..lp1.off as usize + lp1.len as usize];
        let ctid_off = u16::from_le_bytes([tup1[16], tup1[17]]);
        assert_eq!(ctid_off, 2, "root t_ctid points at the HOT child");
        let im2 = u16::from_le_bytes([tup1[18], tup1[19]]);
        assert_ne!(im2 & HEAP_HOT_UPDATED, 0);
    }

    #[test]
    fn dropped_column_round_trips() {
        // (id int4, <dropped int4>, txt text): heapam stores the dropped slot as
        // NULL, so a null bitmap appears even when both live values are present.
        use crate::schema::{Col, PgType};
        let cols =
            vec![Col { name: "id".into(), ty: PgType::Int4 }, Col { name: "txt".into(), ty: PgType::Text }];
        let d = TableDesc {
            name: "t".into(),
            oid: 40000,
            db_oid: 5,
            rel_node: 40000,
            toast_rel_node: None,
            phys: vec![
                PhysCol::Live(cols[0].clone()),
                PhysCol::Dropped { attlen: 4, align: 4 },
                PhysCol::Live(cols[1].clone()),
            ],
            cols,
            has_fast_defaults: false,
            pk: vec!["id".into()],
        };
        let page = build_page(&d, 0, 0, &[Slot::live(row(7, "hi")), Slot::live(vec![Some(Value::I32(8)), None])])
            .unwrap();
        let dec = |o| decode_tuple_from_page(&page, o, &d, &ToastCache::default()).unwrap().0;
        assert_eq!(dec(1), row(7, "hi"));
        assert_eq!(dec(2), vec![Some(Value::I32(8)), None]); // live NULL alongside the dropped slot
    }

    #[test]
    fn oversized_varlena_is_rejected() {
        // >126-byte text can't be a short on-page varlena — needs TOAST (P6).
        let big = "x".repeat(200);
        assert!(build_page(&desc(), 0, 0, &[Slot::live(row(1, &big))]).is_err());
    }
}
