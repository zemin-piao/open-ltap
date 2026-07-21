//! Catalog-from-pages end-to-end (`src/catalog.rs`, V2a unit C / P0-2):
//! synthetic pg_class / pg_attribute / pg_index heap pages driven through the
//! real `Catalog::load` → `desc()`, including CLOG-resolved visibility
//! (v2-scope P2). Complements the field-parser unit tests inside the module by
//! exercising the whole assembly: line-pointer walk, FormData decode, the
//! mapped-vs-own-pg_class filenode split for pg_index, PK derivation, dropped
//! slots / fast defaults, and — the load-bearing bit — that a stale (aborted)
//! catalog tuple version is dropped by the CLOG check, not by luck of scan
//! order.

use std::collections::HashMap;

use anyhow::Result;
use open_ltap::catalog::{Catalog, MappedRels, PG_INDEX_OID, PageSource};
use open_ltap::clog::{CLOG_XACTS_PER_BYTE, CLOG_XACTS_PER_PAGE, ClogSource, xid_to_page};
use open_ltap::schema::{PgType, PhysCol};

const PAGE_SZ: usize = 8192;

// ---------------------------------------------------------------------------
// Synthetic heap page + tuple + FormData builders
// ---------------------------------------------------------------------------

/// One heap tuple as `page_tuples` reads it: a 24-byte header (xmin@0, xmax@4,
/// t_infomask@20, t_hoff@22) followed by the FormData attribute bytes.
fn tuple(xmin: u32, xmax: u32, infomask: u16, formdata: &[u8]) -> Vec<u8> {
    let mut t = vec![0u8; 24];
    t[0..4].copy_from_slice(&xmin.to_le_bytes());
    t[4..8].copy_from_slice(&xmax.to_le_bytes());
    t[20..22].copy_from_slice(&infomask.to_le_bytes());
    t[22] = 24; // t_hoff: no null bitmap, MAXALIGN(23) = 24
    t.extend_from_slice(formdata);
    t
}

/// An 8 KB heap page: 24-byte page header, an ItemId array from offset 24, and
/// the tuples packed downward from the end (as Postgres lays them out).
fn heap_page(tuples: &[Vec<u8>]) -> Vec<u8> {
    let mut page = vec![0u8; PAGE_SZ];
    let pd_lower = 24 + 4 * tuples.len();
    let mut cursor = PAGE_SZ;
    for (i, t) in tuples.iter().enumerate() {
        let len = t.len();
        cursor -= len;
        cursor -= cursor % 8; // MAXALIGN the tuple start
        page[cursor..cursor + len].copy_from_slice(t);
        // ItemIdData: (len << 17) | (LP_NORMAL << 15) | off
        let lp = ((len as u32) << 17) | (1u32 << 15) | (cursor as u32);
        let lp_off = 24 + 4 * i;
        page[lp_off..lp_off + 4].copy_from_slice(&lp.to_le_bytes());
    }
    page[12..14].copy_from_slice(&(pd_lower as u16).to_le_bytes()); // pd_lower
    page[14..16].copy_from_slice(&(cursor as u16).to_le_bytes()); // pd_upper
    page
}

fn put_name(a: &mut [u8], off: usize, name: &str) {
    a[off..off + name.len()].copy_from_slice(name.as_bytes());
}

/// FormData_pg_class (PG17 offsets, see catalog.rs::parse_pg_class).
#[allow(clippy::too_many_arguments)]
fn pg_class(
    oid: u32,
    name: &str,
    namespace: u32,
    filenode: u32,
    toastrelid: u32,
    relkind: u8,
    natts: i16,
) -> Vec<u8> {
    let mut a = vec![0u8; 118];
    a[0..4].copy_from_slice(&oid.to_le_bytes());
    put_name(&mut a, 4, name);
    a[68..72].copy_from_slice(&namespace.to_le_bytes());
    a[88..92].copy_from_slice(&filenode.to_le_bytes());
    a[108..112].copy_from_slice(&toastrelid.to_le_bytes());
    a[115] = relkind;
    a[116..118].copy_from_slice(&natts.to_le_bytes());
    a
}

/// FormData_pg_attribute (PG17 offsets, see catalog.rs::parse_pg_attribute).
fn pg_attr(
    attrelid: u32,
    name: &str,
    typid: u32,
    attlen: i16,
    attnum: i16,
    attalign: u8,
    hasmissing: bool,
    isdropped: bool,
) -> Vec<u8> {
    let mut a = vec![0u8; 96];
    a[0..4].copy_from_slice(&attrelid.to_le_bytes());
    put_name(&mut a, 4, name);
    a[68..72].copy_from_slice(&typid.to_le_bytes());
    a[72..74].copy_from_slice(&attlen.to_le_bytes());
    a[74..76].copy_from_slice(&attnum.to_le_bytes());
    a[87] = attalign;
    a[92] = hasmissing as u8;
    a[95] = isdropped as u8;
    a
}

/// FormData_pg_index + indkey int2vector (PG17, see catalog.rs::parse_pg_index).
fn pg_index(indexrelid: u32, indrelid: u32, isprimary: bool, keys: &[i16]) -> Vec<u8> {
    let n = keys.len();
    let mut a = vec![0u8; 48 + n * 2];
    a[0..4].copy_from_slice(&indexrelid.to_le_bytes());
    a[4..8].copy_from_slice(&indrelid.to_le_bytes());
    a[8..10].copy_from_slice(&(n as i16).to_le_bytes()); // indnatts
    a[10..12].copy_from_slice(&(n as i16).to_le_bytes()); // indnkeyatts
    a[12] = 1; // indisunique
    a[14] = isprimary as u8;
    // int2vector at 24: vl_len (4B header, len<<2), ndim, dataoffset, elemtype,
    // dim1, lbound1, then int16 values from 48.
    let vl = ((24 + n * 2) as u32) << 2;
    a[24..28].copy_from_slice(&vl.to_le_bytes());
    a[28..32].copy_from_slice(&1i32.to_le_bytes()); // ndim
    a[36..40].copy_from_slice(&21u32.to_le_bytes()); // elemtype int2
    a[40..44].copy_from_slice(&(n as i32).to_le_bytes()); // dim1
    a[44..48].copy_from_slice(&1i32.to_le_bytes()); // lbound1
    for (i, k) in keys.iter().enumerate() {
        a[48 + i * 2..50 + i * 2].copy_from_slice(&k.to_le_bytes());
    }
    a
}

// ---------------------------------------------------------------------------
// Fake sources
// ---------------------------------------------------------------------------

struct FakeSource {
    db: u32,
    pages: HashMap<u32, Vec<Vec<u8>>>,
}

impl PageSource for FakeSource {
    fn db(&self) -> u32 {
        self.db
    }
    async fn rel_nblocks(&mut self, filenode: u32) -> Result<u32> {
        Ok(self.pages.get(&filenode).map_or(0, |p| p.len()) as u32)
    }
    async fn get_page(&mut self, filenode: u32, blk: u32) -> Result<Vec<u8>> {
        Ok(self.pages[&filenode][blk as usize].clone())
    }
}

/// CLOG source over an in-memory status map (1 = committed, 2 = aborted); any
/// xid not listed reads as InProgress (all-zero page), just like real CLOG.
struct FakeClog {
    status: HashMap<u32, u8>,
}

impl ClogSource for FakeClog {
    async fn clog_page(&mut self, pageno: u32) -> Result<Vec<u8>> {
        let mut page = vec![0u8; PAGE_SZ];
        for (&xid, &st) in &self.status {
            if xid_to_page(xid) != pageno {
                continue;
            }
            let idx = xid % CLOG_XACTS_PER_PAGE;
            let byteno = (idx / CLOG_XACTS_PER_BYTE) as usize;
            let shift = (idx % CLOG_XACTS_PER_BYTE) * 2;
            page[byteno] |= st << shift;
        }
        Ok(page)
    }
}

// public namespace oid = 2200 (catalog.rs PUBLIC_NAMESPACE_OID); pg_catalog =
// 11; pg_toast = 99 (only its inequality with 2200 matters here).
const PUBLIC_NS: u32 = 2200;
const CATALOG_NS: u32 = 11;
const TOAST_NS: u32 = 99;

const T_OID: u32 = 40000;
const T_FILENODE: u32 = 40000;
const T_STALE_FILENODE: u32 = 39000; // an earlier, rewritten filenode of `t`
const TOAST_OID: u32 = 40010;
const PG_INDEX_FILENODE: u32 = 2700;

const XID_COMMIT: u32 = 100;
const XID_DELETER: u32 = 101;
const XID_ABORT: u32 = 200;

/// Build the three catalog pages for a single user table `t`:
///   id int4 PRIMARY KEY, <dropped int4>, name text (with a fast default).
/// `stale_committed` decides whether the aborted pre-rewrite `t` version is
/// (incorrectly) marked committed — the visibility knob under test.
fn build(db: u32, stale_committed: bool) -> (FakeSource, FakeClog) {
    let class_page = heap_page(&[
        // stale pre-rewrite version of `t` (old filenode) — aborted.
        tuple(XID_ABORT, 0, 0, &pg_class(T_OID, "t", PUBLIC_NS, T_STALE_FILENODE, TOAST_OID, b'r', 3)),
        // current committed `t`.
        tuple(XID_COMMIT, 0, 0, &pg_class(T_OID, "t", PUBLIC_NS, T_FILENODE, TOAST_OID, b'r', 3)),
        // a dropped table: inserted then deleted (xmax committed) — invisible.
        tuple(XID_COMMIT, XID_DELETER, 0, &pg_class(40001, "t_gone", PUBLIC_NS, 40001, 0, b'r', 1)),
        // t's toast rel (excluded from table_names by relkind/namespace).
        tuple(XID_COMMIT, 0, 0, &pg_class(TOAST_OID, "pg_toast_40000", TOAST_NS, TOAST_OID, 0, b't', 3)),
        // pg_index's own pg_class row — supplies its (unmapped) filenode.
        tuple(XID_COMMIT, 0, 0, &pg_class(PG_INDEX_OID, "pg_index", CATALOG_NS, PG_INDEX_FILENODE, 0, b'r', 3)),
    ]);

    let attr_page = heap_page(&[
        tuple(XID_COMMIT, 0, 0, &pg_attr(T_OID, "id", 23, 4, 1, b'i', false, false)),
        tuple(XID_COMMIT, 0, 0, &pg_attr(T_OID, "........pg.dropped.2........", 0, 4, 2, b'i', false, true)),
        tuple(XID_COMMIT, 0, 0, &pg_attr(T_OID, "name", 25, -1, 3, b'i', true, false)),
    ]);

    let index_page = heap_page(&[
        tuple(XID_COMMIT, 0, 0, &pg_index(50000, T_OID, true, &[1])),
    ]);

    let mut pages = HashMap::new();
    pages.insert(700u32, vec![class_page]); // mapped pg_class filenode
    pages.insert(701u32, vec![attr_page]); // mapped pg_attribute filenode
    pages.insert(PG_INDEX_FILENODE, vec![index_page]);

    let mut status = HashMap::from([(XID_COMMIT, 1u8), (XID_DELETER, 1u8)]);
    status.insert(XID_ABORT, if stale_committed { 1 } else { 2 });

    (FakeSource { db, pages }, FakeClog { status })
}

fn mapped() -> MappedRels {
    // pg_class / pg_attribute are mapped; pg_index is not (comes from its own
    // pg_class row above).
    MappedRels { pg_class: 700, pg_attribute: 701, pg_index: None }
}

#[tokio::test]
async fn derives_desc_with_pk_dropped_slot_and_fast_default() {
    let (mut src, mut clog) = build(5, false);
    let cat = Catalog::load(&mut src, &mut clog, &mapped()).await.unwrap();

    // Only the live public ordinary table shows up (toast/pg_index excluded by
    // namespace/relkind; t_gone excluded by visibility).
    assert_eq!(cat.table_names(), vec!["t"]);

    let d = cat.desc("t").unwrap();
    assert_eq!(d.name, "t");
    assert_eq!(d.db_oid, 5);
    // The committed version wins, not the aborted pre-rewrite filenode.
    assert_eq!(d.rel_node, T_FILENODE);
    assert_eq!(d.toast_rel_node, Some(TOAST_OID));

    // Live columns, in order, with the dropped slot skipped.
    let names: Vec<&str> = d.cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name"]);
    assert_eq!(d.cols[0].ty, PgType::Int4);
    assert_eq!(d.cols[1].ty, PgType::Text);

    // Physical slots keep the dropped column between the two live ones.
    assert_eq!(d.phys.len(), 3);
    assert!(matches!(d.phys[0], PhysCol::Live(_)));
    match &d.phys[1] {
        PhysCol::Dropped { attlen, align } => {
            assert_eq!(*attlen, 4);
            assert_eq!(*align, 4);
        }
        other => panic!("expected dropped slot, got {other:?}"),
    }
    assert!(matches!(d.phys[2], PhysCol::Live(_)));

    // atthasmissing on `name` → fast defaults present; PK from pg_index.
    assert!(d.has_fast_defaults);
    assert_eq!(d.pk, vec!["id"]);
}

#[tokio::test]
async fn stale_version_leaks_when_clog_is_not_consulted() {
    // Same pages, but the aborted pre-rewrite `t` row is (wrongly) committed.
    // Both versions are then visible, proving the CLOG check in the test above
    // — not scan order or dedup — is what dropped the stale one.
    let (mut src, mut clog) = build(5, true);
    let cat = Catalog::load(&mut src, &mut clog, &mapped()).await.unwrap();
    assert_eq!(cat.table_names(), vec!["t", "t"]);
}
