//! Catalog-from-pages: derive `TableDesc`s by decoding `pg_class` /
//! `pg_attribute` / `pg_index` heap pages directly — zero SQL (P0-2,
//! productized for V2a; hard problem P1 in `docs/v2-scope.md`).
//!
//! Page-source-agnostic: the same derivation runs over an image-layer file
//! (`examples/layerscan.rs`), the pagestream API (`pgwire::ReplConn`), or
//! native `Timeline` reads inside the pageserver fork. The relmapper blob is
//! *not* a relation page (it lives at its own key in the pageserver keyspace
//! and in `global/pg_filenode.map` on disk), so sources that can read it use
//! [`parse_relmap`] + [`MappedRels::from_relmap`]; sources that can't (the
//! pagestream API serves only rel blocks) obtain the two mapped filenodes out
//! of band.
//!
//! Visibility: real CLOG-resolved xmin/xmax visibility, via [`crate::clog`]
//! (v2-scope P2) — [`Catalog::load`] takes a [`crate::clog::ClogSource`] and
//! resolves every catalog tuple version through it. This replaced the P0-2
//! spike heuristic (`xmax == 0`); see `crate::clog`'s module doc for what
//! "visibility" means here and its one honest gap (current-state CLOG reads,
//! not yet pinned to an arbitrary past LSN).
//!
//! All FormData offsets are PG17 (fetched from `REL_17_STABLE` headers, and
//! byte-identical in PG18 for the fields used here — see the layout notes on
//! each parser).

use std::collections::HashMap;

use anyhow::{Context, Result, bail};

use crate::clog::{ClogSource, tuple_visible};
use crate::schema::{Col, PgType, PhysCol, TableDesc};
use crate::wal::heap::{ToastCache, decode_toast_chunk_from_page};

pub const PG_CLASS_OID: u32 = 1259;
pub const PG_ATTRIBUTE_OID: u32 = 1249;
pub const PG_INDEX_OID: u32 = 2610;
/// `pg_namespace` oid of `public` — the namespace the engine tracks.
pub const PUBLIC_NAMESPACE_OID: u32 = 2200;

const PAGE_SZ: usize = 8192;

/// A source of main-fork relation pages for one database, consistent at one
/// LSN. `rel_pages` has a default implementation over `rel_nblocks` +
/// `get_page`; sources that can enumerate a relation's pages more cheaply
/// (an image-layer file) override it.
pub trait PageSource {
    fn db(&self) -> u32;
    async fn rel_nblocks(&mut self, filenode: u32) -> Result<u32>;
    async fn get_page(&mut self, filenode: u32, blk: u32) -> Result<Vec<u8>>;
    async fn rel_pages(&mut self, filenode: u32) -> Result<Vec<Vec<u8>>> {
        let n = self.rel_nblocks(filenode).await?;
        let mut pages = Vec::with_capacity(n as usize);
        for blk in 0..n {
            pages.push(self.get_page(filenode, blk).await?);
        }
        Ok(pages)
    }
}

/// oid -> filenode for mapped relations, from the 512-byte `pg_filenode.map`
/// blob (relmapper.c `RelMapFile`: magic, num_mappings, then (oid, filenode)
/// pairs — little-endian like everything on-page).
pub fn parse_relmap(b: &[u8]) -> Result<HashMap<u32, u32>> {
    const RELMAP_MAGIC: u32 = 0x59_27_17; // relmapper.c RELMAPPER_FILEMAGIC
    if b.len() < 8 {
        bail!("relmap blob too short: {} bytes", b.len());
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != RELMAP_MAGIC {
        bail!("relmap magic {magic:#x} != {RELMAP_MAGIC:#x}");
    }
    let n = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
    let mut map = HashMap::with_capacity(n);
    for i in 0..n {
        let off = 8 + i * 8;
        let oid = u32::from_le_bytes(b.get(off..off + 4).context("relmap truncated")?.try_into().unwrap());
        let node = u32::from_le_bytes(b[off + 4..off + 8].try_into().unwrap());
        map.insert(oid, node);
    }
    Ok(map)
}

/// The filenodes of the catalogs we must read before `pg_class` can tell us
/// anything: `pg_class` and `pg_attribute` are mapped relations (their own
/// `pg_class.relfilenode` reads 0). `pg_index` is not mapped — its filenode
/// comes from its `pg_class` row — but a relmapper entry wins if one exists.
#[derive(Debug, Clone, Copy)]
pub struct MappedRels {
    pub pg_class: u32,
    pub pg_attribute: u32,
    pub pg_index: Option<u32>,
}

impl MappedRels {
    pub fn from_relmap(relmap: &HashMap<u32, u32>) -> Result<Self> {
        Ok(MappedRels {
            pg_class: *relmap.get(&PG_CLASS_OID).context("pg_class not in relmapper")?,
            pg_attribute: *relmap
                .get(&PG_ATTRIBUTE_OID)
                .context("pg_attribute not in relmapper")?,
            pg_index: relmap.get(&PG_INDEX_OID).copied(),
        })
    }
}

/// Every LP_NORMAL tuple on a heap page as (xmin, xmax, t_infomask,
/// attribute bytes from t_hoff on). The attribute bytes of a catalog tuple
/// are its FormData C struct. `xmin`/`xmax`/`infomask` are exactly what
/// [`crate::clog::tuple_visible`] needs to decide whether this row version
/// is live.
pub fn page_tuples(page: &[u8]) -> Vec<(u32, u32, u16, Vec<u8>)> {
    let mut out = Vec::new();
    if page.len() != PAGE_SZ {
        return out;
    }
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    for lp_off in (24..pd_lower.min(PAGE_SZ)).step_by(4) {
        let lp = u32::from_le_bytes(page[lp_off..lp_off + 4].try_into().unwrap());
        let (off, flags, len) = ((lp & 0x7FFF) as usize, (lp >> 15) & 3, (lp >> 17) as usize);
        if flags != 1 || off + len > PAGE_SZ || len < 23 {
            continue; // not LP_NORMAL
        }
        let tup = &page[off..off + len];
        let xmin = u32::from_le_bytes(tup[0..4].try_into().unwrap());
        let xmax = u32::from_le_bytes(tup[4..8].try_into().unwrap());
        let infomask = u16::from_le_bytes(tup[20..22].try_into().unwrap());
        let t_hoff = tup[22] as usize;
        if t_hoff <= len {
            out.push((xmin, xmax, infomask, tup[t_hoff..].to_vec()));
        }
    }
    out
}

fn name64(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len().min(64));
    String::from_utf8_lossy(&b[..end]).into_owned()
}

/// FormData_pg_class fixed part (PG17): oid 0, relname 4 (NameData, 64B),
/// then 7 oids (relnamespace 68, reltype, reloftype, relowner, relam,
/// relfilenode 88, reltablespace 92), relpages/reltuples/relallvisible
/// 96..108, reltoastrelid 108, relkind 115, relnatts 116.
#[derive(Debug, Clone)]
pub struct ClassRow {
    pub oid: u32,
    pub name: String,
    pub namespace: u32,
    pub filenode: u32,
    pub toastrelid: u32,
    pub relkind: u8,
    pub natts: i16,
}

fn parse_pg_class(a: &[u8]) -> Option<ClassRow> {
    Some(ClassRow {
        oid: u32::from_le_bytes(a.get(0..4)?.try_into().unwrap()),
        name: name64(a.get(4..68)?),
        namespace: u32::from_le_bytes(a.get(68..72)?.try_into().unwrap()),
        filenode: u32::from_le_bytes(a.get(88..92)?.try_into().unwrap()),
        toastrelid: u32::from_le_bytes(a.get(108..112)?.try_into().unwrap()),
        relkind: *a.get(115)?,
        natts: i16::from_le_bytes(a.get(116..118)?.try_into().unwrap()),
    })
}

/// FormData_pg_attribute fixed part (PG17, attcacheoff still present):
/// attrelid 0, attname 4, atttypid 68, attlen 72, attnum 74, attcacheoff 76,
/// atttypmod 80, attndims 84, attbyval 86, attalign 87, ... atthasmissing 92,
/// ... attisdropped 95.
#[derive(Debug, Clone)]
pub struct AttRow {
    pub attrelid: u32,
    pub name: String,
    pub typid: u32,
    pub attlen: i16,
    pub attnum: i16,
    pub attalign: u8,
    pub hasmissing: bool,
    pub isdropped: bool,
}

fn parse_pg_attribute(a: &[u8]) -> Option<AttRow> {
    Some(AttRow {
        attrelid: u32::from_le_bytes(a.get(0..4)?.try_into().unwrap()),
        name: name64(a.get(4..68)?),
        typid: u32::from_le_bytes(a.get(68..72)?.try_into().unwrap()),
        attlen: i16::from_le_bytes(a.get(72..74)?.try_into().unwrap()),
        attnum: i16::from_le_bytes(a.get(74..76)?.try_into().unwrap()),
        attalign: *a.get(87)?,
        hasmissing: *a.get(92)? != 0,
        isdropped: *a.get(95)? != 0,
    })
}

/// FormData_pg_index fixed part (PG17): indexrelid 0, indrelid 4, indnatts 8,
/// indnkeyatts 10, then 11 bools 12..22 (indisprimary 14, indisvalid 18,
/// indislive 21); `indkey` (int2vector, typalign 'i', typstorage plain so its
/// 4-byte varlena header survives) at 24: vl_len_ 24, ndim 28, dataoffset 32,
/// elemtype 36, dim1 40, lbound1 44, int16 values from 48.
#[derive(Debug, Clone)]
pub struct IndexRow {
    pub indrelid: u32,
    pub isprimary: bool,
    pub key_attnums: Vec<i16>,
}

fn parse_pg_index(a: &[u8]) -> Option<IndexRow> {
    let indrelid = u32::from_le_bytes(a.get(4..8)?.try_into().unwrap());
    let nkeyatts = i16::from_le_bytes(a.get(10..12)?.try_into().unwrap());
    let isprimary = *a.get(14)? != 0;
    let dim1 = i32::from_le_bytes(a.get(40..44)?.try_into().unwrap());
    let nkeys = (nkeyatts.max(0) as i32).min(dim1.max(0)) as usize;
    let mut key_attnums = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        let off = 48 + i * 2;
        key_attnums.push(i16::from_le_bytes(a.get(off..off + 2)?.try_into().unwrap()));
    }
    Some(IndexRow { indrelid, isprimary, key_attnums })
}

/// The parsed catalog of one database at one LSN: everything the engine's
/// table discovery/attach/remap flows ask SQL for today.
pub struct Catalog {
    pub db: u32,
    class_rows: Vec<ClassRow>,
    att_rows: Vec<AttRow>,
    index_rows: Vec<IndexRow>,
}

impl Catalog {
    /// Scan pg_class, pg_attribute and pg_index pages from `src`, keeping
    /// only tuple versions `clog` resolves as visible (real CLOG visibility,
    /// v2-scope P2 — see the module doc).
    pub async fn load<S: PageSource, C: ClogSource>(
        src: &mut S,
        clog: &mut C,
        mapped: &MappedRels,
    ) -> Result<Catalog> {
        let db = src.db();
        let class_rows: Vec<ClassRow> = scan(src, clog, mapped.pg_class)
            .await?
            .iter()
            .filter_map(|a| parse_pg_class(a))
            .collect();
        let att_rows: Vec<AttRow> = scan(src, clog, mapped.pg_attribute)
            .await?
            .iter()
            .filter_map(|a| parse_pg_attribute(a))
            .collect();

        // pg_index is not mapped: its filenode comes from its own pg_class row
        // (a relmapper entry, if present, wins — covers any future remapping).
        let index_node = mapped.pg_index.or_else(|| {
            class_rows
                .iter()
                .find(|c| c.oid == PG_INDEX_OID)
                .map(|c| c.filenode)
                .filter(|&n| n != 0)
        });
        let index_rows = match index_node {
            Some(node) => scan(src, clog, node)
                .await?
                .iter()
                .filter_map(|a| parse_pg_index(a))
                .collect(),
            None => {
                tracing::warn!("pg_index filenode unresolved; primary keys unavailable");
                Vec::new()
            }
        };
        Ok(Catalog { db, class_rows, att_rows, index_rows })
    }

    /// Ordinary tables in the `public` namespace — what the engine's
    /// auto-discovery tracks.
    pub fn table_names(&self) -> Vec<&str> {
        self.class_rows
            .iter()
            .filter(|c| c.relkind == b'r' && c.namespace == PUBLIC_NAMESPACE_OID)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Derive the `TableDesc` for `table` — same shape `schema.rs` builds via
    /// SQL, including the primary key from pg_index.
    pub fn desc(&self, table: &str) -> Result<TableDesc> {
        let cls = self
            .class_rows
            .iter()
            .find(|c| c.name == table && c.relkind == b'r')
            .with_context(|| format!("table '{table}' not found in pg_class pages"))?;
        let toast_rel_node = (cls.toastrelid != 0)
            .then(|| {
                self.class_rows
                    .iter()
                    .find(|c| c.oid == cls.toastrelid)
                    .map(|c| c.filenode)
            })
            .flatten();

        let mut atts: Vec<&AttRow> = self
            .att_rows
            .iter()
            .filter(|a| a.attrelid == cls.oid && a.attnum >= 1)
            .collect();
        atts.sort_by_key(|a| a.attnum);
        atts.dedup_by_key(|a| a.attnum); // paranoia; the visible version per attnum should be unique
        if atts.len() != cls.natts as usize {
            bail!(
                "pg_attribute yielded {} atts for '{table}', pg_class.relnatts says {}",
                atts.len(),
                cls.natts
            );
        }

        let (mut cols, mut phys, mut has_fast_defaults) = (Vec::new(), Vec::new(), false);
        for a in &atts {
            if a.isdropped {
                let align = match a.attalign {
                    b'c' => 1,
                    b's' => 2,
                    b'i' => 4,
                    b'd' => 8,
                    x => bail!("attalign '{}'?", x as char),
                };
                phys.push(PhysCol::Dropped { attlen: a.attlen, align });
            } else {
                let col = Col { name: a.name.clone(), ty: PgType::from_oid(a.typid)? };
                phys.push(PhysCol::Live(col.clone()));
                cols.push(col);
                has_fast_defaults |= a.hasmissing;
            }
        }

        // Primary key: column names in key order, exactly what the SQL path
        // reads via pg_index.indisprimary + indkey.
        let mut pk = Vec::new();
        if let Some(idx) = self
            .index_rows
            .iter()
            .find(|i| i.indrelid == cls.oid && i.isprimary)
        {
            for attnum in &idx.key_attnums {
                let att = atts
                    .iter()
                    .find(|a| a.attnum == *attnum && !a.isdropped)
                    .with_context(|| format!("pk attnum {attnum} of '{table}' not a live column"))?;
                pk.push(att.name.clone());
            }
        }

        Ok(TableDesc {
            name: table.to_string(),
            oid: cls.oid,
            db_oid: self.db,
            rel_node: cls.filenode,
            toast_rel_node,
            cols,
            phys,
            has_fast_defaults,
            pk,
        })
    }
}

async fn scan<S: PageSource, C: ClogSource>(src: &mut S, clog: &mut C, filenode: u32) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for page in src.rel_pages(filenode).await? {
        for (xmin, xmax, infomask, attrs) in page_tuples(&page) {
            match tuple_visible(clog, xmin, xmax, infomask).await {
                Ok(true) => out.push(attrs),
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    "visibility check failed for xmin={xmin} xmax={xmax} infomask={infomask:#x}: \
                     {e:#}; treating as not visible"
                ),
            }
        }
    }
    Ok(out)
}

/// Feed every chunk of `toast_node`'s pages into `cache` (under xid 0), so
/// out-of-line values resolve when decoding tuples read from pages at the
/// same LSN. Returns the number of chunks loaded.
pub async fn preload_toast<S: PageSource>(
    src: &mut S,
    toast_node: u32,
    cache: &mut ToastCache,
) -> Result<usize> {
    let mut chunks = 0usize;
    for page in src.rel_pages(toast_node).await? {
        if page.len() != PAGE_SZ {
            continue;
        }
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        for offnum in 1..=(pd_lower.saturating_sub(24) / 4) as u16 {
            if let Ok((valueid, seq, data)) = decode_toast_chunk_from_page(&page, offnum) {
                cache.add_chunk(0, valueid, seq, data);
                chunks += 1;
            }
        }
    }
    Ok(chunks)
}

/// [`PageSource`] over the pagestream API ([`crate::pgwire::ReplConn`] in
/// `pagestream_v3` mode), pinned to one LSN. Pagestream serves only relation
/// blocks — the relmapper blob is not reachable through it, so [`MappedRels`]
/// must come from elsewhere (in-process keyspace reads, a layer file, or out
/// of band).
pub struct PagestreamSource<'a> {
    pub conn: &'a mut crate::pgwire::ReplConn,
    pub spc: u32,
    pub db: u32,
    pub lsn: u64,
}

impl PageSource for PagestreamSource<'_> {
    fn db(&self) -> u32 {
        self.db
    }
    async fn rel_nblocks(&mut self, filenode: u32) -> Result<u32> {
        self.conn.rel_nblocks(self.rel(filenode), self.lsn).await
    }
    async fn get_page(&mut self, filenode: u32, blk: u32) -> Result<Vec<u8>> {
        self.conn.get_page(self.rel(filenode), blk, self.lsn).await
    }
}

impl PagestreamSource<'_> {
    fn rel(&self, filenode: u32) -> crate::pgwire::RelTag {
        crate::pgwire::RelTag { spcnode: self.spc, dbnode: self.db, relnode: filenode, forknum: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relmap_parses_pairs() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&0x59_27_17u32.to_le_bytes());
        blob.extend_from_slice(&2u32.to_le_bytes());
        blob.extend_from_slice(&PG_CLASS_OID.to_le_bytes());
        blob.extend_from_slice(&41000u32.to_le_bytes());
        blob.extend_from_slice(&PG_ATTRIBUTE_OID.to_le_bytes());
        blob.extend_from_slice(&41001u32.to_le_bytes());
        let map = parse_relmap(&blob).unwrap();
        let mapped = MappedRels::from_relmap(&map).unwrap();
        assert_eq!(mapped.pg_class, 41000);
        assert_eq!(mapped.pg_attribute, 41001);
        assert_eq!(mapped.pg_index, None);
    }

    #[test]
    fn pg_index_row_parses_pk() {
        // FormData_pg_index fixed part + int2vector for a 2-column pk (3, 1).
        let mut a = vec![0u8; 52];
        a[0..4].copy_from_slice(&9001u32.to_le_bytes()); // indexrelid
        a[4..8].copy_from_slice(&41000u32.to_le_bytes()); // indrelid
        a[8..10].copy_from_slice(&2i16.to_le_bytes()); // indnatts
        a[10..12].copy_from_slice(&2i16.to_le_bytes()); // indnkeyatts
        a[12] = 1; // indisunique
        a[14] = 1; // indisprimary
        // int2vector at 24: vl_len (4B header form: len<<2), ndim, dataoffset,
        // elemtype (int2 = 21), dim1, lbound1, values.
        let vl = (24 + 2 * 2) << 2;
        a[24..28].copy_from_slice(&(vl as u32).to_le_bytes());
        a[28..32].copy_from_slice(&1i32.to_le_bytes());
        a[36..40].copy_from_slice(&21u32.to_le_bytes());
        a[40..44].copy_from_slice(&2i32.to_le_bytes());
        a[48..50].copy_from_slice(&3i16.to_le_bytes());
        a[50..52].copy_from_slice(&1i16.to_le_bytes());
        let row = parse_pg_index(&a).unwrap();
        assert_eq!(row.indrelid, 41000);
        assert!(row.isprimary);
        assert_eq!(row.key_attnums, vec![3, 1]);
    }
}
