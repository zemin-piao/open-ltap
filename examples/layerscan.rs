//! Offline pageserver layer-file reader — the P0-1 probe of docs/v2-scope.md:
//! parse Neon image/delta layer files with no pageserver, no fork, no S3 SDK,
//! and decode heap pages from them with the existing decoder.
//!
//!   cargo run --example layerscan -- <layer-file> [rel=<relnode>] [cols=<ty,ty,..>]
//!   cargo run --example layerscan -- <image-layer> table=<relname> db=<dboid>
//!
//! The `table=` mode is the P0-2 catalog-from-pages spike: it resolves the
//! relmapper blob (key spc/db/0/0/0 — pg_class and pg_attribute are mapped
//! relations whose pg_class.relfilenode reads 0), scans their heap pages by
//! the PG17 FormData layouts, derives the table's TableDesc (columns, types,
//! dropped slots, toast filenode, fast defaults), and then decodes the
//! table's own pages with it — schema and data from the same layer file,
//! zero SQL. Version-picking is a spike heuristic: it keeps catalog tuples
//! with xmax == 0 (a live row's xmax is only nonzero mid-DDL); the real
//! answer is CLOG-at-LSN visibility (v2-scope P2).
//!
//! Layer files live in the pageserver's data dir (host-mounted by
//! neon-compose: neon-compose/pageserver_config/tenants/<t>/timelines/<tl>/)
//! and, identically, under the `neon` MinIO bucket. Image layers are named
//! `<keystart>-<keyend>__<LSN>`, delta layers `<keystart>-<keyend>__<start>-<end>`.
//!
//! With `rel=` and `cols=` (comma-separated: bool,int2,int4,int8,float4,
//! float8,text,bytea,uuid,date,timestamp,timestamptz), every LP_NORMAL tuple
//! on that relation's pages is decoded and printed.
//!
//! Formats per neondatabase/neon @ 8f60b04 (pageserver/src/tenant/
//! {storage_layer/{image,delta}_layer,disk_btree,blob_io}.rs): a bincode
//! (big-endian, fixint) Summary on block 0; blobs from block 1 (1-byte
//! length < 0x80, else 4-byte BE with high bit + 3 compression bits, 0b001 =
//! zstd); a fixed-width B-tree index at `index_start_blk` whose root and
//! child pointers are blocks relative to that; 18-byte keys (rel blocks:
//! 0x00, spc u32, db u32, rel u32, fork u8, blk u32) — delta layers append
//! the LSN (u64 BE) and their leaf values are BlobRef (offset << 1 |
//! will_init) pointing at bincode `Value`s (tag 0 = Image, 1 = WalRecord;
//! WalRecord tag 0 = Postgres { will_init, rec } — a raw WAL record).

use anyhow::{Context, Result, bail};
use open_ltap::catalog::{Catalog, MappedRels, PageSource, parse_relmap, preload_toast};
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{ToastCache, decode_tuple_from_page};
use std::collections::BTreeMap;

const PAGE_SZ: usize = 8192;
const IMAGE_FILE_MAGIC: u16 = 0x5A60;
const DELTA_FILE_MAGIC: u16 = 0x5A61;
const KEY_SIZE: usize = 18;

fn be_u16(b: &[u8]) -> u16 {
    u16::from_be_bytes(b[..2].try_into().unwrap())
}
fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[..4].try_into().unwrap())
}
fn be_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}

/// The bincode(BE, fixint) Summary both layer kinds put on block 0. The only
/// difference: image layers store one LSN where delta layers store a range.
struct Summary {
    magic: u16,
    tenant: [u8; 16],
    timeline: [u8; 16],
    key_range: (Key, Key),
    lsn_range: (u64, u64), // image: (lsn, lsn)
    index_start_blk: u32,
    index_root_blk: u32,
}

fn parse_summary(file: &[u8]) -> Result<Summary> {
    let b = &file[..PAGE_SZ.min(file.len())];
    let magic = be_u16(b);
    let version = be_u16(&b[2..]);
    if magic != IMAGE_FILE_MAGIC && magic != DELTA_FILE_MAGIC {
        bail!("not a layer file (magic {magic:#06x})");
    }
    if version != 3 {
        bail!("storage format version {version}, expected 3");
    }
    let mut off = 4;
    let mut take = |n: usize| {
        let s = &b[off..off + n];
        off += n;
        s
    };
    let tenant: [u8; 16] = take(16).try_into().unwrap();
    let timeline: [u8; 16] = take(16).try_into().unwrap();
    let key_range = (parse_key(take(KEY_SIZE)), parse_key(take(KEY_SIZE)));
    let lsn_range = if magic == IMAGE_FILE_MAGIC {
        let l = be_u64(take(8));
        (l, l)
    } else {
        (be_u64(take(8)), be_u64(take(8)))
    };
    let index_start_blk = be_u32(take(4));
    let index_root_blk = be_u32(take(4));
    Ok(Summary { magic, tenant, timeline, key_range, lsn_range, index_start_blk, index_root_blk })
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    f1: u8,
    spc: u32,
    db: u32,
    rel: u32,
    fork: u8,
    blk: u32,
}

fn parse_key(b: &[u8]) -> Key {
    Key { f1: b[0], spc: be_u32(&b[1..]), db: be_u32(&b[5..]), rel: be_u32(&b[9..]), fork: b[13], blk: be_u32(&b[14..]) }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.f1, self.blk) {
            (0, u32::MAX) => write!(f, "rel {}/{}/{}.{} SIZE", self.spc, self.db, self.rel, self.fork),
            (0, _) => write!(f, "rel {}/{}/{}.{} blk {}", self.spc, self.db, self.rel, self.fork, self.blk),
            _ => write!(
                f,
                "meta {:02x}/{:08x}/{:08x}/{:08x}/{:02x}/{:08x}",
                self.f1, self.spc, self.db, self.rel, self.fork, self.blk
            ),
        }
    }
}

/// Walk the fixed-width disk B-tree, yielding (key bytes, leaf value u64).
fn walk_btree<const L: usize>(
    file: &[u8],
    index_start_blk: u32,
    node_blk: u32,
    out: &mut Vec<([u8; L], u64)>,
) -> Result<()> {
    let off = (index_start_blk + node_blk) as usize * PAGE_SZ;
    let b = file.get(off..off + PAGE_SZ).context("btree node beyond file")?;
    let num = be_u16(b) as usize;
    let level = b[2];
    let (plen, slen) = (b[3] as usize, b[4] as usize);
    if plen + slen != L {
        bail!("btree node key width {} != {L}", plen + slen);
    }
    let prefix = &b[5..5 + plen];
    let keys = &b[5 + plen..5 + plen + num * slen];
    let vals = &b[5 + plen + num * slen..5 + plen + num * slen + num * 5];
    for i in 0..num {
        let v = &vals[i * 5..i * 5 + 5];
        if level > 0 {
            // Inner: value = child block (0x80 marker + u32 BE), key = separator.
            walk_btree::<L>(file, index_start_blk, be_u32(&v[1..]), out)?;
        } else {
            let mut key = [0u8; L];
            key[..plen].copy_from_slice(prefix);
            key[plen..].copy_from_slice(&keys[i * slen..(i + 1) * slen]);
            let val =
                ((v[0] as u64) << 32) | ((v[1] as u64) << 24) | ((v[2] as u64) << 16) | ((v[3] as u64) << 8) | v[4] as u64;
            out.push((key, val));
        }
    }
    Ok(())
}

/// Read a blob at a byte offset: 1-byte length < 0x80, else 4-byte BE with
/// the high bit set and 3 compression bits (0b011 = zstd).
fn read_blob(file: &[u8], off: u64) -> Result<Vec<u8>> {
    let off = off as usize;
    let b0 = *file.get(off).context("blob offset beyond file")?;
    if b0 < 0x80 {
        return Ok(file[off + 1..off + 1 + b0 as usize].to_vec());
    }
    let word = be_u32(&file[off..]);
    let len = (word & 0x0FFF_FFFF) as usize;
    let data = file.get(off + 4..off + 4 + len).context("blob beyond file")?;
    // First byte & 0xf0: 0x80 = uncompressed, 0x90 = zstd (blob_io.rs
    // constants BYTE_UNCOMPRESSED/BYTE_ZSTD — its doc comment says 0b011,
    // but the code says 0b001; trust the code).
    match (word >> 28) & 0x7 {
        0b000 => Ok(data.to_vec()),
        0b001 => zstd::decode_all(data).context("zstd blob"),
        c => bail!("unknown blob compression bits {c:#05b}"),
    }
}

fn desc_from_cols(spec: &str, relnode: u32) -> Result<TableDesc> {
    let mut cols = Vec::new();
    for (i, t) in spec.split(',').enumerate() {
        let ty = match t.trim() {
            "bool" => PgType::Bool,
            "int2" => PgType::Int2,
            "int4" | "int" => PgType::Int4,
            "int8" | "bigint" => PgType::Int8,
            "float4" => PgType::Float4,
            "float8" => PgType::Float8,
            "text" | "varchar" => PgType::Text,
            "bytea" => PgType::Bytea,
            "uuid" => PgType::Uuid,
            "date" => PgType::Date,
            "timestamp" => PgType::Timestamp,
            "timestamptz" => PgType::TimestampTz,
            other => bail!("unknown column type '{other}'"),
        };
        cols.push(Col { name: format!("c{i}"), ty });
    }
    Ok(TableDesc {
        name: "layerscan".into(),
        db_oid: 0,
        rel_node: relnode,
        toast_rel_node: None,
        phys: cols.iter().map(|c| PhysCol::Live(c.clone())).collect(),
        cols,
        has_fast_defaults: false,
        pk: Vec::new(),
    })
}

// ---- P0-2: catalog-from-pages (the derivation lives in open_ltap::catalog;
// this is the image-layer PageSource feeding it) --------------------------

/// A relation-page source over an image layer's (key -> blob) entries.
struct LayerSource<'a> {
    file: &'a [u8],
    entries: &'a [([u8; KEY_SIZE], u64)],
    db: u32,
}

impl PageSource for LayerSource<'_> {
    fn db(&self) -> u32 {
        self.db
    }
    async fn rel_nblocks(&mut self, filenode: u32) -> Result<u32> {
        Ok(self
            .entries
            .iter()
            .map(|(kb, _)| parse_key(kb))
            .filter(|k| k.f1 == 0 && k.db == self.db && k.rel == filenode && k.fork == 0 && k.blk != u32::MAX)
            .map(|k| k.blk + 1)
            .max()
            .unwrap_or(0))
    }
    async fn get_page(&mut self, filenode: u32, blk: u32) -> Result<Vec<u8>> {
        let (_, off) = self
            .entries
            .iter()
            .find(|(kb, _)| {
                let k = parse_key(kb);
                k.f1 == 0 && k.db == self.db && k.rel == filenode && k.fork == 0 && k.blk == blk
            })
            .with_context(|| format!("no page for rel {filenode} blk {blk} in the layer"))?;
        read_blob(self.file, *off)
    }
    /// Cheaper than nblocks+get_page: one pass over the sorted entries.
    async fn rel_pages(&mut self, filenode: u32) -> Result<Vec<Vec<u8>>> {
        let mut pages = Vec::new();
        for (kb, off) in self.entries {
            let k = parse_key(kb);
            if k.f1 == 0 && k.db == self.db && k.rel == filenode && k.fork == 0 && k.blk != u32::MAX {
                pages.push(read_blob(self.file, *off)?);
            }
        }
        Ok(pages)
    }
}

/// The relmapper blob at key (0, spc, db, 0, 0, 0) — pg_class/pg_attribute
/// are mapped relations, unreachable through pg_class.relfilenode.
fn layer_relmap(file: &[u8], entries: &[([u8; KEY_SIZE], u64)], db: u32) -> Result<Vec<u8>> {
    entries
        .iter()
        .find(|(kb, _)| {
            let k = parse_key(kb);
            (k.f1, k.spc, k.db, k.rel, k.fork, k.blk) == (0, 1663, db, 0, 0, 0)
        })
        .map(|(_, off)| read_blob(file, *off))
        .context("no relmapper key for this db in the layer")?
}

fn decode_page(page: &[u8], key: Key, desc: &TableDesc, toast: &ToastCache) {
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    for offnum in 1..=(pd_lower.saturating_sub(24) / 4) as u16 {
        match decode_tuple_from_page(page, offnum, desc, toast) {
            Ok((row, _)) => println!("  ({},{offnum}): {row:?}", key.blk),
            Err(e) if e.to_string().contains("not LP_NORMAL") => {}
            Err(e) => println!("  ({},{offnum}): <{e}>", key.blk),
        }
    }
}

fn fmt_lsn(l: u64) -> String {
    format!("{:X}/{:X}", l >> 32, l & 0xFFFF_FFFF)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut path = None;
    let (mut rel_filter, mut cols_spec) = (None::<u32>, None::<String>);
    let (mut table, mut db) = (None::<String>, None::<u32>);
    for arg in std::env::args().skip(1) {
        if let Some(v) = arg.strip_prefix("rel=") {
            rel_filter = Some(v.parse()?);
        } else if let Some(v) = arg.strip_prefix("cols=") {
            cols_spec = Some(v.to_string());
        } else if let Some(v) = arg.strip_prefix("table=") {
            table = Some(v.to_string());
        } else if let Some(v) = arg.strip_prefix("db=") {
            db = Some(v.parse()?);
        } else {
            path = Some(arg);
        }
    }
    let path = path
        .context("usage: layerscan <layer-file> [rel=<relnode>] [cols=<ty,..>] [table=<name> db=<dboid>]")?;
    let file = std::fs::read(&path)?;
    let s = parse_summary(&file)?;
    let kind = if s.magic == IMAGE_FILE_MAGIC { "image" } else { "delta" };
    println!(
        "{kind} layer: tenant {} timeline {} lsn {}{} keys {}..{}",
        hex(&s.tenant),
        hex(&s.timeline),
        fmt_lsn(s.lsn_range.0),
        if s.magic == DELTA_FILE_MAGIC { format!("-{}", fmt_lsn(s.lsn_range.1)) } else { String::new() },
        s.key_range.0,
        s.key_range.1,
    );

    let desc = match (&cols_spec, rel_filter) {
        (Some(spec), Some(rel)) => Some(desc_from_cols(spec, rel)?),
        (Some(_), None) => bail!("cols= needs rel="),
        _ => None,
    };
    let toast = ToastCache::default();

    if s.magic == IMAGE_FILE_MAGIC {
        let mut entries: Vec<([u8; KEY_SIZE], u64)> = Vec::new();
        walk_btree(&file, s.index_start_blk, s.index_root_blk, &mut entries)?;

        // P0-2 mode: derive the schema from catalog pages, then use it.
        if let Some(table) = &table {
            let db = db.context("table= needs db=<dboid>")?;
            let relmap = parse_relmap(&layer_relmap(&file, &entries, db)?)?;
            let mapped = MappedRels::from_relmap(&relmap)?;
            println!(
                "relmapper: {} mappings, pg_class -> {}, pg_attribute -> {}",
                relmap.len(),
                mapped.pg_class,
                mapped.pg_attribute
            );
            let mut src = LayerSource { file: &file, entries: &entries, db };
            let cat = Catalog::load(&mut src, &mapped).await?;
            let desc = cat.desc(table)?;
            // The toast relation's chunk tuples are pages in this same layer:
            // preload the cache so out-of-line values decode too.
            let mut toast = ToastCache::default();
            if let Some(tnode) = desc.toast_rel_node {
                let chunks = preload_toast(&mut src, tnode, &mut toast).await?;
                println!("preloaded {chunks} toast chunks from rel {tnode}");
            }
            println!(
                "derived from catalog pages: {} rel_node={} toast={:?} fast_defaults={} pk={:?} cols={:?} phys_slots={}",
                desc.name,
                desc.rel_node,
                desc.toast_rel_node,
                desc.has_fast_defaults,
                desc.pk,
                desc.cols.iter().map(|c| format!("{}:{:?}", c.name, c.ty)).collect::<Vec<_>>(),
                desc.phys.len(),
            );
            for (kb, off) in &entries {
                let key = parse_key(kb);
                if key.f1 == 0 && key.db == db && key.rel == desc.rel_node && key.fork == 0 && key.blk != u32::MAX {
                    decode_page(&read_blob(&file, *off)?, key, &desc, &toast);
                }
            }
            return Ok(());
        }

        // Aggregate per relation fork; decode filtered pages.
        let mut rels: BTreeMap<(u8, u32, u32, u32, u8), (u32, u32, usize)> = BTreeMap::new();
        for (kb, off) in &entries {
            let key = parse_key(kb);
            let e = rels
                .entry((key.f1, key.spc, key.db, key.rel, key.fork))
                .or_insert((u32::MAX, 0, 0));
            e.0 = e.0.min(key.blk);
            e.1 = e.1.max(key.blk);
            e.2 += 1;
            if key.f1 == 0 && Some(key.rel) == rel_filter && key.blk != u32::MAX {
                if let Some(desc) = &desc {
                    let page = read_blob(&file, *off)?;
                    if page.len() == PAGE_SZ {
                        decode_page(&page, key, desc, &toast);
                    } else {
                        println!("  blk {}: {} bytes (not a page)", key.blk, page.len());
                    }
                }
            }
        }
        println!("{} keys in {} groups:", entries.len(), rels.len());
        for ((f1, spc, db, rel, fork), (lo, hi, n)) in rels {
            println!(
                "  {} x{n} (blk {lo}..{hi})",
                Key { f1, spc, db, rel, fork, blk: lo }
            );
        }
    } else {
        let mut entries: Vec<([u8; KEY_SIZE + 8], u64)> = Vec::new();
        walk_btree(&file, s.index_start_blk, s.index_root_blk, &mut entries)?;
        println!("{} key@lsn entries:", entries.len());
        let show_all = rel_filter.is_none();
        for (kb, blobref) in &entries {
            let key = parse_key(&kb[..KEY_SIZE]);
            let lsn = be_u64(&kb[KEY_SIZE..]);
            if !(show_all || (key.f1 == 0 && Some(key.rel) == rel_filter)) {
                continue;
            }
            let (pos, will_init) = (blobref >> 1, blobref & 1 != 0);
            let val = read_blob(&file, pos)?;
            // bincode Value: tag 0 = Image(Bytes), 1 = WalRecord(NeonWalRecord).
            let what = match be_u32(&val) {
                0 => {
                    let n = be_u64(&val[4..]);
                    let img = &val[12..12 + n as usize];
                    let decoded = match (&desc, img.len()) {
                        (Some(d), PAGE_SZ) => {
                            decode_page(img, key, d, &toast);
                            " (decoded above)"
                        }
                        _ => "",
                    };
                    format!("image {n}B{decoded}")
                }
                1 => match be_u32(&val[4..]) {
                    // NeonWalRecord tag 0 = Postgres { will_init: bool, rec: Bytes }
                    0 => {
                        let rec = &val[8 + 1 + 8..];
                        format!(
                            "walrec postgres {}B rmid {} info {:#04x}",
                            rec.len(),
                            rec.get(17).copied().unwrap_or(255),
                            rec.get(16).copied().unwrap_or(0),
                        )
                    }
                    t => format!("walrec neon-special tag {t}"),
                },
                t => format!("value tag {t}?"),
            };
            println!("  {key} @ {} init={} {what}", fmt_lsn(lsn), will_init as u8);
        }
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
