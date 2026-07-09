//! Shared builders for synthetic WAL: byte-exact XLogRecords (xlogrecord.h),
//! page framing for `WalReader`, heap pages (bufpage.h), and heap record
//! payloads in both the vanilla and Neon WAL dialects.
//!
//! The Neon layouts follow neon_xlog.h as field-verified against
//! neondatabase/postgres@REL_17_STABLE_neon_17_5 during the M5 live
//! validation (2026-07-08): xl_neon_heap_header is 9 bytes with t_cid
//! between the infomasks and t_hoff; xl_neon_heap_update inserts t_cid after
//! flags (new_offnum at byte 16); xl_neon_heap_multi_insert puts t_cid
//! between ntuples and the offsets array (offsets at byte 8);
//! xl_neon_heap_delete appends t_cid at the end (offnum unmoved); the
//! per-tuple multi-insert struct has no t_cid at all.
#![allow(dead_code)]

use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};

pub const PAGE: usize = 8192;
pub const SEG: u64 = 16 * 1024 * 1024;
/// PG17 xlp_magic — one of the majors `WalReader` allowlists.
pub const MAGIC_PG17: u16 = 0xD116;
pub const T_HOFF: u8 = 24; // MAXALIGN(23 + <=1 bitmap byte)
pub const HEAP_HASNULL: u16 = 0x0001;

pub fn maxalign(v: u64) -> u64 {
    (v + 7) & !7
}

// ---------------------------------------------------------------------------
// XLogRecord builder (xlogrecord.h)
// ---------------------------------------------------------------------------

pub struct ImageSpec {
    /// Full 8192-byte page; the hole (pd_lower..pd_upper) is elided from the
    /// carried image exactly as XLogRecordAssemble does.
    pub page: Vec<u8>,
    pub hole_offset: u16,
    pub hole_len: u16,
    pub compress_pglz: bool,
}

pub struct BlockSpec {
    pub id: u8,
    pub rel: (u32, u32, u32), // spc, db, rel
    pub blkno: u32,
    pub image: Option<ImageSpec>,
    pub data: Vec<u8>,
}

/// All-literal pglz stream: control byte 0 = the next 8 bytes are literals.
/// Valid pglz that trivially round-trips (no back-references needed).
pub fn pglz_literals(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len() + src.len() / 8 + 1);
    for chunk in src.chunks(8) {
        out.push(0u8);
        out.extend_from_slice(chunk);
    }
    out
}

/// Assemble a complete XLogRecord: 24-byte header (CRC computed the way
/// xloginsert.c does: body first, then header bytes 0..20), block headers,
/// per-block image/data payloads, main data.
pub fn build_record(xid: u32, rmid: u8, info: u8, blocks: &[BlockSpec], main_data: &[u8]) -> Vec<u8> {
    let mut hdrs = Vec::new();
    let mut payload = Vec::new();
    for b in blocks {
        hdrs.push(b.id);
        let mut fork_flags = 0u8; // fork number 0 = main
        if b.image.is_some() {
            fork_flags |= 0x10; // BKPBLOCK_HAS_IMAGE
        }
        if !b.data.is_empty() {
            fork_flags |= 0x20; // BKPBLOCK_HAS_DATA
        }
        hdrs.push(fork_flags);
        hdrs.extend_from_slice(&(b.data.len() as u16).to_le_bytes());
        if let Some(img) = &b.image {
            assert_eq!(img.page.len(), PAGE);
            let mut raw = Vec::with_capacity(PAGE - img.hole_len as usize);
            raw.extend_from_slice(&img.page[..img.hole_offset as usize]);
            raw.extend_from_slice(&img.page[img.hole_offset as usize + img.hole_len as usize..]);
            let mut bimg_info = 0u8;
            if img.hole_len > 0 {
                bimg_info |= 0x01; // BKPIMAGE_HAS_HOLE
            }
            let bytes = if img.compress_pglz {
                bimg_info |= 0x04; // BKPIMAGE_COMPRESS_PGLZ
                pglz_literals(&raw)
            } else {
                raw
            };
            hdrs.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            hdrs.extend_from_slice(&img.hole_offset.to_le_bytes());
            hdrs.push(bimg_info);
            if bimg_info & 0x04 != 0 && bimg_info & 0x01 != 0 {
                hdrs.extend_from_slice(&img.hole_len.to_le_bytes()); // explicit for compressed
            }
            payload.extend_from_slice(&bytes);
        }
        for v in [b.rel.0, b.rel.1, b.rel.2, b.blkno] {
            hdrs.extend_from_slice(&v.to_le_bytes());
        }
        payload.extend_from_slice(&b.data);
    }
    if main_data.len() > 255 {
        hdrs.push(254); // XLR_BLOCK_ID_DATA_LONG
        hdrs.extend_from_slice(&(main_data.len() as u32).to_le_bytes());
    } else if !main_data.is_empty() {
        hdrs.push(255); // XLR_BLOCK_ID_DATA_SHORT
        hdrs.push(main_data.len() as u8);
    }
    payload.extend_from_slice(main_data);

    let mut body = hdrs;
    body.extend_from_slice(&payload);
    let mut rec = vec![0u8; 24];
    rec[0..4].copy_from_slice(&((24 + body.len()) as u32).to_le_bytes()); // xl_tot_len
    rec[4..8].copy_from_slice(&xid.to_le_bytes()); // xl_xid
    // xl_prev left 0 (the reader doesn't chain-check)
    rec[16] = info;
    rec[17] = rmid;
    let crc = crc32c::crc32c_append(crc32c::crc32c(&body), &rec[0..20]);
    rec[20..24].copy_from_slice(&crc.to_le_bytes());
    rec.extend_from_slice(&body);
    rec
}

// ---------------------------------------------------------------------------
// Page framing (what a walsender's XLogData payloads carry)
// ---------------------------------------------------------------------------

fn push_page_header(out: &mut Vec<u8>, pos: &mut u64, rem_len: u32) {
    let long = *pos % SEG == 0;
    let len = if long { 40 } else { 24 };
    let mut h = vec![0u8; len];
    h[0..2].copy_from_slice(&MAGIC_PG17.to_le_bytes());
    let xlp_info: u16 = if long { 0x0002 } else { 0 }; // XLP_LONG_HEADER
    h[2..4].copy_from_slice(&xlp_info.to_le_bytes());
    h[4..8].copy_from_slice(&1u32.to_le_bytes()); // timeline
    h[8..16].copy_from_slice(&pos.to_le_bytes()); // xlp_pageaddr
    h[16..20].copy_from_slice(&rem_len.to_le_bytes()); // xlp_rem_len
    out.extend_from_slice(&h);
    *pos += len as u64;
}

/// Lay records into 8KB WAL pages from a page-aligned start LSN: page headers
/// at every boundary (long form at segment starts), xlp_rem_len = bytes of
/// the spanning record still to come, maxalign padding between records.
pub fn frame_wal(records: &[Vec<u8>], start_lsn: u64) -> Vec<u8> {
    assert_eq!(start_lsn % PAGE as u64, 0, "start LSN must be page-aligned");
    let mut out = Vec::new();
    let mut pos = start_lsn;
    for rec in records {
        assert_eq!(pos % 8, 0);
        let mut written = 0usize;
        while written < rec.len() {
            if pos % PAGE as u64 == 0 {
                let rem = if written > 0 { rec.len() - written } else { 0 };
                push_page_header(&mut out, &mut pos, rem as u32);
            }
            let page_rem = PAGE - (pos % PAGE as u64) as usize;
            let take = page_rem.min(rec.len() - written);
            out.extend_from_slice(&rec[written..written + take]);
            pos += take as u64;
            written += take;
        }
        let pad = (maxalign(pos) - pos) as usize;
        out.extend(std::iter::repeat(0u8).take(pad));
        pos += pad as u64;
    }
    out
}

// ---------------------------------------------------------------------------
// Heap pages and tuples (bufpage.h, htup_details.h)
// ---------------------------------------------------------------------------

/// On-disk heap tuple: 23-byte HeapTupleHeader (infomasks at 18/20, t_hoff
/// at 22) + payload (bitmap/pad + attribute data).
pub fn disk_tuple(t_infomask2: u16, t_infomask: u16, t_hoff: u8, payload: &[u8]) -> Vec<u8> {
    let mut t = vec![0u8; 23];
    t[18..20].copy_from_slice(&t_infomask2.to_le_bytes());
    t[20..22].copy_from_slice(&t_infomask.to_le_bytes());
    t[22] = t_hoff;
    t.extend_from_slice(payload);
    t
}

/// On-disk tuple for our test tables: t_hoff 24, one bitmap/pad byte.
/// `null_bitmap` = Some(bits) sets HEAP_HASNULL (bit set = attr present).
pub fn page_tuple(natts: u16, null_bitmap: Option<u8>, attrs: &[u8]) -> Vec<u8> {
    let infomask = if null_bitmap.is_some() { HEAP_HASNULL } else { 0 };
    let mut payload = vec![null_bitmap.unwrap_or(0)];
    payload.extend_from_slice(attrs);
    disk_tuple(natts, infomask, T_HOFF, &payload)
}

/// 8KB heap page holding `tuples` at line pointers 1..=n (tuples maxaligned
/// down from the page end, LP_NORMAL). Returns (page, pd_lower, pd_upper) —
/// lower..upper is the hole an FPI elides.
pub fn heap_page(tuples: &[Vec<u8>]) -> (Vec<u8>, u16, u16) {
    let mut page = vec![0u8; PAGE];
    let mut upper = PAGE;
    for (i, t) in tuples.iter().enumerate() {
        upper = (upper - t.len()) & !7;
        page[upper..upper + t.len()].copy_from_slice(t);
        let itemid = upper as u32 | (1 << 15) | ((t.len() as u32) << 17); // off | LP_NORMAL | len
        page[24 + i * 4..24 + i * 4 + 4].copy_from_slice(&itemid.to_le_bytes());
    }
    let lower = (24 + tuples.len() * 4) as u16;
    page[12..14].copy_from_slice(&lower.to_le_bytes());
    page[14..16].copy_from_slice(&(upper as u16).to_le_bytes());
    page[16..18].copy_from_slice(&(PAGE as u16).to_le_bytes()); // pd_special
    page[18..20].copy_from_slice(&((PAGE as u16) | 4).to_le_bytes()); // size | layout version
    (page, lower, upper as u16)
}

// ---------------------------------------------------------------------------
// Attribute encodings (varatt.h)
// ---------------------------------------------------------------------------

pub fn short_varlena(payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= 126);
    let mut v = vec![(((payload.len() + 1) << 1) | 1) as u8];
    v.extend_from_slice(payload);
    v
}

/// Uncompressed 4-byte-header varlena; caller must place it 4-aligned.
pub fn long_varlena(payload: &[u8]) -> Vec<u8> {
    let mut v = ((payload.len() as u32 + 4) << 2).to_le_bytes().to_vec();
    v.extend_from_slice(payload);
    v
}

/// varatt_external toast pointer (VARTAG_ONDISK = 18), 18 bytes unaligned.
pub fn toast_pointer(rawsize: i32, extsize: u32, valueid: u32, toastrel: u32) -> Vec<u8> {
    let mut v = vec![0x01u8, 18];
    v.extend_from_slice(&rawsize.to_le_bytes());
    v.extend_from_slice(&extsize.to_le_bytes());
    v.extend_from_slice(&valueid.to_le_bytes());
    v.extend_from_slice(&toastrel.to_le_bytes());
    v
}

/// Attribute bytes for the (id int4, txt text) test table.
pub fn attrs_int_text(id: i32, txt: &str) -> Vec<u8> {
    let mut a = id.to_le_bytes().to_vec();
    a.extend_from_slice(&short_varlena(txt.as_bytes()));
    a
}

// ---------------------------------------------------------------------------
// WAL heap record payloads, vanilla and Neon
// ---------------------------------------------------------------------------

/// The t_cid value stamped into every Neon struct we synthesize; arbitrary,
/// but non-zero so a wrong-dialect read can't accidentally look right.
pub const NEON_T_CID: u32 = 7;

/// xl_heap_header (5B) or xl_neon_heap_header (9B: t_cid between the
/// infomasks and t_hoff).
pub fn wal_hdr(natts: u16, hasnull: bool, neon: bool) -> Vec<u8> {
    let infomask: u16 = if hasnull { HEAP_HASNULL } else { 0 };
    let mut h = natts.to_le_bytes().to_vec();
    h.extend_from_slice(&infomask.to_le_bytes());
    if neon {
        h.extend_from_slice(&NEON_T_CID.to_le_bytes());
    }
    h.push(T_HOFF);
    h
}

/// INSERT block-0 data: WAL heap header + bitmap/pad byte + attrs.
pub fn insert_block_data(natts: u16, null_bitmap: Option<u8>, attrs: &[u8], neon: bool) -> Vec<u8> {
    let mut d = wal_hdr(natts, null_bitmap.is_some(), neon);
    d.push(null_bitmap.unwrap_or(0));
    d.extend_from_slice(attrs);
    d
}

/// xl_heap_insert main data { offnum u16, flags u8 } (offnum shared by both
/// dialects — Neon's t_cid for INSERT lives in the header struct).
pub fn insert_main(offnum: u16) -> Vec<u8> {
    let mut m = offnum.to_le_bytes().to_vec();
    m.push(0);
    m
}

/// xl_heap_delete { xmax u32, offnum u16, infobits u8, flags u8 };
/// xl_neon_heap_delete appends t_cid at the end (offnum unmoved).
pub fn delete_main(offnum: u16, neon: bool) -> Vec<u8> {
    let mut m = 100u32.to_le_bytes().to_vec();
    m.extend_from_slice(&offnum.to_le_bytes());
    m.push(0);
    m.push(0);
    if neon {
        m.extend_from_slice(&NEON_T_CID.to_le_bytes());
    }
    m
}

/// xl_heap_update (14B, new_offnum at 12) or xl_neon_heap_update (18B,
/// t_cid after flags pushes new_offnum to 16).
pub fn update_main(old_offnum: u16, new_offnum: u16, flags: u8, neon: bool) -> Vec<u8> {
    let mut m = 100u32.to_le_bytes().to_vec(); // old_xmax
    m.extend_from_slice(&old_offnum.to_le_bytes());
    m.push(0); // old_infobits
    m.push(flags);
    if neon {
        m.extend_from_slice(&NEON_T_CID.to_le_bytes());
    }
    m.extend_from_slice(&0u32.to_le_bytes()); // new_xmax
    m.extend_from_slice(&new_offnum.to_le_bytes());
    m
}

/// xl_heap_multi_insert { flags u8, pad, ntuples u16, offsets[] } or
/// xl_neon_heap_multi_insert (t_cid between ntuples and the offsets array).
/// INIT_PAGE records elide the offsets array.
pub fn multi_insert_main(offsets: &[u16], init_page: bool, neon: bool) -> Vec<u8> {
    let mut m = vec![0u8, 0];
    m.extend_from_slice(&(offsets.len() as u16).to_le_bytes());
    if neon {
        m.extend_from_slice(&NEON_T_CID.to_le_bytes());
    }
    if !init_page {
        for o in offsets {
            m.extend_from_slice(&o.to_le_bytes());
        }
    }
    m
}

/// Multi-insert block-0 data: SHORTALIGNed xl_multi_insert_tuple structs
/// { datalen u16, t_infomask2 u16, t_infomask u16, t_hoff u8 } + payload.
/// Identical in both dialects (xl_neon_multi_insert_tuple has no t_cid).
pub fn multi_insert_block_data(natts: u16, tuples: &[(Option<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut d = Vec::new();
    for (bitmap, attrs) in tuples {
        if d.len() % 2 == 1 {
            d.push(0);
        }
        let mut payload = vec![bitmap.unwrap_or(0)];
        payload.extend_from_slice(attrs);
        d.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        d.extend_from_slice(&natts.to_le_bytes());
        let infomask: u16 = if bitmap.is_some() { HEAP_HASNULL } else { 0 };
        d.extend_from_slice(&infomask.to_le_bytes());
        d.push(T_HOFF);
        d.extend_from_slice(&payload);
    }
    d
}

// ---------------------------------------------------------------------------
// Table descriptors
// ---------------------------------------------------------------------------

pub const TEST_DB: u32 = 5;
pub const TEST_REL: u32 = 16384;
pub const TEST_TOAST_REL: u32 = 16387;

pub fn desc_int_text() -> TableDesc {
    let cols = vec![
        Col { name: "id".into(), ty: PgType::Int4 },
        Col { name: "txt".into(), ty: PgType::Text },
    ];
    TableDesc {
        name: "t".into(),
        db_oid: TEST_DB,
        rel_node: TEST_REL,
        toast_rel_node: Some(TEST_TOAST_REL),
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: vec!["id".into()],
    }
}
