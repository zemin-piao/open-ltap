//! Decoding heap INSERT records into typed rows.
//!
//! A WAL insert record's block-0 data is:
//!   xl_heap_header { t_infomask2: u16, t_infomask: u16, t_hoff: u8 }   (5 bytes)
//!   followed by the tuple *minus* the fixed 23-byte HeapTupleHeader —
//!   i.e. null bitmap (if any) + alignment padding + attribute data.
//!
//! References: postgres src/include/access/htup_details.h, heapam_xlog.h,
//! varatt.h. Little-endian, 64-bit maxalign assumed (matches the dev
//! containers and every platform we target).

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail};

use crate::schema::{PgType, TableDesc};

// heapam_xlog.h: opcode lives in the top bits of xl_info.
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
/// Set alongside the opcode when the operation initializes a fresh page.
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

// transam/xact.h
pub const XLOG_XACT_OPMASK: u8 = 0x70;
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;

/// Extract the subtransaction xid list from a commit or abort record's main
/// data (xactdesc.c ParseCommitRecord/ParseAbortRecord — both start with
/// xact_time, then xinfo if XLOG_XACT_HAS_INFO, then dbinfo, then subxacts;
/// later chunks don't matter here).
pub fn parse_xact_subxacts(info: u8, main_data: &[u8]) -> Result<Vec<u32>> {
    if info & XLOG_XACT_HAS_INFO == 0 {
        return Ok(Vec::new());
    }
    let mut off = 8; // xact_time (TimestampTz)
    let xinfo_bytes = main_data
        .get(off..off + 4)
        .ok_or_else(|| anyhow::anyhow!("xact record too short for xinfo"))?;
    let xinfo = u32::from_le_bytes(xinfo_bytes.try_into().unwrap());
    off += 4;
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        off += 8; // dbId + tsId
    }
    if xinfo & XACT_XINFO_HAS_SUBXACTS == 0 {
        return Ok(Vec::new());
    }
    let n_bytes = main_data
        .get(off..off + 4)
        .ok_or_else(|| anyhow::anyhow!("xact record too short for nsubxacts"))?;
    let n = i32::from_le_bytes(n_bytes.try_into().unwrap()) as usize;
    off += 4;
    let xids_bytes = main_data
        .get(off..off + 4 * n)
        .ok_or_else(|| anyhow::anyhow!("xact record too short for {n} subxacts"))?;
    Ok(xids_bytes.chunks_exact(4).map(|c| u32::from_le_bytes(c.try_into().unwrap())).collect())
}

// heapam_xlog.h: xl_heap_update flags
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

/// XLOG_SMGR_CREATE (storage rmgr): a new relfilenode came into existence.
/// Main data: RelFileLocator { spc u32, db u32, rel u32 } + ForkNumber i32.
/// Returns (db oid, relfilenode) for main-fork creates.
pub const XLOG_SMGR_CREATE: u8 = 0x10;

pub fn parse_smgr_create(main_data: &[u8]) -> Result<Option<(u32, u32)>> {
    if main_data.len() < 16 {
        bail!("smgr create record too short");
    }
    let db = u32::from_le_bytes(main_data[4..8].try_into().unwrap());
    let rel = u32::from_le_bytes(main_data[8..12].try_into().unwrap());
    let fork = i32::from_le_bytes(main_data[12..16].try_into().unwrap());
    Ok(if fork == 0 { Some((db, rel)) } else { None })
}

// htup_details.h
const HEAP_HASNULL: u16 = 0x0001;
const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

/// Days between 1970-01-01 (unix/Delta epoch) and 2000-01-01 (PG epoch).
pub const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds between the same two epochs.
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

pub type Row = Vec<Option<Value>>;

/// Hyphenated text form of a 16-byte uuid.
pub fn format_uuid(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

// ---------------------------------------------------------------------------
// Extracting tuples out of full-page images (full_page_writes=on)
// ---------------------------------------------------------------------------

/// xl_heap_delete: { xmax u32, offnum u16, infobits u8, flags u8 }.
pub fn delete_offnum(main_data: &[u8]) -> Result<u16> {
    let s = main_data.get(4..6).ok_or_else(|| anyhow::anyhow!("delete main data too short"))?;
    Ok(u16::from_le_bytes(s.try_into().unwrap()))
}

/// xl_heap_update: { old_xmax u32, old_offnum u16, old_infobits u8,
/// flags u8, new_xmax u32, new_offnum u16 } (14 bytes).
pub struct UpdateInfo {
    pub old_offnum: u16,
    pub new_offnum: u16,
    pub flags: u8,
}

pub fn parse_update_main(main_data: &[u8]) -> Result<UpdateInfo> {
    if main_data.len() < 14 {
        bail!("update main data too short");
    }
    Ok(UpdateInfo {
        old_offnum: u16::from_le_bytes(main_data[4..6].try_into().unwrap()),
        flags: main_data[7],
        new_offnum: u16::from_le_bytes(main_data[12..14].try_into().unwrap()),
    })
}

/// Offset number targeted by a heap INSERT (xl_heap_insert main data).
pub fn insert_offnum(main_data: &[u8]) -> Result<u16> {
    let s = main_data.get(0..2).ok_or_else(|| anyhow::anyhow!("insert main data too short"))?;
    Ok(u16::from_le_bytes(s.try_into().unwrap()))
}

/// Offset numbers targeted by a multi-insert. With INIT_PAGE the offsets
/// array is elided and the tuples occupy slots 1..=ntuples.
pub fn multi_insert_offsets(main_data: &[u8], info: u8) -> Result<Vec<u16>> {
    if main_data.len() < 4 {
        bail!("multi-insert main data too short");
    }
    let ntuples = u16::from_le_bytes(main_data[2..4].try_into().unwrap());
    if info & XLOG_HEAP_INIT_PAGE != 0 {
        return Ok((1..=ntuples).collect());
    }
    let arr = main_data
        .get(4..4 + 2 * ntuples as usize)
        .ok_or_else(|| anyhow::anyhow!("multi-insert offsets truncated"))?;
    Ok(arr.chunks_exact(2).map(|c| u16::from_le_bytes(c.try_into().unwrap())).collect())
}

/// Locate a tuple on a restored 8KB heap page via its line pointer and hand
/// back the same (payload, masks, hoff) shape the WAL-data path produces.
fn tuple_on_page(page: &[u8], offnum: u16) -> Result<(&[u8], u16, u16, usize)> {
    if offnum == 0 {
        bail!("invalid offset number 0");
    }
    let idx = 24 + (offnum as usize - 1) * 4; // ItemIdData array after the page header
    let s = page.get(idx..idx + 4).ok_or_else(|| anyhow::anyhow!("line pointer {offnum} beyond page"))?;
    let itemid = u32::from_le_bytes(s.try_into().unwrap());
    let lp_off = (itemid & 0x7FFF) as usize;
    let lp_flags = (itemid >> 15) & 0x3;
    let lp_len = (itemid >> 17) as usize;
    if lp_flags != 1 {
        bail!("line pointer {offnum} is not LP_NORMAL (flags {lp_flags})");
    }
    let tuple = page
        .get(lp_off..lp_off + lp_len)
        .ok_or_else(|| anyhow::anyhow!("tuple {offnum} beyond page"))?;
    if tuple.len() < SIZEOF_HEAP_TUPLE_HEADER {
        bail!("tuple {offnum} shorter than its header");
    }
    let t_infomask2 = u16::from_le_bytes(tuple[18..20].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(tuple[20..22].try_into().unwrap());
    let t_hoff = tuple[22] as usize;
    Ok((&tuple[SIZEOF_HEAP_TUPLE_HEADER..], t_infomask2, t_infomask, t_hoff))
}

/// Decode one tuple (by offset number) from a restored page image.
pub fn decode_tuple_from_page(
    page: &[u8],
    offnum: u16,
    desc: &TableDesc,
    toast: &ToastCache,
) -> Result<(Row, Vec<u8>)> {
    let (payload, im2, im, hoff) = tuple_on_page(page, offnum)?;
    decode_tuple_payload(payload, im2, im, hoff, desc, toast, None)
}

/// Decode the tuple carried by a heap INSERT record's block-0 data.
pub fn decode_insert_tuple(
    block_data: &[u8],
    desc: &TableDesc,
    toast: &ToastCache,
) -> Result<(Row, Vec<u8>)> {
    if block_data.len() < 5 {
        bail!("heap insert block data too short");
    }
    let t_infomask2 = u16::from_le_bytes(block_data[0..2].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(block_data[2..4].try_into().unwrap());
    let t_hoff = block_data[4] as usize;
    decode_tuple_payload(&block_data[5..], t_infomask2, t_infomask, t_hoff, desc, toast, None)
}

/// Decode the new tuple of an UPDATE from block-0 data. Layout
/// (log_heap_update): [prefix_len u16 if PREFIX flag][suffix_len u16 if
/// SUFFIX flag] xl_heap_header, null bitmap (+padding), then the attribute
/// data minus the prefix/suffix shared with the old tuple's attribute bytes.
pub fn decode_update_new_tuple(
    block_data: &[u8],
    flags: u8,
    old_attrs: Option<&[u8]>,
    old_row: Option<&Row>,
    desc: &TableDesc,
    toast: &ToastCache,
) -> Result<(Row, Vec<u8>)> {
    let mut off = 0usize;
    let mut prefix = 0usize;
    let mut suffix = 0usize;
    if flags & XLH_UPDATE_PREFIX_FROM_OLD != 0 {
        prefix = u16::from_le_bytes(
            block_data.get(0..2).ok_or_else(|| anyhow::anyhow!("truncated prefix len"))?.try_into().unwrap(),
        ) as usize;
        off += 2;
    }
    if flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0 {
        suffix = u16::from_le_bytes(
            block_data.get(off..off + 2).ok_or_else(|| anyhow::anyhow!("truncated suffix len"))?.try_into().unwrap(),
        ) as usize;
        off += 2;
    }
    let hdr = block_data
        .get(off..off + 5)
        .ok_or_else(|| anyhow::anyhow!("update block data too short"))?;
    let t_infomask2 = u16::from_le_bytes(hdr[0..2].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(hdr[2..4].try_into().unwrap());
    let t_hoff = hdr[4] as usize;
    off += 5;

    let bits_len = t_hoff
        .checked_sub(SIZEOF_HEAP_TUPLE_HEADER)
        .ok_or_else(|| anyhow::anyhow!("t_hoff {t_hoff} < header size"))?;
    let bitmap = block_data
        .get(off..off + bits_len)
        .ok_or_else(|| anyhow::anyhow!("update tuple shorter than its null bitmap"))?;
    let partial = &block_data[off + bits_len..];

    let payload: std::borrow::Cow<[u8]> = if prefix == 0 && suffix == 0 {
        block_data[off..].into()
    } else {
        let old = old_attrs.ok_or_else(|| {
            anyhow::anyhow!("update shares {prefix}+{suffix} bytes with an old tuple we don't have")
        })?;
        if prefix + suffix > old.len() {
            bail!("prefix {prefix} + suffix {suffix} exceed old tuple ({} bytes)", old.len());
        }
        let mut buf = Vec::with_capacity(bits_len + prefix + partial.len() + suffix);
        buf.extend_from_slice(bitmap);
        buf.extend_from_slice(&old[..prefix]);
        buf.extend_from_slice(partial);
        buf.extend_from_slice(&old[old.len() - suffix..]);
        buf.into()
    };
    decode_tuple_payload(&payload, t_infomask2, t_infomask, t_hoff, desc, toast, old_row)
}

/// Re-encode a decoded row into on-page attribute bytes — the pre-image
/// prefix/suffix compression works against. Only possible when every varlena
/// is short enough for the 1-byte-header form (payload <= 126 bytes): longer
/// values may be stored inline-compressed or toasted, whose bytes we cannot
/// reproduce. Returns None when re-encoding would be unfaithful.
pub fn encode_attrs(row: &Row, desc: &TableDesc) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    for (i, col) in desc.cols.iter().enumerate() {
        let Some(v) = row.get(i)?.as_ref() else { continue };
        match (col.ty, v) {
            (PgType::Bool, Value::Bool(b)) => buf.push(*b as u8),
            (PgType::Int2, Value::I16(x)) => pad_put(&mut buf, 2, &x.to_le_bytes()),
            (PgType::Int4, Value::I32(x)) => pad_put(&mut buf, 4, &x.to_le_bytes()),
            (PgType::Int8, Value::I64(x)) => pad_put(&mut buf, 8, &x.to_le_bytes()),
            (PgType::Float4, Value::F32(x)) => pad_put(&mut buf, 4, &x.to_le_bytes()),
            (PgType::Float8, Value::F64(x)) => pad_put(&mut buf, 8, &x.to_le_bytes()),
            (PgType::Date, Value::I32(x)) => pad_put(&mut buf, 4, &(x - PG_EPOCH_DAYS).to_le_bytes()),
            (PgType::Timestamp | PgType::TimestampTz, Value::I64(x)) => {
                pad_put(&mut buf, 8, &(x - PG_EPOCH_MICROS).to_le_bytes())
            }
            (PgType::Uuid, Value::Text(t)) => {
                let hex: String = t.chars().filter(|c| *c != '-').collect();
                if hex.len() != 32 {
                    return None;
                }
                for j in (0..32).step_by(2) {
                    buf.push(u8::from_str_radix(&hex[j..j + 2], 16).ok()?);
                }
            }
            (PgType::Text, Value::Text(t)) => encode_short_varlena(&mut buf, t.as_bytes())?,
            (PgType::Bytea, Value::Bytes(b)) => encode_short_varlena(&mut buf, b)?,
            _ => return None, // type/value mismatch
        }
    }
    Some(buf)
}

fn pad_put(buf: &mut Vec<u8>, align: usize, bytes: &[u8]) {
    buf.resize(align_to(buf.len(), align), 0);
    buf.extend_from_slice(bytes);
}

fn encode_short_varlena(buf: &mut Vec<u8>, payload: &[u8]) -> Option<()> {
    if payload.len() > 126 {
        return None; // could be inline-compressed or toasted on page
    }
    buf.push((((payload.len() + 1) << 1) | 1) as u8);
    buf.extend_from_slice(payload);
    Some(())
}

/// Decode all tuples of a HEAP2 MULTI_INSERT record (COPY, multi-row INSERT).
/// Block-0 data is a sequence of xl_multi_insert_tuple structs, each
/// 2-byte-aligned: { datalen: u16, t_infomask2: u16, t_infomask: u16,
/// t_hoff: u8 } followed by `datalen` bytes of tuple payload. The tuple
/// count lives in the record's main data (xl_heap_multi_insert).
pub fn decode_multi_insert(
    block_data: &[u8],
    main_data: &[u8],
    desc: &TableDesc,
    toast: &ToastCache,
) -> Result<Vec<(Row, Vec<u8>)>> {
    if main_data.len() < 4 {
        bail!("multi-insert main data too short");
    }
    let ntuples = u16::from_le_bytes(main_data[2..4].try_into().unwrap()) as usize;

    let mut rows = Vec::with_capacity(ntuples);
    let mut off = 0usize;
    for i in 0..ntuples {
        off = align_to(off, 2); // SHORTALIGN between tuples (heapam.c)
        let hdr = block_data
            .get(off..off + 7)
            .ok_or_else(|| anyhow::anyhow!("multi-insert truncated at tuple {i}"))?;
        let datalen = u16::from_le_bytes(hdr[0..2].try_into().unwrap()) as usize;
        let t_infomask2 = u16::from_le_bytes(hdr[2..4].try_into().unwrap());
        let t_infomask = u16::from_le_bytes(hdr[4..6].try_into().unwrap());
        let t_hoff = hdr[6] as usize;
        let payload = block_data
            .get(off + 7..off + 7 + datalen)
            .ok_or_else(|| anyhow::anyhow!("multi-insert tuple {i} data truncated"))?;
        rows.push(decode_tuple_payload(payload, t_infomask2, t_infomask, t_hoff, desc, toast, None)?);
        off += 7 + datalen;
    }
    Ok(rows)
}

/// Decode a tuple payload: null bitmap + padding + attribute data — i.e. the
/// on-disk tuple minus its fixed 23-byte HeapTupleHeader.
fn decode_tuple_payload(
    tuple: &[u8],
    t_infomask2: u16,
    t_infomask: u16,
    t_hoff: usize,
    desc: &TableDesc,
    toast: &ToastCache,
    old_row: Option<&Row>,
) -> Result<(Row, Vec<u8>)> {
    let natts = (t_infomask2 & 0x07FF) as usize;
    if natts != desc.cols.len() {
        bail!(
            "tuple has {} attributes but table '{}' has {} (dropped/added columns are M2)",
            natts,
            desc.name,
            desc.cols.len()
        );
    }
    let has_nulls = t_infomask & HEAP_HASNULL != 0;

    // The WAL tuple data starts at on-disk offset 23; attributes start at
    // t_hoff. Everything before that (within the WAL bytes) is the null
    // bitmap + padding.
    let bits_len = t_hoff
        .checked_sub(SIZEOF_HEAP_TUPLE_HEADER)
        .ok_or_else(|| anyhow::anyhow!("t_hoff {} < header size", t_hoff))?;
    if tuple.len() < bits_len {
        bail!("tuple shorter than its null bitmap");
    }
    let bitmap = &tuple[..bits_len];
    let data = &tuple[bits_len..];

    let mut row = Vec::with_capacity(natts);
    let mut off = 0usize; // offset into `data`; alignment-correct because t_hoff is maxaligned
    for (i, col) in desc.cols.iter().enumerate() {
        if has_nulls && bitmap[i / 8] & (1 << (i % 8)) == 0 {
            row.push(None);
            continue;
        }
        let (value, new_off) = decode_value(data, off, col.ty, toast)
            .map_err(|e| anyhow::anyhow!("column '{}': {}", col.name, e))?;
        match value {
            Some(v) => row.push(Some(v)),
            // Unresolved toast pointer: the value is unchanged from the
            // previous version of this row (UPDATE), so take it from there.
            None => match old_row.and_then(|r| r.get(i)).and_then(|v| v.clone()) {
                Some(v) => row.push(Some(v)),
                None => bail!(
                    "column '{}': toast value has no buffered chunks and no previous row version",
                    col.name
                ),
            },
        }
        off = new_off;
    }
    Ok((row, data.to_vec()))
}

fn align_to(off: usize, align: usize) -> usize {
    (off + align - 1) & !(align - 1)
}

fn fixed<const N: usize>(data: &[u8], off: usize, align: usize, what: &str) -> Result<([u8; N], usize)> {
    let off = align_to(off, align);
    let s = data
        .get(off..off + N)
        .ok_or_else(|| anyhow::anyhow!("truncated {what}"))?;
    Ok((s.try_into().unwrap(), off + N))
}

/// `Ok((None, _))` means an unresolved toast pointer (offset still advanced).
fn decode_value(data: &[u8], off: usize, ty: PgType, toast: &ToastCache) -> Result<(Option<Value>, usize)> {
    match ty {
        PgType::Bool => {
            let b = *data.get(off).ok_or_else(|| anyhow::anyhow!("truncated bool"))?;
            Ok((Some(Value::Bool(b != 0)), off + 1))
        }
        PgType::Int2 => {
            let (s, off) = fixed::<2>(data, off, 2, "int2")?;
            Ok((Some(Value::I16(i16::from_le_bytes(s))), off))
        }
        PgType::Int4 => {
            let (s, off) = fixed::<4>(data, off, 4, "int4")?;
            Ok((Some(Value::I32(i32::from_le_bytes(s))), off))
        }
        PgType::Int8 => {
            let (s, off) = fixed::<8>(data, off, 8, "int8")?;
            Ok((Some(Value::I64(i64::from_le_bytes(s))), off))
        }
        PgType::Float4 => {
            let (s, off) = fixed::<4>(data, off, 4, "float4")?;
            Ok((Some(Value::F32(f32::from_le_bytes(s))), off))
        }
        PgType::Float8 => {
            let (s, off) = fixed::<8>(data, off, 8, "float8")?;
            Ok((Some(Value::F64(f64::from_le_bytes(s))), off))
        }
        PgType::Date => {
            let (s, off) = fixed::<4>(data, off, 4, "date")?;
            Ok((Some(Value::I32(i32::from_le_bytes(s) + PG_EPOCH_DAYS)), off))
        }
        PgType::Timestamp | PgType::TimestampTz => {
            let (s, off) = fixed::<8>(data, off, 8, "timestamp")?;
            Ok((Some(Value::I64(i64::from_le_bytes(s) + PG_EPOCH_MICROS)), off))
        }
        PgType::Uuid => {
            // typalign 'c': no padding, 16 raw bytes.
            let s = data
                .get(off..off + 16)
                .ok_or_else(|| anyhow::anyhow!("truncated uuid"))?;
            Ok((Some(Value::Text(format_uuid(s))), off + 16))
        }
        PgType::Text => {
            let (bytes, new_off) = decode_varlena(data, off, toast)?;
            Ok((bytes.map(|b| Value::Text(String::from_utf8_lossy(&b).into_owned())), new_off))
        }
        PgType::Bytea => {
            let (bytes, new_off) = decode_varlena(data, off, toast)?;
            Ok((bytes.map(Value::Bytes), new_off))
        }
    }
}

/// Decode a varlena datum (varatt.h, little-endian):
///  - first byte odd  -> 1-byte header, inline "short" value (len includes hdr)
///  - first byte 0x01 -> external/toast pointer (unsupported here)
///  - first byte 0x00 -> alignment padding before a 4-byte header
///  - first byte even -> already-aligned 4-byte header
fn decode_varlena(data: &[u8], mut off: usize, toast: &ToastCache) -> Result<(Option<Vec<u8>>, usize)> {
    let first = *data.get(off).ok_or_else(|| anyhow::anyhow!("truncated varlena"))?;
    if first == 0x01 {
        // varatt_external pointer: [0x01][vartag][rawsize i32][extinfo u32]
        // [valueid u32][toastrelid u32], unaligned (18 bytes total).
        let tag = *data.get(off + 1).ok_or_else(|| anyhow::anyhow!("truncated toast pointer"))?;
        if tag != 18 {
            bail!("unsupported varatt tag {tag} (only VARTAG_ONDISK=18)");
        }
        let s = data
            .get(off + 2..off + 18)
            .ok_or_else(|| anyhow::anyhow!("truncated varatt_external"))?;
        let rawsize = i32::from_le_bytes(s[0..4].try_into().unwrap());
        let extinfo = u32::from_le_bytes(s[4..8].try_into().unwrap());
        let valueid = u32::from_le_bytes(s[8..12].try_into().unwrap());
        if !toast.contains(valueid) {
            // Chunks not buffered: an UPDATE keeping an old toast value
            // unchanged. Caller substitutes the previous row's value.
            return Ok((None, off + 18));
        }
        return Ok((Some(toast.resolve(valueid, rawsize, extinfo)?), off + 18));
    }
    if first & 0x01 == 1 {
        // 1-byte header: total length in bytes 1..=126, header included.
        let total = (first >> 1) as usize;
        if total == 0 {
            bail!("corrupt short varlena");
        }
        let payload = data
            .get(off + 1..off + total)
            .ok_or_else(|| anyhow::anyhow!("truncated short varlena"))?;
        return Ok((Some(payload.to_vec()), off + total));
    }
    // 4-byte header (possibly after padding).
    off = align_to(off, 4);
    let hdr_bytes =
        data.get(off..off + 4).ok_or_else(|| anyhow::anyhow!("truncated varlena header"))?;
    let hdr = u32::from_le_bytes(hdr_bytes.try_into().unwrap());
    let total = (hdr >> 2) as usize; // includes the 4 header bytes
    if total < 4 {
        bail!("corrupt varlena length {total}");
    }
    if hdr & 0x03 == 0x02 {
        // VARATT_4B_C: inline-compressed. After va_header comes va_tcinfo:
        // raw (uncompressed) size in the low 30 bits, method in the top 2.
        let tcinfo_bytes = data
            .get(off + 4..off + 8)
            .ok_or_else(|| anyhow::anyhow!("truncated compressed varlena"))?;
        let tcinfo = u32::from_le_bytes(tcinfo_bytes.try_into().unwrap());
        let rawsize = (tcinfo & 0x3FFF_FFFF) as usize;
        let method = tcinfo >> 30;
        let payload = data
            .get(off + 8..off + total)
            .ok_or_else(|| anyhow::anyhow!("truncated compressed varlena body"))?;
        let out = match method {
            0 => pglz_decompress(payload, rawsize)?,
            1 => bail!("lz4-compressed value: set default_toast_compression=pglz (lz4 unsupported)"),
            m => bail!("unknown toast compression method {m}"),
        };
        return Ok((Some(out), off + total));
    }
    let payload =
        data.get(off + 4..off + total).ok_or_else(|| anyhow::anyhow!("truncated varlena body"))?;
    Ok((Some(payload.to_vec()), off + total))
}

// ---------------------------------------------------------------------------
// TOAST: out-of-line values
// ---------------------------------------------------------------------------

/// Buffers toast-table chunk inserts until the pointer tuple that references
/// them is decoded. Chunks for a value are always WAL-logged (same xid)
/// before the referencing tuple, so resolution at decode time always hits.
/// Entries are dropped when their owning transaction commits or aborts.
#[derive(Default)]
pub struct ToastCache {
    /// valueid -> (owning xid, chunk_seq -> bytes)
    vals: HashMap<u32, (u32, BTreeMap<i32, Vec<u8>>)>,
}

impl ToastCache {
    pub fn add_chunk(&mut self, xid: u32, valueid: u32, seq: i32, data: Vec<u8>) {
        self.vals.entry(valueid).or_insert_with(|| (xid, BTreeMap::new())).1.insert(seq, data);
    }

    /// Drop all chunks owned by a finished (sub)transaction.
    pub fn gc_xid(&mut self, xid: u32) {
        self.vals.retain(|_, (owner, _)| *owner != xid);
    }

    pub fn len(&self) -> usize {
        self.vals.len()
    }

    pub fn contains(&self, valueid: u32) -> bool {
        self.vals.contains_key(&valueid)
    }

    fn resolve(&self, valueid: u32, rawsize: i32, extinfo: u32) -> Result<Vec<u8>> {
        let (_, chunks) = self
            .vals
            .get(&valueid)
            .ok_or_else(|| anyhow::anyhow!("toast value {valueid} has no buffered chunks"))?;
        let extsize = (extinfo & 0x3FFF_FFFF) as usize;
        let mut buf = Vec::with_capacity(extsize);
        for (i, (seq, chunk)) in chunks.iter().enumerate() {
            if *seq != i as i32 {
                bail!("toast value {valueid}: missing chunk {i} (got {seq})");
            }
            buf.extend_from_slice(chunk);
        }
        if buf.len() != extsize {
            bail!("toast value {valueid}: {} bytes reassembled, expected {extsize}", buf.len());
        }
        let raw_data = rawsize as usize - 4; // va_rawsize includes the 4-byte header
        if buf.len() == raw_data {
            return Ok(buf); // stored uncompressed
        }
        match extinfo >> 30 {
            0 => pglz_decompress(&buf, raw_data),
            1 => bail!("lz4-compressed toast value: set default_toast_compression=pglz"),
            m => bail!("unknown toast compression method {m}"),
        }
    }
}

/// Decode a toast-table row — (chunk_id oid, chunk_seq int4, chunk_data bytea)
/// — from a tuple payload (either WAL block data or a page-image tuple).
pub fn decode_toast_chunk(
    payload: &[u8],
    t_hoff: usize,
) -> Result<(u32, i32, Vec<u8>)> {
    let bits_len = t_hoff
        .checked_sub(SIZEOF_HEAP_TUPLE_HEADER)
        .ok_or_else(|| anyhow::anyhow!("toast tuple t_hoff {t_hoff} < header size"))?;
    let data = payload
        .get(bits_len..)
        .ok_or_else(|| anyhow::anyhow!("toast tuple shorter than its null bitmap"))?;
    let valueid_bytes = data.get(0..4).ok_or_else(|| anyhow::anyhow!("truncated toast chunk_id"))?;
    let seq_bytes = data.get(4..8).ok_or_else(|| anyhow::anyhow!("truncated toast chunk_seq"))?;
    let valueid = u32::from_le_bytes(valueid_bytes.try_into().unwrap());
    let seq = i32::from_le_bytes(seq_bytes.try_into().unwrap());
    // chunk_data is a plain (never external/compressed) varlena.
    let (bytes, _) = decode_varlena(data, 8, &ToastCache::default())?;
    let bytes = bytes.ok_or_else(|| anyhow::anyhow!("toast chunk_data is itself a toast pointer"))?;
    Ok((valueid, seq, bytes))
}

/// Decode a toast chunk from an INSERT record's block-0 data.
pub fn decode_toast_chunk_from_wal(block_data: &[u8]) -> Result<(u32, i32, Vec<u8>)> {
    if block_data.len() < 5 {
        bail!("toast insert block data too short");
    }
    let t_hoff = block_data[4] as usize;
    decode_toast_chunk(&block_data[5..], t_hoff)
}

/// Decode a toast chunk from a restored page image at an offset number.
pub fn decode_toast_chunk_from_page(page: &[u8], offnum: u16) -> Result<(u32, i32, Vec<u8>)> {
    let (payload, _, _, t_hoff) = tuple_on_page(page, offnum)?;
    decode_toast_chunk(payload, t_hoff)
}

/// PGLZ decompression (common/pg_lzcompress.c): a control byte governs the
/// next 8 items; bit set = back-reference (offset 1..4095, length 3..273,
/// may overlap its own output), bit clear = literal byte.
pub fn pglz_decompress(src: &[u8], rawsize: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(rawsize);
    let mut sp = 0usize;
    while sp < src.len() && out.len() < rawsize {
        let ctrl = src[sp];
        sp += 1;
        for bit in 0..8 {
            if sp >= src.len() || out.len() >= rawsize {
                break;
            }
            if ctrl & (1 << bit) != 0 {
                if sp + 1 >= src.len() {
                    bail!("pglz: truncated back-reference");
                }
                let mut len = ((src[sp] & 0x0F) as usize) + 3;
                let off = (((src[sp] & 0xF0) as usize) << 4) | src[sp + 1] as usize;
                sp += 2;
                if len == 18 {
                    if sp >= src.len() {
                        bail!("pglz: truncated extended length");
                    }
                    len += src[sp] as usize;
                    sp += 1;
                }
                if off == 0 || off > out.len() {
                    bail!("pglz: bad back-reference offset {off} at output {}", out.len());
                }
                for _ in 0..len {
                    if out.len() >= rawsize {
                        break; // PG bounds the copy by the destination, not len
                    }
                    let b = out[out.len() - off];
                    out.push(b);
                }
            } else {
                out.push(src[sp]);
                sp += 1;
            }
        }
    }
    if out.len() != rawsize {
        bail!("pglz: expected {rawsize} bytes, produced {}", out.len());
    }
    Ok(out)
}
