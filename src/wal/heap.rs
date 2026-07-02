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

use anyhow::{Result, bail};

use crate::schema::{PgType, TableDesc};

// heapam_xlog.h: opcode lives in the top bits of xl_info.
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;

// transam/xact.h
pub const XLOG_XACT_OPMASK: u8 = 0x70;
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_ABORT: u8 = 0x20;

// htup_details.h
const HEAP_HASNULL: u16 = 0x0001;
const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I32(i32),
    I64(i64),
    Text(String),
}

pub type Row = Vec<Option<Value>>;

/// Decode the tuple carried by a heap INSERT record's block-0 data.
pub fn decode_insert_tuple(block_data: &[u8], desc: &TableDesc) -> Result<Row> {
    if block_data.len() < 5 {
        bail!("heap insert block data too short");
    }
    let t_infomask2 = u16::from_le_bytes(block_data[0..2].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(block_data[2..4].try_into().unwrap());
    let t_hoff = block_data[4] as usize;
    let tuple = &block_data[5..];

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
        let (value, new_off) = decode_value(data, off, col.ty)
            .map_err(|e| anyhow::anyhow!("column '{}': {}", col.name, e))?;
        row.push(Some(value));
        off = new_off;
    }
    Ok(row)
}

fn align_to(off: usize, align: usize) -> usize {
    (off + align - 1) & !(align - 1)
}

fn decode_value(data: &[u8], off: usize, ty: PgType) -> Result<(Value, usize)> {
    match ty {
        PgType::Bool => {
            let b = *data.get(off).ok_or_else(|| anyhow::anyhow!("truncated bool"))?;
            Ok((Value::Bool(b != 0), off + 1))
        }
        PgType::Int4 => {
            let off = align_to(off, 4);
            let s = data.get(off..off + 4).ok_or_else(|| anyhow::anyhow!("truncated int4"))?;
            Ok((Value::I32(i32::from_le_bytes(s.try_into().unwrap())), off + 4))
        }
        PgType::Int8 => {
            let off = align_to(off, 8);
            let s = data.get(off..off + 8).ok_or_else(|| anyhow::anyhow!("truncated int8"))?;
            Ok((Value::I64(i64::from_le_bytes(s.try_into().unwrap())), off + 8))
        }
        PgType::Text => {
            let (bytes, new_off) = decode_varlena(data, off)?;
            Ok((Value::Text(String::from_utf8_lossy(&bytes).into_owned()), new_off))
        }
    }
}

/// Decode a varlena datum (varatt.h, little-endian):
///  - first byte odd  -> 1-byte header, inline "short" value (len includes hdr)
///  - first byte 0x01 -> external/toast pointer (unsupported here)
///  - first byte 0x00 -> alignment padding before a 4-byte header
///  - first byte even -> already-aligned 4-byte header
fn decode_varlena(data: &[u8], mut off: usize) -> Result<(Vec<u8>, usize)> {
    let first = *data.get(off).ok_or_else(|| anyhow::anyhow!("truncated varlena"))?;
    if first == 0x01 {
        bail!("TOASTed (out-of-line) value: unsupported until the TOAST milestone");
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
        return Ok((payload.to_vec(), off + total));
    }
    // 4-byte header (possibly after padding).
    off = align_to(off, 4);
    let hdr_bytes =
        data.get(off..off + 4).ok_or_else(|| anyhow::anyhow!("truncated varlena header"))?;
    let hdr = u32::from_le_bytes(hdr_bytes.try_into().unwrap());
    if hdr & 0x03 == 0x02 {
        bail!("inline-compressed value: unsupported until the TOAST milestone");
    }
    let total = (hdr >> 2) as usize; // includes the 4 header bytes
    if total < 4 {
        bail!("corrupt varlena length {total}");
    }
    let payload =
        data.get(off + 4..off + total).ok_or_else(|| anyhow::anyhow!("truncated varlena body"))?;
    Ok((payload.to_vec(), off + total))
}
