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
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;

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
const PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds between the same two epochs.
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

pub type Row = Vec<Option<Value>>;

/// Decode the tuple carried by a heap INSERT record's block-0 data.
pub fn decode_insert_tuple(block_data: &[u8], desc: &TableDesc) -> Result<Row> {
    if block_data.len() < 5 {
        bail!("heap insert block data too short");
    }
    let t_infomask2 = u16::from_le_bytes(block_data[0..2].try_into().unwrap());
    let t_infomask = u16::from_le_bytes(block_data[2..4].try_into().unwrap());
    let t_hoff = block_data[4] as usize;
    decode_tuple_payload(&block_data[5..], t_infomask2, t_infomask, t_hoff, desc)
}

/// Decode all tuples of a HEAP2 MULTI_INSERT record (COPY, multi-row INSERT).
/// Block-0 data is a sequence of xl_multi_insert_tuple structs, each
/// 2-byte-aligned: { datalen: u16, t_infomask2: u16, t_infomask: u16,
/// t_hoff: u8 } followed by `datalen` bytes of tuple payload. The tuple
/// count lives in the record's main data (xl_heap_multi_insert).
pub fn decode_multi_insert(block_data: &[u8], main_data: &[u8], desc: &TableDesc) -> Result<Vec<Row>> {
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
        rows.push(decode_tuple_payload(payload, t_infomask2, t_infomask, t_hoff, desc)?);
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
) -> Result<Row> {
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

fn fixed<const N: usize>(data: &[u8], off: usize, align: usize, what: &str) -> Result<([u8; N], usize)> {
    let off = align_to(off, align);
    let s = data
        .get(off..off + N)
        .ok_or_else(|| anyhow::anyhow!("truncated {what}"))?;
    Ok((s.try_into().unwrap(), off + N))
}

fn decode_value(data: &[u8], off: usize, ty: PgType) -> Result<(Value, usize)> {
    match ty {
        PgType::Bool => {
            let b = *data.get(off).ok_or_else(|| anyhow::anyhow!("truncated bool"))?;
            Ok((Value::Bool(b != 0), off + 1))
        }
        PgType::Int2 => {
            let (s, off) = fixed::<2>(data, off, 2, "int2")?;
            Ok((Value::I16(i16::from_le_bytes(s)), off))
        }
        PgType::Int4 => {
            let (s, off) = fixed::<4>(data, off, 4, "int4")?;
            Ok((Value::I32(i32::from_le_bytes(s)), off))
        }
        PgType::Int8 => {
            let (s, off) = fixed::<8>(data, off, 8, "int8")?;
            Ok((Value::I64(i64::from_le_bytes(s)), off))
        }
        PgType::Float4 => {
            let (s, off) = fixed::<4>(data, off, 4, "float4")?;
            Ok((Value::F32(f32::from_le_bytes(s)), off))
        }
        PgType::Float8 => {
            let (s, off) = fixed::<8>(data, off, 8, "float8")?;
            Ok((Value::F64(f64::from_le_bytes(s)), off))
        }
        PgType::Date => {
            let (s, off) = fixed::<4>(data, off, 4, "date")?;
            Ok((Value::I32(i32::from_le_bytes(s) + PG_EPOCH_DAYS), off))
        }
        PgType::Timestamp | PgType::TimestampTz => {
            let (s, off) = fixed::<8>(data, off, 8, "timestamp")?;
            Ok((Value::I64(i64::from_le_bytes(s) + PG_EPOCH_MICROS), off))
        }
        PgType::Uuid => {
            // typalign 'c': no padding, 16 raw bytes.
            let s = data
                .get(off..off + 16)
                .ok_or_else(|| anyhow::anyhow!("truncated uuid"))?;
            let hex: String = s.iter().map(|b| format!("{b:02x}")).collect();
            let text = format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]
            );
            Ok((Value::Text(text), off + 16))
        }
        PgType::Text => {
            let (bytes, new_off) = decode_varlena(data, off)?;
            Ok((Value::Text(String::from_utf8_lossy(&bytes).into_owned()), new_off))
        }
        PgType::Bytea => {
            let (bytes, new_off) = decode_varlena(data, off)?;
            Ok((Value::Bytes(bytes), new_off))
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
        return Ok((out, off + total));
    }
    let payload =
        data.get(off + 4..off + total).ok_or_else(|| anyhow::anyhow!("truncated varlena body"))?;
    Ok((payload.to_vec(), off + total))
}

/// PGLZ decompression (common/pg_lzcompress.c): a control byte governs the
/// next 8 items; bit set = back-reference (offset 1..4095, length 3..273,
/// may overlap its own output), bit clear = literal byte.
fn pglz_decompress(src: &[u8], rawsize: usize) -> Result<Vec<u8>> {
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
