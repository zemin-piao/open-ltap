//! Compressed varlena decode through the *real* tuple path — the integration
//! the `compression.rs` codec tests and the `neon_dialect.rs` TOAST test both
//! leave open. Neon's TOAST test resolves only an *uncompressed* out-of-line
//! value, and no test drives an inline-compressed (`VARATT_4B_C`) attribute at
//! all — yet those two branches (`decode_varlena`'s VARATT_4B_C arm and
//! `ToastCache::resolve`'s compressed arm) are exactly what the lz4 support
//! added. Here a real `decode_insert_tuple` decodes a tuple whose text column
//! is inline-compressed, and one whose text column is an out-of-line
//! compressed TOAST pointer, for both pglz (method 0) and lz4 (method 1).

mod common;

use common::*;
use open_ltap::wal::heap::{self, HeapFmt, ToastCache, Value};

fn row(id: i32, txt: &str) -> Vec<Option<Value>> {
    vec![Some(Value::I32(id)), Some(Value::Text(txt.into()))]
}

/// A repetitive, genuinely lz4-compressible payload.
fn payload() -> String {
    "the quick brown fox jumps over the lazy dog. ".repeat(20)
}

/// Build a `VARATT_4B_C` (inline-compressed) varlena: 4-byte length header
/// (low 2 bits = 0b10), then va_tcinfo (raw size in low 30 bits, 2-bit method
/// on top), then the compressed stream. Caller places it 4-aligned.
fn inline_compressed(method: u32, rawsize: usize, stream: &[u8]) -> Vec<u8> {
    let total = 8 + stream.len(); // 4 header + 4 tcinfo + stream
    let hdr = ((total as u32) << 2) | 0x02;
    let tcinfo = (rawsize as u32 & 0x3FFF_FFFF) | (method << 30);
    let mut v = hdr.to_le_bytes().to_vec();
    v.extend_from_slice(&tcinfo.to_le_bytes());
    v.extend_from_slice(stream);
    v
}

/// Compress `text` in the raw form Postgres would store for `method`:
/// pglz (0) uses an all-literal stream (compression isn't implemented, only
/// decode — a literal stream round-trips identically); lz4 (1) uses the real
/// raw block encoder.
fn compress(method: u32, text: &[u8]) -> Vec<u8> {
    match method {
        0 => pglz_literals(text),
        1 => lz4_flex::block::compress(text),
        _ => unreachable!(),
    }
}

fn inline_case(method: u32) {
    let text = payload();
    let stream = compress(method, text.as_bytes());
    let mut attrs = 7i32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&inline_compressed(method, text.len(), &stream));

    let bd = insert_block_data(2, None, &attrs, false);
    let (r, _) =
        heap::decode_insert_tuple(&bd, &desc_int_text(), &ToastCache::default(), HeapFmt::Vanilla)
            .unwrap();
    assert_eq!(r, row(7, &text));
}

#[test]
fn inline_pglz_datum() {
    inline_case(0);
}

#[test]
fn inline_lz4_datum() {
    inline_case(1);
}

fn toast_case(method: u32) {
    let text = payload();
    let stream = compress(method, text.as_bytes());

    // What `toast_save_datum` writes for a compressed value: the 4-byte
    // va_tcinfo header, then the compressed stream — chunked into the toast rel.
    let tcinfo = (text.len() as u32 & 0x3FFF_FFFF) | (method << 30);
    let mut stored = tcinfo.to_le_bytes().to_vec();
    stored.extend_from_slice(&stream);

    let valueid = 424_242u32;
    let mut cache = ToastCache::default();
    for (seq, chunk) in stored.chunks(1996).enumerate() {
        let mut attrs = valueid.to_le_bytes().to_vec();
        attrs.extend_from_slice(&(seq as i32).to_le_bytes());
        attrs.extend_from_slice(&long_varlena(chunk));
        let bd = insert_block_data(3, None, &attrs, false);
        let (vid, s, bytes) = heap::decode_toast_chunk_from_wal(&bd, HeapFmt::Vanilla).unwrap();
        cache.add_chunk(700, vid, s, bytes);
    }

    // The pointer tuple: va_rawsize = uncompressed size + 4-byte header;
    // va_extinfo = stored length in the low 30 bits, method in the top 2.
    let extinfo = (stored.len() as u32) | (method << 30);
    let mut attrs = 9i32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&toast_pointer(text.len() as i32 + 4, extinfo, valueid, TEST_TOAST_REL));
    let bd = insert_block_data(2, None, &attrs, false);
    let (r, _) = heap::decode_insert_tuple(&bd, &desc_int_text(), &cache, HeapFmt::Vanilla).unwrap();
    assert_eq!(r, row(9, &text));
}

#[test]
fn out_of_line_pglz_toast() {
    toast_case(0);
}

#[test]
fn out_of_line_lz4_toast() {
    toast_case(1);
}
