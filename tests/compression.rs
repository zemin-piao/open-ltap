//! Compression decode paths: TOAST/inline datums (pglz method 0, lz4 method 1)
//! and WAL full-page-image compression (`wal_compression` = pglz/lz4/zstd).
//!
//! Postgres compresses TOAST datums and lz4 page images with
//! `LZ4_compress_default` (the raw LZ4 *block* format) and zstd page images
//! with `ZSTD_compress` (a standard zstd frame) — the exact formats
//! `lz4_flex::block` and the `zstd` crate produce. So round-tripping a payload
//! through those reference encoders and decoding it with our paths exercises
//! the decoders faithfully without a live Postgres to emit the bytes. pglz has
//! no reference *encoder* here, so its all-literal round-trip is checked via
//! `decompress_datum` dispatch and its back-reference machinery (offsets,
//! overlapping copies, the len==18 escape, bounds checks) via hand-built
//! streams at the bottom of the file.

use open_ltap::wal::PageImage;
use open_ltap::wal::heap::{decompress_datum, lz4_decompress, pglz_decompress};

const BLCKSZ: usize = 8192;

// bimg_info bits (xlogrecord.h) — the same values wal/mod.rs matches on.
const HAS_HOLE: u8 = 0x01;
const COMPRESS_LZ4: u8 = 0x08;
const COMPRESS_ZSTD: u8 = 0x10;

/// Compressible-but-non-trivial bytes: 64-byte runs of a rolling value.
fn patterned(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i / 64) as u8).collect()
}

#[test]
fn lz4_block_roundtrip() {
    let data = patterned(5000);
    let comp = lz4_flex::block::compress(&data);
    assert!(comp.len() < data.len(), "runs should compress");
    assert_eq!(lz4_decompress(&comp, data.len()).unwrap(), data);
}

#[test]
fn lz4_wrong_rawsize_is_rejected() {
    let data = patterned(1000);
    let comp = lz4_flex::block::compress(&data);
    assert!(lz4_decompress(&comp, data.len() - 1).is_err());
}

#[test]
fn decompress_datum_dispatch() {
    // method 1 = lz4
    let data = patterned(2048);
    let comp = lz4_flex::block::compress(&data);
    assert_eq!(decompress_datum(1, &comp, data.len()).unwrap(), data);

    // method 0 = pglz, via a hand-built all-literal stream (a 0x00 control
    // byte clears all 8 flags → the next up-to-8 bytes are literals). We only
    // prove the dispatch reaches pglz_decompress; its guts are pinned elsewhere.
    let literal = b"hello, pglz";
    let mut pglz = Vec::new();
    for chunk in literal.chunks(8) {
        pglz.push(0u8);
        pglz.extend_from_slice(chunk);
    }
    assert_eq!(decompress_datum(0, &pglz, literal.len()).unwrap(), literal);

    // methods 2/3 do not exist for TOAST compression.
    assert!(decompress_datum(2, &comp, data.len()).is_err());
}

/// A full page with a zeroed pd_lower..pd_upper hole, compressed as PG would
/// (hole elided, then the remaining body compressed), must restore byte-exact.
fn fpi_roundtrip(compress: impl Fn(&[u8]) -> Vec<u8>, bimg: u8) {
    let mut page = patterned(BLCKSZ);
    let (hole_off, hole_len) = (2000usize, 1600usize);
    page[hole_off..hole_off + hole_len].fill(0); // the hole is zeros on restore

    let mut body = Vec::with_capacity(BLCKSZ - hole_len);
    body.extend_from_slice(&page[..hole_off]);
    body.extend_from_slice(&page[hole_off + hole_len..]);

    let img = PageImage {
        data: compress(&body),
        hole_offset: hole_off as u16,
        hole_len: hole_len as u16,
        bimg_info: HAS_HOLE | bimg,
    };
    assert_eq!(img.restore().unwrap(), page);
}

#[test]
fn fpi_lz4_with_hole() {
    fpi_roundtrip(|b| lz4_flex::block::compress(b), COMPRESS_LZ4);
}

#[test]
fn fpi_zstd_with_hole() {
    fpi_roundtrip(|b| zstd::bulk::compress(b, 3).unwrap(), COMPRESS_ZSTD);
}

#[test]
fn fpi_zstd_no_hole() {
    // hole_len 0: the compressed body is the whole page, nothing reinserted.
    let page = patterned(BLCKSZ);
    let img = PageImage {
        data: zstd::bulk::compress(&page, 3).unwrap(),
        hole_offset: 0,
        hole_len: 0,
        bimg_info: COMPRESS_ZSTD,
    };
    assert_eq!(img.restore().unwrap(), page);
}

// ---------------------------------------------------------------------------
// pglz back-references — the part of pglz_decompress no other test reaches.
// Everything elsewhere feeds all-literal streams (control byte 0x00); the
// back-reference machinery (offset math, self-overlapping copies, the len==18
// extended-length escape, and the bounds checks) only ran under the live
// gauntlet until now. Streams are hand-built to the common/pg_lzcompress.c
// format: a control byte's bits (LSB first) select literal (0) or a 2-byte
// back-reference (1); a reference encodes len = (b0 & 0x0F) + 3 and
// off = ((b0 & 0xF0) << 4) | b1, and when len hits 18 a third byte extends it.
// ---------------------------------------------------------------------------

#[test]
fn pglz_simple_backreference() {
    // "abc" then copy 3 bytes from offset 3 → "abcabc".
    // ctrl 0x08: items 0..2 literal, item 3 a back-reference.
    let stream = [0x08, b'a', b'b', b'c', /*len-3=0,off hi*/ 0x00, /*off lo=3*/ 0x03];
    assert_eq!(pglz_decompress(&stream, 6).unwrap(), b"abcabc");
}

#[test]
fn pglz_overlapping_backreference() {
    // literal 'a', then a length-5 copy from offset 1 — the source overlaps the
    // destination byte-by-byte (RLE), the trickiest copy in the decoder.
    // ctrl 0x02: item 0 literal, item 1 back-reference. len-3=2 → 0x02; off=1.
    let stream = [0x02, b'a', 0x02, 0x01];
    assert_eq!(pglz_decompress(&stream, 6).unwrap(), b"aaaaaa");
}

#[test]
fn pglz_extended_length_backreference() {
    // literal 'x', then a copy longer than 18 (the escape: low nibble 0x0F makes
    // len==18, then a third byte adds 3 → len 21) from offset 1 → 22 x's.
    let stream = [0x02, b'x', 0x0F, 0x01, 0x03];
    assert_eq!(pglz_decompress(&stream, 22).unwrap(), &[b'x'; 22]);
}

#[test]
fn pglz_backreference_before_any_output_is_rejected() {
    // A reference at output position 0 (off > out.len()) must error, not panic.
    let stream = [0x01, 0x00, 0x05];
    assert!(pglz_decompress(&stream, 4).is_err());
}

#[test]
fn pglz_short_output_is_rejected() {
    // Two literals but a claimed rawsize of 5 → decoder must reject the shortfall.
    let stream = [0x00, b'h', b'i'];
    assert!(pglz_decompress(&stream, 5).is_err());
}
