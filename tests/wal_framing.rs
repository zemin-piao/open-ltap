//! WAL framing + CRC parity checks (M5 roadmap item): synthesize byte-exact
//! records and page streams per xlogrecord.h and verify `WalReader`
//! reassembles them identically whole vs. chunked (XLogData framing is
//! chunk-size agnostic), that `parse_record`'s CRC32C matches what
//! xloginsert.c would stamp, and that corruption/desync is caught.

mod common;

use common::*;
use open_ltap::wal::{self, WalReader, rmgr};

/// A realistic small heap-INSERT record with a distinguishing row id.
fn insert_record(i: u32) -> Vec<u8> {
    build_record(
        600 + i,
        rmgr::HEAP,
        wal::heap::XLOG_HEAP_INSERT,
        &[BlockSpec {
            id: 0,
            rel: (1663, TEST_DB, TEST_REL),
            blkno: i,
            image: None,
            data: insert_block_data(2, None, &attrs_int_text(i as i32, "framing"), false),
        }],
        &insert_main(1),
    )
}

/// A record whose main data is `n` filler bytes (LONG main-data header when
/// > 255) — used to force records across page boundaries.
fn filler_record(n: usize) -> Vec<u8> {
    build_record(999, rmgr::XACT, 0x00, &[], &vec![0xAB; n])
}

fn feed_chunked(reader: &mut WalReader, start: u64, stream: &[u8], chunk: usize) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < stream.len() {
        let end = (pos + chunk).min(stream.len());
        out.extend(reader.feed(start + pos as u64, &stream[pos..end]).unwrap());
        pos = end;
    }
    out
}

#[test]
fn roundtrip_whole_vs_chunked() {
    // Mix of sizes, including one record big enough to span two pages.
    let recs = vec![insert_record(1), insert_record(2), filler_record(12_000), insert_record(3)];
    let start = 0x0300_0000u64 + 3 * PAGE as u64; // mid-segment: short page headers
    let stream = frame_wal(&recs, start);

    let whole = WalReader::new(start).feed(start, &stream).unwrap();
    let chunked = feed_chunked(&mut WalReader::new(start), start, &stream, 7);
    assert_eq!(whole, chunked, "reassembly must not depend on XLogData chunking");

    assert_eq!(whole.len(), recs.len());
    assert_eq!(whole[0].0, start + 24, "first record right after the short page header");
    for ((_, bytes), orig) in whole.iter().zip(&recs) {
        assert_eq!(bytes, orig, "reassembled record must be byte-identical");
        wal::parse_record(bytes).expect("our CRC computation must match the reader's check");
    }
}

#[test]
fn record_header_split_across_pages() {
    // Size the filler so the next record starts 8 bytes before the page end:
    // only xl_tot_len + 4 header bytes fit — the rest of the 24-byte header
    // continues on the next page (the case xlogreader.c warns about).
    let filler = filler_record(8131); // tot_len 24 + 5 (long main hdr) + 8131 = 8160
    assert_eq!(filler.len(), 8160);
    let rec = insert_record(9);
    let start = 0x0300_0000u64 + 8 * PAGE as u64;
    let stream = frame_wal(&[filler.clone(), rec.clone()], start);

    for chunk in [stream.len(), 5] {
        let got = feed_chunked(&mut WalReader::new(start), start, &stream, chunk);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].0, start + 24 + 8160, "second record starts 8 bytes shy of the page end");
        assert_eq!(got[1].1, rec);
        wal::parse_record(&got[1].1).unwrap();
    }
}

#[test]
fn long_page_header_at_segment_start() {
    let start = 5 * SEG;
    let recs = vec![insert_record(1)];
    let stream = frame_wal(&recs, start);
    let got = WalReader::new(start).feed(start, &stream).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, start + 40, "records start after the 40-byte long header");
    assert_eq!(got[0].1, recs[0]);
}

#[test]
fn join_midstream_skips_partial_record() {
    // Record A spans page 1 -> page 2; joining at page 2 must skip A's tail
    // (via xlp_rem_len) and yield B at the right LSN.
    let a = filler_record(8971); // tot_len 24 + 5 + 8971 = 9000, spans into page 2
    assert_eq!(a.len(), 9000);
    let b = insert_record(4);
    let start = 0x0300_0000u64 + 16 * PAGE as u64;
    let stream = frame_wal(&[a, b.clone()], start);

    let page2 = start + PAGE as u64;
    let got = WalReader::new(page2).feed(page2, &stream[PAGE..]).unwrap();
    assert_eq!(got.len(), 1, "the in-flight record's tail must be skipped");
    // B's LSN counts both page headers: page 1's, plus page 2's sitting in
    // the middle of A.
    assert_eq!(got[0].0, start + 24 + 9000 + 24);
    assert_eq!(got[0].1, b);
}

#[test]
fn crc_mismatch_detected() {
    let mut rec = insert_record(5);
    rec[30] ^= 0x01; // flip a bit in the body
    let err = wal::parse_record(&rec).unwrap_err().to_string();
    assert!(err.contains("CRC"), "expected CRC error, got: {err}");
}

#[test]
fn bad_page_magic_rejected() {
    let start = 0x0300_0000u64 + PAGE as u64;
    let mut stream = frame_wal(&[insert_record(6)], start);
    stream[0] = 0x00; // corrupt xlp_magic
    let err = WalReader::new(start).feed(start, &stream).unwrap_err().to_string();
    assert!(err.contains("magic"), "expected magic error, got: {err}");
}
