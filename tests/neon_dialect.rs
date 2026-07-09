//! Neon-dialect decode coverage (M5 roadmap): the gaps the live neon-compose
//! validation couldn't trigger, exercised against synthetic records built
//! from the neon_xlog.h layouts that WERE field-verified against
//! neondatabase/postgres@REL_17_STABLE_neon_17_5:
//!
//!  - the FPI-restore path (`img.restore()` -> `decode_tuple_from_page`)
//!    under rmgr-NEON records — never fired live because Neon includes block
//!    data alongside images; here we synthesize image-only records
//!  - TOAST chunk decode + pointer resolution under the Neon dialect
//!  - every t_cid offset shift, asserted against its vanilla twin (and, where
//!    it matters, asserted to MISdecode when read with the wrong dialect —
//!    proving the shift is load-bearing)
//!  - the rmgr-134 -> vanilla (rmid, op) normalization the engine applies

mod common;

use common::*;
use open_ltap::wal::heap::{
    self, HeapFmt, ToastCache, Value, XLH_UPDATE_CONTAINS_NEW_TUPLE, XLH_UPDATE_PREFIX_FROM_OLD,
    XLH_UPDATE_SUFFIX_FROM_OLD, XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE,
    XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INSERT, XLOG_HEAP_UPDATE,
};
use open_ltap::wal::{self, rmgr};

fn row(id: i32, txt: &str) -> Vec<Option<Value>> {
    vec![Some(Value::I32(id)), Some(Value::Text(txt.into()))]
}

// ---------------------------------------------------------------------------
// Opcode normalization (rmgr 134 -> vanilla space)
// ---------------------------------------------------------------------------

#[test]
fn normalize_dml_mapping() {
    use heap::normalize_dml;
    let neon = |op: u8| normalize_dml(rmgr::NEON, op);
    assert_eq!(neon(0x00), Some((rmgr::HEAP, XLOG_HEAP_INSERT, HeapFmt::Neon)));
    assert_eq!(neon(0x10), Some((rmgr::HEAP, XLOG_HEAP_DELETE, HeapFmt::Neon)));
    assert_eq!(neon(0x20), Some((rmgr::HEAP, XLOG_HEAP_UPDATE, HeapFmt::Neon)));
    // Neon renumbers HOT_UPDATE to 0x30 (vanilla: 0x40 in heap)
    assert_eq!(neon(0x30), Some((rmgr::HEAP, XLOG_HEAP_HOT_UPDATE, HeapFmt::Neon)));
    assert_eq!(neon(0x50), Some((rmgr::HEAP2, XLOG_HEAP2_MULTI_INSERT, HeapFmt::Neon)));
    // 0x40 is Neon LOCK: no row change
    assert_eq!(neon(0x40), None);
    // INIT_PAGE bit rides outside the opmask
    assert_eq!(
        neon(0x50 | XLOG_HEAP_INIT_PAGE),
        Some((rmgr::HEAP2, XLOG_HEAP2_MULTI_INSERT, HeapFmt::Neon))
    );
    // vanilla rmgrs pass through with the op masked out
    assert_eq!(
        normalize_dml(rmgr::HEAP, XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE),
        Some((rmgr::HEAP, XLOG_HEAP_INSERT, HeapFmt::Vanilla))
    );
    assert_eq!(normalize_dml(rmgr::XACT, 0x00), Some((rmgr::XACT, 0x00, HeapFmt::Vanilla)));
}

// ---------------------------------------------------------------------------
// t_cid offset shifts, one per struct
// ---------------------------------------------------------------------------

#[test]
fn neon_insert_header_shift() {
    let desc = desc_int_text();
    let cache = ToastCache::default();
    let attrs = attrs_int_text(7, "row");
    let vanilla = heap::decode_insert_tuple(
        &insert_block_data(2, None, &attrs, false),
        &desc,
        &cache,
        HeapFmt::Vanilla,
    )
    .unwrap();
    let neon = heap::decode_insert_tuple(
        &insert_block_data(2, None, &attrs, true),
        &desc,
        &cache,
        HeapFmt::Neon,
    )
    .unwrap();
    assert_eq!(vanilla, neon, "both dialects must yield the same row and attr bytes");
    assert_eq!(neon.0, row(7, "row"));
    // Reading the 9-byte Neon header as vanilla misparses t_hoff (it lands on
    // a t_cid byte) — the shift is load-bearing, not cosmetic.
    let misread = heap::decode_insert_tuple(
        &insert_block_data(2, None, &attrs, true),
        &desc,
        &cache,
        HeapFmt::Vanilla,
    );
    assert!(
        misread.is_err() || misread.unwrap().0 != row(7, "row"),
        "wrong-dialect read must not accidentally produce the right row"
    );
}

#[test]
fn neon_insert_with_null() {
    let desc = desc_int_text();
    // bitmap 0b01: id present, txt NULL
    let bd = insert_block_data(2, Some(0b01), &42i32.to_le_bytes(), true);
    let (r, _) =
        heap::decode_insert_tuple(&bd, &desc, &ToastCache::default(), HeapFmt::Neon).unwrap();
    assert_eq!(r, vec![Some(Value::I32(42)), None]);
}

#[test]
fn neon_update_main_shift() {
    for neon in [false, true] {
        let fmt = if neon { HeapFmt::Neon } else { HeapFmt::Vanilla };
        let info = heap::parse_update_main(&update_main(5, 9, 0, neon), fmt).unwrap();
        assert_eq!((info.old_offnum, info.new_offnum), (5, 9));
    }
    // Vanilla read of a Neon update lands new_offnum inside t_cid/new_xmax.
    let info = heap::parse_update_main(&update_main(5, 9, 0, true), HeapFmt::Vanilla).unwrap();
    assert_ne!(info.new_offnum, 9);
}

#[test]
fn neon_delete_offnum_unmoved() {
    // t_cid is appended at the END of xl_neon_heap_delete: offnum stays at 4.
    assert_eq!(heap::delete_offnum(&delete_main(11, true)).unwrap(), 11);
    assert_eq!(heap::delete_offnum(&delete_main(11, false)).unwrap(), 11);
}

#[test]
fn neon_multi_insert_offsets_shift() {
    let main = multi_insert_main(&[4, 5, 6], false, true);
    assert_eq!(heap::multi_insert_offsets(&main, 0x50, HeapFmt::Neon).unwrap(), vec![4, 5, 6]);
    // Vanilla read starts the offsets array inside t_cid.
    assert_ne!(heap::multi_insert_offsets(&main, 0x50, HeapFmt::Vanilla).unwrap(), vec![4, 5, 6]);
    // INIT_PAGE elides the array in both dialects.
    let init = multi_insert_main(&[4, 5, 6], true, true);
    assert_eq!(
        heap::multi_insert_offsets(&init, 0x50 | XLOG_HEAP_INIT_PAGE, HeapFmt::Neon).unwrap(),
        vec![1, 2, 3]
    );
}

#[test]
fn neon_multi_insert_tuples_have_no_tcid() {
    // The per-tuple struct inside a multi-insert is identical across
    // dialects — decode_multi_insert deliberately takes no fmt.
    let desc = desc_int_text();
    let tuples =
        vec![(None, attrs_int_text(1, "a")), (None, attrs_int_text(2, "bb")), (Some(0b01), 3i32.to_le_bytes().to_vec())];
    let bd = multi_insert_block_data(2, &tuples);
    let main = multi_insert_main(&[4, 5, 6], false, true);
    let rows = heap::decode_multi_insert(&bd, &main, &desc, &ToastCache::default()).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, row(1, "a"));
    assert_eq!(rows[1].0, row(2, "bb"));
    assert_eq!(rows[2].0, vec![Some(Value::I32(3)), None]);
}

#[test]
fn neon_update_prefix_suffix_reconstruction() {
    let desc = desc_int_text();
    let cache = ToastCache::default();
    let old_attrs = attrs_int_text(1, "hello");
    let old_row = row(1, "hello");

    // (1,"hello") -> (2,"hello"): the 6-byte varlena is a shared suffix.
    let flags = XLH_UPDATE_CONTAINS_NEW_TUPLE | XLH_UPDATE_SUFFIX_FROM_OLD;
    let mut bd = 6u16.to_le_bytes().to_vec(); // suffix_len
    bd.extend_from_slice(&wal_hdr(2, false, true)); // Neon 9-byte header
    bd.push(0); // bitmap/pad byte
    bd.extend_from_slice(&2i32.to_le_bytes()); // only the new id is logged
    let (r, attrs) =
        heap::decode_update_new_tuple(&bd, flags, Some(&old_attrs), Some(&old_row), &desc, &cache, HeapFmt::Neon)
            .unwrap();
    assert_eq!(r, row(2, "hello"));
    assert_eq!(attrs[4..], old_attrs[4..], "suffix bytes carried over from the old tuple");

    // (1,"hello") -> (1,"world"): the int4 is a shared prefix.
    let flags = XLH_UPDATE_CONTAINS_NEW_TUPLE | XLH_UPDATE_PREFIX_FROM_OLD;
    let mut bd = 4u16.to_le_bytes().to_vec(); // prefix_len
    bd.extend_from_slice(&wal_hdr(2, false, true));
    bd.push(0);
    bd.extend_from_slice(&short_varlena(b"world"));
    let (r, _) =
        heap::decode_update_new_tuple(&bd, flags, Some(&old_attrs), Some(&old_row), &desc, &cache, HeapFmt::Neon)
            .unwrap();
    assert_eq!(r, row(1, "world"));
}

// ---------------------------------------------------------------------------
// TOAST under the Neon dialect
// ---------------------------------------------------------------------------

#[test]
fn neon_toast_chunks_resolve_through_pointer() {
    let desc = desc_int_text();
    let valueid = 55_555u32;
    let data: Vec<u8> = (0..3000).map(|i| b'a' + (i % 23) as u8).collect();

    // Chunk inserts into the toast relation, logged with Neon headers
    // (TOAST_MAX_CHUNK_SIZE-ish split).
    let mut cache = ToastCache::default();
    for (seq, chunk) in data.chunks(1996).enumerate() {
        let mut attrs = valueid.to_le_bytes().to_vec(); // chunk_id oid
        attrs.extend_from_slice(&(seq as i32).to_le_bytes()); // chunk_seq
        attrs.extend_from_slice(&long_varlena(chunk)); // chunk_data, 4-aligned at data offset 8
        let bd = insert_block_data(3, None, &attrs, true);
        let (vid, s, bytes) = heap::decode_toast_chunk_from_wal(&bd, HeapFmt::Neon).unwrap();
        assert_eq!((vid, s), (valueid, seq as i32));
        assert_eq!(bytes, chunk);
        cache.add_chunk(700, vid, s, bytes);
    }

    // The pointer tuple in the main table, also Neon-dialect.
    let mut attrs = 9i32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&toast_pointer(3000 + 4, 3000, valueid, TEST_TOAST_REL));
    let bd = insert_block_data(2, None, &attrs, true);
    let (r, _) = heap::decode_insert_tuple(&bd, &desc, &cache, HeapFmt::Neon).unwrap();
    assert_eq!(r, row(9, std::str::from_utf8(&data).unwrap()));
}

// ---------------------------------------------------------------------------
// FPI-restore path under rmgr-NEON records (image, no block data)
// ---------------------------------------------------------------------------

/// Run the engine's exact FPI fallback on a parsed record: normalize the
/// rmgr, insist there's no block data, restore the image, decode by offnum.
fn decode_fpi_insert(rec_bytes: &[u8]) -> (Vec<Option<Value>>, Vec<u8>) {
    let rec = wal::parse_record(rec_bytes).unwrap();
    let (rmid, op, fmt) = heap::normalize_dml(rec.rmid, rec.info).unwrap();
    assert_eq!((rmid, op, fmt), (rmgr::HEAP, XLOG_HEAP_INSERT, HeapFmt::Neon));
    let block0 = rec.blocks.iter().find(|b| b.id == 0).unwrap();
    assert!(block0.data.is_empty(), "FPI-only record must carry no block data");
    let page = block0.image.as_ref().unwrap().restore().unwrap();
    let offnum = heap::insert_offnum(&rec.main_data).unwrap();
    heap::decode_tuple_from_page(&page, offnum, &desc_int_text(), &ToastCache::default()).unwrap()
}

fn fpi_insert_record(image: ImageSpec, offnum: u16) -> Vec<u8> {
    build_record(
        700,
        rmgr::NEON,
        0x00, // XLOG_NEON_HEAP_INSERT
        &[BlockSpec { id: 0, rel: (1663, TEST_DB, TEST_REL), blkno: 3, image: Some(image), data: vec![] }],
        &insert_main(offnum),
    )
}

#[test]
fn neon_fpi_insert_raw_image_with_hole() {
    let tup = page_tuple(2, None, &attrs_int_text(42, "fpi-neon"));
    let (page, lower, upper) = heap_page(&[tup]);
    let rec = fpi_insert_record(
        ImageSpec { page, hole_offset: lower, hole_len: upper - lower, compress_pglz: false },
        1,
    );
    assert_eq!(decode_fpi_insert(&rec).0, row(42, "fpi-neon"));
}

#[test]
fn neon_fpi_insert_pglz_compressed_image() {
    let tup = page_tuple(2, None, &attrs_int_text(43, "fpi-pglz"));
    let (page, lower, upper) = heap_page(&[tup]);
    let rec = fpi_insert_record(
        ImageSpec { page, hole_offset: lower, hole_len: upper - lower, compress_pglz: true },
        1,
    );
    assert_eq!(decode_fpi_insert(&rec).0, row(43, "fpi-pglz"));
}

#[test]
fn neon_fpi_insert_full_image_no_hole() {
    let tup = page_tuple(2, Some(0b01), &44i32.to_le_bytes());
    let (page, _, _) = heap_page(&[tup]);
    let rec = fpi_insert_record(ImageSpec { page, hole_offset: 0, hole_len: 0, compress_pglz: false }, 1);
    assert_eq!(decode_fpi_insert(&rec).0, vec![Some(Value::I32(44)), None]);
}

#[test]
fn neon_fpi_multi_insert_from_page() {
    let desc = desc_int_text();
    let tuples: Vec<Vec<u8>> =
        (1..=3).map(|i| page_tuple(2, None, &attrs_int_text(i, &format!("m{i}")))).collect();
    let (page, lower, upper) = heap_page(&tuples);

    for init_page in [false, true] {
        let info = if init_page { 0x50 | XLOG_HEAP_INIT_PAGE } else { 0x50 };
        let rec_bytes = build_record(
            701,
            rmgr::NEON,
            info,
            &[BlockSpec {
                id: 0,
                rel: (1663, TEST_DB, TEST_REL),
                blkno: 8,
                image: Some(ImageSpec {
                    page: page.clone(),
                    hole_offset: lower,
                    hole_len: upper - lower,
                    compress_pglz: false,
                }),
                data: vec![],
            }],
            &multi_insert_main(&[1, 2, 3], init_page, true),
        );
        let rec = wal::parse_record(&rec_bytes).unwrap();
        let (rmid, op, fmt) = heap::normalize_dml(rec.rmid, rec.info).unwrap();
        assert_eq!((rmid, op, fmt), (rmgr::HEAP2, XLOG_HEAP2_MULTI_INSERT, HeapFmt::Neon));
        let block0 = &rec.blocks[0];
        assert!(block0.data.is_empty());
        let restored = block0.image.as_ref().unwrap().restore().unwrap();
        let offsets = heap::multi_insert_offsets(&rec.main_data, rec.info, fmt).unwrap();
        assert_eq!(offsets, vec![1, 2, 3]);
        for (i, off) in offsets.iter().enumerate() {
            let (r, _) =
                heap::decode_tuple_from_page(&restored, *off, &desc, &ToastCache::default()).unwrap();
            assert_eq!(r, row(i as i32 + 1, &format!("m{}", i + 1)));
        }
    }
}

#[test]
fn neon_fpi_toast_chunk_from_page() {
    let chunk: Vec<u8> = (0..500).map(|i| (i % 251) as u8).collect();
    let mut attrs = 77_777u32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&0i32.to_le_bytes());
    attrs.extend_from_slice(&long_varlena(&chunk));
    let tup = page_tuple(3, None, &attrs);
    let (page, _, _) = heap_page(&[tup]);
    let (vid, seq, bytes) = heap::decode_toast_chunk_from_page(&page, 1).unwrap();
    assert_eq!((vid, seq), (77_777, 0));
    assert_eq!(bytes, chunk);
}

/// End-to-end: Neon FPI record framed into WAL pages, streamed through
/// `WalReader` in small chunks, parsed (CRC-checked), normalized, decoded.
#[test]
fn neon_fpi_end_to_end_through_reader() {
    use open_ltap::wal::WalReader;
    let tup = page_tuple(2, None, &attrs_int_text(99, "end-to-end"));
    let (page, lower, upper) = heap_page(&[tup]);
    let fpi = fpi_insert_record(
        ImageSpec { page, hole_offset: lower, hole_len: upper - lower, compress_pglz: false },
        1,
    );
    let plain = build_record(
        702,
        rmgr::NEON,
        0x00,
        &[BlockSpec {
            id: 0,
            rel: (1663, TEST_DB, TEST_REL),
            blkno: 3,
            image: None,
            data: insert_block_data(2, None, &attrs_int_text(100, "wal-data"), true),
        }],
        &insert_main(2),
    );

    let start = 0x0300_0000u64 + 32 * PAGE as u64;
    let stream = frame_wal(&[fpi.clone(), plain.clone()], start);
    let mut reader = WalReader::new(start);
    let mut recs = Vec::new();
    let mut pos = 0usize;
    while pos < stream.len() {
        let end = (pos + 113).min(stream.len());
        recs.extend(reader.feed(start + pos as u64, &stream[pos..end]).unwrap());
        pos = end;
    }
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].1, fpi);
    assert_eq!(decode_fpi_insert(&recs[0].1).0, row(99, "end-to-end"));

    let rec = wal::parse_record(&recs[1].1).unwrap();
    let (_, op, fmt) = heap::normalize_dml(rec.rmid, rec.info).unwrap();
    assert_eq!((op, fmt), (XLOG_HEAP_INSERT, HeapFmt::Neon));
    let (r, _) = heap::decode_insert_tuple(
        &rec.blocks[0].data,
        &desc_int_text(),
        &ToastCache::default(),
        fmt,
    )
    .unwrap();
    assert_eq!(r, row(100, "wal-data"));
}
