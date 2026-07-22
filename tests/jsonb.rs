//! jsonb decode (JsonbContainer/JEntry binary format -> canonical JSON text).
//! The decoder was live-verified byte-exact against a real PG16 `jsonb` column
//! (objects, arrays, nesting, scalars, escaping, UTF-8, numerics-in-jsonb, key
//! reordering — all matched `jsonb::text`). These offline cases pin the format
//! for CI (which has no Postgres): hand-built container bytes per jsonb.h, plus
//! a full jsonb column decoded through `decode_insert_tuple`.

mod common;

use common::*;
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{self, HeapFmt, ToastCache, Value, jsonb_to_string, numeric_from_string};

/// A numeric as a full varlena (short 1-byte header + NumericChoice), the shape
/// jsonb stores a number as.
fn numeric_varlena(s: &str) -> Vec<u8> {
    let payload = numeric_from_string(s).unwrap();
    let total = payload.len() + 1;
    let mut v = vec![(((total) << 1) | 1) as u8]; // short varlena header
    v.extend_from_slice(&payload);
    v
}

fn u32le(x: u32) -> [u8; 4] {
    x.to_le_bytes()
}

const JB_FOBJECT: u32 = 0x2000_0000;
const JB_FSCALAR: u32 = 0x1000_0000;
const JB_FARRAY: u32 = 0x4000_0000;
const ISSTRING: u32 = 0x0000_0000;
const ISNUMERIC: u32 = 0x1000_0000;

#[test]
fn scalar_number() {
    // A top-level scalar is a 1-element FSCALAR array holding numeric 42.
    let num = numeric_varlena("42"); // 7 bytes, starts 4-aligned at data_start=8
    let mut c = Vec::new();
    c.extend_from_slice(&u32le(JB_FARRAY | JB_FSCALAR | 1)); // header
    c.extend_from_slice(&u32le(ISNUMERIC | num.len() as u32)); // 1 JEntry, length = 7
    c.extend_from_slice(&num);
    assert_eq!(jsonb_to_string(&c).unwrap(), "42");
}

#[test]
fn small_object_with_padding() {
    // {"a": 1} — the numeric value needs 3 bytes of alignment padding after the
    // 1-byte key, exercising the pad/offset logic.
    let key = b"a";
    let num = numeric_varlena("1");
    let pad = 3; // data_start(12) + key(1) = 13 -> pad to 16
    let val_len = pad + num.len();
    let mut c = Vec::new();
    c.extend_from_slice(&u32le(JB_FOBJECT | 1)); // object, 1 pair -> 2 JEntries
    c.extend_from_slice(&u32le(ISSTRING | key.len() as u32));
    c.extend_from_slice(&u32le(ISNUMERIC | val_len as u32));
    c.extend_from_slice(key);
    c.extend_from_slice(&vec![0u8; pad]);
    c.extend_from_slice(&num);
    assert_eq!(jsonb_to_string(&c).unwrap(), r#"{"a": 1}"#);
}

#[test]
fn array_of_scalars() {
    // [true, "x", null] — bools/null are zero-length, string is inline.
    const ISBOOL_TRUE: u32 = 0x3000_0000;
    const ISNULL: u32 = 0x4000_0000;
    let mut c = Vec::new();
    c.extend_from_slice(&u32le(JB_FARRAY | 3));
    c.extend_from_slice(&u32le(ISBOOL_TRUE)); // len 0
    c.extend_from_slice(&u32le(ISSTRING | 1)); // "x"
    c.extend_from_slice(&u32le(ISNULL)); // len 0
    c.push(b'x');
    assert_eq!(jsonb_to_string(&c).unwrap(), r#"[true, "x", null]"#);
}

#[test]
fn string_escaping() {
    // A top-level scalar string with a quote and a newline.
    let s = "a\"b\nc";
    let mut c = Vec::new();
    c.extend_from_slice(&u32le(JB_FARRAY | JB_FSCALAR | 1));
    c.extend_from_slice(&u32le(ISSTRING | s.len() as u32));
    c.extend_from_slice(s.as_bytes());
    assert_eq!(jsonb_to_string(&c).unwrap(), r#""a\"b\nc""#);
}

#[test]
fn jsonb_column_decodes_through_a_tuple() {
    let cols = vec![
        Col { name: "id".into(), ty: PgType::Int4 },
        Col { name: "doc".into(), ty: PgType::Jsonb },
    ];
    let desc = TableDesc {
        name: "t".into(),
        oid: 40000,
        db_oid: 5,
        rel_node: 40000,
        toast_rel_node: None,
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: vec!["id".into()],
    };

    // {"a": 1} as the jsonb container, wrapped in a short varlena in the tuple.
    let num = numeric_varlena("1");
    let mut container = Vec::new();
    container.extend_from_slice(&u32le(JB_FOBJECT | 1));
    container.extend_from_slice(&u32le(ISSTRING | 1));
    container.extend_from_slice(&u32le(ISNUMERIC | (3 + num.len()) as u32));
    container.push(b'a');
    container.extend_from_slice(&[0u8; 3]);
    container.extend_from_slice(&num);

    let mut attrs = 7i32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&short_varlena(&container));
    let bd = insert_block_data(2, None, &attrs, false);

    let (row, _) =
        heap::decode_insert_tuple(&bd, &desc, &ToastCache::default(), HeapFmt::Vanilla).unwrap();
    assert_eq!(row, vec![Some(Value::I32(7)), Some(Value::Text(r#"{"a": 1}"#.into()))]);
}
