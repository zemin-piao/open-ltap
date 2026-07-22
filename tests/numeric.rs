//! `numeric` / `decimal` decode + encode. Postgres stores numeric as
//! arbitrary-precision base-10000 digits; we carry the exact decimal string
//! (String-backed like uuid). Three wire forms are involved and all covered:
//! the on-disk `NumericChoice` (short + long) that `numeric_to_string` reads
//! off a heap tuple, the `numeric_send` binary-COPY form `numeric_from_binary`
//! reads during snapshot, and `numeric_from_string` which re-encodes to the
//! on-disk long form. Layouts are per utils/adt/numeric.c.

mod common;

use common::*;
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{
    self, HeapFmt, ToastCache, Value, numeric_from_binary, numeric_from_string, numeric_to_string,
};

/// numeric_from_string -> on-disk bytes -> numeric_to_string round-trips the
/// value up to Postgres canonicalization (−0 → 0).
#[test]
fn string_roundtrips_through_on_disk_form() {
    for s in [
        "0",
        "1",
        "-1",
        "123",
        "-123",
        "0.5",
        "-0.05",
        "1234.5",
        "1.50",   // trailing zero kept by dscale
        "100.00", // trailing zero groups + dscale padding
        "0.0",
        "1000000",
        "0.000001",
        "12345678901234567890",     // > one base-10000 group of integer
        "99999999999999.99999",     // mixed, many digits
        "-999.999",
    ] {
        let bytes = numeric_from_string(s).unwrap();
        assert_eq!(numeric_to_string(&bytes).unwrap(), s, "round trip of {s:?}");
    }
}

#[test]
fn zero_is_canonicalized() {
    // -0 and -0.00 normalize to unsigned zero (with the requested scale).
    assert_eq!(numeric_to_string(&numeric_from_string("-0").unwrap()).unwrap(), "0");
    assert_eq!(numeric_to_string(&numeric_from_string("-0.00").unwrap()).unwrap(), "0.00");
}

#[test]
fn specials_round_trip() {
    for s in ["NaN", "Infinity", "-Infinity"] {
        let bytes = numeric_from_string(s).unwrap();
        assert_eq!(numeric_to_string(&bytes).unwrap(), s);
    }
}

/// Ground `numeric_to_string` against hand-built on-disk bytes per numeric.c,
/// so the decode is pinned to the documented layout, not just to our encoder.
#[test]
fn on_disk_long_form_decodes() {
    // 1234.5: NUMERIC_LONG, pos, dscale=1, weight=0, digits [1234, 5000].
    let bytes = [
        0x01, 0x00, // n_sign_dscale = 0x0001 (pos | dscale 1)
        0x00, 0x00, // n_weight = 0
        0xD2, 0x04, // digit 1234 (0x04D2) LE
        0x88, 0x13, // digit 5000 (0x1388) LE
    ];
    assert_eq!(numeric_to_string(&bytes).unwrap(), "1234.5");
}

#[test]
fn on_disk_short_form_decodes() {
    // 5: NUMERIC_SHORT, pos, dscale 0, weight 0, digit [5].
    let bytes = [
        0x00, 0x80, // n_header = 0x8000 (short, pos, dscale 0, weight 0)
        0x05, 0x00, // digit 5
    ];
    assert_eq!(numeric_to_string(&bytes).unwrap(), "5");
}

/// Ground `numeric_from_binary` against the `numeric_send` form (all big-endian:
/// ndigits, weight, sign, dscale, digits).
#[test]
fn binary_copy_form_decodes() {
    let bytes = [
        0x00, 0x02, // ndigits = 2
        0x00, 0x00, // weight = 0
        0x00, 0x00, // sign = NUMERIC_POS
        0x00, 0x01, // dscale = 1
        0x04, 0xD2, // digit 1234 BE
        0x13, 0x88, // digit 5000 BE
    ];
    assert_eq!(numeric_from_binary(&bytes).unwrap(), "1234.5");

    // NaN via the send sign field.
    let nan = [0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00];
    assert_eq!(numeric_from_binary(&nan).unwrap(), "NaN");
}

/// The real decode path: a tuple with a numeric column, through
/// `decode_insert_tuple`.
#[test]
fn numeric_column_decodes_through_a_tuple() {
    let cols = vec![
        Col { name: "id".into(), ty: PgType::Int4 },
        Col { name: "amount".into(), ty: PgType::Numeric },
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

    let n_bytes = numeric_from_string("-12345.6789").unwrap();
    let mut attrs = 42i32.to_le_bytes().to_vec();
    attrs.extend_from_slice(&short_varlena(&n_bytes));
    let bd = insert_block_data(2, None, &attrs, false);

    let (row, _) =
        heap::decode_insert_tuple(&bd, &desc, &ToastCache::default(), HeapFmt::Vanilla).unwrap();
    assert_eq!(row, vec![Some(Value::I32(42)), Some(Value::Text("-12345.6789".into()))]);
}
