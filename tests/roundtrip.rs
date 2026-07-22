//! Randomized round-trip fuzz for the V2c page pair: rows -> page
//! (`reconstruct::build_page`) -> rows (`fragment::emit_page`). Many hand-
//! written cases pin specific layouts; this sweeps a large space of shapes —
//! varied column types and alignments, NULLs in every position, short/long
//! varlenas that force MAXALIGN padding, and interspersed LP_UNUSED / LP_DEAD
//! gaps — asserting that every live row survives the round trip at its original
//! offnum. All rows are frozen, so visibility is trivially "all visible" and
//! the property under test is purely the structural encode/decode inverse.
//!
//! Deterministic (fixed-seed xorshift), so a failure reproduces exactly.

use anyhow::Result;
use open_ltap::clog::ClogSource;
use open_ltap::fragment::{FragmentRow, emit_page};
use open_ltap::reconstruct::{Slot, build_page};
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{Row, ToastCache, Value, numeric_from_string, numeric_to_string};

/// CLOG that never needs to answer: every row we build is frozen (xmin =
/// FrozenTransactionId), which `clog::resolve` special-cases as committed
/// without ever fetching a page.
struct FrozenClog;
impl ClogSource for FrozenClog {
    async fn clog_page(&mut self, _pageno: u32) -> Result<Vec<u8>> {
        Ok(vec![0u8; 8192])
    }
}

struct Rng(u32);
impl Rng {
    fn next(&mut self) -> u32 {
        // xorshift32
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u32) -> u32 {
        self.next() % n
    }
    fn boolean(&mut self) -> bool {
        self.next() & 1 == 0
    }
}

fn desc() -> TableDesc {
    // Mixed widths/alignments + two varlenas, to stress padding and the bitmap.
    let cols = vec![
        Col { name: "a".into(), ty: PgType::Int4 },
        Col { name: "b".into(), ty: PgType::Text },
        Col { name: "c".into(), ty: PgType::Int8 },
        Col { name: "d".into(), ty: PgType::Bool },
        Col { name: "e".into(), ty: PgType::Bytea },
        Col { name: "f".into(), ty: PgType::Numeric },
    ];
    TableDesc {
        name: "fuzz".into(),
        oid: 40000,
        db_oid: 5,
        rel_node: 40000,
        toast_rel_node: None,
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: vec!["a".into()],
    }
}

/// A random row for `desc()`, each column independently NULL or a random value.
fn random_row(rng: &mut Rng) -> Row {
    fn maybe(rng: &mut Rng, v: Value) -> Option<Value> {
        if rng.below(5) == 0 { None } else { Some(v) }
    }
    // Each value is computed before the `maybe` NULL-roll so the two rng
    // borrows stay sequential.
    let a = Value::I32(rng.next() as i32);
    let a = maybe(rng, a);
    let textlen = rng.below(130) as usize; // spans the >126 short-varlena limit
    let b = Value::Text(std::iter::repeat_n('x', textlen).collect());
    let b = maybe(rng, b);
    let c = Value::I64(((rng.next() as i64) << 32) | rng.next() as i64);
    let c = maybe(rng, c);
    let d = Value::Bool(rng.boolean());
    let d = maybe(rng, d);
    let byteslen = rng.below(130) as usize;
    let e = Value::Bytes(vec![0xABu8; byteslen]);
    let e = maybe(rng, e);
    // A numeric carried as its canonical decimal string — round it through the
    // codec so the stored value already equals what decode will yield.
    let f = Value::Text(random_numeric(rng));
    let f = maybe(rng, f);
    vec![a, b, c, d, e, f]
}

/// A random canonical decimal string (sign, 1–6 integer digits, 0–4 fraction
/// digits), normalized through the numeric codec so it survives round trip.
fn random_numeric(rng: &mut Rng) -> String {
    let mut raw = String::new();
    if rng.boolean() {
        raw.push('-');
    }
    for _ in 0..=rng.below(6) {
        raw.push((b'0' + rng.below(10) as u8) as char);
    }
    let fraclen = rng.below(5);
    if fraclen > 0 {
        raw.push('.');
        for _ in 0..fraclen {
            raw.push((b'0' + rng.below(10) as u8) as char);
        }
    }
    numeric_to_string(&numeric_from_string(&raw).unwrap()).unwrap()
}

#[tokio::test]
async fn fuzz_rows_page_rows_round_trip() {
    let desc = desc();
    let block = 12u32;
    let mut rng = Rng(0x9E37_79B9);
    let mut cases = 0u32;

    for _ in 0..4000 {
        let n = 1 + rng.below(28) as usize;
        let mut slots = Vec::with_capacity(n);
        let mut expected: Vec<FragmentRow> = Vec::new();
        for i in 0..n {
            let offnum = (i + 1) as u16;
            match rng.below(4) {
                0 => slots.push(Slot::Unused),
                1 => slots.push(Slot::Dead),
                _ => {
                    let row = random_row(&mut rng);
                    slots.push(Slot::live(row.clone()));
                    expected.push(FragmentRow { block, offnum, row });
                }
            }
        }

        // A row with a >126-byte varlena can't be a short on-page datum, so
        // build_page rejects it (it would need TOAST — P6). Skip those cases;
        // they're covered by the dedicated rejection test.
        let page = match build_page(&desc, block, 0, &slots) {
            Ok(p) => p,
            Err(_) => continue,
        };
        assert_eq!(page.len(), 8192);

        let got = emit_page(&page, block, &desc, &ToastCache::default(), &mut FrozenClog).await.unwrap();
        assert_eq!(got, expected, "round trip mismatch for {n} slots");
        cases += 1;
    }

    // Sanity: the sweep actually exercised a healthy number of pages, not just
    // skips.
    assert!(cases > 1000, "only {cases} cases survived the varlena filter");
}
