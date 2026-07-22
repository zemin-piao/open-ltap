//! Micro-benchmarks for the V2c hot-path functions — the per-page work the
//! pageserver tee (V2b) and GetPage-miss rebuild (V2c) do. Run in release:
//!   cargo run --release --example bench
//! Numbers are single-threaded, one core; the real tee is per-relation-page so
//! it parallelizes across cores.

use std::time::Instant;

use open_ltap::clog::ClogSource;
use open_ltap::fragment::emit_page;
use open_ltap::reconstruct::{PAGE_SIZE, Slot, build_page, pg_checksum_page};
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{Row, ToastCache, Value, decode_tuple_from_page, numeric_from_string};

fn desc() -> TableDesc {
    let cols = vec![
        Col { name: "id".into(), ty: PgType::Int8 },
        Col { name: "amt".into(), ty: PgType::Numeric },
        Col { name: "txt".into(), ty: PgType::Text },
    ];
    TableDesc {
        name: "b".into(),
        oid: 1,
        db_oid: 1,
        rel_node: 1,
        toast_rel_node: None,
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: vec!["id".into()],
    }
}

fn row(i: usize) -> Row {
    // A canonical numeric like "700.42", a bigint id, a short text payload.
    let amt = numeric_from_string(&format!("{}.{:02}", i * 7, i % 100)).unwrap();
    vec![
        Some(Value::I64(i as i64)),
        Some(Value::Text(open_ltap::wal::heap::numeric_to_string(&amt).unwrap())),
        Some(Value::Text(format!("row-{i}"))),
    ]
}

/// Fill a page with as many rows as fit; return the slots and the built page.
fn full_page() -> (Vec<Slot>, Vec<u8>) {
    let d = desc();
    let mut slots: Vec<Slot> = Vec::new();
    for i in 0.. {
        let mut trial = slots.clone();
        trial.push(Slot::live(row(i)));
        if build_page(&d, 0, 0, &trial).is_err() {
            break;
        }
        slots = trial;
    }
    let page = build_page(&d, 0, 0, &slots).unwrap();
    (slots, page)
}

fn bench(name: &str, iters: u64, bytes_each: u64, mut f: impl FnMut()) {
    for _ in 0..iters / 20 + 1 {
        f();
    }
    let t = Instant::now();
    for _ in 0..iters {
        f();
    }
    let el = t.elapsed();
    let ns = el.as_nanos() as f64 / iters as f64;
    let per_s = iters as f64 / el.as_secs_f64();
    let mbps = per_s * bytes_each as f64 / 1e6;
    println!("  {name:<34} {ns:>8.0} ns/op   {per_s:>12.0} op/s   {mbps:>8.1} MB/s");
}

/// Frozen rows are always visible without any CLOG fetch.
struct NoClog;
impl ClogSource for NoClog {
    async fn clog_page(&mut self, _p: u32) -> anyhow::Result<Vec<u8>> {
        Ok(vec![0u8; PAGE_SIZE])
    }
}

fn main() {
    let d = desc();
    let (slots, page) = full_page();
    let ntup = slots.len();
    println!("page: {ntup} rows/8KB (id int8, amt numeric, txt text)\n");

    bench("pg_checksum_page", 200_000, PAGE_SIZE as u64, || {
        std::hint::black_box(pg_checksum_page(std::hint::black_box(&page), 0));
    });

    bench("build_page (rebuild, incl checksum)", 50_000, PAGE_SIZE as u64, || {
        std::hint::black_box(build_page(&d, 0, 0, &slots).unwrap());
    });

    // The byte-exact raw path: no per-value re-encode, just place the datum
    // bytes. This is what real page demotion (P6) uses.
    let raw_slots: Vec<Slot> = (1..=slots.len() as u16)
        .map(|off| Slot::Raw(open_ltap::reconstruct::RawTuple::from_page(&page, off).unwrap()))
        .collect();
    bench("build_page raw (byte-exact rebuild)", 50_000, PAGE_SIZE as u64, || {
        std::hint::black_box(build_page(&d, 0, 0, &raw_slots).unwrap());
    });

    let cache = ToastCache::default();
    bench("decode all tuples on page", 50_000, PAGE_SIZE as u64, || {
        for off in 1..=ntup as u16 {
            std::hint::black_box(decode_tuple_from_page(&page, off, &d, &cache).unwrap());
        }
    });

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    bench("fragment emit_page (visibility+decode)", 50_000, PAGE_SIZE as u64, || {
        let out = rt.block_on(emit_page(&page, 0, &d, &cache, &mut NoClog)).unwrap();
        std::hint::black_box(out);
    });

    println!("\n(per-row throughput = op/s * {ntup} rows)");
}
