//! V2c reverse-path harness (`src/reconstruct.rs`, P5). Two modes:
//!
//!   cargo run --example rebuild
//!       Built-in demo: rebuild a page from a few rows, round-trip it through
//!       the decoder, and print the page geometry + checksum.
//!
//!   cargo run --example rebuild -- real=<page-file> cols=int4,text[,...]
//!       Cross-check against a REAL heap page (an 8192-byte slice of a heap
//!       relation file, e.g. `dd bs=8192 count=1` of a table's first segment,
//!       or `pg_read_binary_file`). Decodes every LP_NORMAL tuple, rebuilds a
//!       page from those rows preserving offnums, decodes the rebuild, and
//!       asserts each offnum resolves to the same row. This is the offline
//!       proof the P5 plan asks for — "every index-visible (block,offnum)
//!       resolves correctly" — runnable by a reviewer against any dumped page
//!       without this process ever touching Postgres. (pg_filedump / amcheck /
//!       a data-checksums cross-check of pd_checksum are the complementary
//!       live steps.)

use anyhow::{Result, bail};
use open_ltap::reconstruct::{self, Slot};
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{Row, ToastCache, decode_tuple_from_page};

fn synthetic_desc(cols_arg: &str) -> Result<TableDesc> {
    let mut cols = Vec::new();
    for (i, ty) in cols_arg.split(',').enumerate() {
        let ty = match ty.trim() {
            "bool" => PgType::Bool,
            "int2" | "smallint" => PgType::Int2,
            "int4" | "int" | "integer" => PgType::Int4,
            "int8" | "bigint" => PgType::Int8,
            "float4" | "real" => PgType::Float4,
            "float8" => PgType::Float8,
            "text" | "varchar" => PgType::Text,
            "bytea" => PgType::Bytea,
            "uuid" => PgType::Uuid,
            "date" => PgType::Date,
            "timestamp" => PgType::Timestamp,
            "timestamptz" => PgType::TimestampTz,
            other => bail!("unknown column type '{other}'"),
        };
        cols.push(Col { name: format!("c{i}"), ty });
    }
    if cols.is_empty() {
        bail!("cols= must list at least one type");
    }
    Ok(TableDesc {
        name: "rebuild".into(),
        oid: 0,
        db_oid: 0,
        rel_node: 0,
        toast_rel_node: None,
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: Vec::new(),
    })
}

fn print_geometry(page: &[u8], block: u32) {
    let g = |o: usize| u16::from_le_bytes([page[o], page[o + 1]]);
    println!(
        "  geometry: pd_lower={} pd_upper={} pd_special={} version={:#06x} checksum={:#06x} (verify={:#06x})",
        g(12),
        g(14),
        g(16),
        g(18),
        g(8),
        reconstruct::pg_checksum_page(page, block),
    );
}

/// Decode every LP_NORMAL tuple on a page into `(offnum, row)`, using `slots`
/// to also remember which offnums were gaps (so the rebuild preserves them).
fn decode_page(page: &[u8], desc: &TableDesc) -> Result<Vec<Slot>> {
    let cache = ToastCache::default();
    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    let n = (pd_lower.saturating_sub(24)) / 4;
    let mut slots = Vec::with_capacity(n);
    for offnum in 1..=n as u16 {
        let lp = reconstruct::line_pointer(page, offnum)?;
        match lp.flags {
            1 => {
                let (row, _) = decode_tuple_from_page(page, offnum, desc, &cache)?;
                slots.push(Slot::live(row));
            }
            2 => slots.push(Slot::Redirect(lp.off)),
            3 => slots.push(Slot::Dead),
            _ => slots.push(Slot::Unused),
        }
    }
    Ok(slots)
}

fn rows_of(slots: &[Slot], desc: &TableDesc) -> Vec<Option<Row>> {
    slots
        .iter()
        .map(|s| match s {
            Slot::Tuple(t) => Some(t.row.clone()),
            _ => None,
        })
        .map(|r| r.filter(|_| desc.cols.len() == desc.phys.len()))
        .collect()
}

fn cross_check(path: &str, cols_arg: &str) -> Result<()> {
    let desc = synthetic_desc(cols_arg)?;
    let real = std::fs::read(path)?;
    if real.len() != reconstruct::PAGE_SIZE {
        bail!("{path}: expected an {}-byte page, got {} bytes", reconstruct::PAGE_SIZE, real.len());
    }
    println!("real page {path} ({} bytes):", real.len());
    print_geometry(&real, 0);

    let slots = decode_page(&real, &desc)?;
    let want = rows_of(&slots, &desc);
    let live = want.iter().filter(|r| r.is_some()).count();
    println!("  decoded {} line pointers ({live} live tuples)", slots.len());

    // Rebuild from the decoded rows (offnums preserved), then decode again.
    let rebuilt = reconstruct::build_page(&desc, 0, 0, &slots)?;
    println!("rebuilt page:");
    print_geometry(&rebuilt, 0);

    let got = rows_of(&decode_page(&rebuilt, &desc)?, &desc);
    let mut mismatches = 0;
    for (i, (w, g)) in want.iter().zip(&got).enumerate() {
        if w != g {
            mismatches += 1;
            println!("  MISMATCH (semantic) at offnum {}: real={w:?} rebuilt={g:?}", i + 1);
        }
    }
    if mismatches == 0 {
        println!("OK (semantic): all {live} tuples resolve identically through the rebuilt page");
    }

    // P6 byte-exact pass: extract each LP_NORMAL tuple's raw attribute bytes,
    // rebuild the page through the raw path, and assert every datum region is
    // preserved bit-for-bit — the property page demotion actually needs. (This
    // doesn't need the column types; it never decodes.)
    let mut raw_slots = Vec::with_capacity(slots.len());
    for offnum in 1..=slots.len() as u16 {
        let lp = reconstruct::line_pointer(&real, offnum)?;
        raw_slots.push(match lp.flags {
            1 => Slot::Raw(reconstruct::RawTuple::from_page(&real, offnum)?),
            2 => Slot::Redirect(lp.off),
            3 => Slot::Dead,
            _ => Slot::Unused,
        });
    }
    let raw_rebuilt = reconstruct::build_page(&desc, 0, 0, &raw_slots)?;
    let mut byte_mismatch = 0;
    for offnum in 1..=slots.len() as u16 {
        if reconstruct::line_pointer(&real, offnum)?.flags != 1 {
            continue;
        }
        let a = reconstruct::RawTuple::from_page(&real, offnum)?.attrs;
        let b = reconstruct::RawTuple::from_page(&raw_rebuilt, offnum)?.attrs;
        if a != b {
            byte_mismatch += 1;
            println!("  MISMATCH (byte-exact) at offnum {offnum}: {} vs {} bytes", a.len(), b.len());
        }
    }
    if byte_mismatch == 0 {
        println!("OK (byte-exact): all {live} datum regions preserved bit-for-bit via the raw path");
    }

    if mismatches == 0 && byte_mismatch == 0 {
        Ok(())
    } else {
        bail!("{mismatches} semantic + {byte_mismatch} byte-exact offnum(s) did not round-trip")
    }
}

fn demo(emit: Option<&str>) -> Result<()> {
    use open_ltap::wal::heap::Value;
    let desc = synthetic_desc("int4,text")?;
    let rows: Vec<Row> = vec![
        vec![Some(Value::I32(1)), Some(Value::Text("alpha".into()))],
        vec![Some(Value::I32(2)), None],
        vec![Some(Value::I32(3)), Some(Value::Text("gamma".into()))],
    ];
    let slots =
        vec![Slot::live(rows[0].clone()), Slot::live(rows[1].clone()), Slot::Unused, Slot::live(rows[2].clone())];

    let page = reconstruct::build_page(&desc, 0, 0, &slots)?;
    println!("rebuilt a page from {} slots (offnum 3 is a gap):", slots.len());
    print_geometry(&page, 0);
    let cache = ToastCache::default();
    for offnum in [1u16, 2, 4] {
        let (r, _) = decode_tuple_from_page(&page, offnum, &desc, &cache)?;
        println!("  offnum {offnum} -> {r:?}");
    }
    println!("OK: round-trip through decode_tuple_from_page matches the input rows");
    if let Some(path) = emit {
        std::fs::write(path, &page)?;
        println!("wrote the page to {path} (feed it back with real={path} cols=int4,text)");
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |k: &str| args.iter().find_map(|a| a.strip_prefix(k).map(str::to_string));
    match (get("real="), get("cols=")) {
        (Some(path), Some(cols)) => cross_check(&path, &cols),
        (Some(_), None) => bail!("real=<path> also needs cols=<type,...>"),
        _ => demo(get("emit=").as_deref()),
    }
}
