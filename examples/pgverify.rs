//! Live-verification harness: decode a REAL Postgres heap page and
//! (1) check the on-disk data-checksum against `reconstruct::pg_checksum_page`,
//! (2) print each tuple's decoded columns so they can be diffed against
//! Postgres's own output. Usage: `pgverify <page-file> <block> <type,type,...>`.

use anyhow::{Result, bail};
use open_ltap::reconstruct::{self, line_pointer};
use open_ltap::schema::{Col, PgType, PhysCol, TableDesc};
use open_ltap::wal::heap::{ToastCache, Value, decode_tuple_from_page};

fn pgtype(s: &str) -> Result<PgType> {
    Ok(match s {
        "bool" => PgType::Bool,
        "int2" => PgType::Int2,
        "int4" => PgType::Int4,
        "int8" => PgType::Int8,
        "float4" => PgType::Float4,
        "float8" => PgType::Float8,
        "text" => PgType::Text,
        "numeric" => PgType::Numeric,
        "jsonb" => PgType::Jsonb,
        "bytea" => PgType::Bytea,
        "uuid" => PgType::Uuid,
        "date" => PgType::Date,
        "timestamp" => PgType::Timestamp,
        "timestamptz" => PgType::TimestampTz,
        other => bail!("unknown type '{other}'"),
    })
}

fn show(v: &Option<Value>) -> String {
    match v {
        None => "NULL".into(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::I16(x)) => x.to_string(),
        Some(Value::I32(x)) => x.to_string(),
        Some(Value::I64(x)) => x.to_string(),
        Some(Value::F32(x)) => x.to_string(),
        Some(Value::F64(x)) => x.to_string(),
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Bytes(b)) => format!("\\x{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
    }
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or_else(|| anyhow::anyhow!("usage: pgverify <page> <block> <types>"))?;
    let block: u32 = args.next().unwrap_or_else(|| "0".into()).parse()?;
    let types = args.next().unwrap_or_else(|| "int4".into());
    let cols: Vec<Col> = types
        .split(',')
        .enumerate()
        .map(|(i, t)| Ok(Col { name: format!("c{i}"), ty: pgtype(t.trim())? }))
        .collect::<Result<_>>()?;
    let desc = TableDesc {
        name: "v".into(),
        oid: 0,
        db_oid: 0,
        rel_node: 0,
        toast_rel_node: None,
        phys: cols.iter().cloned().map(PhysCol::Live).collect(),
        cols,
        has_fast_defaults: false,
        pk: vec![],
    };

    let page = std::fs::read(&path)?;
    if page.len() != 8192 {
        bail!("expected 8192-byte page, got {}", page.len());
    }
    let stored = u16::from_le_bytes([page[8], page[9]]);
    let computed = reconstruct::pg_checksum_page(&page, block);
    println!(
        "CHECKSUM: stored={stored:#06x} computed={computed:#06x}  -> {}",
        if stored == computed { "MATCH" } else { "MISMATCH" }
    );

    let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
    let n = pd_lower.saturating_sub(24) / 4;
    let cache = ToastCache::default();
    for offnum in 1..=n as u16 {
        if line_pointer(&page, offnum)?.flags != 1 {
            continue;
        }
        let (row, _) = decode_tuple_from_page(&page, offnum, &desc, &cache)?;
        println!("  {}", row.iter().map(show).collect::<Vec<_>>().join("|"));
    }
    Ok(())
}
