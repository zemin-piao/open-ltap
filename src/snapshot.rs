//! Initial snapshot with consistent cutover.
//!
//! Under `LOCK TABLE ... IN EXCLUSIVE MODE` (writers blocked, readers fine)
//! no transaction that has written the table can still be in flight, so
//! `pg_current_wal_insert_lsn()` taken under the lock is a clean cut:
//! every commit touching the table is either fully before it (in the COPY)
//! or fully after it (in the WAL stream). The snapshot ships as ONE Delta
//! commit carrying that LSN as both watermarks, so a crash mid-snapshot
//! just re-snapshots from scratch.
//!
//! Data comes over `COPY ... TO STDOUT (FORMAT binary)`: big-endian typed
//! fields that map 1:1 onto the same `Value`s the WAL decoder produces
//! (identical epoch shifts, uuid bytes, raw text/bytea payloads).

use anyhow::{Context, Result, bail};
use futures_util::TryStreamExt;
use tokio_postgres::NoTls;

use crate::pgwire::parse_lsn;
use crate::schema::{PgType, TableDesc};
use crate::wal::heap::{self, Row, Value};

/// Take a consistent snapshot: returns the cutover LSN and all rows.
pub async fn take(conninfo: &str, desc: &TableDesc) -> Result<(u64, Vec<Row>)> {
    let (mut client, conn) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .context("connecting for snapshot")?;
    let handle = tokio::spawn(conn);

    let tx = client.transaction().await?;
    tx.batch_execute(&format!("LOCK TABLE public.\"{}\" IN EXCLUSIVE MODE", desc.name))
        .await
        .context("locking table for snapshot")?;
    let lsn_row = tx.query_one("SELECT pg_current_wal_insert_lsn()::text", &[]).await?;
    let cutover = parse_lsn(lsn_row.get(0))?;

    let stream = tx
        .copy_out(&format!("COPY public.\"{}\" TO STDOUT (FORMAT binary)", desc.name))
        .await
        .context("starting binary COPY")?;
    let chunks: Vec<bytes::Bytes> = stream.try_collect().await.context("reading COPY stream")?;
    tx.commit().await?; // release the lock before any object-store work
    handle.abort();

    let data: Vec<u8> = chunks.concat();
    let rows = parse_copy_binary(&data, desc)?;
    Ok((cutover, rows))
}

/// Parse PG binary COPY output: 19+ byte header, then per tuple an i16
/// column count and per column an i32 length (-1 = NULL) + payload,
/// all big-endian; a -1 column count terminates.
fn parse_copy_binary(data: &[u8], desc: &TableDesc) -> Result<Vec<Row>> {
    const SIG: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
    if data.len() < 19 || &data[..11] != SIG {
        bail!("bad COPY binary signature");
    }
    let ext_len = u32::from_be_bytes(data[15..19].try_into().unwrap()) as usize;
    let mut off = 19 + ext_len;

    let mut rows = Vec::new();
    loop {
        let nc = data
            .get(off..off + 2)
            .map(|s| i16::from_be_bytes(s.try_into().unwrap()))
            .ok_or_else(|| anyhow::anyhow!("COPY stream truncated at tuple header"))?;
        off += 2;
        if nc == -1 {
            break; // trailer
        }
        if nc as usize != desc.cols.len() {
            bail!("COPY tuple has {nc} columns, table has {}", desc.cols.len());
        }
        let mut row: Row = Vec::with_capacity(desc.cols.len());
        for col in &desc.cols {
            let len = data
                .get(off..off + 4)
                .map(|s| i32::from_be_bytes(s.try_into().unwrap()))
                .ok_or_else(|| anyhow::anyhow!("COPY stream truncated at field length"))?;
            off += 4;
            if len < 0 {
                row.push(None);
                continue;
            }
            let field = data
                .get(off..off + len as usize)
                .ok_or_else(|| anyhow::anyhow!("COPY stream truncated in field"))?;
            off += len as usize;
            row.push(Some(decode_field(field, col.ty).with_context(|| format!("column '{}'", col.name))?));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn be<const N: usize>(f: &[u8], what: &str) -> Result<[u8; N]> {
    f.try_into().map_err(|_| anyhow::anyhow!("bad {what} field length {}", f.len()))
}

fn decode_field(f: &[u8], ty: PgType) -> Result<Value> {
    Ok(match ty {
        PgType::Bool => Value::Bool(*f.first().ok_or_else(|| anyhow::anyhow!("empty bool"))? != 0),
        PgType::Int2 => Value::I16(i16::from_be_bytes(be(f, "int2")?)),
        PgType::Int4 => Value::I32(i32::from_be_bytes(be(f, "int4")?)),
        PgType::Int8 => Value::I64(i64::from_be_bytes(be(f, "int8")?)),
        PgType::Float4 => Value::F32(f32::from_be_bytes(be(f, "float4")?)),
        PgType::Float8 => Value::F64(f64::from_be_bytes(be(f, "float8")?)),
        PgType::Text => Value::Text(String::from_utf8_lossy(f).into_owned()),
        PgType::Bytea => Value::Bytes(f.to_vec()),
        PgType::Uuid => Value::Text(heap::format_uuid(f)),
        PgType::Date => Value::I32(i32::from_be_bytes(be(f, "date")?) + heap::PG_EPOCH_DAYS),
        PgType::Timestamp | PgType::TimestampTz => {
            Value::I64(i64::from_be_bytes(be(f, "timestamp")?) + heap::PG_EPOCH_MICROS)
        }
    })
}
