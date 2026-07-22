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
use crate::txbuf::Ctid;
use crate::wal::heap::{self, Row, Value};

/// Take a consistent snapshot: returns the cutover LSN and all rows with
/// their physical addresses (which seed the pre-image mirror), plus — when
/// the pageinspect extension is available — the exact on-page attribute
/// bytes per ctid (the faithful pre-image that prefix/suffix-compressed
/// updates need; re-encoding can't reproduce toast pointers or inline
/// compression).
pub async fn take(
    conninfo: &str,
    desc: &TableDesc,
) -> Result<(u64, Vec<(Ctid, Row)>, std::collections::HashMap<Ctid, Vec<u8>>)> {
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
        .copy_out(&format!(
            "COPY (SELECT ctid, t.* FROM public.\"{0}\" t) TO STDOUT (FORMAT binary)",
            desc.name
        ))
        .await
        .context("starting binary COPY")?;
    let chunks: Vec<bytes::Bytes> = stream.try_collect().await.context("reading COPY stream")?;

    // Best effort: exact on-page tuple bytes via pageinspect, inside the
    // same locked transaction so they match the COPY.
    let raw_attrs = read_raw_attrs(&tx, desc).await;

    tx.commit().await?; // release the lock before any object-store work
    handle.abort();

    let data: Vec<u8> = chunks.concat();
    let rows = parse_copy_binary(&data, desc)?;
    Ok((cutover, rows, raw_attrs))
}

/// Parse PG binary COPY output: 19+ byte header, then per tuple an i16
/// column count and per column an i32 length (-1 = NULL) + payload,
/// all big-endian; a -1 column count terminates.
fn parse_copy_binary(data: &[u8], desc: &TableDesc) -> Result<Vec<(Ctid, Row)>> {
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
        if nc as usize != desc.cols.len() + 1 {
            bail!("COPY tuple has {nc} columns, expected ctid + {}", desc.cols.len());
        }
        // First field: ctid (type tid) — u32 block number, u16 offset, BE.
        let len = i32::from_be_bytes(
            data.get(off..off + 4).ok_or_else(|| anyhow::anyhow!("truncated ctid length"))?.try_into().unwrap(),
        );
        off += 4;
        if len != 6 {
            bail!("ctid field has length {len}, expected 6");
        }
        let f = &data[off..off + 6];
        let ctid: Ctid = (
            u32::from_be_bytes(f[0..4].try_into().unwrap()),
            u16::from_be_bytes(f[4..6].try_into().unwrap()),
        );
        off += 6;

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
        rows.push((ctid, row));
    }
    Ok(rows)
}

/// Exact on-page attribute bytes per live ctid, via pageinspect (best
/// effort — returns empty and warns when the extension is unavailable).
/// Used at snapshot time (under the table lock) and again at restart to
/// refresh pre-images the change log cannot reproduce (toast pointers,
/// inline-compressed values). At restart this reads pages *ahead* of the
/// resume point, which is safe: any ctid that changed since is refreshed
/// by the replayed WAL before an update record can reference it.
pub async fn read_raw_attrs(
    client: &impl tokio_postgres::GenericClient,
    desc: &TableDesc,
) -> std::collections::HashMap<Ctid, Vec<u8>> {
    let mut raw_attrs = std::collections::HashMap::new();
    if client.batch_execute("CREATE EXTENSION IF NOT EXISTS pageinspect").await.is_err() {
        tracing::warn!(
            "pageinspect unavailable — pre-images for long/toasted rows will be approximate; \
             their first prefix-compressed UPDATE may be skipped"
        );
        return raw_attrs;
    }
    let items = client
        .query(
            &format!(
                "SELECT b.blkno::int8, i.lp::int8, i.t_data
                 FROM generate_series(0, (pg_relation_size('public.\"{0}\"') / 8192) - 1) AS b(blkno),
                      LATERAL heap_page_items(get_raw_page('public.\"{0}\"', b.blkno::int)) i
                 WHERE i.lp_flags = 1 AND i.t_data IS NOT NULL",
                desc.name
            ),
            &[],
        )
        .await;
    match items {
        Ok(items) => {
            for item in &items {
                let blkno: i64 = item.get(0);
                let lp: i64 = item.get(1);
                let data: Vec<u8> = item.get(2);
                raw_attrs.insert((blkno as u32, lp as u16), data);
            }
            tracing::debug!(tuples = raw_attrs.len(), "captured raw pre-image bytes via pageinspect");
        }
        Err(e) => tracing::warn!("pageinspect read failed: {e}"),
    }
    raw_attrs
}

/// Open a plain SQL connection and read raw attrs (restart path).
pub async fn read_raw_attrs_conn(
    conninfo: &str,
    desc: &TableDesc,
) -> Result<std::collections::HashMap<Ctid, Vec<u8>>> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
    let handle = tokio::spawn(conn);
    let map = read_raw_attrs(&client, desc).await;
    handle.abort();
    Ok(map)
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
        PgType::Numeric => Value::Text(heap::numeric_from_binary(f)?),
        // jsonb_send: a version byte (1) then the JSON text.
        PgType::Jsonb => Value::Text(String::from_utf8_lossy(f.get(1..).unwrap_or_default()).into_owned()),
        PgType::Date => Value::I32(i32::from_be_bytes(be(f, "date")?) + heap::PG_EPOCH_DAYS),
        PgType::Timestamp | PgType::TimestampTz => {
            Value::I64(i64::from_be_bytes(be(f, "timestamp")?) + heap::PG_EPOCH_MICROS)
        }
    })
}
