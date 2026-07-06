//! M4 freshness read path: serve the in-memory tail over HTTP so readers
//! can merge it with the Delta table and see every committed transaction
//! the transcoder has processed — no waiting for the next Delta flush,
//! no load on Postgres.
//!
//!   GET /tail/<table>.parquet[?min_lsn=N[&timeout_ms=M]]
//!       The tail as a Parquet file: rows committed in PG but not yet
//!       flushed to Delta, plus a retention window of recently flushed
//!       rows. With min_lsn, long-polls until the transcoder has applied
//!       WAL up to that LSN — read-your-writes for a client that captured
//!       pg_current_wal_lsn() after its commit.
//!   GET /status
//!       JSON: applied LSN and per-table tail row counts.
//!
//! Merged read (DuckDB; overlap between Delta and tail is collapsed by the
//! same latest-per-key dedupe every reader already uses):
//!
//!   SET force_download=true;  -- tail is small; skip range requests
//!   WITH log AS (
//!     SELECT * FROM delta_scan('s3://lake/t')
//!     UNION ALL BY NAME
//!     SELECT * FROM read_parquet('http://localhost:8088/tail/t.parquet')
//!   )
//!   SELECT * FROM log
//!   QUALIFY row_number() OVER (PARTITION BY id
//!     ORDER BY _ltap_lsn DESC, _ltap_seq DESC) = 1 AND NOT _ltap_deleted;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::parquet::arrow::ArrowWriter;
use std::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct TailBatch {
    pub batch: RecordBatch,
    /// None while pending; set when the batch went durable in Delta. Kept
    /// for a retention window afterwards so `delta_scan` + tail never has a
    /// gap regardless of which side a reader queries first.
    pub flushed_at: Option<Instant>,
}

#[derive(Default)]
pub struct TableTail {
    pub batches: Vec<TailBatch>,
}

#[derive(Default)]
pub struct TailStore {
    /// WAL position fully applied to the tails (commits at or below are
    /// queryable via Delta + tail).
    pub applied_lsn: u64,
    pub tables: HashMap<String, TableTail>,
}

impl TailStore {
    pub fn push(&mut self, table: &str, batch: RecordBatch) {
        self.tables
            .entry(table.to_string())
            .or_default()
            .batches
            .push(TailBatch { batch, flushed_at: None });
    }

    /// The table's pending rows went durable: start their retention clock,
    /// and GC batches whose retention has passed.
    pub fn mark_flushed(&mut self, table: &str, retain: Duration) {
        if let Some(t) = self.tables.get_mut(table) {
            let now = Instant::now();
            for b in &mut t.batches {
                b.flushed_at.get_or_insert(now);
            }
            t.batches.retain(|b| b.flushed_at.is_none_or(|at| at.elapsed() < retain));
        }
    }

    /// Bound tail memory: evict oldest *flushed* batches (already durable in
    /// Delta — eviction only narrows the overlap window) until the table is
    /// under `cap` rows. Unflushed batches are never evicted: they are the
    /// only copy outside Postgres until the next flush.
    pub fn enforce_cap(&mut self, table: &str, cap: usize) {
        if let Some(t) = self.tables.get_mut(table) {
            let mut total: usize = t.batches.iter().map(|b| b.batch.num_rows()).sum();
            while total > cap {
                let Some(pos) = t.batches.iter().position(|b| b.flushed_at.is_some()) else {
                    break; // everything left is unflushed: must be kept
                };
                total -= t.batches[pos].batch.num_rows();
                t.batches.remove(pos);
            }
        }
    }

    /// Schema changed / table re-snapshotted: drop the tail (everything is
    /// durable in Delta at this point — remaps flush first).
    pub fn clear(&mut self, table: &str) {
        self.tables.remove(table);
    }
}

pub type SharedTail = Arc<RwLock<TailStore>>;

pub async fn serve(store: SharedTail, port: u16) -> Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "freshness endpoint listening (GET /tail/<table>.parquet, /status)");
    loop {
        let (mut sock, _) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&mut sock, store).await {
                tracing::debug!("tail request failed: {e:#}");
            }
        });
    }
}

async fn handle(sock: &mut TcpStream, store: SharedTail) -> Result<()> {
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 512];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        if buf.len() > 8192 {
            anyhow::bail!("request too large");
        }
        let n = sock.read(&mut byte).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&byte[..n]);
    }
    let req = String::from_utf8_lossy(&buf);
    let line = req.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    if method != "GET" && method != "HEAD" {
        return respond(sock, 405, "text/plain", b"method not allowed", 0).await;
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params: HashMap<&str, &str> =
        query.split('&').filter_map(|kv| kv.split_once('=')).collect();

    if path == "/status" {
        let (body, applied) = {
            let s = store.read().unwrap();
            let mut tables: Vec<String> = s
                .tables
                .iter()
                .map(|(name, t)| {
                    let rows: usize = t.batches.iter().map(|b| b.batch.num_rows()).sum();
                    let pending: usize = t
                        .batches
                        .iter()
                        .filter(|b| b.flushed_at.is_none())
                        .map(|b| b.batch.num_rows())
                        .sum();
                    format!("\"{name}\":{{\"tail_rows\":{rows},\"unflushed_rows\":{pending}}}")
                })
                .collect();
            tables.sort();
            let body = format!(
                "{{\"applied_lsn\":{},\"tables\":{{{}}}}}\n",
                s.applied_lsn,
                tables.join(",")
            );
            (body, s.applied_lsn)
        };
        return respond(sock, 200, "application/json", body.as_bytes(), applied).await;
    }

    let Some(table) = path.strip_prefix("/tail/").and_then(|p| p.strip_suffix(".parquet")) else {
        return respond(sock, 404, "text/plain", b"not found", 0).await;
    };

    // Read-your-writes: wait until the stream has applied the caller's LSN.
    if let Some(min_lsn) = params.get("min_lsn").and_then(|v| v.parse::<u64>().ok()) {
        let timeout = params
            .get("timeout_ms")
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(10));
        let deadline = Instant::now() + timeout;
        loop {
            if store.read().unwrap().applied_lsn >= min_lsn {
                break;
            }
            if Instant::now() >= deadline {
                return respond(sock, 408, "text/plain", b"timed out waiting for min_lsn", 0).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let (batches, applied) = {
        let s = store.read().unwrap();
        let batches: Vec<RecordBatch> = match s.tables.get(table) {
            Some(t) if !t.batches.is_empty() => {
                // Serve only batches matching the newest schema (older ones
                // predate a schema change and are already durable in Delta).
                let latest = t.batches.last().unwrap().batch.schema();
                t.batches
                    .iter()
                    .filter(|b| b.batch.schema() == latest)
                    .map(|b| b.batch.clone())
                    .collect()
            }
            _ => Vec::new(),
        };
        (batches, s.applied_lsn)
    };
    if batches.is_empty() {
        // An empty tail is still a valid (zero-row) answer, but we need a
        // schema to write a Parquet file; readers treat 204 as "no tail".
        return respond(sock, 204, "application/octet-stream", b"", applied).await;
    }
    let bytes = {
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batches[0].schema(), None)?;
        for b in &batches {
            writer.write(b)?;
        }
        writer.close()?;
        out
    };
    if method == "HEAD" {
        return respond_head(sock, bytes.len(), applied).await;
    }
    respond(sock, 200, "application/octet-stream", &bytes, applied).await
}

async fn respond(
    sock: &mut TcpStream,
    code: u16,
    ctype: &str,
    body: &[u8],
    applied: u64,
) -> Result<()> {
    let reason = match code {
        200 => "OK",
        204 => "No Content",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        _ => "Error",
    };
    let hdr = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nX-Ltap-Applied-Lsn: {applied}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(hdr.as_bytes()).await?;
    sock.write_all(body).await?;
    sock.shutdown().await?;
    Ok(())
}

async fn respond_head(sock: &mut TcpStream, len: usize, applied: u64) -> Result<()> {
    let hdr = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {len}\r\nX-Ltap-Applied-Lsn: {applied}\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\n"
    );
    sock.write_all(hdr.as_bytes()).await?;
    sock.shutdown().await?;
    Ok(())
}
