//! open-ltap: stream physical WAL out of Postgres, decode committed heap
//! changes, and land them in a Delta table on object storage.
//!
//! M1: exactly-once across restarts (LSN watermarks persisted in Delta
//! commits), replication slot so PG retains WAL while we're down, CRC32C
//! record validation, COPY (multi-insert), and batched Delta commits.
//! See README for the milestone map.

mod pgwire;
mod schema;
mod sink;
mod txbuf;
mod wal;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use wal::heap;
use wal::rmgr;

struct Config {
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    pg_password: String,
    pg_db: String,
    table: String,
    slot: String,
    delta_uri: String,
    s3_endpoint: String,
    s3_access_key: String,
    s3_secret_key: String,
    /// Flush to Delta once this many rows are pending...
    flush_rows: usize,
    /// ...or this much time has passed since the first pending row.
    flush_interval: Duration,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Self {
        let table = std::env::args().nth(1).unwrap_or_else(|| env_or("LTAP_TABLE", "t"));
        Config {
            pg_host: env_or("PG_HOST", "localhost"),
            pg_port: env_or("PG_PORT", "5432").parse().expect("PG_PORT"),
            pg_user: env_or("PG_USER", "postgres"),
            pg_password: env_or("PG_PASSWORD", "postgres"),
            pg_db: env_or("PG_DB", "app"),
            slot: env_or("LTAP_SLOT", &format!("ltap_{table}")),
            delta_uri: env_or("DELTA_URI", &format!("s3://lake/{table}")),
            s3_endpoint: env_or("S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key: env_or("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key: env_or("S3_SECRET_KEY", "minioadmin"),
            flush_rows: env_or("LTAP_FLUSH_ROWS", "5000").parse().expect("LTAP_FLUSH_ROWS"),
            flush_interval: Duration::from_millis(
                env_or("LTAP_FLUSH_MS", "750").parse().expect("LTAP_FLUSH_MS"),
            ),
            table,
        }
    }

    fn sql_conninfo(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.pg_host, self.pg_port, self.pg_user, self.pg_password, self.pg_db
        )
    }

    fn storage_options(&self) -> HashMap<String, String> {
        HashMap::from([
            ("AWS_ACCESS_KEY_ID".into(), self.s3_access_key.clone()),
            ("AWS_SECRET_ACCESS_KEY".into(), self.s3_secret_key.clone()),
            ("AWS_ENDPOINT_URL".into(), self.s3_endpoint.clone()),
            ("AWS_REGION".into(), "us-east-1".into()),
            ("AWS_ALLOW_HTTP".into(), "true".into()),
            // Single-writer dev setup; multi-writer safety is a later milestone.
            ("AWS_S3_ALLOW_UNSAFE_RENAME".into(), "true".into()),
        ])
    }
}

/// Committed rows waiting to be flushed to Delta as one commit.
#[derive(Default)]
struct PendingBatch {
    rows: Vec<sink::TaggedRow>,
    first_at: Option<Instant>,
    /// Highest PG commit LSN among the pending rows.
    max_commit_lsn: u64,
}

impl PendingBatch {
    fn push_commit(&mut self, commit_lsn: u64, rows: Vec<heap::Row>) {
        if self.first_at.is_none() {
            self.first_at = Some(Instant::now());
        }
        self.max_commit_lsn = self.max_commit_lsn.max(commit_lsn);
        self.rows.extend(rows.into_iter().map(|r| (commit_lsn, r)));
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "open_ltap=info,deltalake=warn".into()),
        )
        .init();
    let cfg = Config::from_env();

    // 1. Discover the table shape over a normal SQL connection.
    let desc = schema::discover(&cfg.sql_conninfo(), &cfg.table).await?;
    tracing::info!(
        table = %desc.name,
        rel_node = desc.rel_node,
        cols = ?desc.cols.iter().map(|c| format!("{}:{:?}", c.name, c.ty)).collect::<Vec<_>>(),
        "discovered table"
    );

    // 2. Open (or create) the Delta table and read back where we left off.
    let mut sink = sink::DeltaSink::open_or_create(&cfg.delta_uri, cfg.storage_options(), &desc).await?;
    let resume = sink.resume_state().await?;
    // Commits at or below this LSN are already in Delta: drop them on replay.
    let dedupe_below = resume.commit_lsn.unwrap_or(0);

    // 3. Attach to the WAL stream (slot keeps WAL retained while we're down).
    let mut conn = pgwire::ReplConn::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_user).await?;
    let (timeline, flush_lsn) = conn.identify_system().await?;
    if conn.create_slot(&cfg.slot).await? {
        tracing::info!(slot = %cfg.slot, "created replication slot");
    }
    // Resume priority: Delta watermark > pre-existing slot's retained WAL
    // (Delta empty but the slot pinned history — don't drop it) > "now".
    let slot_restart = conn.slot_restart_lsn(&cfg.slot).await?;
    let resume_from = resume.restart_lsn.or(slot_restart).unwrap_or(flush_lsn);
    let start_lsn = resume_from & !(wal::XLOG_PAGE_SIZE - 1); // page-align: reader syncs via page header
    tracing::info!(
        timeline,
        slot = %cfg.slot,
        start = %pgwire::fmt_lsn(start_lsn),
        dedupe_below = %pgwire::fmt_lsn(dedupe_below),
        resumed = resume.restart_lsn.is_some(),
        "starting physical replication"
    );
    conn.start_replication(&cfg.slot, start_lsn, timeline).await?;

    let mut reader = wal::WalReader::new(start_lsn);
    let mut txbuf = txbuf::TxBuffer::default();
    let mut toast = heap::ToastCache::default();
    let mut pending = PendingBatch::default();
    let mut last_recv_lsn = start_lsn;
    // What the slot may prune up to: only advances when Delta is durable.
    let mut persisted_restart = resume_from;
    let mut warned_unsupported = false;
    let mut status_interval = tokio::time::interval(Duration::from_secs(10));
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut flush_tick = tokio::time::interval(cfg.flush_interval.min(Duration::from_millis(250)));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = conn.next_msg() => match msg? {
                pgwire::ReplMsg::XLogData(x) => {
                    last_recv_lsn = x.wal_end.max(last_recv_lsn);
                    for (lsn, rec) in reader.feed(x.start_lsn, &x.data)? {
                        handle_record(lsn, &rec, &desc, &mut txbuf, &mut toast, &mut pending, dedupe_below, &mut warned_unsupported)?;
                    }
                    if pending.rows.len() >= cfg.flush_rows {
                        flush(&mut sink, &mut pending, &txbuf, &mut persisted_restart).await?;
                    }
                }
                pgwire::ReplMsg::Keepalive { wal_end, reply_requested } => {
                    last_recv_lsn = wal_end.max(last_recv_lsn);
                    if reply_requested {
                        conn.send_status(last_recv_lsn, persisted_restart).await?;
                    }
                }
            },
            _ = flush_tick.tick() => {
                let due = pending
                    .first_at
                    .is_some_and(|t| t.elapsed() >= cfg.flush_interval);
                if due {
                    flush(&mut sink, &mut pending, &txbuf, &mut persisted_restart).await?;
                    conn.send_status(last_recv_lsn, persisted_restart).await?;
                }
            }
            _ = status_interval.tick() => {
                conn.send_status(last_recv_lsn, persisted_restart).await?;
            }
        }
    }
}

/// Write everything pending as one Delta commit and advance the watermarks.
async fn flush(
    sink: &mut sink::DeltaSink,
    pending: &mut PendingBatch,
    txbuf: &txbuf::TxBuffer,
    persisted_restart: &mut u64,
) -> Result<()> {
    if pending.rows.is_empty() {
        return Ok(());
    }
    let commit_lsn = pending.max_commit_lsn;
    // Resume point: the oldest still-open transaction's first record, or —
    // if nothing is in flight — the newest flushed commit record (replaying
    // it is harmless: the dedupe watermark drops it).
    let restart_lsn = txbuf.oldest_first_lsn().unwrap_or(commit_lsn);
    let n = pending.rows.len();
    let version = sink.append(&pending.rows, commit_lsn, restart_lsn).await?;
    tracing::info!(
        rows = n,
        commit_lsn = %pgwire::fmt_lsn(commit_lsn),
        restart_lsn = %pgwire::fmt_lsn(restart_lsn),
        delta_version = version,
        "flushed batch to Delta"
    );
    pending.rows.clear();
    pending.first_at = None;
    *persisted_restart = restart_lsn;
    Ok(())
}

fn handle_record(
    lsn: u64,
    rec: &[u8],
    desc: &schema::TableDesc,
    txbuf: &mut txbuf::TxBuffer,
    toast: &mut heap::ToastCache,
    pending: &mut PendingBatch,
    dedupe_below: u64,
    warned_unsupported: &mut bool,
) -> Result<()> {
    let record = match wal::parse_record(rec) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "skipping unparseable record: {e}");
            return Ok(());
        }
    };

    match record.rmid {
        rmgr::HEAP => {
            let op = record.info & heap::XLOG_HEAP_OPMASK;
            let Some(block0) = record.blocks.iter().find(|b| b.id == 0) else {
                return Ok(());
            };
            if block0.rel.db != desc.db_oid {
                return Ok(());
            }
            let is_main = block0.rel.rel == desc.rel_node;
            let is_toast = desc.toast_rel_node == Some(block0.rel.rel);
            if !is_main && !is_toast {
                return Ok(()); // some other relation (indexes, catalogs, other tables)
            }
            match op {
                heap::XLOG_HEAP_INSERT if is_toast => {
                    // A chunk of an out-of-line value; buffer it for the
                    // pointer tuple that follows in the same transaction.
                    let chunk = if !block0.data.is_empty() {
                        heap::decode_toast_chunk_from_wal(&block0.data)
                    } else if let Some(img) = &block0.image {
                        img.restore().and_then(|page| {
                            heap::decode_toast_chunk_from_page(&page, heap::insert_offnum(&record.main_data)?)
                        })
                    } else {
                        Err(anyhow::anyhow!("toast insert with neither data nor image"))
                    };
                    match chunk {
                        Ok((valueid, seq, data)) => toast.add_chunk(record.xid, valueid, seq, data),
                        Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode toast chunk: {e}"),
                    }
                }
                heap::XLOG_HEAP_INSERT => {
                    let row = if !block0.data.is_empty() {
                        heap::decode_insert_tuple(&block0.data, desc, toast)
                    } else if let Some(img) = &block0.image {
                        // full_page_writes=on: the tuple lives in the image.
                        img.restore().and_then(|page| {
                            heap::decode_tuple_from_page(&page, heap::insert_offnum(&record.main_data)?, desc, toast)
                        })
                    } else {
                        Err(anyhow::anyhow!("insert with neither data nor image"))
                    };
                    match row {
                        Ok(row) => txbuf.add(record.xid, lsn, row),
                        Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode insert: {e}"),
                    }
                }
                heap::XLOG_HEAP_DELETE | heap::XLOG_HEAP_UPDATE | heap::XLOG_HEAP_HOT_UPDATE
                    if is_main =>
                {
                    if !*warned_unsupported {
                        *warned_unsupported = true;
                        tracing::warn!(
                            "UPDATE/DELETE on '{}' observed — not yet transcoded (milestone M2: deletion vectors)",
                            desc.name
                        );
                    }
                }
                _ => {}
            }
        }
        rmgr::HEAP2 => {
            let op = record.info & heap::XLOG_HEAP_OPMASK;
            if op != heap::XLOG_HEAP2_MULTI_INSERT {
                return Ok(());
            }
            let Some(block0) = record.blocks.iter().find(|b| b.id == 0) else {
                return Ok(());
            };
            if block0.rel.db != desc.db_oid || block0.rel.rel != desc.rel_node {
                return Ok(());
            }
            let rows = if !block0.data.is_empty() {
                heap::decode_multi_insert(&block0.data, &record.main_data, desc, toast)
            } else if let Some(img) = &block0.image {
                img.restore().and_then(|page| {
                    heap::multi_insert_offsets(&record.main_data, record.info)?
                        .iter()
                        .map(|&off| heap::decode_tuple_from_page(&page, off, desc, toast))
                        .collect()
                })
            } else {
                Err(anyhow::anyhow!("multi-insert with neither data nor image"))
            };
            match rows {
                Ok(rows) => txbuf.add_many(record.xid, lsn, rows),
                Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode multi-insert: {e}"),
            }
        }
        rmgr::XACT => {
            let op = record.info & heap::XLOG_XACT_OPMASK;
            let subxids = match op {
                heap::XLOG_XACT_COMMIT | heap::XLOG_XACT_ABORT => {
                    heap::parse_xact_subxacts(record.info, &record.main_data).unwrap_or_else(|e| {
                        tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to parse subxact list: {e}");
                        Vec::new()
                    })
                }
                _ => Vec::new(),
            };
            match op {
                heap::XLOG_XACT_COMMIT => {
                    toast.gc_xid(record.xid);
                    for sub in &subxids {
                        toast.gc_xid(*sub);
                    }
                    let rows = txbuf.commit(record.xid, &subxids);
                    if rows.is_empty() {
                        return Ok(());
                    }
                    if lsn <= dedupe_below {
                        tracing::debug!(
                            xid = record.xid,
                            rows = rows.len(),
                            commit_lsn = %pgwire::fmt_lsn(lsn),
                            "replayed commit already in Delta — skipped"
                        );
                        return Ok(());
                    }
                    tracing::debug!(
                        xid = record.xid,
                        rows = rows.len(),
                        commit_lsn = %pgwire::fmt_lsn(lsn),
                        "transaction committed — buffered for flush"
                    );
                    pending.push_commit(lsn, rows);
                }
                heap::XLOG_XACT_ABORT => {
                    toast.gc_xid(record.xid);
                    for sub in &subxids {
                        toast.gc_xid(*sub);
                    }
                    let dropped = txbuf.abort(record.xid, &subxids);
                    if dropped > 0 {
                        tracing::info!(xid = record.xid, rows = dropped, "aborted transaction discarded");
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
}
