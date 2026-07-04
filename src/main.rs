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
mod snapshot;
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
    /// Take an initial snapshot when the Delta table has no watermark yet.
    snapshot: bool,
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
            snapshot: !matches!(env_or("LTAP_SNAPSHOT", "on").as_str(), "off" | "0" | "false"),
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

/// Committed change rows waiting to be flushed to Delta as one commit.
#[derive(Default)]
struct PendingBatch {
    rows: Vec<sink::EmitRow>,
    first_at: Option<Instant>,
    /// Highest PG commit LSN among the pending rows.
    max_commit_lsn: u64,
    /// Global tie-break counter within equal commit LSNs.
    seq: u64,
}

impl PendingBatch {
    fn emit(&mut self, lsn: u64, deleted: bool, ctid: txbuf::Ctid, row: heap::Row) {
        if self.first_at.is_none() {
            self.first_at = Some(Instant::now());
        }
        self.max_commit_lsn = self.max_commit_lsn.max(lsn);
        self.seq += 1;
        self.rows.push(sink::EmitRow { lsn, seq: self.seq, deleted, ctid, row });
    }
}

/// Last committed version of every live row, keyed by physical address.
/// Pre-images for DELETE (row content for the tombstone) and UPDATE
/// (prefix/suffix reconstruction, unchanged-toast carry-over) come from
/// here (or from an open transaction's overlay for intra-txn chains).
type Mirror = std::collections::HashMap<txbuf::Ctid, txbuf::RowVersion>;

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
    let mut dedupe_below = resume.commit_lsn.unwrap_or(0);

    // 3. Attach to the WAL stream (slot keeps WAL retained while we're down).
    let mut conn = pgwire::ReplConn::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_user).await?;
    let (timeline, flush_lsn) = conn.identify_system().await?;
    if conn.create_slot(&cfg.slot).await? {
        tracing::info!(slot = %cfg.slot, "created replication slot");
    }
    // Resume priority: Delta watermark > fresh snapshot > pre-existing
    // slot's retained WAL (snapshot disabled) > "now".
    let slot_restart = conn.slot_restart_lsn(&cfg.slot).await?;
    let mut mirror: Mirror = Mirror::new();
    let resume_from = match resume.restart_lsn {
        Some(r) => {
            // The change log IS the table state at the dedupe watermark —
            // rebuild the pre-image mirror from it before replaying, and
            // refresh raw pre-image bytes the log can't reproduce (toast
            // pointers, inline compression) from live pages.
            mirror = sink.load_mirror(&desc).await?;
            let raw = snapshot::read_raw_attrs_conn(&cfg.sql_conninfo(), &desc).await?;
            let mut refreshed = 0usize;
            for (ctid, ver) in mirror.iter_mut() {
                if ver.attrs.is_none() {
                    if let Some(bytes) = raw.get(ctid) {
                        ver.attrs = Some(bytes.clone());
                        refreshed += 1;
                    }
                }
            }
            tracing::info!(rows = mirror.len(), refreshed, "pre-image mirror rebuilt from Delta");
            r
        }
        None if cfg.snapshot => {
            let (cutover, rows, mut raw_attrs) = snapshot::take(&cfg.sql_conninfo(), &desc).await?;
            tracing::info!(
                rows = rows.len(),
                cutover = %pgwire::fmt_lsn(cutover),
                "initial snapshot taken (table was write-locked until here)"
            );
            for (ctid, row) in &rows {
                // Exact on-page bytes if pageinspect provided them; else a
                // re-encoding (faithful only when every varlena is short).
                let attrs = raw_attrs.remove(ctid).or_else(|| heap::encode_attrs(row, &desc));
                mirror.insert(*ctid, txbuf::RowVersion { row: row.clone(), attrs });
            }
            if !rows.is_empty() {
                let emits: Vec<sink::EmitRow> = rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, (ctid, row))| sink::EmitRow {
                        lsn: cutover,
                        seq: i as u64,
                        deleted: false,
                        ctid,
                        row,
                    })
                    .collect();
                let version = sink.append(&emits, cutover, cutover).await?;
                tracing::info!(delta_version = version, "initial snapshot committed to Delta");
            }
            dedupe_below = cutover;
            cutover
        }
        None => slot_restart.unwrap_or(flush_lsn),
    };
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
                        handle_record(lsn, &rec, &desc, &mut txbuf, &mut toast, &mut mirror, &mut pending, dedupe_below)?;
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

/// Pre-image lookup: an open transaction's own uncommitted version wins
/// over the last committed one.
fn preimage<'a>(txbuf: &'a txbuf::TxBuffer, mirror: &'a Mirror, ctid: txbuf::Ctid) -> Option<&'a txbuf::RowVersion> {
    txbuf.lookup(ctid).or_else(|| mirror.get(&ctid))
}

fn handle_record(
    lsn: u64,
    rec: &[u8],
    desc: &schema::TableDesc,
    txbuf: &mut txbuf::TxBuffer,
    toast: &mut heap::ToastCache,
    mirror: &mut Mirror,
    pending: &mut PendingBatch,
    dedupe_below: u64,
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
                    let decoded = if !block0.data.is_empty() {
                        heap::decode_insert_tuple(&block0.data, desc, toast)
                    } else if let Some(img) = &block0.image {
                        // full_page_writes=on: the tuple lives in the image.
                        img.restore().and_then(|page| {
                            heap::decode_tuple_from_page(&page, heap::insert_offnum(&record.main_data)?, desc, toast)
                        })
                    } else {
                        Err(anyhow::anyhow!("insert with neither data nor image"))
                    };
                    match (decoded, heap::insert_offnum(&record.main_data)) {
                        (Ok((row, attrs)), Ok(offnum)) => {
                            let ctid = (block0.blkno, offnum);
                            txbuf.add_op(record.xid, lsn, txbuf::Op::Insert {
                                ctid,
                                ver: txbuf::RowVersion { row, attrs: Some(attrs) },
                            });
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            if lsn <= dedupe_below {
                                tracing::debug!(lsn = %pgwire::fmt_lsn(lsn), "replay: failed to decode insert: {e}");
                            } else {
                                tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode insert: {e}");
                            }
                        }
                    }
                }
                heap::XLOG_HEAP_DELETE if is_main => {
                    match heap::delete_offnum(&record.main_data) {
                        Ok(offnum) => {
                            let ctid = (block0.blkno, offnum);
                            let old_row = preimage(txbuf, mirror, ctid).map(|v| v.row.clone());
                            if old_row.is_none() && lsn > dedupe_below {
                                tracing::warn!(
                                    lsn = %pgwire::fmt_lsn(lsn),
                                    ctid = ?ctid,
                                    "DELETE of a row not in the mirror — tombstone will be empty"
                                );
                            }
                            txbuf.add_op(record.xid, lsn, txbuf::Op::Delete { ctid, old_row });
                        }
                        Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to parse delete: {e}"),
                    }
                }
                heap::XLOG_HEAP_UPDATE | heap::XLOG_HEAP_HOT_UPDATE if is_main => {
                    if let Err(e) = handle_update(lsn, &record, block0, desc, txbuf, toast, mirror) {
                        if lsn <= dedupe_below {
                            tracing::debug!(lsn = %pgwire::fmt_lsn(lsn), "replay: failed to decode update: {e}");
                        } else {
                            tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode update: {e}");
                        }
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
            match (rows, heap::multi_insert_offsets(&record.main_data, record.info)) {
                (Ok(rows), Ok(offsets)) if rows.len() == offsets.len() => {
                    for ((row, attrs), offnum) in rows.into_iter().zip(offsets) {
                        txbuf.add_op(record.xid, lsn, txbuf::Op::Insert {
                            ctid: (block0.blkno, offnum),
                            ver: txbuf::RowVersion { row, attrs: Some(attrs) },
                        });
                    }
                }
                (Ok(rows), Ok(offsets)) => {
                    tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "multi-insert: {} rows vs {} offsets", rows.len(), offsets.len())
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode multi-insert: {e}")
                }
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
                    let ops = txbuf.commit(record.xid, &subxids);
                    toast.gc_xid(record.xid);
                    for sub in &subxids {
                        toast.gc_xid(*sub);
                    }
                    if ops.is_empty() {
                        return Ok(());
                    }
                    if lsn <= dedupe_below {
                        // Replay of a commit whose effects are both in Delta
                        // AND in the mirror rebuilt from it: skip entirely.
                        tracing::debug!(
                            xid = record.xid,
                            ops = ops.len(),
                            commit_lsn = %pgwire::fmt_lsn(lsn),
                            "replayed commit already in Delta — skipped"
                        );
                        return Ok(());
                    }
                    tracing::debug!(
                        xid = record.xid,
                        ops = ops.len(),
                        commit_lsn = %pgwire::fmt_lsn(lsn),
                        "transaction committed — applying"
                    );
                    for pending_op in ops {
                        match pending_op.op {
                            txbuf::Op::Insert { ctid, ver } => {
                                pending.emit(lsn, false, ctid, ver.row.clone());
                                mirror.insert(ctid, ver);
                            }
                            txbuf::Op::Update { old_ctid, ctid, ver } => {
                                mirror.remove(&old_ctid);
                                pending.emit(lsn, false, ctid, ver.row.clone());
                                mirror.insert(ctid, ver);
                            }
                            txbuf::Op::Delete { ctid, old_row } => {
                                let row = old_row
                                    .or_else(|| mirror.get(&ctid).map(|v| v.row.clone()))
                                    .unwrap_or_else(|| vec![None; desc.cols.len()]);
                                mirror.remove(&ctid);
                                pending.emit(lsn, true, ctid, row);
                            }
                        }
                    }
                }
                heap::XLOG_XACT_ABORT => {
                    toast.gc_xid(record.xid);
                    for sub in &subxids {
                        toast.gc_xid(*sub);
                    }
                    let dropped = txbuf.abort(record.xid, &subxids);
                    if dropped > 0 {
                        tracing::info!(xid = record.xid, ops = dropped, "aborted transaction discarded");
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
    Ok(())
}

/// Decode an UPDATE/HOT UPDATE: find the pre-image, reconstruct the new
/// tuple (prefix/suffix bytes come from the old tuple; unchanged toast
/// values from the old row), and buffer the op.
fn handle_update(
    lsn: u64,
    record: &wal::Record,
    block0: &wal::BlockRef,
    desc: &schema::TableDesc,
    txbuf: &mut txbuf::TxBuffer,
    toast: &heap::ToastCache,
    mirror: &Mirror,
) -> Result<()> {
    let info = heap::parse_update_main(&record.main_data)?;
    // Block 0 = the new tuple's page; block 1 (if present) = the old page.
    let old_blkno = record.blocks.iter().find(|b| b.id == 1).map(|b| b.blkno).unwrap_or(block0.blkno);
    let old_ctid = (old_blkno, info.old_offnum);
    let new_ctid = (block0.blkno, info.new_offnum);

    let old = preimage(txbuf, mirror, old_ctid);
    let (old_attrs, old_row) = match old {
        Some(v) => (v.attrs.clone(), Some(v.row.clone())),
        None => (None, None),
    };

    let (row, attrs) = if !block0.data.is_empty() {
        heap::decode_update_new_tuple(
            &block0.data,
            info.flags,
            old_attrs.as_deref(),
            old_row.as_ref(),
            desc,
            toast,
        )?
    } else if let Some(img) = &block0.image {
        // FPI carries the complete new tuple; prefix/suffix never apply here.
        let page = img.restore()?;
        let (row, attrs) = heap::decode_tuple_from_page(&page, info.new_offnum, desc, toast)?;
        (row, attrs)
    } else {
        anyhow::bail!("update with neither data nor image");
    };
    let _ = lsn;
    txbuf.add_op(record.xid, lsn, txbuf::Op::Update {
        old_ctid,
        ctid: new_ctid,
        ver: txbuf::RowVersion { row, attrs: Some(attrs) },
    });
    Ok(())
}
