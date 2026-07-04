//! open-ltap: stream physical WAL out of Postgres, decode committed heap
//! changes, and land them in Delta tables on object storage.
//!
//! M3a: multi-table — one replication slot and one WAL stream feed N
//! tables (auto-discovered or configured), each with its own Delta table,
//! pre-image mirror, snapshot, and LSN watermarks. Records are routed by
//! relfilenode. See README for the milestone map.

mod pgwire;
mod schema;
mod sink;
mod snapshot;
mod txbuf;
mod wal;

use std::collections::{HashMap, HashSet};
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
    /// Tables to transcode; None = every ordinary table in `public`.
    tables: Option<Vec<String>>,
    slot: String,
    /// Delta tables land at `{lake}/{table}`.
    lake: String,
    s3_endpoint: String,
    s3_access_key: String,
    s3_secret_key: String,
    /// Flush to Delta once this many rows are pending (across tables)...
    flush_rows: usize,
    /// ...or this much time has passed since a batch's first pending row.
    flush_interval: Duration,
    /// Take an initial snapshot when a Delta table has no watermark yet.
    snapshot: bool,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Self {
        let mut tables: Vec<String> = std::env::args().skip(1).collect();
        if tables.is_empty() {
            if let Ok(list) = std::env::var("LTAP_TABLES") {
                tables = list.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            }
        }
        let pg_db = env_or("PG_DB", "app");
        Config {
            pg_host: env_or("PG_HOST", "localhost"),
            pg_port: env_or("PG_PORT", "5432").parse().expect("PG_PORT"),
            pg_user: env_or("PG_USER", "postgres"),
            pg_password: env_or("PG_PASSWORD", "postgres"),
            slot: env_or("LTAP_SLOT", &format!("ltap_{pg_db}")),
            lake: env_or("LTAP_LAKE", "s3://lake"),
            s3_endpoint: env_or("S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key: env_or("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key: env_or("S3_SECRET_KEY", "minioadmin"),
            flush_rows: env_or("LTAP_FLUSH_ROWS", "5000").parse().expect("LTAP_FLUSH_ROWS"),
            flush_interval: Duration::from_millis(
                env_or("LTAP_FLUSH_MS", "750").parse().expect("LTAP_FLUSH_MS"),
            ),
            snapshot: !matches!(env_or("LTAP_SNAPSHOT", "on").as_str(), "off" | "0" | "false"),
            tables: if tables.is_empty() { None } else { Some(tables) },
            pg_db,
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

/// Committed change rows waiting to be flushed as one Delta commit.
#[derive(Default)]
struct PendingBatch {
    rows: Vec<sink::EmitRow>,
    first_at: Option<Instant>,
    /// Highest PG commit LSN among the pending rows.
    max_commit_lsn: u64,
    /// Tie-break counter within equal commit LSNs.
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
type Mirror = HashMap<txbuf::Ctid, txbuf::RowVersion>;

/// Everything belonging to one transcoded table.
struct Table {
    desc: schema::TableDesc,
    sink: sink::DeltaSink,
    mirror: Mirror,
    /// Commits at or below this are already in this table's Delta log.
    dedupe_below: u64,
    pending: PendingBatch,
}

/// The whole transcoding state: N tables fed by one WAL stream.
struct Engine {
    tables: Vec<Table>,
    /// main-table relfilenode -> index into `tables`
    rel_to_table: HashMap<u32, usize>,
    /// relfilenodes of the tracked tables' toast relations
    toast_rels: HashSet<u32>,
    txbuf: txbuf::TxBuffer,
    toast: heap::ToastCache,
    /// LSN of the last commit record processed (restart floor when idle).
    last_commit_lsn: u64,
}

impl Engine {
    fn pending_total(&self) -> usize {
        self.tables.iter().map(|t| t.pending.rows.len()).sum()
    }

    fn oldest_batch_age(&self) -> Option<Duration> {
        self.tables.iter().filter_map(|t| t.pending.first_at).map(|t| t.elapsed()).max()
    }

    /// Safe global WAL resume position right now: everything at or after it
    /// that matters is either still in flight (txbuf) or being made durable
    /// by the current flush round.
    fn restart_lsn(&self) -> u64 {
        self.txbuf.oldest_first_lsn().unwrap_or(self.last_commit_lsn)
    }

    /// Flush EVERY table with pending rows (one round), stamping each with
    /// the same global restart LSN so any table's watermark is a safe
    /// stream-wide resume point after the round.
    async fn flush_all(&mut self, persisted_restart: &mut u64) -> Result<()> {
        if self.pending_total() == 0 {
            return Ok(());
        }
        let restart = self.restart_lsn();
        for t in &mut self.tables {
            if t.pending.rows.is_empty() {
                continue;
            }
            let commit_lsn = t.pending.max_commit_lsn;
            let n = t.pending.rows.len();
            let version = t.sink.append(&t.pending.rows, commit_lsn, restart).await?;
            tracing::info!(
                table = %t.desc.name,
                rows = n,
                commit_lsn = %pgwire::fmt_lsn(commit_lsn),
                restart_lsn = %pgwire::fmt_lsn(restart),
                delta_version = version,
                "flushed batch to Delta"
            );
            t.pending.rows.clear();
            t.pending.first_at = None;
        }
        *persisted_restart = restart;
        Ok(())
    }

    /// Pre-image lookup: an open transaction's own uncommitted version wins
    /// over the last committed one.
    fn preimage(&self, table: usize, ctid: txbuf::Ctid) -> Option<&txbuf::RowVersion> {
        self.txbuf.lookup(table, ctid).or_else(|| self.tables[table].mirror.get(&ctid))
    }

    fn handle_record(&mut self, lsn: u64, rec: &[u8]) -> Result<()> {
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
                let main = self.rel_to_table.get(&block0.rel.rel).copied();
                let is_toast = self.toast_rels.contains(&block0.rel.rel);
                if main.is_none() && !is_toast {
                    return Ok(()); // untracked relation (indexes, catalogs, other dbs)
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
                            Ok((valueid, seq, data)) => self.toast.add_chunk(record.xid, valueid, seq, data),
                            Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode toast chunk: {e}"),
                        }
                    }
                    heap::XLOG_HEAP_INSERT => {
                        let ti = main.unwrap();
                        let desc = &self.tables[ti].desc;
                        let decoded = if !block0.data.is_empty() {
                            heap::decode_insert_tuple(&block0.data, desc, &self.toast)
                        } else if let Some(img) = &block0.image {
                            // full_page_writes=on: the tuple lives in the image.
                            img.restore().and_then(|page| {
                                heap::decode_tuple_from_page(&page, heap::insert_offnum(&record.main_data)?, desc, &self.toast)
                            })
                        } else {
                            Err(anyhow::anyhow!("insert with neither data nor image"))
                        };
                        match (decoded, heap::insert_offnum(&record.main_data)) {
                            (Ok((row, attrs)), Ok(offnum)) => {
                                let ctid = (block0.blkno, offnum);
                                self.txbuf.add_op(record.xid, lsn, ti, txbuf::Op::Insert {
                                    ctid,
                                    ver: txbuf::RowVersion { row, attrs: Some(attrs) },
                                });
                            }
                            (Err(e), _) | (_, Err(e)) => {
                                if lsn <= self.tables[ti].dedupe_below {
                                    tracing::debug!(lsn = %pgwire::fmt_lsn(lsn), "replay: failed to decode insert: {e}");
                                } else {
                                    tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode insert: {e}");
                                }
                            }
                        }
                    }
                    heap::XLOG_HEAP_DELETE if main.is_some() => {
                        let ti = main.unwrap();
                        match heap::delete_offnum(&record.main_data) {
                            Ok(offnum) => {
                                let ctid = (block0.blkno, offnum);
                                let old_row = self.preimage(ti, ctid).map(|v| v.row.clone());
                                if old_row.is_none() && lsn > self.tables[ti].dedupe_below {
                                    tracing::warn!(
                                        lsn = %pgwire::fmt_lsn(lsn),
                                        table = %self.tables[ti].desc.name,
                                        ctid = ?ctid,
                                        "DELETE of a row not in the mirror — tombstone will be empty"
                                    );
                                }
                                self.txbuf.add_op(record.xid, lsn, ti, txbuf::Op::Delete { ctid, old_row });
                            }
                            Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to parse delete: {e}"),
                        }
                    }
                    heap::XLOG_HEAP_UPDATE | heap::XLOG_HEAP_HOT_UPDATE if main.is_some() => {
                        let ti = main.unwrap();
                        if let Err(e) = self.handle_update(lsn, &record, block0, ti) {
                            if lsn <= self.tables[ti].dedupe_below {
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
                let Some(ti) = self.rel_to_table.get(&block0.rel.rel).copied() else {
                    return Ok(());
                };
                let desc = &self.tables[ti].desc;
                let rows = if !block0.data.is_empty() {
                    heap::decode_multi_insert(&block0.data, &record.main_data, desc, &self.toast)
                } else if let Some(img) = &block0.image {
                    img.restore().and_then(|page| {
                        heap::multi_insert_offsets(&record.main_data, record.info)?
                            .iter()
                            .map(|&off| heap::decode_tuple_from_page(&page, off, desc, &self.toast))
                            .collect()
                    })
                } else {
                    Err(anyhow::anyhow!("multi-insert with neither data nor image"))
                };
                match (rows, heap::multi_insert_offsets(&record.main_data, record.info)) {
                    (Ok(rows), Ok(offsets)) if rows.len() == offsets.len() => {
                        for ((row, attrs), offnum) in rows.into_iter().zip(offsets) {
                            self.txbuf.add_op(record.xid, lsn, ti, txbuf::Op::Insert {
                                ctid: (block0.blkno, offnum),
                                ver: txbuf::RowVersion { row, attrs: Some(attrs) },
                            });
                        }
                    }
                    (Ok(rows), Ok(offsets)) => {
                        tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "multi-insert: {} rows vs {} offsets", rows.len(), offsets.len())
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        if lsn <= self.tables[ti].dedupe_below {
                            tracing::debug!(lsn = %pgwire::fmt_lsn(lsn), "replay: failed to decode multi-insert: {e}");
                        } else {
                            tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode multi-insert: {e}");
                        }
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
                        self.last_commit_lsn = lsn;
                        let ops = self.txbuf.commit(record.xid, &subxids);
                        self.toast.gc_xid(record.xid);
                        for sub in &subxids {
                            self.toast.gc_xid(*sub);
                        }
                        for pending_op in ops {
                            let t = &mut self.tables[pending_op.table];
                            if lsn <= t.dedupe_below {
                                // Replayed commit: already in this table's
                                // Delta log AND in the mirror rebuilt from it.
                                continue;
                            }
                            match pending_op.op {
                                txbuf::Op::Insert { ctid, ver } => {
                                    t.pending.emit(lsn, false, ctid, ver.row.clone());
                                    t.mirror.insert(ctid, ver);
                                }
                                txbuf::Op::Update { old_ctid, ctid, ver } => {
                                    t.mirror.remove(&old_ctid);
                                    t.pending.emit(lsn, false, ctid, ver.row.clone());
                                    t.mirror.insert(ctid, ver);
                                }
                                txbuf::Op::Delete { ctid, old_row } => {
                                    let row = old_row
                                        .or_else(|| t.mirror.get(&ctid).map(|v| v.row.clone()))
                                        .unwrap_or_else(|| vec![None; t.desc.cols.len()]);
                                    t.mirror.remove(&ctid);
                                    t.pending.emit(lsn, true, ctid, row);
                                }
                            }
                        }
                    }
                    heap::XLOG_XACT_ABORT => {
                        self.toast.gc_xid(record.xid);
                        for sub in &subxids {
                            self.toast.gc_xid(*sub);
                        }
                        let dropped = self.txbuf.abort(record.xid, &subxids);
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
        &mut self,
        lsn: u64,
        record: &wal::Record,
        block0: &wal::BlockRef,
        ti: usize,
    ) -> Result<()> {
        let info = heap::parse_update_main(&record.main_data)?;
        // Block 0 = the new tuple's page; block 1 (if present) = the old page.
        let old_blkno = record.blocks.iter().find(|b| b.id == 1).map(|b| b.blkno).unwrap_or(block0.blkno);
        let old_ctid = (old_blkno, info.old_offnum);
        let new_ctid = (block0.blkno, info.new_offnum);

        let (old_attrs, old_row) = match self.preimage(ti, old_ctid) {
            Some(v) => (v.attrs.clone(), Some(v.row.clone())),
            None => (None, None),
        };
        let desc = &self.tables[ti].desc;

        let (row, attrs) = if !block0.data.is_empty() {
            heap::decode_update_new_tuple(
                &block0.data,
                info.flags,
                old_attrs.as_deref(),
                old_row.as_ref(),
                desc,
                &self.toast,
            )?
        } else if let Some(img) = &block0.image {
            // FPI carries the complete new tuple; prefix/suffix never apply.
            let page = img.restore()?;
            heap::decode_tuple_from_page(&page, info.new_offnum, desc, &self.toast)?
        } else {
            anyhow::bail!("update with neither data nor image");
        };
        self.txbuf.add_op(record.xid, lsn, ti, txbuf::Op::Update {
            old_ctid,
            ctid: new_ctid,
            ver: txbuf::RowVersion { row, attrs: Some(attrs) },
        });
        Ok(())
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

    // 1. Discover the tables over a normal SQL connection.
    let descs = schema::discover_all(&cfg.sql_conninfo(), cfg.tables.as_deref()).await?;
    tracing::info!(
        tables = ?descs.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
        "discovered tables"
    );

    // 2. Open (or create) the Delta tables and read back where we left off.
    let mut tables = Vec::with_capacity(descs.len());
    for desc in descs {
        let uri = format!("{}/{}", cfg.lake.trim_end_matches('/'), desc.name);
        let sink = sink::DeltaSink::open_or_create(&uri, cfg.storage_options(), &desc).await?;
        tables.push(Table { desc, sink, mirror: Mirror::new(), dedupe_below: 0, pending: PendingBatch::default() });
    }

    // 3. Attach to the WAL stream (slot keeps WAL retained while we're down).
    let mut conn = pgwire::ReplConn::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_user).await?;
    let (timeline, flush_lsn) = conn.identify_system().await?;
    if conn.create_slot(&cfg.slot).await? {
        tracing::info!(slot = %cfg.slot, "created replication slot");
    }
    let slot_restart = conn.slot_restart_lsn(&cfg.slot).await?;

    // 4. Per table: resume from its watermark (rebuilding the pre-image
    //    mirror from its own change log) or take an initial snapshot.
    let mut restart_candidates: Vec<u64> = Vec::new();
    for t in &mut tables {
        let resume = t.sink.resume_state().await?;
        t.dedupe_below = resume.commit_lsn.unwrap_or(0);
        match resume.restart_lsn {
            Some(r) => {
                t.mirror = t.sink.load_mirror(&t.desc).await?;
                let raw = snapshot::read_raw_attrs_conn(&cfg.sql_conninfo(), &t.desc).await?;
                let mut refreshed = 0usize;
                for (ctid, ver) in t.mirror.iter_mut() {
                    if ver.attrs.is_none() {
                        if let Some(bytes) = raw.get(ctid) {
                            ver.attrs = Some(bytes.clone());
                            refreshed += 1;
                        }
                    }
                }
                tracing::info!(table = %t.desc.name, rows = t.mirror.len(), refreshed, "pre-image mirror rebuilt from Delta");
                restart_candidates.push(r);
            }
            None if cfg.snapshot => {
                let (cutover, rows, mut raw_attrs) = snapshot::take(&cfg.sql_conninfo(), &t.desc).await?;
                tracing::info!(
                    table = %t.desc.name,
                    rows = rows.len(),
                    cutover = %pgwire::fmt_lsn(cutover),
                    "initial snapshot taken (table was write-locked until here)"
                );
                for (ctid, row) in &rows {
                    let attrs = raw_attrs.remove(ctid).or_else(|| heap::encode_attrs(row, &t.desc));
                    t.mirror.insert(*ctid, txbuf::RowVersion { row: row.clone(), attrs });
                }
                if !rows.is_empty() {
                    let emits: Vec<sink::EmitRow> = rows
                        .into_iter()
                        .enumerate()
                        .map(|(i, (ctid, row))| sink::EmitRow { lsn: cutover, seq: i as u64, deleted: false, ctid, row })
                        .collect();
                    let version = t.sink.append(&emits, cutover, cutover).await?;
                    tracing::info!(table = %t.desc.name, delta_version = version, "initial snapshot committed to Delta");
                }
                t.dedupe_below = cutover;
                restart_candidates.push(cutover);
            }
            None => {} // no watermark, snapshot disabled: attach at the tip
        }
    }
    // Resume priority: earliest per-table need > pre-existing slot's
    // retained WAL (snapshot disabled) > "now".
    let resume_from = restart_candidates.iter().copied().min().or(slot_restart).unwrap_or(flush_lsn);
    let start_lsn = resume_from & !(wal::XLOG_PAGE_SIZE - 1); // page-align: reader syncs via page header
    tracing::info!(
        timeline,
        slot = %cfg.slot,
        start = %pgwire::fmt_lsn(start_lsn),
        tables = tables.len(),
        "starting physical replication"
    );
    conn.start_replication(&cfg.slot, start_lsn, timeline).await?;

    let mut engine = Engine {
        rel_to_table: tables.iter().enumerate().map(|(i, t)| (t.desc.rel_node, i)).collect(),
        toast_rels: tables.iter().filter_map(|t| t.desc.toast_rel_node).collect(),
        tables,
        txbuf: txbuf::TxBuffer::default(),
        toast: heap::ToastCache::default(),
        last_commit_lsn: resume_from,
    };

    let mut reader = wal::WalReader::new(start_lsn);
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
                        engine.handle_record(lsn, &rec)?;
                    }
                    if engine.pending_total() >= cfg.flush_rows {
                        engine.flush_all(&mut persisted_restart).await?;
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
                if engine.oldest_batch_age().is_some_and(|age| age >= cfg.flush_interval) {
                    engine.flush_all(&mut persisted_restart).await?;
                    conn.send_status(last_recv_lsn, persisted_restart).await?;
                }
            }
            _ = status_interval.tick() => {
                conn.send_status(last_recv_lsn, persisted_restart).await?;
            }
        }
    }
}
