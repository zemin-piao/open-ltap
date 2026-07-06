//! open-ltap: stream physical WAL out of Postgres, decode committed heap
//! changes, and land them in Delta tables on object storage.
//!
//! M3a: multi-table — one replication slot and one WAL stream feed N
//! tables (auto-discovered or configured), each with its own Delta table,
//! pre-image mirror, snapshot, and LSN watermarks. Records are routed by
//! relfilenode. See README for the milestone map.

mod pgwire;
mod schema;
mod serve;
mod sink;
mod snapshot;
mod txbuf;
mod wal;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
    /// Port for the freshness endpoint (0 disables).
    http_port: u16,
    /// How long flushed rows stay in the served tail (gap-free merges).
    tail_retain: Duration,
    /// Compact a table's change log once this many rows have accumulated
    /// since its last compaction (0 disables).
    compact_rows: u64,
    /// Reclaim compaction-orphaned files older than this after compacting;
    /// None = never vacuum.
    vacuum_after: Option<Duration>,
    /// Ceiling on served-tail rows per table (flushed batches evict first).
    tail_max_rows: usize,
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
            http_port: env_or("LTAP_HTTP_PORT", "8088").parse().expect("LTAP_HTTP_PORT"),
            tail_retain: Duration::from_millis(
                env_or("LTAP_TAIL_RETAIN_MS", "60000").parse().expect("LTAP_TAIL_RETAIN_MS"),
            ),
            compact_rows: env_or("LTAP_COMPACT_ROWS", "1000000").parse().expect("LTAP_COMPACT_ROWS"),
            vacuum_after: match env_or("LTAP_VACUUM_MINS", "1440").as_str() {
                "off" => None,
                v => Some(Duration::from_secs(v.parse::<u64>().expect("LTAP_VACUUM_MINS") * 60)),
            },
            tail_max_rows: env_or("LTAP_TAIL_MAX_ROWS", "100000").parse().expect("LTAP_TAIL_MAX_ROWS"),
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
    /// Decode hit a schema-drift or similar inconsistency: converge by
    /// tombstoning + re-snapshotting at the next catalog check.
    needs_resnapshot: bool,
    /// Change-log rows written since the last compaction of this table.
    rows_since_compaction: u64,
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
    /// relfilenodes of pg_class / pg_attribute: writes to them signal DDL.
    catalog_rels: HashSet<u32>,
    /// Transactions that created a new main-fork relfilenode in our DB, or
    /// wrote to the catalogs: their commit might be DDL on a tracked table.
    smgr_suspects: HashSet<u32>,
    /// Set when a suspect committed: (commit LSN) — the main loop runs a
    /// catalog re-check (needs SQL, so it happens outside record handling).
    remap_at: Option<u64>,
    /// Tables that failed to attach (unsupported/conflicting): warn once.
    attach_failed: HashSet<String>,
    /// Freshness endpoint's view of the world.
    tail: serve::SharedTail,
    tail_retain: Duration,
    tail_max_rows: usize,
    compact_rows: u64,
    vacuum_after: Option<Duration>,
    db_oid: u32,
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
            let version = t.sink.append(&t.pending.rows, commit_lsn, restart, t.desc.rel_node).await?;
            tracing::info!(
                table = %t.desc.name,
                rows = n,
                commit_lsn = %pgwire::fmt_lsn(commit_lsn),
                restart_lsn = %pgwire::fmt_lsn(restart),
                delta_version = version,
                "flushed batch to Delta"
            );
            t.rows_since_compaction += n as u64;
            t.pending.rows.clear();
            t.pending.first_at = None;
            {
                let mut tail = self.tail.write().unwrap();
                tail.mark_flushed(&t.desc.name, self.tail_retain);
                tail.enforce_cap(&t.desc.name, self.tail_max_rows);
            }
        }
        *persisted_restart = restart;
        Ok(())
    }

    /// Collapse any table whose change log has grown past the threshold.
    /// Inline in the single writer — no concurrent-writer coordination.
    async fn maybe_compact(&mut self) -> Result<()> {
        if self.compact_rows == 0 {
            return Ok(());
        }
        for ti in 0..self.tables.len() {
            if self.tables[ti].rows_since_compaction < self.compact_rows
                || self.tables[ti].desc.pk.is_empty()
            {
                continue;
            }
            let name = self.tables[ti].desc.name.clone();
            let desc = self.tables[ti].desc.clone();
            match self.tables[ti].sink.compact(&desc).await {
                Ok(Some((before, after))) => {
                    tracing::info!(table = %name, rows_before = before, rows_after = after, "compacted change log");
                    if let Some(retention) = self.vacuum_after {
                        match self.tables[ti].sink.vacuum(retention).await {
                            Ok(0) => {}
                            Ok(n) => tracing::info!(table = %name, files_deleted = n, "vacuumed orphaned files"),
                            Err(e) => tracing::warn!(table = %name, "vacuum failed: {e:#}"),
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(table = %name, "compaction failed: {e:#}"),
            }
            self.tables[ti].rows_since_compaction = 0;
        }
        Ok(())
    }

    /// Pre-image lookup: an open transaction's own uncommitted version wins
    /// over the last committed one.
    fn preimage(&self, table: usize, ctid: txbuf::Ctid) -> Option<&txbuf::RowVersion> {
        self.txbuf.lookup(table, ctid).or_else(|| self.tables[table].mirror.get(&ctid))
    }

    fn rebuild_routing(&mut self) {
        self.rel_to_table =
            self.tables.iter().enumerate().map(|(i, t)| (t.desc.rel_node, i)).collect();
        self.toast_rels = self.tables.iter().filter_map(|t| t.desc.toast_rel_node).collect();
    }

    /// A DDL-suspect transaction committed (relfilenode created or catalog
    /// heap written): re-read the catalog and reconcile every tracked table —
    /// relfilenode changes remap (TRUNCATE / VACUUM FULL / rewrites), column
    /// changes evolve the Delta schema in place (ADD/DROP COLUMN).
    async fn remap_check(&mut self, ddl_lsn: u64, cfg: &Config, persisted_restart: &mut u64) -> Result<()> {
        let conninfo = &cfg.sql_conninfo();
        // Everything pending was decoded and mapped under the old schema:
        // make it durable before anything changes shape.
        self.flush_all(persisted_restart).await?;
        // Catalogs themselves can be rewritten (VACUUM FULL pg_class).
        if let Ok(nodes) = schema::catalog_filenodes(conninfo).await {
            self.catalog_rels = nodes.into_iter().collect();
        }

        let mut changed = false;
        for ti in 0..self.tables.len() {
            let mut name = self.tables[ti].desc.name.clone();
            let old_node = self.tables[ti].desc.rel_node;
            if old_node == 0 {
                continue; // already detached
            }
            let mut fresh = schema::discover(conninfo, &name).await;
            if fresh.is_err() {
                // Renamed rather than dropped? The filenode survives a rename.
                if let Ok(Some(new_name)) = schema::table_name_by_filenode(conninfo, old_node).await {
                    tracing::warn!(
                        table = %name,
                        renamed_to = %new_name,
                        "table renamed — following it (the Delta table stays at its original path)"
                    );
                    self.tables[ti].desc.name = new_name.clone();
                    name = new_name;
                    fresh = schema::discover(conninfo, &name).await;
                }
            }
            let fresh = match fresh {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(table = %name, "table vanished from catalog — detaching: {e:#}");
                    self.tables[ti].desc.rel_node = 0; // routes nowhere (0 = invalid filenode)
                    self.tables[ti].desc.toast_rel_node = None;
                    changed = true;
                    continue;
                }
            };
            let filenode_changed = fresh.rel_node != old_node;
            let cols_changed = fresh
                .cols
                .iter()
                .map(|c| (&c.name, c.ty))
                .ne(self.tables[ti].desc.cols.iter().map(|c| (&c.name, c.ty)));
            let needs_resnapshot = self.tables[ti].needs_resnapshot
                || (cols_changed && fresh.cols.len() > self.tables[ti].desc.cols.len() && fresh.has_fast_defaults);
            if !filenode_changed && !cols_changed && !needs_resnapshot {
                self.tables[ti].desc = fresh; // phys/dropped bookkeeping may still differ
                continue;
            }
            changed = true;

            if cols_changed {
                if let Err(e) = self.tables[ti].sink.evolve(&fresh.cols) {
                    tracing::warn!(table = %name, "schema change Delta can't follow — detaching: {e:#}");
                    self.tables[ti].desc.rel_node = 0;
                    self.tables[ti].desc.toast_rel_node = None;
                    continue;
                }
                tracing::info!(table = %name, cols = fresh.cols.len(), "column set changed — Delta schema evolved");
                let old_cols = self.tables[ti].desc.cols.clone();
                reshape_rows(&mut self.tables[ti].mirror, &old_cols, &fresh.cols);
            }

            let restart = self.restart_lsn();
            self.tables[ti].desc = fresh;
            if filenode_changed || needs_resnapshot {
                tracing::info!(
                    table = %name,
                    old_filenode = old_node,
                    new_filenode = self.tables[ti].desc.rel_node,
                    resnapshot = needs_resnapshot,
                    "remapping (tombstone + re-snapshot)"
                );
                // When only the contents are suspect (schema drift / fast
                // defaults) the filenode is unchanged; stamp the tombstone
                // flush with filenode 0 so a crash mid-remap is detected at
                // startup and the remap re-runs.
                let stamp = if filenode_changed { old_node } else { 0 };
                remap_table(&mut self.tables[ti], ddl_lsn, stamp, restart, conninfo).await?;
                self.tables[ti].needs_resnapshot = false;
            }
        }
        // Auto mode: attach tables created since startup (their pre-attach
        // DML is covered by the snapshot cutover + dedupe, like at startup).
        if cfg.tables.is_none() {
            if let Ok(names) = schema::list_tables(conninfo).await {
                for n in names {
                    if self.tables.iter().any(|t| t.desc.name == n) || self.attach_failed.contains(&n) {
                        continue;
                    }
                    match attach_table(&n, cfg).await {
                        Ok(t) => {
                            tracing::info!(table = %n, "new table attached");
                            self.tables.push(t);
                            changed = true;
                        }
                        Err(e) => {
                            tracing::warn!(table = %n, "cannot attach new table (won't retry): {e:#}");
                            self.attach_failed.insert(n);
                        }
                    }
                }
            }
        }
        if changed {
            self.rebuild_routing();
        }
        Ok(())
    }

    /// Log a decode failure; schema drift additionally schedules a catalog
    /// check + re-snapshot so the table converges instead of diverging.
    fn note_decode_error(&mut self, ti: usize, lsn: u64, what: &str, e: &anyhow::Error) {
        if e.downcast_ref::<heap::SchemaDrift>().is_some() {
            tracing::info!(
                table = %self.tables[ti].desc.name,
                lsn = %pgwire::fmt_lsn(lsn),
                "schema drift detected — scheduling catalog check + re-snapshot"
            );
            self.tables[ti].needs_resnapshot = true;
            self.remap_at.get_or_insert(lsn);
            return;
        }
        if lsn <= self.tables[ti].dedupe_below {
            tracing::debug!(lsn = %pgwire::fmt_lsn(lsn), "replay: failed to decode {what}: {e}");
        } else {
            tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode {what}: {e}");
        }
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
            rmgr::SMGR => {
                if record.info & 0xF0 == heap::XLOG_SMGR_CREATE {
                    if let Ok(Some((db, _rel))) = heap::parse_smgr_create(&record.main_data) {
                        if db == self.db_oid && record.xid != 0 {
                            self.smgr_suspects.insert(record.xid);
                        }
                    }
                }
            }
            rmgr::HEAP => {
                let op = record.info & heap::XLOG_HEAP_OPMASK;
                let Some(block0) = record.blocks.iter().find(|b| b.id == 0) else {
                    return Ok(());
                };
                if block0.rel.db == self.db_oid && self.catalog_rels.contains(&block0.rel.rel) {
                    if record.xid != 0 {
                        self.smgr_suspects.insert(record.xid); // DDL in flight
                    }
                    return Ok(());
                }
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
                                self.note_decode_error(ti, lsn, "insert", &e);
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
                            self.note_decode_error(ti, lsn, "update", &e);
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
                if block0.rel.db == self.db_oid && self.catalog_rels.contains(&block0.rel.rel) {
                    if record.xid != 0 {
                        self.smgr_suspects.insert(record.xid);
                    }
                    return Ok(());
                }
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
                        self.note_decode_error(ti, lsn, "multi-insert", &e);
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
                        if self.smgr_suspects.remove(&record.xid)
                            || subxids.iter().any(|x| self.smgr_suspects.remove(x))
                        {
                            self.remap_at = Some(lsn);
                        }
                        let ops = self.txbuf.commit(record.xid, &subxids);
                        self.toast.gc_xid(record.xid);
                        for sub in &subxids {
                            self.toast.gc_xid(*sub);
                        }
                        let mut emitted: HashMap<usize, usize> = HashMap::new();
                        for pending_op in ops {
                            let t = &mut self.tables[pending_op.table];
                            if lsn <= t.dedupe_below {
                                // Replayed commit: already in this table's
                                // Delta log AND in the mirror rebuilt from it.
                                continue;
                            }
                            let start = *emitted
                                .entry(pending_op.table)
                                .or_insert_with(|| t.pending.rows.len());
                            let _ = start;
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
                        // Publish this commit's rows to the freshness tail.
                        for (ti, start) in emitted {
                            let t = &self.tables[ti];
                            if t.pending.rows.len() > start {
                                if let Ok(batch) = t.sink.make_batch(&t.pending.rows[start..]) {
                                    let mut tail = self.tail.write().unwrap();
                                    tail.push(&t.desc.name, batch);
                                    tail.enforce_cap(&t.desc.name, self.tail_max_rows);
                                }
                            }
                        }
                    }
                    heap::XLOG_XACT_ABORT => {
                        self.smgr_suspects.remove(&record.xid);
                        for sub in &subxids {
                            self.smgr_suspects.remove(sub);
                        }
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

/// Bring a table under management mid-stream: open its Delta table and
/// snapshot it at a fresh cutover. Anything the stream already passed for
/// this table is covered by the snapshot; anything after the cutover is
/// replayed against dedupe. (Same reasoning as the startup snapshot.)
async fn attach_table(name: &str, cfg: &Config) -> Result<Table> {
    let conninfo = cfg.sql_conninfo();
    let desc = schema::discover(&conninfo, name).await?;
    let uri = format!("{}/{}", cfg.lake.trim_end_matches('/'), desc.name);
    let sink = sink::DeltaSink::open_or_create(&uri, cfg.storage_options(), &desc).await?;
    let mut t = Table {
        desc,
        sink,
        mirror: Mirror::new(),
        dedupe_below: 0,
        pending: PendingBatch::default(),
        needs_resnapshot: false,
        rows_since_compaction: 0,
    };
    let resume = t.sink.resume_state().await?;
    t.dedupe_below = resume.commit_lsn.unwrap_or(0);
    let (cutover, rows, mut raw_attrs) = snapshot::take(&conninfo, &t.desc).await?;
    for (ctid, row) in &rows {
        let attrs = raw_attrs.remove(ctid).or_else(|| heap::encode_attrs(row, &t.desc));
        t.mirror.insert(*ctid, txbuf::RowVersion { row: row.clone(), attrs });
    }
    let emits: Vec<sink::EmitRow> = rows
        .into_iter()
        .enumerate()
        .map(|(i, (ctid, row))| sink::EmitRow { lsn: cutover, seq: i as u64, deleted: false, ctid, row })
        .collect();
    t.sink.append(&emits, cutover, cutover, t.desc.rel_node).await?;
    t.dedupe_below = cutover;
    Ok(t)
}

/// Re-shape mirror rows from an old live-column order to a new one,
/// matching by name (new columns read as NULL; dropped ones vanish).
fn reshape_rows(mirror: &mut Mirror, old_cols: &[schema::Col], new_cols: &[schema::Col]) {
    let map: Vec<Option<usize>> =
        new_cols.iter().map(|nc| old_cols.iter().position(|oc| oc.name == nc.name)).collect();
    for ver in mirror.values_mut() {
        ver.row = map.iter().map(|oi| oi.and_then(|i| ver.row.get(i).cloned().flatten())).collect();
    }
}

/// Tombstone everything the mirror holds (flushed under the OLD filenode's
/// watermark) and re-snapshot the table at a fresh cutover (committed under
/// the NEW filenode). Idempotent across crashes: the filenode watermark only
/// advances with the snapshot commit, so an interrupted remap re-runs.
/// `t.desc` must already be the fresh descriptor.
async fn remap_table(
    t: &mut Table,
    tombstone_lsn: u64,
    old_filenode: u32,
    restart_lsn: u64,
    conninfo: &str,
) -> Result<()> {
    let entries: Vec<_> = t.mirror.drain().collect();
    for (ctid, ver) in entries {
        t.pending.emit(tombstone_lsn, true, ctid, ver.row);
    }
    if !t.pending.rows.is_empty() {
        let commit_lsn = t.pending.max_commit_lsn;
        let n = t.pending.rows.len();
        t.sink.append(&t.pending.rows, commit_lsn, restart_lsn, old_filenode).await?;
        tracing::info!(table = %t.desc.name, tombstones = n, "flushed pre-remap state");
        t.pending.rows.clear();
        t.pending.first_at = None;
    }

    let (cutover, rows, mut raw_attrs) = snapshot::take(conninfo, &t.desc).await?;
    tracing::info!(
        table = %t.desc.name,
        rows = rows.len(),
        cutover = %pgwire::fmt_lsn(cutover),
        "post-remap snapshot taken"
    );
    for (ctid, row) in &rows {
        let attrs = raw_attrs.remove(ctid).or_else(|| heap::encode_attrs(row, &t.desc));
        t.mirror.insert(*ctid, txbuf::RowVersion { row: row.clone(), attrs });
    }
    let emits: Vec<sink::EmitRow> = rows
        .into_iter()
        .map(|(ctid, row)| {
            t.pending.seq += 1;
            sink::EmitRow { lsn: cutover, seq: t.pending.seq, deleted: false, ctid, row }
        })
        .collect();
    // Commit even when empty (plain TRUNCATE): the filenode watermark must
    // advance to the new node or restart would re-run the remap forever.
    t.sink.append(&emits, cutover, restart_lsn, t.desc.rel_node).await?;
    t.dedupe_below = cutover;
    Ok(())
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
        let sink = match sink::DeltaSink::open_or_create(&uri, cfg.storage_options(), &desc).await {
            Ok(s) => s,
            Err(e) if cfg.tables.is_none() => {
                tracing::warn!(table = %desc.name, "skipping table (Delta open failed): {e:#}");
                continue;
            }
            Err(e) => return Err(e),
        };
        tables.push(Table {
            desc,
            sink,
            mirror: Mirror::new(),
            dedupe_below: 0,
            pending: PendingBatch::default(),
            needs_resnapshot: false,
            rows_since_compaction: 0,
        });
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
                if t.sink.schema_added_columns() && t.desc.has_fast_defaults {
                    // A column with a fast default was added while we were
                    // down: WAL can't materialize it for old rows —
                    // re-snapshot instead.
                    tracing::warn!(table = %t.desc.name, "column with DEFAULT added while offline — re-snapshotting");
                    let tomb_lsn = t.dedupe_below + 1;
                    remap_table(t, tomb_lsn, 0, r, &cfg.sql_conninfo()).await?;
                } else if let Some(stored) = resume.filenode {
                    if stored != t.desc.rel_node {
                        // TRUNCATE / rewrite happened while we were down (or
                        // a remap was interrupted): tombstone the old state
                        // and re-snapshot. Idempotent — the filenode
                        // watermark only advances with the snapshot commit.
                        tracing::warn!(
                            table = %t.desc.name,
                            stored_filenode = stored,
                            live_filenode = t.desc.rel_node,
                            "relfilenode changed while offline — re-snapshotting"
                        );
                        let tomb_lsn = t.dedupe_below + 1;
                        remap_table(t, tomb_lsn, stored, r, &cfg.sql_conninfo()).await?;
                    }
                }
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
                    let version = t.sink.append(&emits, cutover, cutover, t.desc.rel_node).await?;
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

    let tail: serve::SharedTail = Arc::new(std::sync::RwLock::new(serve::TailStore::default()));
    if cfg.http_port != 0 {
        let t = tail.clone();
        let port = cfg.http_port;
        tokio::spawn(async move {
            if let Err(e) = serve::serve(t, port).await {
                tracing::error!("freshness endpoint failed: {e:#}");
            }
        });
    }
    let db_oid = tables[0].desc.db_oid;
    let catalog_rels: HashSet<u32> =
        schema::catalog_filenodes(&cfg.sql_conninfo()).await?.into_iter().collect();
    let mut engine = Engine {
        rel_to_table: tables.iter().enumerate().map(|(i, t)| (t.desc.rel_node, i)).collect(),
        toast_rels: tables.iter().filter_map(|t| t.desc.toast_rel_node).collect(),
        tables,
        txbuf: txbuf::TxBuffer::default(),
        toast: heap::ToastCache::default(),
        last_commit_lsn: resume_from,
        catalog_rels,
        smgr_suspects: HashSet::new(),
        remap_at: None,
        attach_failed: HashSet::new(),
        tail: tail.clone(),
        tail_retain: cfg.tail_retain,
        tail_max_rows: cfg.tail_max_rows,
        compact_rows: cfg.compact_rows,
        vacuum_after: cfg.vacuum_after,
        db_oid,
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
                    if let Some(ddl_lsn) = engine.remap_at.take() {
                        engine.remap_check(ddl_lsn, &cfg, &mut persisted_restart).await?;
                    }
                    engine.tail.write().unwrap().applied_lsn = last_recv_lsn;
                    if engine.pending_total() >= cfg.flush_rows {
                        engine.flush_all(&mut persisted_restart).await?;
                        engine.maybe_compact().await?;
                    }
                }
                pgwire::ReplMsg::Keepalive { wal_end, reply_requested } => {
                    last_recv_lsn = wal_end.max(last_recv_lsn);
                    engine.tail.write().unwrap().applied_lsn = last_recv_lsn;
                    if reply_requested {
                        conn.send_status(last_recv_lsn, persisted_restart).await?;
                    }
                }
            },
            _ = flush_tick.tick() => {
                if engine.oldest_batch_age().is_some_and(|age| age >= cfg.flush_interval) {
                    engine.flush_all(&mut persisted_restart).await?;
                    engine.maybe_compact().await?;
                    conn.send_status(last_recv_lsn, persisted_restart).await?;
                }
            }
            _ = status_interval.tick() => {
                conn.send_status(last_recv_lsn, persisted_restart).await?;
            }
        }
    }
}
