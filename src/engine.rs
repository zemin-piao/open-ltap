//! The transcoding engine, as a library: N tables fed by one physical-WAL
//! record stream. The binary (`main.rs`) drives it from a pgwire replication
//! connection; any other embedder (the v2a pageserver fork) constructs
//! `Engine` the same way and feeds `handle_record(lsn, bytes)`.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::wal::{heap, rmgr};
use crate::{pgwire, schema, serve, sink, snapshot, txbuf, wal};

/// Where the physical WAL stream comes from. SQL (catalog, snapshots) always
/// goes to the Postgres endpoint (`PG_*`) — on Neon that's the compute node.
pub enum WalSource {
    /// A vanilla walsender: slot-backed retention, TIMELINE clause.
    Postgres,
    /// A Neon safekeeper: tenant/timeline named in the startup packet, no
    /// slots (Delta watermarks are the only resume authority).
    Safekeeper,
}

pub struct Config {
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_db: String,
    pub source: WalSource,
    pub sk_host: String,
    pub sk_port: u16,
    /// Hex tenant/timeline ids for the safekeeper; auto-discovered from the
    /// compute's neon.tenant_id/neon.timeline_id GUCs when unset.
    pub sk_tenant: Option<String>,
    pub sk_timeline: Option<String>,
    /// JWT for safekeepers with auth enabled (sent as a cleartext password).
    pub sk_token: Option<String>,
    /// Pageserver GetPage@LSN oracle (safekeeper source only): pre-image
    /// fallback when the mirror can't answer, replacing pageinspect.
    /// LTAP_PS=off disables; connect failures degrade to mirror-only.
    pub ps_enabled: bool,
    pub ps_host: String,
    pub ps_port: u16,
    pub ps_token: Option<String>,
    /// Tables to transcode; None = every ordinary table in `public`.
    pub tables: Option<Vec<String>>,
    pub slot: String,
    /// Delta tables land at `{lake}/{table}`.
    pub lake: String,
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    /// Flush to Delta once this many rows are pending (across tables)...
    pub flush_rows: usize,
    /// ...or this much time has passed since a batch's first pending row.
    pub flush_interval: Duration,
    /// Take an initial snapshot when a Delta table has no watermark yet.
    pub snapshot: bool,
    /// Port for the freshness endpoint (0 disables).
    pub http_port: u16,
    /// How long flushed rows stay in the served tail (gap-free merges).
    pub tail_retain: Duration,
    /// Compact a table's change log once this many rows have accumulated
    /// since its last compaction (0 disables).
    pub compact_rows: u64,
    /// Reclaim compaction-orphaned files older than this after compacting;
    /// None = never vacuum.
    pub vacuum_after: Option<Duration>,
    /// Ceiling on served-tail rows per table (flushed batches evict first).
    pub tail_max_rows: usize,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    pub fn from_env() -> Self {
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
            source: match env_or("LTAP_SOURCE", "postgres").as_str() {
                "postgres" => WalSource::Postgres,
                "safekeeper" => WalSource::Safekeeper,
                s => panic!("LTAP_SOURCE must be postgres or safekeeper, got {s}"),
            },
            sk_host: env_or("LTAP_SK_HOST", "localhost"),
            sk_port: env_or("LTAP_SK_PORT", "5454").parse().expect("LTAP_SK_PORT"),
            sk_tenant: std::env::var("LTAP_TENANT_ID").ok(),
            sk_timeline: std::env::var("LTAP_TIMELINE_ID").ok(),
            sk_token: std::env::var("LTAP_SK_TOKEN").ok(),
            ps_enabled: !matches!(env_or("LTAP_PS", "on").as_str(), "off" | "0" | "false"),
            ps_host: env_or("LTAP_PS_HOST", "localhost"),
            ps_port: env_or("LTAP_PS_PORT", "6400").parse().expect("LTAP_PS_PORT"),
            ps_token: std::env::var("LTAP_PS_TOKEN").ok(),
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

    pub fn sql_conninfo(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.pg_host, self.pg_port, self.pg_user, self.pg_password, self.pg_db
        )
    }

    pub fn storage_options(&self) -> HashMap<String, String> {
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
pub struct PendingBatch {
    pub rows: Vec<sink::EmitRow>,
    pub first_at: Option<Instant>,
    /// Highest PG commit LSN among the pending rows.
    pub max_commit_lsn: u64,
    /// Tie-break counter within equal commit LSNs.
    pub seq: u64,
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
pub type Mirror = HashMap<txbuf::Ctid, txbuf::RowVersion>;

/// Everything belonging to one transcoded table.
pub struct Table {
    pub desc: schema::TableDesc,
    pub sink: sink::DeltaSink,
    pub mirror: Mirror,
    /// Commits at or below this are already in this table's Delta log.
    pub dedupe_below: u64,
    pub pending: PendingBatch,
    /// Decode hit a schema-drift or similar inconsistency: converge by
    /// tombstoning + re-snapshotting at the next catalog check.
    pub needs_resnapshot: bool,
    /// Change-log rows written since the last compaction of this table.
    pub rows_since_compaction: u64,
}

/// The whole transcoding state: N tables fed by one WAL stream.
pub struct Engine {
    pub tables: Vec<Table>,
    /// main-table relfilenode -> index into `tables`
    pub rel_to_table: HashMap<u32, usize>,
    /// relfilenodes of the tracked tables' toast relations
    pub toast_rels: HashSet<u32>,
    pub txbuf: txbuf::TxBuffer,
    pub toast: heap::ToastCache,
    /// LSN of the last commit record processed (restart floor when idle).
    pub last_commit_lsn: u64,
    /// relfilenodes of pg_class / pg_attribute: writes to them signal DDL.
    pub catalog_rels: HashSet<u32>,
    /// Transactions that created a new main-fork relfilenode in our DB, or
    /// wrote to the catalogs: their commit might be DDL on a tracked table.
    pub smgr_suspects: HashSet<u32>,
    /// Set when a suspect committed: (commit LSN) — the main loop runs a
    /// catalog re-check (needs SQL, so it happens outside record handling).
    pub remap_at: Option<u64>,
    /// Tables that failed to attach (unsupported/conflicting): warn once.
    pub attach_failed: HashSet<String>,
    /// Freshness endpoint's view of the world.
    pub tail: serve::SharedTail,
    pub tail_retain: Duration,
    pub tail_max_rows: usize,
    pub compact_rows: u64,
    pub vacuum_after: Option<Duration>,
    pub db_oid: u32,
    /// GetPage@LSN oracle (safekeeper source): authoritative pre-image
    /// fallback when the mirror can't answer. None on the vanilla path.
    pub oracle: Option<Oracle>,
}

/// Lazy pagestream connection to the pageserver. Connect failures disable it
/// with one warning (the engine then degrades to mirror-only, as before);
/// per-request failures drop the connection and retry on the next need.
pub struct Oracle {
    pub host: String,
    pub port: u16,
    pub tenant: String,
    pub timeline: String,
    pub token: Option<String>,
    pub conn: Option<pgwire::ReplConn>,
    pub disabled: bool,
}

impl Oracle {
    /// The main-fork page of `rel` block `blkno` as of `lsn`. Page versions
    /// are keyed by record-END LSN, so passing an update record's start LSN
    /// yields the page state just before that record — the pre-image.
    async fn page(&mut self, rel: wal::RelTag, blkno: u32, lsn: u64) -> Option<Vec<u8>> {
        if self.disabled {
            return None;
        }
        if self.conn.is_none() {
            match pgwire::ReplConn::connect_pageserver(
                &self.host,
                self.port,
                "open-ltap",
                &self.tenant,
                &self.timeline,
                self.token.as_deref(),
            )
            .await
            {
                Ok(c) => {
                    tracing::info!(ps = %format!("{}:{}", self.host, self.port), "pageserver oracle connected");
                    self.conn = Some(c);
                }
                Err(e) => {
                    tracing::warn!(
                        "pageserver oracle unavailable (LTAP_PS=off silences this) — \
                         pre-image fallback disabled: {e:#}"
                    );
                    self.disabled = true;
                    return None;
                }
            }
        }
        let tag = pgwire::RelTag { spcnode: rel.spc, dbnode: rel.db, relnode: rel.rel, forknum: 0 };
        match self.conn.as_mut().unwrap().get_page(tag, blkno, lsn).await {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(blkno, lsn = %pgwire::fmt_lsn(lsn), "GetPage failed: {e:#}");
                self.conn = None; // stale sub-protocol state — reconnect next time
                None
            }
        }
    }
}

impl Engine {
    pub fn pending_total(&self) -> usize {
        self.tables.iter().map(|t| t.pending.rows.len()).sum()
    }

    pub fn oldest_batch_age(&self) -> Option<Duration> {
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
    pub async fn flush_all(&mut self, persisted_restart: &mut u64) -> Result<()> {
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
    pub async fn maybe_compact(&mut self) -> Result<()> {
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
    pub async fn remap_check(&mut self, ddl_lsn: u64, cfg: &Config, persisted_restart: &mut u64) -> Result<()> {
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

    pub async fn handle_record(&mut self, lsn: u64, rec: &[u8]) -> Result<()> {
        let record = match wal::parse_record(rec) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "skipping unparseable record: {e}");
                return Ok(());
            }
        };

        // Normalize Neon-rmgr DML onto the vanilla (rmid, op) space, keeping
        // the dialect so parsers read the shifted offsets.
        let Some((rmid, op, fmt)) = heap::normalize_dml(record.rmid, record.info) else {
            return Ok(()); // Neon LOCK etc.: no row change
        };

        match rmid {
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
                            heap::decode_toast_chunk_from_wal(&block0.data, fmt)
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
                            heap::decode_insert_tuple(&block0.data, desc, &self.toast, fmt)
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
                                let mut old_row = self.preimage(ti, ctid).map(|v| v.row.clone());
                                if old_row.is_none() && self.oracle.is_some() {
                                    // Oracle fallback: the deleted tuple as it
                                    // was on-page just before this record.
                                    if let Some(page) =
                                        self.oracle.as_mut().unwrap().page(block0.rel, block0.blkno, lsn).await
                                    {
                                        old_row = heap::decode_tuple_from_page(
                                            &page,
                                            offnum,
                                            &self.tables[ti].desc,
                                            &self.toast,
                                        )
                                        .ok()
                                        .map(|(r, _)| r);
                                    }
                                }
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
                        if let Err(e) = self.handle_update(lsn, &record, block0, ti, fmt).await {
                            self.note_decode_error(ti, lsn, "update", &e);
                        }
                    }
                    _ => {}
                }
            }
            rmgr::HEAP2 => {
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
                        heap::multi_insert_offsets(&record.main_data, record.info, fmt)?
                            .iter()
                            .map(|&off| heap::decode_tuple_from_page(&page, off, desc, &self.toast))
                            .collect()
                    })
                } else {
                    Err(anyhow::anyhow!("multi-insert with neither data nor image"))
                };
                match (rows, heap::multi_insert_offsets(&record.main_data, record.info, fmt)) {
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
    async fn handle_update(
        &mut self,
        lsn: u64,
        record: &wal::Record,
        block0: &wal::BlockRef,
        ti: usize,
        fmt: heap::HeapFmt,
    ) -> Result<()> {
        let info = heap::parse_update_main(&record.main_data, fmt)?;
        // Block 0 = the new tuple's page; block 1 (if present) = the old page.
        let old_block = record.blocks.iter().find(|b| b.id == 1).unwrap_or(block0);
        let old_ctid = (old_block.blkno, info.old_offnum);
        let new_ctid = (block0.blkno, info.new_offnum);

        let (mut old_attrs, mut old_row) = match self.preimage(ti, old_ctid) {
            Some(v) => (v.attrs.clone(), Some(v.row.clone())),
            None => (None, None),
        };
        // Oracle fallback: fetch the old tuple's page as it was just before
        // this record. The raw attr bytes (what prefix/suffix reconstruct
        // against) never need toast resolution; the decoded row (unchanged-
        // toast carry-over) may fail on out-of-line values whose chunks are
        // long gone — tolerated, decode below will error only if needed.
        if (old_attrs.is_none() || old_row.is_none()) && self.oracle.is_some() {
            if let Some(page) =
                self.oracle.as_mut().unwrap().page(old_block.rel, old_block.blkno, lsn).await
            {
                if old_attrs.is_none() {
                    match heap::raw_attrs_from_page(&page, info.old_offnum) {
                        Ok(a) => old_attrs = Some(a),
                        Err(e) => tracing::warn!(
                            lsn = %pgwire::fmt_lsn(lsn),
                            ctid = ?old_ctid,
                            "oracle page fetched but old tuple unreadable: {e}"
                        ),
                    }
                }
                if old_row.is_none() {
                    old_row = heap::decode_tuple_from_page(
                        &page,
                        info.old_offnum,
                        &self.tables[ti].desc,
                        &self.toast,
                    )
                    .ok()
                    .map(|(r, _)| r);
                }
            }
        }
        let desc = &self.tables[ti].desc;

        let (row, attrs) = if !block0.data.is_empty() {
            heap::decode_update_new_tuple(
                &block0.data,
                info.flags,
                old_attrs.as_deref(),
                old_row.as_ref(),
                desc,
                &self.toast,
                fmt,
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
pub async fn attach_table(name: &str, cfg: &Config) -> Result<Table> {
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
pub async fn remap_table(
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
