//! open-ltap: stream physical WAL out of Postgres, decode committed heap
//! changes, and land them in a Delta table on object storage.
//!
//! M0 vertical slice: single table, INSERT only, fixed schema discovered at
//! startup. See README for the milestone map.

mod pgwire;
mod schema;
mod sink;
mod txbuf;
mod wal;

use std::collections::HashMap;
use std::time::Duration;

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
    delta_uri: String,
    s3_endpoint: String,
    s3_access_key: String,
    s3_secret_key: String,
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
            delta_uri: env_or("DELTA_URI", &format!("s3://lake/{table}")),
            s3_endpoint: env_or("S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key: env_or("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key: env_or("S3_SECRET_KEY", "minioadmin"),
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

    // 2. Open (or create) the Delta table.
    let mut sink = sink::DeltaSink::open_or_create(&cfg.delta_uri, cfg.storage_options(), &desc).await?;

    // 3. Attach to the WAL stream.
    let mut conn = pgwire::ReplConn::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_user).await?;
    let (timeline, flush_lsn) = conn.identify_system().await?;
    let start_lsn = flush_lsn & !(wal::XLOG_PAGE_SIZE - 1); // page-align: reader syncs via page header
    tracing::info!(
        timeline,
        flush = %pgwire::fmt_lsn(flush_lsn),
        start = %pgwire::fmt_lsn(start_lsn),
        "starting physical replication"
    );
    conn.start_replication(start_lsn, timeline).await?;

    let mut reader = wal::WalReader::new(start_lsn);
    let mut txbuf = txbuf::TxBuffer::default();
    let mut last_recv_lsn = start_lsn;
    let mut warned_unsupported = false;
    let mut status_interval = tokio::time::interval(Duration::from_secs(10));
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = conn.next_msg() => match msg? {
                pgwire::ReplMsg::XLogData(x) => {
                    last_recv_lsn = x.wal_end.max(last_recv_lsn);
                    for (lsn, rec) in reader.feed(x.start_lsn, &x.data)? {
                        handle_record(lsn, &rec, &desc, &mut txbuf, &mut sink, &mut warned_unsupported).await?;
                    }
                }
                pgwire::ReplMsg::Keepalive { wal_end, reply_requested } => {
                    last_recv_lsn = wal_end.max(last_recv_lsn);
                    if reply_requested {
                        conn.send_status(last_recv_lsn).await?;
                    }
                }
            },
            _ = status_interval.tick() => {
                conn.send_status(last_recv_lsn).await?;
            }
        }
    }
}

async fn handle_record(
    lsn: u64,
    rec: &[u8],
    desc: &schema::TableDesc,
    txbuf: &mut txbuf::TxBuffer,
    sink: &mut sink::DeltaSink,
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
            if block0.rel.db != desc.db_oid || block0.rel.rel != desc.rel_node {
                return Ok(()); // some other relation (indexes, catalogs, other tables)
            }
            match op {
                heap::XLOG_HEAP_INSERT => {
                    if block0.data.is_empty() {
                        // Tuple data can live in the FPI instead; shouldn't
                        // happen with full_page_writes=off.
                        tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "insert without tuple data (FPI-only record?) — skipped");
                        return Ok(());
                    }
                    match heap::decode_insert_tuple(&block0.data, desc) {
                        Ok(row) => txbuf.add(record.xid, row),
                        Err(e) => tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "failed to decode insert: {e}"),
                    }
                }
                heap::XLOG_HEAP_DELETE | heap::XLOG_HEAP_UPDATE | heap::XLOG_HEAP_HOT_UPDATE => {
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
            // XLOG_HEAP2_MULTI_INSERT (COPY / multi-row insert) is milestone M1.
            if record.blocks.iter().any(|b| b.id == 0 && b.rel.db == desc.db_oid && b.rel.rel == desc.rel_node)
                && !*warned_unsupported
            {
                *warned_unsupported = true;
                tracing::warn!("multi-insert (COPY) on '{}' observed — not yet transcoded (M1)", desc.name);
            }
        }
        rmgr::XACT => {
            let op = record.info & heap::XLOG_XACT_OPMASK;
            match op {
                heap::XLOG_XACT_COMMIT => {
                    let rows = txbuf.commit(record.xid);
                    if !rows.is_empty() {
                        let n = rows.len();
                        let version = sink.append(&rows, lsn).await?;
                        tracing::info!(
                            xid = record.xid,
                            rows = n,
                            commit_lsn = %pgwire::fmt_lsn(lsn),
                            delta_version = version,
                            "committed transaction transcoded to Delta"
                        );
                    }
                }
                heap::XLOG_XACT_ABORT => {
                    let dropped = txbuf.abort(record.xid);
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
