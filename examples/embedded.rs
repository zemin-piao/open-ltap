//! Live harness for the embedded driver (V2a step (c) unit E1): connect to a
//! safekeeper exactly like the binary does, but feed `embed::run` through a
//! channel, pre-decoding XACT and SMGR records into [`SourceEvent`]s — the
//! same diet the pageserver fork's transcode tee provides (the interpreted
//! protocol never delivers raw XACT/SMGR records). Everything else ships as
//! `Raw`. The harness's diet is a superset of the real feed for records the
//! engine ignores anyway (CLOG, checkpoints, ...arrive here as Raw, not at
//! all there); identical for everything the engine consumes.
//!
//! The stream deliberately starts at the safekeeper's **current** flush LSN —
//! not our Delta watermark — because that is what the in-pageserver source
//! does (it replays from the walreceiver's position). A resumed engine whose
//! watermark is below that start exercises the driver's gap re-snapshot
//! policy, which is part of what this harness verifies.
//!
//!   PG_HOST=localhost PG_PORT=55433 PG_USER=cloud_admin ... \
//!     cargo run --example embedded

use anyhow::{Context, Result};
use open_ltap::embed::{self, SourceEvent};
use open_ltap::engine::Config;
use open_ltap::wal::{self, heap, rmgr};
use open_ltap::{pgwire, schema};
use tokio::sync::mpsc;

fn translate(lsn: u64, rec: &[u8]) -> Vec<SourceEvent> {
    let record = match wal::parse_record(rec) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(lsn = %pgwire::fmt_lsn(lsn), "harness: unparseable record: {e}");
            return Vec::new();
        }
    };
    match record.rmid {
        rmgr::XACT => {
            let op = record.info & heap::XLOG_XACT_OPMASK;
            let subxids = if matches!(op, heap::XLOG_XACT_COMMIT | heap::XLOG_XACT_ABORT) {
                heap::parse_xact_subxacts(record.info, &record.main_data).unwrap_or_default()
            } else {
                return Vec::new(); // prepared etc.: the interpreted feed's parity
            };
            match op {
                heap::XLOG_XACT_COMMIT => vec![SourceEvent::Commit { lsn, xid: record.xid, subxids }],
                heap::XLOG_XACT_ABORT => vec![SourceEvent::Abort { lsn, xid: record.xid, subxids }],
                _ => Vec::new(),
            }
        }
        rmgr::SMGR => {
            if record.info & 0xF0 == heap::XLOG_SMGR_CREATE {
                if let Ok(Some((db, _rel))) = heap::parse_smgr_create(&record.main_data) {
                    return vec![SourceEvent::SmgrCreate { xid: record.xid, db }];
                }
            }
            Vec::new()
        }
        _ => vec![SourceEvent::Raw { lsn, rec: rec.to_vec() }],
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "open_ltap=info,embedded=info,deltalake=warn".into()),
        )
        .init();
    let cfg = Config::from_env();

    let (tenant, tl) = match (cfg.sk_tenant.clone(), cfg.sk_timeline.clone()) {
        (Some(t), Some(tl)) => (t, tl),
        _ => schema::neon_ids(&cfg.sql_conninfo())
            .await
            .context("LTAP_TENANT_ID/LTAP_TIMELINE_ID unset and the compute has no neon GUCs")?,
    };
    let options = format!("tenant_id={tenant} timeline_id={tl}");
    let params = [("options", options.as_str()), ("application_name", "open-ltap-embedded")];
    let mut conn = pgwire::ReplConn::connect(
        &cfg.sk_host,
        cfg.sk_port,
        &cfg.pg_user,
        &params,
        cfg.sk_token.as_deref(),
    )
    .await?;
    let (_timeline, flush_lsn) = conn.identify_system().await?;

    // Pageserver-like: replay from the source's own position, page-aligned
    // for the reader.
    let start_lsn = flush_lsn & !(wal::XLOG_PAGE_SIZE - 1);
    tracing::info!(
        tenant = %tenant,
        timeline = %tl,
        start = %pgwire::fmt_lsn(start_lsn),
        "harness: streaming from safekeeper, feeding the embedded driver"
    );
    conn.start_replication_safekeeper(start_lsn).await?;

    let (tx, rx) = mpsc::channel::<SourceEvent>(8192);
    let mut driver = tokio::spawn(embed::run(cfg, rx));

    let mut reader = wal::WalReader::new(start_lsn);
    let mut last_recv = start_lsn;
    loop {
        tokio::select! {
            res = &mut driver => {
                return res.context("driver task")?.context("embedded driver failed");
            }
            msg = conn.next_msg() => match msg? {
                pgwire::ReplMsg::XLogData(x) => {
                    last_recv = x.wal_end.max(last_recv);
                    for (lsn, rec) in reader.feed(x.start_lsn, &x.data)? {
                        for ev in translate(lsn, &rec) {
                            if tx.send(ev).await.is_err() {
                                anyhow::bail!("driver hung up");
                            }
                        }
                    }
                }
                pgwire::ReplMsg::Keepalive { wal_end, reply_requested } => {
                    last_recv = wal_end.max(last_recv);
                    if tx.send(SourceEvent::Progress { lsn: last_recv }).await.is_err() {
                        anyhow::bail!("driver hung up");
                    }
                    if reply_requested {
                        conn.send_status(last_recv, last_recv).await?;
                    }
                }
            },
        }
    }
}
