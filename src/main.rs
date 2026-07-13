//! open-ltap: stream physical WAL out of Postgres, decode committed heap
//! changes, and land them in Delta tables on object storage.
//!
//! M3a: multi-table — one replication slot and one WAL stream feed N
//! tables (auto-discovered or configured), each with its own Delta table,
//! pre-image mirror, snapshot, and LSN watermarks. Records are routed by
//! relfilenode. See README for the milestone map.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use open_ltap::engine::{self, Config, WalSource};
use open_ltap::wal;
use open_ltap::{pgwire, schema, serve};

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
    let mut tables = engine::open_tables(&cfg, descs).await?;

    // 3. Attach to the WAL stream. Vanilla: a slot keeps WAL retained while
    //    we're down. Safekeeper: no slots — Delta watermarks are the resume
    //    authority and the safekeeper keeps WAL per its own horizon.
    let mut neon_tt: Option<(String, String)> = None; // tenant/timeline, safekeeper source only
    let (mut conn, slot_restart) = match cfg.source {
        WalSource::Postgres => {
            let mut conn =
                pgwire::ReplConn::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_user, &[], None).await?;
            if conn.create_slot(&cfg.slot).await? {
                tracing::info!(slot = %cfg.slot, "created replication slot");
            }
            let slot_restart = conn.slot_restart_lsn(&cfg.slot).await?;
            (conn, slot_restart)
        }
        WalSource::Safekeeper => {
            let (tenant, tl) = match (cfg.sk_tenant.clone(), cfg.sk_timeline.clone()) {
                (Some(t), Some(tl)) => (t, tl),
                _ => schema::neon_ids(&cfg.sql_conninfo()).await.context(
                    "LTAP_TENANT_ID/LTAP_TIMELINE_ID unset and the compute has no neon GUCs",
                )?,
            };
            tracing::info!(tenant = %tenant, timeline = %tl, sk = %format!("{}:{}", cfg.sk_host, cfg.sk_port), "safekeeper source");
            neon_tt = Some((tenant.clone(), tl.clone()));
            // Safekeepers don't read tenant_id/timeline_id as top-level startup
            // params — they're packed into the standard libpq `options` param as
            // whitespace-separated `key=value` tokens (safekeeper/src/handler.rs
            // startup(), via pq_proto's options_raw()).
            let options = format!("tenant_id={tenant} timeline_id={tl}");
            let params = [("options", options.as_str()), ("application_name", "open-ltap")];
            let conn = pgwire::ReplConn::connect(
                &cfg.sk_host,
                cfg.sk_port,
                &cfg.pg_user,
                &params,
                cfg.sk_token.as_deref(),
            )
            .await?;
            (conn, None)
        }
    };
    let (timeline, flush_lsn) = conn.identify_system().await?;

    // 4. Per table: resume from its watermark (rebuilding the pre-image
    //    mirror from its own change log) or take an initial snapshot.
    let restart_candidates =
        engine::resume_tables(&cfg, &mut tables, neon_tt.is_some() && cfg.ps_enabled).await?;
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
    match cfg.source {
        WalSource::Postgres => conn.start_replication(&cfg.slot, start_lsn, timeline).await?,
        WalSource::Safekeeper => conn.start_replication_safekeeper(start_lsn).await?,
    }

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
    let mut engine =
        engine::build_engine(&cfg, tables, tail.clone(), resume_from, neon_tt.clone()).await?;

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
                        engine.handle_record(lsn, &rec).await?;
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
