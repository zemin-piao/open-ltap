//! Embedded driver: run the engine off an in-process event stream instead of
//! a pgwire connection — the V2a in-pageserver deployment shape. The source
//! (the pageserver fork's transcode tee, or the `embedded` example harness)
//! translates its records into [`SourceEvent`]s; this driver owns startup
//! (discovery, sinks, resume/snapshot), the flush cadence, remap checks, and
//! the two failure policies the embedded shape forces:
//!
//! - **Gap at stream start.** The source replays from wherever the pageserver
//!   walreceiver is (its `last_record_lsn`), not from our Delta watermark. A
//!   table whose watermark is below the first event's LSN may have missed
//!   commits in between → tombstone-all + re-snapshot (the idempotent M3b
//!   remap path). Conservative: an idle gap re-snapshots needlessly; the
//!   gauntlet will tell us if that needs refining.
//! - **Records lost mid-stream** ([`SourceEvent::Lost`], emitted by the source
//!   when the fail-open tee dropped records). In-flight transactions may be
//!   missing ops and any commit may have been missed: discard txbuf/toast
//!   state and re-snapshot every table. Drastic but correct; P4 says the tee
//!   must never backpressure ingest, so this is the price of lagging.

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::engine::{self, Config, Engine};
use crate::{pgwire, schema, serve};

/// One record-source event, pre-translated for the engine. LSNs are record
/// END LSNs (`next_record_lsn` in pageserver terms), matching what
/// `handle_record` receives from the pgwire path.
#[derive(Debug)]
pub enum SourceEvent {
    /// A complete raw WAL record (DML, TOAST chunks, catalog heap writes).
    Raw { lsn: u64, rec: Vec<u8> },
    /// Pre-decoded XLOG_XACT_COMMIT (the interpreted protocol never delivers
    /// the raw record).
    Commit { lsn: u64, xid: u32, subxids: Vec<u32> },
    /// Pre-decoded XLOG_XACT_ABORT.
    Abort { lsn: u64, xid: u32, subxids: Vec<u32> },
    /// Pre-decoded XLOG_SMGR_CREATE of a main fork.
    SmgrCreate { xid: u32, db: u32 },
    /// Stream position advanced without a record we consume (keepalive
    /// analogue): freshens `applied_lsn` for the tail endpoint.
    Progress { lsn: u64 },
    /// The source dropped `count` records since the last event (tee overflow).
    Lost { count: u64 },
}

impl SourceEvent {
    fn lsn(&self) -> Option<u64> {
        match self {
            SourceEvent::Raw { lsn, .. }
            | SourceEvent::Commit { lsn, .. }
            | SourceEvent::Abort { lsn, .. }
            | SourceEvent::Progress { lsn } => Some(*lsn),
            SourceEvent::SmgrCreate { .. } | SourceEvent::Lost { .. } => None,
        }
    }
}

/// Run the engine until `events` closes (source gone for good) or an
/// unrecoverable error. Startup mirrors `main.rs` steps 1–4 minus the wire:
/// no slot, no IDENTIFY_SYSTEM — Delta watermarks are the only resume
/// authority, and the stream starts wherever the source is.
pub async fn run(cfg: Config, mut events: mpsc::Receiver<SourceEvent>) -> Result<()> {
    let descs = schema::discover_all(&cfg.sql_conninfo(), cfg.tables.as_deref()).await?;
    tracing::info!(
        tables = ?descs.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
        "embedded driver: discovered tables"
    );
    let mut tables = engine::open_tables(&cfg, descs).await?;

    // Tenant/timeline for the GetPage oracle (in-pageserver this is a
    // self-connect to the page service). Soft: without it, mirror-only.
    let neon_tt = match (cfg.sk_tenant.clone(), cfg.sk_timeline.clone()) {
        (Some(t), Some(tl)) => Some((t, tl)),
        _ => schema::neon_ids(&cfg.sql_conninfo()).await.ok(),
    };

    let restart_candidates =
        engine::resume_tables(&cfg, &mut tables, neon_tt.is_some() && cfg.ps_enabled).await?;
    let resume_from = restart_candidates.iter().copied().min().unwrap_or(0);

    let tail: serve::SharedTail = std::sync::Arc::new(std::sync::RwLock::new(serve::TailStore::default()));
    if cfg.http_port != 0 {
        let t = tail.clone();
        let port = cfg.http_port;
        tokio::spawn(async move {
            if let Err(e) = serve::serve(t, port).await {
                tracing::error!("freshness endpoint failed: {e:#}");
            }
        });
    }
    let mut engine = engine::build_engine(&cfg, tables, tail, resume_from, neon_tt).await?;

    let mut persisted_restart = resume_from;
    let mut stream_start: Option<u64> = None;
    let mut flush_tick = tokio::time::interval(cfg.flush_interval.min(Duration::from_millis(250)));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            ev = events.recv() => {
                let Some(ev) = ev else {
                    // Source gone for good: make what we have durable and stop.
                    engine.flush_all(&mut persisted_restart).await?;
                    tracing::info!("event source closed; embedded driver exiting");
                    return Ok(());
                };
                if stream_start.is_none() {
                    if let Some(first) = ev.lsn() {
                        stream_start = Some(first);
                        gap_check(&mut engine, &cfg, first).await?;
                    }
                }
                match ev {
                    SourceEvent::Raw { lsn, rec } => {
                        engine.handle_record(lsn, &rec).await?;
                        engine.tail.write().unwrap().applied_lsn = lsn;
                    }
                    SourceEvent::Commit { lsn, xid, subxids } => {
                        engine.handle_commit(lsn, xid, &subxids);
                        engine.tail.write().unwrap().applied_lsn = lsn;
                    }
                    SourceEvent::Abort { lsn, xid, subxids } => {
                        engine.handle_abort(lsn, xid, &subxids);
                    }
                    SourceEvent::SmgrCreate { xid, db } => {
                        engine.handle_smgr_create(xid, db);
                    }
                    SourceEvent::Progress { lsn } => {
                        engine.tail.write().unwrap().applied_lsn = lsn;
                    }
                    SourceEvent::Lost { count } => {
                        tracing::warn!(count, "source lost records; discarding in-flight state and re-snapshotting all tables");
                        engine.txbuf = crate::txbuf::TxBuffer::default();
                        engine.toast = crate::wal::heap::ToastCache::default();
                        engine.smgr_suspects.clear();
                        resnapshot_all(&mut engine, &cfg).await?;
                    }
                }
                if let Some(ddl_lsn) = engine.remap_at.take() {
                    engine.remap_check(ddl_lsn, &cfg, &mut persisted_restart).await?;
                }
                if engine.pending_total() >= cfg.flush_rows {
                    engine.flush_all(&mut persisted_restart).await?;
                    engine.maybe_compact().await?;
                }
            }
            _ = flush_tick.tick() => {
                if engine.oldest_batch_age().is_some_and(|age| age >= cfg.flush_interval) {
                    engine.flush_all(&mut persisted_restart).await?;
                    engine.maybe_compact().await?;
                }
            }
        }
    }
}

/// Tables whose watermark is below the first event's LSN may have missed
/// commits between the watermark and the stream start: re-snapshot them.
/// Fresh tables (dedupe_below = a cutover taken just now) land above the
/// replayed stream start and are skipped naturally.
async fn gap_check(engine: &mut Engine, cfg: &Config, first_lsn: u64) -> Result<()> {
    for ti in 0..engine.tables.len() {
        let (node, below, name) = {
            let t = &engine.tables[ti];
            (t.desc.rel_node, t.dedupe_below, t.desc.name.clone())
        };
        if below > 0 && below < first_lsn {
            tracing::warn!(
                table = %name,
                watermark = %pgwire::fmt_lsn(below),
                stream_start = %pgwire::fmt_lsn(first_lsn),
                "watermark below stream start (possible gap) — re-snapshotting"
            );
            let t = &mut engine.tables[ti];
            let tomb = t.dedupe_below + 1;
            engine::remap_table(t, tomb, node, below, &cfg.sql_conninfo())
                .await
                .with_context(|| format!("gap re-snapshot of {name}"))?;
        }
    }
    Ok(())
}

async fn resnapshot_all(engine: &mut Engine, cfg: &Config) -> Result<()> {
    for ti in 0..engine.tables.len() {
        let (node, name) = {
            let t = &engine.tables[ti];
            (t.desc.rel_node, t.desc.name.clone())
        };
        let t = &mut engine.tables[ti];
        let tomb = t.dedupe_below + 1;
        let restart = t.dedupe_below;
        engine::remap_table(t, tomb, node, restart, &cfg.sql_conninfo())
            .await
            .with_context(|| format!("lost-records re-snapshot of {name}"))?;
    }
    Ok(())
}
