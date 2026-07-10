//! GetPage@LSN harness: fetch a table's pages from a Neon pageserver via the
//! pagestream_v3 sub-protocol and decode the tuples with the existing
//! page decoder — the M5 "pageserver oracle" proven end to end.
//!
//!   PG_PORT=55433 PG_USER=cloud_admin PG_DB=postgres \
//!     cargo run --example getpage -- t
//!
//! Env: PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DB point at the Neon compute
//! (SQL is used only to look up oids/filenode/LSN and to cross-check row
//! counts); LTAP_PS_HOST/LTAP_PS_PORT point at the pageserver (default
//! localhost:6400); LTAP_TENANT_ID/LTAP_TIMELINE_ID override the compute's
//! neon.* GUCs; LTAP_PS_TOKEN answers a password (JWT) request.

use anyhow::{Context, Result};
use open_ltap::pgwire::{self, RelTag, ReplConn};
use open_ltap::schema;
use open_ltap::wal::heap::{ToastCache, decode_tuple_from_page};
use tokio_postgres::NoTls;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let table = std::env::args().nth(1).unwrap_or_else(|| "t".into());
    let conninfo = format!(
        "host={} port={} user={} password={} dbname={}",
        env_or("PG_HOST", "localhost"),
        env_or("PG_PORT", "55433"),
        env_or("PG_USER", "cloud_admin"),
        env_or("PG_PASSWORD", "cloud_admin"),
        env_or("PG_DB", "postgres"),
    );

    // SQL side: physical identity of the table + a committed LSN to read at.
    let (client, conn) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .context("connecting to compute for oids")?;
    tokio::spawn(conn);
    let row = client
        .query_one(
            "SELECT (SELECT oid FROM pg_database WHERE datname = current_database()),
                    CASE WHEN c.reltablespace = 0
                         THEN (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')
                         ELSE c.reltablespace END,
                    pg_relation_filenode(c.oid),
                    pg_current_wal_flush_lsn()::text
             FROM pg_class c WHERE c.relname = $1 AND c.relkind = 'r'",
            &[&table],
        )
        .await
        .with_context(|| format!("looking up table {table}"))?;
    let rel = RelTag {
        spcnode: row.get::<_, u32>(1),
        dbnode: row.get::<_, u32>(0),
        relnode: row.get::<_, u32>(2),
        forknum: 0, // main
    };
    // LTAP_AT_LSN pins the read to a past LSN (time travel — the pre-image
    // use case); default is the compute's current flush LSN.
    let lsn = match std::env::var("LTAP_AT_LSN") {
        Ok(s) => pgwire::parse_lsn(&s)?,
        Err(_) => pgwire::parse_lsn(row.get(3))?,
    };
    let sql_count: i64 = client
        .query_one(&format!("SELECT count(*) FROM \"{table}\""), &[])
        .await?
        .get(0);
    let desc = schema::discover(&conninfo, &table).await?;

    let (tenant, timeline) = match (std::env::var("LTAP_TENANT_ID"), std::env::var("LTAP_TIMELINE_ID")) {
        (Ok(t), Ok(tl)) => (t, tl),
        _ => schema::neon_ids(&conninfo).await.context("neon tenant/timeline discovery")?,
    };

    let ps_host = env_or("LTAP_PS_HOST", "localhost");
    let ps_port: u16 = env_or("LTAP_PS_PORT", "6400").parse()?;
    let token = std::env::var("LTAP_PS_TOKEN").ok();
    println!(
        "table {table} rel {}/{}/{} @ {} — pageserver {ps_host}:{ps_port} tenant {tenant} timeline {timeline}",
        rel.spcnode, rel.dbnode, rel.relnode, pgwire::fmt_lsn(lsn)
    );

    let mut ps =
        ReplConn::connect_pageserver(&ps_host, ps_port, "ltap", &tenant, &timeline, token.as_deref())
            .await?;
    anyhow::ensure!(ps.rel_exists(rel, lsn).await?, "pageserver says relation does not exist");
    let nblocks = ps.rel_nblocks(rel, lsn).await?;
    println!("nblocks {nblocks}");

    // Decode every LP_NORMAL tuple on every page. Dead/aborted versions not
    // yet vacuumed away decode too — the harness reports them; visibility is
    // the engine's job, not the oracle's.
    let toast = ToastCache::default();
    let (mut decoded, mut skipped) = (0usize, 0usize);
    for blk in 0..nblocks {
        let page = ps.get_page(rel, blk, lsn).await?;
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        let nlps = pd_lower.saturating_sub(24) / 4;
        for offnum in 1..=nlps as u16 {
            match decode_tuple_from_page(&page, offnum, &desc, &toast) {
                Ok((row, _attrs)) => {
                    decoded += 1;
                    println!("({blk},{offnum}): {row:?}");
                }
                Err(e) if e.to_string().contains("not LP_NORMAL") => skipped += 1,
                Err(e) => return Err(e.context(format!("decoding ({blk},{offnum})"))),
            }
        }
    }
    println!(
        "decoded {decoded} tuple versions ({skipped} non-normal line pointers skipped); \
         SQL count(*) = {sql_count}"
    );
    Ok(())
}
