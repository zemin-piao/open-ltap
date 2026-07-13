//! Live P2 validation: derive a table's catalog + decode its rows straight
//! off a Neon pageserver (GetPage@LSN, no image layer needed) and resolve
//! visibility for each row version through real CLOG, comparing against the
//! old `xmax == 0` spike heuristic this replaced. SQL is used only as an
//! oracle (row values, and to bootstrap oids/filenodes/LSN/tenant/timeline —
//! same "SQL only to cross-check" posture as `examples/getpage.rs`); the
//! catalog-from-pages + visibility path never uses it.
//!
//!   PG_PORT=55433 PG_USER=cloud_admin PG_DB=postgres \
//!     cargo run --example clogvis -- <table> <pg_xact dir>
//!
//! `docs/v2-scope.md` P2's scenarios this is built to catch (see
//! `docs/... ` state notes): a row inserted then the deleting transaction
//! aborted (old heuristic: xmax != 0 -> wrongly hidden); a row whose insert
//! itself aborted (old heuristic: xmax == 0 -> wrongly kept); a row holding
//! a plain-xid FOR UPDATE lock (old heuristic: xmax != 0 -> wrongly hidden).

use anyhow::{Context, Result};
use open_ltap::catalog::{Catalog, MappedRels, PageSource, PagestreamSource};
use open_ltap::clog::{self, FileClogSource};
use open_ltap::pgwire::{self, ReplConn};
use open_ltap::schema;
use open_ltap::wal::heap::{ToastCache, decode_tuple_from_page};
use tokio_postgres::NoTls;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let table = args.next().context("usage: clogvis <table> <pg_xact dir>")?;
    let pgxact = args.next().context("usage: clogvis <table> <pg_xact dir>")?;

    let conninfo = format!(
        "host={} port={} user={} password={} dbname={}",
        env_or("PG_HOST", "localhost"),
        env_or("PG_PORT", "55433"),
        env_or("PG_USER", "cloud_admin"),
        env_or("PG_PASSWORD", "cloud_admin"),
        env_or("PG_DB", "postgres"),
    );

    let (client, conn) = tokio_postgres::connect(&conninfo, NoTls).await.context("connecting for oids")?;
    tokio::spawn(conn);
    let row = client
        .query_one(
            "SELECT (SELECT oid FROM pg_database WHERE datname = current_database()),
                    CASE WHEN c.reltablespace = 0
                         THEN (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default')
                         ELSE c.reltablespace END,
                    pg_current_wal_flush_lsn()::text,
                    pg_relation_filenode(1259::regclass),  -- pg_class
                    pg_relation_filenode(1249::regclass),  -- pg_attribute
                    pg_relation_filenode(2610::regclass)   -- pg_index
             FROM pg_class c WHERE c.relname = $1 AND c.relkind = 'r'",
            &[&table],
        )
        .await
        .with_context(|| format!("looking up table {table}"))?;
    let (db, spc): (u32, u32) = (row.get(0), row.get(1));
    let lsn = pgwire::parse_lsn(row.get(2))?;
    let mapped = MappedRels { pg_class: row.get(3), pg_attribute: row.get(4), pg_index: Some(row.get(5)) };

    // Oracle: current-state row values straight from Postgres, for the
    // human comparing this program's visibility calls against ground truth.
    let sql_rows: Vec<(i32, String)> = client
        .query(&format!("SELECT id, note FROM \"{table}\" ORDER BY id"), &[])
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    let (tenant, timeline) = schema::neon_ids(&conninfo).await.context("neon tenant/timeline discovery")?;
    let ps_host = env_or("LTAP_PS_HOST", "localhost");
    let ps_port: u16 = env_or("LTAP_PS_PORT", "6400").parse()?;
    let token = std::env::var("LTAP_PS_TOKEN").ok();
    let mut ps =
        ReplConn::connect_pageserver(&ps_host, ps_port, "ltap", &tenant, &timeline, token.as_deref()).await?;
    println!("pageserver {ps_host}:{ps_port} tenant {tenant} timeline {timeline} @ {}", pgwire::fmt_lsn(lsn));

    let mut src = PagestreamSource { conn: &mut ps, spc, db, lsn };
    let mut clog_a = FileClogSource::new(&pgxact);
    let cat = Catalog::load(&mut src, &mut clog_a, &mapped).await?;
    let desc = cat.desc(&table)?;
    println!(
        "catalog-from-pages (real CLOG visibility): {} rel_node={} pk={:?} cols={:?}",
        desc.name,
        desc.rel_node,
        desc.pk,
        desc.cols.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // Now decode every LP_NORMAL tuple version on the table's own pages —
    // dead/aborted/locked versions included — and resolve each one both
    // ways: the real resolver, and the old xmax==0 heuristic it replaced.
    let toast = ToastCache::default();
    let mut clog_b = FileClogSource::new(&pgxact);
    let nblocks = src.rel_nblocks(desc.rel_node).await?;
    let (mut agree, mut disagree) = (0u32, 0u32);
    for blk in 0..nblocks {
        let page = src.get_page(desc.rel_node, blk).await?;
        let pd_lower = u16::from_le_bytes([page[12], page[13]]) as usize;
        let nlps = pd_lower.saturating_sub(24) / 4;
        for offnum in 1..=nlps as u16 {
            let lp_off = 24 + (offnum as usize - 1) * 4;
            let lp = u32::from_le_bytes(page[lp_off..lp_off + 4].try_into().unwrap());
            let (off, flags, len) = ((lp & 0x7FFF) as usize, (lp >> 15) & 3, (lp >> 17) as usize);
            if flags != 1 || off + len > page.len() || len < 23 {
                continue; // not LP_NORMAL
            }
            let tup = &page[off..off + len];
            let xmin = u32::from_le_bytes(tup[0..4].try_into().unwrap());
            let xmax = u32::from_le_bytes(tup[4..8].try_into().unwrap());
            let infomask = u16::from_le_bytes(tup[20..22].try_into().unwrap());

            let new_visible = clog::tuple_visible(&mut clog_b, xmin, xmax, infomask).await?;
            let old_visible = xmax == 0; // the P0-2 spike heuristic
            if let Ok((row, _)) = decode_tuple_from_page(&page, offnum, &desc, &toast) {
                let mark = if new_visible == old_visible { "     " } else { "DIFF!" };
                if new_visible == old_visible {
                    agree += 1;
                } else {
                    disagree += 1;
                }
                println!(
                    "{mark} blk {blk} off {offnum}: {row:?} xmin={xmin} xmax={xmax} infomask={infomask:#06x} \
                     clog_visible={new_visible} old_heuristic_visible={old_visible}"
                );
            }
        }
    }
    println!(
        "{agree} row versions where old heuristic agreed, {disagree} where it disagreed (fixed by P2)"
    );
    println!("SQL ground truth (current state): {sql_rows:?}");
    Ok(())
}
