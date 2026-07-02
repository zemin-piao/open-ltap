//! Table descriptor discovery via SQL ("catalog tracking lite").
//!
//! M0 limitation, on purpose: we read the schema once at startup over a
//! normal SQL connection and assume it doesn't change while streaming.
//! Real catalog tracking driven by the WAL itself (DDL mid-stream,
//! relfilenode changes from TRUNCATE/rewrite) is a later milestone — it is
//! also the single hardest part of the whole project.

use anyhow::{Context, Result, bail};
use tokio_postgres::NoTls;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Bool,
    Int4,
    Int8,
    Text,
}

impl PgType {
    fn from_oid(oid: u32) -> Result<Self> {
        Ok(match oid {
            16 => PgType::Bool,
            23 => PgType::Int4,
            20 => PgType::Int8,
            25 | 1043 => PgType::Text, // text, varchar
            other => bail!("unsupported column type oid {other} (M0 supports bool/int4/int8/text/varchar)"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    pub ty: PgType,
}

#[derive(Debug, Clone)]
pub struct TableDesc {
    pub name: String,
    pub db_oid: u32,
    pub rel_node: u32,
    pub cols: Vec<Col>,
}

pub async fn discover(conninfo: &str, table: &str) -> Result<TableDesc> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .context("connecting for catalog discovery")?;
    let handle = tokio::spawn(conn);

    let row = client
        .query_opt(
            "SELECT c.relfilenode,
                    (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = 'public' AND c.relkind = 'r'",
            &[&table],
        )
        .await?
        .with_context(|| format!("table '{table}' not found"))?;
    let rel_node: u32 = row.get(0);
    let db_oid: u32 = row.get(1);

    let attrs = client
        .query(
            "SELECT a.attname, a.atttypid
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = 'public'
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&table],
        )
        .await?;

    let mut cols = Vec::with_capacity(attrs.len());
    for a in &attrs {
        let name: String = a.get(0);
        let oid: u32 = a.get(1);
        cols.push(Col { name: name.clone(), ty: PgType::from_oid(oid).with_context(|| format!("column '{name}'"))? });
    }
    if cols.is_empty() {
        bail!("table '{table}' has no columns?");
    }
    handle.abort();
    Ok(TableDesc { name: table.to_string(), db_oid, rel_node, cols })
}
