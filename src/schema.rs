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
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Bytea,
    Uuid,
    /// Arbitrary-precision `numeric`/`decimal`: decoded to its exact decimal
    /// string (base-10000 digits on the wire), stored String-backed like uuid.
    Numeric,
    /// i32 days since 2000-01-01 on the wire; shifted to unix epoch on decode.
    Date,
    /// i64 microseconds since 2000-01-01; shifted to unix epoch on decode.
    Timestamp,
    TimestampTz,
}

impl PgType {
    pub fn from_oid(oid: u32) -> Result<Self> {
        Ok(match oid {
            16 => PgType::Bool,
            21 => PgType::Int2,
            23 => PgType::Int4,
            20 => PgType::Int8,
            700 => PgType::Float4,
            701 => PgType::Float8,
            25 | 1043 | 1042 => PgType::Text, // text, varchar, bpchar
            114 | 142 => PgType::Text, // json, xml — stored as plain text varlenas
            17 => PgType::Bytea,
            2950 => PgType::Uuid,
            1700 => PgType::Numeric, // numeric / decimal
            1082 => PgType::Date,
            1114 => PgType::Timestamp,
            1184 => PgType::TimestampTz,
            other => bail!(
                "unsupported column type oid {other} \
                 (supported: bool/int2/int4/int8/float4/float8/text/varchar/bytea/uuid/numeric/date/timestamp/timestamptz)"
            ),
        })
    }
}

#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    pub ty: PgType,
}

/// One physical attribute slot as tuples store them — including dropped
/// columns, which keep their width/alignment so tuples remain walkable.
#[derive(Debug, Clone)]
pub enum PhysCol {
    Live(Col),
    /// attisdropped: skip `attlen` bytes at `align` (attlen -1 = varlena).
    Dropped { attlen: i16, align: usize },
}

#[derive(Debug, Clone)]
pub struct TableDesc {
    pub name: String,
    /// The table's `pg_class` OID — its stable identity. Unlike `name` (which a
    /// rename changes) and `rel_node` (which a rewrite changes), the OID lives
    /// for the table's whole lifetime, so remap/discovery keys on it: a reused
    /// table name can't rebind a tracked slot to a different table's data.
    pub oid: u32,
    pub db_oid: u32,
    pub rel_node: u32,
    /// relfilenode of the table's toast relation, if it has one.
    pub toast_rel_node: Option<u32>,
    /// Live (non-dropped) columns, in attnum order — the logical row shape.
    pub cols: Vec<Col>,
    /// All physical attribute slots, in attnum order — the tuple layout.
    pub phys: Vec<PhysCol>,
    /// Any live column has a "fast default" (ADD COLUMN ... DEFAULT):
    /// rows written before it read as NULL from WAL, so the table needs a
    /// re-snapshot to materialize the defaults.
    pub has_fast_defaults: bool,
    /// Primary-key column names, in key order. Empty if the table has no
    /// primary key — change-log compaction needs a key and is skipped then.
    pub pk: Vec<String>,
}

/// Discover every table to transcode. `tables` = None means all ordinary
/// tables in the public schema. Tables with unsupported column types are
/// skipped with a warning rather than failing the run.
pub async fn discover_all(conninfo: &str, tables: Option<&[String]>) -> Result<Vec<TableDesc>> {
    let names: Vec<String> = match tables {
        Some(list) => list.to_vec(),
        None => {
            let (client, conn) = tokio_postgres::connect(conninfo, NoTls)
                .await
                .context("connecting for table discovery")?;
            let handle = tokio::spawn(conn);
            let rows = client
                .query(
                    "SELECT c.relname FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'public' AND c.relkind = 'r'
                     ORDER BY c.relname",
                    &[],
                )
                .await?;
            handle.abort();
            rows.iter().map(|r| r.get::<_, String>(0)).collect()
        }
    };

    let mut descs = Vec::with_capacity(names.len());
    for name in &names {
        match discover(conninfo, name).await {
            Ok(d) => descs.push(d),
            Err(e) if tables.is_none() => {
                tracing::warn!(table = %name, "skipping table: {e:#}");
            }
            Err(e) => return Err(e), // explicitly requested table must work
        }
    }
    if descs.is_empty() {
        bail!("no transcodable tables found");
    }
    Ok(descs)
}

/// The compute's (tenant_id, timeline_id), from the neon extension's GUCs.
pub async fn neon_ids(conninfo: &str) -> Result<(String, String)> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .context("connecting for neon tenant/timeline discovery")?;
    let handle = tokio::spawn(conn);
    let mut ids = Vec::with_capacity(2);
    for guc in ["neon.tenant_id", "neon.timeline_id"] {
        let row = client
            .query_one(&format!("SHOW {guc}") as &str, &[])
            .await
            .with_context(|| format!("SHOW {guc} (is this a Neon compute?)"))?;
        ids.push(row.get::<_, String>(0));
    }
    handle.abort();
    let timeline = ids.pop().unwrap();
    Ok((ids.pop().unwrap(), timeline))
}

/// All ordinary tables in the public schema, as `(pg_class oid, name)` — the
/// OID lets auto-attach key on table identity, so a renamed table isn't
/// re-attached under its new name and a new table reusing an old name is still
/// recognised as new.
pub async fn list_tables(conninfo: &str) -> Result<Vec<(u32, String)>> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
    let handle = tokio::spawn(conn);
    let rows = client
        .query(
            "SELECT c.oid, c.relname FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relkind = 'r'
             ORDER BY c.relname",
            &[],
        )
        .await?;
    handle.abort();
    Ok(rows.iter().map(|r| (r.get::<_, u32>(0), r.get::<_, String>(1))).collect())
}

/// relfilenodes of pg_class (1259) and pg_attribute (1249): heap writes to
/// these are the WAL-visible signature of DDL.
pub async fn catalog_filenodes(conninfo: &str) -> Result<Vec<u32>> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls).await?;
    let handle = tokio::spawn(conn);
    // pg_class and pg_attribute are themselves mapped relations (their
    // pg_class.relfilenode column reads 0; the real filenode lives in the
    // relmapper). pg_relation_filenode() resolves that indirection.
    let rows = client
        .query("SELECT pg_relation_filenode(oid) FROM pg_class WHERE oid IN (1259, 1249)", &[])
        .await?;
    handle.abort();
    Ok(rows.iter().map(|r| r.get::<_, u32>(0)).collect())
}

pub async fn discover(conninfo: &str, table: &str) -> Result<TableDesc> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .context("connecting for catalog discovery")?;
    let handle = tokio::spawn(conn);

    let row = client
        .query_opt(
            "SELECT c.relfilenode,
                    (SELECT d.oid FROM pg_database d WHERE d.datname = current_database()),
                    tc.relfilenode,
                    c.oid
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_class tc ON tc.oid = c.reltoastrelid
             WHERE c.relname = $1 AND n.nspname = 'public' AND c.relkind = 'r'",
            &[&table],
        )
        .await?
        .with_context(|| format!("table '{table}' not found"))?;
    let rel_node: u32 = row.get(0);
    let db_oid: u32 = row.get(1);
    let toast_rel_node: Option<u32> = row.get(2);
    let oid: u32 = row.get(3);

    let pk_rows = client
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             WHERE i.indrelid = format('public.%I', $1::text)::regclass AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)",
            &[&table],
        )
        .await?;
    let pk: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let attrs = client
        .query(
            "SELECT a.attname, a.atttypid, a.attisdropped, a.attlen::int4, a.attalign::text,
                    a.atthasmissing
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = 'public'
               AND a.attnum > 0
             ORDER BY a.attnum",
            &[&table],
        )
        .await?;

    let mut cols = Vec::new();
    let mut phys = Vec::with_capacity(attrs.len());
    let mut has_fast_defaults = false;
    for a in &attrs {
        let name: String = a.get(0);
        let oid: u32 = a.get(1);
        let dropped: bool = a.get(2);
        let attlen: i32 = a.get(3);
        let align: &str = a.get(4);
        let has_missing: bool = a.get(5);
        let align = match align {
            "c" => 1,
            "s" => 2,
            "i" => 4,
            _ => 8, // 'd'
        };
        if dropped {
            phys.push(PhysCol::Dropped { attlen: attlen as i16, align });
            continue;
        }
        let col = Col { name: name.clone(), ty: PgType::from_oid(oid).with_context(|| format!("column '{name}'"))? };
        has_fast_defaults |= has_missing;
        cols.push(col.clone());
        phys.push(PhysCol::Live(col));
    }
    if cols.is_empty() {
        bail!("table '{table}' has no columns?");
    }
    handle.abort();
    Ok(TableDesc { name: table.to_string(), oid, db_oid, rel_node, toast_rel_node, cols, phys, has_fast_defaults, pk })
}

/// Re-discover a tracked table by its stable `pg_class` OID. This is what
/// [`crate::engine::Engine::remap_check`] follows: because the OID survives
/// both rename and relfilenode rewrite, resolving through it (rather than the
/// possibly-reused name) means a slot is never rebound to a different table
/// that happens to have taken the old name. Resolves the OID's current name,
/// then reuses [`discover`] (relname is unique within the namespace, so the
/// second lookup can't land on a different table).
pub async fn discover_by_oid(conninfo: &str, oid: u32) -> Result<TableDesc> {
    let (client, conn) = tokio_postgres::connect(conninfo, NoTls)
        .await
        .context("connecting for catalog discovery")?;
    let handle = tokio::spawn(conn);
    let row = client
        .query_opt(
            "SELECT c.relname FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.oid = $1 AND n.nspname = 'public' AND c.relkind = 'r'",
            &[&oid],
        )
        .await?
        .with_context(|| format!("table oid {oid} not found (dropped)"))?;
    let name: String = row.get(0);
    handle.abort();
    discover(conninfo, &name).await
}
