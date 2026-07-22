//! Delta Lake sink: create-if-absent, then append batches of committed
//! changes as an append-only change log. Every change row carries:
//!   `_ltap_lsn`     — its transaction's commit LSN
//!   `_ltap_seq`     — tie-break ordering within one commit
//!   `_ltap_deleted` — tombstone flag (UPDATEs append the new version;
//!                     DELETEs append the old row with this set)
//!   `_ltap_ctid`    — the row's physical address, used to rebuild the
//!                     in-memory pre-image mirror after a restart
//! Current state = latest (_ltap_lsn, _ltap_seq) per key, minus tombstones:
//!   SELECT * FROM delta_scan(...) QUALIFY row_number() OVER
//!     (PARTITION BY <pk> ORDER BY _ltap_lsn DESC, _ltap_seq DESC) = 1
//!     AND NOT _ltap_deleted
//!
//! Exactly-once across restarts: every Delta commit also records two
//! app-level `txn` actions (Delta's idempotent-streaming mechanism):
//!   - `open-ltap.commit`  = highest PG commit LSN contained in the table
//!   - `open-ltap.restart` = WAL position to resume reading from (early
//!     enough to replay any transaction still in flight at commit time)
//! On startup we read them back and (a) restart the stream at `restart`,
//! (b) drop replayed transactions whose commit LSN <= `commit`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use deltalake::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema, TimeUnit};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{
    Action, DataType as DeltaType, MetadataExt as _, PrimitiveType, StructField, StructType,
    Transaction,
};
use deltalake::writer::WriteMode;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::logstore::object_store::ObjectStoreExt as _;
use url::Url;

use crate::schema::{Col, PgType, TableDesc};
use crate::txbuf::{self, Ctid, RowVersion};
use crate::wal::heap::{self, Row, Value};

pub const LSN_COL: &str = "_ltap_lsn";
pub const SEQ_COL: &str = "_ltap_seq";
pub const DELETED_COL: &str = "_ltap_deleted";
pub const CTID_COL: &str = "_ltap_ctid";
const TXN_COMMIT: &str = "open-ltap.commit";
const TXN_RESTART: &str = "open-ltap.restart";
const TXN_FILENODE: &str = "open-ltap.filenode";

/// One change-log entry bound for the lake.
pub struct EmitRow {
    pub lsn: u64,
    pub seq: u64,
    pub deleted: bool,
    pub ctid: Ctid,
    pub row: Row,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ResumeState {
    /// Highest PG commit LSN already in the table (dedupe watermark).
    pub commit_lsn: Option<u64>,
    /// WAL position to resume reading from.
    pub restart_lsn: Option<u64>,
    /// relfilenode the table state corresponds to — a mismatch with the
    /// live catalog at startup means a TRUNCATE/rewrite happened while we
    /// were down (or mid-remap) and the table must be re-snapshotted.
    pub filenode: Option<u32>,
}

/// A column of the Delta table: its type, and where its values come from in
/// emitted rows (None = dropped in Postgres, kept in Delta receiving NULLs).
struct DeltaCol {
    col: Col,
    live: Option<usize>,
}

pub struct DeltaSink {
    table: DeltaTable,
    arrow_schema: Arc<ArrowSchema>,
    delta_cols: Vec<DeltaCol>,
    /// Delta table schema needs a Metadata action on the next commit.
    schema_dirty: bool,
}

/// Reverse of `delta_type`: recover our type tag from an existing Delta
/// table's schema (uuid round-trips as Text, which is how we store it).
fn pg_from_delta(dt: &DeltaType) -> Option<PgType> {
    match dt {
        DeltaType::Primitive(p) => Some(match p {
            PrimitiveType::Boolean => PgType::Bool,
            PrimitiveType::Short => PgType::Int2,
            PrimitiveType::Integer => PgType::Int4,
            PrimitiveType::Long => PgType::Int8,
            PrimitiveType::Float => PgType::Float4,
            PrimitiveType::Double => PgType::Float8,
            PrimitiveType::String => PgType::Text,
            PrimitiveType::Binary => PgType::Bytea,
            PrimitiveType::Date => PgType::Date,
            PrimitiveType::TimestampNtz => PgType::Timestamp,
            PrimitiveType::Timestamp => PgType::TimestampTz,
            _ => return None,
        }),
        _ => None,
    }
}

fn build_arrow_schema(delta_cols: &[DeltaCol]) -> Arc<ArrowSchema> {
    let fields: Vec<Field> = delta_cols
        .iter()
        .map(|dc| Field::new(&dc.col.name, arrow_type(dc.col.ty), true))
        .chain([
            Field::new(LSN_COL, ArrowType::Int64, false),
            Field::new(SEQ_COL, ArrowType::Int64, false),
            Field::new(DELETED_COL, ArrowType::Boolean, false),
            Field::new(CTID_COL, ArrowType::Int64, false),
        ])
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

fn delta_type(ty: PgType) -> DeltaType {
    DeltaType::Primitive(match ty {
        PgType::Bool => PrimitiveType::Boolean,
        PgType::Int2 => PrimitiveType::Short,
        PgType::Int4 => PrimitiveType::Integer,
        PgType::Int8 => PrimitiveType::Long,
        PgType::Float4 => PrimitiveType::Float,
        PgType::Float8 => PrimitiveType::Double,
        PgType::Text | PgType::Uuid | PgType::Numeric => PrimitiveType::String,
        PgType::Bytea => PrimitiveType::Binary,
        PgType::Date => PrimitiveType::Date,
        PgType::Timestamp => PrimitiveType::TimestampNtz,
        PgType::TimestampTz => PrimitiveType::Timestamp,
    })
}

fn arrow_type(ty: PgType) -> ArrowType {
    match ty {
        PgType::Bool => ArrowType::Boolean,
        PgType::Int2 => ArrowType::Int16,
        PgType::Int4 => ArrowType::Int32,
        PgType::Int8 => ArrowType::Int64,
        PgType::Float4 => ArrowType::Float32,
        PgType::Float8 => ArrowType::Float64,
        PgType::Text | PgType::Uuid | PgType::Numeric => ArrowType::Utf8,
        PgType::Bytea => ArrowType::Binary,
        PgType::Date => ArrowType::Date32,
        PgType::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
        PgType::TimestampTz => ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    }
}

impl DeltaSink {
    pub async fn open_or_create(
        uri: &str,
        storage_options: HashMap<String, String>,
        desc: &TableDesc,
    ) -> Result<Self> {
        deltalake::aws::register_handlers(None);

        let url = Url::parse(uri).context("parsing Delta table URI")?;
        let table = match deltalake::open_table_with_storage_options(url, storage_options.clone())
            .await
        {
            Ok(t) => {
                tracing::info!(uri, version = ?t.version(), "opened existing Delta table");
                t
            }
            Err(_) => {
                let delta_fields: Vec<StructField> = desc
                    .cols
                    .iter()
                    .map(|c| StructField::new(c.name.clone(), delta_type(c.ty), true))
                    .chain([
                        StructField::new(LSN_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                        StructField::new(SEQ_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                        StructField::new(DELETED_COL.to_string(), DeltaType::Primitive(PrimitiveType::Boolean), false),
                        StructField::new(CTID_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                    ])
                    .collect();
                let t = CreateBuilder::new()
                    .with_location(uri)
                    .with_storage_options(storage_options)
                    .with_table_name(desc.name.clone())
                    .with_comment("open-ltap transcoded table")
                    .with_columns(delta_fields)
                    .await
                    .context("creating Delta table")?;
                tracing::info!(uri, "created new Delta table");
                t
            }
        };

        // Delta columns = the table's existing user columns (retired ones —
        // dropped in PG while we were down — keep receiving NULLs), plus any
        // live columns Delta doesn't have yet.
        let mut delta_cols: Vec<DeltaCol> = Vec::new();
        if let Ok(snapshot) = table.snapshot() {
            for field in snapshot.schema().fields() {
                let name = field.name().to_string();
                if name.starts_with("_ltap_") {
                    continue;
                }
                let ty = pg_from_delta(field.data_type())
                    .ok_or_else(|| anyhow::anyhow!("Delta column '{name}' has an unsupported type"))?;
                let live = desc.cols.iter().position(|c| c.name == name);
                if let Some(li) = live {
                    // Compare by Delta representation, not the exact PgType: uuid
                    // and numeric both store as String, and a String column read
                    // back can't be told apart from Text — so re-opening a uuid
                    // or numeric table must not read as an incompatible change.
                    if delta_type(desc.cols[li].ty) != delta_type(ty) {
                        anyhow::bail!(
                            "column '{name}' changed type (Delta {ty:?} vs PG {:?}) — \
                             drop the Delta table to re-snapshot",
                            desc.cols[li].ty
                        );
                    }
                }
                delta_cols.push(DeltaCol { col: Col { name, ty }, live });
            }
        }
        let mut schema_dirty = false;
        for (li, c) in desc.cols.iter().enumerate() {
            if !delta_cols.iter().any(|dc| dc.col.name == c.name) {
                delta_cols.push(DeltaCol { col: c.clone(), live: Some(li) });
                schema_dirty = true; // column added while we were down
            }
        }
        let arrow_schema = build_arrow_schema(&delta_cols);
        Ok(DeltaSink { table, arrow_schema, delta_cols, schema_dirty })
    }

    /// The open step found live columns Delta didn't know yet (added while
    /// we were down) — relevant because fast defaults then need a re-snapshot.
    pub fn schema_added_columns(&self) -> bool {
        self.schema_dirty
    }

    /// Adjust to a new live column set (ADD/DROP COLUMN). Additive by name:
    /// new columns join the Delta schema; columns dropped in PG stay and
    /// receive NULLs. A type change on an existing name is an error.
    pub fn evolve(&mut self, cols: &[Col]) -> Result<()> {
        for dc in &mut self.delta_cols {
            dc.live = cols.iter().position(|c| c.name == dc.col.name);
            if let Some(li) = dc.live {
                if delta_type(cols[li].ty) != delta_type(dc.col.ty) {
                    anyhow::bail!(
                        "column '{}' changed type ({:?} -> {:?})",
                        dc.col.name,
                        dc.col.ty,
                        cols[li].ty
                    );
                }
            }
        }
        for (li, c) in cols.iter().enumerate() {
            if !self.delta_cols.iter().any(|dc| dc.col.name == c.name) {
                tracing::info!(column = %c.name, "adding column to Delta schema");
                self.delta_cols.push(DeltaCol { col: c.clone(), live: Some(li) });
                self.schema_dirty = true;
            }
        }
        self.arrow_schema = build_arrow_schema(&self.delta_cols);
        Ok(())
    }

    /// Read back the LSN watermarks persisted with the last Delta commit.
    pub async fn resume_state(&self) -> Result<ResumeState> {
        let snapshot = match self.table.snapshot() {
            Ok(s) => s,
            Err(_) => return Ok(ResumeState::default()), // fresh table, no state yet
        };
        let log_store = self.table.log_store();
        let commit_lsn = snapshot
            .transaction_version(log_store.as_ref(), TXN_COMMIT)
            .await?
            .map(|v| v as u64);
        let restart_lsn = snapshot
            .transaction_version(log_store.as_ref(), TXN_RESTART)
            .await?
            .map(|v| v as u64);
        let filenode = snapshot
            .transaction_version(log_store.as_ref(), TXN_FILENODE)
            .await?
            .map(|v| v as u32);
        Ok(ResumeState { commit_lsn, restart_lsn, filenode })
    }

    /// Build an Arrow batch in this table's Delta shape from emitted rows.
    /// Also used by the freshness endpoint to serve the in-memory tail.
    pub fn make_batch(&self, rows: &[EmitRow]) -> Result<RecordBatch> {
        // Pull a live column out of every row as Option<T>, tolerating rows
        // shaped under an older schema (short rows read as NULL).
        macro_rules! col_vals {
            ($li:expr, $variant:ident) => {
                rows.iter()
                    .map(|e| match e.row.get($li).and_then(|v| v.as_ref()) {
                        Some(Value::$variant(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.delta_cols.len() + 4);
        for dc in &self.delta_cols {
            let Some(li) = dc.live else {
                // Dropped in Postgres, retained in Delta: NULLs.
                arrays.push(deltalake::arrow::array::new_null_array(
                    &arrow_type(dc.col.ty),
                    rows.len(),
                ));
                continue;
            };
            let array: ArrayRef = match dc.col.ty {
                PgType::Bool => Arc::new(BooleanArray::from(col_vals!(li, Bool))),
                PgType::Int2 => Arc::new(Int16Array::from(col_vals!(li, I16))),
                PgType::Int4 => Arc::new(Int32Array::from(col_vals!(li, I32))),
                PgType::Int8 => Arc::new(Int64Array::from(col_vals!(li, I64))),
                PgType::Float4 => Arc::new(Float32Array::from(col_vals!(li, F32))),
                PgType::Float8 => Arc::new(Float64Array::from(col_vals!(li, F64))),
                PgType::Text | PgType::Uuid | PgType::Numeric => {
                    Arc::new(StringArray::from(col_vals!(li, Text)))
                }
                PgType::Bytea => Arc::new(BinaryArray::from(
                    col_vals!(li, Bytes).iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
                )),
                PgType::Date => Arc::new(Date32Array::from(col_vals!(li, I32))),
                PgType::Timestamp => Arc::new(TimestampMicrosecondArray::from(col_vals!(li, I64))),
                PgType::TimestampTz => Arc::new(
                    TimestampMicrosecondArray::from(col_vals!(li, I64)).with_timezone("UTC"),
                ),
            };
            arrays.push(array);
        }
        arrays.push(Arc::new(Int64Array::from(
            rows.iter().map(|e| e.lsn as i64).collect::<Vec<_>>(),
        )));
        arrays.push(Arc::new(Int64Array::from(
            rows.iter().map(|e| e.seq as i64).collect::<Vec<_>>(),
        )));
        arrays.push(Arc::new(BooleanArray::from(
            rows.iter().map(|e| e.deleted).collect::<Vec<_>>(),
        )));
        arrays.push(Arc::new(Int64Array::from(
            rows.iter().map(|e| txbuf::pack_ctid(e.ctid)).collect::<Vec<_>>(),
        )));
        Ok(RecordBatch::try_new(self.arrow_schema.clone(), arrays)?)
    }

    /// Append a batch of committed rows (possibly spanning many PG commits)
    /// as ONE Delta commit, carrying the new LSN watermarks atomically.
    pub async fn append(
        &mut self,
        rows: &[EmitRow],
        commit_lsn: u64,
        restart_lsn: u64,
        filenode: u32,
    ) -> Result<i64> {
        let batch = self.make_batch(rows)?;

        // Schema evolution must be its own commit BEFORE the data is written:
        // RecordBatchWriter's MergeSchema places new columns after the meta
        // columns while the underlying parquet writer keeps the old schema,
        // silently dropping the new column's values from the first batch.
        if self.schema_dirty {
            let fields: Vec<StructField> = self
                .delta_cols
                .iter()
                .map(|dc| StructField::new(dc.col.name.clone(), delta_type(dc.col.ty), true))
                .chain([
                    StructField::new(LSN_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                    StructField::new(SEQ_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                    StructField::new(DELETED_COL.to_string(), DeltaType::Primitive(PrimitiveType::Boolean), false),
                    StructField::new(CTID_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false),
                ])
                .collect();
            let schema = StructType::try_new(fields)?;
            let metadata = self.table.snapshot()?.metadata().clone().with_schema(&schema)?;
            let operation = DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            };
            let finalized = CommitBuilder::from(CommitProperties::default())
                .with_actions(vec![Action::Metadata(metadata)])
                .build(Some(self.table.snapshot()?), self.table.log_store(), operation)
                .await
                .context("committing Delta schema evolution")?;
            self.table.state = Some(finalized.snapshot());
            self.schema_dirty = false;
            tracing::info!("Delta schema evolved (metadata-only commit)");
        }

        let mut writer = RecordBatchWriter::for_table(&self.table)?;
        writer.write_with_mode(batch, WriteMode::MergeSchema).await?;
        let adds: Vec<Action> = writer.flush().await?.into_iter().map(Action::Add).collect();

        let props = CommitProperties::default().with_application_transactions(vec![
            Transaction::new(TXN_COMMIT, commit_lsn as i64),
            Transaction::new(TXN_RESTART, restart_lsn as i64),
            Transaction::new(TXN_FILENODE, filenode as i64),
        ]);
        let operation =
            DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None };
        let finalized = CommitBuilder::from(props)
            .with_actions(adds)
            .build(Some(self.table.snapshot()?), self.table.log_store(), operation)
            .await
            .context("committing to Delta table")?;
        let version = finalized.version();
        self.table.state = Some(finalized.snapshot());
        Ok(version as i64)
    }

    /// Rebuild the in-memory pre-image mirror from the change log: the
    /// latest (_ltap_lsn, _ltap_seq) entry per ctid, tombstones removed.
    /// This is exactly the table state at the dedupe watermark, which is
    /// what replay-after-restart needs pre-images against.
    pub async fn load_mirror(&self, desc: &TableDesc) -> Result<HashMap<Ctid, RowVersion>> {
        use deltalake::arrow::array::{
            Array, BinaryArray as ABin, BooleanArray as ABool, Date32Array as ADate,
            Float32Array as AF32, Float64Array as AF64, Int16Array as AI16, Int32Array as AI32,
            Int64Array as AI64, StringArray as AStr, TimestampMicrosecondArray as ATs,
        };

        let files = self.table.get_files_by_partitions(&[]).await?;
        let store = self.table.log_store().object_store(None);
        // ctid -> (lsn, seq, deleted, row); keep the max (lsn, seq).
        let mut latest: HashMap<Ctid, (i64, i64, bool, Row)> = HashMap::new();

        for path in files {
            let bytes = store.get(&path).await?.bytes().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
            for batch in reader {
                let batch = batch?;
                let idx = |name: &str| -> Result<usize> {
                    batch.schema().index_of(name).map_err(Into::into)
                };
                let lsns = batch.column(idx(LSN_COL)?).as_any().downcast_ref::<AI64>()
                    .ok_or_else(|| anyhow::anyhow!("bad {LSN_COL} type"))?.clone();
                let seqs = batch.column(idx(SEQ_COL)?).as_any().downcast_ref::<AI64>()
                    .ok_or_else(|| anyhow::anyhow!("bad {SEQ_COL} type"))?.clone();
                let dels = batch.column(idx(DELETED_COL)?).as_any().downcast_ref::<ABool>()
                    .ok_or_else(|| anyhow::anyhow!("bad {DELETED_COL} type"))?.clone();
                let ctids = batch.column(idx(CTID_COL)?).as_any().downcast_ref::<AI64>()
                    .ok_or_else(|| anyhow::anyhow!("bad {CTID_COL} type"))?.clone();

                for i in 0..batch.num_rows() {
                    let key = txbuf::unpack_ctid(ctids.value(i));
                    let lsn = lsns.value(i);
                    let seq = seqs.value(i);
                    if let Some((l, s, _, _)) = latest.get(&key) {
                        if (lsn, seq) <= (*l, *s) {
                            continue;
                        }
                    }
                    let mut row: Row = Vec::with_capacity(desc.cols.len());
                    for (j, col) in desc.cols.iter().enumerate() {
                        // Column may postdate this parquet file (added later).
                        let Ok(ci) = batch.schema().index_of(&col.name) else {
                            row.push(None);
                            continue;
                        };
                        let arr = batch.column(ci);
                        if arr.is_null(i) {
                            row.push(None);
                            continue;
                        }
                        let any = arr.as_any();
                        let v = match col.ty {
                            PgType::Bool => Value::Bool(any.downcast_ref::<ABool>().unwrap().value(i)),
                            PgType::Int2 => Value::I16(any.downcast_ref::<AI16>().unwrap().value(i)),
                            PgType::Int4 => Value::I32(any.downcast_ref::<AI32>().unwrap().value(i)),
                            PgType::Int8 => Value::I64(any.downcast_ref::<AI64>().unwrap().value(i)),
                            PgType::Float4 => Value::F32(any.downcast_ref::<AF32>().unwrap().value(i)),
                            PgType::Float8 => Value::F64(any.downcast_ref::<AF64>().unwrap().value(i)),
                            PgType::Text | PgType::Uuid | PgType::Numeric => {
                                Value::Text(any.downcast_ref::<AStr>().unwrap().value(i).to_string())
                            }
                            PgType::Bytea => Value::Bytes(any.downcast_ref::<ABin>().unwrap().value(i).to_vec()),
                            PgType::Date => Value::I32(any.downcast_ref::<ADate>().unwrap().value(i)),
                            PgType::Timestamp | PgType::TimestampTz => {
                                Value::I64(any.downcast_ref::<ATs>().unwrap().value(i))
                            }
                        };
                        let _ = j;
                        row.push(Some(v));
                    }
                    latest.insert(key, (lsn, seq, dels.value(i), row));
                }
            }
        }

        let mut mirror = HashMap::new();
        for (ctid, (_, _, deleted, row)) in latest {
            if deleted {
                continue;
            }
            let attrs = heap::encode_attrs(&row, desc);
            mirror.insert(ctid, RowVersion { row, attrs });
        }
        Ok(mirror)
    }

    /// Reclaim data files no longer referenced by the log (orphaned by
    /// compaction), older than `retention`. Runs in the single writer; the
    /// retention window protects readers that resolved a file list just
    /// before a compaction commit.
    pub async fn vacuum(&mut self, retention: std::time::Duration) -> Result<usize> {
        let (table, metrics) = self
            .table
            .clone()
            .vacuum()
            .with_retention_period(chrono::Duration::from_std(retention).map_err(|e| anyhow::anyhow!("{e}"))?)
            .with_enforce_retention_duration(false)
            .await
            .context("vacuuming Delta table")?;
        self.table = table;
        Ok(metrics.files_deleted.len())
    }

    /// Collapse the append-only change log to current state: keep the latest
    /// (_ltap_lsn, _ltap_seq) row per primary key, drop tombstoned keys and
    /// all superseded versions, and rewrite the table as fresh files in ONE
    /// atomic commit (remove-all + add-compacted) that carries the exactly-
    /// once watermarks forward. Runs inline in the single writer, so there is
    /// no concurrency to coordinate. Works in Arrow space, so every Delta
    /// column — including ones dropped in Postgres but retained here — is
    /// preserved byte-for-byte. Returns (rows_before, rows_after), or None if
    /// skipped (no primary key, or nothing to gain).
    pub async fn compact(&mut self, desc: &TableDesc) -> Result<Option<(usize, usize)>> {
        use deltalake::arrow::array::{
            new_null_array, Array, BooleanArray as ABool, Int64Array as AI64, UInt32Array,
        };
        use deltalake::arrow::compute::{concat_batches, take};

        if desc.pk.is_empty() {
            return Ok(None); // no key to collapse on
        }
        let pk_idx: Vec<usize> = desc
            .pk
            .iter()
            .filter_map(|name| self.delta_cols.iter().position(|dc| &dc.col.name == name))
            .collect();
        if pk_idx.len() != desc.pk.len() {
            return Ok(None); // a PK column isn't in the Delta table (mid-evolution)
        }

        // Read every active file, conform it to the current schema (older
        // files lack columns added later), and concat into one batch.
        let files = self.table.get_files_by_partitions(&[]).await?;
        if files.len() < 2 {
            // A single file still benefits if it has superseded rows, but the
            // common "already compact" case is one file — cheap to check below.
        }
        let store = self.table.log_store().object_store(None);
        let mut batches = Vec::new();
        for path in &files {
            let bytes = store.get(path).await?.bytes().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
            for b in reader {
                batches.push(conform_batch(&b?, &self.arrow_schema));
            }
        }
        if batches.is_empty() {
            return Ok(None);
        }
        let all = concat_batches(&self.arrow_schema, &batches)?;
        let total = all.num_rows();

        let lsns = all.column(all.schema().index_of(LSN_COL)?).as_any().downcast_ref::<AI64>()
            .ok_or_else(|| anyhow::anyhow!("bad {LSN_COL} type"))?.clone();
        let seqs = all.column(all.schema().index_of(SEQ_COL)?).as_any().downcast_ref::<AI64>()
            .ok_or_else(|| anyhow::anyhow!("bad {SEQ_COL} type"))?.clone();
        let dels = all.column(all.schema().index_of(DELETED_COL)?).as_any().downcast_ref::<ABool>()
            .ok_or_else(|| anyhow::anyhow!("bad {DELETED_COL} type"))?.clone();
        let pk_types: Vec<PgType> = desc.pk.iter().map(|n| {
            desc.cols.iter().find(|c| &c.name == n).map(|c| c.ty).unwrap()
        }).collect();

        // Latest (lsn, seq) row index per primary key.
        let mut best: HashMap<String, (i64, i64, u32, bool)> = HashMap::new();
        for i in 0..total {
            let mut key = String::new();
            for (k, &ci) in pk_idx.iter().enumerate() {
                key.push_str(&arrow_cell_key(all.column(ci), pk_types[k], i));
                key.push('\x1f');
            }
            let lsn = lsns.value(i);
            let seq = seqs.value(i);
            match best.get(&key) {
                Some((l, s, _, _)) if (lsn, seq) <= (*l, *s) => {}
                _ => {
                    best.insert(key, (lsn, seq, i as u32, dels.value(i)));
                }
            }
        }
        let mut survivors: Vec<u32> = best
            .values()
            .filter(|(_, _, _, deleted)| !*deleted)
            .map(|(_, _, idx, _)| *idx)
            .collect();
        survivors.sort_unstable();
        let after = survivors.len();

        // Nothing to gain: already one file with no superseded/deleted rows.
        if after == total && files.len() <= 1 {
            return Ok(None);
        }

        let idx_array = UInt32Array::from(survivors);
        let cols: Vec<_> = all
            .columns()
            .iter()
            .map(|c| take(c, &idx_array, None))
            .collect::<std::result::Result<_, _>>()?;
        let compacted = RecordBatch::try_new(self.arrow_schema.clone(), cols)?;

        // Build the replace commit: remove every current file, add the
        // compacted file(s), and re-assert the watermarks so resume is
        // unaffected even after the old log entries are checkpointed away.
        let removes: Vec<Action> = self
            .table
            .snapshot()?
            .snapshot()
            .log_data()
            .into_iter()
            .map(|v| Action::Remove(v.remove_action(true)))
            .collect();

        let mut adds: Vec<Action> = Vec::new();
        if compacted.num_rows() > 0 {
            let mut writer = RecordBatchWriter::for_table(&self.table)?;
            writer.write(compacted).await?;
            adds = writer.flush().await?.into_iter().map(Action::Add).collect();
        }

        let resume = self.resume_state().await?;
        let mut txns = Vec::new();
        if let Some(v) = resume.commit_lsn {
            txns.push(Transaction::new(TXN_COMMIT, v as i64));
        }
        if let Some(v) = resume.restart_lsn {
            txns.push(Transaction::new(TXN_RESTART, v as i64));
        }
        txns.push(Transaction::new(TXN_FILENODE, resume.filenode.unwrap_or(desc.rel_node) as i64));

        let mut actions = removes;
        actions.extend(adds);
        let op = DeltaOperation::Write {
            mode: SaveMode::Overwrite,
            partition_by: None,
            predicate: None,
        };
        let finalized = CommitBuilder::from(CommitProperties::default().with_application_transactions(txns))
            .with_actions(actions)
            .build(Some(self.table.snapshot()?), self.table.log_store(), op)
            .await
            .context("committing compaction")?;
        self.table.state = Some(finalized.snapshot());
        Ok(Some((total, after)))
    }
}

/// Reshape a batch to `schema`: keep columns by name, fill absent ones (added
/// after this file was written) with nulls.
fn conform_batch(batch: &RecordBatch, schema: &Arc<ArrowSchema>) -> RecordBatch {
    use deltalake::arrow::array::new_null_array;
    let cols: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| {
            batch
                .column_by_name(f.name())
                .cloned()
                .unwrap_or_else(|| new_null_array(f.data_type(), batch.num_rows()))
        })
        .collect();
    RecordBatch::try_new(schema.clone(), cols).expect("conform batch to schema")
}

/// A stable string key for the value in `col` at `row`, for grouping by PK.
fn arrow_cell_key(col: &ArrayRef, ty: PgType, row: usize) -> String {
    use deltalake::arrow::array::{
        Array, BinaryArray as ABin, BooleanArray as ABool, Date32Array as ADate,
        Float32Array as AF32, Float64Array as AF64, Int16Array as AI16, Int32Array as AI32,
        Int64Array as AI64, StringArray as AStr, TimestampMicrosecondArray as ATs,
    };
    if col.is_null(row) {
        return "\0NULL".to_string();
    }
    let a = col.as_any();
    match ty {
        PgType::Bool => (a.downcast_ref::<ABool>().unwrap().value(row) as u8).to_string(),
        PgType::Int2 => a.downcast_ref::<AI16>().unwrap().value(row).to_string(),
        PgType::Int4 | PgType::Date => a.downcast_ref::<AI32>().unwrap().value(row).to_string(),
        PgType::Int8 => a.downcast_ref::<AI64>().unwrap().value(row).to_string(),
        PgType::Float4 => a.downcast_ref::<AF32>().unwrap().value(row).to_bits().to_string(),
        PgType::Float8 => a.downcast_ref::<AF64>().unwrap().value(row).to_bits().to_string(),
        PgType::Text | PgType::Uuid | PgType::Numeric => {
            a.downcast_ref::<AStr>().unwrap().value(row).to_string()
        }
        PgType::Bytea => hex_of(a.downcast_ref::<ABin>().unwrap().value(row)),
        PgType::Timestamp | PgType::TimestampTz => a.downcast_ref::<ATs>().unwrap().value(row).to_string(),
    }
}

fn hex_of(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
