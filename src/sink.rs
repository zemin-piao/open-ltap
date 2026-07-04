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
use deltalake::kernel::{Action, DataType as DeltaType, PrimitiveType, StructField, Transaction};
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
}

pub struct DeltaSink {
    table: DeltaTable,
    arrow_schema: Arc<ArrowSchema>,
    cols: Vec<Col>,
}

fn delta_type(ty: PgType) -> DeltaType {
    DeltaType::Primitive(match ty {
        PgType::Bool => PrimitiveType::Boolean,
        PgType::Int2 => PrimitiveType::Short,
        PgType::Int4 => PrimitiveType::Integer,
        PgType::Int8 => PrimitiveType::Long,
        PgType::Float4 => PrimitiveType::Float,
        PgType::Float8 => PrimitiveType::Double,
        PgType::Text | PgType::Uuid => PrimitiveType::String,
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
        PgType::Text | PgType::Uuid => ArrowType::Utf8,
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

        let arrow_fields: Vec<Field> = desc
            .cols
            .iter()
            .map(|c| Field::new(&c.name, arrow_type(c.ty), true))
            .chain([
                Field::new(LSN_COL, ArrowType::Int64, false),
                Field::new(SEQ_COL, ArrowType::Int64, false),
                Field::new(DELETED_COL, ArrowType::Boolean, false),
                Field::new(CTID_COL, ArrowType::Int64, false),
            ])
            .collect();
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

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

        Ok(DeltaSink { table, arrow_schema, cols: desc.cols.clone() })
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
        Ok(ResumeState { commit_lsn, restart_lsn })
    }

    /// Append a batch of committed rows (possibly spanning many PG commits)
    /// as ONE Delta commit, carrying the new LSN watermarks atomically.
    pub async fn append(
        &mut self,
        rows: &[EmitRow],
        commit_lsn: u64,
        restart_lsn: u64,
    ) -> Result<i64> {
        // Pull column j out of every row as Option<T>, tolerating type
        // mismatches as NULL (decode already validated shapes).
        macro_rules! col_vals {
            ($j:expr, $variant:ident) => {
                rows.iter()
                    .map(|e| match &e.row[$j] {
                        Some(Value::$variant(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len() + 1);
        for (j, col) in self.cols.iter().enumerate() {
            let array: ArrayRef = match col.ty {
                PgType::Bool => Arc::new(BooleanArray::from(col_vals!(j, Bool))),
                PgType::Int2 => Arc::new(Int16Array::from(col_vals!(j, I16))),
                PgType::Int4 => Arc::new(Int32Array::from(col_vals!(j, I32))),
                PgType::Int8 => Arc::new(Int64Array::from(col_vals!(j, I64))),
                PgType::Float4 => Arc::new(Float32Array::from(col_vals!(j, F32))),
                PgType::Float8 => Arc::new(Float64Array::from(col_vals!(j, F64))),
                PgType::Text | PgType::Uuid => Arc::new(StringArray::from(col_vals!(j, Text))),
                PgType::Bytea => Arc::new(BinaryArray::from(
                    col_vals!(j, Bytes).iter().map(|o| o.as_deref()).collect::<Vec<_>>(),
                )),
                PgType::Date => Arc::new(Date32Array::from(col_vals!(j, I32))),
                PgType::Timestamp => Arc::new(TimestampMicrosecondArray::from(col_vals!(j, I64))),
                PgType::TimestampTz => Arc::new(
                    TimestampMicrosecondArray::from(col_vals!(j, I64)).with_timezone("UTC"),
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

        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;
        let mut writer = RecordBatchWriter::for_table(&self.table)?;
        writer.write(batch).await?;
        let adds: Vec<Action> = writer.flush().await?.into_iter().map(Action::Add).collect();

        let props = CommitProperties::default().with_application_transactions(vec![
            Transaction::new(TXN_COMMIT, commit_lsn as i64),
            Transaction::new(TXN_RESTART, restart_lsn as i64),
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
                        let arr = batch.column(idx(&col.name)?);
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
                            PgType::Text | PgType::Uuid => {
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
}
