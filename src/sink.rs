//! Delta Lake sink: create-if-absent, then append batches of committed rows.
//! Every row carries `_ltap_lsn` — its transaction's commit LSN — which is
//! what later makes LSN-consistent snapshot reads possible.
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
use url::Url;

use crate::schema::{Col, PgType, TableDesc};
use crate::wal::heap::{Row, Value};

pub const LSN_COL: &str = "_ltap_lsn";
const TXN_COMMIT: &str = "open-ltap.commit";
const TXN_RESTART: &str = "open-ltap.restart";

/// A committed PG row tagged with its transaction's commit LSN.
pub type TaggedRow = (u64, Row);

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
            .chain([Field::new(LSN_COL, ArrowType::Int64, false)])
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
                    .chain([StructField::new(LSN_COL.to_string(), DeltaType::Primitive(PrimitiveType::Long), false)])
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
        rows: &[TaggedRow],
        commit_lsn: u64,
        restart_lsn: u64,
    ) -> Result<i64> {
        // Pull column j out of every row as Option<T>, tolerating type
        // mismatches as NULL (decode already validated shapes).
        macro_rules! col_vals {
            ($j:expr, $variant:ident) => {
                rows.iter()
                    .map(|(_, r)| match &r[$j] {
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
            rows.iter().map(|(lsn, _)| *lsn as i64).collect::<Vec<_>>(),
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
}
