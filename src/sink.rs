//! Delta Lake sink: create-if-absent, then append one RecordBatch per
//! committed transaction. Every row carries `_ltap_lsn` — the commit LSN —
//! which is what later makes LSN-consistent snapshot reads possible.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use deltalake::arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::DeltaTable;
use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use url::Url;

use crate::schema::{Col, PgType, TableDesc};
use crate::wal::heap::{Row, Value};

pub const LSN_COL: &str = "_ltap_lsn";

pub struct DeltaSink {
    table: DeltaTable,
    arrow_schema: Arc<ArrowSchema>,
    cols: Vec<Col>,
}

fn delta_type(ty: PgType) -> DeltaType {
    DeltaType::Primitive(match ty {
        PgType::Bool => PrimitiveType::Boolean,
        PgType::Int4 => PrimitiveType::Integer,
        PgType::Int8 => PrimitiveType::Long,
        PgType::Text => PrimitiveType::String,
    })
}

fn arrow_type(ty: PgType) -> ArrowType {
    match ty {
        PgType::Bool => ArrowType::Boolean,
        PgType::Int4 => ArrowType::Int32,
        PgType::Int8 => ArrowType::Int64,
        PgType::Text => ArrowType::Utf8,
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

    pub async fn append(&mut self, rows: &[Row], commit_lsn: u64) -> Result<i64> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len() + 1);
        for (j, col) in self.cols.iter().enumerate() {
            let array: ArrayRef = match col.ty {
                PgType::Bool => Arc::new(BooleanArray::from(
                    rows.iter()
                        .map(|r| match &r[j] {
                            Some(Value::Bool(b)) => Some(*b),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                PgType::Int4 => Arc::new(Int32Array::from(
                    rows.iter()
                        .map(|r| match &r[j] {
                            Some(Value::I32(v)) => Some(*v),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                PgType::Int8 => Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| match &r[j] {
                            Some(Value::I64(v)) => Some(*v),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                PgType::Text => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| match &r[j] {
                            Some(Value::Text(s)) => Some(s.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
            };
            arrays.push(array);
        }
        arrays.push(Arc::new(Int64Array::from(vec![commit_lsn as i64; rows.len()])));

        let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)?;
        let mut writer = RecordBatchWriter::for_table(&self.table)?;
        writer.write(batch).await?;
        let version = writer
            .flush_and_commit(&mut self.table)
            .await
            .context("appending to Delta table")?;
        Ok(version as i64)
    }
}
