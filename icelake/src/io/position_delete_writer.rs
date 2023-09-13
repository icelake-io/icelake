//! A module provide `PositionDeleteWriter`.

use std::sync::Arc;

use crate::config::TableConfigRef;
use crate::types::{Any, DataFileBuilder, Field, Primitive, Schema};
use crate::{types::Struct, Result};
use arrow_array::RecordBatch;
use opendal::Operator;

use super::{location_generator::DataFileLocationGenerator, rolling_writer::RollingWriter};

/// A writer capable of splitting incoming delete into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
///
///
/// # NOTE
/// According to spec, The data write to position delete file should be:
/// - Sorting by file_path allows filter pushdown by file in columnar storage formats.
/// - Sorting by pos allows filtering rows while scanning, to avoid keeping deletes in memory.
/// - They're belong to partition.
///
/// But PositionDeleteWriter will not gurantee and check above. It is the caller's responsibility to gurantee them.
pub struct PositionDeleteWriter {
    inner_writer: RollingWriter,
    table_location: String,
}

impl PositionDeleteWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<DataFileLocationGenerator>,
        row_type: Option<Arc<Struct>>,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let mut fields = vec![
            Arc::new(Field::required(
                2147483546,
                "file_path",
                Any::Primitive(Primitive::String),
            )),
            Arc::new(Field::required(
                2147483545,
                "pos",
                Any::Primitive(Primitive::Long),
            )),
        ];
        if let Some(row_type) = row_type {
            fields.push(Arc::new(Field::required(
                2147483544,
                "row",
                Any::Struct(row_type),
            )));
        }
        let schema = Arc::new(Schema::new(1, None, Struct::new(fields)).try_into()?);
        Ok(Self {
            inner_writer: RollingWriter::try_new(
                operator,
                location_generator,
                schema,
                table_config,
            )
            .await?,
            table_location,
        })
    }

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await?;
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    pub async fn close(self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_content(crate::types::DataContentType::PostionDeletes)
                    .with_table_location(self.table_location.clone())
            })
            .collect())
    }
}
