//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use opendal::Operator;

use super::data_file_writer::DataFileWriter;
use crate::error::Result;
use crate::io::location_generator::DataFileLocationGenerator;
use crate::types::{DataFile, TableMetadata};

/// `TaskWriter` used to write data for a table. It will
/// split the data of different partition key into different
/// data file writer if find that the table metadata has partition spec.
///
/// # TODO
///
/// The `TaskWriter` only support unpartitioned write now.
pub struct TaskWriter {
    /// # TODO
    ///
    /// Support to config the data file writer.
    data_file_writer: DataFileWriter,
}

impl TaskWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        table_metadata: TableMetadata,
        operator: Operator,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
    ) -> Result<Self> {
        // # TODO
        // Support partitioned write.
        if let Some(partition_spec) = table_metadata
            .partition_specs
            .get(table_metadata.default_spec_id as usize)
        {
            if !partition_spec.fields.is_empty() {
                return Err(crate::error::Error::new(
                    crate::ErrorKind::IcebergFeatureUnsupported,
                    "Partitioned write is not supported yet",
                ));
            }
        }

        let schema: ArrowSchema = table_metadata
            .schemas
            .clone()
            .into_iter()
            .find(|schema| schema.schema_id == table_metadata.current_schema_id)
            .ok_or_else(|| {
                crate::error::Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    "Can't find current schema",
                )
            })?
            .try_into()
            .map_err(|e| {
                crate::error::Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!("Can't convert iceberg schema to arrow schema: {}", e),
                )
            })?;

        let location_generator =
            DataFileLocationGenerator::try_new(table_metadata, partition_id, task_id, suffix)?;

        let data_writer = DataFileWriter::try_new(
            operator,
            location_generator,
            schema.into(),
            1024,
            1024 * 1024,
        )
        .await?;

        Ok(Self {
            data_file_writer: data_writer,
        })
    }

    /// Write a record batch using data file writer.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.data_file_writer.write(batch.clone()).await
    }

    /// Complete the write and return the data files.
    /// It didn't mean the write take effect in table.
    /// To make the write take effect, you should commit the data file using transaction api.
    ///
    /// # TODO
    /// The data file should split by partition key.
    /// We don't support partitioned write now so use `()` as placeholder.
    pub async fn close(self) -> Result<HashMap<(), Vec<DataFile>>> {
        let datafiles = self.data_file_writer.close().await?;
        let mut result = HashMap::new();
        result.insert((), datafiles);
        Ok(result)
    }
}
