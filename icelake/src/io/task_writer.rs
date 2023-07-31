//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use opendal::Operator;

use super::data_file_writer::DataFileWriter;
use super::location_generator;
use crate::error::Result;
use crate::io::location_generator::DataFileLocationGenerator;
use crate::types::{DataFile, StructValue, TableMetadata};

/// `TaskWriter` used to write data for a table.
///
/// If it find that the table metadata has partition spec, it will create a
/// partitioned task writer. The partition task writer will split the data according
/// the partition key and write them using different data file writer.
///
/// If the table metadata has no partition spec, it will create a unpartitioned
/// task writer. The unpartitioned task writer will write all data using a single
/// data file writer.
pub enum TaskWriter {
    /// Unpartitioned task writer
    Unpartitioned(UnpartitionedWriter),
    // Partitioned(PartitionedTaskWriter),
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

        let partition_spec = table_metadata
            .partition_specs
            .get(table_metadata.default_spec_id as usize)
            .ok_or(crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "Can't find default partition spec",
            ))?;

        if partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(
                UnpartitionedWriter::try_new(
                    schema,
                    location_generator::DataFileLocationGenerator::try_new(
                        &table_metadata,
                        partition_id,
                        task_id,
                        suffix,
                    )?,
                    operator,
                )
                .await?,
            ))
        } else {
            todo!()
        }
    }

    /// Write a record batch.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Unpartitioned(writer) => writer.write(batch).await,
        }
    }

    /// Close the writer and return the data files.
    pub async fn close(self) -> Result<HashMap<StructValue, Vec<DataFile>>> {
        match self {
            Self::Unpartitioned(writer) => writer.close().await,
        }
    }
}

/// Unpartitioned task writer
pub struct UnpartitionedWriter {
    /// # TODO
    ///
    /// Support to config the data file writer.
    data_file_writer: DataFileWriter,
}

impl UnpartitionedWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        schema: ArrowSchema,
        location_generator: DataFileLocationGenerator,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            data_file_writer: DataFileWriter::try_new(
                operator,
                location_generator,
                schema.into(),
                1024,
                1024 * 1024,
            )
            .await?,
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
    /// # Note
    ///
    /// For unpartitioned table, the key of the result map is default partition key.
    pub async fn close(self) -> Result<HashMap<StructValue, Vec<DataFile>>> {
        let datafiles = self.data_file_writer.close().await?;
        let mut result = HashMap::new();
        result.insert(StructValue::default(), datafiles);
        Ok(result)
    }
}
