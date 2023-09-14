//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.

use super::data_file_writer::DataFileWriter;
use super::location_generator;
use crate::config::TableConfigRef;
use crate::error::Result;
use crate::io::location_generator::DataFileLocationGenerator;
use crate::types::Any;
use crate::types::PartitionSpec;
use crate::types::PartitionSplitter;
use crate::types::{DataFile, TableMetadata};
use arrow_array::RecordBatch;
use arrow_row::OwnedRow;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use opendal::Operator;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
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
    /// Partitioned task writer
    Partitioned(PartitionedWriter),
}

impl TaskWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        table_metadata: TableMetadata,
        operator: Operator,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let current_schema = table_metadata
            .schemas
            .clone()
            .into_iter()
            .find(|schema| schema.schema_id == table_metadata.current_schema_id)
            .ok_or_else(|| {
                crate::error::Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    "Can't find current schema",
                )
            })?;

        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        let partition_spec = table_metadata
            .partition_specs
            .get(table_metadata.default_spec_id as usize)
            .ok_or(crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "Can't find default partition spec",
            ))?;

        let location_generator = location_generator::DataFileLocationGenerator::try_new(
            &table_metadata,
            partition_id,
            task_id,
            suffix,
        )?;

        if partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(
                UnpartitionedWriter::try_new(
                    arrow_schema,
                    table_metadata.location,
                    location_generator,
                    operator,
                    table_config,
                )
                .await?,
            ))
        } else {
            let partition_type =
                Any::Struct(partition_spec.partition_type(&current_schema)?.into());
            Ok(Self::Partitioned(PartitionedWriter::new(
                arrow_schema,
                table_metadata.location,
                location_generator,
                partition_spec,
                partition_type,
                operator,
                table_config,
            )?))
        }
    }

    /// Write a record batch.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            TaskWriter::Unpartitioned(writer) => writer.write(batch).await,
            TaskWriter::Partitioned(writer) => writer.write(batch).await,
        }
    }

    /// Close the writer and return the data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self {
            TaskWriter::Unpartitioned(writer) => writer.close().await,
            TaskWriter::Partitioned(writer) => writer.close().await,
        }
    }
}

/// Unpartitioned task writer
pub struct UnpartitionedWriter {
    data_file_writer: DataFileWriter,
}

impl UnpartitionedWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        schema: ArrowSchemaRef,
        table_location: String,
        location_generator: DataFileLocationGenerator,
        operator: Operator,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            data_file_writer: DataFileWriter::try_new(
                operator,
                table_location,
                location_generator.into(),
                schema,
                table_config,
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
    pub async fn close(self) -> Result<Vec<DataFile>> {
        Ok(self
            .data_file_writer
            .close()
            .await?
            .into_iter()
            .map(|x| x.build())
            .collect())
    }
}

/// Partition task writer
pub struct PartitionedWriter {
    operator: Operator,
    table_location: String,
    location_generator: Arc<DataFileLocationGenerator>,
    table_config: TableConfigRef,
    schema: ArrowSchemaRef,

    writers: HashMap<OwnedRow, DataFileWriter>,
    partition_splitter: PartitionSplitter,
}

impl PartitionedWriter {
    /// Create a new `PartitionedWriter`.
    pub fn new(
        schema: ArrowSchemaRef,
        table_location: String,
        location_generator: DataFileLocationGenerator,
        partition_spec: &PartitionSpec,
        partition_type: Any,
        operator: Operator,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            operator,
            table_location,
            location_generator: location_generator.into(),
            table_config,
            writers: HashMap::new(),
            partition_splitter: PartitionSplitter::new(partition_spec, &schema, partition_type)?,
            schema,
        })
    }

    /// Write a record batch using data file writer.
    /// It will split the batch by partition spec and write the batch to different data file writer.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let split_batch = self.partition_splitter.split_by_partition(batch).await?;

        for (row, batch) in split_batch.into_iter() {
            match self.writers.entry(row) {
                Entry::Occupied(mut writer) => {
                    writer.get_mut().write(batch).await?;
                }
                Entry::Vacant(writer) => {
                    writer
                        .insert(
                            DataFileWriter::try_new(
                                self.operator.clone(),
                                self.table_location.clone(),
                                self.location_generator.clone(),
                                self.schema.clone(),
                                self.table_config.clone(),
                            )
                            .await?,
                        )
                        .write(batch)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Complete the write and return the data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        let mut res = vec![];
        let mut partition_values = self.partition_splitter.partition_values();
        for (row, writer) in self.writers.into_iter() {
            let data_file_builders = writer.close().await?;
            // Update the partition value in data file.
            res.extend(data_file_builders.into_iter().map(|data_file_builder| {
                data_file_builder
                    .with_partition_value(partition_values.remove(&row).unwrap())
                    .build()
            }));
        }
        Ok(res)
    }
}
