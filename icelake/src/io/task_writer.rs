//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.
use super::file_writer::DataFileWriter;
use super::location_generator;
use super::DefaultFileAppender;
use super::FileAppender;
use super::FileAppenderBuilder;
use super::FileAppenderLayer;
use crate::config::TableConfigRef;
use crate::error::Result;
use crate::io::location_generator::FileLocationGenerator;
use crate::types::Any;
use crate::types::PartitionKey;
use crate::types::PartitionSpec;
use crate::types::PartitionSplitter;
use crate::types::{DataFile, TableMetadata};
use arrow_array::RecordBatch;
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
pub enum TaskWriter<L: FileAppenderLayer<DefaultFileAppender>> {
    /// Unpartitioned task writer
    Unpartitioned(UnpartitionedWriter<L::R>),
    /// Partitioned task writer
    Partitioned(PartitionedWriter<L>),
}

impl<L: FileAppenderLayer<DefaultFileAppender>> TaskWriter<L> {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        table_metadata: TableMetadata,
        operator: Operator,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
        table_config: TableConfigRef,
        file_appender_factory: FileAppenderBuilder<L>,
    ) -> Result<Self> {
        let current_schema = table_metadata.current_schema()?;
        let current_partition_spec = table_metadata.current_partition_spec()?;

        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        let location_generator = location_generator::FileLocationGenerator::try_new_for_data_file(
            &table_metadata,
            partition_id,
            task_id,
            suffix,
        )?;

        if current_partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(UnpartitionedWriter::try_new(
                file_appender_factory.build(arrow_schema).await?,
            )?))
        } else {
            let partition_type = Any::Struct(
                current_partition_spec
                    .partition_type(current_schema)?
                    .into(),
            );
            Ok(Self::Partitioned(PartitionedWriter::try_new(
                arrow_schema,
                table_metadata.location.clone(),
                location_generator,
                current_partition_spec,
                partition_type,
                operator,
                table_config,
                file_appender_factory,
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
pub struct UnpartitionedWriter<F: FileAppender> {
    data_file_writer: DataFileWriter<F>,
}

impl<F: FileAppender> UnpartitionedWriter<F> {
    /// Create a new `TaskWriter`.
    pub fn try_new(writer: F) -> Result<Self> {
        Ok(Self {
            data_file_writer: DataFileWriter::try_new(writer)?,
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
pub struct PartitionedWriter<L: FileAppenderLayer<DefaultFileAppender>> {
    operator: Operator,
    table_location: String,
    location_generator: Arc<FileLocationGenerator>,
    table_config: TableConfigRef,
    schema: ArrowSchemaRef,
    partition_type: Any,

    writers: HashMap<PartitionKey, DataFileWriter<L::R>>,
    partition_splitter: PartitionSplitter,
    file_appender_factory: FileAppenderBuilder<L>,
}

impl<L: FileAppenderLayer<DefaultFileAppender>> PartitionedWriter<L> {
    /// Create a new `PartitionedWriter`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: ArrowSchemaRef,
        table_location: String,
        location_generator: FileLocationGenerator,
        partition_spec: &PartitionSpec,
        partition_type: Any,
        operator: Operator,
        table_config: TableConfigRef,
        file_appender_factory: FileAppenderBuilder<L>,
    ) -> Result<Self> {
        Ok(Self {
            operator,
            table_location,
            location_generator: location_generator.into(),
            table_config,
            writers: HashMap::new(),
            partition_splitter: PartitionSplitter::try_new(
                partition_spec,
                &schema,
                partition_type.clone(),
            )?,
            partition_type,
            schema,
            file_appender_factory,
        })
    }

    /// Write a record batch using data file writer.
    /// It will split the batch by partition spec and write the batch to different data file writer.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let split_batch = self.partition_splitter.split_by_partition(batch)?;

        for (row, batch) in split_batch.into_iter() {
            match self.writers.entry(row) {
                Entry::Occupied(mut writer) => {
                    writer.get_mut().write(batch).await?;
                }
                Entry::Vacant(writer) => {
                    writer
                        .insert(DataFileWriter::try_new(
                            self.file_appender_factory
                                .build(self.schema.clone())
                                .await?,
                        )?)
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
        for (key, writer) in self.writers.into_iter() {
            let data_file_builders = writer.close().await?;

            let partition_value = self.partition_splitter.convert_key_to_value(key)?;

            res.extend(data_file_builders.into_iter().map(|data_file_builder| {
                data_file_builder
                    .with_partition_value(Some(partition_value.clone()))
                    .build()
            }));
        }
        Ok(res)
    }
}
