//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.
use crate::error::Result;
use crate::io::DataFileWriter;
use crate::io::RecordBatchWriter;
use crate::io::RecordBatchWriterBuilder;
use crate::types::Any;
use crate::types::FieldProjector;
use crate::types::PartitionKey;
use crate::types::PartitionSplitter;
use crate::types::{DataFile, TableMetadata};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use itertools::Itertools;
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
pub enum AppendOnlyWriter<B: RecordBatchWriterBuilder> {
    /// Unpartitioned task writer
    Unpartitioned(DataFileWriter<B::R>),
    /// Partitioned task writer
    Partitioned(PartitionedAppendOnlyWriter<B>),
}

impl<B: RecordBatchWriterBuilder> AppendOnlyWriter<B> {
    /// Create a new `TaskWriter`.
    pub async fn try_new(table_metadata: TableMetadata, writer_builder: B) -> Result<Self> {
        let current_schema = table_metadata.current_schema()?;
        let current_partition_spec = table_metadata.current_partition_spec()?;
        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        if current_partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(DataFileWriter::try_new(
                writer_builder.build(&arrow_schema).await?,
            )?))
        } else {
            let column_ids = current_partition_spec
                .fields
                .iter()
                .map(|field| field.source_column_id as usize)
                .collect_vec();
            let (col_extractor, _) = FieldProjector::new(arrow_schema.fields(), &column_ids)?;
            let partition_splitter = PartitionSplitter::try_new(
                col_extractor,
                current_partition_spec,
                Any::Struct(
                    current_partition_spec
                        .partition_type(current_schema)?
                        .into(),
                ),
            )?;
            Ok(Self::Partitioned(PartitionedAppendOnlyWriter::try_new(
                arrow_schema,
                partition_splitter,
                writer_builder,
            )?))
        }
    }

    /// Write a record batch.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            AppendOnlyWriter::Unpartitioned(writer) => writer.write(batch.clone()).await,
            AppendOnlyWriter::Partitioned(writer) => writer.write(batch).await,
        }
    }

    /// Close the writer and return the data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self {
            AppendOnlyWriter::Unpartitioned(mut writer) => Ok(writer
                .close()
                .await?
                .into_iter()
                .map(|x| x.build())
                .collect()),
            AppendOnlyWriter::Partitioned(writer) => writer.close().await,
        }
    }
}

/// Partition append only writer
pub struct PartitionedAppendOnlyWriter<B: RecordBatchWriterBuilder> {
    schema: ArrowSchemaRef,

    writers: HashMap<PartitionKey, DataFileWriter<B::R>>,
    partition_splitter: PartitionSplitter,
    writer_builder: B,
}

impl<B: RecordBatchWriterBuilder> PartitionedAppendOnlyWriter<B> {
    /// Create a new `PartitionedWriter`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: ArrowSchemaRef,
        partition_splitter: PartitionSplitter,
        writer_builder: B,
    ) -> Result<Self> {
        Ok(Self {
            writers: HashMap::new(),
            partition_splitter,
            writer_builder,
            schema,
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
                            self.writer_builder
                                .clone()
                                .build(&self.schema.clone())
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
        for (key, mut writer) in self.writers.into_iter() {
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
