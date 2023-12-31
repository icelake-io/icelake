use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use crate::{
    io::{RecordBatchWriterBuilder, SingletonWriter},
    types::{Any, FieldProjector, PartitionKey},
    ErrorKind, Result,
};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::SchemaRef;
use itertools::Itertools;

use crate::{
    config::TableConfigRef,
    types::{PartitionSplitter, TableMetadata},
    Error,
};

use arrow_ord::partition::partition;

use super::{DeltaWriterResult, EqualityDeltaWriter};

pub enum UpsertWriter<B: RecordBatchWriterBuilder>
where
    B::R: SingletonWriter,
{
    Unpartitioned(EqualityDeltaWriter<B>),
    Partitioned(PartitionedUpsertWriter<B>),
}

pub const INSERT_OP: i32 = 1;
pub const DELETE_OP: i32 = 2;

impl<B: RecordBatchWriterBuilder> UpsertWriter<B>
where
    B::R: SingletonWriter,
{
    pub async fn try_new(
        table_metadata: TableMetadata,
        table_config: TableConfigRef,
        unique_column_ids: Vec<usize>,
        writer_builder: B,
    ) -> Result<Self> {
        let current_schema = table_metadata.current_schema()?;
        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        let current_partition_spec = table_metadata.current_partition_spec()?;

        if current_partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(
                EqualityDeltaWriter::try_new(
                    arrow_schema,
                    table_config,
                    unique_column_ids,
                    writer_builder,
                )
                .await?,
            ))
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
            Ok(Self::Partitioned(PartitionedUpsertWriter::new(
                table_config,
                unique_column_ids,
                arrow_schema,
                partition_splitter,
                writer_builder,
            )))
        }
    }

    /// Write a record batch with op
    /// `INSRET_OP`: insert
    /// `DELETE_OP`: delete
    pub async fn write(&mut self, ops: Vec<i32>, batch: &RecordBatch) -> Result<()> {
        let ops_array = Arc::new(Int32Array::from(ops));
        let partitions = partition(&[ops_array.clone()]).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!("Failed to partition ops, error: {}", err),
            )
        })?;
        for range in partitions.ranges() {
            let batch = batch.slice(range.start, range.end - range.start);
            match ops_array.value(range.start) {
                // Insert
                INSERT_OP => match self {
                    UpsertWriter::Unpartitioned(writer) => writer.write(batch).await?,
                    UpsertWriter::Partitioned(writer) => writer.write(batch).await?,
                },
                // Delete
                DELETE_OP => match self {
                    UpsertWriter::Unpartitioned(writer) => writer.delete(batch).await?,
                    UpsertWriter::Partitioned(writer) => writer.delete(batch).await?,
                },
                op => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Invalid ops: {op}"),
                    ))
                }
            }
        }
        Ok(())
    }

    pub async fn close(self) -> Result<Vec<DeltaWriterResult>> {
        match self {
            UpsertWriter::Unpartitioned(writer) => Ok(vec![writer.close(None).await?]),
            UpsertWriter::Partitioned(writer) => writer.close().await,
        }
    }
}

pub struct PartitionedUpsertWriter<B: RecordBatchWriterBuilder>
where
    B::R: SingletonWriter,
{
    table_config: TableConfigRef,
    schema: SchemaRef,
    writers: HashMap<crate::types::PartitionKey, EqualityDeltaWriter<B>>,
    partition_splitter: PartitionSplitter,
    unique_column_ids: Vec<usize>,
    writer_builder: B,
}

impl<B: RecordBatchWriterBuilder> PartitionedUpsertWriter<B>
where
    B::R: SingletonWriter,
{
    pub fn new(
        table_config: TableConfigRef,
        unique_column_ids: Vec<usize>,
        schema: SchemaRef,
        partition_splitter: PartitionSplitter,
        writer_builder: B,
    ) -> Self {
        Self {
            table_config,
            schema,
            writers: HashMap::new(),
            partition_splitter,
            unique_column_ids,
            writer_builder,
        }
    }

    async fn get_writer_partition_key(
        &mut self,
        partition_key: PartitionKey,
    ) -> Result<&mut EqualityDeltaWriter<B>> {
        match self.writers.entry(partition_key) {
            Entry::Vacant(v) => {
                let writer = EqualityDeltaWriter::try_new(
                    self.schema.clone(),
                    self.table_config.clone(),
                    self.unique_column_ids.clone(),
                    self.writer_builder.clone(),
                )
                .await?;
                Ok(v.insert(writer))
            }
            Entry::Occupied(v) => Ok(v.into_mut()),
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let partitions = self.partition_splitter.split_by_partition(&batch)?;
        for (partition_key, batch) in partitions {
            self.get_writer_partition_key(partition_key)
                .await?
                .write(batch)
                .await?;
        }
        Ok(())
    }

    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let partitions = self.partition_splitter.split_by_partition(&batch)?;
        for (partition_key, batch) in partitions {
            self.get_writer_partition_key(partition_key)
                .await?
                .delete(batch)
                .await?;
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DeltaWriterResult>> {
        let mut res = Vec::with_capacity(self.writers.len());
        for (partition_key, writer) in self.writers.drain() {
            let partition_value = self
                .partition_splitter
                .convert_key_to_value(partition_key)?;
            let delta_result = writer.close(Some(partition_value)).await?;
            res.push(delta_result);
        }
        Ok(res)
    }
}
