use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::io::{RecordBatchWriter, RecordBatchWriterBuilder};
use crate::types::DataFileBuilder;
use crate::Result;

/// DispatcherWriter used to dispatch the partitioned and unpartitioned writer so that user can store them as a single writer.
#[derive(Clone)]
pub struct DispatcherWriterBuilder<L: RecordBatchWriterBuilder, R: RecordBatchWriterBuilder> {
    is_partition: bool,
    partition_builder: L,
    no_partition_builder: R,
}

impl<L: RecordBatchWriterBuilder, R: RecordBatchWriterBuilder> DispatcherWriterBuilder<L, R> {
    pub fn new(is_partition: bool, partition_builder: L, no_partition_builder: R) -> Self {
        Self {
            is_partition,
            partition_builder,
            no_partition_builder,
        }
    }
}

#[async_trait::async_trait]
impl<L: RecordBatchWriterBuilder, R: RecordBatchWriterBuilder> RecordBatchWriterBuilder
    for DispatcherWriterBuilder<L, R>
{
    type R = DispatcherWriter<L::R, R::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        match self.is_partition {
            true => Ok(DispatcherWriter::Partition(
                self.partition_builder.build(schema).await?,
            )),
            false => Ok(DispatcherWriter::Unpartition(
                self.no_partition_builder.build(schema).await?,
            )),
        }
    }
}

pub enum DispatcherWriter<L: RecordBatchWriter, R: RecordBatchWriter> {
    Partition(L),
    Unpartition(R),
}

#[async_trait::async_trait]
impl<UP: RecordBatchWriter, P: RecordBatchWriter> RecordBatchWriter for DispatcherWriter<UP, P> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            DispatcherWriter::Partition(writer) => writer.write(batch).await,
            DispatcherWriter::Unpartition(writer) => writer.write(batch).await,
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        match self {
            DispatcherWriter::Partition(writer) => writer.close().await,
            DispatcherWriter::Unpartition(writer) => writer.close().await,
        }
    }
}
