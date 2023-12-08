use crate::io_v2::{IcebergWriter, IcebergWriterBuilder};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// DispatcherWriter used to dispatch the partitioned and unpartitioned writer so that user can store them as a single writer.
#[derive(Clone)]
pub struct DispatcherWriterBuilder<UP: IcebergWriterBuilder, P: IcebergWriterBuilder> {
    is_partition: bool,
    partition_builder: P,
    no_partition_builder: UP,
}

impl<
        UP: IcebergWriterBuilder,
        P: IcebergWriterBuilder<R = impl IcebergWriter<R = <UP::R as IcebergWriter>::R>>,
    > DispatcherWriterBuilder<UP, P>
{
    pub fn new(is_partition: bool, partition_builder: P, no_partition_builder: UP) -> Self {
        Self {
            is_partition,
            partition_builder,
            no_partition_builder,
        }
    }
}

#[async_trait::async_trait]
impl<
        UP: IcebergWriterBuilder,
        P: IcebergWriterBuilder<R = impl IcebergWriter<R = <UP::R as IcebergWriter>::R>>,
    > IcebergWriterBuilder for DispatcherWriterBuilder<UP, P>
{
    type R = DispatcherWriter<UP::R, P::R>;

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

pub enum DispatcherWriter<UP: IcebergWriter, P: IcebergWriter<R = UP::R>> {
    Partition(P),
    Unpartition(UP),
}

#[async_trait::async_trait]
impl<UP: IcebergWriter, P: IcebergWriter<R = UP::R>> IcebergWriter for DispatcherWriter<UP, P> {
    type R = UP::R;

    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        match self {
            DispatcherWriter::Partition(writer) => writer.write(input).await,
            DispatcherWriter::Unpartition(writer) => writer.write(input).await,
        }
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        match self {
            DispatcherWriter::Partition(writer) => writer.flush().await,
            DispatcherWriter::Unpartition(writer) => writer.flush().await,
        }
    }
}
