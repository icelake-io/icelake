//! This module provide `WriterBuilder`.
use crate::types::Any;
use crate::Result;
use crate::{config::TableConfigRef, types::TableMetadata};
use opendal::Operator;

use super::location_generator::{FileLocationGenerator, FileLocationGeneratorRef};
use super::{
    BaseFileWriterBuilder, DataFileWriterBuilder, DispatcherWriterBuilder,
    EqualityDeleteWriterBuilder, FanoutPartitionedWriterBuilder, FileWriterBuilder, IcebergWriter,
    IcebergWriterBuilder, ParquetWriterBuilder, PositionDeleteWriterBuilder,
};

/// `WriterBuilderHelper` used to create kinds of writer builder.
pub struct WriterBuilderHelper {
    table_metadata: TableMetadata,
    operator: Operator,
    partition_id: usize,
    task_id: usize,
    table_config: TableConfigRef,
}

impl WriterBuilderHelper {
    pub fn new(
        table_metadata: TableMetadata,
        operator: Operator,
        task_id: usize,
        table_config: TableConfigRef,
    ) -> Result<WriterBuilderHelper> {
        Ok(WriterBuilderHelper {
            table_metadata,
            operator,
            partition_id: 0,
            task_id,
            table_config,
        })
    }

    /// Add partition_id for file name.
    pub fn with_partition_id(self, partition_id: usize) -> Self {
        Self {
            partition_id,
            ..self
        }
    }

    fn data_location_generator(&self, suffix: Option<String>) -> Result<FileLocationGeneratorRef> {
        Ok(FileLocationGenerator::try_new(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            suffix,
        )?
        .into())
    }

    pub fn parquet_writer_builder(
        &self,
        init_buffer_size: usize,
        file_name_suffix: Option<String>,
    ) -> Result<ParquetWriterBuilder<FileLocationGeneratorRef>> {
        Ok(ParquetWriterBuilder::new(
            self.operator.clone(),
            init_buffer_size,
            self.table_config.parquet_writer.clone(),
            self.table_metadata.location.clone(),
            self.data_location_generator(file_name_suffix)?,
        ))
    }

    pub fn rolling_writer_builder<B: FileWriterBuilder>(
        &self,
        inner_builder: B,
    ) -> Result<BaseFileWriterBuilder<B>> {
        Ok(BaseFileWriterBuilder::new(
            Some(self.table_config.rolling_writer.clone()),
            inner_builder,
        ))
    }

    pub fn simple_writer_builder<B: FileWriterBuilder>(
        &self,
        inner_builder: B,
    ) -> Result<BaseFileWriterBuilder<B>> {
        Ok(BaseFileWriterBuilder::new(None, inner_builder))
    }

    pub fn fanout_partition_writer_builder<B: IcebergWriterBuilder>(
        &self,
        inner_builder: B,
    ) -> Result<FanoutPartitionedWriterBuilder<B>> {
        let partition_spec = self.table_metadata.current_partition_spec()?;
        let partition_type = Any::Struct(
            partition_spec
                .partition_type(self.table_metadata.current_schema()?)?
                .into(),
        );
        Ok(FanoutPartitionedWriterBuilder::new(
            inner_builder,
            partition_type,
            partition_spec.clone(),
        ))
    }

    pub fn dispatcher_writer_builder<
        P: IcebergWriterBuilder<R = impl IcebergWriter<R = <UP::R as IcebergWriter>::R>>,
        UP: IcebergWriterBuilder,
    >(
        &self,
        partitioned_builder: P,
        unpartitioned_builder: UP,
    ) -> Result<DispatcherWriterBuilder<UP, P>>
    where
        UP::R: IcebergWriter,
        P::R: IcebergWriter,
    {
        let partition_spec = self.table_metadata.current_partition_spec()?;
        Ok(DispatcherWriterBuilder::new(
            !partition_spec.is_unpartitioned(),
            partitioned_builder,
            unpartitioned_builder,
        ))
    }

    pub fn position_delete_writer_builder(
        &self,
        init_buffer_size: usize,
        cache_num: usize,
    ) -> Result<
        PositionDeleteWriterBuilder<
            BaseFileWriterBuilder<ParquetWriterBuilder<FileLocationGeneratorRef>>,
        >,
    > {
        Ok(PositionDeleteWriterBuilder::new(
            self.simple_writer_builder(
                self.parquet_writer_builder(init_buffer_size, Some("pos-del".to_string()))?,
            )?,
            cache_num,
        ))
    }

    pub fn data_file_writer_builder(
        &self,
        init_buffer_size: usize,
    ) -> Result<
        DataFileWriterBuilder<
            BaseFileWriterBuilder<ParquetWriterBuilder<FileLocationGeneratorRef>>,
        >,
    > {
        Ok(DataFileWriterBuilder::new(self.rolling_writer_builder(
            self.parquet_writer_builder(init_buffer_size, None)?,
        )?))
    }

    pub fn equality_delete_writer_builder(
        &self,
        equality_ids: Vec<usize>,
        init_buffer_size: usize,
    ) -> Result<
        EqualityDeleteWriterBuilder<
            BaseFileWriterBuilder<ParquetWriterBuilder<FileLocationGeneratorRef>>,
        >,
    > {
        Ok(EqualityDeleteWriterBuilder::new(
            self.rolling_writer_builder(
                self.parquet_writer_builder(init_buffer_size, Some("eq-del".to_string()))?,
            )?,
            equality_ids,
        ))
    }
}
