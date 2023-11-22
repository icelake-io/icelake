//! This module provide `WriterBuilder`.
use std::sync::Arc;

use crate::types::Any;
use crate::Result;
use crate::{config::TableConfigRef, types::TableMetadata};
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::file_writer::{new_eq_delete_writer, EqualityDeleteWriter, SortedPositionDeleteWriter};
use super::location_generator::FileLocationGenerator;
use super::{
    DispatcherWriterBuilder, EqualityDeltaWriter, FileWriter, PartitionedWriterBuilder,
    RollingWriterBuilder, SingletonWriter, UpsertWriter, WriterBuilder,
};

/// `WriterBuilder` used to create kinds of writer.
pub struct WriterFactory {
    table_metadata: TableMetadata,
    cur_arrow_schema: SchemaRef,
    operator: Operator,
    partition_id: usize,
    task_id: usize,
    table_config: TableConfigRef,
}

impl WriterFactory {
    pub fn new(
        table_metadata: TableMetadata,
        operator: Operator,
        task_id: usize,
        table_config: TableConfigRef,
    ) -> Result<WriterFactory> {
        let cur_arrow_schema = Arc::new(
            table_metadata
                .current_schema()?
                .clone()
                .try_into()
                .map_err(|e| {
                    crate::error::Error::new(
                        crate::ErrorKind::IcebergDataInvalid,
                        format!("Can't convert iceberg schema to arrow schema: {}", e),
                    )
                })?,
        );

        Ok(WriterFactory {
            table_metadata,
            operator,
            cur_arrow_schema,
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

    fn data_location_generator(&self, suffix: Option<String>) -> Result<FileLocationGenerator> {
        FileLocationGenerator::try_new(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            suffix,
        )
    }

    pub fn rolling_writer_builder(
        &self,
        file_name_suffix: Option<String>,
    ) -> Result<RollingWriterBuilder> {
        Ok(RollingWriterBuilder::new(
            self.operator.clone(),
            self.data_location_generator(file_name_suffix)?.into(),
            self.table_config.rolling_writer.clone(),
            self.table_config.parquet_writer.clone(),
        ))
    }

    pub fn partition_writer_builder<B: WriterBuilder>(
        &self,
        inner_builder: B,
        is_upsert: bool,
    ) -> Result<PartitionedWriterBuilder<B>> {
        let partition_spec = self.table_metadata.current_partition_spec()?;
        let partition_type = Any::Struct(
            partition_spec
                .partition_type(self.table_metadata.current_schema()?)?
                .into(),
        );
        Ok(PartitionedWriterBuilder::new(
            inner_builder,
            partition_type,
            partition_spec.clone(),
            is_upsert,
        ))
    }

    pub fn dispatcher_writer_builder<P: WriterBuilder, UP: WriterBuilder>(
        &self,
        partitioned_builder: P,
        unpartitioned_builder: UP,
    ) -> Result<DispatcherWriterBuilder<P, UP>> {
        let partition_spec = self.table_metadata.current_partition_spec()?;
        Ok(DispatcherWriterBuilder::new(
            !partition_spec.is_unpartitioned(),
            partitioned_builder,
            unpartitioned_builder,
        ))
    }

    /// Build a `PositionDeleteWriter`.
    pub async fn build_sorted_position_delete_writer<B: WriterBuilder>(
        self,
        builder: B,
    ) -> Result<SortedPositionDeleteWriter<B>>
    where
        B::R: FileWriter,
    {
        Ok(SortedPositionDeleteWriter::new(self.table_config, builder))
    }

    /// Build a `EqualityDeleteWriter`.
    pub async fn build_equality_delete_writer<B: WriterBuilder>(
        self,
        equality_ids: Vec<i32>,
        builder: B,
    ) -> Result<EqualityDeleteWriter<B::R>>
    where
        B::R: FileWriter,
    {
        new_eq_delete_writer(self.cur_arrow_schema, equality_ids, builder).await
    }

    /// Build a `EqualityDeltaWriter`.
    pub async fn build_equality_delta_writer<B: WriterBuilder>(
        self,
        unique_column_ids: Vec<i32>,
        builder: B,
    ) -> Result<EqualityDeltaWriter<B>>
    where
        B::R: SingletonWriter + FileWriter,
    {
        EqualityDeltaWriter::try_new(
            self.cur_arrow_schema,
            self.table_config,
            unique_column_ids,
            builder,
        )
        .await
    }

    pub async fn build_upsert_writer<B: WriterBuilder>(
        self,
        unique_column_ids: Vec<i32>,
        builder: B,
    ) -> Result<UpsertWriter<B>>
    where
        B::R: SingletonWriter + FileWriter,
    {
        UpsertWriter::try_new(
            self.table_metadata,
            self.table_config,
            unique_column_ids,
            builder,
        )
        .await
    }
}
