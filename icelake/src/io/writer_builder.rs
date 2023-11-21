//! This module provide `WriterBuilder`.
use std::sync::Arc;

use crate::Result;
use crate::{config::TableConfigRef, types::TableMetadata};
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::file_writer::{new_eq_delete_writer, EqualityDeleteWriter, SortedPositionDeleteWriter};
use super::location_generator::FileLocationGenerator;
use super::{
    AppendOnlyWriter, EqualityDeltaWriter, RecordBatchWriterBuilder, RollingWriterBuilder,
    SingletonWriter, UpsertWriter,
};

/// `WriterBuilder` used to create kinds of writer.
pub struct WriterBuilder {
    table_metadata: TableMetadata,
    cur_arrow_schema: SchemaRef,
    operator: Operator,
    partition_id: usize,
    task_id: usize,
    table_config: TableConfigRef,
}

impl WriterBuilder {
    pub fn new(
        table_metadata: TableMetadata,
        operator: Operator,
        task_id: usize,
        table_config: TableConfigRef,
    ) -> Result<WriterBuilder> {
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

        Ok(WriterBuilder {
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

    /// Build a `PositionDeleteWriter`.
    pub async fn build_sorted_position_delete_writer<B: RecordBatchWriterBuilder>(
        self,
        builder: B,
    ) -> Result<SortedPositionDeleteWriter<B>> {
        Ok(SortedPositionDeleteWriter::new(self.table_config, builder))
    }

    /// Build a `EqualityDeleteWriter`.
    pub async fn build_equality_delete_writer<B: RecordBatchWriterBuilder>(
        self,
        equality_ids: Vec<usize>,
        builder: B,
    ) -> Result<EqualityDeleteWriter<B::R>> {
        new_eq_delete_writer(self.cur_arrow_schema, equality_ids, builder).await
    }

    /// Build a `EqualityDeltaWriter`.
    pub async fn build_equality_delta_writer<B: RecordBatchWriterBuilder>(
        self,
        unique_column_ids: Vec<usize>,
        builder: B,
    ) -> Result<EqualityDeltaWriter<B>>
    where
        B::R: SingletonWriter,
    {
        EqualityDeltaWriter::try_new(
            self.cur_arrow_schema,
            self.table_config,
            unique_column_ids,
            builder,
        )
        .await
    }

    pub async fn build_append_only_writer<B: RecordBatchWriterBuilder>(
        self,
        builder: B,
    ) -> Result<AppendOnlyWriter<B>> {
        AppendOnlyWriter::try_new(self.table_metadata, builder).await
    }

    pub async fn build_upsert_writer<B: RecordBatchWriterBuilder>(
        self,
        unique_column_ids: Vec<usize>,
        builder: B,
    ) -> Result<UpsertWriter<B>>
    where
        B::R: SingletonWriter,
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
