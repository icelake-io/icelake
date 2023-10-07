//! This module provide `WriterBuilder`.
use std::sync::Arc;

use crate::Result;
use crate::{config::TableConfigRef, types::TableMetadata};
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::file_writer::{
    new_eq_delete_writer, EqualityDeleteWriter, EqualityDeltaWriter, SortedPositionDeleteWriter,
};
use super::location_generator::FileLocationGenerator;
use super::task_writer::TaskWriter;
use super::{
    new_file_appender_builder, ChainedFileAppenderLayer, DefaultFileAppender, EmptyLayer,
    FileAppenderBuilder, FileAppenderLayer,
};

/// `WriterBuilder` used to create kinds of writer.
pub struct WriterBuilder<L: FileAppenderLayer<DefaultFileAppender>> {
    table_metadata: TableMetadata,
    cur_arrow_schema: SchemaRef,
    operator: Operator,
    partition_id: usize,
    task_id: usize,
    table_config: TableConfigRef,
    table_location: String,
    suffix: Option<String>,

    file_appender_builder: FileAppenderBuilder<L>,
}

pub async fn new_writer_builder(
    table_metadata: TableMetadata,
    operator: Operator,
    task_id: usize,
    table_config: TableConfigRef,
) -> Result<WriterBuilder<EmptyLayer>> {
    let table_location = table_metadata.location.clone();
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

    let file_appender_builder = new_file_appender_builder(
        operator.clone(),
        table_location.clone(),
        Arc::new(FileLocationGenerator::try_new_for_data_file(
            &table_metadata,
            0,
            task_id,
            None,
        )?),
        table_config.clone(),
    );

    Ok(WriterBuilder {
        table_metadata,
        operator,
        cur_arrow_schema,
        partition_id: 0,
        task_id,
        table_config,
        table_location,
        suffix: None,
        file_appender_builder,
    })
}

impl<L: FileAppenderLayer<DefaultFileAppender>> WriterBuilder<L> {
    /// Add suffix for file name.
    pub fn with_suffix(self, suffix: String) -> Self {
        Self {
            suffix: Some(suffix),
            ..self
        }
    }

    /// Add partition_id for file name.
    pub fn with_partition_id(self, partition_id: usize) -> Self {
        Self {
            partition_id,
            ..self
        }
    }

    pub fn with_file_appender_layer<L1: FileAppenderLayer<L::R>>(
        self,
        layer: L1,
    ) -> WriterBuilder<ChainedFileAppenderLayer<L, DefaultFileAppender, L1>> {
        WriterBuilder {
            table_metadata: self.table_metadata,
            operator: self.operator,
            partition_id: self.partition_id,
            task_id: self.task_id,
            table_config: self.table_config,
            table_location: self.table_location,
            suffix: self.suffix,
            cur_arrow_schema: self.cur_arrow_schema,
            file_appender_builder: self.file_appender_builder.layer(layer),
        }
    }

    /// Build a `PositionDeleteWriter`.
    pub async fn build_sorted_position_delete_writer(self) -> Result<SortedPositionDeleteWriter> {
        let location_generator = FileLocationGenerator::try_new_for_delete_file(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            self.suffix,
        )?
        .into();
        Ok(SortedPositionDeleteWriter::new(
            self.operator,
            self.table_location,
            location_generator,
            self.table_config,
        ))
    }

    /// Build a `EqualityDeleteWriter`.
    pub async fn build_equality_delete_writer(
        self,
        equality_ids: Vec<usize>,
    ) -> Result<EqualityDeleteWriter<L::R>> {
        new_eq_delete_writer(
            self.cur_arrow_schema,
            equality_ids,
            &self.file_appender_builder,
        )
        .await
    }

    /// Build a `EqualityDeltaWriter`.
    pub async fn build_equality_delta_writer(
        self,
        unique_column_ids: Vec<usize>,
    ) -> Result<EqualityDeltaWriter<FileAppenderBuilder<L>>> {
        let delete_location_generator = FileLocationGenerator::try_new_for_delete_file(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            self.suffix,
        )?
        .into();
        EqualityDeltaWriter::try_new(
            self.operator,
            self.table_location,
            delete_location_generator,
            self.cur_arrow_schema,
            self.table_config,
            unique_column_ids,
            self.file_appender_builder,
        )
        .await
    }

    pub async fn build_task_writer(self) -> Result<TaskWriter<FileAppenderBuilder<L>>> {
        TaskWriter::try_new(
            self.table_metadata,
            self.operator,
            self.partition_id,
            self.task_id,
            self.suffix,
            self.table_config,
            self.file_appender_builder,
        )
        .await
    }
}
