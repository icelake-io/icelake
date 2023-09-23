//! This module provide `WriterBuilder`.
use std::sync::Arc;

use crate::Result;
use crate::{config::TableConfigRef, types::TableMetadata};
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::file_writer::{EqualityDeleteWriter, EqualityDeltaWriter, SortedPositionDeleteWriter};
use super::location_generator::FileLocationGenerator;

/// `WriterBuilder` used to create kinds of writer.
pub struct WriterBuilder {
    table_metadata: TableMetadata,
    current_arrow_schema: SchemaRef,
    operator: Operator,
    partition_id: usize,
    task_id: usize,
    table_config: TableConfigRef,
    table_location: String,
    suffix: Option<String>,
}

impl WriterBuilder {
    /// Try to create a new `WriterBuilder`.
    pub async fn try_new(
        table_metadata: TableMetadata,
        operator: Operator,
        task_id: usize,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let table_location = table_metadata.location.clone();

        let current_arrow_schema = Arc::new(
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

        Ok(Self {
            table_metadata,
            current_arrow_schema,
            operator,
            partition_id: 0,
            task_id,
            table_config,
            table_location,
            suffix: None,
        })
    }

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
    ) -> Result<EqualityDeleteWriter> {
        let location_generator = FileLocationGenerator::try_new_for_delete_file(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            self.suffix,
        )?
        .into();
        EqualityDeleteWriter::try_new(
            self.operator,
            self.table_location,
            location_generator,
            self.current_arrow_schema,
            self.table_config,
            equality_ids,
        )
        .await
    }

    /// Build a `EqualityDeltaWriter`.
    pub async fn build_equality_delta_writer(
        self,
        primary_column_ids: Vec<usize>,
    ) -> Result<EqualityDeltaWriter> {
        let data_location_generator = FileLocationGenerator::try_new_for_data_file(
            &self.table_metadata,
            self.partition_id,
            self.task_id,
            self.suffix.clone(),
        )?
        .into();
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
            data_location_generator,
            delete_location_generator,
            self.current_arrow_schema,
            self.table_config,
            primary_column_ids,
        )
        .await
    }
}
