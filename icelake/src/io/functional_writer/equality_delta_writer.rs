//! This module provide the `EqualityDeltaWriter`.
use crate::config::TableConfigRef;
use crate::io::new_eq_delete_writer;
use crate::io::DataFileWriter;
use crate::io::EqualityDeleteWriter;
use crate::io::RecordBatchWriter;
use crate::io::RecordBatchWriterBuilder;
use crate::io::SingletonWriter;
use crate::io::SortedPositionDeleteWriter;
use crate::types::DataFile;
use crate::types::FieldProjector;
use crate::types::StructValue;
use crate::{Error, ErrorKind, Result};
use arrow_array::builder::BooleanBuilder;
use arrow_array::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::filter;
use std::collections::HashMap;

struct PathOffset {
    pub path: String,
    pub offset: usize,
}

/// The close result return by DeltaWriter.
pub struct DeltaWriterResult {
    /// DataFileBuilder for written data.
    pub data: Vec<DataFile>,
    /// DataFileBuilder for position delete.
    pub pos_delete: Vec<DataFile>,
    /// DataFileBuilder for equality delete.
    pub eq_delete: Vec<DataFile>,
}

/// `EqualityDeltaWriter` is same as: https://github.com/apache/iceberg/blob/2e1ec5fde9e6fecfbc0883465a585a1dacb58c05/core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java#L108
///
/// EqualityDeltaWriter will guarantee that there is only one row with the unique columns value written. When
/// insert a row with the same unique columns value, it will delete the previous row.
///
/// NOTE:
/// This write is not as same with `upsert`. If the row with same unique columns is not written in
/// this writer, it will not delete it.
pub struct EqualityDeltaWriter<B: RecordBatchWriterBuilder>
where
    B::R: SingletonWriter,
{
    data_file_writer: DataFileWriter<B::R>,
    sorted_pos_delete_writer: SortedPositionDeleteWriter<B>,
    eq_delete_writer: EqualityDeleteWriter<B::R>,
    inserted_rows: HashMap<OwnedRow, PathOffset>,
    row_converter: RowConverter,
    col_extractor: FieldProjector,
}

pub struct EqDeltaWriterMetrics {
    pub buffer_path_offset_count: usize,
    pub sorted_pos_delete_cache_count: usize,
}

impl<B: RecordBatchWriterBuilder> EqualityDeltaWriter<B>
where
    B::R: SingletonWriter,
{
    /// Create a new `EqualityDeltaWriter`.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
        unique_column_ids: Vec<usize>,
        writer_builder: B,
    ) -> Result<Self> {
        let (col_extractor, unique_col_fields) =
            FieldProjector::new(arrow_schema.fields(), &unique_column_ids)?;

        // Create the row converter for unique columns.
        let row_converter = RowConverter::new(
            unique_col_fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!("Failed to create row converter, error: {}", err),
            )
        })?;

        Ok(Self {
            data_file_writer: DataFileWriter::try_new(
                writer_builder.clone().build(&arrow_schema.clone()).await?,
            )?,
            eq_delete_writer: new_eq_delete_writer(
                arrow_schema.clone(),
                unique_column_ids,
                writer_builder.clone(),
            )
            .await?,
            sorted_pos_delete_writer: SortedPositionDeleteWriter::new(
                table_config.clone(),
                writer_builder,
            ),
            inserted_rows: HashMap::new(),
            row_converter,
            col_extractor,
        })
    }

    pub fn current_metrics(&self) -> EqDeltaWriterMetrics {
        EqDeltaWriterMetrics {
            buffer_path_offset_count: self.inserted_rows.len(),
            sorted_pos_delete_cache_count: self.sorted_pos_delete_writer.current_record_num(),
        }
    }

    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_file_writer.current_file();
        let current_file_offset = self.data_file_writer.current_row_num();
        for (idx, row) in rows.iter().enumerate() {
            let previous_row_offset = self.inserted_rows.insert(
                row.owned(),
                PathOffset {
                    path: current_file_path.clone(),
                    offset: current_file_offset + idx,
                },
            );
            if let Some(previous_row_offset) = previous_row_offset {
                self.sorted_pos_delete_writer
                    .delete(previous_row_offset.path, previous_row_offset.offset as i64)
                    .await?;
            }
        }

        self.data_file_writer.write(batch).await?;

        Ok(())
    }

    /// Delete the batch.
    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::new();
        for row in rows.iter() {
            if let Some(previous_row_offset) = self.inserted_rows.remove(&row.owned()) {
                self.sorted_pos_delete_writer
                    .delete(previous_row_offset.path, previous_row_offset.offset as i64)
                    .await?;
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }
        let delete_batch =
            filter::filter_record_batch(&batch, &delete_row.finish()).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to filter record batch, error: {}", err),
                )
            })?;
        self.eq_delete_writer.write(delete_batch).await?;
        Ok(())
    }

    /// Close the writer and return the result.
    pub async fn close(
        mut self,
        partition_value: Option<StructValue>,
    ) -> Result<DeltaWriterResult> {
        let data = self
            .data_file_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_partition_value(partition_value.clone())
                    .build()
            })
            .collect();
        let pos_delete = self
            .sorted_pos_delete_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_partition_value(partition_value.clone())
                    .build()
            })
            .collect();
        let eq_delete = self
            .eq_delete_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_partition_value(partition_value.clone())
                    .build()
            })
            .collect();
        Ok(DeltaWriterResult {
            data,
            pos_delete,
            eq_delete,
        })
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.col_extractor.project(batch.columns()))
            .map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}
