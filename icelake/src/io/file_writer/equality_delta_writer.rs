//! This module provide the `EqualityDeltaWriter`.
use crate::io::DefaultFileAppender;
use crate::io::FileAppenderBuilder;
use crate::io::FileAppenderLayer;
use crate::types::DataFile;
use crate::types::StructValue;
use crate::types::COLUMN_ID_META_KEY;
use crate::{config::TableConfigRef, io::location_generator::FileLocationGenerator};
use crate::{Error, ErrorKind, Result};
use arrow_array::builder::BooleanBuilder;
use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::filter;
use opendal::Operator;
use std::collections::HashMap;
use std::sync::Arc;

use super::new_eq_delete_writer;
use super::{DataFileWriter, EqualityDeleteWriter, SortedPositionDeleteWriter};

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
/// EqualityDeltaWriter will gurantee that there is only one row with the unique columns value written. When
/// insert a row with the same unique columns value, it will delete the previous row.
///
/// NOTE:
/// This write is not as same with `upsert`. If the row with same unique columns is not written in
/// this writer, it will not delete it.
pub struct EqualityDeltaWriter<L: FileAppenderLayer<DefaultFileAppender>> {
    data_file_writer: DataFileWriter<L::R>,
    sorted_pos_delete_writer: SortedPositionDeleteWriter,
    eq_delete_writer: EqualityDeleteWriter<L::R>,
    inserted_rows: HashMap<OwnedRow, PathOffset>,
    row_converter: RowConverter,
    unique_column_idx: Vec<usize>,
    file_appender_factory: FileAppenderBuilder<L>,
}

pub struct EqDeltaWriterMetrics {
    buffer_path_offset_count: usize,
    sorted_pos_delete_cache_count: usize,
}

impl<L: FileAppenderLayer<DefaultFileAppender>> EqualityDeltaWriter<L> {
    /// Create a new `EqualityDeltaWriter`.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        delete_location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
        unique_column_ids: Vec<usize>,
        file_appender_factory: FileAppenderBuilder<L>,
    ) -> Result<Self> {
        // Convert column id into corresponding index.
        let mut unique_column_idx = Vec::with_capacity(unique_column_ids.len());
        for &id in unique_column_ids.iter() {
            let res = arrow_schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| {
                    f.metadata()
                        .get(COLUMN_ID_META_KEY)
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                        == id
                })
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "Failed to find column with id: {} in schema {:?}",
                            id, arrow_schema
                        ),
                    )
                })?;
            unique_column_idx.push(res.0);
        }

        // Create the row converter for unique columns.
        let unique_col_schema = arrow_schema.project(&unique_column_idx).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!(
                    "Failed to project schema with equality ids: {:?}, error: {}",
                    unique_column_idx, err
                ),
            )
        })?;
        let row_converter = RowConverter::new(
            unique_col_schema
                .fields()
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
                file_appender_factory.build(arrow_schema.clone()).await?,
            )?,
            sorted_pos_delete_writer: SortedPositionDeleteWriter::new(
                operator.clone(),
                table_location.clone(),
                delete_location_generator.clone(),
                table_config.clone(),
            ),
            eq_delete_writer: new_eq_delete_writer(
                arrow_schema.clone(),
                unique_column_ids,
                &file_appender_factory,
            )
            .await?,
            inserted_rows: HashMap::new(),
            row_converter,
            unique_column_idx,
            file_appender_factory,
        })
    }

    pub fn current_metrics(&self) -> EqDeltaWriterMetrics {
        EqDeltaWriterMetrics {
            buffer_path_offset_count: self.inserted_rows.len(),
            sorted_pos_delete_cache_count: self.sorted_pos_delete_writer.record_num,
        }
    }
    /// Delta write will write and delete the row automatically according to the ops.
    /// 1. If op == 1, write the row.
    /// 2. If op == 2, delete the row.
    /// This interface will batch the row automatically. E.g.
    /// ```ignore
    /// | ops | batch |
    /// |  1  |  "a"  |
    /// |  1  |  "b"  |
    /// |  2  |  "c"  |
    /// |  2  |  "d"  |
    /// ```
    /// It will write "a" and "b" together.
    /// It will delete "c" and "d" together.
    pub async fn delta_write(&mut self, ops: Vec<i32>, batch: RecordBatch) -> Result<()> {
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
                1 => self.write(batch).await?,
                // Delete
                2 => self.delete(batch).await?,
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

    /// Write the batch.
    /// 1. If a row wtih the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_file_writer.current_file();
        let current_file_offset = self.data_file_writer.current_row();
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
                    .delete(
                        previous_row_offset.path,
                        previous_row_offset.offset as i64,
                        &self.file_appender_factory,
                    )
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
                    .delete(
                        previous_row_offset.path,
                        previous_row_offset.offset as i64,
                        &self.file_appender_factory,
                    )
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
    pub async fn close(self, partition_value: Option<StructValue>) -> Result<DeltaWriterResult> {
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
            .close(&self.file_appender_factory)
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
        let batch_with_unique_column = batch.project(&self.unique_column_idx).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!(
                    "Failed to project record batch with index: {:?}, error: {}",
                    self.unique_column_idx, err
                ),
            )
        })?;
        self.row_converter
            .convert_columns(batch_with_unique_column.columns())
            .map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}
