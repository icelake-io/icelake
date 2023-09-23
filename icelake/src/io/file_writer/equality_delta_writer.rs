//! This module provide the `EqualityDeltaWriter`.
use crate::types::DataFile;
use crate::types::StructValue;
use crate::types::COLUMN_ID_META_KEY;
use crate::{config::TableConfigRef, io::location_generator::FileLocationGenerator};
use crate::{Error, ErrorKind, Result};
use arrow_array::builder::BooleanBuilder;
use arrow_array::RecordBatch;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::filter;
use opendal::Operator;
use std::collections::HashMap;
use std::sync::Arc;

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
/// EqualityDeltaWriter will indentify a row by primiary columns.
///
/// For write:
/// 1. If the primiary columns of a row is not exist in the `inserted_rows`, then insert the row and the postition of it into inserted_rows.
/// 2. If the primiary columns of a row is exist in the `inserted_rows`, then delete the row using
///    `SortedPositionDeleteWriter`.
/// NOTE:
/// This write is not as same with `upsert`. If the row with same primiary columns is not in the
/// `inserted_rows`, this writer will not delete it.
///
/// For delele:
/// It will check whether the delete row in `inserted_rows`, if so, it delete this row using `SortedPositionDeleteWriter`.
/// Otherwise, it will delete this row using `EqualityDeltaWriter`
pub struct EqualityDeltaWriter {
    data_file_writer: DataFileWriter,
    sorted_pos_delete_writer: SortedPositionDeleteWriter,
    eq_delete_writer: EqualityDeleteWriter,
    inserted_rows: HashMap<OwnedRow, PathOffset>,
    row_converter: RowConverter,
    primary_column_idx: Vec<usize>,
}

impl EqualityDeltaWriter {
    /// Create a new `EqualityDeltaWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        data_location_generator: Arc<FileLocationGenerator>,
        delete_location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
        primary_column_ids: Vec<usize>,
    ) -> Result<Self> {
        let mut primary_column_idx = vec![];
        for &id in primary_column_ids.iter() {
            arrow_schema.fields().iter().enumerate().any(|(idx, f)| {
                if f.metadata()
                    .get(COLUMN_ID_META_KEY)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
                    == id
                {
                    primary_column_idx.push(idx);
                    true
                } else {
                    false
                }
            });
        }
        let primary_schema = arrow_schema.project(&primary_column_idx).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!(
                    "Failed to project schema with equality ids: {:?}, error: {}",
                    primary_column_idx, err
                ),
            )
        })?;
        let row_converter = RowConverter::new(
            primary_schema
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
                operator.clone(),
                table_location.clone(),
                data_location_generator.clone(),
                arrow_schema.clone(),
                table_config.clone(),
            )
            .await?,
            sorted_pos_delete_writer: SortedPositionDeleteWriter::new(
                operator.clone(),
                table_location.clone(),
                delete_location_generator.clone(),
                table_config.clone(),
            ),
            eq_delete_writer: EqualityDeleteWriter::try_new(
                operator,
                table_location,
                delete_location_generator,
                arrow_schema,
                table_config,
                primary_column_ids,
            )
            .await?,
            inserted_rows: HashMap::new(),
            row_converter,
            primary_column_idx,
        })
    }

    /// Write the batch.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_primary_key(&batch)?;
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
                    .delete(previous_row_offset.path, previous_row_offset.offset as i64)
                    .await?;
            }
        }

        self.data_file_writer.write(batch).await?;

        Ok(())
    }

    /// Delete the batch.
    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_primary_key(&batch)?;
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

    fn extract_primary_key(&mut self, batch: &RecordBatch) -> Result<Rows> {
        let primary_key_batch = batch.project(&self.primary_column_idx).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!(
                    "Failed to project record batch with equality ids: {:?}, error: {}",
                    self.primary_column_idx, err
                ),
            )
        })?;
        self.row_converter
            .convert_columns(primary_key_batch.columns())
            .map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}
