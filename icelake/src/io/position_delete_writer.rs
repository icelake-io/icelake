//! A module provide `PositionDeleteWriter`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::TableConfigRef;
use crate::types::{Any, DataFile, Field, Primitive, Schema, StructValue};
use crate::{types::Struct, Result};
use arrow_array::RecordBatch;
use opendal::Operator;
use parquet::format::FileMetaData;

use super::{location_generator::DataFileLocationGenerator, rolling_writer::RollingWriter};

/// A writer capable of splitting incoming delete into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
///
///
/// # NOTE
/// According to spec, The data write to position delete file should be:
/// - Sorting by file_path allows filter pushdown by file in columnar storage formats.
/// - Sorting by pos allows filtering rows while scanning, to avoid keeping deletes in memory.
/// - They're belong to partition.
///
/// But PositionDeleteWriter will not gurantee and check above. It is the caller's responsibility to gurantee them.
pub struct PositionDeleteWriter {
    inner_writer: RollingWriter,
    table_location: String,
}

impl PositionDeleteWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<DataFileLocationGenerator>,
        row_type: Option<Arc<Struct>>,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let mut fields = vec![
            Arc::new(Field::required(
                2147483546,
                "file_path",
                Any::Primitive(Primitive::String),
            )),
            Arc::new(Field::required(
                2147483545,
                "pos",
                Any::Primitive(Primitive::Long),
            )),
        ];
        if let Some(row_type) = row_type {
            fields.push(Arc::new(Field::required(
                2147483544,
                "row",
                Any::Struct(row_type),
            )));
        }
        let schema = Arc::new(Schema::new(1, None, Struct::new(fields)).try_into()?);
        Ok(Self {
            inner_writer: RollingWriter::try_new(
                operator,
                location_generator,
                schema,
                table_config,
            )
            .await?,
            table_location,
        })
    }

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await?;
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        let table_location = self.table_location;
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|meta| {
                Self::convert_meta_to_datafile(
                    meta.meta_data,
                    meta.written_size,
                    &table_location,
                    &meta.location,
                )
            })
            .collect())
    }

    fn convert_meta_to_datafile(
        meta_data: FileMetaData,
        written_size: u64,
        table_location: &str,
        current_location: &str,
    ) -> DataFile {
        log::info!("{meta_data:?}");
        let (column_sizes, value_counts, null_value_counts, distinct_counts) = {
            // how to decide column id
            let mut per_col_size: HashMap<i32, _> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_distinct_val_num: HashMap<i32, _> = HashMap::new();
            meta_data.row_groups.iter().for_each(|group| {
                group
                    .columns
                    .iter()
                    .enumerate()
                    .for_each(|(column_id, column_chunk)| {
                        if let Some(column_chunk_metadata) = &column_chunk.meta_data {
                            *per_col_size.entry(column_id as i32).or_insert(0) +=
                                column_chunk_metadata.total_compressed_size;
                            *per_col_val_num.entry(column_id as i32).or_insert(0) +=
                                column_chunk_metadata.num_values;
                            *per_col_null_val_num
                                .entry(column_id as i32)
                                .or_insert(0_i64) += column_chunk_metadata
                                .statistics
                                .as_ref()
                                .map(|s| s.null_count)
                                .unwrap_or(None)
                                .unwrap_or(0);
                            *per_col_distinct_val_num
                                .entry(column_id as i32)
                                .or_insert(0_i64) += column_chunk_metadata
                                .statistics
                                .as_ref()
                                .map(|s| s.distinct_count)
                                .unwrap_or(None)
                                .unwrap_or(0);
                        }
                    })
            });
            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                per_col_distinct_val_num,
            )
        };
        DataFile {
            content: crate::types::DataContentType::PostionDeletes,
            file_path: format!("{}/{}", table_location, current_location),
            file_format: crate::types::DataFileFormat::Parquet,
            // /// # NOTE
            // ///
            // /// DataFileWriter only response to write data. Partition should place by more high level writer.
            partition: StructValue::default(),
            record_count: meta_data.num_rows,
            column_sizes: Some(column_sizes),
            value_counts: Some(value_counts),
            null_value_counts: Some(null_value_counts),
            distinct_counts: Some(distinct_counts),
            key_metadata: meta_data.footer_signing_key_metadata,
            file_size_in_bytes: written_size as i64,
            /// # TODO
            ///
            /// Following fields unsupported now:
            /// - `file_size_in_bytes` can't get from `FileMetaData` now.
            /// - `file_offset` in `FileMetaData` always be None now.
            /// - `nan_value_counts` can't get from `FileMetaData` now.
            // Currently arrow parquet writer doesn't fill row group offsets, we can use first column chunk offset for it.
            split_offsets: Some(meta_data
                .row_groups
                .iter()
                .filter_map(|group| group.file_offset)
                .collect()),
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            equality_ids: None,
            sort_order_id: None,
        }
    }
}
