//! A module provide `DataFileWriter`.
use std::{collections::HashMap, sync::Arc};

use crate::config::TableConfigRef;
use crate::{
    types::{DataFile, StructValue},
    Result,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Operator;
use parquet::format::FileMetaData;

use super::location_generator::DataFileLocationGenerator;
use super::rolling_writer::RollingWriter;

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
///
/// # NOTE
/// This writer will not gurantee the writen data is within one spec/partition. It is the caller's responsibility to make sure the data is within one spec/partition.
pub struct DataFileWriter {
    inner_writer: RollingWriter,
    table_location: String,
}

impl DataFileWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<DataFileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            inner_writer: RollingWriter::try_new(
                operator,
                location_generator,
                arrow_schema,
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
            content: crate::types::DataContentType::Data,
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

#[cfg(test)]
mod test {
    use std::{env, fs, sync::Arc};

    use arrow_array::RecordBatch;
    use arrow_array::{ArrayRef, Int64Array};
    use bytes::Bytes;
    use opendal::{services::Memory, Operator};

    use anyhow::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use crate::config::TableConfig;
    use crate::{
        io::{data_file_writer, location_generator::DataFileLocationGenerator},
        types::parse_table_metadata,
    };

    #[tokio::test]
    async fn tets_data_file_writer() -> Result<()> {
        let mut builder = Memory::default();
        builder.root("/tmp/table");
        let op = Operator::new(builder)?.finish();

        let location_generator = {
            let mut metadata = {
                let path = format!(
                    "{}/../testdata/simple_table/metadata/v1.metadata.json",
                    env!("CARGO_MANIFEST_DIR")
                );

                let bs = fs::read(path).expect("read_file must succeed");

                parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
            };
            metadata.location = "/tmp/table".to_string();

            DataFileLocationGenerator::try_new(&metadata, 0, 0, None)?
        };

        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col.clone())]).unwrap();

        let mut writer = data_file_writer::DataFileWriter::try_new(
            op.clone(),
            "/tmp/table".to_string(),
            location_generator.into(),
            to_write.schema(),
            Arc::new(TableConfig::default()),
        )
        .await?;

        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        let data_files = writer.close().await?;

        let mut row_num = 0;
        for data_file in data_files {
            let res = op
                .read(data_file.file_path.strip_prefix("/tmp/table").unwrap())
                .await?;
            let res = Bytes::from(res);
            let reader = ParquetRecordBatchReaderBuilder::try_new(res)
                .unwrap()
                .build()
                .unwrap();
            reader.into_iter().for_each(|batch| {
                row_num += batch.unwrap().num_rows();
            });
        }
        assert_eq!(row_num, 1024 * 1024 * 3);

        Ok(())
    }
}
