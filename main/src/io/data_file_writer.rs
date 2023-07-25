//! data_file is used create a data file writer to write data into files.

use std::collections::HashMap;

use crate::{
    types::{DataFile, StructValue},
    Result,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Operator;
use parquet::format::FileMetaData;

use super::{
    location_generator::DataFileLocationGenerator,
    parquet::{ParquetWriter, ParquetWriterBuilder},
};

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
pub struct DataFileWriter {
    operator: Operator,
    location_generator: DataFileLocationGenerator,
    arrow_schema: SchemaRef,

    rows_divisor: usize,
    target_file_size_in_bytes: u64,

    /// # TODO
    ///
    /// support to config `ParquetWriter` using `buffer_size` and writer properties.
    current_writer: Option<ParquetWriter>,
    current_row_num: usize,
    /// `current_location` used to clean up the file when no row is written to it.
    current_location: String,

    result: Vec<DataFile>,
}

impl DataFileWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        location_generator: DataFileLocationGenerator,
        arrow_schema: SchemaRef,
        rows_divisor: usize,
        target_file_size_in_bytes: u64,
    ) -> Result<Self> {
        let mut writer = Self {
            operator,
            location_generator,
            arrow_schema,
            rows_divisor,
            target_file_size_in_bytes,
            current_writer: None,
            current_row_num: 0,
            current_location: String::new(),
            result: vec![],
        };
        writer.open_new_writer().await?;
        Ok(writer)
    }

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.current_writer
            .as_mut()
            .expect("Should not be none here")
            .write(&batch)
            .await?;
        self.current_row_num += batch.num_rows();

        if self.should_split() {
            self.close_current_writer().await?;
            self.open_new_writer().await?;
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        self.close_current_writer().await?;
        Ok(self.result)
    }

    fn should_split(&self) -> bool {
        self.current_row_num % self.rows_divisor == 0
            && self.current_writer.as_ref().unwrap().get_written_size()
                >= self.target_file_size_in_bytes
    }

    async fn close_current_writer(&mut self) -> Result<()> {
        let current_writer = self.current_writer.take().expect("Should not be none here");
        let (meta_data, written_size) = current_writer.close().await?;

        // Check if this file is empty
        if meta_data.num_rows == 0 {
            self.operator
                .delete(&self.current_location)
                .await
                .expect("Delete file failed");
            return Ok(());
        }

        let file = self.convert_meta_to_datafile(meta_data, written_size);
        self.result.push(file);
        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        // open new write must call when current writer is closed or inited.
        assert!(self.current_writer.is_none());

        let location = self.location_generator.generate_name();
        let file_writer = self.operator.writer(&location).await?;
        let current_writer =
            ParquetWriterBuilder::new(file_writer, self.arrow_schema.clone()).build()?;
        self.current_writer = Some(current_writer);
        self.current_row_num = 0;
        self.current_location = location;
        Ok(())
    }

    /// # TODO
    ///
    /// This function may be refactor when we support more file format.
    fn convert_meta_to_datafile(&self, meta_data: FileMetaData, written_size: u64) -> DataFile {
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
            file_path: format!("{}/{}", self.operator.info().root(), self.current_location),
            file_format: crate::types::DataFileFormat::Parquet,
            /// # TODO
            ///
            /// Support write partition info to file.
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
            split_offsets: Some(
                meta_data
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            ),
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

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use opendal::{services::Memory, Operator};

    use anyhow::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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

            DataFileLocationGenerator::try_new(metadata, 0, 0, None)?
        };

        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col.clone())]).unwrap();

        let mut writer = data_file_writer::DataFileWriter::try_new(
            op.clone(),
            location_generator,
            to_write.schema(),
            1024,
            1024 * 1024,
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
