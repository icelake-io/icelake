use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Operator;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::format::FileMetaData;

use crate::config::ParquetWriterConfig;
use crate::io_v2::{LocationGenerator, SingleFileWriterStatus};
use crate::types::DataFileBuilderV2;
use crate::Result;

use super::track_writer::TrackWriter;
use super::{FileWriteResult, FileWriter, FileWriterBuilder};

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
#[derive(Clone)]
pub struct ParquetWriterBuilder<L: LocationGenerator> {
    operator: Operator,
    /// `buffer_size` determines the initial size of the intermediate buffer.
    /// The intermediate buffer will automatically be resized if necessary
    init_buffer_size: usize,
    props: WriterProperties,
    table_location: String,
    location_generator: L,
}

impl<L: LocationGenerator> ParquetWriterBuilder<L> {
    /// Initiate a new builder.
    pub fn new(
        operator: Operator,
        init_buffer_size: usize,
        parquet_config: ParquetWriterConfig,
        table_location: String,
        location_generator: L,
    ) -> Self {
        let mut props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_bloom_filter_enabled(parquet_config.enable_bloom_filter)
            .set_compression(parquet_config.compression)
            .set_max_row_group_size(parquet_config.max_row_group_size)
            .set_write_batch_size(parquet_config.write_batch_size)
            .set_data_page_size_limit(parquet_config.data_page_size);
        if let Some(created_by) = parquet_config.created_by.as_ref() {
            props = props.set_created_by(created_by.to_string());
        }
        Self {
            operator,
            init_buffer_size,
            props: props.build(),
            table_location,
            location_generator,
        }
    }
}

#[async_trait::async_trait]
impl<L: LocationGenerator> FileWriterBuilder for ParquetWriterBuilder<L> {
    type R = ParquetWriter;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let file_name = self.location_generator.generate_name();

        let written_size = Arc::new(AtomicI64::new(0));
        let writer = TrackWriter::new(
            self.operator.writer(&file_name).await?,
            written_size.clone(),
        );

        let writer = AsyncArrowWriter::try_new(
            writer,
            schema.clone(),
            self.init_buffer_size,
            Some(self.props),
        )?;

        Ok(ParquetWriter {
            file_path: format!("{}/{}", self.table_location, file_name),
            operator: self.operator,
            file_name: file_name.to_string(),
            writer,
            written_size,
            current_row_num: 0,
        })
    }
}

/// ParquetWriter is used to write arrow data into parquet file on storage.
///
/// Initiate a new writer with `ParquetWriterBuilder::new()`.
pub struct ParquetWriter {
    operator: Operator,
    /// Used to delete file when no data is written.
    file_name: String,
    file_path: String,
    writer: AsyncArrowWriter<TrackWriter>,
    written_size: Arc<AtomicI64>,
    current_row_num: usize,
}

#[async_trait::async_trait]
impl FileWriter for ParquetWriter {
    type R = ParquetResult;

    /// Write data into the file.
    ///
    /// Note: It will not guarantee to take effect immediately.
    async fn write(&mut self, data: &RecordBatch) -> Result<()> {
        self.current_row_num += data.num_rows();
        self.writer.write(data).await?;
        Ok(())
    }

    /// Write footer, flush rest data and close file.
    ///
    /// # Note
    ///
    /// This function must be called before complete the write process.
    async fn close(self) -> Result<Vec<Self::R>> {
        let metadata = self.writer.close().await?;
        let written_size = self.written_size.load(std::sync::atomic::Ordering::Relaxed);
        if self.current_row_num == 0 {
            self.operator.delete(&self.file_name).await?;
            return Ok(vec![]);
        }
        Ok(vec![ParquetResult {
            metadata,
            written_size,
            file_path: self.file_path,
        }])
    }
}

impl SingleFileWriterStatus for ParquetWriter {
    fn current_file_path(&self) -> String {
        self.file_path.clone()
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }

    fn current_written_size(&self) -> usize {
        self.written_size.load(std::sync::atomic::Ordering::SeqCst) as usize
    }
}

pub struct ParquetResult {
    metadata: FileMetaData,
    written_size: i64,
    file_path: String,
}

impl FileWriteResult for ParquetResult {
    type R = DataFileBuilderV2;
    fn to_iceberg_result(self) -> DataFileBuilderV2 {
        let (column_sizes, value_counts, null_value_counts, distinct_counts) = {
            let mut per_col_size: HashMap<i32, _> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_distinct_val_num: HashMap<i32, _> = HashMap::new();
            self.metadata.row_groups.iter().for_each(|group| {
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

        let mut builder = DataFileBuilderV2::default();
        builder
            .with_file_format(crate::types::DataFileFormat::Parquet)
            .with_column_sizes(column_sizes)
            .with_value_counts(value_counts)
            .with_null_value_counts(null_value_counts)
            .with_distinct_counts(distinct_counts)
            .with_file_size_in_bytes(self.written_size)
            .with_record_count(self.metadata.num_rows)
            .with_key_metadata(self.metadata.footer_signing_key_metadata)
            .with_file_path(self.file_path)
            .with_split_offsets(
                self.metadata
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            );
        builder
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use bytes::Bytes;
    use opendal::services::Memory;
    use opendal::Operator;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    #[derive(Clone)]
    struct TestLocationGen;

    impl LocationGenerator for TestLocationGen {
        fn generate_name(&self) -> String {
            "test".to_string()
        }
    }

    #[tokio::test]
    async fn parquet_write_test() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let location_generator = TestLocationGen;

        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let mut pw = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            location_generator,
        )
        .build(&to_write.schema())
        .await?;
        pw.write(&to_write).await?;
        pw.write(&to_write).await?;
        pw.close().await?;

        let res = op.read("test").await?;
        let res = Bytes::from(res);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(res)
            .unwrap()
            .build()
            .unwrap();
        let res = reader.next().unwrap().unwrap();
        assert_eq!(to_write, res);
        let res = reader.next().unwrap().unwrap();
        assert_eq!(to_write, res);

        Ok(())
    }
}
