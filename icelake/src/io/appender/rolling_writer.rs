//! A module provide `RollingWriter`.
use arrow_array::RecordBatch;
use arrow_cast::cast;
use async_trait::async_trait;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::sync::Arc;

use crate::config::{ParquetWriterConfig, RollingWriterConfig};
use crate::io::location_generator::FileLocationGenerator;
use crate::io::parquet::ParquetWriter;
use crate::io::{RecordBatchWriter, RecordBatchWriterBuilder, SingletonWriter};

use crate::io::parquet::ParquetWriterBuilder;
use crate::types::DataFileBuilder;
use crate::{Error, Result};
use arrow_schema::{DataType, SchemaRef};
use opendal::Operator;

#[derive(Clone)]
pub struct RollingWriterBuilder {
    location_generator: Arc<FileLocationGenerator>,
    config: RollingWriterConfig,
    parquet_config: ParquetWriterConfig,
    operator: Operator,
}

impl RollingWriterBuilder {
    pub fn new(
        operator: Operator,
        location_generator: Arc<FileLocationGenerator>,
        config: RollingWriterConfig,
        parquet_config: ParquetWriterConfig,
    ) -> Self {
        Self {
            location_generator,
            config,
            parquet_config,
            operator,
        }
    }
}

#[async_trait::async_trait]
impl RecordBatchWriterBuilder for RollingWriterBuilder {
    type R = RollingWriter;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        RollingWriter::try_new(
            self.operator,
            self.location_generator,
            schema.clone(),
            self.config,
            self.parquet_config,
        )
        .await
    }
}

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `FileMetaData`.
/// This writer should be used by specific content writer(`DataFileWriter` and `PositionDeleteFileWriter`), they should convert
/// `FileMetaData` to specific `DataFile`.
pub struct RollingWriter {
    operator: Operator,
    location_generator: Arc<FileLocationGenerator>,
    arrow_schema: SchemaRef,

    current_writer: Option<ParquetWriter>,
    current_row_num: usize,
    /// `current_location` used to clean up the file when no row is written to it.
    current_location: String,

    result: Vec<DataFileBuilder>,

    config: RollingWriterConfig,
    parquet_config: ParquetWriterConfig,
}

impl RollingWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        config: RollingWriterConfig,
        parquet_config: ParquetWriterConfig,
    ) -> Result<Self> {
        let mut writer = Self {
            operator,
            location_generator,
            arrow_schema,
            current_writer: None,
            current_row_num: 0,
            current_location: String::new(),
            result: vec![],
            config,
            parquet_config,
        };
        writer.open_new_writer().await?;
        Ok(writer)
    }

    fn should_split(&self) -> bool {
        self.current_row_num % self.config.rows_per_file == 0
            && self.current_writer.as_ref().unwrap().get_written_size()
                >= self.config.target_file_size_in_bytes
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

        self.result.push(DataFileBuilder::new(
            meta_data,
            self.location_generator.table_location().to_string(),
            self.current_location.clone(),
            written_size,
        ));

        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        // open new write must call when current writer is closed or inited.
        assert!(self.current_writer.is_none());

        let location = self.location_generator.generate_name();
        let file_writer = self.operator.writer(&location).await?.into_futures_async_write();
        let current_writer = {
            let mut props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_bloom_filter_enabled(self.parquet_config.enable_bloom_filter)
                .set_compression(self.parquet_config.compression)
                .set_max_row_group_size(self.parquet_config.max_row_group_size)
                .set_write_batch_size(self.parquet_config.write_batch_size)
                .set_data_page_size_limit(self.parquet_config.data_page_size);

            if let Some(created_by) = self.parquet_config.created_by.as_ref() {
                props = props.set_created_by(created_by.to_string());
            }

            ParquetWriterBuilder::new(file_writer, self.arrow_schema.clone())
                .with_properties(props.build())
                .build()?
        };
        self.current_writer = Some(current_writer);
        self.current_row_num = 0;
        self.current_location = location;
        Ok(())
    }

    // Try to cast the batch to compatitble with the schema of this writer.
    // It only try to do the simple cast to avoid the performance cost:
    // - timestamp with different timezone
    fn try_cast_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut need_cast = false;
        let mut columns = batch.columns().to_vec();
        for (idx, (actual_field, expect_field)) in batch
            .schema()
            .fields()
            .iter()
            .zip(self.arrow_schema.fields())
            .enumerate()
        {
            match (actual_field.data_type(), expect_field.data_type()) {
                (
                    DataType::Timestamp(actual_unit, actual_tz),
                    DataType::Timestamp(expect_unit, expect_tz),
                ) => {
                    if actual_unit == expect_unit && actual_tz != expect_tz {
                        need_cast = true;
                        let array =
                            cast(&columns[idx], expect_field.data_type()).map_err(|err| {
                                Error::new(crate::ErrorKind::ArrowError, err.to_string())
                            })?;
                        columns[idx] = array;
                    }
                }
                _ => continue,
            }
        }
        if need_cast {
            RecordBatch::try_new(self.arrow_schema.clone(), columns)
                .map_err(|err| Error::new(crate::ErrorKind::IcebergDataInvalid, err.to_string()))
        } else {
            Ok(batch)
        }
    }
}

// unsafe impl Sync for RollingWriter {}

#[async_trait]
impl RecordBatchWriter for RollingWriter {
    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = self.try_cast_batch(batch)?;
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
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.close_current_writer().await?;
        Ok(self.result.drain(0..).collect())
    }
}

impl SingletonWriter for RollingWriter {
    fn current_file(&self) -> String {
        format!(
            "{}/{}",
            self.location_generator.table_location(),
            self.current_location
        )
    }

    fn current_row_num(&self) -> usize {
        self.current_row_num
    }
}

#[cfg(test)]
mod test {
    use std::{fs, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use opendal::{services::Memory, Operator};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use crate::{
        io::{
            location_generator::FileLocationGenerator, RecordBatchWriter, RecordBatchWriterBuilder,
            RollingWriterBuilder,
        },
        types::parse_table_metadata,
    };

    #[tokio::test]
    async fn test_rolling_writer() -> Result<(), anyhow::Error> {
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

            FileLocationGenerator::try_new(&metadata, 0, 0, None)?
        };

        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col.clone())]).unwrap();

        let mut writer = RollingWriterBuilder::new(
            op.clone(),
            Arc::new(location_generator),
            Default::default(),
            Default::default(),
        )
        .build(&to_write.schema())
        .await
        .unwrap();

        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        let data_files = writer
            .close()
            .await?
            .into_iter()
            .map(|f| f.with_content(crate::types::DataContentType::Data).build())
            .collect::<Vec<_>>();

        let mut row_num = 0;
        for data_file in data_files {
            let res = op
                .read(data_file.file_path.strip_prefix("/tmp/table").unwrap())
                .await?.to_bytes();
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
