//! A module provide `RollingWriter`.
use arrow_array::RecordBatch;
use arrow_cast::cast;
use async_trait::async_trait;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::sync::Arc;

use crate::io::location_generator::FileLocationGenerator;
use crate::io::parquet::ParquetWriter;

use crate::types::DataFileBuilder;
use crate::{config::TableConfigRef, io::parquet::ParquetWriterBuilder};
use crate::{Error, Result};
use arrow_schema::{DataType, SchemaRef};
use opendal::Operator;

use super::FileAppender;

struct InnerFileWriter {
    pub location: String,
    pub row_num: usize,

    /// To avoid async new, the writer will be laze created util open. So use `Option` here.
    /// But most of the time, the current writer will not be none.
    pub writer: Option<ParquetWriter>,
}

impl InnerFileWriter {
    fn new(file_location: String) -> Self {
        Self {
            location: file_location,
            row_num: 0,
            writer: None,
        }
    }

    fn open(&mut self, writer: ParquetWriter) {
        self.writer = Some(writer);
    }

    fn is_open(&self) -> bool {
        self.writer.is_some()
    }
}

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `FileMetaData`.
/// This writer should be used by specific content writer(`DataFileWriter` and `PositionDeleteFileWriter`), they should convert
/// `FileMetaData` to specific `DataFile`.
pub struct RollingWriter {
    operator: Operator,
    table_location: String,
    location_generator: Arc<FileLocationGenerator>,
    arrow_schema: SchemaRef,

    file_writer: InnerFileWriter,

    result: Vec<DataFileBuilder>,

    table_config: TableConfigRef,
}

impl RollingWriter {
    /// Create a new `DataFileWriter`.
    pub fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            operator,
            table_location,
            file_writer: InnerFileWriter::new(location_generator.generate_name()),
            location_generator,
            arrow_schema,
            result: vec![],
            table_config,
        })
    }

    fn should_split(&self) -> bool {
        self.file_writer.row_num % self.table_config.rolling_writer.rows_per_file == 0
            && self.file_writer.writer.as_ref().unwrap().get_written_size()
                >= self.table_config.rolling_writer.target_file_size_in_bytes
    }

    async fn flush_open_file(&mut self) -> Result<()> {
        if !self.file_writer.is_open() {
            return Ok(());
        }
        let open_writer = std::mem::replace(
            &mut self.file_writer,
            InnerFileWriter::new(self.location_generator.generate_name()),
        );
        // close current writer
        let (meta_data, written_size) = open_writer.writer.unwrap().close().await?;
        self.result.push(DataFileBuilder::new(
            meta_data,
            self.table_location.clone(),
            open_writer.location,
            written_size,
        ));

        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        let new_writer = {
            let file_writer = self.operator.writer(&self.file_writer.location).await?;
            let mut props = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_bloom_filter_enabled(self.table_config.parquet_writer.enable_bloom_filter)
                .set_compression(self.table_config.parquet_writer.compression)
                .set_max_row_group_size(self.table_config.parquet_writer.max_row_group_size)
                .set_write_batch_size(self.table_config.parquet_writer.write_batch_size)
                .set_data_page_size_limit(self.table_config.parquet_writer.data_page_size);

            if let Some(created_by) = self.table_config.parquet_writer.created_by.as_ref() {
                props = props.set_created_by(created_by.to_string());
            }

            ParquetWriterBuilder::new(file_writer, self.arrow_schema.clone())
                .with_properties(props.build())
                .build()?
        };
        self.file_writer.open(new_writer);
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
impl FileAppender for RollingWriter {
    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // # TODO
        // Optimize by `unlikely`
        if !self.file_writer.is_open() {
            self.open_new_writer().await?;
        }

        let batch = self.try_cast_batch(batch)?;
        self.file_writer
            .writer
            .as_mut()
            .unwrap()
            .write(&batch)
            .await?;
        self.file_writer.row_num += batch.num_rows();

        if self.should_split() {
            self.flush_open_file().await?;
            self.open_new_writer().await?;
        }

        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.flush_open_file().await?;
        Ok(self.result.drain(0..).collect())
    }

    fn current_file(&self) -> String {
        format!("{}/{}", self.table_location, self.file_writer.location)
    }

    fn current_row(&self) -> usize {
        self.file_writer.row_num
    }
}
