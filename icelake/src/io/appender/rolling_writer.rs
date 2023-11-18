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

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `FileMetaData`.
/// This writer should be used by specific content writer(`DataFileWriter` and `PositionDeleteFileWriter`), they should convert
/// `FileMetaData` to specific `DataFile`.
pub struct RollingWriter {
    operator: Operator,
    table_location: String,
    location_generator: Arc<FileLocationGenerator>,
    arrow_schema: SchemaRef,

    /// To avoid async new, the current writer will be laze created util the first write. So use `Option` here.
    /// But most of the time, the current writer will not be none.
    current_writer: Option<ParquetWriter>,
    current_row_num: usize,
    current_location: String,

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
            current_location: location_generator.generate_name(),
            location_generator,
            arrow_schema,
            current_writer: None,
            current_row_num: 0,
            result: vec![],
            table_config,
        })
    }

    fn should_split(&self) -> bool {
        self.current_row_num % self.table_config.rolling_writer.rows_per_file == 0
            && self.current_writer.as_ref().unwrap().get_written_size()
                >= self.table_config.rolling_writer.target_file_size_in_bytes
    }

    async fn prepare_new_writer(&mut self) -> Result<()> {
        // close current writer
        let current_writer = self
            .current_writer
            .take()
            .expect("Call this when current writer is not none");
        let (meta_data, written_size) = current_writer.close().await?;

        // Check if this file is empty
        if meta_data.num_rows == 0 {
            self.operator
                .delete(&self.current_location)
                .await
                .expect("Delete file failed");
            return Ok(());
        }

        // reset status
        let ori_location = std::mem::replace(
            &mut self.current_location,
            self.location_generator.generate_name(),
        );
        self.current_row_num = 0;

        self.result.push(DataFileBuilder::new(
            meta_data,
            self.table_location.clone(),
            ori_location,
            written_size,
        ));

        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        let current_writer = {
            let file_writer = self.operator.writer(&self.current_location).await?;
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
        self.current_writer = Some(current_writer);
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
        if self.current_writer.is_none() {
            self.open_new_writer().await?;
        }

        let batch = self.try_cast_batch(batch)?;
        self.current_writer
            .as_mut()
            .expect("Should not be none here")
            .write(&batch)
            .await?;
        self.current_row_num += batch.num_rows();

        if self.should_split() {
            self.prepare_new_writer().await?;
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.prepare_new_writer().await?;
        Ok(self.result.drain(0..).collect())
    }

    fn current_file(&self) -> String {
        format!("{}/{}", self.table_location, self.current_location)
    }

    fn current_row(&self) -> usize {
        self.current_row_num
    }
}
