//! A module provide `RollingWriter`.
use arrow_array::RecordBatch;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::sync::Arc;

use crate::{config::TableConfigRef, io::parquet::ParquetWriterBuilder};
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::location_generator::DataFileLocationGenerator;
use super::parquet::ParquetWriter;
use crate::types::DataFileBuilder;
use crate::Result;

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `FileMetaData`.
/// This writer should be used by specific content writer(`DataFileWriter` and `PositionDeleteFileWriter`), they should convert
/// `FileMetaData` to specific `DataFile`.
pub(crate) struct RollingWriter {
    operator: Operator,
    location_generator: Arc<DataFileLocationGenerator>,
    arrow_schema: SchemaRef,

    current_writer: Option<ParquetWriter>,
    current_row_num: usize,
    /// `current_location` used to clean up the file when no row is written to it.
    current_location: String,

    result: Vec<DataFileBuilder>,

    table_config: TableConfigRef,
}

impl RollingWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        location_generator: Arc<DataFileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let mut writer = Self {
            operator,
            location_generator,
            arrow_schema,
            current_writer: None,
            current_row_num: 0,
            current_location: String::new(),
            result: vec![],
            table_config,
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
    pub async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        self.close_current_writer().await?;
        Ok(self.result)
    }

    fn should_split(&self) -> bool {
        self.current_row_num % self.table_config.datafile_writer.rows_per_file == 0
            && self.current_writer.as_ref().unwrap().get_written_size()
                >= self.table_config.datafile_writer.target_file_size_in_bytes
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
            self.current_location.clone(),
            written_size,
        ));
        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        // open new write must call when current writer is closed or inited.
        assert!(self.current_writer.is_none());

        let location = self.location_generator.generate_name();
        let file_writer = self.operator.writer(&location).await?;
        let current_writer = {
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
        self.current_row_num = 0;
        self.current_location = location;
        Ok(())
    }
}
