//! A module provide `RollingWriter`.
use crate::{config::RollingWriterConfig, io_v2::SingleFileWriterStatus};
use arrow_array::RecordBatch;
use arrow_cast::cast;
use async_trait::async_trait;

use crate::{Error, Result};
use arrow_schema::{DataType, SchemaRef};

use super::{FileWriter, FileWriterBuilder};

#[derive(Clone)]
pub struct BaseFileWriterBuilder<B: FileWriterBuilder> {
    rolling_config: Option<RollingWriterConfig>,
    inner: B,
}

impl<B: FileWriterBuilder> BaseFileWriterBuilder<B> {
    pub fn new(rolling_config: Option<RollingWriterConfig>, inner: B) -> Self {
        Self {
            rolling_config,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> FileWriterBuilder for BaseFileWriterBuilder<B>
where
    B::R: SingleFileWriterStatus,
{
    type R = BaseFileWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<BaseFileWriter<B>> {
        BaseFileWriter::try_new(self.inner, schema.clone(), self.rolling_config).await
    }
}

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `FileMetaData`.
/// This writer should be used by specific content writer(`DataFileWriter` and `PositionDeleteFileWriter`), they should convert
/// `FileMetaData` to specific `DataFile`.
pub struct BaseFileWriter<B: FileWriterBuilder> {
    arrow_schema: SchemaRef,

    writer_builder: B,

    current_writer: Option<B::R>,

    result: Vec<<<B as FileWriterBuilder>::R as FileWriter>::R>,
    rolling_config: Option<RollingWriterConfig>,
}

impl<B: FileWriterBuilder> BaseFileWriter<B>
where
    B::R: SingleFileWriterStatus,
{
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        writer_builder: B,
        arrow_schema: SchemaRef,
        rolling_config: Option<RollingWriterConfig>,
    ) -> Result<Self> {
        let mut writer = Self {
            writer_builder,
            arrow_schema,
            current_writer: None,
            result: vec![],
            rolling_config,
        };
        writer.open_new_writer().await?;
        Ok(writer)
    }

    fn should_split(&self) -> bool {
        if let Some(rolling_config) = &self.rolling_config {
            self.current_row_num() % rolling_config.rows_per_file == 0
                && self.current_writer.as_ref().unwrap().current_written_size() as u64
                    >= rolling_config.target_file_size_in_bytes
        } else {
            false
        }
    }

    async fn close_current_writer(&mut self) -> Result<()> {
        let current_writer = self.current_writer.take().expect("Should not be none here");
        let res = current_writer.close().await?;
        self.result.extend(res);
        Ok(())
    }

    async fn open_new_writer(&mut self) -> Result<()> {
        // open new write must call when current writer is closed or inited.
        assert!(self.current_writer.is_none());

        let current_writer = self
            .writer_builder
            .clone()
            .build(&self.arrow_schema)
            .await?;

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
impl<B: FileWriterBuilder> FileWriter for BaseFileWriter<B>
where
    B::R: SingleFileWriterStatus,
{
    type R = <<B as FileWriterBuilder>::R as FileWriter>::R;
    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = self.try_cast_batch(batch.clone())?;
        self.current_writer
            .as_mut()
            .expect("Should not be none here")
            .write(&batch)
            .await?;

        if self.should_split() {
            self.close_current_writer().await?;
            self.open_new_writer().await?;
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn close(mut self) -> Result<Vec<Self::R>> {
        self.close_current_writer().await?;
        Ok(self.result)
    }
}

impl<B: FileWriterBuilder> SingleFileWriterStatus for BaseFileWriter<B>
where
    B::R: SingleFileWriterStatus,
{
    fn current_file_path(&self) -> String {
        self.current_writer.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.current_writer.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.current_writer.as_ref().unwrap().current_written_size()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        config::RollingWriterConfig,
        io_v2::{
            file_writer::{BaseFileWriterBuilder, ParquetWriterBuilder},
            test::{
                create_arrow_schema, create_batch, create_location_generator, create_operator,
                read_batch,
            },
            FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriteResult,
        },
    };

    #[tokio::test]
    async fn test_rolling_writer_1_file() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let mut rolling_writer = BaseFileWriterBuilder::new(
            Some(RollingWriterConfig {
                rows_per_file: 1024,
                target_file_size_in_bytes: 0,
            }),
            parquet_writer_builder,
        )
        .build(&schema)
        .await
        .unwrap();

        // write 1024 * 3 column in 1 write
        let to_write = create_batch(
            &schema,
            vec![vec![1; 1024 * 3], vec![2; 1024 * 3], vec![3; 1024 * 3]],
        );
        rolling_writer.write(&to_write).await?;

        // check output is 1 file.
        let mut res = rolling_writer
            .close()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.to_iceberg_result())
            .collect_vec();
        assert!(res.len() == 1);

        // check row num
        res.iter_mut().for_each(|v| {
            v.set_content(crate::types::DataContentType::Data)
                .set_partition(None);
        });
        let mut row_num = 0;
        for builder in res {
            let data_file = builder.build().unwrap();
            let batch = read_batch(&op, &data_file.file_path).await;
            row_num += batch.num_rows();
        }
        assert_eq!(row_num, 1024 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_rolling_writer_3_file() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let mut rolling_writer = BaseFileWriterBuilder::new(
            Some(RollingWriterConfig {
                rows_per_file: 1024,
                target_file_size_in_bytes: 0,
            }),
            parquet_writer_builder,
        )
        .build(&schema)
        .await
        .unwrap();

        // write 1024 * 3 column in 3 write
        let to_write = create_batch(&schema, vec![vec![1; 1024], vec![2; 1024], vec![3; 1024]]);
        rolling_writer.write(&to_write).await?;
        rolling_writer.write(&to_write).await?;
        rolling_writer.write(&to_write).await?;

        // check output is 3 file
        let mut res = rolling_writer
            .close()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.to_iceberg_result())
            .collect_vec();
        assert!(res.len() == 3);

        // check row num
        res.iter_mut().for_each(|v| {
            v.set_content(crate::types::DataContentType::Data)
                .set_partition(None);
        });
        let mut row_num = 0;
        for builder in res {
            let data_file = builder.build().unwrap();
            let batch = read_batch(&op, &data_file.file_path).await;
            row_num += batch.num_rows();
        }
        assert_eq!(row_num, 1024 * 3);

        Ok(())
    }

    // Check that simple writer should write all data into one file.
    #[tokio::test]
    async fn test_simple_writer_1_file() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let mut rolling_writer = BaseFileWriterBuilder::new(None, parquet_writer_builder)
            .build(&schema)
            .await
            .unwrap();

        // write 1024 * 3 column in 1 write
        let to_write = create_batch(
            &schema,
            vec![vec![1; 1024 * 3], vec![2; 1024 * 3], vec![3; 1024 * 3]],
        );
        rolling_writer.write(&to_write).await?;

        // check output is 1 file.
        let mut res = rolling_writer
            .close()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.to_iceberg_result())
            .collect_vec();
        assert!(res.len() == 1);

        // check row num
        res.iter_mut().for_each(|v| {
            v.set_content(crate::types::DataContentType::Data)
                .set_partition(None);
        });
        let mut row_num = 0;
        for builder in res {
            let data_file = builder.build().unwrap();
            let batch = read_batch(&op, &data_file.file_path).await;
            row_num += batch.num_rows();
        }
        assert_eq!(row_num, 1024 * 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_simple_writer_3_file() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let mut rolling_writer = BaseFileWriterBuilder::new(None, parquet_writer_builder)
            .build(&schema)
            .await
            .unwrap();

        // write 1024 * 3 column in 3 write
        let to_write = create_batch(&schema, vec![vec![1; 1024], vec![2; 1024], vec![3; 1024]]);
        rolling_writer.write(&to_write).await?;
        rolling_writer.write(&to_write).await?;
        rolling_writer.write(&to_write).await?;

        // check output is 1 file
        let mut res = rolling_writer
            .close()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.to_iceberg_result())
            .collect_vec();
        assert!(res.len() == 1);

        // check row num
        res.iter_mut().for_each(|v| {
            v.set_content(crate::types::DataContentType::Data)
                .set_partition(None);
        });
        let mut row_num = 0;
        for builder in res {
            let data_file = builder.build().unwrap();
            let batch = read_batch(&op, &data_file.file_path).await;
            row_num += batch.num_rows();
        }
        assert_eq!(row_num, 1024 * 3);

        Ok(())
    }
}
