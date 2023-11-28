//! A module provide `DataFileWriter`.

use crate::io::{
    FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriteResult, IcebergWriter,
    IcebergWriterBuilder, SingleFileWriter,
};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct DataFileWriterBuilder<B: FileWriterBuilder> {
    inner: B,
}

impl<B: FileWriterBuilder> DataFileWriterBuilder<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for DataFileWriterBuilder<B> {
    type R = DataFileWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: self.inner.clone().build(schema).await?,
            builder: self.inner,
            schema: schema.clone(),
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<B: FileWriterBuilder> {
    builder: B,
    schema: SchemaRef,

    inner_writer: B::R,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for DataFileWriter<B> {
    type R = <<B::R as FileWriter>::R as FileWriteResult>::R;

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(&batch).await
    }

    async fn flush(&mut self) -> Result<Self::R> {
        let writer = std::mem::replace(
            &mut self.inner_writer,
            self.builder.clone().build(&self.schema).await?,
        );
        let mut res = writer.close().await?.to_iceberg_result();
        res.with_content(crate::types::DataContentType::Data);
        Ok(res)
    }
}

impl<B: FileWriterBuilder> SingleFileWriter for DataFileWriter<B>
where
    B::R: SingleFileWriter,
{
    fn current_file_path(&self) -> String {
        self.inner_writer.current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner_writer.current_written_size()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::io::{
        test::{
            create_arrow_schema, create_batch, create_location_generator, create_operator,
            read_batch,
        },
        BaseFileWriterBuilder, DataFileWriterBuilder, IcebergWriteResult, IcebergWriter,
        IcebergWriterBuilder, ParquetWriterBuilder,
    };

    #[tokio::test]
    async fn test_data_file_writer() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            Arc::new(location_generator),
        );
        let rolling_writer_builder = BaseFileWriterBuilder::new(None, parquet_writer_builder);
        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(&schema)
            .await?;

        // write 1024 * 3 column in 1 write
        let to_write = create_batch(
            &schema,
            vec![vec![1; 1024 * 3], vec![2; 1024 * 3], vec![3; 1024 * 3]],
        );
        data_file_writer.write(to_write).await?;

        // check output is 1 file.
        let mut res = data_file_writer.flush().await.unwrap();
        assert!(res.len() == 1);

        // check row num
        let res = res
            .with_content(crate::types::DataContentType::Data)
            .with_partition(None);
        let mut row_num = 0;
        for builder in res {
            let data_file = builder.build().unwrap();
            let batch = read_batch(&op, &data_file.file_path).await;
            row_num += batch.num_rows();
        }
        assert_eq!(row_num, 1024 * 3);

        // write 1024 * 3 column in 1 write
        let to_write = create_batch(
            &schema,
            vec![vec![4; 1024 * 3], vec![5; 1024 * 3], vec![6; 1024 * 3]],
        );
        data_file_writer.write(to_write).await?;

        // check output is 1 file.
        let mut res = data_file_writer.flush().await.unwrap();
        assert!(res.len() == 1);

        // check row num
        let res = res
            .with_content(crate::types::DataContentType::Data)
            .with_partition(None);
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
