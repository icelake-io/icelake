//! A module provide `DataFileWriter`.

use crate::io::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder, SingletonWriter};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct DataFileWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
}

impl<B: IcebergWriterBuilder> DataFileWriterBuilder<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for DataFileWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = DataFileWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: self.inner.build(schema).await?,
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<F: IcebergWriter> {
    inner_writer: F,
}

impl<F: IcebergWriter> DataFileWriter<F> {
    /// Create writer context.
    pub fn new(inner_writer: F) -> Self {
        Self { inner_writer }
    }
}

#[async_trait::async_trait]
impl<F: IcebergWriter> IcebergWriter for DataFileWriter<F> {
    type R = F::R;

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await
    }

    async fn close(&mut self) -> Result<F::R> {
        let mut res = self.inner_writer.close().await?;
        res.with_content(crate::types::DataContentType::Data);
        Ok(res)
    }
}

impl<F: SingletonWriter + IcebergWriter> SingletonWriter for DataFileWriter<F> {
    fn current_file(&self) -> String {
        self.inner_writer.current_file()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner_writer.current_written_size()
    }
}
