//! A module provide `DataFileWriter`.

use crate::io::{FileWriter, RecordBatchWriter, SingletonWriter, WriterBuilder};
use crate::types::DataFileBuilder;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct DataFileWriterBuilder<B: WriterBuilder> {
    inner: B,
}

impl<B: WriterBuilder> DataFileWriterBuilder<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: WriterBuilder> WriterBuilder for DataFileWriterBuilder<B>
where
    B::R: FileWriter,
{
    type R = DataFileWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: self.inner.build(schema).await?,
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<F: FileWriter> {
    inner_writer: F,
}

impl<F: FileWriter> DataFileWriter<F> {
    /// Create writer context.
    pub fn new(inner_writer: F) -> Self {
        Self { inner_writer }
    }
}

#[async_trait::async_trait]
impl<F: FileWriter> RecordBatchWriter for DataFileWriter<F> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await
    }

    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| builder.with_content(crate::types::DataContentType::Data))
            .collect())
    }
}

impl<F: SingletonWriter + FileWriter> SingletonWriter for DataFileWriter<F> {
    fn current_file(&self) -> String {
        self.inner_writer.current_file()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }
}
