//! A module provide `DataFileWriter`.

use crate::io::{RecordBatchWriter, RecordBatchWriterBuilder, SingletonWriter};
use crate::types::DataFileBuilder;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct DataFileWriterBuilder<B: RecordBatchWriterBuilder> {
    inner: B,
}

impl<B: RecordBatchWriterBuilder> DataFileWriterBuilder<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: RecordBatchWriterBuilder> RecordBatchWriterBuilder for DataFileWriterBuilder<B> {
    type R = DataFileWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner_writer: self.inner.build(schema).await?,
        })
    }
}

/// A writer write data is within one spec/partition.
pub struct DataFileWriter<F: RecordBatchWriter> {
    inner_writer: F,
}

impl<F: RecordBatchWriter> DataFileWriter<F> {
    /// Create writer context.
    pub fn new(inner_writer: F) -> Self {
        Self { inner_writer }
    }
}

#[async_trait::async_trait]
impl<F: RecordBatchWriter> RecordBatchWriter for DataFileWriter<F> {
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

impl<F: SingletonWriter> SingletonWriter for DataFileWriter<F> {
    fn current_file(&self) -> String {
        self.inner_writer.current_file()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};

    use crate::io::{RecordBatchWriter, RecordBatchWriterBuilder, TestWriterBuilder};

    #[tokio::test]
    async fn test_data_file() {
        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let mut writer = super::DataFileWriterBuilder::new(TestWriterBuilder)
            .build(&to_write.schema())
            .await
            .unwrap();

        writer.write(to_write.clone()).await.unwrap();
        let result = writer.inner_writer.res();
        assert_eq!(result, to_write);
    }
}
