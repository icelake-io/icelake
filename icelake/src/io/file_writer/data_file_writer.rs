//! A module provide `DataFileWriter`.

use crate::io::{RecordBatchWriter, SingletonWriter};
use crate::types::DataFileBuilder;
use crate::Result;
use arrow_array::RecordBatch;

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
///
/// # NOTE
/// This writer will not guarantee the written data is within one spec/partition. It is the caller's responsibility to make sure the data is within one spec/partition.
pub struct DataFileWriter<F: RecordBatchWriter> {
    inner_writer: F,
}

impl<F: RecordBatchWriter> DataFileWriter<F> {
    /// Create a new `DataFileWriter`.
    pub fn try_new(writer: F) -> Result<Self> {
        Ok(Self {
            inner_writer: writer,
        })
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

    use crate::io::{RecordBatchWriter, TestWriter};

    #[tokio::test]
    async fn test_data_file() {
        let inner_writer = TestWriter::default();
        let mut writer = super::DataFileWriter::try_new(inner_writer).unwrap();

        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        writer.write(to_write.clone()).await.unwrap();
        let result = writer.inner_writer.res();
        assert_eq!(result, to_write);
    }
}
