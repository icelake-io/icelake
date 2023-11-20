//! A module provide `DataFileWriter`.

use crate::io::{RecordBatchWriter, SingletonWriterStatus};
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

    /// Write a record batch.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await?;
        Ok(())
    }

    /// Complte the write and return the list of `DataFileBuilder` as result.
    pub async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| builder.with_content(crate::types::DataContentType::Data))
            .collect())
    }
}

impl<F: RecordBatchWriter + SingletonWriterStatus> SingletonWriterStatus for DataFileWriter<F> {
    fn current_file(&self) -> String {
        self.inner_writer.current_file()
    }

    fn current_row_num(&self) -> usize {
        self.inner_writer.current_row_num()
    }
}
