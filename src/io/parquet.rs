use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Writer;
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

pub struct ParquetWriterBuilder {
    writer: Writer,
    arrow_schema: SchemaRef,

    /// `buffer_size` determines the initial size of the intermediate buffer.
    /// The intermediate buffer will automatically be resized if necessary
    buffer_size: usize,
    props: Option<WriterProperties>,
}

impl ParquetWriterBuilder {
    /// Initiate a new builder.
    pub fn new(w: Writer, arrow_schema: SchemaRef) -> Self {
        Self {
            writer: w,
            arrow_schema,

            buffer_size: 0,
            props: None,
        }
    }

    /// Configure the buffer size for writer.
    ///
    /// `buffer_size` determines the initial size of the intermediate buffer.
    /// The intermediate buffer will automatically be resized if necessary
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Configure the writer properties for writer.
    ///
    /// # FIXME
    ///
    /// This API will expose the parquet API to the user directly, which
    /// is not a good idea. We should hide it if possible.
    pub fn with_properties(mut self, props: WriterProperties) -> Self {
        self.props = Some(props);
        self
    }

    /// Consume the current builder to build a new writer.
    pub fn build(self) -> Result<ParquetWriter> {
        let writer = AsyncArrowWriter::try_new(
            self.writer,
            self.arrow_schema,
            self.buffer_size,
            self.props,
        )?;

        Ok(ParquetWriter { writer })
    }
}

/// ParquetWriter is used to write arrow data into parquet file on storage.
///
/// Initiate a new writer with `ParquetWriterBuilder::new()`.
pub struct ParquetWriter {
    writer: AsyncArrowWriter<Writer>,
}

impl ParquetWriter {
    /// Write data into the file.
    ///
    /// Note: It will not guarantee to take effect imediately.
    pub async fn write(&mut self, data: &RecordBatch) -> Result<()> {
        self.writer.write(data).await?;
        Ok(())
    }

    /// Write footer, flush rest data and close file.
    ///
    /// # Note
    ///
    /// This function must be called before complete the write process.
    ///
    /// # TODO
    ///
    /// Maybe we can return the `FileMetaData` to the user after close.
    pub async fn close(self) -> Result<()> {
        let _ = self.writer.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use opendal::{services::Memory, Operator};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn parquet_append_test() -> Result<()> {
        let mut builder = Memory::default();
        builder.root("/tmp");
        let op: Operator = Operator::new(builder)?.finish();

        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let w = op.writer("/tmp/test").await?;
        let mut pw = ParquetWriterBuilder::new(w, to_write.schema()).build()?;
        pw.write(&to_write).await?;
        pw.write(&to_write).await?;
        pw.close().await?;

        let res = op.read("/tmp/test").await?;
        let res = Bytes::from(res);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(res)
            .unwrap()
            .build()
            .unwrap();
        let res = reader.next().unwrap().unwrap();
        assert_eq!(to_write, res);
        let res = reader.next().unwrap().unwrap();
        assert_eq!(to_write, res);

        Ok(())
    }
}
