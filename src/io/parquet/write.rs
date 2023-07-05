use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Writer;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;

use crate::Result;

/// ParquetWriterBuilder is used to builder a [`ParquetWriter`]
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
    pub async fn close(self) -> Result<FileMetaData> {
        Ok(self.writer.close().await?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use bytes::Bytes;
    use opendal::services::Memory;
    use opendal::Operator;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    #[tokio::test]
    async fn parquet_write_test() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();

        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let w = op.writer("test").await?;
        let mut pw = ParquetWriterBuilder::new(w, to_write.schema()).build()?;
        pw.write(&to_write).await?;
        pw.write(&to_write).await?;
        pw.close().await?;

        let res = op.read("test").await?;
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
