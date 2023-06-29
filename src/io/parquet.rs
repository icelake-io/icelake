use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::{Operator, Writer};
use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

pub struct ParquetWriter {
    writer: AsyncArrowWriter<Writer>,
}

impl ParquetWriter {
    /// Try to create a new Writer.
    pub async fn try_new(
        operator: Operator,
        path: &str,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
    ) -> Result<Self> {
        let writer = operator.writer(path).await?;
        Ok(ParquetWriter {
            writer: AsyncArrowWriter::try_new(writer, arrow_schema, 0, props)?,
        })
    }

    /// Append data into the file.
    /// Note: It will not guarantee to take effect imediately.
    pub async fn append(&mut self, data: &RecordBatch) -> Result<()> {
        self.writer.write(data).await?;
        Ok(())
    }

    /// Write footer ,flush rest data and close file.
    /// Note: This function must be called before complete the write process.
    pub async fn flush_and_close(self) -> Result<()> {
        self.writer.close().await?;
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

    use crate::io::parquet::ParquetWriter;

    #[tokio::test]
    async fn parquet_append_test() -> Result<()> {
        let mut builder = Memory::default();
        builder.root("/tmp");
        let op: Operator = Operator::new(builder)?.finish();

        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let mut appender =
            ParquetWriter::try_new(op.clone(), "/tmp/test", to_write.schema(), None).await?;
        appender.append(&to_write).await?;
        appender.append(&to_write).await?;
        appender.flush_and_close().await?;

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
