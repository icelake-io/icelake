use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use arrow_array::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use crate::Error;
use crate::Result;

/// ParquetStreamBuilder is used to builder a [`ParquetStream`].
///
///
/// # TODO
///
/// We should support options like: row_groups, projection and so on.
pub struct ParquetStreamBuilder {
    r: Reader,
    options: ArrowReaderOptions,
}

impl ParquetStreamBuilder {
    /// Initiate a new builder.
    pub fn new(r: Reader) -> Self {
        Self {
            r,
            options: ArrowReaderOptions::default(),
        }
    }

    /// Consume the current builder to build a new writer.
    pub async fn build(self) -> Result<ParquetStream> {
        let builder = ArrowReaderBuilder::new_with_options(self.r, self.options).await?;

        Ok(ParquetStream {
            reader: builder.build()?,
        })
    }
}

/// ParquetStream will read data from parquet file and produce arrow
/// record batch.
///
/// ParquetStream implements `Stream<Item = Result<RecordBatch>>`
///
/// # TODO
///
/// Possible optimization:
///
/// - If we have known the size of the file, we can avoid once seek.
pub struct ParquetStream {
    reader: ParquetRecordBatchStream<Reader>,
}

impl Stream for ParquetStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.reader
            .poll_next_unpin(cx)
            .map(|v| v.map(|v| v.map_err(Error::from)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use opendal::services::Memory;
    use opendal::Operator;
    use parquet::arrow::AsyncArrowWriter;

    use super::*;

    #[tokio::test]
    async fn parquet_stream_test() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();

        let col = Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

        let mut buf = vec![];
        let mut w = AsyncArrowWriter::try_new(&mut buf, to_write.schema(), 0, None)?;
        w.write(&to_write).await?;
        w.write(&to_write).await?;
        w.close().await?;

        // Write into operator
        op.write("test", buf).await?;

        let r = op.reader("test").await?;
        let mut reader = ParquetStreamBuilder::new(r).build().await?;
        let res = reader.next().await.unwrap()?;
        assert_eq!(to_write, res);
        let res = reader.next().await.unwrap()?;
        assert_eq!(to_write, res);

        Ok(())
    }
}
