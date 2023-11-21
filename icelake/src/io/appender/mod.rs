//! File appender.
#[cfg(feature = "prometheus")]
pub mod prometheus;

mod rolling_writer;
pub use self::rolling_writer::*;

#[cfg(test)]
pub use self::test::*;

#[cfg(test)]
mod test {
    use crate::io::{RecordBatchWriter, RecordBatchWriterBuilder};
    use crate::Result;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;

    #[derive(Clone)]
    pub struct TestWriterBuilder;

    #[async_trait::async_trait]
    impl RecordBatchWriterBuilder for TestWriterBuilder {
        type R = TestWriter;

        async fn build(self, _schema: &arrow_schema::SchemaRef) -> Result<Self::R> {
            Ok(TestWriter { batch: vec![] })
        }
    }

    #[derive(Default)]
    pub struct TestWriter {
        batch: Vec<RecordBatch>,
    }

    impl TestWriter {
        pub fn res(&self) -> RecordBatch {
            concat_batches(&self.batch[0].schema(), self.batch.iter()).unwrap()
        }
    }

    #[async_trait::async_trait]
    impl RecordBatchWriter for TestWriter {
        async fn write(&mut self, batch: RecordBatch) -> Result<()> {
            self.batch.push(batch);
            Ok(())
        }

        async fn close(&mut self) -> crate::Result<Vec<crate::types::DataFileBuilder>> {
            unimplemented!()
        }
    }
}
