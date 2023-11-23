//! This module is for writer writing into single partition.
pub mod data_file_writer;
pub use data_file_writer::*;
pub mod position_delete_writer;
pub use position_delete_writer::*;
pub mod equality_delete_writer;
pub use equality_delete_writer::*;

#[cfg(test)]
pub use self::test::*;

#[cfg(test)]
mod test {
    use crate::io::{IcebergWriter, IcebergWriterBuilder};
    use crate::types::DataFileBuilder;
    use crate::Result;
    use arrow_array::RecordBatch;
    use arrow_select::concat::concat_batches;

    /// A writer used to test other iceberg writer.
    #[derive(Clone)]
    pub struct TestWriterBuilder;

    #[async_trait::async_trait]
    impl IcebergWriterBuilder for TestWriterBuilder {
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
    impl IcebergWriter for TestWriter {
        type R = Vec<DataFileBuilder>;

        async fn write(&mut self, batch: RecordBatch) -> Result<()> {
            self.batch.push(batch);
            Ok(())
        }

        async fn close(&mut self) -> crate::Result<Self::R> {
            unimplemented!()
        }
    }
}
