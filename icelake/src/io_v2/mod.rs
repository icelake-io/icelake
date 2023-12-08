//! io module provides the ability to read and write data from various
//! sources.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::types::{DataFileBuilderV2, StructValue};
use crate::Result;

pub mod file_writer;
pub use file_writer::*;
pub mod base_writer;
pub use base_writer::*;
pub mod functional_writer;
pub use functional_writer::*;
pub mod input_wrapper;
pub mod location_generator;
pub use location_generator::*;
pub mod builder_helper;
pub use builder_helper::*;

type DefaultInput = RecordBatch;

#[async_trait::async_trait]
pub trait IcebergWriterBuilder<I = DefaultInput>: Send + Clone + 'static {
    type R: IcebergWriter<I>;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput>: Send + 'static {
    type R: IcebergWriteResult;
    async fn write(&mut self, input: I) -> Result<()>;
    async fn flush(&mut self) -> Result<Vec<Self::R>>;
}

pub trait IcebergWriteResult: Send + Sync + 'static {
    fn set_content(&mut self, content: crate::types::DataContentType) -> &mut Self;
    fn set_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self;
    fn set_partition(&mut self, partition_value: Option<StructValue>) -> &mut Self;
}

pub trait CurrentFileStatus {
    fn current_file_path(&self) -> String;
    fn current_row_num(&self) -> usize;
    fn current_written_size(&self) -> usize;
}

impl IcebergWriteResult for DataFileBuilderV2 {
    fn set_content(&mut self, content: crate::types::DataContentType) -> &mut Self {
        self.with_content(content)
    }

    fn set_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self {
        self.with_equality_ids(equality_ids)
    }

    fn set_partition(&mut self, partition_value: Option<StructValue>) -> &mut Self {
        if let Some(partition_value) = partition_value {
            self.with_partition(partition_value)
        } else {
            self.with_partition(StructValue::default())
        }
    }
}

// This module provide the test utils for iceberg writer.
#[cfg(test)]
mod test {
    use std::{fs, sync::Arc};

    use crate::types::{parse_table_metadata, DataFileBuilderV2, Field, Schema, Struct};
    use crate::Result;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::SchemaRef;
    use arrow_select::concat::concat_batches;
    use bytes::Bytes;
    use itertools::Itertools;
    use opendal::{services::Memory, Operator};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::{FileLocationGenerator, IcebergWriter, IcebergWriterBuilder};

    pub async fn read_batch(op: &Operator, path: &str) -> RecordBatch {
        let res = op.read(path).await.unwrap();
        let res = Bytes::from(res);
        let reader = ParquetRecordBatchReaderBuilder::try_new(res)
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.into_iter().map(|batch| batch.unwrap()).collect_vec();
        concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    pub fn create_schema(col_num: usize) -> Schema {
        let fields = (1..=col_num)
            .map(|i| {
                Arc::new(Field::required(
                    i as i32,
                    format!("col{}", i),
                    crate::types::Any::Primitive(crate::types::Primitive::Long),
                ))
            })
            .collect_vec();
        Schema::new(1, None, Struct::new(fields))
    }

    pub fn create_arrow_schema(col_num: usize) -> SchemaRef {
        let schema = create_schema(col_num);
        Arc::new(schema.try_into().unwrap())
    }

    pub fn create_batch(schema: &SchemaRef, cols: Vec<Vec<i64>>) -> RecordBatch {
        let mut columns = vec![];
        for col in cols.into_iter() {
            let col = Arc::new(Int64Array::from_iter_values(col)) as ArrayRef;
            columns.push(col);
        }
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    pub fn create_operator() -> Operator {
        let mut builder = Memory::default();
        builder.root("/");
        Operator::new(builder).unwrap().finish()
    }

    pub fn create_location_generator() -> FileLocationGenerator {
        let mut metadata = {
            let path = format!(
                "{}/../testdata/simple_table/metadata/v1.metadata.json",
                env!("CARGO_MANIFEST_DIR")
            );

            let bs = fs::read(path).expect("read_file must succeed");

            parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
        };
        metadata.location = "/".to_string();

        FileLocationGenerator::try_new(&metadata, 0, 0, None).unwrap()
    }
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
        type R = DataFileBuilderV2;

        async fn write(&mut self, batch: RecordBatch) -> Result<()> {
            self.batch.push(batch);
            Ok(())
        }

        async fn flush(&mut self) -> crate::Result<Vec<Self::R>> {
            Ok(vec![])
        }
    }
}
