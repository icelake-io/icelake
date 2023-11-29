//! io module provides the ability to read and write data from various
//! sources.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::types::{DataFileBuilder, StructValue};
use crate::Result;

pub mod file_writer;
pub use file_writer::*;
pub mod location_generator;
pub use location_generator::*;

type DefaultInput = RecordBatch;

#[async_trait::async_trait]
pub trait IcebergWriterBuilder: Send + Clone + 'static {
    type R;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput>: Send + 'static {
    type R: IcebergWriteResultVector;
    async fn write(&mut self, input: I) -> Result<()>;
    async fn flush(&mut self) -> Result<Self::R>;
}

pub trait IcebergWriteResultVector: Send + Sync + 'static {
    fn empty() -> Self;
    fn len(&self) -> usize;
    fn with_content(&mut self, content: crate::types::DataContentType) -> &mut Self;
    fn with_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self;
    fn with_partition(&mut self, partition_value: Option<StructValue>) -> &mut Self;
    fn extend_res(&mut self, other: Self);
    fn clear(&mut self) -> Self;
}

pub trait SingleFileWriter {
    fn current_file_path(&self) -> String;
    fn current_row_num(&self) -> usize;
    fn current_written_size(&self) -> usize;
}

impl IcebergWriteResultVector for Vec<DataFileBuilder> {
    fn with_content(&mut self, content: crate::types::DataContentType) -> &mut Self {
        self.iter_mut().for_each(|builder| {
            builder.with_content(content);
        });
        self
    }

    fn with_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self {
        self.iter_mut().for_each(|builder| {
            builder.with_equality_ids(equality_ids.clone());
        });
        self
    }

    fn with_partition(&mut self, partition_value: Option<StructValue>) -> &mut Self {
        self.iter_mut().for_each(|builder| {
            if let Some(partition_value) = &partition_value {
                builder.with_partition(partition_value.clone());
            } else {
                builder.with_partition(StructValue::default());
            }
        });
        self
    }

    fn extend_res(&mut self, other: Self) {
        self.extend(other);
    }

    fn clear(&mut self) -> Self {
        std::mem::take(self)
    }

    fn empty() -> Self {
        vec![]
    }

    fn len(&self) -> usize {
        self.len()
    }
}

// This module provide the test utils for iceberg writer.
#[cfg(test)]
// # TODO
// Remove this allow after the v2 is implemented.
#[allow(dead_code)]
mod test {
    use std::{fs, sync::Arc};

    use crate::types::{parse_table_metadata, DataFileBuilder, Field, Schema, Struct};
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
        type R = Vec<DataFileBuilder>;

        async fn write(&mut self, batch: RecordBatch) -> Result<()> {
            self.batch.push(batch);
            Ok(())
        }

        async fn flush(&mut self) -> crate::Result<Self::R> {
            unimplemented!()
        }
    }
}
