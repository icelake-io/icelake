//! io module provides the ability to read and write data from various
//! sources.

mod appender;
pub use appender::*;
pub mod file_writer;
pub use file_writer::*;
pub mod functional_writer;
pub mod location_generator;
pub use functional_writer::*;

pub mod parquet;
mod scan;
pub mod writer_builder;
pub use scan::*;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::types::{DataFileBuilder, StructValue};
use crate::Result;

type DefaultInput = RecordBatch;

#[async_trait::async_trait]
pub trait FileWriterBuilder: Send + Sync + Clone + 'static {
    type R: FileWriter;
    async fn build(self, schema: &SchemaRef, file_name: &str) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait FileWriter: Send + 'static {
    type R: FileWriteResult;
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn close(self) -> Result<Self::R>;
}

pub trait FileWriteResult: Send + Sync + 'static {
    type R: IcebergWriteResult;
    /// return None Indicates the result is empty.
    fn to_iceberg_result(self) -> Option<Self::R>;
}

#[async_trait::async_trait]
pub trait IcebergWriterBuilder: Send + Sync + Clone + 'static {
    type R;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput>: Send + 'static {
    type R: IcebergWriteResult;
    async fn write(&mut self, input: I) -> Result<()>;
    async fn close(&mut self) -> Result<Self::R>;
}

pub trait IcebergWriteResult: Send + Sync + 'static + Default {
    fn with_file_path(&mut self, file_name: String) -> &mut Self;
    fn with_content(&mut self, content: crate::types::DataContentType) -> &mut Self;
    fn with_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self;
    fn with_partition(&mut self, partition_value: Option<StructValue>) -> &mut Self;
    fn combine(&mut self, other: Self);
    fn flush(&mut self) -> Self;
}

pub trait SingletonWriter {
    fn current_file(&self) -> String;
    fn current_row_num(&self) -> usize;
    fn current_written_size(&self) -> usize;
}

impl IcebergWriteResult for Vec<DataFileBuilder> {
    fn with_file_path(&mut self, file_name: String) -> &mut Self {
        self.iter_mut().for_each(|builder| {
            builder.with_file_path(file_name.clone());
        });
        self
    }

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

    fn combine(&mut self, other: Self) {
        self.extend(other);
    }

    fn flush(&mut self) -> Self {
        std::mem::take(self)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{Fields, SchemaRef};
    use opendal::{services::Memory, Operator};

    use crate::types::{parse_table_metadata, COLUMN_ID_META_KEY};

    use super::location_generator::FileLocationGenerator;

    pub fn create_schema(col_num: usize) -> SchemaRef {
        let mut fields = vec![];
        for i in 1..=col_num {
            let mut field =
                arrow_schema::Field::new(format!("col{}", i), arrow_schema::DataType::Int64, false);
            field.set_metadata(HashMap::from([(
                COLUMN_ID_META_KEY.to_string(),
                (i).to_string(),
            )]));
            fields.push(field);
        }
        Arc::new(arrow_schema::Schema::new(Fields::from(fields)))
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
}
