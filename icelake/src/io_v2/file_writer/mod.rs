use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use crate::Result;

use super::IcebergWriteResultVector;

pub mod parquet_writer;
pub use parquet_writer::*;
pub mod base_file_writer;
pub use base_file_writer::*;
pub mod track_writer;


#[async_trait::async_trait]
pub trait FileWriterBuilder: Send + Sync + Clone + 'static {
    type R: FileWriter;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait FileWriter: Send + 'static {
    type R: FileWriteResultVector;
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn close(self) -> Result<Self::R>;
}

pub trait FileWriteResultVector: Send + 'static {
    type R: IcebergWriteResultVector;
    fn empty() -> Self;
    fn extend_res(&mut self, other: Self);
    fn to_iceberg_result(self) -> Self::R;
    fn len(&self) -> usize;
}