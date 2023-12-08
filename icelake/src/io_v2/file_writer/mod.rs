use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{CurrentFileStatus, IcebergWriteResult};

pub mod parquet_writer;
pub use parquet_writer::*;
pub mod base_file_writer;
pub use base_file_writer::*;
pub mod track_writer;

#[async_trait::async_trait]
pub trait FileWriterBuilder: Send + Clone + 'static {
    type R: FileWriter;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait FileWriter: Send + 'static + CurrentFileStatus {
    type R: FileWriteResult;
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn close(self) -> Result<Vec<Self::R>>;
}

pub trait FileWriteResult: Send + 'static {
    type R: IcebergWriteResult;
    fn to_iceberg_result(self) -> Self::R;
}
