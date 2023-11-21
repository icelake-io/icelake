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

use crate::types::DataFileBuilder;
use crate::Result;

#[async_trait::async_trait]
pub trait RecordBatchWriter: Send + 'static {
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>>;
}

#[async_trait::async_trait]
pub trait RecordBatchWriterBuilder: Send + Sync + Clone + 'static {
    type R: RecordBatchWriter;
    async fn build(self, schema: &SchemaRef) -> Result<Self::R>;
}

pub trait SingletonWriter: RecordBatchWriter {
    fn current_file(&self) -> String;
    fn current_row_num(&self) -> usize;
}
