//! parquet module provides the ability to read and write parquet data.

mod write;
pub use write::ParquetWriter;
pub use write::ParquetWriterBuilder;

mod track_writer;
