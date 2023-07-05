//! io module provides the ability to read and write data from various
//! sources.

#[cfg(feature = "io_parquet")]
pub mod data_file_writer;
pub mod location_generator;
#[cfg(feature = "io_parquet")]
pub mod parquet;
