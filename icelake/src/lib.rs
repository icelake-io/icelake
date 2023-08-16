//! icelake is a library for reading and writing data lake table formats
//! like [Apache Iceberg](https://iceberg.apache.org/).

// Make sure all our public APIs have docs.
#![deny(missing_docs)]
#![allow(dead_code)]

mod table;
pub use table::Table;
mod error;
pub use error::Error;
pub use error::ErrorKind;
pub use error::Result;

pub mod config;
pub mod io;
pub mod transaction;
pub mod types;
