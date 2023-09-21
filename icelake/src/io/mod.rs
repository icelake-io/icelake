//! io module provides the ability to read and write data from various
//! sources.

use std::collections::HashMap;

use prometheus::{labels, Registry};

use crate::TableIdentifier;

mod appender;
pub mod file_writer;
pub mod location_generator;
pub mod parquet;
pub mod task_writer;
pub mod writer_builder;
pub use appender::*;

/// File writer context
#[derive(Clone)]
pub struct WriterPrometheusLayer {
    context_id: String,
    catalog_name: String,
    table_name: String,
    registry: Registry,
}

impl WriterPrometheusLayer {
    /// Create writer context.
    pub fn new(
        context_id: impl ToString,
        catalog_name: impl ToString,
        table_name: &TableIdentifier,
        registry: Registry,
    ) -> Self {
        Self {
            context_id: context_id.to_string(),
            catalog_name: catalog_name.to_string(),
            table_name: format!("{}", table_name),
            registry,
        }
    }

    /// Get table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get metrics labels.
    pub fn writer_metrics_labels(&self) -> HashMap<&str, &str> {
        labels! {
            "context_id" => self.context_id.as_str(),
            "catalog" => self.catalog_name.as_str(),
            "table" => self.table_name(),
        }
    }
}
