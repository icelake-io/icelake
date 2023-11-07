//! This module contains catalog wrapper.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use prometheus::core::AtomicU64;
use prometheus::core::GenericCounter;
use prometheus::Histogram;

use crate::types::PartitionSpec;
use crate::types::Schema;
use crate::Namespace;
use crate::Result;
use crate::Table;
use crate::TableIdentifier;

use super::Catalog;
use super::CatalogLayer;
use super::CatalogRef;
use super::UpdateTable;

#[derive(Clone)]
pub struct CatalogMetrics {
    pub load_table_qps: GenericCounter<AtomicU64>,
    pub load_table_latency: Histogram,
    pub list_table_qps: GenericCounter<AtomicU64>,
    pub list_table_latency: Histogram,
    pub update_table_qps: GenericCounter<AtomicU64>,
    pub update_table_latency: Histogram,
}

/// Iceberg prometheus layer.
#[derive(Clone)]
pub struct CatalogPrometheusLayer {
    metrics: CatalogMetrics,
}

impl CatalogPrometheusLayer {
    /// Create iceberg context.
    pub fn new(metrics: CatalogMetrics) -> Self {
        Self { metrics }
    }
}

impl CatalogLayer for CatalogPrometheusLayer {
    type LayeredCatalog = PrometheusLayeredCatalog;

    fn layer(&self, catalog: CatalogRef) -> Arc<Self::LayeredCatalog> {
        Arc::new(PrometheusLayeredCatalog {
            inner: catalog,
            metrics: self.metrics.clone(),
        })
    }
}

pub struct PrometheusLayeredCatalog {
    inner: CatalogRef,
    metrics: CatalogMetrics,
}

#[async_trait]
impl Catalog for PrometheusLayeredCatalog {
    /// Return catalog's name.
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// List tables under namespace.
    async fn list_tables(self: Arc<Self>, ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        self.metrics.list_table_qps.inc();
        let _ = self.metrics.list_table_latency.start_timer();
        self.inner.clone().list_tables(ns).await
    }

    /// Creates a table.
    async fn create_table(
        self: Arc<Self>,
        table_name: &TableIdentifier,
        schema: &Schema,
        spec: &PartitionSpec,
        location: &str,
        props: HashMap<String, String>,
    ) -> Result<Table> {
        self.inner
            .clone()
            .create_table(table_name, schema, spec, location, props)
            .await
    }

    /// Check table exists.
    async fn table_exists(self: Arc<Self>, table_name: &TableIdentifier) -> Result<bool> {
        self.inner.clone().table_exists(table_name).await
    }

    /// Drop table.
    async fn drop_table(self: Arc<Self>, table_name: &TableIdentifier, purge: bool) -> Result<()> {
        self.inner.clone().drop_table(table_name, purge).await
    }

    /// Rename table.
    async fn rename_table(
        self: Arc<Self>,
        from: &TableIdentifier,
        to: &TableIdentifier,
    ) -> Result<()> {
        self.inner.clone().rename_table(from, to).await
    }

    /// Load table.
    async fn load_table(self: Arc<Self>, table_name: &TableIdentifier) -> Result<Table> {
        self.metrics.load_table_qps.inc();
        let _ = self.metrics.list_table_latency.start_timer();
        self.inner.clone().load_table(table_name).await
    }

    /// Invalidate table.
    async fn invalidate_table(self: Arc<Self>, table_name: &TableIdentifier) -> Result<()> {
        self.inner.clone().invalidate_table(table_name).await
    }

    /// Register a table using metadata file location.
    async fn register_table(
        self: Arc<Self>,
        table_name: &TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table> {
        self.inner
            .clone()
            .register_table(table_name, metadata_file_location)
            .await
    }

    /// Update table.
    async fn update_table(self: Arc<Self>, udpate_table: &UpdateTable) -> Result<Table> {
        self.metrics.update_table_qps.inc();
        let _ = self.metrics.update_table_latency.start_timer();
        self.inner.clone().update_table(udpate_table).await
    }
}
