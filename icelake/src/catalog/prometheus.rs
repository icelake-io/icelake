//! This module contains catalog wrapper.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use prometheus::core::AtomicU64;
use prometheus::core::GenericCounter;
use prometheus::histogram_opts;
use prometheus::opts;
use prometheus::register_histogram_vec_with_registry;
use prometheus::register_int_counter_vec_with_registry;
use prometheus::Histogram;
use prometheus::HistogramVec;
use prometheus::IntCounterVec;
use prometheus::Registry;
use prometheus::DEFAULT_BUCKETS;

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

const CATALOG_METRICS_LABEL_NAMES: &[&str] = &["context", "catalog"];
#[derive(Clone)]
pub(crate) struct CatalogMetricsDef {
    load_table_qps: IntCounterVec,
    load_table_latency: HistogramVec,

    list_table_qps: IntCounterVec,
    list_table_latency: HistogramVec,

    update_table_qps: IntCounterVec,
    update_table_latency: HistogramVec,
}

pub(crate) struct CatalogMetrics {
    pub(crate) load_table_qps: GenericCounter<AtomicU64>,
    pub(crate) load_table_latency: Histogram,
    pub(crate) list_table_qps: GenericCounter<AtomicU64>,
    pub(crate) list_table_latency: Histogram,
    pub(crate) update_table_qps: GenericCounter<AtomicU64>,
    pub(crate) update_table_latency: Histogram,
}

impl CatalogMetricsDef {
    fn new(registry: &Registry) -> Self {
        let load_table_qps = register_int_counter_vec_with_registry!(
            opts!(
                "iceberg_load_table_qps",
                "Iceberg load table qps by desc and catallog name",
            ),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let load_table_latency = register_histogram_vec_with_registry!(
            histogram_opts!(
                "iceberg_load_table_latency",
                "Iceberg load table latency by desc and catalog name",
                DEFAULT_BUCKETS.to_vec(),
            ),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let list_table_qps = register_int_counter_vec_with_registry!(
            opts!(
                "iceberg_list_table_qps",
                "Iceberg list table qps by desc and catalog name",
            ),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let list_table_latency = register_histogram_vec_with_registry!(
            histogram_opts!(
                "iceberg_list_table_latency",
                "Iceberg list table latency by desc and catalog name",
                DEFAULT_BUCKETS.to_vec(),
            ),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let update_table_qps = register_int_counter_vec_with_registry!(
            opts!("iceberg_update_table_qps", "Iceberg update table qps",),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let update_table_latency = register_histogram_vec_with_registry!(
            histogram_opts!(
                "iceberg_update_table_latency",
                "Iceberg update table latency",
                DEFAULT_BUCKETS.to_vec(),
            ),
            CATALOG_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        Self {
            load_table_qps,
            load_table_latency,

            list_table_qps,
            list_table_latency,

            update_table_qps,
            update_table_latency,
        }
    }
}

/// Iceberg prometheus layer.
#[derive(Clone)]
pub struct CatalogPrometheusLayer {
    /// Field for identify current context.
    ///
    /// One of the motivation case is to prefix of metrics.
    context_desc: String,
    prometheus_registry: Registry,

    name: String,
}

impl CatalogPrometheusLayer {
    /// Create iceberg context.
    pub fn new(desc: impl ToString, prometheus_registry: Registry, name: impl ToString) -> Self {
        Self {
            context_desc: desc.to_string(),
            name: name.to_string(),
            prometheus_registry,
        }
    }

    /// Description of current context.
    pub fn desc(&self) -> &str {
        self.context_desc.as_str()
    }

    /// Prometheus registry.
    pub fn prometheus_registry(&self) -> &Registry {
        &self.prometheus_registry
    }

    /// Catalog name
    pub fn name(&self) -> &str {
        &self.name
    }

    fn metrics(&self) -> CatalogMetrics {
        let def = CatalogMetricsDef::new(&self.prometheus_registry);

        let label_values = [self.desc(), self.name()];
        CatalogMetrics {
            load_table_qps: def
                .load_table_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            load_table_latency: def
                .load_table_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            list_table_qps: def
                .list_table_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            list_table_latency: def
                .list_table_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            update_table_qps: def
                .update_table_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            update_table_latency: def
                .update_table_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }
}

impl CatalogLayer for CatalogPrometheusLayer {
    type LayeredCatalog = PrometheusLayeredCatalog;

    fn layer(&self, catalog: CatalogRef) -> Arc<Self::LayeredCatalog> {
        Arc::new(PrometheusLayeredCatalog {
            inner: catalog,
            metrics: self.metrics(),
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
