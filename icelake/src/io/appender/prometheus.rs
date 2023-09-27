//! Prometheus layer for FileAppender.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use prometheus::{
    core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge},
    histogram_opts, labels, opts, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, Histogram,
    HistogramVec, IntCounterVec, IntGaugeVec, Registry, DEFAULT_BUCKETS,
};

use crate::TableIdentifier;
use crate::{types::DataFileBuilder, Result};

use super::{FileAppender, FileAppenderLayer};

const WRITER_METRICS_LABEL_NAMES: &[&str] = &["context", "catalog", "table"];

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
#[derive(Clone)]
pub(crate) struct FileAppenderMetricsDef {
    write_qps: IntCounterVec,
    write_latency: HistogramVec,

    flush_qps: IntCounterVec,
    flush_latency: HistogramVec,

    in_memory_data_file_num: IntGaugeVec,
}

impl FileAppenderMetricsDef {
    fn new(registry: &Registry) -> Self {
        let write_qps = {
            register_int_counter_vec_with_registry!(
                opts!(
                    "iceberg_file_appender_write_qps",
                    "Iceberg file appender write qps",
                ),
                WRITER_METRICS_LABEL_NAMES,
                registry,
            )
            .unwrap()
        };

        let write_latency = {
            register_histogram_vec_with_registry!(
                histogram_opts!(
                    "iceberg_file_appender_write_latency",
                    "Iceberg file appender write latency",
                    DEFAULT_BUCKETS.to_vec(),
                ),
                WRITER_METRICS_LABEL_NAMES,
                registry,
            )
            .unwrap()
        };

        let flush_qps = {
            register_int_counter_vec_with_registry!(
                opts!(
                    "iceberg_file_appender_flush_qps",
                    "Iceberg file appender flush qps",
                ),
                WRITER_METRICS_LABEL_NAMES,
                registry,
            )
            .unwrap()
        };

        let flush_latency = {
            register_histogram_vec_with_registry!(
                histogram_opts!(
                    "iceberg_file_appender_flush_latency",
                    "Iceberg file appender flush latency",
                    DEFAULT_BUCKETS.to_vec(),
                ),
                WRITER_METRICS_LABEL_NAMES,
                registry,
            )
            .unwrap()
        };

        let in_memory_data_file_num = {
            register_int_gauge_vec_with_registry!(
                opts!(
                    "iceberg_in_memory_data_file_num",
                    "The number of in memory data file of rolling file writer",
                ),
                WRITER_METRICS_LABEL_NAMES,
                registry,
            )
            .unwrap()
        };

        Self {
            write_qps,
            write_latency,
            flush_qps,
            flush_latency,
            in_memory_data_file_num,
        }
    }
}

pub(crate) struct FileAppenderMetrics {
    pub(crate) write_qps: GenericCounter<AtomicU64>,
    pub(crate) write_latency: Histogram,

    pub(crate) flush_qps: GenericCounter<AtomicU64>,
    pub(crate) flush_latency: Histogram,

    pub(crate) in_memory_data_file_num: GenericGauge<AtomicI64>,
}

impl WriterPrometheusLayer {
    pub(crate) fn file_appender_metrics(&self) -> FileAppenderMetrics {
        let label_values = [
            self.context_id.as_str(),
            self.catalog_name.as_str(),
            self.table_name(),
        ];

        let def = FileAppenderMetricsDef::new(&self.registry);

        FileAppenderMetrics {
            write_qps: def
                .write_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            write_latency: def
                .write_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            flush_qps: def
                .flush_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            flush_latency: def
                .flush_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            in_memory_data_file_num: def
                .in_memory_data_file_num
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }
}

impl<F: FileAppender + Sync> FileAppenderLayer<F> for WriterPrometheusLayer {
    type R = PrometheusLayeredFileAppender<F>;

    fn layer(&self, appender: F) -> Self::R {
        PrometheusLayeredFileAppender {
            appender,
            metrics: self.file_appender_metrics(),
        }
    }
}

pub struct PrometheusLayeredFileAppender<F: FileAppender> {
    appender: F,
    metrics: FileAppenderMetrics,
}

#[async_trait]
impl<F: FileAppender> FileAppender for PrometheusLayeredFileAppender<F> {
    async fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.metrics.write_qps.inc();
        let _ = self.metrics.write_latency.start_timer();
        self.appender.write(record).await
    }

    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.appender.close().await
    }

    fn current_file(&self) -> String {
        self.appender.current_file()
    }

    fn current_row(&self) -> usize {
        self.appender.current_row()
    }
}
