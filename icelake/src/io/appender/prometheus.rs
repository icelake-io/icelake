//! Prometheus layer for FileAppender.

const WRITER_METRICS_LABEL_NAMES: &[&str] = &["context", "catalog", "table"];
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
            self.catalog_context().desc(),
            self.catalog_name(),
            self.table_name(),
        ];

        let def = self.catalog_context().metrics_def();
        FileAppenderMetrics {
            write_qps: def
                .file_appender_metrics_def
                .write_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            write_latency: def
                .file_appender_metrics_def
                .write_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            flush_qps: def
                .file_appender_metrics_def
                .flush_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            flush_latency: def
                .file_appender_metrics_def
                .flush_latency
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            in_memory_data_file_num: def
                .file_appender_metrics_def
                .in_memory_data_file_num
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }
}

impl FileAppenderLayer<F: FileAppender> for WriterPrometheusLayer {
    type LayeredFileAppender = PrometheusLayeredFileAppender<F>;

    fn layer(&self, appender: F) -> Self::LayeredFileAppender {
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

impl FileAppender for PrometheusLayeredFileAppender<F: FileAppender> {
    fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.metrics.write_qps.inc();
        let _ = self.metrics.write_latency.start_timer();
        self.appender.write(record)
    }

    fn close(&mut self) -> Result<()> {
        self.appender.close()
    }
}

impl FileAppender for PrometheusLayeredFileAppender<RollingWriter> {
    fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.metrics.write_qps.inc();
        let _ = self.metrics.write_latency.start_timer();
        self.appender.write(record)
    }

    fn close(&mut self) -> Result<()> {
        self.appender.close()
    }
}
