//! Prometheus layer for FileAppender.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use prometheus::{
    core::{AtomicU64, GenericCounter},
    Histogram,
};

use crate::{types::DataFileBuilder, Result};

use super::{FileAppender, FileAppenderLayer};

/// File writer context
#[derive(Clone)]
pub struct WriterPrometheusLayer {
    metrics: FileAppenderMetrics,
}

impl WriterPrometheusLayer {
    /// Create writer context.
    pub fn new(metrics: FileAppenderMetrics) -> Self {
        Self { metrics }
    }
}

impl<F: FileAppender> FileAppenderLayer<F> for WriterPrometheusLayer {
    type R = PrometheusLayeredFileAppender<F>;

    fn layer(&self, appender: F) -> Self::R {
        PrometheusLayeredFileAppender {
            appender,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct FileAppenderMetrics {
    write_qps: GenericCounter<AtomicU64>,
    write_latency: Histogram,
}

impl FileAppenderMetrics {
    pub fn new(write_qps: GenericCounter<AtomicU64>, write_latency: Histogram) -> Self {
        Self {
            write_qps,
            write_latency,
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
