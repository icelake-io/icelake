//! Prometheus layer for FileAppender.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use prometheus::{
    core::{AtomicU64, GenericCounter},
    Histogram,
};

use crate::{
    io::{RecordBatchWriter, RecordBatchWriterBuilder},
    types::DataFileBuilder,
    Result,
};

#[derive(Clone)]
pub struct AppenderMetrics {
    write_qps: GenericCounter<AtomicU64>,
    write_latency: Histogram,
}

impl AppenderMetrics {
    pub fn new(write_qps: GenericCounter<AtomicU64>, write_latency: Histogram) -> Self {
        Self {
            write_qps,
            write_latency,
        }
    }
}

#[derive(Clone)]
pub struct PrometheusAppenderBuilder<B: RecordBatchWriterBuilder> {
    inner: B,
    metrics: AppenderMetrics,
}

impl<B: RecordBatchWriterBuilder> PrometheusAppenderBuilder<B> {
    /// Create writer context.
    pub fn new(inner: B, metrics: AppenderMetrics) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait::async_trait]
impl<B: RecordBatchWriterBuilder> RecordBatchWriterBuilder for PrometheusAppenderBuilder<B> {
    type R = PrometheusAppender<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let appender = self.inner.build(schema).await?;
        Ok(PrometheusAppender {
            appender,
            metrics: self.metrics,
        })
    }
}

pub struct PrometheusAppender<F: RecordBatchWriter> {
    appender: F,
    metrics: AppenderMetrics,
}

#[async_trait]
impl<F: RecordBatchWriter> RecordBatchWriter for PrometheusAppender<F> {
    async fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.metrics.write_qps.inc();
        let _ = self.metrics.write_latency.start_timer();
        self.appender.write(record).await
    }

    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.appender.close().await
    }
}
