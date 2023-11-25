//! Prometheus layer for FileAppender.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use prometheus::{
    core::{AtomicU64, GenericCounter},
    Histogram,
};

use crate::{
    io::{IcebergWriter, IcebergWriterBuilder},
    Result,
};

#[derive(Clone)]
pub struct WriterMetrics {
    write_qps: GenericCounter<AtomicU64>,
    write_latency: Histogram,
}

impl WriterMetrics {
    pub fn new(write_qps: GenericCounter<AtomicU64>, write_latency: Histogram) -> Self {
        Self {
            write_qps,
            write_latency,
        }
    }
}

#[derive(Clone)]
pub struct PrometheusWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    metrics: WriterMetrics,
}

impl<B: IcebergWriterBuilder> PrometheusWriterBuilder<B> {
    /// Create writer context.
    pub fn new(inner: B, metrics: WriterMetrics) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for PrometheusWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = PrometheusWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let appender = self.inner.build(schema).await?;
        Ok(PrometheusWriter {
            appender,
            metrics: self.metrics,
        })
    }
}

pub struct PrometheusWriter<F: IcebergWriter> {
    appender: F,
    metrics: WriterMetrics,
}

#[async_trait]
impl<F: IcebergWriter> IcebergWriter for PrometheusWriter<F> {
    type R = F::R;

    async fn write(&mut self, record: RecordBatch) -> Result<()> {
        self.metrics.write_qps.inc();
        let _ = self.metrics.write_latency.start_timer();
        self.appender.write(record).await
    }

    async fn flush(&mut self) -> Result<Self::R> {
        self.appender.flush().await
    }
}
