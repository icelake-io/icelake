//! File appender.
#[cfg(feature = "prometheus")]
pub mod prometheus;
mod rolling_writer;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use opendal::Operator;

use crate::error::Result;
use crate::{config::TableConfigRef, types::DataFileBuilder};
use arrow_array::RecordBatch;
use async_trait::async_trait;

pub use self::rolling_writer::RollingWriter;

use super::location_generator::FileLocationGenerator;

#[async_trait]
pub trait FileAppender: Send + 'static {
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    fn current_file(&self) -> String;
    fn current_row(&self) -> usize;
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>>;
}

pub trait FileAppenderLayer<F: FileAppender>: Send {
    type R: FileAppender;
    fn layer(&self, appender: F) -> Self::R;

    fn chain<L2>(self, layer: L2) -> ChainedFileAppenderLayer<Self, F, L2>
    where
        Self: Sized,
        L2: FileAppenderLayer<Self::R>,
    {
        ChainedFileAppenderLayer {
            prev: self,
            cur: layer,
            _f1: PhantomData,
        }
    }
}

pub type DefaultFileAppender = RollingWriter;
pub type DefaultFileAppenderLayer<R> = dyn FileAppenderLayer<DefaultFileAppender, R = R>;

pub struct EmptyLayer;

impl<F: FileAppender> FileAppenderLayer<F> for EmptyLayer {
    type R = F;
    fn layer(&self, appender: F) -> F {
        appender
    }
}

pub struct ChainedFileAppenderLayer<L1, F1, L2>
where
    L1: FileAppenderLayer<F1>,
    F1: FileAppender,
    L2: FileAppenderLayer<L1::R>,
{
    prev: L1,
    _f1: PhantomData<fn() -> F1>,
    cur: L2,
}

impl<L1, F1, L2> FileAppenderLayer<F1> for ChainedFileAppenderLayer<L1, F1, L2>
where
    L1: FileAppenderLayer<F1>,
    F1: FileAppender,
    L2: FileAppenderLayer<L1::R>,
{
    type R = L2::R;
    fn layer(&self, appender: F1) -> Self::R {
        self.cur.layer(self.prev.layer(appender))
    }
}

pub struct FileAppenderBuilder<L: FileAppenderLayer<DefaultFileAppender>> {
    // Underlying file appender builder
    operator: Operator,
    table_location: String,
    location_generator: Arc<FileLocationGenerator>,
    table_config: TableConfigRef,

    layer: L,
}

pub fn new_file_appender_builder(
    operator: Operator,
    table_location: String,
    location_generator: Arc<FileLocationGenerator>,
    table_config: TableConfigRef,
) -> FileAppenderBuilder<EmptyLayer> {
    FileAppenderBuilder {
        operator,
        table_location,
        location_generator,
        table_config,

        layer: EmptyLayer,
    }
}

impl<L: FileAppenderLayer<DefaultFileAppender>> FileAppenderBuilder<L> {
    pub fn layer<L2: FileAppenderLayer<L::R>>(
        self,
        layer: L2,
    ) -> FileAppenderBuilder<ChainedFileAppenderLayer<L, DefaultFileAppender, L2>> {
        FileAppenderBuilder {
            operator: self.operator,
            table_location: self.table_location,
            location_generator: self.location_generator,
            table_config: self.table_config,
            layer: self.layer.chain(layer),
        }
    }

    pub async fn build(&self, schema: SchemaRef) -> Result<L::R> {
        let inner = RollingWriter::try_new(
            self.operator.clone(),
            self.table_location.clone(),
            self.location_generator.clone(),
            schema,
            self.table_config.clone(),
        )
        .await?;

        Ok(self.layer.layer(inner))
    }
}
