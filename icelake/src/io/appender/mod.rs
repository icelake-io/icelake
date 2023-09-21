//! File appender.

mod rolling_writer;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use opendal::Operator;

use crate::error::Result;
use crate::{config::TableConfigRef, types::DataFileBuilder};
use arrow_array::RecordBatch;
use async_trait::async_trait;

use self::rolling_writer::RollingWriter;

use super::location_generator::FileLocationGenerator;

#[async_trait]
pub trait FileAppender: Send + 'static {
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    fn current_file(&self) -> String;
    fn current_row(&self) -> usize;
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>>;
}

pub trait FileAppenderLayer: Send + Sync {
    type LayeredFileAppender: FileAppender;
    fn layer<F: FileAppender>(&self, appender: F) -> Self::LayeredFileAppender;

    fn chain<L2>(self, layer: L2) -> ChainedFileAppenderLayer<Self, L2>
    where
        Self: Sized,
        L2: FileAppenderLayer,
    {
        ChainedFileAppenderLayer {
            prev: self,
            cur: layer,
        }
    }
}

struct EmptyLayer;

impl FileAppenderLayer for EmptyLayer {
    type LayeredFileAppender = Box<dyn FileAppender>;

    fn layer<F: FileAppender>(&self, appender: F) -> Self::LayeredFileAppender {
        Box::new(appender)
    }
}

#[async_trait]
impl<T: FileAppender + ?Sized> FileAppender for Box<T> {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.as_mut().write(batch).await
    }
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.as_mut().close().await
    }

    fn current_file(&self) -> String {
        self.as_ref().current_file()
    }

    fn current_row(&self) -> usize {
        self.as_ref().current_row()
    }
}

pub struct ChainedFileAppenderLayer<L1, L2>
where
    L1: FileAppenderLayer,
    L2: FileAppenderLayer,
{
    prev: L1,
    cur: L2,
}

impl<L1, L2> FileAppenderLayer for ChainedFileAppenderLayer<L1, L2>
where
    L1: FileAppenderLayer,
    L2: FileAppenderLayer,
{
    type LayeredFileAppender = L2::LayeredFileAppender;

    fn layer<F: FileAppender>(&self, appender: F) -> Self::LayeredFileAppender {
        self.cur.layer(self.prev.layer(appender))
    }
}

pub struct FileAppenderBuilder<L: FileAppenderLayer> {
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
) -> FileAppenderBuilder<impl FileAppenderLayer> {
    FileAppenderBuilder {
        operator,
        table_location,
        location_generator,
        table_config,

        layer: EmptyLayer,
    }
}

impl<L: FileAppenderLayer> FileAppenderBuilder<L> {
    pub fn layer(
        self,
        layer: impl FileAppenderLayer,
    ) -> FileAppenderBuilder<impl FileAppenderLayer> {
        FileAppenderBuilder {
            operator: self.operator,
            table_location: self.table_location,
            location_generator: self.location_generator,
            table_config: self.table_config,
            layer: layer.chain(self.layer),
        }
    }
}

#[async_trait]
pub trait FileAppenderFactory: Send + Sync {
    type F: FileAppender;
    async fn build(&self, schema: SchemaRef) -> Result<Self::F>;
}

#[async_trait]
impl<L: FileAppenderLayer> FileAppenderFactory for FileAppenderBuilder<L> {
    type F = L::LayeredFileAppender;

    async fn build(&self, schema: SchemaRef) -> Result<Self::F> {
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
