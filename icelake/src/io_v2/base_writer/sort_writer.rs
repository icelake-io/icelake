use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use itertools::Itertools;

use crate::io_v2::{
    FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriter, IcebergWriterBuilder,
};
use crate::Result;

pub struct SortWriterMetrics {
    pub current_cache_number: usize,
}

pub(crate) struct SortWriterBuilder<I, B: FileWriterBuilder> {
    inner: B,
    sort_col_index: Vec<usize>,
    cache_number: usize,
    _marker: std::marker::PhantomData<I>,
}

impl<I, B: FileWriterBuilder> Clone for SortWriterBuilder<I, B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            sort_col_index: self.sort_col_index.clone(),
            cache_number: self.cache_number,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, B: FileWriterBuilder> SortWriterBuilder<I, B> {
    pub fn new(inner: B, sort_col_index: Vec<usize>, cache_number: usize) -> Self {
        Self {
            inner,
            sort_col_index,
            cache_number,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<I: Combinable + Ord, B: FileWriterBuilder> IcebergWriterBuilder<I>
    for SortWriterBuilder<I, B>
{
    type R = SortWriter<I, B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        Ok(SortWriter {
            inner_builder: self.inner,
            schema: schema.clone(),
            cache: vec![],
            current_cache_number: 0,
            cache_number: self.cache_number,
            _sort_col_index: self.sort_col_index,
            result: vec![],
        })
    }
}

pub struct SortWriter<I: Combinable, B: FileWriterBuilder> {
    inner_builder: B,
    schema: SchemaRef,

    cache: Vec<I>,
    current_cache_number: usize,

    cache_number: usize,
    _sort_col_index: Vec<usize>,
    result: Vec<<<B::R as FileWriter>::R as FileWriteResult>::R>,
}

impl<I: Combinable + Ord, B: FileWriterBuilder> SortWriter<I, B> {
    async fn flush_by_ord(&mut self) -> Result<()> {
        let mut new_writer = self.inner_builder.clone().build(&self.schema).await?;

        self.current_cache_number = 0;
        let mut cache = std::mem::take(&mut self.cache);
        cache.sort();
        let batch = Combinable::combine(cache);

        // Write batch
        new_writer.write(&batch).await?;
        let res = new_writer
            .close()
            .await?
            .into_iter()
            .map(|res| res.to_iceberg_result())
            .collect_vec();
        self.result.extend(res);
        Ok(())
    }

    pub fn metrics(&self) -> SortWriterMetrics {
        SortWriterMetrics {
            current_cache_number: self.current_cache_number,
        }
    }
}

#[async_trait::async_trait]
impl<I: Combinable + Ord, B: FileWriterBuilder> IcebergWriter<I> for SortWriter<I, B> {
    type R = <<B::R as FileWriter>::R as FileWriteResult>::R;
    async fn write(&mut self, input: I) -> Result<()> {
        self.current_cache_number += input.size();
        self.cache.push(input);
        if self.current_cache_number >= self.cache_number {
            self.flush_by_ord().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        if !self.cache.is_empty() {
            self.flush_by_ord().await?;
        }
        Ok(std::mem::take(&mut self.result))
    }
}

pub trait Combinable: Sized + Send + 'static {
    fn combine(vec: Vec<Self>) -> RecordBatch;
    fn size(&self) -> usize;
}
