use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::io::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder};
use crate::Result;

pub(crate) struct SortWriterBuilder<I, B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner: B,
    sort_col_index: Vec<usize>,
    cache_number: usize,
    _marker: std::marker::PhantomData<I>,
}

impl<I, B: IcebergWriterBuilder> Clone for SortWriterBuilder<I, B>
where
    B::R: IcebergWriter,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            sort_col_index: self.sort_col_index.clone(),
            cache_number: self.cache_number,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, B: IcebergWriterBuilder> SortWriterBuilder<I, B>
where
    B::R: IcebergWriter,
{
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
impl<I: Combinable, B: IcebergWriterBuilder> IcebergWriterBuilder for SortWriterBuilder<I, B>
where
    B::R: IcebergWriter,
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
            result: Default::default(),
        })
    }
}

pub struct SortWriter<I: Combinable, B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner_builder: B,
    schema: SchemaRef,
    cache: Vec<I>,
    current_cache_number: usize,
    cache_number: usize,
    _sort_col_index: Vec<usize>,
    result: <<B as IcebergWriterBuilder>::R as IcebergWriter>::R,
}

impl<I: Combinable + Ord, B: IcebergWriterBuilder> SortWriter<I, B>
where
    B::R: IcebergWriter,
{
    async fn flush_by_ord(&mut self) -> Result<()> {
        let mut new_writer = self.inner_builder.clone().build(&self.schema).await?;

        let mut cache = std::mem::take(&mut self.cache);
        cache.sort();
        let batch = Combinable::combine(cache);

        // Write batch
        new_writer.write(batch).await?;
        self.result.combine(new_writer.close().await?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<I: Combinable + Ord, B: IcebergWriterBuilder> IcebergWriter<I> for SortWriter<I, B>
where
    B::R: IcebergWriter,
{
    type R = <<B as IcebergWriterBuilder>::R as IcebergWriter>::R;
    async fn write(&mut self, input: I) -> Result<()> {
        self.current_cache_number += input.size();
        self.cache.push(input);
        if self.current_cache_number == self.cache_number {
            self.flush_by_ord().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Self::R> {
        if !self.cache.is_empty() {
            self.flush_by_ord().await?;
        }
        Ok(self.result.flush())
    }
}

pub trait Combinable: Sized + Send + 'static {
    fn combine(vec: Vec<Self>) -> RecordBatch;
    fn size(&self) -> usize;
}
