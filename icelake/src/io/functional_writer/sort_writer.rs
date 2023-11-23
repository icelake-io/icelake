use arrow_array::RecordBatch;
use arrow_ord::sort::sort_to_indices;
use arrow_schema::SchemaRef;
use arrow_select::{concat::concat_batches, take::take};
use async_trait::async_trait;

use crate::io::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

#[derive(Clone)]
pub struct SortWriterBuilder<I: Cacheable, B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner: B,
    sort_col_index: usize,
    cache_number: usize,
    _marker: std::marker::PhantomData<I>,
}

impl<I: Cacheable, B: IcebergWriterBuilder> SortWriterBuilder<I, B>
where
    B::R: IcebergWriter,
{
    pub fn new(inner: B, sort_col_index: usize, cache_number: usize) -> Self {
        Self {
            inner,
            sort_col_index,
            cache_number,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<I: Cacheable, B: IcebergWriterBuilder> IcebergWriterBuilder for SortWriterBuilder<I, B>
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
            sort_col_index: self.sort_col_index,
            result: Default::default(),
        })
    }
}

pub struct SortWriter<I: Cacheable, B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner_builder: B,
    schema: SchemaRef,
    cache: Vec<I>,
    current_cache_number: usize,
    cache_number: usize,
    sort_col_index: usize,
    result: <<B as IcebergWriterBuilder>::R as IcebergWriter>::R,
}

impl<I: Cacheable, B: IcebergWriterBuilder> SortWriter<I, B>
where
    B::R: IcebergWriter,
{
    async fn flush(&mut self) -> Result<()> {
        let mut new_writer = self.inner_builder.clone().build(&self.schema).await?;

        // Concat batch
        let cache = std::mem::take(&mut self.cache);
        let batch = I::combine(cache)?;

        // Sort batch by sort_col_index
        let indices =
            sort_to_indices(batch.column(self.sort_col_index), None, None).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Fail to sort cached batch: {}", err),
                )
            })?;
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();

        // Write batch
        new_writer.write(sorted).await?;
        self.result.combine(new_writer.close().await?);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<I: Cacheable, B: IcebergWriterBuilder> IcebergWriter<I> for SortWriter<I, B>
where
    B::R: IcebergWriter,
{
    type R = <B::R as IcebergWriter>::R;
    async fn write(&mut self, input: I) -> Result<()> {
        self.current_cache_number += input.size();
        self.cache.push(input);
        if self.current_cache_number >= self.cache_number {
            self.flush().await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Self::R> {
        if self.current_cache_number > 0 {
            self.flush().await?;
        }
        Ok(std::mem::take(&mut self.result))
    }
}

pub trait Cacheable: Sized + Clone + Send + Sync + 'static {
    fn combine(vec: Vec<Self>) -> Result<RecordBatch>;
    fn size(&self) -> usize;
}

impl Cacheable for RecordBatch {
    fn combine(vec: Vec<Self>) -> Result<RecordBatch> {
        concat_batches(&vec[0].schema(), vec.iter()).map_err(|err| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Fail concat cached batch: {}", err),
            )
        })
    }

    fn size(&self) -> usize {
        self.num_rows()
    }
}
