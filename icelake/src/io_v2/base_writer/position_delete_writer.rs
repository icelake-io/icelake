//! A module provide `PositionDeleteWriter`.

use std::sync::Arc;

use crate::io_v2::{
    Combinable, FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriteResult, IcebergWriter,
    IcebergWriterBuilder, SortWriter, SortWriterBuilder,
};
use crate::types::{Any, Field, Primitive};
use crate::Result;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef as ArrowFieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use itertools::Itertools;

#[cfg(feature = "prometheus")]
pub use prometheus::*;

pub struct PositionDeleteMetrics {
    pub current_cache_number: usize,
}

#[derive(Clone)]
pub struct PositionDeleteWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    cache_num: usize,
}

impl<B: FileWriterBuilder> PositionDeleteWriterBuilder<B> {
    pub fn new(inner: B, cache_num: usize) -> Self {
        Self { inner, cache_num }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for PositionDeleteWriterBuilder<B> {
    type R = PositionDeleteWriter<B>;

    async fn build(self, _schema: &ArrowSchemaRef) -> Result<Self::R> {
        let fields: Vec<ArrowFieldRef> = vec![
            Arc::new(
                Field::required(2147483546, "file_path", Any::Primitive(Primitive::String))
                    .try_into()?,
            ),
            Arc::new(
                Field::required(2147483545, "pos", Any::Primitive(Primitive::Long)).try_into()?,
            ),
        ];
        let schema = ArrowSchema::new(fields).into();
        Ok(PositionDeleteWriter {
            inner_writer: SortWriterBuilder::new(self.inner, vec![], self.cache_num)
                .build(&schema)
                .await?,
        })
    }
}

//Position deletes are required to be sorted by file and position,
pub struct PositionDeleteWriter<B: FileWriterBuilder> {
    inner_writer: SortWriter<PositionDeleteInput, B>,
}

impl<B: FileWriterBuilder> PositionDeleteWriter<B> {
    pub fn metrics(&self) -> PositionDeleteMetrics {
        let metrics = self.inner_writer.metrics();
        PositionDeleteMetrics {
            current_cache_number: metrics.current_cache_number,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter<PositionDeleteInput> for PositionDeleteWriter<B> {
    type R = <<B::R as FileWriter>::R as FileWriteResult>::R;
    async fn write(&mut self, input: PositionDeleteInput) -> Result<()> {
        self.inner_writer.write(input).await
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        let res = self
            .inner_writer
            .flush()
            .await?
            .into_iter()
            .map(|mut res| {
                res.set_content(crate::types::DataContentType::PositionDeletes)
                    .set_partition(None);
                res
            })
            .collect_vec();
        Ok(res)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PositionDeleteInput {
    pub path: String,
    pub offset: i64,
}

impl Combinable for PositionDeleteInput {
    fn combine(vec: Vec<Self>) -> RecordBatch {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false),
        ]);
        let columns = vec![
            Arc::new(arrow_array::StringArray::from(
                vec.iter().map(|i| i.path.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(arrow_array::Int64Array::from(
                vec.iter().map(|i| i.offset).collect::<Vec<_>>(),
            )) as ArrayRef,
        ];
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn size(&self) -> usize {
        1
    }
}
#[cfg(feature = "prometheus")]
mod prometheus {
    use crate::io_v2::{FileWriterBuilder, IcebergWriter, IcebergWriterBuilder};
    use crate::Result;
    use prometheus::core::{AtomicU64, GenericGauge};

    use super::{
        PositionDeleteInput, PositionDeleteMetrics, PositionDeleteWriter,
        PositionDeleteWriterBuilder,
    };

    #[derive(Clone)]
    pub struct PositionDeleteWriterWithMetricsBuilder<B: FileWriterBuilder> {
        current_cache_number: GenericGauge<AtomicU64>,
        inner: PositionDeleteWriterBuilder<B>,
    }

    impl<B: FileWriterBuilder> PositionDeleteWriterWithMetricsBuilder<B> {
        pub fn new(
            inner: PositionDeleteWriterBuilder<B>,
            current_cache_number: GenericGauge<AtomicU64>,
        ) -> Self {
            Self {
                inner,
                current_cache_number,
            }
        }
    }

    #[async_trait::async_trait]
    impl<B: FileWriterBuilder> IcebergWriterBuilder for PositionDeleteWriterWithMetricsBuilder<B> {
        type R = PositionDeleteWriterWithMetrics<B>;

        async fn build(self, schema: &arrow_schema::SchemaRef) -> crate::Result<Self::R> {
            let writer = self.inner.build(schema).await?;
            Ok(PositionDeleteWriterWithMetrics {
                writer,
                cache_number: self.current_cache_number,
                current_metrics: PositionDeleteMetrics {
                    current_cache_number: 0,
                },
            })
        }
    }

    pub struct PositionDeleteWriterWithMetrics<B: FileWriterBuilder> {
        writer: PositionDeleteWriter<B>,

        // metrics
        cache_number: GenericGauge<AtomicU64>,
        current_metrics: PositionDeleteMetrics,
    }

    impl<B: FileWriterBuilder> PositionDeleteWriterWithMetrics<B> {
        fn update_metrics(&mut self) -> Result<()> {
            let last_metrics = std::mem::replace(&mut self.current_metrics, self.writer.metrics());
            {
                let delta = self.current_metrics.current_cache_number as i64
                    - last_metrics.current_cache_number as i64;
                if delta > 0 {
                    self.cache_number.add(delta as u64);
                } else {
                    self.cache_number.sub(delta.unsigned_abs());
                }
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl<B: FileWriterBuilder> IcebergWriter<PositionDeleteInput>
        for PositionDeleteWriterWithMetrics<B>
    {
        type R = <PositionDeleteWriter<B> as IcebergWriter<PositionDeleteInput>>::R;

        async fn write(&mut self, input: crate::io_v2::PositionDeleteInput) -> crate::Result<()> {
            self.writer.write(input).await?;
            self.update_metrics()?;
            Ok(())
        }

        async fn flush(&mut self) -> crate::Result<Vec<Self::R>> {
            let res = self.writer.flush().await?;
            self.update_metrics()?;
            Ok(res)
        }
    }

    #[cfg(test)]
    mod test {
        use std::sync::Arc;

        use prometheus::core::GenericGauge;

        use crate::io_v2::{
            position_delete_writer::test::generate_test_data,
            test::{create_arrow_schema, create_location_generator, create_operator},
            BaseFileWriterBuilder, IcebergWriter, IcebergWriterBuilder, ParquetWriterBuilder,
            PositionDeleteWriterBuilder,
        };

        #[tokio::test]
        async fn test_metrics_writer() {
            // create writer
            let op = create_operator();
            let location_generator = create_location_generator();
            let parquet_writer_builder = ParquetWriterBuilder::new(
                op.clone(),
                0,
                Default::default(),
                "/".to_string(),
                Arc::new(location_generator),
            );
            let delete_writer_builder = PositionDeleteWriterBuilder::new(
                BaseFileWriterBuilder::new(None, parquet_writer_builder),
                4,
            );
            let metrics = GenericGauge::new("test", "test").unwrap();
            let metrics_builder = super::PositionDeleteWriterWithMetricsBuilder::new(
                delete_writer_builder,
                metrics.clone(),
            );

            // prepare data
            let (position_delete, _, _) = generate_test_data(vec![("file1", 3)]);

            let mut writer_1 = metrics_builder
                .clone()
                .build(&create_arrow_schema(2))
                .await
                .unwrap();
            let mut writer_2 = metrics_builder
                .clone()
                .build(&create_arrow_schema(2))
                .await
                .unwrap();

            writer_1.write(position_delete[0].clone()).await.unwrap();
            writer_2.write(position_delete[0].clone()).await.unwrap();
            writer_1.write(position_delete[0].clone()).await.unwrap();
            writer_2.write(position_delete[0].clone()).await.unwrap();
            writer_1.write(position_delete[0].clone()).await.unwrap();
            writer_2.write(position_delete[0].clone()).await.unwrap();

            assert_eq!(metrics.get(), 6);

            let mut writer_3 = metrics_builder
                .clone()
                .build(&create_arrow_schema(2))
                .await
                .unwrap();

            writer_3.write(position_delete[0].clone()).await.unwrap();
            assert_eq!(metrics.get(), 7);

            writer_1.write(position_delete[0].clone()).await.unwrap();
            assert_eq!(metrics.get(), 4);

            writer_2.write(position_delete[0].clone()).await.unwrap();
            assert_eq!(metrics.get(), 1);

            writer_3.flush().await.unwrap();
            assert_eq!(metrics.get(), 0);
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::io_v2::test::{
        create_arrow_schema, create_location_generator, create_operator, read_batch,
    };
    use crate::io_v2::{
        BaseFileWriterBuilder, IcebergWriter, IcebergWriterBuilder, ParquetWriterBuilder,
        PositionDeleteInput, PositionDeleteWriterBuilder,
    };

    pub fn generate_test_data(
        mut input: Vec<(&str, i64)>,
    ) -> (Vec<PositionDeleteInput>, Vec<&str>, Vec<i64>) {
        let position_delete = input
            .iter()
            .map(|(path, offset)| PositionDeleteInput {
                path: path.to_string(),
                offset: *offset,
            })
            .collect_vec();
        input.sort();
        let path_col = input.iter().map(|(path, _)| *path).collect_vec();
        let offset_col = input.iter().map(|(_, offset)| *offset).collect_vec();
        (position_delete, path_col, offset_col)
    }

    #[tokio::test]
    async fn test_position_delete_writer() {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let mut delete_writer = PositionDeleteWriterBuilder::new(
            BaseFileWriterBuilder::new(None, parquet_writer_builder),
            7,
        )
        .build(&create_arrow_schema(2))
        .await
        .unwrap();

        // prepare data
        let (position_delete, path_col, offset_col) = generate_test_data(vec![
            ("file1", 3),
            ("file1", 2),
            ("file1", 2),
            ("file2", 10),
            ("file3", 30),
            ("file1", 1),
            ("file2", 20),
        ]);

        // write data
        for input in position_delete {
            delete_writer.write(input).await.unwrap()
        }

        // check result
        let data_file_builder = delete_writer.flush().await.unwrap();
        assert_eq!(data_file_builder.len(), 1);
        let data_file = data_file_builder[0].build().unwrap();
        let batch = read_batch(&op, &data_file.file_path).await;
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap(),
            &arrow_array::StringArray::from(path_col)
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap(),
            &arrow_array::Int64Array::from(offset_col)
        );

        // prepare data
        let (position_delete, path_col, offset_col) = generate_test_data(vec![
            ("file1", 3),
            ("file1", 2),
            ("file1", 2),
            ("file2", 10),
            ("file3", 30),
            ("file1", 1),
            ("file2", 20),
        ]);
        let (position_delete2, path_col2, offset_col2) = generate_test_data(vec![
            ("file4", 123),
            ("file1", 12),
            ("file7", 11),
            ("file7", 1),
            ("file3", 999),
            ("file2", 2),
            ("file2", 23),
        ]);

        // write data
        for input in position_delete {
            delete_writer.write(input).await.unwrap()
        }
        for input in position_delete2 {
            delete_writer.write(input).await.unwrap()
        }

        // check result
        let data_file_builder = delete_writer.flush().await.unwrap();
        assert_eq!(data_file_builder.len(), 2);

        // check file 1
        let data_file = data_file_builder[0].build().unwrap();
        let batch = read_batch(&op, &data_file.file_path).await;
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap(),
            &arrow_array::StringArray::from(path_col)
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap(),
            &arrow_array::Int64Array::from(offset_col)
        );

        // check file 2
        let data_file = data_file_builder[1].build().unwrap();
        let batch = read_batch(&op, &data_file.file_path).await;
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap(),
            &arrow_array::StringArray::from(path_col2)
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap(),
            &arrow_array::Int64Array::from(offset_col2)
        );
    }
}
