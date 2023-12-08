//! This module provide the `EqualityDeltaWriter`.

use crate::{
    io_v2::{
        CurrentFileStatus, IcebergWriteResult, IcebergWriter, IcebergWriterBuilder,
        PositionDeleteInput,
    },
    Result,
};
use arrow_array::{builder::BooleanBuilder, Int32Array, RecordBatch};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::filter;
use itertools::Itertools;
use std::collections::HashMap;

use crate::{types::FieldProjector, Error, ErrorKind};

pub const INSERT_OP: i32 = 1;
pub const DELETE_OP: i32 = 2;

///`EqualityDeltaWriter` is same as: https://github.com/apache/iceberg/blob/2e1ec5fde9e6fecfbc0883465a585a1dacb58c05/core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java#L108
///
///EqualityDeltaWriter will guarantee that there is only one row with the unique columns value written. When
///insert a row with the same unique columns value, it will delete the previous row.
///
///NOTE:
///This write is not as same with `upsert`. If the row with same unique columns is not written in
///this writer, it will not delete it.
#[derive(Clone)]
pub struct EqualityDeltaWriterBuilder<
    DB: IcebergWriterBuilder,
    PDB: IcebergWriterBuilder<PositionDeleteInput>,
    EDB: IcebergWriterBuilder,
> where
    DB::R: CurrentFileStatus,
{
    data_file_writer_builder: DB,
    sorted_cache_writer_builder: PDB,
    equality_delete_writer_builder: EDB,
    unique_column_ids: Vec<usize>,
}

impl<
        DB: IcebergWriterBuilder,
        PDB: IcebergWriterBuilder<PositionDeleteInput>,
        EDB: IcebergWriterBuilder,
    > EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB::R: CurrentFileStatus,
{
    pub fn new(
        data_file_writer_builder: DB,
        sorted_cache_writer_builder: PDB,
        equality_delete_writer_builder: EDB,
        unique_column_ids: Vec<usize>,
    ) -> Self {
        Self {
            data_file_writer_builder,
            sorted_cache_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
        }
    }
}

#[async_trait::async_trait]
impl<
        DB: IcebergWriterBuilder,
        PDB: IcebergWriterBuilder<PositionDeleteInput>,
        EDB: IcebergWriterBuilder,
    > IcebergWriterBuilder for EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB::R: CurrentFileStatus,
    PDB::R: IcebergWriter<PositionDeleteInput, R = <DB::R as IcebergWriter>::R>,
    EDB::R: IcebergWriter<R = <DB::R as IcebergWriter>::R>,
{
    type R = EqualityDeltaWriter<DB::R, PDB::R, EDB::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let data_file_writer = self.data_file_writer_builder.build(schema).await?;
        let position_delete_writer = self.sorted_cache_writer_builder.build(schema).await?;
        let equality_delete_writer = self.equality_delete_writer_builder.build(schema).await?;
        EqualityDeltaWriter::try_new(
            data_file_writer,
            position_delete_writer,
            equality_delete_writer,
            self.unique_column_ids,
            schema,
        )
    }
}

pub struct EqualityDeltaWriter<
    D: CurrentFileStatus + IcebergWriter,
    PD: IcebergWriter<PositionDeleteInput, R = D::R>,
    ED: IcebergWriter<R = D::R>,
> {
    data_file_writer: D,
    position_delete_writer: PD,
    equality_delete_writer: ED,
    inserted_rows: HashMap<OwnedRow, PositionDeleteInput>,
    row_converter: RowConverter,
    projector: FieldProjector,
}

impl<
        D: CurrentFileStatus + IcebergWriter,
        PD: IcebergWriter<PositionDeleteInput, R = D::R>,
        ED: IcebergWriter<R = D::R>,
    > EqualityDeltaWriter<D, PD, ED>
{
    pub fn try_new(
        data_file_writer: D,
        position_delete_writer: PD,
        equality_delete_writer: ED,
        unique_column_ids: Vec<usize>,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let (projector, unique_col_fields) =
            FieldProjector::new(schema.fields(), &unique_column_ids)?;

        let row_converter = RowConverter::new(
            unique_col_fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!("Failed to create row converter, error: {}", err),
            )
        })?;
        Ok(Self {
            data_file_writer,
            position_delete_writer,
            equality_delete_writer,
            inserted_rows: HashMap::new(),
            row_converter,
            projector,
        })
    }

    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_file_writer.current_file_path();
        let current_file_offset = self.data_file_writer.current_row_num();
        for (idx, row) in rows.iter().enumerate() {
            let previous_input = self.inserted_rows.insert(
                row.owned(),
                PositionDeleteInput {
                    path: current_file_path.clone(),
                    offset: (current_file_offset + idx) as i64,
                },
            );
            if let Some(previous_input) = previous_input {
                self.position_delete_writer.write(previous_input).await?;
            }
        }

        self.data_file_writer.write(batch).await?;

        Ok(())
    }

    /// Delete the batch.
    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::new();
        for row in rows.iter() {
            if let Some(previous_input) = self.inserted_rows.remove(&row.owned()) {
                self.position_delete_writer.write(previous_input).await?;
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }
        let delete_batch =
            filter::filter_record_batch(&batch, &delete_row.finish()).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to filter record batch, error: {}", err),
                )
            })?;
        self.equality_delete_writer.write(delete_batch).await?;
        Ok(())
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.projector.project(batch.columns()))
            .map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}

#[async_trait::async_trait]
impl<
        D: CurrentFileStatus + IcebergWriter,
        PD: IcebergWriter<PositionDeleteInput, R = D::R>,
        ED: IcebergWriter<R = D::R>,
    > IcebergWriter for EqualityDeltaWriter<D, PD, ED>
{
    type R = DeltaResult<D::R>;

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // check the last column is int32 array.
        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(ErrorKind::IcebergDataInvalid, ""))?;
        // partition the ops.
        let partitions =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to partition ops, error: {}", err),
                )
            })?;
        for range in partitions.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .unwrap()
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                // Insert
                INSERT_OP => self.insert(batch).await?,
                // Delete
                DELETE_OP => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Invalid ops: {op}"),
                    ))
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        let data = self.data_file_writer.flush().await?;
        let eq_delete = self.equality_delete_writer.flush().await?;
        let pos_delete = self.position_delete_writer.flush().await?;
        self.inserted_rows.clear();
        Ok(vec![DeltaResult {
            data,
            pos_delete,
            eq_delete,
        }])
    }
}

#[derive(Default)]
pub struct DeltaResult<D: IcebergWriteResult> {
    pub data: Vec<D>,
    pub pos_delete: Vec<D>,
    pub eq_delete: Vec<D>,
}

impl<D: IcebergWriteResult> IcebergWriteResult for DeltaResult<D> {
    fn set_content(&mut self, content: crate::types::DataContentType) -> &mut Self {
        self.data.iter_mut().for_each(|v| {
            v.set_content(content);
        });
        self.pos_delete.iter_mut().for_each(|v| {
            v.set_content(content);
        });
        self.eq_delete.iter_mut().for_each(|v| {
            v.set_content(content);
        });
        self
    }

    fn set_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self {
        self.data.iter_mut().for_each(|v| {
            v.set_equality_ids(equality_ids.clone());
        });
        self.pos_delete.iter_mut().for_each(|v| {
            v.set_equality_ids(equality_ids.clone());
        });
        self.eq_delete.iter_mut().for_each(|v| {
            v.set_equality_ids(equality_ids.clone());
        });
        self
    }

    fn set_partition(&mut self, partition_value: Option<crate::types::StructValue>) -> &mut Self {
        self.data.iter_mut().for_each(|v| {
            v.set_partition(partition_value.clone());
        });
        self.pos_delete.iter_mut().for_each(|v| {
            v.set_partition(partition_value.clone());
        });
        self.eq_delete.iter_mut().for_each(|v| {
            v.set_partition(partition_value.clone());
        });
        self
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::Array;

    use crate::io_v2::{
        test::{
            create_arrow_schema, create_batch, create_location_generator, create_operator,
            read_batch,
        },
        BaseFileWriterBuilder, DataFileWriterBuilder, EqualityDeleteWriterBuilder,
        EqualityDeltaWriterBuilder, IcebergWriter, IcebergWriterBuilder, ParquetWriterBuilder,
        PositionDeleteWriterBuilder,
    };

    #[tokio::test]
    async fn test_delta_writer() {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let arrow_schema = create_arrow_schema(3);
        let simple_builder = BaseFileWriterBuilder::new(
            None,
            ParquetWriterBuilder::new(
                op.clone(),
                0,
                Default::default(),
                "/".to_string(),
                Arc::new(location_generator),
            ),
        );
        let pos_delete_writer_builder =
            PositionDeleteWriterBuilder::new(simple_builder.clone(), 100);
        let data_writer_buidler = DataFileWriterBuilder::new(simple_builder.clone());
        let equality_delete_writer_builder =
            EqualityDeleteWriterBuilder::new(simple_builder, vec![1, 2]);
        let mut delta_writer = EqualityDeltaWriterBuilder::new(
            data_writer_buidler,
            pos_delete_writer_builder,
            equality_delete_writer_builder,
            vec![1, 2],
        )
        .build(&arrow_schema)
        .await
        .unwrap();

        // test insert
        let to_insert = create_batch(
            &arrow_schema,
            vec![
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
                vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
            ],
        );
        delta_writer.insert(to_insert.clone()).await.unwrap();
        let res = delta_writer.flush().await.unwrap().remove(0);
        assert_eq!(res.data.len(), 1);
        assert_eq!(res.pos_delete.len(), 0);
        assert_eq!(res.eq_delete.len(), 0);
        let res = read_batch(&op, &res.data[0].build().unwrap().file_path).await;
        assert_eq!(res, to_insert);

        // test upsert
        let to_insert = create_batch(
            &arrow_schema,
            vec![vec![10, 11], vec![10, 11], vec![10, 11]],
        );
        let to_delete = create_batch(&arrow_schema, vec![vec![11, 1], vec![11, 1], vec![11, 1]]);
        delta_writer.insert(to_insert.clone()).await.unwrap();
        delta_writer.delete(to_delete.clone()).await.unwrap();
        let delta_res = delta_writer.flush().await.unwrap().remove(0);
        assert_eq!(delta_res.data.len(), 1);
        assert_eq!(delta_res.pos_delete.len(), 1);
        assert_eq!(delta_res.eq_delete.len(), 1);
        let data = delta_res.data[0].build().unwrap();
        let pos_delete = delta_res.pos_delete[0].build().unwrap();
        let eq_delete = delta_res.eq_delete[0].build().unwrap();
        // check return type
        assert_eq!(data.content, crate::types::DataContentType::Data);
        assert_eq!(
            pos_delete.content,
            crate::types::DataContentType::PositionDeletes
        );
        assert_eq!(
            eq_delete.content,
            crate::types::DataContentType::EqualityDeletes
        );
        let data_batch = read_batch(&op, &data.file_path).await;
        let pos_delete_batch = read_batch(&op, &pos_delete.file_path).await;
        let eq_delete_batch = read_batch(&op, &eq_delete.file_path).await;
        // check data
        assert_eq!(data_batch, to_insert);
        // check position delete
        let paths = pos_delete_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths.value(0), data.file_path);
        assert_eq!(
            eq_delete_batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        // check equality delete
        assert_eq!(
            eq_delete_batch,
            create_batch(&create_arrow_schema(2), vec![vec![1], vec![1]])
        );
    }
}
