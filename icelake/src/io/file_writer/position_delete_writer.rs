//! A module provide `PositionDeleteWriter`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::io::{Cacheable, IcebergWriteResult, IcebergWriter, IcebergWriterBuilder};
use crate::types::{Any, Field, FieldProjector, Primitive, COLUMN_ID_META_KEY};
use crate::Result;
use crate::{Error, ErrorKind};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{
    DataType, Field as ArrowField, FieldRef as ArrowFieldRef, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};

#[derive(Clone)]
pub struct PositionDeleteWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    row_field_ids: Option<Vec<i32>>,
}

impl<B: IcebergWriterBuilder> PositionDeleteWriterBuilder<B> {
    /// According iceberg spec, PositionDeleteFile have a row field whose schema
    /// may be any subset of the table schema and must use field ids matching the table.
    /// ref: https://iceberg.apache.org/spec/#position-delete-files:~:text=When%20the%20deleted%20row%20column%20is%20present%2C%20its%20schema%20may%20be%20any%20subset%20of%20the%20table%20schema%20and%20must%20use%20field%20ids%20matching%20the%20table.
    ///
    /// User can specify the field id in  `row_field_ids` and pass the whole row in write. The writer will extract the row field automatically.
    /// If it is `None``, this field will be omitted.
    pub fn new(inner: B, row_field_ids: Option<Vec<i32>>) -> Self {
        Self {
            inner,
            row_field_ids,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for PositionDeleteWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = PositionDeleteWriter<B::R>;

    async fn build(self, schema: &ArrowSchemaRef) -> Result<Self::R> {
        let mut fields: Vec<ArrowFieldRef> = vec![
            Arc::new(
                Field::required(2147483546, "file_path", Any::Primitive(Primitive::String))
                    .try_into()?,
            ),
            Arc::new(
                Field::required(2147483545, "pos", Any::Primitive(Primitive::Long)).try_into()?,
            ),
        ];
        let row_projector = if let Some(col_ids) = self.row_field_ids {
            let (projector, row_fields) = FieldProjector::new(schema.fields(), &col_ids)?;
            let field = ArrowField::new("row", DataType::Struct(row_fields.clone()), false)
                .with_metadata(HashMap::from([(
                    COLUMN_ID_META_KEY.to_string(),
                    "2147483544".to_string(),
                )]))
                .into();
            fields.push(field);
            Some(projector)
        } else {
            None
        };
        let schema = ArrowSchema::new(fields).into();
        Ok(PositionDeleteWriter {
            inner_writer: self.inner.build(&schema).await?,
            row_projector,
            schema,
        })
    }
}

pub struct PositionDeleteWriter<F: IcebergWriter> {
    inner_writer: F,
    row_projector: Option<FieldProjector>,
    schema: ArrowSchemaRef,
}

#[async_trait::async_trait]
impl<F: IcebergWriter> IcebergWriter for PositionDeleteWriter<F> {
    type R = F::R;
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(projector) = &self.row_projector {
            let row_column = projector.project(
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            "Last column should be row for this position delete writer.",
                        )
                    })?
                    .columns(),
            );
            let row_fields = if let DataType::Struct(fields) = self.schema.field(2).data_type() {
                fields
            } else {
                unreachable!()
            };
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    batch.column(0).clone(),
                    batch.column(1).clone(),
                    Arc::new(StructArray::new(row_fields.clone(), row_column, None)),
                ],
            )
            .map_err(|err| Error::new(ErrorKind::IcebergDataInvalid, format!("{err}")))?;
            self.inner_writer.write(batch).await?;
        } else {
            self.inner_writer.write(batch).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<F::R> {
        let mut res = self.inner_writer.close().await?;
        res.with_content(crate::types::DataContentType::PositionDeletes);
        Ok(res)
    }
}

#[derive(Clone)]
pub struct PositionDeleteInput {
    pub path: String,
    pub offset: i64,
}

impl Cacheable for PositionDeleteInput {
    fn combine(vec: Vec<Self>) -> Result<RecordBatch> {
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
        RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Fail concat cached batch: {}", err),
            )
        })
    }

    fn size(&self) -> usize {
        1
    }
}
