use std::{collections::HashMap, sync::Arc};

use super::{
    create_transform_function, Any, AnyValue, BoxedTransformFunction, PartitionSpec, StructValue,
    COLUMN_ID_META_KEY,
};
use crate::{types::struct_to_anyvalue_array_with_type, Error, ErrorKind, Result};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, FieldRef, Fields};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;

/// Help to project specific field from `RecordBatch`` according to the column id.
pub struct FieldProjector {
    index_vec_vec: Vec<Vec<usize>>,
}

impl FieldProjector {
    pub fn new(batch_fields: &Fields, column_ids: &[usize]) -> Result<(Self, Fields)> {
        let mut index_vec_vec = Vec::with_capacity(column_ids.len());
        let mut fields = Vec::with_capacity(column_ids.len());
        for &id in column_ids {
            let mut index_vec = vec![];
            if let Some(field) = Self::fetch_column_index(batch_fields, &mut index_vec, id as i64) {
                fields.push(field.clone());
                index_vec_vec.push(index_vec);
            } else {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Can't find source column id: {}", id),
                ));
            }
        }
        Ok((Self { index_vec_vec }, Fields::from_iter(fields)))
    }

    fn fetch_column_index(
        fields: &Fields,
        index_vec: &mut Vec<usize>,
        col_id: i64,
    ) -> Option<FieldRef> {
        for (pos, field) in fields.iter().enumerate() {
            let id: i64 = field
                .metadata()
                .get(COLUMN_ID_META_KEY)
                .expect("column_id must be set")
                .parse()
                .expect("column_id must can be parse as i64");
            if col_id == id {
                index_vec.push(pos);
                return Some(field.clone());
            }
            if let DataType::Struct(inner) = field.data_type() {
                let res = Self::fetch_column_index(inner, index_vec, col_id);
                if !index_vec.is_empty() {
                    index_vec.push(pos);
                    return res;
                }
            }
        }
        None
    }

    pub fn project(&self, batch: &[ArrayRef]) -> Vec<ArrayRef> {
        self.index_vec_vec
            .iter()
            .map(|index_vec| Self::get_column_by_index_vec(batch, index_vec))
            .collect_vec()
    }

    fn get_column_by_index_vec(batch: &[ArrayRef], index_vec: &[usize]) -> ArrayRef {
        let mut rev_iterator = index_vec.iter().rev();
        let mut array = batch[*rev_iterator.next().unwrap()].clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(*idx)
                .clone();
        }
        array
    }
}

/// `PartitionSplitter` is used to separate a given `RecordBatch`` according partition spec.
pub struct PartitionSplitter {
    col_extractor: FieldProjector,
    transforms: Vec<BoxedTransformFunction>,
    row_converter: RowConverter,
    arrow_partition_type_fields: Fields,
    partition_type: Any,
}

#[derive(Hash, PartialEq, PartialOrd, Eq, Ord, Clone)]
/// `PartitionKey` is the wrapper of OwnedRow to avoid user depend OwnedRow directly.
pub struct PartitionKey {
    inner: OwnedRow,
}

impl From<OwnedRow> for PartitionKey {
    fn from(value: OwnedRow) -> Self {
        Self { inner: value }
    }
}

impl PartitionSplitter {
    /// Create a new `PartitionSplitter`.
    pub fn try_new(
        col_extractor: FieldProjector,
        partition_spec: &PartitionSpec,
        partition_type: Any,
    ) -> Result<Self> {
        let transforms = partition_spec
            .fields
            .iter()
            .map(|field| create_transform_function(&field.transform))
            .try_collect()?;

        let arrow_partition_type_fields =
            if let DataType::Struct(fields) = partition_type.clone().try_into()? {
                fields
            } else {
                unreachable!()
            };
        let row_converter = RowConverter::new(vec![SortField::new(DataType::Struct(
            arrow_partition_type_fields.clone(),
        ))])
        .map_err(|e| crate::error::Error::new(crate::ErrorKind::ArrowError, format!("{}", e)))?;

        Ok(Self {
            col_extractor,
            transforms,
            arrow_partition_type_fields,
            row_converter,
            partition_type,
        })
    }

    /// This function do two things:
    /// 1. Separate the batch by partition spec.
    /// 2. Compute the partition value.
    pub fn split_by_partition(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<HashMap<PartitionKey, RecordBatch>> {
        let arrays = self.col_extractor.project(batch.columns());
        let value_array = Arc::new(StructArray::new(
            self.arrow_partition_type_fields.clone(),
            arrays
                .iter()
                .zip(self.transforms.iter())
                .zip(self.arrow_partition_type_fields.iter())
                .map(|((array, transform), field)| {
                    let mut array = transform.transform(array.clone())?;
                    if array.data_type() != field.data_type() {
                        if let DataType::Timestamp(unit, _) = array.data_type() {
                            if let DataType::Timestamp(field_unit, _) = field.data_type() {
                                if unit == field_unit {
                                    array = cast(&array, field.data_type()).map_err(|e| {
                                        crate::error::Error::new(
                                            crate::ErrorKind::ArrowError,
                                            format!("{e}"),
                                        )
                                    })?
                                }
                            }
                        }
                    }
                    Ok(array)
                })
                .collect::<Result<Vec<_>>>()?,
            None,
        ));

        let rows = self
            .row_converter
            .convert_columns(&[value_array])
            .map_err(|e| {
                crate::error::Error::new(crate::ErrorKind::ArrowError, format!("{}", e))
            })?;

        // Group the batch by row value.
        let mut group_ids = HashMap::new();
        rows.into_iter().enumerate().for_each(|(row_id, row)| {
            group_ids.entry(row.owned()).or_insert(vec![]).push(row_id);
        });

        // Partition the batch with same partition partition_values
        let mut partition_batches = HashMap::new();
        for (row, row_ids) in group_ids.into_iter() {
            // generate the bool filter array from column_ids
            let filter_array: BooleanArray = {
                let mut filter = vec![false; batch.num_rows()];
                row_ids.into_iter().for_each(|row_id| {
                    filter[row_id] = true;
                });
                filter.into()
            };

            // filter the RecordBatch
            partition_batches.insert(
                row.clone().into(),
                filter_record_batch(batch, &filter_array)
                    .expect("We should guarantee the filter array is valid"),
            );
        }

        Ok(partition_batches)
    }

    /// Convert the `PartitionKey` to `PartitionValue`
    ///
    /// The reason we separate them is to save memory cost, when in write process, we only need to
    /// keep the `PartitionKey`. It's effiect to used in Hash. When write complete, we can use it to convert `PartitionKey` to
    /// `PartitionValue` to store it in `DataFile`.
    pub fn convert_key_to_value(&self, key: PartitionKey) -> Result<StructValue> {
        let array = {
            let mut arrays = self
                .row_converter
                .convert_rows([key.inner.row()].into_iter())
                .map_err(|e| {
                    crate::error::Error::new(crate::ErrorKind::ArrowError, format!("{e}"))
                })?;
            assert!(arrays.len() == 1);
            arrays.pop().unwrap()
        };
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        let mut value_array =
            struct_to_anyvalue_array_with_type(struct_array, self.partition_type.clone())?;

        assert!(value_array.len() == 1);
        let value = value_array.pop().unwrap().unwrap();
        if let AnyValue::Struct(value) = value {
            Ok(value)
        } else {
            unreachable!()
        }
    }
}
