use serde::{Deserialize, Serialize};

use crate::types;
use crate::Error;
use crate::Result;

/// Parse schema from json bytes.
pub fn parse_sort_order(bs: &[u8]) -> Result<types::SortOrder> {
    let t: SortOrder = serde_json::from_slice(bs)?;
    t.try_into()
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    order_id: i32,
    fields: Vec<SortField>,
}

impl TryFrom<SortOrder> for types::SortOrder {
    type Error = Error;

    fn try_from(v: SortOrder) -> Result<Self> {
        let mut fields = Vec::with_capacity(v.fields.len());
        for field in v.fields {
            fields.push(field.try_into()?);
        }

        Ok(types::SortOrder {
            order_id: v.order_id,
            fields,
        })
    }
}

impl TryFrom<types::SortOrder> for SortOrder {
    type Error = Error;

    fn try_from(value: types::SortOrder) -> Result<Self> {
        Ok(Self {
            order_id: value.order_id,
            fields: value
                .fields
                .into_iter()
                .map(SortField::try_from)
                .collect::<Result<Vec<SortField>>>()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SortField {
    transform: String,
    source_id: i32,
    direction: String,
    null_order: String,
}

impl TryFrom<SortField> for types::SortField {
    type Error = Error;

    fn try_from(v: SortField) -> Result<Self> {
        Ok(types::SortField {
            source_column_id: v.source_id,
            transform: v.transform.as_str().parse()?,
            direction: v.direction.parse()?,
            null_order: v.null_order.parse()?,
        })
    }
}

impl TryFrom<types::SortField> for SortField {
    type Error = Error;

    fn try_from(value: types::SortField) -> Result<Self> {
        Ok(Self {
            transform: (&value.transform).to_string(),
            source_id: value.source_column_id,
            direction: value.direction.to_string(),
            null_order: value.null_order.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sort_order() {
        let content = r#"
{
    "order-id": 1,
    "fields": [ {
        "transform": "identity",
        "source-id": 2,
        "direction": "asc",
        "null-order": "nulls-first"
    }, {
        "transform": "bucket[4]",
        "source-id": 3,
        "direction": "desc",
        "null-order": "nulls-last"
    } ]
}
        "#;

        let v = parse_sort_order(content.as_bytes()).unwrap();

        assert_eq!(v.order_id, 1);
        assert_eq!(v.fields.len(), 2);
        assert_eq!(
            v.fields[0],
            types::SortField {
                source_column_id: 2,
                transform: types::Transform::Identity,
                direction: types::SortDirection::ASC,
                null_order: types::NullOrder::First
            }
        );
        assert_eq!(
            v.fields[1],
            types::SortField {
                source_column_id: 3,
                transform: types::Transform::Bucket(4),
                direction: types::SortDirection::DESC,
                null_order: types::NullOrder::Last
            }
        );
    }
}
