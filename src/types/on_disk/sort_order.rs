use serde::Deserialize;

use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

use super::transform::parse_transform;

/// Parse schema from json bytes.
pub fn parse_sort_order(bs: &[u8]) -> Result<types::SortOrder> {
    let t: SortOrder = serde_json::from_slice(bs)?;
    t.try_into()
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
            transform: parse_transform(&v.transform)?,
            direction: parse_sort_direction(&v.direction)?,
            null_order: parse_null_order(&v.null_order)?,
        })
    }
}

/// Parse transform string represent into types::Transform enum.
fn parse_sort_direction(s: &str) -> Result<types::SortDirection> {
    let t = match s {
        "asc" => types::SortDirection::ASC,
        "desc" => types::SortDirection::DESC,
        v => {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("sort direction {:?} is invalid", v),
            ))
        }
    };

    Ok(t)
}

/// Parse transform string represent into types::Transform enum.
fn parse_null_order(s: &str) -> Result<types::NullOrder> {
    let t = match s {
        "nulls-first" => types::NullOrder::First,
        "nulls-last" => types::NullOrder::Last,
        v => {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("null order {:?} is invalid", v),
            ))
        }
    };

    Ok(t)
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
