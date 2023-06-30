use serde::Deserialize;

use super::transform::parse_transform;
use crate::types;
use crate::Error;
use crate::Result;

/// Parse schema from json bytes.
pub fn parse_partition_spec(bs: &[u8]) -> Result<types::PartitionSpec> {
    let t: PartitionSpec = serde_json::from_slice(bs)?;
    t.try_into()
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionSpec {
    spec_id: i32,
    fields: Vec<PartitionField>,
}

impl TryFrom<PartitionSpec> for types::PartitionSpec {
    type Error = Error;

    fn try_from(v: PartitionSpec) -> Result<Self> {
        let mut fields = Vec::with_capacity(v.fields.len());
        for field in v.fields {
            fields.push(field.try_into()?);
        }

        Ok(types::PartitionSpec {
            spec_id: v.spec_id,
            fields,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PartitionField {
    source_id: i32,
    field_id: i32,
    name: String,
    transform: String,
}

impl TryFrom<PartitionField> for types::PartitionField {
    type Error = Error;

    fn try_from(v: PartitionField) -> Result<Self> {
        let transform = parse_transform(&v.transform)?;

        Ok(types::PartitionField {
            source_column_id: v.source_id,
            partition_field_id: v.field_id,
            transform,
            name: v.name,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_spec() {
        let content = r#"
{
    "spec-id": 1,
    "fields": [ {
        "source-id": 4,
        "field-id": 1000,
        "name": "ts_day",
        "transform": "day"
    }, {
        "source-id": 1,
        "field-id": 1001,
        "name": "id_bucket",
        "transform": "bucket[16]"
    } ]
}
        "#;

        let v = parse_partition_spec(content.as_bytes()).unwrap();

        assert_eq!(v.spec_id, 1);
        assert_eq!(v.fields.len(), 2);
        assert_eq!(
            v.fields[0],
            types::PartitionField {
                source_column_id: 4,
                partition_field_id: 1000,
                transform: types::Transform::Day,
                name: "ts_day".to_string(),
            }
        );
        assert_eq!(
            v.fields[1],
            types::PartitionField {
                source_column_id: 1,
                partition_field_id: 1001,
                transform: types::Transform::Bucket(16),
                name: "id_bucket".to_string(),
            }
        )
    }
}
