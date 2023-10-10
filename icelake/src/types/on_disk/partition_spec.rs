use serde::{Deserialize, Serialize};

use crate::types;
use crate::Error;
use crate::Result;

/// Parse schema from json bytes.
pub fn parse_partition_spec(bs: &[u8]) -> Result<types::PartitionSpec> {
    let t: PartitionSpec = serde_json::from_slice(bs)?;
    t.try_into()
}

/// Serialize partition spec to json bytes.
pub fn serialize_partition_spec(spec: &types::PartitionSpec) -> Result<String> {
    let t = PartitionSpec::try_from(spec)?;
    Ok(serde_json::to_string(&t)?)
}

pub fn serialize_partition_spec_fields(spec: &types::PartitionSpec) -> Result<String> {
    let t = PartitionSpec::try_from(spec)?;
    Ok(serde_json::to_string(&t.fields)?)
}

pub fn parse_partition_spec_fields(bs: &[u8]) -> Result<Vec<types::PartitionField>> {
    let t: Vec<PartitionField> = serde_json::from_slice(bs)?;
    t.into_iter()
        .map(types::PartitionField::try_from)
        .collect::<Result<Vec<types::PartitionField>>>()
}

#[derive(Serialize, Deserialize)]
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

impl<'a> TryFrom<&'a types::PartitionSpec> for PartitionSpec {
    type Error = Error;
    fn try_from(v: &'a types::PartitionSpec) -> Result<Self> {
        Ok(Self {
            spec_id: v.spec_id,
            fields: v
                .fields
                .iter()
                .map(PartitionField::try_from)
                .collect::<Result<Vec<PartitionField>>>()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
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
        let transform = v.transform.as_str().parse()?;

        Ok(types::PartitionField {
            source_column_id: v.source_id,
            partition_field_id: v.field_id,
            transform,
            name: v.name,
        })
    }
}

impl<'a> TryFrom<&'a types::PartitionField> for PartitionField {
    type Error = Error;

    fn try_from(v: &'a types::PartitionField) -> Result<Self> {
        Ok(Self {
            source_id: v.source_column_id,
            field_id: v.partition_field_id,
            name: v.name.clone(),
            transform: (&v.transform).to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_partition_spec_conversion(json: &str, expected_partition_spec: types::PartitionSpec) {
        let parsed = parse_partition_spec(json.as_bytes()).unwrap();
        assert_eq!(expected_partition_spec, parsed);

        let serialized_json = serialize_partition_spec(&expected_partition_spec).unwrap();
        let parsed = parse_partition_spec(serialized_json.as_bytes()).unwrap();
        assert_eq!(expected_partition_spec, parsed);

        let serialized_fields_json =
            serialize_partition_spec_fields(&expected_partition_spec).unwrap();
        let parse_fields: Vec<PartitionField> =
            serde_json::from_slice(serialized_fields_json.as_bytes()).unwrap();
        let parse_type_fields = parse_fields
            .into_iter()
            .map(types::PartitionField::try_from)
            .collect::<Result<Vec<types::PartitionField>>>()
            .unwrap();

        assert_eq!(expected_partition_spec.fields, parse_type_fields);
    }

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

        check_partition_spec_conversion(
            content,
            types::PartitionSpec {
                spec_id: 1,
                fields: vec![
                    types::PartitionField {
                        source_column_id: 4,
                        partition_field_id: 1000,
                        transform: types::Transform::Day,
                        name: "ts_day".to_string(),
                    },
                    types::PartitionField {
                        source_column_id: 1,
                        partition_field_id: 1001,
                        transform: types::Transform::Bucket(16),
                        name: "id_bucket".to_string(),
                    },
                ],
            },
        );
    }
}
