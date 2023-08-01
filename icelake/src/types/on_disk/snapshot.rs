use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types;
use crate::Error;
use crate::Result;

/// Parse snapshot from json bytes.
pub fn parse_snapshot(bs: &[u8]) -> Result<types::Snapshot> {
    let v: Snapshot = serde_json::from_slice(bs)?;
    v.try_into()
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_snapshot_id: Option<i64>,
    #[serde(default)]
    sequence_number: i64,
    timestamp_ms: i64,
    manifest_list: String,
    summary: HashMap<String, String>,
    schema_id: Option<i64>,
}

impl TryFrom<Snapshot> for types::Snapshot {
    type Error = Error;

    fn try_from(v: Snapshot) -> Result<Self> {
        Ok(types::Snapshot {
            snapshot_id: v.snapshot_id,
            parent_snapshot_id: v.parent_snapshot_id,
            sequence_number: v.sequence_number,
            timestamp_ms: v.timestamp_ms,
            manifest_list: v.manifest_list,
            summary: v.summary,
            schema_id: v.schema_id,
        })
    }
}

impl TryFrom<types::Snapshot> for Snapshot {
    type Error = Error;

    fn try_from(value: types::Snapshot) -> Result<Self> {
        Ok(Self {
            snapshot_id: value.snapshot_id,
            parent_snapshot_id: value.parent_snapshot_id,
            sequence_number: value.sequence_number,
            timestamp_ms: value.timestamp_ms,
            manifest_list: value.manifest_list,
            summary: value.summary,
            schema_id: value.schema_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot() {
        let content = r#"
{
    "snapshot-id" : 1646658105718557341,
    "timestamp-ms" : 1686911671713,
    "summary" : {
      "operation" : "append",
      "spark.app.id" : "local-1686911651377",
      "added-data-files" : "3",
      "added-records" : "3",
      "added-files-size" : "1929",
      "changed-partition-count" : "1",
      "total-records" : "3",
      "total-files-size" : "1929",
      "total-data-files" : "3",
      "total-delete-files" : "0",
      "total-position-deletes" : "0",
      "total-equality-deletes" : "0"
    },
    "manifest-list" : "/opt/bitnami/spark/warehouse/db/table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
    "schema-id" : 0
  }
        "#;

        let v = parse_snapshot(content.as_bytes()).unwrap();

        assert_eq!(
            v,
            types::Snapshot {
                snapshot_id: 1646658105718557341,
                parent_snapshot_id: None,
                sequence_number: 0,
                timestamp_ms: 1686911671713,
                manifest_list: "/opt/bitnami/spark/warehouse/db/table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro".to_string(),
                summary: {
                    let mut m = HashMap::new();
                    m.insert("operation", "append");
                    m.insert("spark.app.id", "local-1686911651377");
                    m.insert("added-data-files", "3");
                    m.insert("added-records", "3");
                    m.insert("added-files-size", "1929");
                    m.insert("changed-partition-count", "1");
                    m.insert("total-records", "3");
                    m.insert("total-files-size", "1929");
                    m.insert("total-data-files", "3");
                    m.insert("total-delete-files", "0");
                    m.insert("total-position-deletes", "0");
                    m.insert("total-equality-deletes", "0");
                    m.into_iter().map(|(k,v)|(k.to_string(), v.to_string())).collect()
                },
                schema_id: Some(0)
            }
        )
    }
}
