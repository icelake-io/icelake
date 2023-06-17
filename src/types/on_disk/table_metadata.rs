use std::collections::HashMap;

use serde::Deserialize;

use super::partition_spec::PartitionSpec;
use super::schema::Schema;
use super::snapshot::Snapshot;
use super::sort_order::SortOrder;
use crate::types;
use anyhow::anyhow;
use anyhow::Result;

/// Parse table metadata from json bytes.
pub fn parse_table_metadata(bs: &[u8]) -> Result<types::TableMetadata> {
    let v: TableMetadata = serde_json::from_slice(bs)?;
    v.try_into()
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TableMetadata {
    format_version: i32,
    table_uuid: String,
    location: String,
    #[serde(default)]
    last_sequence_number: i64,
    last_updated_ms: i64,
    last_column_id: i32,
    schemas: Vec<Schema>,
    current_schema_id: i32,
    partition_specs: Vec<PartitionSpec>,
    default_spec_id: i32,
    last_partition_id: i32,
    properties: Option<HashMap<String, String>>,
    current_snapshot_id: Option<i64>,
    snapshots: Option<Vec<Snapshot>>,
    snapshot_log: Option<Vec<SnapshotLog>>,
    metadata_log: Option<Vec<MetadataLog>>,
    sort_orders: Vec<SortOrder>,
    default_sort_order_id: i32,
    refs: Option<HashMap<String, SnapshotReference>>,
}

impl TryFrom<TableMetadata> for types::TableMetadata {
    type Error = anyhow::Error;

    fn try_from(v: TableMetadata) -> Result<Self, Self::Error> {
        let format_version = match v.format_version {
            1 => types::TableFormatVersion::V1,
            2 => types::TableFormatVersion::V2,
            _ => {
                return Err(anyhow!(
                    "invalid table format version: {}",
                    v.format_version
                ))
            }
        };

        let mut schemas = Vec::with_capacity(v.schemas.len());
        for schema in v.schemas {
            schemas.push(schema.try_into()?);
        }

        let mut partition_specs = Vec::with_capacity(v.partition_specs.len());
        for partition_spec in v.partition_specs {
            partition_specs.push(partition_spec.try_into()?);
        }

        let snapshots = match v.snapshots {
            Some(v) => {
                let mut snapshots = Vec::with_capacity(v.len());
                for snapshot in v {
                    snapshots.push(snapshot.try_into()?);
                }
                Some(snapshots)
            }
            None => None,
        };

        let snapshot_log = match v.snapshot_log {
            Some(v) => {
                let mut snapshot_log = Vec::with_capacity(v.len());
                for snapshot in v {
                    snapshot_log.push(snapshot.try_into()?);
                }
                Some(snapshot_log)
            }
            None => None,
        };

        let metadata_log = match v.metadata_log {
            Some(v) => {
                let mut metadata_log = Vec::with_capacity(v.len());
                for metadata in v {
                    metadata_log.push(metadata.try_into()?);
                }
                Some(metadata_log)
            }
            None => None,
        };

        let mut sort_orders = Vec::with_capacity(v.sort_orders.len());
        for sort_order in v.sort_orders {
            sort_orders.push(sort_order.try_into()?);
        }

        let refs = match v.refs {
            Some(v) => {
                let mut refs = HashMap::with_capacity(v.len());
                for (k, v) in v {
                    refs.insert(k, v.try_into()?);
                }
                Some(refs)
            }
            None => None,
        };

        Ok(types::TableMetadata {
            format_version,
            table_uuid: v.table_uuid,
            location: v.location,
            last_sequence_number: v.last_sequence_number,
            last_updated_ms: v.last_updated_ms,
            last_column_id: v.last_column_id,
            schemas,
            current_schema_id: v.current_schema_id,
            partition_specs,
            default_spec_id: v.default_spec_id,
            last_partition_id: v.last_partition_id,
            properties: v.properties,
            current_snapshot_id: v.current_snapshot_id,
            snapshots,
            snapshot_log,
            metadata_log,
            sort_orders,
            default_sort_order_id: v.default_sort_order_id,
            refs,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SnapshotLog {
    timestamp_ms: i64,
    snapshot_id: i64,
}

impl TryFrom<SnapshotLog> for types::SnapshotLog {
    type Error = anyhow::Error;

    fn try_from(v: SnapshotLog) -> Result<Self, Self::Error> {
        Ok(types::SnapshotLog {
            timestamp_ms: v.timestamp_ms,
            snapshot_id: v.snapshot_id,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MetadataLog {
    timestamp_ms: i64,
    metadata_file: String,
}

impl TryFrom<MetadataLog> for types::MetadataLog {
    type Error = anyhow::Error;

    fn try_from(v: MetadataLog) -> Result<Self, Self::Error> {
        Ok(types::MetadataLog {
            timestamp_ms: v.timestamp_ms,
            metadata_file: v.metadata_file,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SnapshotReference {
    snapshot_id: i64,
    #[serde(rename = "type")]
    typ: String,
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_ms: Option<i64>,
    max_ref_age_ms: Option<i64>,
}

impl TryFrom<SnapshotReference> for types::SnapshotReference {
    type Error = anyhow::Error;

    fn try_from(v: SnapshotReference) -> Result<Self, Self::Error> {
        let typ = match v.typ.as_str() {
            "tag" => types::SnapshotReferenceType::Tag,
            "branch" => types::SnapshotReferenceType::Branch,
            v => return Err(anyhow!("invalid snapshot reference type: {}", v)),
        };

        Ok(types::SnapshotReference {
            snapshot_id: v.snapshot_id,
            typ,
            min_snapshots_to_keep: v.min_snapshots_to_keep,
            max_snapshot_age_ms: v.max_snapshot_age_ms,
            max_ref_age_ms: v.max_ref_age_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use super::*;

    #[test]
    fn test_parse_table_metadata_v1() {
        let path = format!(
            "{}/testdata/simple_table/metadata/v1.metadata.json",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let metadata = parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed");

        assert_eq!(metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(metadata.table_uuid, "1932a94b-d2bf-43ca-a66f-3158a09baf1f");
        assert_eq!(metadata.location, "/opt/bitnami/spark/warehouse/db/table");
        assert_eq!(metadata.last_updated_ms, 1686911664577);
        assert_eq!(metadata.last_column_id, 2);
    }

    #[test]
    fn test_parse_table_metadata_v2() {
        let path = format!(
            "{}/testdata/simple_table/metadata/v2.metadata.json",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let metadata = parse_table_metadata(&bs).expect("parse_table_metadata v2 must succeed");

        assert_eq!(metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(metadata.table_uuid, "1932a94b-d2bf-43ca-a66f-3158a09baf1f");
        assert_eq!(metadata.location, "/opt/bitnami/spark/warehouse/db/table");
        assert_eq!(metadata.last_updated_ms, 1686911671713);
        assert_eq!(metadata.last_column_id, 2);
        assert_eq!(metadata.current_snapshot_id, Some(1646658105718557341));
    }
}
