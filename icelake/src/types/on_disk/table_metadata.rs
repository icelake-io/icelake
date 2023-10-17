use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::partition_spec::PartitionSpec;
use super::schema::Schema;
use super::snapshot::Snapshot;
use super::sort_order::SortOrder;
use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Parse table metadata from json bytes.
pub fn parse_table_metadata(bs: &[u8]) -> Result<types::TableMetadata> {
    let v: TableMetadata = serde_json::from_slice(bs)?;
    v.try_into()
}

/// Serialize table meta to json format.
pub fn serialize_table_meta(table_meta: types::TableMetadata) -> Result<String> {
    let v = TableMetadata::try_from(table_meta)?;
    Ok(serde_json::to_string(&v)?)
}

/// Model used in rest catalog

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct TableMetadata {
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
    type Error = Error;

    fn try_from(v: TableMetadata) -> Result<Self> {
        let format_version = match v.format_version {
            1 => types::TableFormatVersion::V1,
            2 => types::TableFormatVersion::V2,
            _ => {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("invalid table format version {}", v.format_version),
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
            Some(rs) => {
                let mut refs = HashMap::with_capacity(rs.len());
                for (k, v) in rs {
                    refs.insert(k, v.try_into()?);
                }
                refs
            }
            None => HashMap::default(),
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

impl TryFrom<types::TableMetadata> for TableMetadata {
    type Error = Error;

    fn try_from(value: types::TableMetadata) -> Result<Self> {
        Ok(Self {
            format_version: value.format_version as i32,
            table_uuid: value.table_uuid,
            location: value.location,
            last_sequence_number: value.last_sequence_number,
            last_updated_ms: value.last_updated_ms,
            last_column_id: value.last_column_id,
            schemas: value
                .schemas
                .iter()
                .map(Schema::try_from)
                .collect::<Result<Vec<Schema>>>()?,
            current_schema_id: value.current_schema_id,
            partition_specs: value
                .partition_specs
                .iter()
                .map(PartitionSpec::try_from)
                .collect::<Result<Vec<PartitionSpec>>>()?,
            default_spec_id: value.default_spec_id,
            last_partition_id: value.last_partition_id,
            properties: value.properties,
            current_snapshot_id: value.current_snapshot_id,
            snapshots: value
                .snapshots
                .map(|ss| {
                    ss.into_iter()
                        .map(Snapshot::try_from)
                        .collect::<Result<Vec<Snapshot>>>()
                })
                .transpose()?,
            snapshot_log: value
                .snapshot_log
                .map(|ss| {
                    ss.into_iter()
                        .map(SnapshotLog::try_from)
                        .collect::<Result<Vec<SnapshotLog>>>()
                })
                .transpose()?,
            metadata_log: value
                .metadata_log
                .map(|ss| {
                    ss.into_iter()
                        .map(MetadataLog::try_from)
                        .collect::<Result<Vec<MetadataLog>>>()
                })
                .transpose()?,
            sort_orders: value
                .sort_orders
                .into_iter()
                .map(SortOrder::try_from)
                .collect::<Result<Vec<SortOrder>>>()?,
            default_sort_order_id: value.default_sort_order_id,
            refs: Some(
                value
                    .refs
                    .into_iter()
                    .map(|e| SnapshotReference::try_from(e.1).map(|s| (e.0, s)))
                    .collect::<Result<HashMap<String, SnapshotReference>>>()?,
            ),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SnapshotLog {
    timestamp_ms: i64,
    snapshot_id: i64,
}

impl TryFrom<SnapshotLog> for types::SnapshotLog {
    type Error = Error;

    fn try_from(v: SnapshotLog) -> Result<Self> {
        Ok(types::SnapshotLog {
            timestamp_ms: v.timestamp_ms,
            snapshot_id: v.snapshot_id,
        })
    }
}

impl TryFrom<types::SnapshotLog> for SnapshotLog {
    type Error = Error;

    fn try_from(value: types::SnapshotLog) -> Result<Self> {
        Ok(Self {
            timestamp_ms: value.timestamp_ms,
            snapshot_id: value.snapshot_id,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct MetadataLog {
    timestamp_ms: i64,
    metadata_file: String,
}

impl TryFrom<MetadataLog> for types::MetadataLog {
    type Error = Error;

    fn try_from(v: MetadataLog) -> Result<Self> {
        Ok(types::MetadataLog {
            timestamp_ms: v.timestamp_ms,
            metadata_file: v.metadata_file,
        })
    }
}

impl TryFrom<types::MetadataLog> for MetadataLog {
    type Error = Error;

    fn try_from(value: types::MetadataLog) -> Result<Self> {
        Ok(Self {
            timestamp_ms: value.timestamp_ms,
            metadata_file: value.metadata_file,
        })
    }
}

#[derive(Serialize, Deserialize)]
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
    type Error = Error;

    fn try_from(v: SnapshotReference) -> Result<Self> {
        Ok(types::SnapshotReference {
            snapshot_id: v.snapshot_id,
            typ: v.typ.as_str().parse()?,
            min_snapshots_to_keep: v.min_snapshots_to_keep,
            max_snapshot_age_ms: v.max_snapshot_age_ms,
            max_ref_age_ms: v.max_ref_age_ms,
        })
    }
}

impl TryFrom<types::SnapshotReference> for SnapshotReference {
    type Error = Error;

    fn try_from(value: types::SnapshotReference) -> Result<Self> {
        Ok(Self {
            snapshot_id: value.snapshot_id,
            typ: value.typ.to_string(),
            min_snapshots_to_keep: value.min_snapshots_to_keep,
            max_snapshot_age_ms: value.max_snapshot_age_ms,
            max_ref_age_ms: value.max_ref_age_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::types::TableFormatVersion;
    use std::env;
    use std::fs;

    use super::*;

    #[test]
    fn test_parse_table_metadata_v1() {
        let path = format!(
            "{}/../testdata/simple_table/metadata/v1.metadata.json",
            env!("CARGO_MANIFEST_DIR")
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
            "{}/../testdata/simple_table/metadata/v2.metadata.json",
            env!("CARGO_MANIFEST_DIR")
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

    #[test]
    fn test_serialize_table_metadata() {
        let metadata = types::TableMetadata {
            format_version: TableFormatVersion::V2,
            table_uuid: "1932a94b-d2bf-43ca-a66f-3158a09baf1f".to_string(),
            location: "/opt/bitnami/spark/warehouse/db/table".to_string(),
            last_sequence_number: 100,
            last_updated_ms: 1686911671713,
            last_column_id: 100,
            schemas: vec![types::Schema::new(
                0,
                None,
                types::Struct::new(vec![types::Field {
                    id: 1,
                    name: "VendorID".to_string(),
                    required: false,
                    field_type: types::Any::Primitive(types::Primitive::Long),
                    comment: None,
                    initial_default: None,
                    write_default: None,
                }
                .into()]),
            )],
            current_schema_id: 0,
            partition_specs: vec![types::PartitionSpec {
                spec_id: 1,
                fields: vec![types::PartitionField {
                    source_column_id: 1,
                    partition_field_id: 1000,
                    transform: types::Transform::Day,
                    name: "ts_day".to_string(),
                }],
            }],
            default_spec_id: 1,
            last_partition_id: 1,
            properties: None,
            current_snapshot_id: Some(1),
            snapshots: Some(vec![types::Snapshot {
                snapshot_id: 1,
                parent_snapshot_id: None,
                sequence_number: 2,
                timestamp_ms: 1686911671713,
                manifest_list: "/opt/bitnami/spark/warehouse/db/table/1.avro".to_string(),
                summary: HashMap::default(),
                schema_id: Some(0),
            }]),
            snapshot_log: Some(vec![types::SnapshotLog {
                timestamp_ms: 1686911671713,
                snapshot_id: 1,
            }]),
            metadata_log: Some(vec![types::MetadataLog {
                timestamp_ms: 1686911671713,
                metadata_file: "/testdata/simple_table/metadata/v2.metadata.json".to_string(),
            }]),
            sort_orders: vec![],
            default_sort_order_id: 1,
            refs: HashMap::default(),
        };

        let json = serialize_table_meta(metadata.clone()).unwrap();

        let parsed_table_meta = parse_table_metadata(json.as_bytes()).unwrap();
        assert_eq!(metadata, parsed_table_meta);
    }
}
