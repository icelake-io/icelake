use apache_avro::from_value;
use apache_avro::Reader;
use serde::Deserialize;

use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;
use crate::types::ManifestList;

/// Parse manifest list from json bytes.
///
/// QUESTION: Will we have more than one manifest list in a single file?
pub fn parse_manifest_list(bs: &[u8]) -> Result<ManifestList> {
    // Parse manifest entries
    let entries = Reader::new(bs)?
        .map(|v| v.and_then(|value| from_value::<ManifestListEntry>(&value)).map_err(|e| Error::new(ErrorKind::IcebergDataInvalid, "Failed to parse manifest list entry").set_source(e)))
        .map(|v| v.and_then(types::ManifestListEntry::try_from))
        .collect::<Result<Vec<_>>>()?;

    Ok(ManifestList { entries })
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct ManifestListEntry {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    #[serde(default)]
    content: i32,
    #[serde(default)]
    sequence_number: i64,
    #[serde(default)]
    min_sequence_number: i64,
    #[serde(default)]
    added_snapshot_id: i64,
    #[serde(default)]
    added_files_count: i32,
    #[serde(default)]
    existing_files_count: i32,
    #[serde(default)]
    deleted_files_count: i32,
    #[serde(default)]
    added_rows_count: i64,
    #[serde(default)]
    existing_rows_count: i64,
    #[serde(default)]
    deleted_rows_count: i64,
    partitions: Option<Vec<FieldSummary>>,
    key_metadata: Option<Vec<u8>>,
}

impl TryFrom<ManifestListEntry> for types::ManifestListEntry {
    type Error = Error;

    fn try_from(v: ManifestListEntry) -> Result<Self> {
        let content = match v.content {
            0 => types::ManifestContentType::Data,
            1 => types::ManifestContentType::Deletes,
            _ => {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("content type {} is invalid", v.content),
                ));
            }
        };

        let partitions = match v.partitions {
            Some(v) => {
                let mut partitions = Vec::with_capacity(v.len());
                for partition in v {
                    partitions.push(partition.try_into()?);
                }
                Some(partitions)
            }
            None => None,
        };

        Ok(types::ManifestListEntry {
            manifest_path: v.manifest_path,
            manifest_length: v.manifest_length,
            partition_spec_id: v.partition_spec_id,
            content,
            sequence_number: v.sequence_number,
            min_sequence_number: v.min_sequence_number,
            added_snapshot_id: v.added_snapshot_id,
            added_files_count: v.added_files_count,
            existing_files_count: v.existing_files_count,
            deleted_files_count: v.deleted_files_count,
            added_rows_count: v.added_rows_count,
            existing_rows_count: v.existing_rows_count,
            deleted_rows_count: v.deleted_rows_count,
            partitions,
            key_metadata: v.key_metadata,
        })
    }
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct FieldSummary {
    /// field: 509
    ///
    /// Whether the manifest contains at least one partition with a null
    /// value for the field
    contains_null: bool,
    /// field: 518
    /// Whether the manifest contains at least one partition with a NaN
    /// value for the field
    contains_nan: Option<bool>,
}

impl TryFrom<FieldSummary> for types::FieldSummary {
    type Error = Error;

    fn try_from(v: FieldSummary) -> Result<Self> {
        Ok(types::FieldSummary {
            contains_null: v.contains_null,
            contains_nan: v.contains_nan,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;

    use anyhow::Result;

    use super::*;

    #[test]
    fn test_load_manifest_file() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let reader = Reader::new(&bs[..]).unwrap();

        let mut files = Vec::new();

        for value in reader {
            files.push(from_value::<ManifestListEntry>(&value?)?);
        }

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0],
            ManifestListEntry {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: 0,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: 0,
                existing_files_count: 0,
                deleted_files_count: 0,
                added_rows_count: 3,
                existing_rows_count: 0,
                deleted_rows_count: 0,
                partitions: Some(vec![]),
                key_metadata: None,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_manifest_list() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let manifest_list = parse_manifest_list(&bs)?;

        assert_eq!(1, manifest_list.entries.len());

        assert_eq!(
            manifest_list.entries[0],
            types::ManifestListEntry {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: types::ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: 0,
                existing_files_count: 0,
                deleted_files_count: 0,
                added_rows_count: 3,
                existing_rows_count: 0,
                deleted_rows_count: 0,
                partitions: Some(vec![]),
                key_metadata: None,
            }
        );

        Ok(())
    }
}
