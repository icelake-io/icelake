use apache_avro::from_value;
use apache_avro::to_value;
use apache_avro::Reader;
use apache_avro::Schema as AvroSchema;
use apache_avro::Writer as AvroWriter;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::types;
use crate::types::to_avro::to_avro_schema;
use crate::types::ManifestList;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Parse manifest list from json bytes.
///
/// QUESTION: Will we have more than one manifest list in a single file?
pub fn parse_manifest_list(bs: &[u8]) -> Result<ManifestList> {
    // Parse manifest entries
    let entries = Reader::new(bs)?
        .map(|v| {
            v.and_then(|value| from_value::<ManifestListEntry>(&value))
                .map_err(|e| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "Failed to parse manifest list entry",
                    )
                    .set_source(e)
                })
        })
        .map(|v| v.and_then(types::ManifestListEntry::try_from))
        .collect::<Result<Vec<_>>>()?;

    Ok(ManifestList { entries })
}

#[derive(Serialize, Deserialize)]
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
        let content = (v.content as u8).try_into()?;

        let partitions = if let Some(partitions) = v.partitions {
            Some(
                partitions
                    .into_iter()
                    .map(types::FieldSummary::try_from)
                    .collect::<Result<Vec<types::FieldSummary>>>()?,
            )
        } else {
            None
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

impl From<types::ManifestListEntry> for ManifestListEntry {
    fn from(value: types::ManifestListEntry) -> Self {
        let content: i32 = value.content as i32;

        let partitions = value.partitions.map(|partitions| {
            partitions
                .into_iter()
                .map(FieldSummary::from)
                .collect::<Vec<FieldSummary>>()
        });

        Self {
            manifest_path: value.manifest_path,
            manifest_length: value.manifest_length,
            partition_spec_id: value.partition_spec_id,
            content,
            sequence_number: value.sequence_number,
            min_sequence_number: value.min_sequence_number,
            added_snapshot_id: value.added_snapshot_id,
            added_files_count: value.added_files_count,
            existing_files_count: value.existing_files_count,
            deleted_files_count: value.deleted_files_count,
            added_rows_count: value.added_rows_count,
            existing_rows_count: value.existing_rows_count,
            deleted_rows_count: value.deleted_rows_count,
            partitions,
            key_metadata: value.key_metadata,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
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

    #[serde(with = "serde_bytes")]
    lower_bound: Option<Vec<u8>>,
    #[serde(with = "serde_bytes")]
    upper_bound: Option<Vec<u8>>,
}

impl TryFrom<FieldSummary> for types::FieldSummary {
    type Error = Error;

    fn try_from(v: FieldSummary) -> Result<Self> {
        Ok(types::FieldSummary {
            contains_null: v.contains_null,
            contains_nan: v.contains_nan,
            lower_bound: v.lower_bound,
            upper_bound: v.upper_bound,
        })
    }
}

impl From<types::FieldSummary> for FieldSummary {
    fn from(v: types::FieldSummary) -> Self {
        Self {
            contains_null: v.contains_null,
            contains_nan: v.contains_nan,
            lower_bound: v.lower_bound,
            upper_bound: v.upper_bound,
        }
    }
}

pub(crate) struct ManifestListWriter {
    op: Operator,
    // Output path relative to operator root.
    output_path: String,
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
}

impl ManifestListWriter {
    pub(crate) fn new(
        op: Operator,
        output_path: String,
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: i64,
    ) -> Self {
        Self {
            op,
            output_path,
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
        }
    }

    /// Write manifest list to file. Return the absolute path of the file.
    pub(crate) async fn write(self, manifest_list: ManifestList) -> Result<()> {
        let avro_schema = to_avro_schema(&types::ManifestList::v2_schema(), Some("manifest_file"))?;
        let mut avro_writer = self.v2_writer(&avro_schema)?;

        for entry in manifest_list.entries {
            let entry = ManifestListEntry::from(entry);
            let value = to_value(entry)?.resolve(&avro_schema)?;
            avro_writer.append(value)?;
        }

        let connect = avro_writer.into_inner()?;
        self.op.write(self.output_path.as_str(), connect).await?;

        Ok(())
    }

    fn v2_writer<'a>(&self, avro_schema: &'a AvroSchema) -> Result<AvroWriter<'a, Vec<u8>>> {
        let mut writer = AvroWriter::new(avro_schema, Vec::new());
        writer.add_user_metadata("snapshot-id".to_string(), &self.snapshot_id.to_string())?;
        writer.add_user_metadata(
            "parent-snapshot-id".to_string(),
            &self
                .parent_snapshot_id
                .map(|id| id.to_string())
                .unwrap_or("null".to_string()),
        )?;
        writer.add_user_metadata(
            "sequence-number".to_string(),
            &self.sequence_number.to_string(),
        )?;
        writer.add_user_metadata("format-version".to_string(), "2")?;
        Ok(writer)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::fs::read;

    use anyhow::Result;
    use opendal::services::Fs;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_load_manifest_file() -> Result<()> {
        let path = format!(
            "{}/../testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env!("CARGO_MANIFEST_DIR")
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
                added_files_count: 3,
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
            "{}/../testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env!("CARGO_MANIFEST_DIR")
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
                added_files_count: 3,
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

    #[tokio::test]
    async fn test_write_manifest_list_v2() -> Result<()> {
        let path = format!(
            "{}/../testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env!("CARGO_MANIFEST_DIR")
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let manifest_list = parse_manifest_list(&bs)?;

        check_manifest_list_serde(manifest_list).await;

        Ok(())
    }

    async fn check_manifest_list_serde(manifest_file: types::ManifestList) {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_str().unwrap();
        let filename = "test.avro";

        let operator = {
            let mut builder = Fs::default();
            builder.root(dir_path);
            Operator::new(builder).unwrap().finish()
        };

        let writer = ManifestListWriter::new(operator, filename.to_string(), 0, Some(0), 0);
        writer.write(manifest_file.clone()).await.unwrap();

        let restored_manifest_file =
            { parse_manifest_list(&read(tmp_dir.path().join(filename)).unwrap()).unwrap() };

        assert_eq!(manifest_file, restored_manifest_file);
    }
}
