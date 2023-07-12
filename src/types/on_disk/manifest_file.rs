use std::cmp::min;
use std::collections::HashMap;
use std::str::FromStr;

use apache_avro::Reader;
use apache_avro::{from_value, Schema as AvroSchema};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::Bytes;

use super::parse_schema;
use crate::types;
use crate::types::on_disk::partition_spec::PartitionSpec;
use crate::types::on_disk::schema::Schema;
use crate::types::{
    DataContentType, ManifestContentType, ManifestFile, ManifestListEntry, UNASSIGNED_SEQ_NUM,
};
use crate::types::{ManifestStatus, TableFormatVersion};
use crate::Error;
use crate::ErrorKind;
use crate::Result;
use apache_avro::Writer as AvroWriter;

/// Parse manifest file from avro bytes.
pub fn parse_manifest_file(bs: &[u8]) -> Result<types::ManifestFile> {
    let reader = Reader::new(bs)?;

    // Parse manifest metadata
    let meta = reader.user_metadata();
    let metadata = types::ManifestMetadata {
        schema: parse_schema(meta.get("schema").ok_or_else(|| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                "schema is required in manifest metadata but not found",
            )
        })?)?,
        schema_id: {
            match meta.get("schema-id") {
                None => 0,
                Some(v) => {
                    let v = String::from_utf8_lossy(v);
                    v.parse().map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("schema-id {:?} is invalid", v),
                        )
                        .set_source(err)
                    })?
                }
            }
        },
        partition_spec_id: {
            match meta.get("partition-spec-id") {
                None => 0,
                Some(v) => {
                    let v = String::from_utf8_lossy(v);
                    v.parse().map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("partition-spec-id {:?} is invalid", v),
                        )
                        .set_source(err)
                    })?
                }
            }
        },
        format_version: {
            meta.get("format-version")
                .map(|v| {
                    let v = String::from_utf8_lossy(v);
                    v.parse::<u8>()
                        .map_err(|err| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                format!("format-version {:?} is invalid", v),
                            )
                            .set_source(err)
                        })
                        .and_then(TableFormatVersion::try_from)
                })
                .transpose()?
        },
        content: {
            let c = match meta.get("partition-spec-id") {
                None => 0,
                Some(v) => {
                    let v = String::from_utf8_lossy(v);
                    v.parse().map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("partition-spec-id {:?} is invalid", v),
                        )
                        .set_source(err)
                    })?
                }
            };

            match c {
                0 => types::ManifestContentType::Data,
                1 => types::ManifestContentType::Deletes,
                _ => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("content type {} is invalid", c),
                    ));
                }
            }
        },
    };

    // Parse manifest entries
    let mut entries = Vec::<types::ManifestEntry>::new();
    for value in reader {
        let v = value?;
        entries.push(from_value::<ManifestEntry>(&v)?.try_into()?);
    }

    Ok(types::ManifestFile { metadata, entries })
}

/// Save manifest file.
pub fn write_manifest_file() -> Result<ManifestListEntry> {
    Err(Error::new(
        ErrorKind::IcebergFeatureUnsupported,
        "Write manifest file!",
    ))
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

impl TryFrom<ManifestEntry> for types::ManifestEntry {
    type Error = Error;

    fn try_from(v: ManifestEntry) -> Result<Self> {
        Ok(types::ManifestEntry {
            status: types::ManifestStatus::try_from(v.status as u8)?,
            snapshot_id: v.snapshot_id,
            sequence_number: v.sequence_number,
            file_sequence_number: v.file_sequence_number,
            data_file: v.data_file.try_into()?,
        })
    }
}

impl TryFrom<types::ManifestEntry> for ManifestEntry {
    type Error = Error;

    fn try_from(v: types::ManifestEntry) -> Result<Self> {
        Ok(Self {
            status: v.status as i32,
            snapshot_id: v.snapshot_id,
            sequence_number: v.sequence_number,
            file_sequence_number: v.file_sequence_number,
            data_file: v.data_file.try_into()?,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct DataFile {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<Vec<I64Entry>>,
    value_counts: Option<Vec<I64Entry>>,
    null_value_counts: Option<Vec<I64Entry>>,
    nan_value_counts: Option<Vec<I64Entry>>,
    distinct_counts: Option<Vec<I64Entry>>,
    lower_bounds: Option<Vec<BytesEntry>>,
    upper_bounds: Option<Vec<BytesEntry>>,
    #[serde_as(as = "Option<Bytes>")]
    key_metadata: Option<Vec<u8>>,
    split_offsets: Vec<i64>,
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
}

impl TryFrom<DataFile> for types::DataFile {
    type Error = Error;

    fn try_from(v: DataFile) -> Result<Self> {
        Ok(types::DataFile {
            content: DataContentType::try_from(v.content as u8)?,
            file_path: v.file_path,
            file_format: parse_data_file_format(&v.file_format)?,
            partition: (),
            record_count: v.record_count,
            file_size_in_bytes: v.file_size_in_bytes,
            column_sizes: v.column_sizes.map(parse_i64_entry),
            value_counts: v.value_counts.map(parse_i64_entry),
            null_value_counts: v.null_value_counts.map(parse_i64_entry),
            nan_value_counts: v.nan_value_counts.map(parse_i64_entry),
            distinct_counts: v.distinct_counts.map(parse_i64_entry),
            lower_bounds: v.lower_bounds.map(parse_bytes_entry),
            upper_bounds: v.upper_bounds.map(parse_bytes_entry),
            key_metadata: v.key_metadata,
            split_offsets: v.split_offsets,
            equality_ids: v.equality_ids,
            sort_order_id: v.sort_order_id,
        })
    }
}

impl TryFrom<types::DataFile> for DataFile {
    type Error = Error;

    fn try_from(v: types::DataFile) -> Result<DataFile> {
        Ok(DataFile {
            content: v.content as i32,
            file_path: v.file_path,
            file_format: v.file_format.to_string(),
            record_count: v.record_count,
            file_size_in_bytes: v.file_size_in_bytes,
            column_sizes: v.column_sizes.map(to_i64_entry),
            value_counts: v.value_counts.map(to_i64_entry),
            null_value_counts: v.null_value_counts.map(to_i64_entry),
            nan_value_counts: v.nan_value_counts.map(to_i64_entry),
            distinct_counts: v.distinct_counts.map(to_i64_entry),
            lower_bounds: v.lower_bounds.map(to_bytes_entry),
            upper_bounds: v.upper_bounds.map(to_bytes_entry),
            key_metadata: v.key_metadata,
            split_offsets: v.split_offsets,
            equality_ids: v.equality_ids,
            sort_order_id: v.sort_order_id,
        })
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct BytesEntry {
    key: i32,
    #[serde_as(as = "Bytes")]
    value: Vec<u8>,
}

fn parse_bytes_entry(v: Vec<BytesEntry>) -> HashMap<i32, Vec<u8>> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        m.insert(entry.key, entry.value);
    }
    m
}

fn to_bytes_entry(v: HashMap<i32, Vec<u8>>) -> Vec<BytesEntry> {
    v.into_iter()
        .map(|e| BytesEntry {
            key: e.0,
            value: e.1,
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct I64Entry {
    key: i32,
    value: i64,
}

fn parse_i64_entry(v: Vec<I64Entry>) -> HashMap<i32, i64> {
    let mut m = HashMap::with_capacity(v.len());
    for entry in v {
        m.insert(entry.key, entry.value);
    }
    m
}

fn to_i64_entry(entries: HashMap<i32, i64>) -> Vec<I64Entry> {
    entries
        .iter()
        .map(|e| I64Entry {
            key: *e.0,
            value: *e.1,
        })
        .collect()
}

fn parse_data_file_format(s: &str) -> Result<types::DataFileFormat> {
    types::DataFileFormat::from_str(s)
}

struct ManifestWriter {
    content_type: ManifestContentType,
    table_schema: types::Schema,
    partition_spec: types::PartitionSpec,
    output_path: String,

    added_files: i64,
    added_rows: i64,
    existing_files: i64,
    existing_rows: i64,
    deleted_files: i64,
    deleted_rows: i64,
    min_seq_num: Option<i64>,
    snapshot_id: i64,
    op: Operator,
}

impl ManifestWriter {
    pub async fn write(mut self, manifest: ManifestFile) -> Result<ManifestListEntry> {
        // A place holder for avro schema since avro writer needs its reference.
        let avro_schema;
        let mut avro_writer = match manifest
            .metadata
            .format_version
            .unwrap_or(TableFormatVersion::V1)
        {
            TableFormatVersion::V1 => {
                let partition_type = self.partition_spec.partition_type(&self.table_schema)?;
                avro_schema = AvroSchema::try_from(&ManifestFile::v1_schema(partition_type))?;
                self.v1_writer(&avro_schema)?
            }
            TableFormatVersion::V2 => {
                let partition_type = self.partition_spec.partition_type(&self.table_schema)?;
                avro_schema = AvroSchema::try_from(&ManifestFile::v1_schema(partition_type))?;
                self.v2_writer(&avro_schema)?
            }
        };

        for entry in manifest.entries {
            match entry.status {
                ManifestStatus::Added => {
                    self.added_files += 1;
                    self.added_rows += entry.data_file.record_count;
                }
                ManifestStatus::Deleted => {
                    self.deleted_files += 1;
                    self.deleted_rows += entry.data_file.record_count;
                }
                ManifestStatus::Existing => {
                    self.existing_files += 1;
                    self.existing_rows += entry.data_file.record_count;
                }
            }

            if entry.is_alive() {
                if let Some(cur_min_seq_num) = self.min_seq_num {
                    self.min_seq_num = Some(
                        entry
                            .sequence_number
                            .map(|v| min(v, cur_min_seq_num))
                            .unwrap_or(cur_min_seq_num),
                    );
                } else {
                    self.min_seq_num = entry.sequence_number;
                }
            }

            // TODO: Add partition summary
            avro_writer.append_ser(ManifestEntry::try_from(entry)?)?;
        }

        let length = avro_writer.flush()?;
        let connect = avro_writer.into_inner()?;
        self.op.write(self.output_path.as_str(), connect).await?;

        Ok(ManifestListEntry {
            manifest_path: "".to_string(),
            manifest_length: length as i64,
            partition_spec_id: manifest.metadata.partition_spec_id,
            content: self.content_type,
            sequence_number: UNASSIGNED_SEQ_NUM,
            min_sequence_number: self.min_seq_num.unwrap_or(UNASSIGNED_SEQ_NUM),
            added_snapshot_id: self.snapshot_id,
            added_files_count: self.added_files as i32,
            existing_files_count: self.existing_files as i32,
            deleted_files_count: self.deleted_files as i32,
            added_rows_count: self.added_rows,
            existing_rows_count: self.existing_rows,
            deleted_rows_count: self.deleted_rows,
            partitions: None,
            key_metadata: None,
        })
    }

    fn v1_writer<'a>(&self, avro_schema: &'a AvroSchema) -> Result<AvroWriter<'a, Vec<u8>>> {
        let mut writer = AvroWriter::new(avro_schema, Vec::new());
        writer.add_user_metadata(
            "schema".to_string(),
            serde_json::to_string(&Schema::try_from(&self.table_schema)?)?,
        )?;
        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(&PartitionSpec::try_from(&self.partition_spec)?)?,
        )?;
        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            format!("{}", self.partition_spec.spec_id),
        )?;
        writer.add_user_metadata(
            "format-version".to_string(),
            TableFormatVersion::V1.to_string(),
        )?;
        Ok(writer)
    }

    fn v2_writer<'a>(&self, avro_schema: &'a AvroSchema) -> Result<AvroWriter<'a, Vec<u8>>> {
        let mut writer = AvroWriter::new(avro_schema, Vec::new());
        writer.add_user_metadata(
            "schema".to_string(),
            serde_json::to_string(&Schema::try_from(&self.table_schema)?)?,
        )?;
        writer.add_user_metadata(
            "partition-spec".to_string(),
            serde_json::to_string(&PartitionSpec::try_from(&self.partition_spec)?)?,
        )?;
        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            format!("{}", self.partition_spec.spec_id),
        )?;
        writer.add_user_metadata(
            "format-version".to_string(),
            TableFormatVersion::V2.to_string(),
        )?;
        writer.add_user_metadata("content".to_string(), "data")?;

        Ok(writer)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;

    use crate::types::ManifestFile;
    use anyhow::Result;
    use apache_avro::from_value;
    use apache_avro::Reader;

    use super::*;

    #[test]
    fn test_load_manifest_entry() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let reader = Reader::new(&bs[..]).unwrap();
        let meta = reader.user_metadata();
        assert!(meta.get("schema").is_some());
        assert!(meta.get("partition-spec").is_some());
        assert_eq!(
            meta.get("partition-spec-id").map(|v| v.as_slice()),
            Some("0".as_bytes())
        );
        assert_eq!(
            meta.get("format-version").map(|v| v.as_slice()),
            Some("1".as_bytes())
        );

        let mut entries = Vec::new();
        for value in reader {
            let v = value?;
            entries.push(from_value::<ManifestEntry>(&v)?);
        }

        assert_eq!(entries.len(), 3);
        Ok(())
    }

    #[test]
    fn test_parse_manifest() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let ManifestFile { metadata, entries } = parse_manifest_file(&bs)?;

        assert_eq!(
            metadata,
            types::ManifestMetadata {
                schema: types::Schema {
                    schema_id: 0,
                    identifier_field_ids: None,
                    fields: vec![
                        types::Field {
                            id: 1,
                            name: "id".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::Long),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        },
                        types::Field {
                            id: 2,
                            name: "data".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::String),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        },
                    ],
                },
                schema_id: 0,
                partition_spec_id: 0,
                format_version: Some(TableFormatVersion::V1),
                content: types::ManifestContentType::Data,
            }
        );

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00000-0-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(entries[1].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00001-1-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(entries[2].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00002-2-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");

        Ok(())
    }
}
