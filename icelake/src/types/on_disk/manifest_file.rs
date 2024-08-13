use std::cmp::min;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use apache_avro::{from_value, Schema as AvroSchema};
use apache_avro::{to_value, Reader};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::Bytes;

use super::parse_schema;
use super::partition_spec::parse_partition_spec_fields;
use super::value::Value;
use crate::types::on_disk::partition_spec::serialize_partition_spec_fields;
use crate::types::on_disk::schema::serialize_schema;
use crate::types::to_avro::to_avro_schema;
use crate::types::{self, Any, AnyValue, PartitionSpec, StructValue};
use crate::types::{DataContentType, ManifestContentType, ManifestListEntry, UNASSIGNED_SEQ_NUM};
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
        partition_spec: PartitionSpec {
            spec_id: {
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
            fields: match meta.get("partition-spec") {
                None => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "partition-spec is required in manifest metadata but not found",
                    ))
                }
                Some(v) => parse_partition_spec_fields(v)?,
            },
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
            if let Some(v) = meta.get("content") {
                let v = String::from_utf8_lossy(v);
                v.parse()?
            } else {
                ManifestContentType::Data
            }
        },
    };

    let partition_type = Any::Struct(Arc::new(
        metadata.partition_spec.partition_type(&metadata.schema)?,
    ));

    // Parse manifest entries
    let mut entries = Vec::<types::ManifestEntry>::new();
    for value in reader {
        let v = value?;
        entries.push(from_value::<ManifestEntry>(&v)?.into_memory(&partition_type)?);
    }

    Ok(types::ManifestFile { metadata, entries })
}

/// Serialize [`DataFile`] to json value.
pub fn data_file_to_json(data_file: types::DataFile) -> Result<serde_json::Value> {
    let data = DataFile::try_from(data_file)?;
    serde_json::to_value(data).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to serialize data file to json",
        )
        .set_source(e)
    })
}

/// Parse [`DataFile`] from json value.
pub fn data_file_from_json(
    value: serde_json::Value,
    partition_type: types::Any,
) -> Result<types::DataFile> {
    let data_file = serde_json::from_value::<DataFile>(value).map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to parse data file from json").set_source(e)
    })?;
    data_file.into_memory(&partition_type)
}

#[derive(Serialize, Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq))]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

impl ManifestEntry {
    pub fn into_memory(self, partition_type: &types::Any) -> Result<types::ManifestEntry> {
        Ok(types::ManifestEntry {
            status: types::ManifestStatus::try_from(self.status as u8)?,
            snapshot_id: self.snapshot_id,
            sequence_number: self.sequence_number,
            file_sequence_number: self.file_sequence_number,
            data_file: self.data_file.into_memory(partition_type)?,
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
#[cfg_attr(test, derive(Debug, PartialEq))]
struct DataFile {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    partition: Value,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<Vec<I64Entry>>,
    value_counts: Option<Vec<I64Entry>>,
    null_value_counts: Option<Vec<I64Entry>>,
    nan_value_counts: Option<Vec<I64Entry>>,
    #[serde(skip_serializing)]
    distinct_counts: Option<Vec<I64Entry>>,
    lower_bounds: Option<Vec<BytesEntry>>,
    upper_bounds: Option<Vec<BytesEntry>>,
    #[serde_as(as = "Option<Bytes>")]
    key_metadata: Option<Vec<u8>>,
    split_offsets: Option<Vec<i64>>,
    #[serde(default)]
    equality_ids: Option<Vec<i32>>,
    sort_order_id: Option<i32>,
}

impl DataFile {
    pub fn into_memory(self, partition_type: &types::Any) -> Result<types::DataFile> {
        let partition_value = match self.partition.into_memory(partition_type)? {
            Some(AnyValue::Struct(s)) => s,
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "partition value is not struct",
                ));
            }
            None => StructValue::default(),
        };

        Ok(types::DataFile {
            content: DataContentType::try_from(self.content as u8)?,
            file_path: self.file_path,
            file_format: parse_data_file_format(&self.file_format)?,
            partition: partition_value,
            record_count: self.record_count,
            file_size_in_bytes: self.file_size_in_bytes,
            column_sizes: self.column_sizes.map(parse_i64_entry),
            value_counts: self.value_counts.map(parse_i64_entry),
            null_value_counts: self.null_value_counts.map(parse_i64_entry),
            nan_value_counts: self.nan_value_counts.map(parse_i64_entry),
            distinct_counts: self.distinct_counts.map(parse_i64_entry),
            lower_bounds: self.lower_bounds.map(parse_bytes_entry),
            upper_bounds: self.upper_bounds.map(parse_bytes_entry),
            key_metadata: self.key_metadata,
            split_offsets: self.split_offsets,
            equality_ids: self.equality_ids,
            sort_order_id: self.sort_order_id,
        })
    }
}

impl TryFrom<types::DataFile> for DataFile {
    type Error = Error;

    fn try_from(v: types::DataFile) -> Result<DataFile> {
        Ok(DataFile {
            content: v.content as i32,
            file_path: v.file_path,
            file_format: v.file_format.to_string().to_ascii_uppercase(),
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
            partition: v.partition.into(),
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

/// Manifest writer to write manifest to file.
pub(crate) struct ManifestWriter {
    partition_spec: types::PartitionSpec,
    op: Operator,
    table_location: String,
    // Output path relative to operator root.
    output_path: String,
    snapshot_id: i64,

    added_files: i64,
    added_rows: i64,
    existing_files: i64,
    existing_rows: i64,
    deleted_files: i64,
    deleted_rows: i64,
    seq_num: i64,
    min_seq_num: Option<i64>,
}

impl ManifestWriter {
    pub(crate) fn new(
        partition_spec: types::PartitionSpec,
        op: Operator,
        table_location: impl Into<String>,
        output_path: impl Into<String>,
        snapshot_id: i64,
        seq_num: i64,
    ) -> Self {
        Self {
            partition_spec,
            op,
            table_location: table_location.into(),
            output_path: output_path.into(),
            snapshot_id,

            added_files: 0,
            added_rows: 0,
            existing_files: 0,
            existing_rows: 0,
            deleted_files: 0,
            deleted_rows: 0,
            seq_num,
            min_seq_num: None,
        }
    }

    pub async fn write(mut self, manifest: types::ManifestFile) -> Result<ManifestListEntry> {
        assert_eq!(
            self.partition_spec.spec_id, manifest.metadata.partition_spec.spec_id,
            "Partition spec id not match!"
        );
        // A place holder for avro schema since avro writer needs its reference.
        let avro_schema;
        let mut avro_writer = match manifest
            .metadata
            .format_version
            .unwrap_or(TableFormatVersion::V1)
        {
            TableFormatVersion::V1 => {
                return Err(Error::new(
                    ErrorKind::IcebergFeatureUnsupported,
                    "Currently only writing v2 format is supported!",
                ));
            }
            TableFormatVersion::V2 => {
                let partition_type = self
                    .partition_spec
                    .partition_type(&manifest.metadata.schema)?;
                avro_schema = to_avro_schema(
                    &types::ManifestFile::v2_schema(partition_type),
                    Some("manifest_entry"),
                )?;
                self.v2_writer(&avro_schema, &manifest.metadata.schema)?
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
            let value = to_value(ManifestEntry::try_from(entry)?)?.resolve(&avro_schema)?;
            avro_writer.append(value)?;
        }

        let length = avro_writer.flush()?;
        let connect = avro_writer.into_inner()?;
        self.op.write(self.output_path.as_str(), connect).await?;

        Ok(ManifestListEntry {
            manifest_path: format!("{}/{}", self.table_location, &self.output_path),
            manifest_length: length as i64,
            partition_spec_id: manifest.metadata.partition_spec.spec_id,
            content: manifest.metadata.content,
            sequence_number: self.seq_num,
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

    fn v2_writer<'a>(
        &self,
        avro_schema: &'a AvroSchema,
        table_schema: &types::Schema,
    ) -> Result<AvroWriter<'a, Vec<u8>>> {
        let mut writer = AvroWriter::new(avro_schema, Vec::new());
        writer.add_user_metadata("schema".to_string(), serialize_schema(table_schema)?)?;
        writer.add_user_metadata(
            "partition-spec".to_string(),
            serialize_partition_spec_fields(&self.partition_spec)?,
        )?;
        writer.add_user_metadata(
            "partition-spec-id".to_string(),
            format!("{}", self.partition_spec.spec_id).as_str(),
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
    use std::fs::{canonicalize, read};
    use std::sync::Arc;

    use anyhow::Result;
    use apache_avro::from_value;
    use apache_avro::Reader;
    use opendal::services::Fs;
    use tempfile::TempDir;

    use crate::types::AnyValue;
    use crate::types::Field;
    use crate::types::PrimitiveValue;
    use crate::types::Struct;
    use crate::types::StructValueBuilder;

    use super::*;

    #[test]
    fn test_load_manifest_entry() -> Result<()> {
        let path = format!(
            "{}/../testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env!("CARGO_MANIFEST_DIR")
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
            "{}/../testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env!("CARGO_MANIFEST_DIR")
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let types::ManifestFile { metadata, entries } = parse_manifest_file(&bs)?;

        assert_eq!(
            metadata,
            types::ManifestMetadata {
                schema: types::Schema::new(
                    0,
                    None,
                    types::Struct::new(vec![
                        types::Field {
                            id: 1,
                            name: "id".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::Long),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        }
                        .into(),
                        types::Field {
                            id: 2,
                            name: "data".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::String),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        }
                        .into(),
                    ],)
                ),
                schema_id: 0,
                partition_spec: types::PartitionSpec {
                    spec_id: 0,
                    fields: vec![],
                },
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

    #[tokio::test]
    async fn test_read_write_manifest_file_v2() {
        let manifest_file = types::ManifestFile {
            metadata: types::ManifestMetadata {
                schema: types::Schema::new(
                    0,
                    None,
                    types::Struct::new(vec![
                        types::Field {
                            id: 1,
                            name: "id".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::Long),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        }
                        .into(),
                        types::Field {
                            id: 2,
                            name: "data".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::String),
                            comment: None,
                            initial_default: None,
                            write_default: None,
                        }
                        .into(),
                    ]),
                ),
                schema_id: 0,
                partition_spec: types::PartitionSpec {
                    spec_id: 1,
                    fields: vec![],
                },
                format_version: Some(TableFormatVersion::V2),
                content: types::ManifestContentType::Data,
            },
            entries: vec![
                types::ManifestEntry {
                    status: types::ManifestStatus::Added,
                    snapshot_id: None,
                    sequence_number: Some(2),
                    file_sequence_number: Some(3),
                    data_file: types::DataFile::new(
                        types::DataContentType::Data,
                        "/tmp/1.parquet",
                        types::DataFileFormat::Parquet,
                        100,
                        200,
                    ),
                },
                types::ManifestEntry {
                    status: types::ManifestStatus::Existing,
                    snapshot_id: Some(12),
                    sequence_number: Some(12),
                    file_sequence_number: None,
                    data_file: types::DataFile::new(
                        types::DataContentType::Data,
                        "/tmp/2.parquet",
                        types::DataFileFormat::Parquet,
                        100,
                        200,
                    ),
                },
            ],
        };

        check_manifest_file_serde(manifest_file).await
    }

    async fn check_manifest_file_serde(manifest_file: types::ManifestFile) {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = {
            let canonicalize = canonicalize(tmp_dir.path().to_str().unwrap()).unwrap();
            canonicalize.to_str().unwrap().to_string()
        };
        let filename = "test.avro";

        let operator = {
            let mut builder = Fs::default();
            builder.root(dir_path.as_str());
            Operator::new(builder).unwrap().finish()
        };

        let partition_spec = types::PartitionSpec {
            spec_id: manifest_file.metadata.partition_spec.spec_id,
            fields: vec![],
        };

        let writer =
            ManifestWriter::new(partition_spec, operator, dir_path.as_str(), filename, 3, 1);
        let manifest_list_entry = writer.write(manifest_file.clone()).await.unwrap();

        assert_eq!(
            format!("{dir_path}/{filename}"),
            manifest_list_entry.manifest_path
        );
        assert_eq!(manifest_file.metadata.content, manifest_list_entry.content);

        let restored_manifest_file =
            { parse_manifest_file(&read(tmp_dir.path().join(filename)).unwrap()).unwrap() };

        assert_eq!(manifest_file, restored_manifest_file);
    }

    #[test]
    fn test_data_file_json() {
        let (partition, ty) = {
            let struct_type = Arc::new(Struct::new(vec![
                Field::optional(
                    1,
                    "a",
                    crate::types::Any::Primitive(crate::types::Primitive::Int),
                )
                .into(),
                Field::required(
                    2,
                    "b",
                    crate::types::Any::Primitive(crate::types::Primitive::String),
                )
                .into(),
            ]));
            let mut builder = StructValueBuilder::new(struct_type.clone());
            builder.add_field(1, None).unwrap();
            builder
                .add_field(
                    2,
                    Some(AnyValue::Primitive(PrimitiveValue::String(
                        "hello".to_string(),
                    ))),
                )
                .unwrap();
            (builder.build().unwrap(), types::Any::Struct(struct_type))
        };
        let data_file = types::DataFile {
            content: DataContentType::Data,
            file_path: "/tmp/1.parquet".to_string(),
            file_format: types::DataFileFormat::Parquet,
            partition,
            record_count: 100,
            file_size_in_bytes: 200,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            distinct_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
        };
        let value = data_file_to_json(data_file.clone()).unwrap();
        let actual_data_file = data_file_from_json(value, ty).unwrap();
        assert_eq!(actual_data_file, data_file);
    }
}
