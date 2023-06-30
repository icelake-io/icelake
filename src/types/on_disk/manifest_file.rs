use std::collections::HashMap;

use apache_avro::from_value;
use apache_avro::Reader;
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::Bytes;

use super::parse_schema;
use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// Parse manifest file from avro bytes.
pub fn parse_manifest_file(
    bs: &[u8],
) -> Result<(types::ManifestMetadata, Vec<types::ManifestFile>)> {
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
            match meta.get("format-version") {
                None => 0,
                Some(v) => {
                    let v = String::from_utf8_lossy(v);
                    v.parse().map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("format-version {:?} is invalid", v),
                        )
                        .set_source(err)
                    })?
                }
            }
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
                    ))
                }
            }
        },
    };

    // Parse manifest entries
    let mut entries = Vec::new();
    for value in reader {
        let v = value?;
        entries.push(from_value::<ManifestFile>(&v)?.try_into()?);
    }

    Ok((metadata, entries))
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct ManifestFile {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

impl TryFrom<ManifestFile> for types::ManifestFile {
    type Error = Error;

    fn try_from(v: ManifestFile) -> Result<Self> {
        Ok(types::ManifestFile {
            status: parse_manifest_status(v.status)?,
            snapshot_id: v.snapshot_id,
            sequence_number: v.sequence_number,
            file_sequence_number: v.file_sequence_number,
            data_file: v.data_file.try_into()?,
        })
    }
}

#[serde_as]
#[derive(Deserialize)]
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
            content: parse_data_content_type(v.content)?,
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

#[serde_as]
#[derive(Deserialize)]
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

#[derive(Deserialize)]
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

fn parse_manifest_status(v: i32) -> Result<types::ManifestStatus> {
    match v {
        0 => Ok(types::ManifestStatus::Existing),
        1 => Ok(types::ManifestStatus::Added),
        2 => Ok(types::ManifestStatus::Deleted),
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("manifest status {} is invalid", v),
        )),
    }
}

fn parse_data_content_type(v: i32) -> Result<types::DataContentType> {
    match v {
        0 => Ok(types::DataContentType::Data),
        1 => Ok(types::DataContentType::PostionDeletes),
        2 => Ok(types::DataContentType::EqualityDeletes),
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("data content type {} is invalid", v),
        )),
    }
}

fn parse_data_file_format(s: &str) -> Result<types::DataFileFormat> {
    match s.to_lowercase().as_str() {
        "avro" => Ok(types::DataFileFormat::Avro),
        "orc" => Ok(types::DataFileFormat::Orc),
        "parquet" => Ok(types::DataFileFormat::Parquet),
        v => Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("data file format {:?} is not supported", v),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use apache_avro::from_value;
    use apache_avro::Reader;

    use super::*;
    use anyhow::Result;

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
            entries.push(from_value::<ManifestFile>(&v)?);
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

        let (meta, manifests) = parse_manifest_file(&bs)?;

        assert_eq!(
            meta,
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
                            comment: None
                        },
                        types::Field {
                            id: 2,
                            name: "data".to_string(),
                            required: false,
                            field_type: types::Any::Primitive(types::Primitive::String),
                            comment: None
                        }
                    ]
                },
                schema_id: 0,
                partition_spec_id: 0,
                format_version: 1,
                content: types::ManifestContentType::Data
            }
        );

        assert_eq!(manifests.len(), 3);
        assert_eq!(manifests[0].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00000-0-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(manifests[1].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00001-1-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(manifests[2].data_file.file_path, "/opt/bitnami/spark/warehouse/db/table/data/00002-2-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");

        Ok(())
    }
}
