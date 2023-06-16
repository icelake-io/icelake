use std::collections::HashMap;

use serde::Deserialize;
use serde_with::serde_as;
use serde_with::Bytes;

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
struct ManifestFile {
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

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

#[derive(Debug, Deserialize)]
struct DataFile {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    record_count: i64,
    file_size_in_bytes: i64,
    // column_sizes: Option<HashMap<i32, i64>>,
    // value_counts: Option<HashMap<i32, i64>>,
    // null_value_counts: Option<HashMap<i32, i64>>,
    // nan_value_counts: Option<HashMap<i32, i64>>,
    // distinct_counts: Option<HashMap<i32, i64>>,
    lower_bounds: Option<Vec<BytesEntry>>,
    upper_bounds: Option<Vec<BytesEntry>>,
    // key_metadata: Option<Vec<u8>>,
    // split_offsets: Vec<i64>,
    // equality_ids: Option<Vec<i32>>,
    // sort_order_id: Option<i32>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct BytesEntry {
    key: i32,
    #[serde_as(as = "Bytes")]
    value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use anyhow::Result;
    use apache_avro::from_value;
    use apache_avro::Reader;

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
            files.push(from_value::<ManifestFile>(&value?)?);
        }

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0],
            ManifestFile {
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
                key_metadata: None
            }
        );

        Ok(())
    }

    #[test]
    fn test_load_manifest_entry() {
        let path = format!(
            "{}/testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let reader = Reader::new(&bs[..]).unwrap();
        println!(
            "{:?}",
            reader
                .user_metadata()
                .iter()
                .map(|(k, v)| (k, String::from_utf8_lossy(v)))
                .collect::<Vec<_>>()
        );

        for value in reader {
            let v = value.unwrap();
            println!("{:?}", from_value::<ManifestEntry>(&v).unwrap());
        }
    }
}
