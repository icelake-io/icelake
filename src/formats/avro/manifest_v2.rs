use std::collections::HashMap;

use serde::Deserialize;
use serde_with::serde_as;
use serde_with::Bytes;

#[derive(Debug, Deserialize)]
struct Manifest {
    schema: ManifestEntry,
    schema_id: i32,
    format_version: i32,
    content: String,
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

    use apache_avro::Reader;
    use apache_avro::{from_value, types::Record, Codec, Error, Schema, Writer};

    use super::*;

    #[test]
    fn test_load_manifest() {
        let path = format!(
            "{}/testdata/simple_table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let reader = Reader::new(&bs[..]).unwrap();

        for value in reader {
            let v = value.unwrap();
            println!("{:?}", from_value::<ManifestEntry>(&v).unwrap());
        }
    }

    #[test]
    fn test_load_manifest_list() {
        let path = format!(
            "{}/testdata/simple_table/metadata/snap-1646658105718557341-1-10d28031-9739-484c-92db-cdf2975cead4.avro",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let bs = fs::read(path).expect("read_file must succeed");

        let reader = Reader::new(&bs[..]).unwrap();

        for value in reader {
            let v = value.unwrap();
            println!("{:?}", v);
            println!("{:?}", from_value::<ManifestEntry>(&v).unwrap());
        }
    }
}
