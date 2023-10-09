mod utils;

use std::{collections::HashMap, fs::File};

use arrow_array::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use futures::{StreamExt, TryStreamExt};
use icelake::{
    catalog::load_catalog,
    io::{TableScan, TableScanBuilder},
    Table, TableIdentifier,
};
pub use utils::*;

pub struct ScanTestCase<F: FnMut(TableScanBuilder) -> TableScan> {
    docker_compose: DockerCompose,
    poetry: Poetry,
    catalog_type: String,
    table_scan: F,
    // List of csv file paths.
    results: Vec<String>,

    // Inited after docker.
    catalog_configs: HashMap<String, String>,
}

impl<F: FnMut(TableScanBuilder) -> TableScan> ScanTestCase<F> {
    fn new(catalog_type: &str, table_scan: F, results: Vec<String>, test_name: String) -> Self {
        set_up();
        let docker_compose = match catalog_type {
            "storage" => DockerCompose::new(test_name, "iceberg-fs"),
            "rest" => DockerCompose::new(test_name, "iceberg-rest"),
            _ => panic!("Unrecognized catalog : {catalog_type}"),
        };
        let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

        ScanTestCase {
            docker_compose,
            poetry,
            catalog_type: catalog_type.to_string(),
            table_scan,
            results,
            catalog_configs: HashMap::new(),
        }
    }
    async fn run(mut self) {
        self.setup_docker();
        self.init_data();
        self.perform_and_check_table_scan().await;
    }

    fn setup_docker(&mut self) {
        self.docker_compose.run();
        let warehouse_root = "demo";
        let table_namespace = "s1";
        let table_name = "t1";
        let catalog_configs = match self.catalog_type.as_str() {
            "storage" => HashMap::from([
                ("iceberg.catalog.name", "demo".to_string()),
                ("iceberg.catalog.type", "storage".to_string()),
                (
                    "iceberg.catalog.demo.warehouse",
                    format!("s3://icebergdata/{}", &warehouse_root),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        self.docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                ("iceberg.table.io.root", warehouse_root.to_string()),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            "rest" => HashMap::from([
                ("iceberg.catalog.name", "demo".to_string()),
                ("iceberg.catalog.type", "rest".to_string()),
                (
                    "iceberg.catalog.demo.uri",
                    format!(
                        "http://{}:{REST_CATALOG_PORT}",
                        self.docker_compose.get_container_ip("rest")
                    ),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        self.docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                (
                    "iceberg.table.io.root",
                    format!("{}/{}/{}", warehouse_root, table_namespace, table_name,),
                ),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            _ => panic!("Unrecognized catalog type: {}", self.catalog_type),
        };

        self.catalog_configs = catalog_configs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    }

    fn init_data(&self) {
        let args = vec![
            "-s".to_string(),
            self.spark_connect_url(),
            "--sql".to_string(),
        ];
        let init_sqls = vec![
            "CREATE SCHEMA if not exists s1;".to_string(),
            "
            CREATE TABLE s1.t1
            (
                id long,
                v_int int,
                v_long long,
                v_float float,
                v_double double,
                v_varchar string,
                v_bool boolean,
                v_date date,
                v_decimal decimal(36, 10)
            ) USING iceberg
            PARTITIONED BY (truncate(1, v_varchar))
            TBLPROPERTIES ('format-version'='2');
            "
            .to_string(),
            "
        INSERT INTO s1.t1 VALUES
        (1, 1, 1, 1.1, 1.1, '1abc', true, date '2020-01-01',  1.1), 
        (2, 2, 2, 2.2, 2.2, '2abc', true, date '2020-02-02',  2.2), 
        (3, 3, 3, 3.3, 3.3, '3abc', true, date '2020-03-03',  3.3), 
        (4, 4, 4, 4.4, 4.4, '1abc', true, date '2020-04-04',  4.4), 
        (5, 5, 5, 5.5, 5.5, '2abc', true, date '2020-05-05',  5.5), 
        (6, 6, 6, 6.6, 6.6, '3abc', true, date '2020-06-06',  6.6), 
        (7, 7, 7, 7.7, 7.7, '1abc', true, date '2020-07-07',  7.7), 
        (8, 8, 8, 8.8, 8.8, '2abc', true, date '2020-08-08',  8.8), 
        (9, 9, 9, 9.9, 9.9, '3abc', true, date '2020-09-09',  9.9); 
            "
            .to_string(),
        ];
        let args: Vec<String> = args.into_iter().chain(init_sqls).collect();
        self.poetry.run_file("init.py", args, "Init t1 with spark")
    }

    fn spark_connect_url(&self) -> String {
        format!(
            "sc://{}:{}",
            self.docker_compose.get_container_ip("spark"),
            SPARK_CONNECT_SERVER_PORT
        )
    }

    async fn create_icelake_table(&self) -> Table {
        let catalog = load_catalog(&self.catalog_configs).await.unwrap();
        catalog
            .load_table(&TableIdentifier::new(vec!["s1", "t1"].into_iter()).unwrap())
            .await
            .unwrap()
    }

    async fn perform_and_check_table_scan(mut self) {
        let table = self.create_icelake_table().await;
        let table_scan_builder = table.new_scan_builder();
        let mut result = (self.table_scan)(table_scan_builder)
            .scan(&table)
            .await
            .unwrap();

        while let Some(file_scan) = result.next().await {
            let file_scan = file_scan.unwrap();
            let path = file_scan.path().to_string();
            assert!(!self.results.is_empty(), "Unexpected path: {}", path);
            let csv = self.results.remove(0);
            let record_batches: Vec<RecordBatch> =
                file_scan.scan().await.unwrap().try_collect().await.unwrap();

            let schema = record_batches[0].schema().clone();

            let record_batch = concat_batches(&schema, &record_batches).unwrap();

            let expected_batches = self.read_csv(schema, &csv);

            assert_eq!(
                record_batch, expected_batches,
                "Data mismatch for path: {}",
                path
            );
        }

        assert!(self.results.is_empty(), "Missing paths: {:?}", self.results);
    }

    fn read_csv(&self, schema: SchemaRef, path: &str) -> RecordBatch {
        let file = File::open(format!(
            "{}/../testdata/csv/{}",
            env!("CARGO_MANIFEST_DIR"),
            path
        ))
        .unwrap();

        let csv_reader = ReaderBuilder::new(schema.clone())
            .has_header(false)
            .build(file)
            .unwrap();

        let batches = csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>();
        concat_batches(&schema, &batches).unwrap()
    }
}

#[tokio::test]
async fn test_scan_all() {
    let case = ScanTestCase::new(
        "rest",
        |builder: TableScanBuilder| builder.build().unwrap(),
        vec![
            "1.csv".to_string(),
            "2.csv".to_string(),
            "3.csv".to_string(),
        ],
        normalize_test_name(format!("{}_test_scan_all_rest_catalog", module_path!())),
    );

    case.run().await
}

// #[tokio::test]
// async fn test_scan_partition() {
//     let case = ScanTestCase::new(
//         "rest",
//         |builder: TableScanBuilder| {
//             let partition_type = Struct::new(vec![Arc::new(Field::required(
//                 1,
//                 "id",
//                 icelake::types::Any::Primitive(icelake::types::Primitive::String),
//             ))]);
//             let mut partition_value_builder = StructValueBuilder::new(partition_type.into());
//             partition_value_builder
//                 .add_field(
//                     1,
//                     Some(AnyValue::Primitive(icelake::types::PrimitiveValue::String(
//                         "1".to_string(),
//                     ))),
//                 )
//                 .unwrap();
//             let partition_value = partition_value_builder.build().unwrap();

//             builder
//                 .with_partition_value(Some(partition_value))
//                 .build()
//                 .unwrap()
//         },
//         vec!["1.csv".to_string()],
//         normalize_test_name(format!(
//             "{}_test_scan_partition_rest_catalog",
//             module_path!()
//         )),
//     );

//     case.run().await
// }
