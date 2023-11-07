use std::{collections::HashMap, fs::File};

use icelake::{catalog::load_catalog, transaction::Transaction, Table};

mod utils;
use tokio::runtime::Builder;
pub use utils::*;

use libtest_mimic::{Arguments, Trial};

const TIMES: usize = 100;

pub struct TestFixture {
    docker_compose: DockerCompose,
    poetry: Poetry,
    catalog_configs: HashMap<String, String>,

    test_case: TestCase,
}

impl TestFixture {
    pub fn new(
        docker_compose: DockerCompose,
        poetry: Poetry,
        toml_file: String,
        catalog_type: &str,
    ) -> Self {
        let toml_file_path = format!(
            "{}/../testdata/toml/{}",
            env!("CARGO_MANIFEST_DIR"),
            toml_file
        );
        let test_case = TestCase::parse(File::open(toml_file_path).unwrap());

        let catalog_configs = match catalog_type {
            "storage" => HashMap::from([
                ("iceberg.catalog.name", "demo".to_string()),
                ("iceberg.catalog.type", "storage".to_string()),
                (
                    "iceberg.catalog.demo.warehouse",
                    format!("s3://icebergdata/{}", &test_case.warehouse_root),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                ("iceberg.table.io.root", test_case.warehouse_root.clone()),
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
                        docker_compose.get_container_ip("rest")
                    ),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                (
                    "iceberg.table.io.root",
                    format!(
                        "{}/{}/{}",
                        test_case.warehouse_root.clone(),
                        test_case.table_name.namespace,
                        test_case.table_name.name,
                    ),
                ),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            _ => panic!("Unrecognized catalog type: {catalog_type}"),
        };
        Self {
            docker_compose,
            poetry,
            test_case,
            catalog_configs: catalog_configs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn init_table_with_spark(&self) {
        let args = vec![
            "-s".to_string(),
            self.spark_connect_url(),
            "--sql".to_string(),
        ];
        let args: Vec<String> = args
            .into_iter()
            .chain(self.test_case.init_sqls.clone())
            .collect();
        self.poetry.run_file(
            "init.py",
            args,
            format!("Init {} with spark", self.test_case.table_name),
        )
    }

    fn check_table_with_spark(&self, num: usize) {
        for check_sqls in &self.test_case.query_sql {
            self.poetry.run_file(
                "check_result.py",
                [
                    "-s",
                    &self.spark_connect_url(),
                    "-q",
                    "select count(*) from s1.t1",
                    "-n",
                    &format!("{num}"),
                ],
                format!("Check {}", check_sqls[0].as_str()),
            )
        }
    }

    fn spark_connect_url(&self) -> String {
        format!(
            "sc://{}:{}",
            self.docker_compose.get_container_ip("spark"),
            SPARK_CONNECT_SERVER_PORT
        )
    }

    pub async fn create_icelake_table(&self) -> Table {
        let catalog = load_catalog(&self.catalog_configs).await.unwrap();
        catalog
            .load_table(&self.test_case.table_name)
            .await
            .unwrap()
    }

    pub async fn write_data_with_icelake(&mut self) {
        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let mut results = Vec::with_capacity(TIMES);
        for i in 0..TIMES {
            log::debug!("Start inserting into icelake table {i} times.");
            let records = &self.test_case.write_date;

            let mut task_writer = table
                .writer_builder()
                .await
                .unwrap()
                .build_append_only_writer()
                .await
                .unwrap();

            for record_batch in records {
                log::info!(
                    "Insert record batch with {} records using icelake.",
                    record_batch.num_rows()
                );
                task_writer.write(record_batch).await.unwrap();
            }

            let result = task_writer.close().await.unwrap();
            log::debug!("Insert {} data files: {:?}", result.len(), result);
            results.push(result);
            log::debug!("Finished inserting into icelake table {i} times.");
        }

        // Commit table transaction
        {
            let mut tx = Transaction::new(&mut table);
            tx.append_data_file(results.into_iter().flatten());
            tx.commit().await.unwrap();
        }
    }

    fn call_spark_to_compact_table_written_with_icelake(&mut self) {
        let args = vec![
            "-s".to_string(),
            self.spark_connect_url(),
            "--sql".to_string(),
            "CALL demo.system.rewrite_data_files(table => 's1.t1', options => map('target-file-size-bytes', '104857600'))".to_string(),
        ];

        self.poetry.run_file(
            "init.py",
            args,
            format!("Init {} with spark", self.test_case.table_name),
        )
    }

    async fn run(mut self) {
        self.init_table_with_spark();
        self.write_data_with_icelake().await;
        self.call_spark_to_compact_table_written_with_icelake();
        self.check_table_with_spark(5 * TIMES);
        self.write_data_with_icelake().await;
        self.call_spark_to_compact_table_written_with_icelake();
        self.check_table_with_spark(5 * TIMES * 2);
    }

    pub fn block_run(self) {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .thread_name(self.docker_compose.project_name())
            .build()
            .unwrap();

        rt.block_on(async { self.run().await })
    }
}

fn create_test_fixture(project_name: &str, toml_file: &str, catalog: &str) -> TestFixture {
    set_up();

    let docker_compose = match catalog {
        "storage" => DockerCompose::new(project_name, "iceberg-fs"),
        "rest" => DockerCompose::new(project_name, "iceberg-rest"),
        _ => panic!("Unrecognized catalog : {catalog}"),
    };
    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    docker_compose.run();

    TestFixture::new(docker_compose, poetry, toml_file.to_string(), catalog)
}

fn main() {
    // Parse command line arguments
    let args = Arguments::from_args();

    let catalogs = vec!["storage", "rest"];
    let test_cases = vec!["partition_month_test.toml"];

    let mut tests = Vec::with_capacity(16);
    for catalog in &catalogs {
        for test_case in &test_cases {
            let test_name = &normalize_test_name(format!(
                "{}_test_compact_insert_{test_case}_with_{catalog}_catalog",
                module_path!()
            ));

            let fixture = create_test_fixture(test_name, test_case, catalog);
            tests.push(Trial::test(test_name, move || {
                fixture.block_run();
                Ok(())
            }));
        }
    }

    // Run all tests and exit the application appropriatly.
    libtest_mimic::run(&args, tests).exit();
}
