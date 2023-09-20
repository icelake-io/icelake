use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use icelake::types::StructValue;
use icelake::{catalog::load_catalog, transaction::Transaction, Table, TableIdentifier};
mod utils;
use tokio::runtime::Builder;
pub use utils::*;

use libtest_mimic::{Arguments, Trial};

pub struct PositionDeleteTest {
    docker_compose: DockerCompose,
    poetry: Poetry,
    catalog_configs: HashMap<String, String>,
    data_file_name: Option<String>,
    partition_value: Option<StructValue>,
}

impl PositionDeleteTest {
    pub fn new(docker_compose: DockerCompose, poetry: Poetry, catalog_type: &str) -> Self {
        let warehouse_root = "demo";
        let table_namespace = "s1";
        let table_name = "t1";
        let catalog_configs = match catalog_type {
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
                        docker_compose.get_container_ip("minio"),
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
                    format!("{}/{}/{}", warehouse_root, table_namespace, table_name,),
                ),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            _ => panic!("Unrecognized catalog type: {catalog_type}"),
        };
        Self {
            docker_compose,
            poetry,
            catalog_configs: catalog_configs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            data_file_name: None,
            partition_value: None,
        }
    }

    fn init(&self) {
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
            vlong long
        )  USING iceberg partitioned by(id)
        TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read');
            "
            .to_string(),
            "
        CREATE TABLE s1.t2
        (
            id long,
            vlong long
        ) USING iceberg partitioned by(id)
        TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read');
            "
            .to_string(),
            "
        CREATE TABLE s1.t3
        (
            id long,
            vlong long
        ) using iceberg partitioned by(id)
        TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read');
            "
            .to_string(),
            "
        INSERT INTO TABLE s1.t2 VALUES (1,1),(1,2),(1,3);
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

    pub async fn create_icelake_table(&self) -> Table {
        let catalog = load_catalog(&self.catalog_configs).await.unwrap();
        catalog
            .load_table(&TableIdentifier::new(vec!["s1", "t1"].into_iter()).unwrap())
            .await
            .unwrap()
    }

    pub async fn write_data_with_icelake(&mut self) {
        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let records = RecordBatch::try_new(
            Arc::new(
                table
                    .current_table_metadata()
                    .current_schema()
                    .unwrap()
                    .clone()
                    .try_into()
                    .unwrap(),
            ),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let mut task_writer = table.task_writer().await.unwrap();

        log::info!(
            "Insert record batch with {} records using icelake.",
            records.num_rows()
        );
        task_writer.write(&records).await.unwrap();

        let result = task_writer.close().await.unwrap();
        assert!(result.len() == 1);
        self.data_file_name = Some(result[0].file_path.clone());
        self.partition_value = Some(result[0].partition.clone());

        // Commit table transaction
        {
            let mut tx = Transaction::new(&mut table);
            tx.append_data_file(result);
            tx.commit().await.unwrap();
        }

        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q1",
                "select * from s1.t1",
                "-q2",
                "select * from s1.t2",
            ],
            "Check select * from s1.t1",
        )
    }

    async fn delete_data(&mut self) {
        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let mut writer = table
            .writer_builder()
            .await
            .unwrap()
            .build_sorted_position_delete_writer()
            .await
            .unwrap();

        writer
            .delete(self.data_file_name.clone().unwrap(), 0)
            .await
            .unwrap();

        let result = writer.close().await.unwrap().pop().unwrap();

        // Commit table transaction
        {
            let mut tx = Transaction::new(&mut table);
            tx.append_delete_file(vec![result
                .with_partition_value(self.partition_value.clone().unwrap())
                .build()]);
            tx.commit().await.unwrap();
        }
        self.poetry.run_file(
            "execute.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q",
                "delete from s1.t2 where vlong = 1;",
            ],
            "delete from s1.t2 where vlong = 1;",
        );
        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q1",
                "select * from s1.t1",
                "-q2",
                "select * from s1.t2",
            ],
            "Check select * from s1.t1",
        )
    }

    async fn run(mut self) {
        self.init();
        self.write_data_with_icelake().await;
        self.delete_data().await;
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

fn create_test_fixture(project_name: &str, catalog: &str) -> PositionDeleteTest {
    set_up();

    let docker_compose = match catalog {
        "storage" => DockerCompose::new(project_name, "iceberg-fs"),
        "rest" => DockerCompose::new(project_name, "iceberg-rest"),
        _ => panic!("Unrecognized catalog : {catalog}"),
    };
    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    docker_compose.run();

    PositionDeleteTest::new(docker_compose, poetry, catalog)
}

fn main() {
    // Parse command line arguments
    let args = Arguments::from_args();

    let catalogs = vec!["storage", "rest"];

    let mut tests = Vec::with_capacity(2);
    for catalog in &catalogs {
        let test_name = format!("{}_delete_position_test_{}", module_path!(), catalog);
        let fixture = create_test_fixture(&test_name, catalog);
        tests.push(Trial::test(test_name, move || {
            fixture.block_run();
            Ok(())
        }));
    }

    // Run all tests and exit the application appropriatly.
    libtest_mimic::run(&args, tests).exit();
}
