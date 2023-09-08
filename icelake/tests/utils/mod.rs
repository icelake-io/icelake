use icelake::catalog::{
    Catalog, FileSystemCatalog, OperatorArgs, OP_ARGS_ACCESS_KEY, OP_ARGS_ACCESS_SECRET,
    OP_ARGS_BUCKET, OP_ARGS_ENDPOINT, OP_ARGS_REGION, OP_ARGS_ROOT,
};
use opendal::Scheme;
use testcontainers::{Container, GenericImage};

mod poetry;

pub use poetry::*;

mod containers;
mod docker;
mod test_generator;

pub use containers::*;
pub use docker::*;

use icelake::transaction::Transaction;
use icelake::Table;

use std::fs::File;
use std::process::Command;

use std::sync::{Arc, Once};

use self::test_generator::TestCase;

static INIT: Once = Once::new();

pub fn set_up() {
    INIT.call_once(env_logger::init);
}

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        log::info!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

pub fn get_cmd_output(mut cmd: Command, desc: impl ToString) -> String {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let output = cmd.output().unwrap();
    if output.status.success() {
        log::info!("{} succeed!", desc);
        String::from_utf8(output.stdout).unwrap()
    } else {
        panic!("{} failed: {:?}", desc, output.status);
    }
}

pub fn normalize_test_name(s: impl ToString) -> String {
    s.to_string().replace("::", "__")
}

pub struct TestFixture<'a> {
    spark: Container<'a, GenericImage>,
    minio: Container<'a, GenericImage>,

    poetry: Poetry,

    test_case: TestCase,
}

impl<'a> TestFixture<'a> {
    pub fn new(
        spark: Container<'a, GenericImage>,
        minio: Container<'a, GenericImage>,
        poetry: Poetry,
        toml_file: String,
    ) -> Self {
        let toml_file_path = format!(
            "{}/../testdata/toml/{}",
            env!("CARGO_MANIFEST_DIR"),
            toml_file
        );
        let test_case = TestCase::parse(File::open(toml_file_path).unwrap());
        Self {
            spark,
            minio,
            poetry,
            test_case,
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

    fn check_table_with_spark(&self) {
        for check_sqls in &self.test_case.query_sql {
            self.poetry.run_file(
                "check.py",
                [
                    "-s",
                    &self.spark_connect_url(),
                    "-q1",
                    check_sqls[0].as_str(),
                    "-q2",
                    check_sqls[1].as_str(),
                ],
                format!("Check {}", check_sqls[0].as_str()),
            )
        }
    }

    fn spark_connect_url(&self) -> String {
        format!(
            "sc://localhost:{}",
            self.spark.get_host_port_ipv4(SPARK_CONNECT_SERVER_PORT)
        )
    }

    pub async fn create_icelake_table(&self) -> Table {
        let op_args = OperatorArgs::builder(Scheme::S3)
            .with_arg(OP_ARGS_ROOT, self.test_case.warehouse_root.clone())
            .with_arg(OP_ARGS_BUCKET, "icebergdata")
            .with_arg(
                OP_ARGS_ENDPOINT,
                format!(
                    "http://localhost:{}",
                    self.minio.get_host_port_ipv4(MINIO_DATA_PORT)
                ),
            )
            .with_arg(OP_ARGS_REGION, "us-east-1")
            .with_arg(OP_ARGS_ACCESS_KEY, "admin")
            .with_arg(OP_ARGS_ACCESS_SECRET, "password")
            .build();

        let catalog = Arc::new(FileSystemCatalog::open(op_args).await.unwrap());

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

        let records = &self.test_case.write_date;

        let mut task_writer = table.task_writer().await.unwrap();

        for record_batch in records {
            log::info!(
                "Insert record batch with {} records using icelake.",
                record_batch.num_rows()
            );
            task_writer.write(record_batch).await.unwrap();
        }

        let result = task_writer.close().await.unwrap();
        log::debug!("Insert {} data files: {:?}", result.len(), result);

        // Commit table transaction
        {
            let mut tx = Transaction::new(&mut table);
            tx.append_file(result);
            tx.commit().await.unwrap();
        }
    }

    pub async fn run(mut self) {
        self.init_table_with_spark();
        self.write_data_with_icelake().await;
        self.check_table_with_spark();
    }
}
