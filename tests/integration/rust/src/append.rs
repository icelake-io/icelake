//! This module contains append only tests.

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use confique::Config;
use icelake::transaction::Transaction;
use icelake::Table;
use opendal::services::S3;
use opendal::Operator;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tokio::runtime::Builder;

#[derive(Config, Debug)]
struct TestConfig {
    s3_bucket: String,
    s3_endpoint: String,
    s3_username: String,
    s3_password: String,
    s3_region: String,

    table_path: String,
    csv_file: String,

    spark_url: String,
}

struct TestFixture {
    args: TestConfig,
}

impl TestFixture {
    async fn write_data_with_icelake(&mut self) {
        let mut table = create_icelake_table(&self.args).await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let records = read_records_to_arrow(self.args.csv_file.as_str());

        let mut task_writer = table.task_writer().await.unwrap();

        for record_batch in &records {
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
}

async fn prepare_env() -> TestFixture {
    env_logger::init();

    TestFixture {
        args: Config::from_file(path_of("../testdata/config.toml")).unwrap(),
    }
}

async fn create_icelake_table(args: &TestConfig) -> Table {
    let mut builder = S3::default();
    builder.root(args.table_path.as_str());
    builder.bucket(args.s3_bucket.as_str());
    builder.endpoint(args.s3_endpoint.as_str());
    builder.access_key_id(args.s3_username.as_str());
    builder.secret_access_key(args.s3_password.as_str());
    builder.region(args.s3_region.as_str());

    let op = Operator::new(builder).unwrap().finish();
    Table::open_with_op(op).await.unwrap()
}

fn read_records_to_arrow(filename: &str) -> Vec<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("distance", DataType::Int64, false),
    ]);

    let csv_reader = ReaderBuilder::new(Arc::new(schema))
        .has_header(false)
        .build(File::open(path_of(format!("../testdata/{}", filename))).unwrap())
        .unwrap();

    csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>()
}

fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        log::info!("{} succeed!", desc.to_string())
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

fn run_poetry_update() {
    let mut cmd = Command::new("poetry");
    cmd.arg("update").current_dir(path_of("../python"));

    run_command(cmd, "poetry update")
}

fn path_of<P: AsRef<Path>>(relative_path: P) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(relative_path)
        .to_str()
        .unwrap()
        .to_string()
}

fn start_docker_compose() {
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "up", "-d", "--wait", "spark"])
        .current_dir(path_of("../docker"));

    run_command(cmd, "start docker compose");
}

fn init_iceberg_table_with_spark(config: &TestConfig) {
    let mut cmd = Command::new("poetry");
    cmd.args([
        "run",
        "python",
        "init.py",
        "-s",
        config.spark_url.as_str(),
        "-f",
        path_of("../testdata/insert1.csv").as_str(),
    ])
    .current_dir(path_of("../python"));

    run_command(cmd, "init iceberg table")
}

fn check_iceberg_table_with_spark(config: &TestConfig) {
    let mut cmd = Command::new("poetry");
    cmd.args([
        "run",
        "python",
        "check.py",
        "-s",
        config.spark_url.as_str(),
        "-f",
        path_of("../testdata/query1.csv").as_str(),
    ])
    .current_dir(path_of("../python"));

    run_command(cmd, "check iceberg table")
}

fn shutdown_docker_compose() {
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "down", "-v", "--remove-orphans"])
        .current_dir(path_of("../docker"));

    run_command(cmd, "shutdown docker compose");
}

async fn do_test_append_data() {
    let mut fixture = prepare_env().await;
    start_docker_compose();

    run_poetry_update();
    init_iceberg_table_with_spark(&fixture.args);

    fixture.write_data_with_icelake().await;

    check_iceberg_table_with_spark(&fixture.args);
    shutdown_docker_compose();
}

pub fn test_append_data() {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();
    rt.block_on(async { do_test_append_data().await });
}
