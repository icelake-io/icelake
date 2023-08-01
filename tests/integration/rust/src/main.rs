use arrow::record_batch::RecordBatch;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use clap::Parser;
use icelake::transaction::Transaction;
use icelake::Table;
use opendal::services::S3;
use opendal::Operator;
use std::fs::File;
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    s3_bucket: String,
    #[arg(long)]
    s3_endpoint: String,
    #[arg(long)]
    s3_username: String,
    #[arg(long)]
    s3_password: String,
    #[arg(long)]
    s3_region: String,

    #[arg(short, long)]
    table_path: String,
    #[arg(short, long)]
    csv_file: String,
}

struct TestFixture {
    args: Args,
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
        args: Args::parse(),
    }
}

async fn create_icelake_table(args: &Args) -> Table {
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

fn read_records_to_arrow(path: &str) -> Vec<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("distance", DataType::Int64, false),
    ]);

    let csv_reader = ReaderBuilder::new(Arc::new(schema))
        .has_header(false)
        .build(File::open(path).unwrap())
        .unwrap();

    csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>()
}

#[tokio::main]
async fn main() {
    let mut fixture = prepare_env().await;

    fixture.write_data_with_icelake().await;
}
