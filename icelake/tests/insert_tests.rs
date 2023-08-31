use crate::utils::{mc_image, minio_image, set_up, spark_image, Poetry, TestFixture};
use testcontainers::clients::Cli;

mod utils;

fn create_test_fixture<'a>(cli: &'a Cli, toml_file: &str) -> TestFixture<'a> {
    set_up();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    TestFixture::new(spark, minio, poetry, toml_file.to_string())
}

#[tokio::test]
async fn test_insert_no_partition() {
    let cli = Cli::default();
    create_test_fixture(&cli, "no_partition_test.toml")
        .run()
        .await
}

#[tokio::test]
async fn test_insert_partition_identity() {
    let cli = Cli::default();
    create_test_fixture(&cli, "partition_identity_test.toml")
        .run()
        .await
}

#[tokio::test]
async fn test_insert_partition_year() {
    let cli = Cli::default();
    create_test_fixture(&cli, "partition_year_test.toml")
        .run()
        .await
}

#[tokio::test]
async fn test_insert_partition_month() {
    let cli = Cli::default();
    create_test_fixture(&cli, "partition_month_test.toml")
        .run()
        .await
}

#[tokio::test]
async fn test_insert_partition_day() {
    let cli = Cli::default();
    create_test_fixture(&cli, "partition_day_test.toml")
        .run()
        .await
}

#[tokio::test]
async fn test_insert_partition_hour() {
    let cli = Cli::default();
    create_test_fixture(&cli, "partition_hour_test.toml")
        .run()
        .await
}
