use crate::utils::{
    mc_image, minio_image, set_up, spark_image, Poetry, TestFixture,
    MINIO_DATA_PORT,
};
use testcontainers::clients::Cli;
use testcontainers::RunnableImage;

mod utils;

#[tokio::test]
async fn test_insert_no_partition() {
    set_up();

    let cli = Cli::default();

    let minio = {
        let img = RunnableImage::from(minio_image());
        cli.run(img)
    };

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = {
        let img = RunnableImage::from(spark_image(&minio_ip_addr)).with_user("root");
        cli.run(img)
    };

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name: "t1".to_string(),
        csv_file: "data.csv".to_string(),
        table_root: "demo/s1/t1".to_string(),
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition() {
    set_up();

    let cli = Cli::default();

    let minio = {
        let img = RunnableImage::from(minio_image());
        cli.run(img)
    };

    log::debug!("Minio ports: {:?}", minio.ports());
    let mapped_minio_data_port = minio.get_host_port_ipv4(MINIO_DATA_PORT);
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = {
        let img = RunnableImage::from(spark_image(&minio_ip_addr)).with_user("root");
        cli.run(img)
    };

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name: "t2".to_string(),
        csv_file: "partition_data.csv".to_string(),
        table_root: "demo/s1/t2".to_string(),
    };

    test_fixture.run().await
}
