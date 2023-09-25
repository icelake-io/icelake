use std::env;

use anyhow::Result;
use icelake::catalog::{IcebergStorageCatalog};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let table_uri = format!("{}/../testdata/simple_table", env!("CARGO_MANIFEST_DIR"));

    let table = IcebergStorageCatalog::load_table(table_uri.as_str()).await?;
    println!("{:?}", table.current_table_metadata());
    Ok(())
}
