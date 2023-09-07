use std::env;

use anyhow::Result;
use icelake::catalog::FileSystemCatalog;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let table_uri = format!("{}/../testdata/simple_table", env!("CARGO_MANIFEST_DIR"));

    let table = FileSystemCatalog::load_table(table_uri.as_str()).await?;
    println!("{:?}", table.current_table_metadata());
    Ok(())
}
