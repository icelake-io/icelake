use anyhow::Result;
use icelake::Table;
use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let table_uri = format!(
        "{}/testdata/simple_table",
        env::current_dir()
            .expect("current_dir must exist")
            .to_string_lossy()
    );

    let table = Table::open(table_uri.as_str()).await?;
    println!("{:?}", table.current_table_metadata());
    Ok(())
}
