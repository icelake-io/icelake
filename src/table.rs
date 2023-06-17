use anyhow::anyhow;
use anyhow::Result;
use opendal::Operator;

/// Table is the main entry point for the IceLake.
pub struct Table {
    op: Operator,
}

impl Table {
    /// Create a new table via the given operator.
    pub fn new(op: Operator) -> Self {
        Self { op }
    }
}

impl Table {
    /// Read version hint of table.
    pub async fn version_hint(&self) -> Result<i32> {
        let content = self.op.read("metadata/version-hint.text").await?;
        let version_hint = String::from_utf8(content)?;

        version_hint
            .parse()
            .map_err(|e| anyhow!("parse version hint failed: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use opendal::{layers::LoggingLayer, services::Fs};

    use super::*;

    #[tokio::test]
    async fn test_table_version_hint() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let table = Table::new(op);

        let version_hint = table.version_hint().await?;

        assert_eq!(version_hint, 2);

        Ok(())
    }
}
