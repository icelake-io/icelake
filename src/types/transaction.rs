use opendal::Operator;
use crate::types::{DataFile, ManifestFile, ManifestStatus, TableMetadata};
use crate::error::Result;
use crate::Table;

/// Operation of a transaction.
enum Operation {
    /// Append a new data file.
    AppendDataFile(DataFile)
}

pub struct Transaction<'a> {
    table: &'a Table,
    io: Operator,
    ops: Vec<Operation>,
}

struct TransactionStage<'a> {
    base: &'a TableMetadata,
    appended_files: Vec<ManifestFile>,
}

impl Transaction {
    /// Append a new data file.
    pub fn append_file(mut self, data_file: DataFile) -> Self {
        self.ops.push(Operation::AppendDataFile(data_file));
        self
    }

    pub async fn commit(self) -> Result<()> {
        let mut stage = TransactionStage {
            base: self.table.current_table_metadata()?,
            appended_files: Vec::new(),
        };

        for op in self.ops {
            op.apply(&mut stage)?;
        }

        unimplemented!()
    }
}

impl Operation {
    fn apply(self, tx_stage: &mut TransactionStage) -> Result<()> {
        match self {
            Operation::AppendDataFile(data_file) => {
                let manifest_file = ManifestFile {
                    status: ManifestStatus::Added,
                    data_file,
                    ..Default::default()
                };
                tx_stage.appended_files.push(manifest_file);
                Ok(())
            }
        }
    }
}

impl TransactionStage<'_> {
    fn execute(self) -> Result<TableMetadata> {

    }

    fn
}


