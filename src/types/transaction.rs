use std::time::{Instant, SystemTime, UNIX_EPOCH};
use crate::error::Result;
use crate::types::{DataFile, DataFileFormat, ManifestContentType, ManifestEntry, ManifestFile, ManifestList, ManifestListWriter, ManifestMetadata, ManifestStatus, ManifestWriter, TableMetadata};
use crate::Table;
use opendal::Operator;
use uuid::Uuid;

/// Operation of a transaction.
enum Operation {
    /// Append a new data file.
    AppendDataFile(DataFile),
}

pub struct Transaction<'a> {
    // Uuid of this transaction
    uuid: Uuid,
    // Number of manifest files
    manifest_num: u32,
    // Attemp num
    attempt: u32,
    table: &'a Table,
    io: Operator,
    ops: Vec<Operation>,
}

impl Transaction {
    /// Append a new data file.
    pub fn append_file(mut self, data_file: DataFile) -> Self {
        self.ops.push(Operation::AppendDataFile(data_file));
        self
    }

    /// Commit this transaction.
    ///
    /// Currently implementation only supports add data file. We should refactor it to be more
    /// general.
    pub async fn commit(mut self) -> Result<()> {
        let cur_metadata = self.table.current_table_metadata();

        let next_snapshot_id = cur_metadata.current_snapshot_id.unwrap_or(0) + 1;
        let next_seq_number = cur_metadata.last_sequence_number + 1;

        let mut manifest_entries: Vec<ManifestEntry> = Vec::with_capacity(self.ops.len());

        for op in self.ops {
            match op {
                Operation::AppendDataFile(data_file) => {
                    let manifest_entry = ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: Some(next_snapshot_id),
                        sequence_number: Some(next_seq_number),
                        file_sequence_number: Some(next_seq_number),
                        data_file,
                    };
                    manifest_entries.push(manifest_entry);
                }
            }
        }

        let manifest_list_path = {
            // Writing manifest file
            let mut writer = ManifestWriter::new(cur_metadata.current_partition_spec()?.clone(),
                                self.table.operator(),
                                self.next_manifest_path(), next_snapshot_id);
            let manifest_file = ManifestFile {
                metadata: ManifestMetadata {
                    schema: cur_metadata.current_schema()?.clone(),
                    schema_id: cur_metadata.current_schema_id,
                    partition_spec_id: cur_metadata.default_spec_id,
                    format_version: Some(cur_metadata.format_version),
                    content: ManifestContentType::Data
                },
                entries: manifest_entries
            };
            let manifest_list_entry = writer.write(manifest_file).await?;


            // Load existing manifest list
            let mut manifest_list = cur_metadata.current_snapshot()?.load_manifest_list(&self.table.operator())?;
            manifest_list.push(manifest_list_entry);


            let manifest_list_path = self.manifest_list_path(next_snapshot_id);
            // Writing manifest list
            ManifestListWriter::new(self.table.operator(), manifest_list_path.clone())
                .write(manifest_list).await?;

            manifest_list_path
        };

        let new_snapshot = {
            let cur_snapshot = cur_metadata.current_snapshot()?;
            let mut new_snapshot = cur_snapshot.clone();
            new_snapshot.snapshot_id = next_snapshot_id;
            new_snapshot.parent_snapshot_id = Some(cur_snapshot.snapshot_id);
            new_snapshot.sequence_number = next_seq_number;
            new_snapshot.timestamp_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
            new_snapshot.manifest_list = manifest_list_path;

            // TODO: Add operations
            new_snapshot
        };

    }

    fn next_manifest_path(&mut self) -> String {
        self.manifest_num += 1;
        Table::metadata_file_path(format!("{}-m{}.{}", self.uuid, self.manifest_num, DataFileFormat::Avro.to_string()))
    }

    fn manifest_list_path(&mut self, snapshot_id: i64) -> String {
        self.attempt += 1;
        Table::metadata_file_path(format!("snap-{}-{}-{}", snapshot_id, self.attempt, self.uuid))
    }
}
