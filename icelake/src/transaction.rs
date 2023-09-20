//! Transaction for manipulating table.

use crate::catalog::{MetadataUpdate, UpdateTable};
use crate::error::Result;
use crate::types::{
    DataFile, DataFileFormat, ManifestContentType, ManifestEntry, ManifestFile, ManifestList,
    ManifestListEntry, ManifestListWriter, ManifestMetadata, ManifestStatus, ManifestWriter,
    Snapshot, SnapshotReferenceType, TableMetadata, MAIN_BRANCH,
};
use crate::Table;
use opendal::Operator;
use std::mem::swap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Operation of a transaction.
enum Operation {
    /// Append a new data file.
    AppendDataFile(DataFile),
    AppendDeleteFile(DataFile),
}

struct CommitContext {
    // Uuid of this transaction
    uuid: Uuid,
    // Number of manifest files
    manifest_num: u32,
    // Attemp num
    attempt: u32,

    // Table io
    io: Operator,
}

/// A transaction manipulate iceberg table.
pub struct Transaction<'a> {
    table: &'a mut Table,

    // Transaction operations
    ops: Vec<Operation>,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table, ops: vec![] }
    }

    /// Append new data files.
    pub fn append_data_file(&mut self, data_file: impl IntoIterator<Item = DataFile>) {
        self.ops
            .extend(data_file.into_iter().map(Operation::AppendDataFile));
    }

    /// Append new delete files.
    pub fn append_delete_file(&mut self, delete_file: impl IntoIterator<Item = DataFile>) {
        self.ops
            .extend(delete_file.into_iter().map(Operation::AppendDeleteFile));
    }

    /// Commit this transaction.
    ///
    /// Currently implementation only supports add data file. We should refactor it to be more
    /// general.
    pub async fn commit(self) -> Result<()> {
        let table = self.table;
        let commit_ctx = CommitContext {
            uuid: Uuid::new_v4(),
            manifest_num: 0,
            attempt: 0,
            io: table.operator(),
        };

        let current_snapshot_ref = table.current_table_metadata().snapshot_ref(MAIN_BRANCH);
        let new_snapshot = Transaction::produce_new_snapshot(commit_ctx, self.ops, table).await?;
        let new_snapshot_id = new_snapshot.snapshot_id;

        let table_update = {
            let mut builder = UpdateTable::builder(table.table_name().clone());
            builder.add_updates(vec![
                MetadataUpdate::AddSnapshot {
                    snapshot: new_snapshot,
                },
                MetadataUpdate::SetSnapshotRef {
                    snapshot_id: new_snapshot_id,
                    ref_name: MAIN_BRANCH.to_string(),
                    typ: SnapshotReferenceType::Branch,
                    min_snapshots_to_keep: current_snapshot_ref
                        .and_then(|f| f.min_snapshots_to_keep),
                    max_snapshot_ages: current_snapshot_ref.and_then(|f| f.max_snapshot_age_ms),
                    max_ref_ages: current_snapshot_ref.and_then(|f| f.max_ref_age_ms),
                },
            ]);
            builder.build()
        };

        // Save new metadata
        swap(
            table,
            &mut table.catalog().update_table(&table_update).await?,
        );
        Ok(())
    }

    fn next_manifest_path(ctx: &mut CommitContext) -> String {
        ctx.manifest_num += 1;
        Table::metadata_path(format!(
            "{}-m{}.{}",
            &ctx.uuid,
            ctx.manifest_num,
            DataFileFormat::Avro.to_string()
        ))
    }

    fn manifest_list_path(ctx: &mut CommitContext, snapshot_id: i64) -> String {
        ctx.attempt += 1;
        Table::metadata_path(format!(
            "snap-{}-{}-{}.{}",
            snapshot_id,
            ctx.attempt,
            &ctx.uuid,
            DataFileFormat::Avro.to_string()
        ))
    }

    async fn produce_new_manifest_list_entry(
        manifest_entries: Vec<ManifestEntry>,
        content: ManifestContentType,
        table: &Table,
        cur_metadata: &TableMetadata,
        ctx: &mut CommitContext,
        next_snapshot_id: i64,
        next_seq_number: i64,
    ) -> Result<ManifestListEntry> {
        // Writing manifest file for data file
        let writer = ManifestWriter::new(
            cur_metadata.current_partition_spec()?.clone(),
            table.operator(),
            cur_metadata.location.as_str(),
            Transaction::next_manifest_path(ctx),
            next_snapshot_id,
            next_seq_number,
        );
        let manifest_file = ManifestFile {
            metadata: ManifestMetadata {
                schema: cur_metadata.current_schema()?.clone(),
                schema_id: cur_metadata.current_schema_id,
                partition_spec_id: cur_metadata.default_spec_id,
                format_version: Some(cur_metadata.format_version),
                content,
            },
            entries: manifest_entries,
        };
        writer.write(manifest_file).await
    }

    async fn produce_new_snapshot(
        mut ctx: CommitContext,
        ops: Vec<Operation>,
        table: &Table,
    ) -> Result<Snapshot> {
        let cur_metadata = table.current_table_metadata();
        let cur_snapshot_id = cur_metadata.current_snapshot_id.unwrap_or(0);
        let next_snapshot_id = cur_snapshot_id + 1;
        let next_seq_number = cur_metadata.last_sequence_number + 1;

        let mut data_manifest_entries: Vec<ManifestEntry> = Vec::with_capacity(ops.len());
        let mut delete_manifest_entries: Vec<ManifestEntry> = Vec::with_capacity(ops.len());

        for op in ops {
            match op {
                Operation::AppendDataFile(data_file) => {
                    let manifest_entry = ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: Some(next_snapshot_id),
                        sequence_number: Some(next_seq_number),
                        file_sequence_number: Some(next_seq_number),
                        data_file,
                    };
                    data_manifest_entries.push(manifest_entry);
                }
                Operation::AppendDeleteFile(data_file) => {
                    let manifest_entry = ManifestEntry {
                        status: ManifestStatus::Added,
                        snapshot_id: Some(next_snapshot_id),
                        sequence_number: Some(next_seq_number),
                        file_sequence_number: Some(next_seq_number),
                        data_file,
                    };
                    delete_manifest_entries.push(manifest_entry);
                }
            }
        }

        let manifest_list_path = {
            let data_manifest_list_entry = Self::produce_new_manifest_list_entry(
                data_manifest_entries,
                ManifestContentType::Data,
                table,
                cur_metadata,
                &mut ctx,
                next_snapshot_id,
                next_seq_number,
            )
            .await?;
            let delete_manifest_list_entry = if delete_manifest_entries.is_empty() {
                None
            } else {
                Some(
                    Self::produce_new_manifest_list_entry(
                        delete_manifest_entries,
                        ManifestContentType::Deletes,
                        table,
                        cur_metadata,
                        &mut ctx,
                        next_snapshot_id,
                        next_seq_number,
                    )
                    .await?,
                )
            };
            // Load existing manifest list
            let manifest_list = match cur_metadata.current_snapshot()? {
                Some(s) => {
                    let mut ret = s.load_manifest_list(&ctx.io).await?;
                    ret.entries.push(data_manifest_list_entry);
                    if let Some(delete_manifest_list_entry) = delete_manifest_list_entry {
                        ret.entries.push(delete_manifest_list_entry);
                    }
                    ret
                }
                None => {
                    let mut entries = vec![data_manifest_list_entry];
                    if let Some(delete_manifest_list_entry) = delete_manifest_list_entry {
                        entries.push(delete_manifest_list_entry);
                    }
                    ManifestList { entries }
                }
            };

            let manifest_list_path = Transaction::manifest_list_path(&mut ctx, next_snapshot_id);
            // Writing manifest list
            ManifestListWriter::new(
                ctx.io.clone(),
                manifest_list_path.clone(),
                next_snapshot_id,
                cur_snapshot_id,
                next_snapshot_id,
            )
            .write(manifest_list)
            .await?;

            // Absolute path stored in snapshot file
            format!("{}/{manifest_list_path}", cur_metadata.location)
        };

        let mut new_snapshot = match cur_metadata.current_snapshot()? {
            Some(cur_snapshot) => {
                let mut new_snapshot = cur_snapshot.clone();
                new_snapshot.parent_snapshot_id = Some(cur_snapshot.snapshot_id);
                new_snapshot
            }
            None => Snapshot::default(),
        };
        new_snapshot.snapshot_id = next_snapshot_id;
        new_snapshot.sequence_number = next_seq_number;
        new_snapshot.timestamp_ms =
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        new_snapshot.manifest_list = manifest_list_path;
        new_snapshot.schema_id = Some(cur_metadata.current_schema_id as i64);

        // TODO: Add operations
        Ok(new_snapshot)
    }
}
