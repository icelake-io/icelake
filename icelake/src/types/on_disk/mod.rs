//! on_disk module provides the definition of iceberg on-disk data
//! formats and the convert functions to in-memory.

mod manifest_file;
pub(crate) use manifest_file::ManifestWriter;
pub use manifest_file::{data_file_from_json, data_file_to_json, parse_manifest_file};

mod manifest_list;
pub use manifest_list::parse_manifest_list;
pub(crate) use manifest_list::ManifestListWriter;

mod partition_spec;
pub use partition_spec::parse_partition_spec;

mod schema;
pub use schema::parse_schema;
pub(crate) use schema::Schema as SchemaSerDe;

mod sort_order;
pub use sort_order::parse_sort_order;

mod transform;

mod snapshot;
pub use snapshot::parse_snapshot;
pub(crate) use snapshot::Snapshot as SnapshotSerDe;

mod table_metadata;
pub use table_metadata::parse_table_metadata;
pub use table_metadata::serialize_table_meta;
pub(crate) use table_metadata::TableMetadata as TableMetadataSerDe;

mod types;

mod value;
