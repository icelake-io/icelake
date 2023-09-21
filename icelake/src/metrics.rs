//! This module contains metrics def.

use prometheus::{
    core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge},
    histogram_opts, opts, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, Histogram,
    HistogramVec, IntCounterVec, IntGaugeVec, Registry, DEFAULT_BUCKETS,
};

use crate::io::WriterPrometheusLayer;

#[derive(Clone)]
pub(crate) struct PosDeleteWriterMetricsDef {
    record_num: IntGaugeVec,
    delete_qps: IntCounterVec,
    flush_qps: IntCounterVec,
}

pub(crate) struct PosDeleteWriterMetrics {
    pub(crate) record_num: GenericGauge<AtomicI64>,
    pub(crate) delete_qps: GenericCounter<AtomicU64>,
    pub(crate) flush_qps: GenericCounter<AtomicU64>,
}

impl PosDeleteWriterMetricsDef {
    fn new(registry: &Registry) -> Self {
        let record_num = register_int_gauge_vec_with_registry!(
            opts!(
                "iceberg_pos_delete_record_num",
                "The number of in memory position delete record number",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let delete_qps = register_int_counter_vec_with_registry!(
            opts!("iceberg_pos_delete_qps", "Iceberg position delete qps",),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let flush_qps = register_int_counter_vec_with_registry!(
            opts!(
                "iceberg_pos_delete_flush_qps",
                "Iceberg position delete flush qps",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        Self {
            record_num,
            delete_qps,
            flush_qps,
        }
    }
}

#[derive(Clone)]
pub(crate) struct EqDeleteMetricsDef {
    eq_delete_qps: IntCounterVec,
}

impl EqDeleteMetricsDef {
    pub fn new(registry: &Registry) -> Self {
        let eq_delete_qps = register_int_counter_vec_with_registry!(
            opts!("iceberg_eq_delete_qps", "Iceberg equality delete qps",),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        Self { eq_delete_qps }
    }
}

pub(crate) struct EqDeleteMetrics {
    pub(crate) eq_delete_qps: GenericCounter<AtomicU64>,
}

#[derive(Clone)]
pub(crate) struct PartitionedWriterMetricsDef {
    writers_num: IntGaugeVec,
}

pub(crate) struct PartitionedWriterMetrics {
    pub(crate) writers_num: GenericGauge<AtomicI64>,
}

impl PartitionedWriterMetricsDef {
    fn new(registry: &Registry) -> Self {
        let writers_num = register_int_gauge_vec_with_registry!(
            opts!(
                "iceberg_append_only_partitioned_writer_num",
                "The number of data file writers in partitioned writer",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        Self { writers_num }
    }
}

#[derive(Clone)]
pub(crate) struct EqDeltaWriterMetricsDef {
    insert_row_num: IntGaugeVec,
    insert_qps: IntCounterVec,
    delete_qps: IntCounterVec,
}

impl EqDeltaWriterMetricsDef {
    fn new(registry: &Registry) -> Self {
        let inserted_row_num = register_int_gauge_vec_with_registry!(
            opts!(
                "iceberg_eq_delta_inserted_row_num",
                "The number of inserted row in equality delta writer",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let insert_qps = register_int_counter_vec_with_registry!(
            opts!(
                "iceberg_eq_delta_insert_qps",
                "The insert qps of equality delta writer",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        let delete_qps = register_int_counter_vec_with_registry!(
            opts!(
                "iceberg_eq_delta_delete_qps",
                "The delete qps of equality delta writer",
            ),
            WRITER_METRICS_LABEL_NAMES,
            registry,
        )
        .unwrap();

        Self {
            insert_row_num: inserted_row_num,
            insert_qps,
            delete_qps,
        }
    }
}

pub(crate) struct EqDeltaWriterMetrics {
    pub(crate) insert_row_num: GenericGauge<AtomicI64>,
    pub(crate) insert_qps: GenericCounter<AtomicU64>,
    pub(crate) delete_qps: GenericCounter<AtomicU64>,
}

impl WriterPrometheusLayer {
    pub(crate) fn pos_delete_writer_metrics(&self) -> PosDeleteWriterMetrics {
        let label_values = [
            self.context_id.as_str(),
            self.catalog_name(),
            self.table_name(),
        ];

        let def = self.catalog_context().metrics_def();
        PosDeleteWriterMetrics {
            record_num: def
                .pos_delete_writer_metrics_def
                .record_num
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            delete_qps: def
                .pos_delete_writer_metrics_def
                .delete_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            flush_qps: def
                .pos_delete_writer_metrics_def
                .flush_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }

    pub(crate) fn eq_delete_metrics(&self) -> EqDeleteMetrics {
        let label_values = [
            self.catalog_context().desc(),
            self.catalog_name(),
            self.table_name(),
        ];

        let def = &self.catalog_context().metrics_def().eq_delete_metrics_def;

        EqDeleteMetrics {
            eq_delete_qps: def
                .eq_delete_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }

    pub(crate) fn partitioned_writer_metrics(&self) -> PartitionedWriterMetrics {
        let label_values = [
            self.catalog_context().desc(),
            self.catalog_name(),
            self.table_name(),
        ];

        let def = &self
            .catalog_context()
            .metrics_def()
            .partitioned_writer_metrics_def;

        PartitionedWriterMetrics {
            writers_num: def
                .writers_num
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }

    pub(crate) fn eq_delta_writer_metrics(&self) -> EqDeltaWriterMetrics {
        let label_values = [
            self.catalog_context().desc(),
            self.catalog_name(),
            self.table_name(),
        ];

        let def = &self
            .catalog_context()
            .metrics_def()
            .eq_delta_writer_metrics_def;

        EqDeltaWriterMetrics {
            insert_row_num: def
                .insert_row_num
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            insert_qps: def
                .insert_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
            delete_qps: def
                .delete_qps
                .get_metric_with_label_values(&label_values)
                .unwrap(),
        }
    }
}

/// All metrics def.
#[derive(Clone)]
pub(crate) struct MetricsDef {
    pub(crate) catalog_metrics_def: CatalogMetricsDef,
    pub(crate) file_appender_metrics_def: FileAppenderMetricsDef,
    pub(crate) pos_delete_writer_metrics_def: PosDeleteWriterMetricsDef,
    pub(crate) eq_delete_metrics_def: EqDeleteMetricsDef,
    pub(crate) partitioned_writer_metrics_def: PartitionedWriterMetricsDef,
    pub(crate) eq_delta_writer_metrics_def: EqDeltaWriterMetricsDef,
}

impl MetricsDef {
    pub(crate) fn new(registry: &Registry) -> Self {
        Self {
            catalog_metrics_def: CatalogMetricsDef::new(registry),
            file_appender_metrics_def: FileAppenderMetricsDef::new(registry),
            pos_delete_writer_metrics_def: PosDeleteWriterMetricsDef::new(registry),
            eq_delete_metrics_def: EqDeleteMetricsDef::new(registry),
            partitioned_writer_metrics_def: PartitionedWriterMetricsDef::new(registry),
            eq_delta_writer_metrics_def: EqDeltaWriterMetricsDef::new(registry),
        }
    }
}
