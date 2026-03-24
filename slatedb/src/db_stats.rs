use crate::metrics::{CounterFn, GaugeFn, MetricsRecorder};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_estimated_bytes: Arc<dyn GaugeFn>,
    pub(crate) wal_buffer_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_flush_requests: Arc<dyn CounterFn>,
    pub(crate) sst_filter_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_negatives: Arc<dyn CounterFn>,
    pub(crate) backpressure_count: Arc<dyn CounterFn>,
    pub(crate) get_requests: Arc<dyn CounterFn>,
    pub(crate) scan_requests: Arc<dyn CounterFn>,
    pub(crate) flush_requests: Arc<dyn CounterFn>,
    pub(crate) write_batch_count: Arc<dyn CounterFn>,
    pub(crate) write_ops: Arc<dyn CounterFn>,
    pub(crate) total_mem_size_bytes: Arc<dyn GaugeFn>,
    pub(crate) l0_sst_count: Arc<dyn GaugeFn>,
}

impl DbStats {
    pub(crate) fn new(recorder: &dyn MetricsRecorder) -> DbStats {
        Self {
            immutable_memtable_flushes: recorder.register_counter(
                "slatedb.db.immutable_memtable_flushes",
                "Number of immutable memtable flushes",
                &[],
            ),
            wal_buffer_estimated_bytes: recorder.register_gauge(
                "slatedb.db.wal_buffer_estimated_bytes",
                "Estimated WAL buffer size in bytes",
                &[],
            ),
            wal_buffer_flushes: recorder.register_counter(
                "slatedb.db.wal_buffer_flushes",
                "Number of WAL buffer flushes",
                &[],
            ),
            wal_buffer_flush_requests: recorder.register_counter(
                "slatedb.db.wal_buffer_flush_requests",
                "Number of WAL buffer flush requests",
                &[],
            ),
            sst_filter_false_positives: recorder.register_counter(
                "slatedb.db.sst_filter_false_positive_count",
                "Number of SST filter false positives",
                &[],
            ),
            sst_filter_positives: recorder.register_counter(
                "slatedb.db.sst_filter_positive_count",
                "Number of SST filter positives",
                &[],
            ),
            sst_filter_negatives: recorder.register_counter(
                "slatedb.db.sst_filter_negative_count",
                "Number of SST filter negatives",
                &[],
            ),
            backpressure_count: recorder.register_counter(
                "slatedb.db.backpressure_count",
                "Number of backpressure events",
                &[],
            ),
            get_requests: recorder.register_counter(
                "slatedb.db.request_count",
                "Number of DB requests",
                &[("op", "get")],
            ),
            scan_requests: recorder.register_counter(
                "slatedb.db.request_count",
                "Number of DB requests",
                &[("op", "scan")],
            ),
            flush_requests: recorder.register_counter(
                "slatedb.db.request_count",
                "Number of DB requests",
                &[("op", "flush")],
            ),
            write_batch_count: recorder.register_counter(
                "slatedb.db.write_batch_count",
                "Number of write batches",
                &[],
            ),
            write_ops: recorder.register_counter(
                "slatedb.db.write_ops",
                "Number of write operations",
                &[],
            ),
            total_mem_size_bytes: recorder.register_gauge(
                "slatedb.db.total_mem_size_bytes",
                "Total memory size in bytes",
                &[],
            ),
            l0_sst_count: recorder.register_gauge(
                "slatedb.db.l0_sst_count",
                "Number of L0 SSTs",
                &[],
            ),
        }
    }
}
