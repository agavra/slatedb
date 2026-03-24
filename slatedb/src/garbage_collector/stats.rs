use crate::metrics::{CounterFn, MetricsRecorder};
use std::sync::Arc;

/// Stats for the garbage collector.
pub(crate) struct GcStats {
    pub(crate) gc_manifest_count: Arc<dyn CounterFn>,
    pub(crate) gc_wal_count: Arc<dyn CounterFn>,
    pub(crate) gc_compacted_count: Arc<dyn CounterFn>,
    pub(crate) gc_compactions_count: Arc<dyn CounterFn>,
    pub(crate) gc_count: Arc<dyn CounterFn>,
}

impl GcStats {
    pub(crate) fn new(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            gc_manifest_count: recorder.register_counter(
                "slatedb.gc.deleted_count",
                "Number of GC deleted resources",
                &[("resource", "manifest")],
            ),
            gc_wal_count: recorder.register_counter(
                "slatedb.gc.deleted_count",
                "Number of GC deleted resources",
                &[("resource", "wal")],
            ),
            gc_compacted_count: recorder.register_counter(
                "slatedb.gc.deleted_count",
                "Number of GC deleted resources",
                &[("resource", "compacted")],
            ),
            gc_compactions_count: recorder.register_counter(
                "slatedb.gc.deleted_count",
                "Number of GC deleted resources",
                &[("resource", "compactions")],
            ),
            gc_count: recorder.register_counter("slatedb.gc.count", "Number of GC runs", &[]),
        }
    }
}
