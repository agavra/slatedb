use crate::metrics::{CounterFn, GaugeFn, MetricsRecorder};
use std::sync::Arc;

#[derive(Clone)]
pub struct CachedObjectStoreStats {
    pub(super) object_store_cache_part_hits: Arc<dyn CounterFn>,
    pub(super) object_store_cache_part_access: Arc<dyn CounterFn>,
    pub(super) object_store_cache_keys: Arc<dyn GaugeFn>,
    pub(super) object_store_cache_bytes: Arc<dyn GaugeFn>,
    pub(super) object_store_cache_evicted_keys: Arc<dyn CounterFn>,
    pub(super) object_store_cache_evicted_bytes: Arc<dyn CounterFn>,
}

impl CachedObjectStoreStats {
    pub fn new(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            object_store_cache_part_hits: recorder.register_counter(
                "slatedb.object_store_cache.part_hit_count",
                "Number of object store cache part hits",
                &[],
            ),
            object_store_cache_part_access: recorder.register_counter(
                "slatedb.object_store_cache.part_access_count",
                "Number of object store cache part accesses",
                &[],
            ),
            object_store_cache_keys: recorder.register_gauge(
                "slatedb.object_store_cache.cache_keys",
                "Number of keys in object store cache",
                &[],
            ),
            object_store_cache_bytes: recorder.register_gauge(
                "slatedb.object_store_cache.cache_bytes",
                "Number of bytes in object store cache",
                &[],
            ),
            object_store_cache_evicted_keys: recorder.register_counter(
                "slatedb.object_store_cache.evicted_keys",
                "Number of evicted keys from object store cache",
                &[],
            ),
            object_store_cache_evicted_bytes: recorder.register_counter(
                "slatedb.object_store_cache.evicted_bytes",
                "Number of evicted bytes from object store cache",
                &[],
            ),
        }
    }
}

impl std::fmt::Debug for CachedObjectStoreStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedObjectStoreStats").finish()
    }
}
