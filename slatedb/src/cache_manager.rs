use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use log::{debug, warn};
use tokio::sync::{oneshot, watch};

use crate::bytes_range::BytesRange;
use crate::db_state::{ManifestCore, SsTableHandle, SsTableId};
use crate::dispatcher::MessageHandler;
use crate::error::SlateDBError;
use crate::partitioned_keyspace;
use crate::tablestore::TableStore;
use crate::DbStatus;

pub(crate) const CACHE_MANAGER_TASK_NAME: &str = "cache_manager";

pub(crate) enum CacheManagerMessage {
    ManifestChanged,
    SetWarmRanges {
        ranges: Vec<BytesRange>,
        reply: oneshot::Sender<Result<(), SlateDBError>>,
    },
    WarmCurrent {
        reply: oneshot::Sender<Result<(), SlateDBError>>,
    },
    WarmSsts {
        sst_ids: Vec<SsTableId>,
        reply: oneshot::Sender<Result<(), SlateDBError>>,
    },
}

impl std::fmt::Debug for CacheManagerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ManifestChanged => write!(f, "ManifestChanged"),
            Self::SetWarmRanges { ranges, .. } => f
                .debug_struct("SetWarmRanges")
                .field("ranges", &ranges.len())
                .finish(),
            Self::WarmCurrent { .. } => write!(f, "WarmCurrent"),
            Self::WarmSsts { sst_ids, .. } => f
                .debug_struct("WarmSsts")
                .field("sst_ids", &sst_ids.len())
                .finish(),
        }
    }
}

/// Handle for interacting with the background cache manager.
///
/// Obtained via [`crate::Db::cache_manager`]. Use this to configure
/// warm ranges at runtime and to trigger manual warming on demand.
#[derive(Clone)]
pub struct CacheManager {
    tx: async_channel::Sender<CacheManagerMessage>,
}

impl CacheManager {
    pub(crate) fn new(tx: async_channel::Sender<CacheManagerMessage>) -> Self {
        Self { tx }
    }

    /// Replaces the subscribed warm set used for future manifest
    /// changes. Passing an empty vec clears the warm set. This call
    /// does not warm the current manifest.
    pub async fn set_warm_ranges<K, T>(&self, ranges: Vec<T>) -> Result<(), crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let bytes_ranges: Vec<BytesRange> = ranges.into_iter().map(BytesRange::from_ref).collect();
        self.send_and_await(|reply| CacheManagerMessage::SetWarmRanges {
            ranges: bytes_ranges,
            reply,
        })
        .await
    }

    /// Convenience helper for prefix-based warm sets.
    /// Passing an empty vec clears the warm set.
    pub async fn set_warm_prefixes<P>(&self, prefixes: Vec<P>) -> Result<(), crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        let bytes_ranges: Vec<BytesRange> = prefixes
            .into_iter()
            .map(|p| BytesRange::from_prefix(p.as_ref()))
            .collect();
        self.send_and_await(|reply| CacheManagerMessage::SetWarmRanges {
            ranges: bytes_ranges,
            reply,
        })
        .await
    }

    /// Warms the current manifest using the warm set active when this
    /// request is processed.
    pub async fn warm_current(&self) -> Result<(), crate::Error> {
        self.send_and_await(|reply| CacheManagerMessage::WarmCurrent { reply })
            .await
    }

    /// Warms the subset of currently reachable SSTs whose physical IDs
    /// match `sst_ids`, using the warm set active when this request is
    /// processed.
    pub async fn warm_ssts(&self, sst_ids: &[SsTableId]) -> Result<(), crate::Error> {
        self.send_and_await(|reply| CacheManagerMessage::WarmSsts {
            sst_ids: sst_ids.to_vec(),
            reply,
        })
        .await
    }

    async fn send_and_await<F>(&self, make_msg: F) -> Result<(), crate::Error>
    where
        F: FnOnce(oneshot::Sender<Result<(), SlateDBError>>) -> CacheManagerMessage,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(make_msg(tx))
            .await
            .map_err(|_| crate::Error::from(SlateDBError::Closed))?;
        rx.await
            .map_err(|_| crate::Error::from(SlateDBError::Closed))?
            .map_err(crate::Error::from)
    }
}

pub(crate) struct CacheManagerWorker {
    table_store: Arc<TableStore>,
    warm_ranges: Vec<BytesRange>,
    cache_eviction_enabled: bool,
    manifest_rx: watch::Receiver<DbStatus>,
    previous_ssts: HashMap<SsTableId, SsTableHandle>,
    initialized: bool,
}

impl CacheManagerWorker {
    pub(crate) fn new(
        table_store: Arc<TableStore>,
        cache_eviction_enabled: bool,
        manifest_rx: watch::Receiver<DbStatus>,
    ) -> Self {
        Self {
            table_store,
            warm_ranges: Vec::new(),
            cache_eviction_enabled,
            manifest_rx,
            previous_ssts: HashMap::new(),
            initialized: false,
        }
    }

    async fn on_manifest_changed(&mut self) -> Result<(), SlateDBError> {
        let current_manifest = {
            let status = self.manifest_rx.borrow_and_update();
            match &status.current_manifest {
                Some(m) => m.clone(),
                None => return Ok(()),
            }
        };

        let current_ssts = collect_physical_ssts(&current_manifest);

        if !self.initialized {
            self.initialized = true;
            self.previous_ssts = current_ssts;
            debug!(
                "cache manager initialized [sst_count={}]",
                self.previous_ssts.len()
            );
            return Ok(());
        }

        let added: Vec<_> = current_ssts
            .keys()
            .filter(|id| !self.previous_ssts.contains_key(id))
            .cloned()
            .collect();
        let removed: Vec<_> = self
            .previous_ssts
            .keys()
            .filter(|id| !current_ssts.contains_key(id))
            .cloned()
            .collect();

        if !added.is_empty() || !removed.is_empty() {
            debug!(
                "cache manager manifest diff [added={}, removed={}]",
                added.len(),
                removed.len()
            );
        }

        for sst_id in &added {
            if let Some(handle) = current_ssts.get(sst_id) {
                if let Err(e) = self.warm_sst(handle, &self.warm_ranges.clone()).await {
                    warn!(
                        "cache manager failed to warm sst [id={:?}, error={:?}]",
                        sst_id, e
                    );
                }
            }
        }

        if self.cache_eviction_enabled {
            for sst_id in &removed {
                if let Some(handle) = self.previous_ssts.get(sst_id) {
                    if let Err(e) = self.evict_sst(handle).await {
                        warn!(
                            "cache manager failed to evict sst [id={:?}, error={:?}]",
                            sst_id, e
                        );
                    }
                }
            }
        }

        self.previous_ssts = current_ssts;
        Ok(())
    }

    async fn warm_all_current_ssts(&self) -> Result<(), SlateDBError> {
        let current_manifest = {
            let status = self.manifest_rx.borrow();
            match &status.current_manifest {
                Some(m) => m.clone(),
                None => return Ok(()),
            }
        };
        let current_ssts = collect_physical_ssts(&current_manifest);
        for handle in current_ssts.values() {
            if let Err(e) = self.warm_sst(handle, &self.warm_ranges).await {
                warn!(
                    "cache manager failed to warm sst [id={:?}, error={:?}]",
                    handle.id, e
                );
            }
        }
        Ok(())
    }

    async fn warm_ssts_by_id(&self, sst_ids: &[SsTableId]) -> Result<(), SlateDBError> {
        let current_manifest = {
            let status = self.manifest_rx.borrow();
            match &status.current_manifest {
                Some(m) => m.clone(),
                None => return Ok(()),
            }
        };
        let current_ssts = collect_physical_ssts(&current_manifest);
        for id in sst_ids {
            if let Some(handle) = current_ssts.get(id) {
                if let Err(e) = self.warm_sst(handle, &self.warm_ranges).await {
                    warn!(
                        "cache manager failed to warm sst [id={:?}, error={:?}]",
                        id, e
                    );
                }
            }
        }
        Ok(())
    }

    async fn warm_sst(
        &self,
        handle: &SsTableHandle,
        warm_ranges: &[BytesRange],
    ) -> Result<(), SlateDBError> {
        if warm_ranges.is_empty() {
            return Ok(());
        }

        let sst_range = sst_key_range(handle);
        let overlapping: Vec<_> = warm_ranges
            .iter()
            .filter(|r| r.intersect(&sst_range).is_some())
            .collect();

        if overlapping.is_empty() {
            return Ok(());
        }

        let index = self.table_store.read_index(handle, true).await?;
        // Also warm the bloom filter for faster point lookups.
        let _ = self.table_store.read_filter(handle, true).await;

        let index_borrowed = index.borrow();
        for range in overlapping {
            let block_range = partitioned_keyspace::blocks_covering_range(&index_borrowed, range);
            if !block_range.is_empty() {
                self.table_store
                    .read_blocks_using_index(handle, index.clone(), block_range, true)
                    .await?;
            }
        }

        Ok(())
    }

    async fn evict_sst(&self, handle: &SsTableHandle) -> Result<(), SlateDBError> {
        let index = match self.table_store.read_index(handle, false).await {
            Ok(idx) => idx,
            Err(e) => {
                warn!(
                    "cache manager could not read index for eviction \
                     [id={:?}, error={:?}]",
                    handle.id, e
                );
                return Ok(());
            }
        };
        self.table_store.remove_sst_from_cache(handle, &index).await;
        Ok(())
    }
}

#[async_trait]
impl MessageHandler<CacheManagerMessage> for CacheManagerWorker {
    async fn handle(&mut self, msg: CacheManagerMessage) -> Result<(), SlateDBError> {
        match msg {
            CacheManagerMessage::ManifestChanged => self.on_manifest_changed().await,
            CacheManagerMessage::SetWarmRanges { ranges, reply } => {
                self.warm_ranges = ranges;
                let _ = reply.send(Ok(()));
                Ok(())
            }
            CacheManagerMessage::WarmCurrent { reply } => {
                let result = self.warm_all_current_ssts().await;
                let _ = reply.send(result);
                Ok(())
            }
            CacheManagerMessage::WarmSsts { sst_ids, reply } => {
                let result = self.warm_ssts_by_id(&sst_ids).await;
                let _ = reply.send(result);
                Ok(())
            }
        }
    }

    async fn cleanup(
        &mut self,
        _messages: BoxStream<'async_trait, CacheManagerMessage>,
        _result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        Ok(())
    }
}

fn collect_physical_ssts(core: &ManifestCore) -> HashMap<SsTableId, SsTableHandle> {
    let mut ssts = HashMap::new();
    for view in &core.l0 {
        ssts.entry(view.sst.id).or_insert_with(|| view.sst.clone());
    }
    for sr in &core.compacted {
        for view in &sr.sst_views {
            ssts.entry(view.sst.id).or_insert_with(|| view.sst.clone());
        }
    }
    ssts
}

fn sst_key_range(handle: &SsTableHandle) -> BytesRange {
    use std::ops::Bound::{Included, Unbounded};
    let start = handle
        .info
        .first_entry
        .clone()
        .map(Included)
        .unwrap_or(Unbounded);
    let end = handle
        .info
        .last_entry
        .clone()
        .map(Included)
        .unwrap_or(Unbounded);
    BytesRange::new(start, end)
}
