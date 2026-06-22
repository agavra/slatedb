use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rand::RngCore;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::compaction_worker::WorkerMessage;
use crate::compactor::stats::{CompactionStats, WorkerStats};
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
    TokioCompactionExecutorOptions,
};
use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
use crate::config::{CompactionWorkerOptions, CompressionCodec};
use crate::db_state::{SsTableHandle, SsTableId, SsTableView};
use crate::error::SlateDBError;
use crate::format::sst::SsTableFormat;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::object_stores::ObjectStores;
use crate::tablestore::{TableStore, TableStoreKind};
use crate::types::RowEntry;
use crate::types::ValueDeletable;
use crate::utils::IdGenerator;
use slatedb_common::clock::{DefaultSystemClock, SystemClock};
use slatedb_common::metrics::MetricsRecorderHelper;
use slatedb_common::DbRand;

pub struct CompactionExecuteBench {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    rand: Arc<DbRand>,
    system_clock: Arc<dyn SystemClock>,
}

impl CompactionExecuteBench {
    pub fn new(path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
        Self::new_with_rand(path, object_store, Arc::new(DbRand::default()))
    }

    fn new_with_rand(path: Path, object_store: Arc<dyn ObjectStore>, rand: Arc<DbRand>) -> Self {
        Self {
            path,
            object_store,
            rand,
            system_clock: Arc::new(DefaultSystemClock::new()),
        }
    }

    fn sst_id(id: u32) -> SsTableId {
        SsTableId::Compacted(Ulid::from((id as u64, id as u64)))
    }

    pub async fn run_load(
        &self,
        num_ssts: usize,
        sst_bytes: usize,
        key_bytes: usize,
        val_bytes: usize,
        compression_codec: Option<CompressionCodec>,
        overlapping_ssts: usize,
    ) -> Result<(), crate::Error> {
        let sst_format = SsTableFormat {
            compression_codec,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(self.object_store.clone(), None),
            sst_format,
            self.path.clone(),
            None,
            TableStoreKind::Main,
        ));
        let num_keys = sst_bytes / (val_bytes + key_bytes);
        // Key layout: [tile byte][big-endian counter][u32 SST id]. An SST spans a
        // contiguous range of tile bytes; one that spans a single tile is a
        // disjoint "base" SST, while the first `overlapping_ssts` span every tile
        // and so overlap all of them (the merge "churn"). This models realistic
        // compactions where most inputs are disjoint and only a small subset
        // overlap, rather than every input overlapping every other.
        let overlapping = overlapping_ssts.min(num_ssts);
        let num_tiles = (num_ssts - overlapping).max(1);
        assert!(num_tiles <= 256, "tile index must fit in one byte");
        let mut futures = FuturesUnordered::<JoinHandle<Result<(), SlateDBError>>>::new();
        for i in 0..num_ssts {
            while futures.len() >= 4 {
                futures
                    .next()
                    .await
                    .expect("expected value")
                    .expect("join failed")?;
            }
            let ts = table_store.clone();
            let (tile_lo, tile_hi) = if i < overlapping {
                (0u32, num_tiles as u32)
            } else {
                let tile = (i - overlapping) as u32;
                (tile, tile + 1)
            };
            let jh = tokio::spawn(CompactionExecuteBench::load_sst(
                i as u32,
                ts,
                key_bytes,
                tile_lo,
                tile_hi,
                num_keys,
                val_bytes,
                self.rand.clone(),
                self.system_clock.clone(),
            ));
            futures.push(jh)
        }
        while !futures.is_empty() {
            futures
                .next()
                .await
                .expect("expected value")
                .expect("join failed")?;
        }
        Ok(())
    }

    /// Builds a key `[tile byte][big-endian counter][u32 suffix]`: the tile byte
    /// is the high-order range coordinate, the counter orders keys within a tile,
    /// and the suffix is the SST id so keys never collide across SSTs.
    fn make_key(key_bytes: usize, tile: u8, counter: u64, suffix: u32) -> Bytes {
        let ctr_width = key_bytes - 1 - mem::size_of::<u32>();
        let ctr_be = counter.to_be_bytes();
        let mut key = BytesMut::with_capacity(key_bytes);
        key.put_u8(tile);
        if ctr_width >= ctr_be.len() {
            key.put_bytes(0, ctr_width - ctr_be.len());
            key.put_slice(&ctr_be);
        } else {
            key.put_slice(&ctr_be[ctr_be.len() - ctr_width..]);
        }
        key.put_u32(suffix);
        key.freeze()
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_sst(
        i: u32,
        table_store: Arc<TableStore>,
        key_bytes: usize,
        tile_lo: u32,
        tile_hi: u32,
        num_keys: usize,
        val_bytes: usize,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<(), SlateDBError> {
        let mut retries = 0;
        loop {
            let result = CompactionExecuteBench::do_load_sst(
                i,
                table_store.clone(),
                key_bytes,
                tile_lo,
                tile_hi,
                num_keys,
                val_bytes,
                system_clock.clone(),
                rand.clone(),
            )
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if retries >= 3 {
                        return Err(err);
                    } else {
                        error!("error loading sst [retry={}]: {:?}", retries, err)
                    }
                }
            }
            retries += 1;
            system_clock
                .clone()
                .sleep(Duration::from_secs(retries + 1))
                .await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_load_sst(
        i: u32,
        table_store: Arc<TableStore>,
        key_bytes: usize,
        tile_lo: u32,
        tile_hi: u32,
        num_keys: usize,
        val_bytes: usize,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<(), SlateDBError> {
        let start = system_clock.now();
        // Spread `num_keys` over the SST's tile span. A single-tile SST is dense
        // (stride 1); a multi-tile SST strides by its span so its keys cover the
        // full counter range of every tile it spans, overlapping each one.
        let num_buckets = (tile_hi - tile_lo).max(1) as u64;
        let keys_per_bucket = num_keys / num_buckets as usize;
        let mut sst_writer = table_store.table_writer(CompactionExecuteBench::sst_id(i));
        for bucket in tile_lo..tile_hi {
            for j in 0..keys_per_bucket {
                let mut val = vec![0u8; val_bytes];
                rand.rng().fill_bytes(val.as_mut_slice());
                let key = CompactionExecuteBench::make_key(
                    key_bytes,
                    bucket as u8,
                    j as u64 * num_buckets,
                    i,
                );
                let row_entry =
                    RowEntry::new(key, ValueDeletable::Value(val.into()), 0, None, None);
                sst_writer.add(row_entry).await?;
            }
        }
        let sst = sst_writer.close().await?;
        let elapsed_ms = system_clock
            .now()
            .signed_duration_since(start)
            .num_milliseconds();
        info!("wrote sst [id={:?}, elapsed_ms={}]", &sst.id, elapsed_ms);
        Ok(())
    }

    #[allow(clippy::panic)]
    pub async fn run_clear(&self, num_ssts: usize) -> Result<(), crate::Error> {
        let mut del_tasks = Vec::new();
        for i in 0u32..num_ssts as u32 {
            let os = self.object_store.clone();
            let path = self.path.clone();
            del_tasks.push(tokio::spawn(async move {
                let sst_id = CompactionExecuteBench::sst_id(i);
                os.delete(&CompactionExecuteBench::sst_path(&sst_id, &path))
                    .await
            }))
        }
        let results = futures::future::join_all(del_tasks).await;
        for result in results {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(SlateDBError::from(err).into()),
                Err(err) => panic!("task failed [error={:?}]", err),
            }
        }
        Ok(())
    }

    async fn load_compaction_job(
        manifest: &StoredManifest,
        num_ssts: usize,
        table_store: &Arc<TableStore>,
        is_dest_last_run: bool,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<StartCompactionJobArgs, SlateDBError> {
        let sst_ids: Vec<SsTableId> = (0u32..num_ssts as u32)
            .map(CompactionExecuteBench::sst_id)
            .collect();
        let mut futures =
            FuturesUnordered::<JoinHandle<Result<(SsTableId, SsTableHandle), SlateDBError>>>::new();
        let mut ssts_by_id = HashMap::new();
        info!("load sst");
        for id in sst_ids.clone().into_iter() {
            if futures.len() > 8 {
                let (id, handle) = futures
                    .next()
                    .await
                    .expect("missing join handle")
                    .expect("join failed")?;
                ssts_by_id.insert(id, handle);
            }
            let table_store_clone = table_store.clone();
            let jh = tokio::spawn(async move {
                match table_store_clone.open_sst(&id).await {
                    Ok(h) => Ok((id, h)),
                    Err(err) => Err(err),
                }
            });
            futures.push(jh);
        }
        while let Some(jh) = futures.next().await {
            let (id, handle) = jh.expect("join failed")?;
            ssts_by_id.insert(id, handle);
        }
        info!("finished loading");
        let sst_views: Vec<SsTableView> = sst_ids
            .into_iter()
            .map(|id| {
                SsTableView::new(
                    rand.rng().gen_ulid(system_clock.as_ref()),
                    ssts_by_id.get(&id).expect("expected sst").clone(),
                )
            })
            .collect();
        // Bind each id to a local first: two `rand.rng()` calls in a single
        // expression would hold the first `RefMut` guard alive while the second
        // tries to borrow the same thread-local RNG, panicking with "RefCell
        // already borrowed".
        let id = rand.rng().gen_ulid(system_clock.as_ref());
        let compaction_id = rand.rng().gen_ulid(system_clock.as_ref());
        Ok(StartCompactionJobArgs {
            id,
            compaction_id,
            destination: 0,
            l0_sst_views: sst_views,
            sorted_runs: vec![],
            compaction_clock_tick: manifest.db_state().last_l0_clock_tick,
            is_dest_last_run,
            retention_min_seq: Some(manifest.db_state().recent_snapshot_min_seq),
            ctx: None,
        })
    }

    fn load_compaction_as_job_args(
        manifest: &StoredManifest,
        job: &Compaction,
        is_dest_last_run: bool,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> StartCompactionJobArgs {
        let state = manifest.db_state();
        let spec = job.spec();
        let srs_by_id: HashMap<_, _> = state
            .tree
            .compacted
            .iter()
            .map(|sr| (sr.id, sr.clone()))
            .collect();
        let srs: Vec<_> = spec
            .sources()
            .iter()
            .map(|sr| {
                srs_by_id
                    .get(&sr.unwrap_sorted_run())
                    .expect("expected src")
                    .clone()
            })
            .collect();
        info!("loaded compaction job");

        StartCompactionJobArgs {
            id: rand.rng().gen_ulid(system_clock.as_ref()),
            compaction_id: job.id(),
            destination: 0,
            l0_sst_views: vec![],
            sorted_runs: srs,
            compaction_clock_tick: state.last_l0_clock_tick,
            is_dest_last_run,
            retention_min_seq: Some(state.recent_snapshot_min_seq),
            ctx: None,
        }
    }

    pub async fn run_bench(
        &self,
        num_ssts: usize,
        source_sr_ids: Option<Vec<u32>>,
        destination_sr_id: u32,
        compression_codec: Option<CompressionCodec>,
        max_subcompactions: usize,
        max_fetch_tasks: usize,
        bytes_to_fetch: usize,
    ) -> Result<(), crate::Error> {
        let sst_format = SsTableFormat {
            compression_codec,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(self.object_store.clone(), None),
            sst_format,
            self.path.clone(),
            None,
            TableStoreKind::Compactor,
        ));
        let (tx, rx) = async_channel::unbounded();
        // Split the compaction into subcompactions (RFC-0028) so the bench
        // exercises the parallel-range path; `max_subcompactions <= 1` disables
        // it and runs the compaction whole. `max_fetch_tasks`/`bytes_to_fetch`
        // tune the per-iterator S3 read concurrency and read-ahead size so the
        // bench can probe whether compaction throughput is I/O-concurrency bound.
        let worker_options = CompactionWorkerOptions {
            max_subcompactions,
            max_fetch_tasks,
            bytes_to_fetch,
            ..CompactionWorkerOptions::default()
        };
        let recorder = MetricsRecorderHelper::noop();
        let stats = Arc::new(CompactionStats::new(&recorder));
        let os = self.object_store.clone();

        let manifest_store = Arc::new(ManifestStore::new(&self.path, os.clone()));

        let executor = TokioCompactionExecutor::new(TokioCompactionExecutorOptions {
            handle: Handle::current(),
            options: Arc::new(worker_options),
            worker_tx: tx,
            table_store: table_store.clone(),
            rand: self.rand.clone(),
            stats: stats.clone(),
            worker_stats: WorkerStats::new(&recorder, "bench"),
            clock: self.system_clock.clone(),
            manifest_store: manifest_store.clone(),
            merge_operator: None,
            #[cfg(feature = "compaction_filters")]
            compaction_filter_supplier: None,
        });

        let manifest = StoredManifest::load(manifest_store, self.system_clock.clone()).await?;

        let sources: Vec<SourceId> = source_sr_ids
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(SourceId::SortedRun)
            .collect();

        let compactor_job = source_sr_ids.map(|_source_sr_ids| {
            let id = self.rand.rng().gen_ulid(self.system_clock.as_ref());
            let spec = CompactionSpec::new(sources, destination_sr_id);
            Compaction::new(id, spec)
        });

        info!("load compaction job");
        let job = match &compactor_job {
            Some(compactor_job) => {
                info!("load job from existing compaction");
                CompactionExecuteBench::load_compaction_as_job_args(
                    &manifest,
                    compactor_job,
                    false,
                    self.rand.clone(),
                    self.system_clock.clone(),
                )
            }
            None => {
                CompactionExecuteBench::load_compaction_job(
                    &manifest,
                    num_ssts,
                    &table_store,
                    false,
                    self.rand.clone(),
                    self.system_clock.clone(),
                )
                .await?
            }
        };
        let start = self.system_clock.now();
        info!("start compaction job");
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || executor.start_compaction_job(job));
        while let Ok(msg) = rx.recv().await {
            if let WorkerMessage::CompactionJobFinished { id: _, result } = msg {
                match result {
                    Ok(_) => {
                        let elapsed_ms = self
                            .system_clock
                            .now()
                            .signed_duration_since(start)
                            .num_milliseconds();
                        info!("compaction finished [elapsed_ms={}]", elapsed_ms);
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::panic)]
    fn sst_path(id: &SsTableId, root_path: &Path) -> Path {
        match id {
            SsTableId::Compacted(ulid) => {
                Path::from(format!("{}/compacted/{}.sst", root_path, ulid.to_string()))
            }
            _ => panic!("invalid sst type"),
        }
    }
}
