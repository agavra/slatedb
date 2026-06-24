//! # RocksDB-style `db_bench` workloads
//!
//! This module adapts the [RocksDB performance benchmarks][rocksdb] to SlateDB so
//! that we can publish comparable numbers. Unlike the mixed read/write workload in
//! [`crate::db`], each workload here is a distinct, named *phase* that mirrors the
//! corresponding `benchmark.sh <phase>` step:
//!
//! - `load` (`fillrandom`): write `num_keys` entries spread across the key space in
//!   randomized sort order.
//! - `read-random` (`readrandom`): random point lookups of existing keys.
//! - `overwrite` (`overwrite`): random updates to existing keys.
//! - `read-while-writing` (`readwhilewriting`): `concurrency` reader tasks running
//!   point lookups while a single writer overwrites at a fixed `--mb-per-sec` rate.
//!
//! ## Deterministic keys
//!
//! Each phase is a separate process invocation against the same object-store path
//! (mirroring `benchmark.sh`). To let a `read-random` process read what a previous
//! `load` process wrote, keys are derived deterministically from an integer index
//! rather than stored in memory (storing 900M keys in a `Vec` is infeasible).
//!
//! Following db_bench's `GenerateKeyFromInt`, [`key_for_index`] writes the index
//! big-endian into the key, so keys are *dense* and sort in integer order — in a
//! sorted SST they delta/prefix-compress to almost nothing, matching RocksDB's
//! on-disk footprint. `fillrandom` semantics come from randomizing the *insertion
//! order*, not the keys: `load` maps each write position through [`perm_index`], a
//! stateless pseudo-random permutation of `[0, num_keys)`, so writes scatter across
//! the key space while still covering it exactly once.
//!
//! ## Full local cache
//!
//! The cache topology mirrors RocksDB-on-NVMe: an on-disk `CachedObjectStore`
//! (configured via [`Settings::object_store_cache_options`]) holds the compressed
//! SSTs on local disk, and an in-memory Foyer block cache (default 6 GiB, == the
//! RocksDB benchmark's `CACHE_SIZE`) holds decompressed blocks. With the disk
//! cache sized >= the compressed dataset, warm reads never hit object storage.
//! The disk cache persists across phase invocations and (with `--cache-puts`) is
//! warmed during `load`. Pass `--warmup` on the read phases to force a full
//! sequential read pass before measuring.
//!
//! SST compression (`--compression-codec zstd`) shrinks both the object-store
//! SSTs and the bytes held in the on-disk cache; pair it with `--compression-ratio`
//! to generate values that actually compress.
//!
//! [rocksdb]: https://github.com/facebook/rocksdb/wiki/performance-benchmarks

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hdrhistogram::Histogram;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::cached_object_store_stats::{PART_ACCESS_COUNT, PART_HIT_COUNT};
use slatedb::config::{PreloadLevel, PutOptions, Settings, WriteOptions};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::db_stats::{
    BACKPRESSURE_COUNT, L0_SST_COUNT, L0_STALL_COUNT, L0_STALL_TYPE_LABEL, L0_STALL_TYPE_NUM_SSTS,
    L0_STALL_TYPE_NUM_SSTS_PER_KEY, SEGMENT_MAX_L0_SST_COUNT, SST_FILTER_FALSE_POSITIVE_COUNT,
    SST_FILTER_NEGATIVE_COUNT, SST_FILTER_POSITIVE_COUNT,
};
use slatedb::Db;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::args::{CacheArgs, DbBenchArgs, DbBenchPhase};

const MIB: f64 = 1024.0 * 1024.0;

/// Seed offset for the writer in `read-while-writing`, so its key/value stream is
/// distinct from the reader tasks.
const WRITER_SEED_OFFSET: u64 = 0xD15E_A5ED;

/// SplitMix64 finalizer, used as the keyed round function of [`perm_index`].
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministically derive the key for a dataset index, mirroring db_bench's
/// `GenerateKeyFromInt`: the index is written big-endian into the trailing 8
/// bytes with the leading bytes zero-padded. Keys are therefore *dense* and sort
/// in integer order, so consecutive keys in a sorted SST share a long prefix and
/// delta/prefix-compress to almost nothing (matching RocksDB's footprint). The
/// mapping is a bijection, so distinct indices yield distinct keys. `key_len`
/// must be >= 8.
fn key_for_index(idx: u64, key_len: usize) -> Bytes {
    let mut buf = vec![0u8; key_len];
    let raw = idx.to_be_bytes();
    let n = key_len.min(8);
    buf[key_len - n..].copy_from_slice(&raw[8 - n..]);
    Bytes::from(buf)
}

/// A stateless pseudo-random permutation of `[0, n)`, used to randomize the
/// *insertion order* of the `load` phase (true `fillrandom`) without storing an
/// N-element shuffle in memory. Implemented as a 4-round balanced Feistel network
/// over the smallest even-bit-width domain >= `n`, with cycle-walking to reject
/// out-of-range outputs. Same `seed` => same permutation, so coverage is exactly
/// `[0, n)` once.
fn perm_index(j: u64, n: u64, seed: u64) -> u64 {
    if n <= 1 {
        return j;
    }
    let mut bits = u64::BITS - (n - 1).leading_zeros(); // ceil(log2(n))
    if bits % 2 == 1 {
        bits += 1;
    }
    let half = bits / 2;
    let mask = (1u64 << half) - 1;
    let mut x = j;
    loop {
        let mut l = (x >> half) & mask;
        let mut r = x & mask;
        for round in 0..4u64 {
            let f = splitmix64(r ^ seed ^ round.wrapping_mul(0x9E37_79B9_7F4A_7C15)) & mask;
            let next_r = l ^ f;
            l = r;
            r = next_r;
        }
        let y = (l << half) | r;
        if y < n {
            return y;
        }
        x = y; // cycle-walk: Feistel is a bijection, so this stays in-domain
    }
}

/// Generate a value of `val_len` bytes that compresses to approximately
/// `ratio` of its size. A `ratio` of 1.0 is fully random (incompressible); a
/// ratio of 0.5 compresses to ~half. This mirrors db_bench's `--compression_ratio`:
/// the leading `ratio * val_len` bytes are random and the rest tile that random
/// prefix, so a block compressor dedupes the repetition down to ~`ratio`.
fn compressible_value(rng: &mut impl RngCore, val_len: usize, ratio: f64) -> Vec<u8> {
    let mut value = vec![0u8; val_len];
    let raw_len = ((val_len as f64 * ratio).ceil() as usize).clamp(1, val_len);
    rng.fill_bytes(&mut value[..raw_len]);
    for i in raw_len..val_len {
        value[i] = value[i % raw_len];
    }
    value
}

/// Build the in-memory Foyer block cache. The on-disk cache is configured
/// separately via [`Settings::object_store_cache_options`], which the
/// `DbBuilder` uses to wrap the object store in a `CachedObjectStore`. Together
/// they mirror RocksDB-on-NVMe: compressed SSTs cached on local disk plus a
/// decompressed in-memory block cache.
fn build_block_cache(args: &CacheArgs) -> Arc<dyn DbCache> {
    info!(
        "in-memory block cache [size={} MiB]",
        args.mem_cache_size / 1024 / 1024,
    );
    Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: args.mem_cache_size,
        ..Default::default()
    })) as Arc<dyn DbCache>
}

/// Snapshot and log the SlateDB metrics that characterize read amplification and
/// write stalls. For read phases, `filter pos/neg` over the get count is the actual
/// block-read amplification: `pos` = SSTs whose bloom filter triggered a block fetch,
/// `neg` = SSTs the filter let us skip. `max_l0_overlap` is the worst-case number of
/// L0 SSTs covering any single key (what `l0_max_ssts_per_key` caps).
fn dump_db_stats(recorder: &DefaultMetricsRecorder, label: &str) {
    let snap = recorder.snapshot();
    let read = |name: &str| -> i64 {
        snap.by_name(name)
            .iter()
            .map(|m| match &m.value {
                MetricValue::Counter(v) => *v as i64,
                MetricValue::Gauge(v) | MetricValue::UpDownCounter(v) => *v,
                _ => 0,
            })
            .sum()
    };
    let stall = |t: &str| -> i64 {
        snap.by_name_and_labels(L0_STALL_COUNT, &[(L0_STALL_TYPE_LABEL, t)])
            .map(|m| match &m.value {
                MetricValue::Counter(v) => *v as i64,
                _ => 0,
            })
            .unwrap_or(0)
    };
    let os_access = read(PART_ACCESS_COUNT);
    let os_hits = read(PART_HIT_COUNT);
    let os_hit_pct = if os_access > 0 {
        os_hits as f64 / os_access as f64 * 100.0
    } else {
        0.0
    };
    info!(
        "db-stats [{label}]: l0_sst_count={} max_l0_overlap={} | filter pos={} neg={} fp={} | \
         os-cache hit={:.1}% ({}/{}) | stalls num_ssts={} per_key={} | backpressure={}",
        read(L0_SST_COUNT),
        read(SEGMENT_MAX_L0_SST_COUNT),
        read(SST_FILTER_POSITIVE_COUNT),
        read(SST_FILTER_NEGATIVE_COUNT),
        read(SST_FILTER_FALSE_POSITIVE_COUNT),
        os_hit_pct,
        os_hits,
        os_access,
        stall(L0_STALL_TYPE_NUM_SSTS),
        stall(L0_STALL_TYPE_NUM_SSTS_PER_KEY),
        read(BACKPRESSURE_COUNT),
    );
}

/// Periodically dump db-stats (L0 depth, filter counts, stalls) until `done` is set.
fn spawn_stats_dumper(recorder: Arc<DefaultMetricsRecorder>, done: Arc<AtomicUsize>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if done.load(Ordering::Relaxed) != 0 {
                return;
            }
            dump_db_stats(&recorder, "interval");
        }
    });
}

fn new_histogram() -> Histogram<u64> {
    // Track 1us .. 60s with 3 significant figures.
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("failed to create histogram")
}

/// The result of running one task (reader or writer) within a phase.
#[derive(Default)]
struct TaskResult {
    hist: Option<Histogram<u64>>,
    ops: u64,
    bytes: u64,
    hits: u64,
}

impl TaskResult {
    fn merge(&mut self, other: TaskResult) {
        self.ops += other.ops;
        self.bytes += other.bytes;
        self.hits += other.hits;
        match (&mut self.hist, other.hist) {
            (Some(h), Some(o)) => h.add(&o).expect("histogram merge failed"),
            (slot @ None, Some(o)) => *slot = Some(o),
            (_, None) => {}
        }
    }
}

/// Aggregate and print a phase report in a style comparable to db_bench output.
fn report(label: &str, elapsed: Duration, result: &TaskResult, report_hits: bool) {
    let secs = elapsed.as_secs_f64().max(1e-9);
    let ops_per_sec = result.ops as f64 / secs;
    let mib_per_sec = result.bytes as f64 / MIB / secs;
    let mut line = format!(
        "{label}: {ops} ops in {secs:.1}s | {ops_per_sec:.0} ops/s | {mib_per_sec:.2} MiB/s",
        ops = result.ops,
    );
    if report_hits && result.ops > 0 {
        let hit_pct = result.hits as f64 / result.ops as f64 * 100.0;
        line.push_str(&format!(" | hit {hit_pct:.2}%"));
    }
    info!("{line}");
    if let Some(hist) = &result.hist {
        info!(
            "{label}: latency(us) p50={} p95={} p99={} p99.9={} p99.99={} max={}",
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.95),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.value_at_quantile(0.9999),
            hist.max(),
        );
    }
}

/// Periodically logs interim ops/s using a shared counter, until `done` is set.
fn spawn_progress(label: &'static str, counter: Arc<AtomicU64>, done: Arc<AtomicUsize>) {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut last = (start, 0u64);
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if done.load(Ordering::Relaxed) != 0 {
                return;
            }
            let now = Instant::now();
            let ops = counter.load(Ordering::Relaxed);
            let interval = (now - last.0).as_secs_f64().max(1e-9);
            let rate = (ops - last.1) as f64 / interval;
            info!(
                "{label}: elapsed {:.0}s | {:.0} ops/s (interval) | {ops} total ops",
                (now - start).as_secs_f64(),
                rate,
            );
            last = (now, ops);
        }
    });
}

/// Entry point for the `db-bench` command: build the DB (with the on-disk
/// `CachedObjectStore` and in-memory block cache), run the requested phase, and
/// close.
pub(crate) async fn exec(path: Path, object_store: Arc<dyn ObjectStore>, args: DbBenchArgs) {
    if args.key_len < 8 {
        panic!("--key-len must be >= 8 for deterministic key generation");
    }
    let mut settings = match &args.db_options_path {
        Some(path) => Settings::from_file(path).expect("failed to load settings from file"),
        None => Settings::load().expect("failed to load settings"),
    };

    // SST compression: shrinks the SSTs in object storage and the bytes cached on
    // local disk by `CachedObjectStore` (which caches the raw, compressed objects).
    if let Some(codec) = args.compression_codec {
        settings.compression_codec = Some(codec);
    }

    // On-disk cache: configure `CachedObjectStore` via settings so the `DbBuilder`
    // wraps the object store. Size `--disk-cache-size` >= the compressed dataset so
    // warm reads never hit object storage.
    if let Err(e) = std::fs::create_dir_all(&args.cache.disk_cache_dir) {
        warn!(
            "failed to create disk cache dir [dir={}, error={}]",
            args.cache.disk_cache_dir.display(),
            e
        );
    }
    info!(
        "on-disk object cache [size={} MiB, dir={}, cache_puts={}]",
        args.cache.disk_cache_size / 1024 / 1024,
        args.cache.disk_cache_dir.display(),
        args.cache.cache_puts,
    );
    settings.object_store_cache_options.root_folder = Some(args.cache.disk_cache_dir.clone());
    settings.object_store_cache_options.max_cache_size_bytes =
        Some(args.cache.disk_cache_size as usize);
    settings.object_store_cache_options.part_size_bytes = args.cache.part_size_bytes;
    settings.object_store_cache_options.cache_puts = args.cache.cache_puts;
    if args.preload {
        // Bulk-load all SSTs into the on-disk cache at open. Needed because
        // multipart-written (Compacted) SSTs are not populated by cache_puts.
        settings
            .object_store_cache_options
            .preload_disk_cache_on_startup = Some(PreloadLevel::AllSst);
    }

    let block_cache = build_block_cache(&args.cache);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = Arc::new(
        Db::builder(path, object_store)
            .with_settings(settings)
            .with_db_cache(block_cache)
            .with_metrics_recorder(recorder.clone())
            .build()
            .await
            .expect("failed to build db"),
    );

    // Dump read-amplification / stall metrics periodically and at phase end.
    let stats_done = Arc::new(AtomicUsize::new(0));
    spawn_stats_dumper(recorder.clone(), stats_done.clone());
    run(db.clone(), args).await;
    stats_done.store(1, Ordering::Relaxed);
    dump_db_stats(&recorder, "final");

    db.close().await.expect("failed to close db");
}

/// Dispatch to the requested phase against an already-open DB.
async fn run(db: Arc<Db>, args: DbBenchArgs) {
    let write_options = WriteOptions {
        await_durable: args.await_durable,
        ..Default::default()
    };
    match args.phase.clone() {
        DbBenchPhase::Load => run_load(db, &args, &write_options).await,
        DbBenchPhase::ReadRandom(d) => {
            if d.warmup {
                warmup(db.clone(), &args).await;
            }
            run_read_random(db, &args, Duration::from_secs(d.duration as u64)).await;
        }
        DbBenchPhase::Overwrite(d) => {
            run_overwrite(
                db,
                &args,
                &write_options,
                Duration::from_secs(d.duration as u64),
            )
            .await;
        }
        DbBenchPhase::ReadWhileWriting(rww) => {
            if rww.warmup {
                warmup(db.clone(), &args).await;
            }
            run_read_while_writing(
                db,
                &args,
                &write_options,
                Duration::from_secs(rww.duration as u64),
                rww.mb_per_sec,
            )
            .await;
        }
    }
}

/// `fillrandom`: write every index in `[0, num_keys)` exactly once. Tasks own
/// contiguous slices of the write sequence, but each write position `j` is mapped
/// through [`perm_index`] to a dense key, so the *insertion order* is randomized
/// across the key space (true `fillrandom`) while coverage stays exactly once.
async fn run_load(db: Arc<Db>, args: &DbBenchArgs, write_options: &WriteOptions) {
    info!(
        "load (fillrandom): writing {} keys [key_len={}, val_len={}, concurrency={}]",
        args.num_keys, args.key_len, args.val_len, args.concurrency
    );
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    spawn_progress("load", counter.clone(), done.clone());

    let concurrency = args.concurrency.max(1) as u64;
    let chunk = args.num_keys.div_ceil(concurrency);
    let start = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..concurrency {
        let db = db.clone();
        let counter = counter.clone();
        let write_options = write_options.clone();
        let (num_keys, key_len, val_len, seed, ratio) = (
            args.num_keys,
            args.key_len,
            args.val_len,
            args.seed,
            args.compression_ratio,
        );
        let lo = t * chunk;
        let hi = ((t + 1) * chunk).min(args.num_keys);
        tasks.push(tokio::spawn(async move {
            let mut rng = XorShiftRng::seed_from_u64(seed.wrapping_add(t));
            let mut hist = new_histogram();
            let mut result = TaskResult::default();
            let mut local = 0u64;
            for j in lo..hi {
                // Randomize insertion order across the key space (fillrandom).
                let idx = perm_index(j, num_keys, seed);
                let key = key_for_index(idx, key_len);
                let value = compressible_value(&mut rng, val_len, ratio);
                let op_start = Instant::now();
                match db
                    .put_with_options(key, value, &PutOptions::default(), &write_options)
                    .await
                {
                    Ok(_) => {
                        hist.saturating_record(op_start.elapsed().as_micros() as u64);
                        result.ops += 1;
                        result.bytes += (key_len + val_len) as u64;
                    }
                    Err(e) => warn!("put failed [error={}]", e),
                }
                local += 1;
                if local.is_multiple_of(1024) {
                    counter.fetch_add(1024, Ordering::Relaxed);
                }
            }
            counter.fetch_add(local % 1024, Ordering::Relaxed);
            result.hist = Some(hist);
            result
        }));
    }
    let mut total = TaskResult::default();
    for task in tasks {
        total.merge(task.await.unwrap());
    }
    done.store(1, Ordering::Relaxed);
    report("load", start.elapsed(), &total, false);
}

/// `readrandom`: random point lookups of existing keys for `duration`.
async fn run_read_random(db: Arc<Db>, args: &DbBenchArgs, duration: Duration) {
    info!(
        "read-random (readrandom): random gets for {:?} [concurrency={}]",
        duration, args.concurrency
    );
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    spawn_progress("read-random", counter.clone(), done.clone());

    let start = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..args.concurrency.max(1) as u64 {
        let db = db.clone();
        let counter = counter.clone();
        let (num_keys, key_len, seed) = (args.num_keys, args.key_len, args.seed);
        tasks.push(tokio::spawn(async move {
            read_loop(
                db,
                num_keys,
                key_len,
                seed.wrapping_add(t),
                duration,
                counter,
            )
            .await
        }));
    }
    let mut total = TaskResult::default();
    for task in tasks {
        total.merge(task.await.unwrap());
    }
    done.store(1, Ordering::Relaxed);
    report("read-random", start.elapsed(), &total, true);
}

/// A single reader task: random gets of existing keys until `duration` elapses.
async fn read_loop(
    db: Arc<Db>,
    num_keys: u64,
    key_len: usize,
    seed: u64,
    duration: Duration,
    counter: Arc<AtomicU64>,
) -> TaskResult {
    let mut rng = XorShiftRng::seed_from_u64(seed);
    let mut hist = new_histogram();
    let mut result = TaskResult::default();
    let start = Instant::now();
    let mut local = 0u64;
    while start.elapsed() < duration {
        let idx = rng.random_range(0..num_keys);
        let key = key_for_index(idx, key_len);
        let op_start = Instant::now();
        match db.get(&key).await {
            Ok(val) => {
                hist.saturating_record(op_start.elapsed().as_micros() as u64);
                result.ops += 1;
                result.hits += val.is_some() as u64;
                result.bytes += key_len as u64 + val.map(|v| v.len() as u64).unwrap_or(0);
            }
            Err(e) => warn!("get failed [error={}]", e),
        }
        local += 1;
        if local.is_multiple_of(1024) {
            counter.fetch_add(1024, Ordering::Relaxed);
        }
    }
    counter.fetch_add(local % 1024, Ordering::Relaxed);
    result.hist = Some(hist);
    result
}

/// `overwrite`: random updates to existing keys for `duration`.
async fn run_overwrite(
    db: Arc<Db>,
    args: &DbBenchArgs,
    write_options: &WriteOptions,
    duration: Duration,
) {
    info!(
        "overwrite: random updates for {:?} [concurrency={}]",
        duration, args.concurrency
    );
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    spawn_progress("overwrite", counter.clone(), done.clone());

    let start = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..args.concurrency.max(1) as u64 {
        let db = db.clone();
        let counter = counter.clone();
        let write_options = write_options.clone();
        let (num_keys, key_len, val_len, ratio, seed) = (
            args.num_keys,
            args.key_len,
            args.val_len,
            args.compression_ratio,
            args.seed,
        );
        tasks.push(tokio::spawn(async move {
            write_loop(
                db,
                num_keys,
                key_len,
                val_len,
                ratio,
                seed.wrapping_add(t),
                write_options,
                duration,
                None,
                counter,
            )
            .await
        }));
    }
    let mut total = TaskResult::default();
    for task in tasks {
        total.merge(task.await.unwrap());
    }
    done.store(1, Ordering::Relaxed);
    report("overwrite", start.elapsed(), &total, false);
}

/// A single writer task: random overwrites until `duration` elapses, optionally
/// rate-limited to `max_bytes_per_sec`.
#[allow(clippy::too_many_arguments)]
async fn write_loop(
    db: Arc<Db>,
    num_keys: u64,
    key_len: usize,
    val_len: usize,
    ratio: f64,
    seed: u64,
    write_options: WriteOptions,
    duration: Duration,
    max_bytes_per_sec: Option<u64>,
    counter: Arc<AtomicU64>,
) -> TaskResult {
    let mut rng = XorShiftRng::seed_from_u64(seed);
    let mut hist = new_histogram();
    let mut result = TaskResult::default();
    let start = Instant::now();
    let mut local = 0u64;
    while start.elapsed() < duration {
        let idx = rng.random_range(0..num_keys);
        let key = key_for_index(idx, key_len);
        let value = compressible_value(&mut rng, val_len, ratio);
        let op_start = Instant::now();
        match db
            .put_with_options(key, value, &PutOptions::default(), &write_options)
            .await
        {
            Ok(_) => {
                hist.saturating_record(op_start.elapsed().as_micros() as u64);
                result.ops += 1;
                result.bytes += (key_len + val_len) as u64;
            }
            Err(e) => warn!("put failed [error={}]", e),
        }
        local += 1;
        if local.is_multiple_of(1024) {
            counter.fetch_add(1024, Ordering::Relaxed);
        }
        // Rate limit: sleep until the elapsed time catches up to the bytes written.
        if let Some(rate) = max_bytes_per_sec {
            let target = Duration::from_secs_f64(result.bytes as f64 / rate as f64);
            let elapsed = start.elapsed();
            if target > elapsed {
                tokio::time::sleep(target - elapsed).await;
            }
        }
    }
    counter.fetch_add(local % 1024, Ordering::Relaxed);
    result.hist = Some(hist);
    result
}

/// `readwhilewriting`: `concurrency` reader tasks plus a single writer overwriting
/// at `mb_per_sec`. Reader and writer stats are reported separately.
async fn run_read_while_writing(
    db: Arc<Db>,
    args: &DbBenchArgs,
    write_options: &WriteOptions,
    duration: Duration,
    mb_per_sec: u64,
) {
    info!(
        "read-while-writing: {} readers + 1 writer ({} MiB/s) for {:?}",
        args.concurrency, mb_per_sec, duration
    );
    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    spawn_progress("read-while-writing", counter.clone(), done.clone());

    let start = Instant::now();

    // Reader tasks.
    let mut readers = Vec::new();
    for t in 0..args.concurrency.max(1) as u64 {
        let db = db.clone();
        let counter = counter.clone();
        let (num_keys, key_len, seed) = (args.num_keys, args.key_len, args.seed);
        readers.push(tokio::spawn(async move {
            read_loop(
                db,
                num_keys,
                key_len,
                seed.wrapping_add(t),
                duration,
                counter,
            )
            .await
        }));
    }

    // Single rate-limited writer.
    let writer = {
        let db = db.clone();
        let write_options = write_options.clone();
        let writer_counter = Arc::new(AtomicU64::new(0));
        let (num_keys, key_len, val_len, ratio, seed) = (
            args.num_keys,
            args.key_len,
            args.val_len,
            args.compression_ratio,
            args.seed,
        );
        let rate = mb_per_sec * 1024 * 1024;
        tokio::spawn(async move {
            write_loop(
                db,
                num_keys,
                key_len,
                val_len,
                ratio,
                seed.wrapping_add(WRITER_SEED_OFFSET),
                write_options,
                duration,
                Some(rate),
                writer_counter,
            )
            .await
        })
    };

    let mut reader_total = TaskResult::default();
    for task in readers {
        reader_total.merge(task.await.unwrap());
    }
    let writer_total = writer.await.unwrap();
    done.store(1, Ordering::Relaxed);
    let elapsed = start.elapsed();
    report("read-while-writing (reads)", elapsed, &reader_total, true);
    report("read-while-writing (writes)", elapsed, &writer_total, false);
}

/// Read every key once to populate the cache before a measured read phase.
/// Exhaustively warm every live SST into the cache before measuring reads.
///
/// Best-effort `--preload` ([`PreloadLevel::AllSst`]) leaves a small fraction
/// of parts cold, so random reads still occasionally miss to object storage
/// (a heavy S3 tail that drags throughput). This instead fans out
/// [`Db::warm_sst`] over every SST in the current manifest, reading every
/// data/index/filter/stats block — a complete read-through that fully
/// populates the on-disk cache, so subsequent reads never hit object storage.
async fn warmup(db: Arc<Db>, args: &DbBenchArgs) {
    use slatedb::{CacheTarget, DbCacheManagerOps};

    let manifest = db.manifest();
    let mut ids = Vec::new();
    for view in manifest.l0() {
        ids.push(view.sst.id);
    }
    for sr in manifest.compacted() {
        for view in &sr.sst_views {
            ids.push(view.sst.id);
        }
    }
    let total = ids.len();
    info!("warmup: exhaustively warming {total} SSTs (data+index+filters+stats)");

    let counter = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    spawn_progress("warmup", counter.clone(), done.clone());

    let sem = Arc::new(tokio::sync::Semaphore::new(args.concurrency.max(1) as usize));
    let mut set = tokio::task::JoinSet::new();
    for id in ids {
        let db = db.clone();
        let counter = counter.clone();
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let targets = [
                CacheTarget::Index,
                CacheTarget::Filters,
                CacheTarget::Stats,
                CacheTarget::data::<&[u8], _>(..),
            ];
            if let Err(e) = db.warm_sst(id, &targets).await {
                warn!("warmup warm_sst failed [error={e}]");
            }
            counter.fetch_add(1, Ordering::Relaxed);
        });
    }
    while set.join_next().await.is_some() {}
    done.store(1, Ordering::Relaxed);
    info!("warmup: complete ({total} SSTs warmed)");
}
