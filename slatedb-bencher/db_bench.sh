#!/usr/bin/env bash
#
# Runs the RocksDB-style db_bench workloads (load, read-random, overwrite,
# read-while-writing) against SlateDB, mirroring RocksDB's benchmark.sh.
#
# Cache topology mirrors RocksDB-on-NVMe: an on-disk CachedObjectStore holds the
# (compressed) SSTs on local disk, plus an in-memory Foyer block cache (default
# 6 GiB, == RocksDB's benchmark CACHE_SIZE) holds decompressed blocks. With the
# disk cache sized >= the compressed dataset, warm reads never hit object storage.
# By default the object store backend is a local filesystem directory
# (CLOUD_PROVIDER=local), so a run never touches the network.
#
# Usage:
#   ./db_bench.sh                          # run all phases with defaults
#   NUM_KEYS=10000000 DURATION=300 ./db_bench.sh
#   COMPRESSION_CODEC=zstd COMPRESSION_RATIO=0.5 ./db_bench.sh
#   ./db_bench.sh load read-random         # run only specific phases
#
# Tunables (env vars):
#   NUM_KEYS            dataset size in keys                  (default 1000000)
#   KEY_LEN             key length in bytes                   (default 16)
#   VAL_LEN             value length in bytes                 (default 1024)
#   CONCURRENCY         reader/writer tasks                   (default 4)
#   DURATION            seconds per read/overwrite phase      (default 30)
#   MB_PER_SEC          writer rate for read-while-writing    (default 2)
#   MEM_CACHE           in-memory block cache bytes           (default 6GiB)
#   DISK_CACHE          on-disk object cache bytes            (default = dataset * 1.5)
#   COMPRESSION_CODEC   snappy|zlib|lz4|zstd (SST compression)(default none)
#   COMPRESSION_RATIO   value compressibility, 1.0=random     (default 1.0)
#   SEED                deterministic key seed                (default 0)
#   WARMUP              "1" to warm the cache before reads    (default 0)
#   DATA_DIR            object store + cache root             (default target/db_bench)
#   BIN                 path to the bencher binary            (default target/release/bencher)

set -euo pipefail

cd "$(dirname "$0")/.."

NUM_KEYS=${NUM_KEYS:-1000000}
KEY_LEN=${KEY_LEN:-16}
VAL_LEN=${VAL_LEN:-1024}
CONCURRENCY=${CONCURRENCY:-4}
DURATION=${DURATION:-30}
MB_PER_SEC=${MB_PER_SEC:-2}
MEM_CACHE=${MEM_CACHE:-6442450944}   # 6 GiB, == RocksDB benchmark CACHE_SIZE
COMPRESSION_CODEC=${COMPRESSION_CODEC:-}
COMPRESSION_RATIO=${COMPRESSION_RATIO:-1.0}
SEED=${SEED:-0}
WARMUP=${WARMUP:-0}
DATA_DIR=${DATA_DIR:-target/db_bench}
BIN=${BIN:-target/release/bencher}

# Size the disk object cache to 1.5x the raw dataset so warm reads never spill to
# the object store. The cache holds the *compressed* SSTs, so with compression the
# default is generous (a safe upper bound).
DEFAULT_DISK_CACHE=$(( (NUM_KEYS * (KEY_LEN + VAL_LEN) * 3) / 2 ))
DISK_CACHE=${DISK_CACHE:-$DEFAULT_DISK_CACHE}

LOCAL_PATH="$DATA_DIR/object_store"
DISK_CACHE_DIR="$DATA_DIR/cache"

export CLOUD_PROVIDER=${CLOUD_PROVIDER:-local}
export LOCAL_PATH
# Quieter logs: bencher output is at info, everything else warn.
export RUST_LOG=${RUST_LOG:-warn,bencher=info,slatedb_bencher=info}

mkdir -p "$LOCAL_PATH" "$DISK_CACHE_DIR"

if [[ ! -x "$BIN" ]]; then
  echo "bencher binary not found at $BIN; build it with:" >&2
  echo "  cargo build --release -p slatedb-bencher" >&2
  exit 1
fi

WARMUP_FLAG=""
if [[ "$WARMUP" == "1" ]]; then
  WARMUP_FLAG="--warmup"
fi

COMMON=(--num-keys "$NUM_KEYS" --key-len "$KEY_LEN" --val-len "$VAL_LEN" \
  --concurrency "$CONCURRENCY" --seed "$SEED" --compression-ratio "$COMPRESSION_RATIO" \
  --mem-cache-size "$MEM_CACHE" --disk-cache-size "$DISK_CACHE" \
  --disk-cache-dir "$DISK_CACHE_DIR")
if [[ -n "$COMPRESSION_CODEC" ]]; then
  COMMON+=(--compression-codec "$COMPRESSION_CODEC")
fi

run_phase() {
  local phase=$1; shift
  echo
  echo "================================================================"
  echo "  phase: $phase"
  echo "================================================================"
  "$BIN" --path /db db-bench "${COMMON[@]}" "$phase" "$@"
}

PHASES=("$@")
if [[ ${#PHASES[@]} -eq 0 ]]; then
  PHASES=(load read-random overwrite read-while-writing)
fi

echo "db_bench config: num_keys=$NUM_KEYS key_len=$KEY_LEN val_len=$VAL_LEN \
concurrency=$CONCURRENCY duration=${DURATION}s mem_cache=$((MEM_CACHE / 1024 / 1024))MiB \
disk_cache=$((DISK_CACHE / 1024 / 1024))MiB compression=${COMPRESSION_CODEC:-none}@${COMPRESSION_RATIO} \
backend=$CLOUD_PROVIDER"

for phase in "${PHASES[@]}"; do
  case "$phase" in
    load)
      run_phase load
      ;;
    read-random)
      run_phase read-random --duration "$DURATION" $WARMUP_FLAG
      ;;
    overwrite)
      run_phase overwrite --duration "$DURATION"
      ;;
    read-while-writing)
      run_phase read-while-writing --duration "$DURATION" --mb-per-sec "$MB_PER_SEC" $WARMUP_FLAG
      ;;
    *)
      echo "unknown phase: $phase" >&2
      exit 1
      ;;
  esac
done

echo
echo "db_bench complete."
