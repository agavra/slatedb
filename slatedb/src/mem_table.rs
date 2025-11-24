use std::cell::Cell;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use crate::error::SlateDBError;
use crate::iter::{IterationOrder, KeyValueIterator};
use crate::merge_operator::MergeOperatorType;
use crate::seq_tracker::{SequenceTracker, TrackedSeq};
use crate::types::{RowEntry, ValueDeletable};
use crate::utils::{WatchableOnceCell, WatchableOnceCellReader};

/// Memtable may contains multiple versions of a single user key, with a monotonically increasing sequence number.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SequencedKey {
    pub(crate) user_key: Bytes,
    pub(crate) seq: u64,
}

impl SequencedKey {
    pub fn new(user_key: Bytes, seq: u64) -> Self {
        Self { user_key, seq }
    }
}

impl Ord for SequencedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(self.seq.cmp(&other.seq).reverse())
    }
}

impl PartialOrd for SequencedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KVTableInternalKeyRange {
    start_bound: Bound<SequencedKey>,
    end_bound: Bound<SequencedKey>,
}

impl RangeBounds<SequencedKey> for KVTableInternalKeyRange {
    fn start_bound(&self) -> Bound<&SequencedKey> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> Bound<&SequencedKey> {
        self.end_bound.as_ref()
    }
}

/// Convert a user key range to a memtable internal key range. The internal key range should contain all the sequence
/// numbers for the given user key in the range. This is used for iterating over the memtable in [`KVTable::range`].
///
/// Please note that the sequence number is ordered in reverse, given a user key range (`key001`..=`key001`), the first
/// sequence number in this range is u64::MAX, and the last sequence number is 0. The output range should be
/// `(key001, u64::MAX) ..= (key001, 0)`.
impl<T: RangeBounds<Bytes>> From<T> for KVTableInternalKeyRange {
    fn from(range: T) -> Self {
        let start_bound = match range.start_bound() {
            Bound::Included(key) => Bound::Included(SequencedKey::new(key.clone(), u64::MAX)),
            Bound::Excluded(key) => Bound::Excluded(SequencedKey::new(key.clone(), 0)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(SequencedKey::new(key.clone(), 0)),
            Bound::Excluded(key) => Bound::Excluded(SequencedKey::new(key.clone(), u64::MAX)),
            Bound::Unbounded => Bound::Unbounded,
        };
        Self {
            start_bound,
            end_bound,
        }
    }
}

pub(crate) struct KVTable {
    map: Arc<SkipMap<SequencedKey, RowEntry>>,
    durable: WatchableOnceCell<Result<(), SlateDBError>>,
    entries_size_in_bytes: AtomicUsize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    last_tick: AtomicI64,
    /// the sequence number of the most recent operation on this KVTable
    last_seq: AtomicU64,
    /// A sequence tracker that correlates sequence numbers with system clock ticks.
    /// The tracker is limited to 8192 entries and downsamples data when it gets full.
    sequence_tracker: Mutex<SequenceTracker>,
}

pub(crate) struct KVTableMetadata {
    pub(crate) entry_num: usize,
    pub(crate) entries_size_in_bytes: usize,
    /// this corresponds to the timestamp of the most recent
    /// modifying operation on this KVTable (insertion or deletion)
    #[allow(dead_code)]
    pub(crate) last_tick: i64,
    /// the sequence number of the most recent operation on this KVTable
    #[allow(dead_code)]
    pub(crate) last_seq: u64,
}

pub(crate) struct WritableKVTable {
    table: Arc<KVTable>,
    merge_operator: Option<MergeOperatorType>,
}

impl WritableKVTable {
    pub(crate) fn new(merge_operator: Option<MergeOperatorType>) -> Self {
        Self {
            table: Arc::new(KVTable::new()),
            merge_operator,
        }
    }

    pub(crate) fn table(&self) -> &Arc<KVTable> {
        &self.table
    }

    pub(crate) fn put(&self, row: RowEntry, recent_snapshot_min_seq: u64) {
        self.table
            .put(row, recent_snapshot_min_seq, self.merge_operator.as_ref());
    }

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        self.table.metadata()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub(crate) fn record_sequence(&self, seq: u64, ts: DateTime<Utc>) {
        self.table.record_sequence(seq, ts);
    }
}

pub(crate) struct ImmutableMemtable {
    /// The recent flushed WAL ID when this IMM is freezed. This is used to determine the starting
    /// position of WAL replay during recovery. After an IMM is flushed to L0, we do not need to
    /// care about the earlier WALs which produced this IMM, all we need to know is the recent
    /// WAL ID of the last L0 compacted.
    ///
    /// Please note that this recent flushed WAL ID might not exactly match the last WAL ID that
    /// produced this IMM, we still need to take the last l0's `last_seq` to filter out the entries
    /// that already contained in the last L0 SST.
    recent_flushed_wal_id: u64,
    table: Arc<KVTable>,
    /// This flushed watchable cell is useful for users who enable `await_durable` on the writes.
    flushed: WatchableOnceCell<Result<(), SlateDBError>>,
    /// A snapshot of the sequence tracker taken when this immutable memtable was created.
    /// This avoids needing to access the sequence tracker through a mutex on the underlying table.
    sequence_tracker: SequenceTracker,
}

#[self_referencing]
pub(crate) struct MemTableIteratorInner<T: RangeBounds<SequencedKey>> {
    map: Arc<SkipMap<SequencedKey, RowEntry>>,
    /// `inner` is the Iterator impl of SkipMap, which is the underlying data structure of MemTable.
    #[borrows(map)]
    #[not_covariant]
    inner: Range<'this, SequencedKey, T, SequencedKey, RowEntry>,
    ordering: IterationOrder,
    item: Option<RowEntry>,
}
pub(crate) type MemTableIterator = MemTableIteratorInner<KVTableInternalKeyRange>;

#[async_trait]
impl KeyValueIterator for MemTableIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        Ok(self.next_entry_sync())
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        loop {
            let front = self.borrow_item().clone();
            if front.is_some_and(|record| record.key < next_key) {
                self.next_entry_sync();
            } else {
                return Ok(());
            }
        }
    }
}

impl MemTableIterator {
    pub(crate) fn next_entry_sync(&mut self) -> Option<RowEntry> {
        let ans = self.borrow_item().clone();
        let next_entry = match self.borrow_ordering() {
            IterationOrder::Ascending => self.with_inner_mut(|inner| inner.next()),
            IterationOrder::Descending => self.with_inner_mut(|inner| inner.next_back()),
        };

        let cloned_entry = next_entry.map(|entry| entry.value().clone());
        self.with_item_mut(|item| *item = cloned_entry);

        ans
    }
}

impl ImmutableMemtable {
    pub(crate) fn new(table: WritableKVTable, recent_flushed_wal_id: u64) -> Self {
        let sequence_tracker = table.table.sequence_tracker_snapshot();
        Self {
            table: table.table,
            recent_flushed_wal_id,
            flushed: WatchableOnceCell::new(),
            sequence_tracker,
        }
    }

    pub(crate) fn table(&self) -> Arc<KVTable> {
        self.table.clone()
    }

    pub(crate) fn recent_flushed_wal_id(&self) -> u64 {
        self.recent_flushed_wal_id
    }

    pub(crate) async fn await_flush_to_l0(&self) -> Result<(), SlateDBError> {
        self.flushed.reader().await_value().await
    }

    pub(crate) fn notify_flush_to_l0(&self, result: Result<(), SlateDBError>) {
        self.flushed.write(result);
    }

    pub(crate) fn sequence_tracker(&self) -> &SequenceTracker {
        &self.sequence_tracker
    }
}

impl KVTable {
    pub(crate) fn new() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            entries_size_in_bytes: AtomicUsize::new(0),
            durable: WatchableOnceCell::new(),
            last_tick: AtomicI64::new(i64::MIN),
            last_seq: AtomicU64::new(0),
            sequence_tracker: Mutex::new(SequenceTracker::new()),
        }
    }

    pub(crate) fn metadata(&self) -> KVTableMetadata {
        let entry_num = self.map.len();
        let entries_size_in_bytes = self.entries_size_in_bytes.load(Ordering::Relaxed);
        let last_tick = self.last_tick.load(SeqCst);
        let last_seq = self.last_seq.load(SeqCst);
        KVTableMetadata {
            entry_num,
            entries_size_in_bytes,
            last_tick,
            last_seq,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn last_tick(&self) -> i64 {
        self.last_tick.load(SeqCst)
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let last_seq = self.last_seq.load(SeqCst);
            Some(last_seq)
        }
    }

    pub(crate) fn iter(&self) -> MemTableIterator {
        self.range_ascending(..)
    }

    pub(crate) fn range_ascending<T: RangeBounds<Bytes>>(&self, range: T) -> MemTableIterator {
        self.range(range, IterationOrder::Ascending)
    }

    pub(crate) fn range<T: RangeBounds<Bytes>>(
        &self,
        range: T,
        ordering: IterationOrder,
    ) -> MemTableIterator {
        let internal_range = KVTableInternalKeyRange::from(range);
        let mut iterator = MemTableIteratorInnerBuilder {
            map: self.map.clone(),
            inner_builder: |map| map.range(internal_range),
            ordering,
            item: None,
        }
        .build();
        iterator.next_entry_sync();
        iterator
    }

    pub(crate) fn put(
        &self,
        row: RowEntry,
        recent_snapshot_min_seq: u64,
        merge_operator: Option<&MergeOperatorType>,
    ) {
        let user_key = row.key.clone();
        let is_merge = matches!(row.value, ValueDeletable::Merge(_));
        let row_seq = row.seq;
        let internal_key = SequencedKey::new(user_key.clone(), row_seq);
        let previous_size = Cell::new(None);

        // it is safe to use fetch_max here to update the last tick
        // because the monotonicity is enforced when generating the clock tick
        // (see [crate::utils::MonotonicClock::now])
        if let Some(create_ts) = row.create_ts {
            self.last_tick
                .fetch_max(create_ts, atomic::Ordering::SeqCst);
        }
        // update the last seq number if it is greater than the current last seq
        self.last_seq.fetch_max(row.seq, atomic::Ordering::SeqCst);

        let row_size = row.estimated_size();
        self.map.compare_insert(internal_key, row, |previous_row| {
            // Optimistically calculate the size of the previous value.
            // `compare_fn` might be called multiple times in case of concurrent
            // writes to the same key, so we use `Cell` to avoid subtracting
            // the size multiple times. The last call will set the correct size.
            previous_size.set(Some(previous_row.estimated_size()));
            true
        });
        if let Some(size) = previous_size.take() {
            self.entries_size_in_bytes
                .fetch_sub(size, Ordering::Relaxed);
            self.entries_size_in_bytes
                .fetch_add(row_size, Ordering::Relaxed);
        } else {
            self.entries_size_in_bytes
                .fetch_add(row_size, Ordering::Relaxed);
        }

        // Eager merge: if this is a merge operand and we have a merge operator,
        // merge older entries below the snapshot watermark (excluding the one we just inserted)
        if is_merge {
            if let Some(merge_op) = merge_operator {
                self.eager_merge_entries(&user_key, row_seq, recent_snapshot_min_seq, merge_op);
            }
        }
    }

    /// Eagerly merge entries for the given key that are below the snapshot watermark.
    /// This is called after inserting a new merge operand to combine it with older entries
    /// that are no longer visible to any active snapshot.
    fn eager_merge_entries(
        &self,
        key: &Bytes,
        exclude_seq: u64,
        recent_snapshot_min_seq: u64,
        merge_operator: &MergeOperatorType,
    ) {
        // Find all entries for this key with seq < recent_snapshot_min_seq
        // These are safe to merge because no snapshot can see them
        let mut entries_to_merge = Vec::new();
        let mut total_size_to_remove = 0usize;

        // Range over all entries for this key (newest to oldest due to seq ordering)
        let range = self
            .map
            .range(KVTableInternalKeyRange::from(key.clone()..=key.clone()));

        for entry in range {
            let sequenced_key = entry.key();
            let row_entry = entry.value();

            // Only collect entries below the watermark, excluding the just-inserted entry
            if sequenced_key.seq < recent_snapshot_min_seq && sequenced_key.seq != exclude_seq {
                total_size_to_remove += row_entry.estimated_size();
                entries_to_merge.push(row_entry.clone());
            }
        }

        // Need at least one entry to potentially merge
        if entries_to_merge.is_empty() {
            return;
        }

        // Entries are in newest-to-oldest order, reverse to get oldest-to-newest
        entries_to_merge.reverse();

        // Apply merge logic manually (same approach as MergeOperatorIterator but synchronous)
        // Separate base value from merge operands
        let mut base_value: Option<Bytes> = None;
        let mut merge_operands: Vec<Bytes> = Vec::new();
        let mut oldest_seq = u64::MAX;
        let mut max_create_ts: Option<i64> = None;
        let mut min_expire_ts: Option<i64> = None;

        for entry in &entries_to_merge {
            oldest_seq = oldest_seq.min(entry.seq);

            // Track max create_ts and min expire_ts (same as MergeOperatorIterator)
            max_create_ts = match (max_create_ts, entry.create_ts) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            min_expire_ts = match (min_expire_ts, entry.expire_ts) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };

            match &entry.value {
                ValueDeletable::Value(v) => {
                    if base_value.is_some() {
                        // Multiple base values shouldn't happen, abort
                        return;
                    }
                    base_value = Some(v.clone());
                }
                ValueDeletable::Merge(m) => {
                    merge_operands.push(m.clone());
                }
                ValueDeletable::Tombstone => {
                    // Tombstone acts as base=None, stop collecting
                    break;
                }
            }
        }

        // Only proceed if we have merge operands to apply
        if merge_operands.is_empty() {
            return;
        }

        // Apply the merge operator (reusing MergeOperator trait's merge_batch)
        if let Ok(merged_value) = merge_operator.merge_batch(key, base_value, &merge_operands) {
            // Remove all the old entries
            for entry in &entries_to_merge {
                let old_key = SequencedKey::new(key.clone(), entry.seq);
                self.map.remove(&old_key);
            }

            // Insert the merged entry
            let merged_entry = RowEntry {
                key: key.clone(),
                value: ValueDeletable::Value(merged_value),
                seq: oldest_seq,
                create_ts: max_create_ts,
                expire_ts: min_expire_ts,
            };

            let merged_size = merged_entry.estimated_size();
            let merged_key = SequencedKey::new(key.clone(), oldest_seq);
            self.map.insert(merged_key, merged_entry);

            // Update size tracking
            self.entries_size_in_bytes
                .fetch_sub(total_size_to_remove, Ordering::Relaxed);
            self.entries_size_in_bytes
                .fetch_add(merged_size, Ordering::Relaxed);
        }
    }

    pub(crate) fn durable_watcher(&self) -> WatchableOnceCellReader<Result<(), SlateDBError>> {
        self.durable.reader()
    }

    pub(crate) async fn await_durable(&self) -> Result<(), SlateDBError> {
        self.durable.reader().await_value().await
    }

    pub(crate) fn notify_durable(&self, result: Result<(), SlateDBError>) {
        self.durable.write(result);
    }

    pub(crate) fn record_sequence(&self, seq: u64, ts: DateTime<Utc>) {
        let mut tracker = self.sequence_tracker.lock();
        tracker.insert(TrackedSeq { seq, ts });
    }

    pub(crate) fn sequence_tracker_snapshot(&self) -> SequenceTracker {
        self.sequence_tracker.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::bytes_range::BytesRange;
    use crate::merge_iterator::MergeIterator;
    use crate::proptest_util::{arbitrary, sample};
    use crate::test_utils::assert_iterator;
    use crate::{proptest_util, test_utils};
    use rstest::rstest;
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn test_memtable_iter() {
        let table = WritableKVTable::new(None);
        table.put(RowEntry::new_value(b"abc333", b"value3", 1), 0);
        table.put(RowEntry::new_value(b"abc111", b"value1", 2), 0);
        table.put(RowEntry::new_value(b"abc555", b"value5", 3), 0);
        table.put(RowEntry::new_value(b"abc444", b"value4", 4), 0);
        table.put(RowEntry::new_value(b"abc222", b"value2", 5), 0);
        assert_eq!(table.table().last_seq(), Some(5));

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc222", b"value2", 5),
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_entry_attrs() {
        let table = WritableKVTable::new(None);
        table.put(RowEntry::new_value(b"abc333", b"value3", 1), 0);
        table.put(RowEntry::new_value(b"abc111", b"value1", 2), 0);

        let mut iter = table.table().iter();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc111", b"value1", 2),
                RowEntry::new_value(b"abc333", b"value3", 1),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_existing_key() {
        let table = WritableKVTable::new(None);
        table.put(RowEntry::new_value(b"abc333", b"value3", 1), 0);
        table.put(RowEntry::new_value(b"abc111", b"value1", 2), 0);
        table.put(RowEntry::new_value(b"abc555", b"value5", 3), 0);
        table.put(RowEntry::new_value(b"abc444", b"value4", 4), 0);
        table.put(RowEntry::new_value(b"abc222", b"value2", 5), 0);

        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc333")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc333", b"value3", 1),
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_range_from_nonexisting_key() {
        let table = WritableKVTable::new(None);
        table.put(RowEntry::new_value(b"abc333", b"value3", 1), 0);
        table.put(RowEntry::new_value(b"abc111", b"value1", 2), 0);
        table.put(RowEntry::new_value(b"abc555", b"value5", 3), 0);
        table.put(RowEntry::new_value(b"abc444", b"value4", 4), 0);
        table.put(RowEntry::new_value(b"abc222", b"value2", 5), 0);

        let mut iter = table
            .table()
            .range_ascending(BytesRange::from(Bytes::from_static(b"abc334")..));
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"abc444", b"value4", 4),
                RowEntry::new_value(b"abc555", b"value5", 3),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_iter_delete() {
        let table = WritableKVTable::new(None);
        table.put(RowEntry::new_tombstone(b"abc333", 2), 0);
        table.put(RowEntry::new_value(b"abc333", b"value3", 1), 0);
        table.put(RowEntry::new_value(b"abc444", b"value4", 4), 0);

        // in merge iterator, it should only return one entry
        let iter = table.table().iter();
        let mut merge_iter = MergeIterator::new(VecDeque::from(vec![iter])).unwrap();
        assert_iterator(
            &mut merge_iter,
            vec![
                RowEntry::new_tombstone(b"abc333", 2),
                RowEntry::new_value(b"abc444", b"value4", 4),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_memtable_track_sz_and_num() {
        let table = WritableKVTable::new(None);
        let mut metadata = table.table().metadata();

        assert_eq!(metadata.entry_num, 0);
        assert_eq!(metadata.entries_size_in_bytes, 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 1);
        assert_eq!(metadata.entries_size_in_bytes, 16);

        table.put(RowEntry::new_tombstone(b"first", 2), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 2);
        assert_eq!(metadata.entries_size_in_bytes, 29);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 3);
        assert_eq!(metadata.entries_size_in_bytes, 47);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 4);
        assert_eq!(metadata.entries_size_in_bytes, 70);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 5);
        assert_eq!(metadata.entries_size_in_bytes, 90);

        table.put(RowEntry::new_tombstone(b"abc333", 4), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.entry_num, 6);
        assert_eq!(metadata.entries_size_in_bytes, 104);
    }

    #[tokio::test]
    async fn test_memtable_track_last_seq() {
        let table = WritableKVTable::new(None);
        let mut metadata = table.table().metadata();

        assert_eq!(metadata.last_seq, 0);
        table.put(RowEntry::new_value(b"first", b"foo", 1), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 1);

        table.put(RowEntry::new_tombstone(b"first", 2), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);

        table.put(RowEntry::new_value(b"abc333", b"val1", 1), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);

        table.put(RowEntry::new_value(b"def456", b"blablabla", 2), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 2);

        table.put(RowEntry::new_value(b"def456", b"blabla", 3), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 3);

        table.put(RowEntry::new_tombstone(b"abc333", 4), 0);
        metadata = table.table().metadata();
        assert_eq!(metadata.last_seq, 4);
    }

    #[rstest]
    #[case(
        BytesRange::from(..),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Unbounded,
        },
        vec![SequencedKey::new(Bytes::from_static(b"abc111"), 1)],
        vec![]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc111")..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc111"), u64::MAX)),
            end_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc111"), 1),
            SequencedKey::new(Bytes::from_static(b"abc222"), 2),
            SequencedKey::new(Bytes::from_static(b"abc333"), 3),
            SequencedKey::new(Bytes::from_static(b"abc333"), 0),
            SequencedKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![SequencedKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    #[case(
        BytesRange::from(Bytes::from_static(b"abc222")..Bytes::from_static(b"abc444")),
        KVTableInternalKeyRange {
            start_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc222"), u64::MAX)),
            end_bound: Bound::Excluded(SequencedKey::new(Bytes::from_static(b"abc444"), u64::MAX)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc222"), 1),
            SequencedKey::new(Bytes::from_static(b"abc333"), 2),
        ],
        vec![
            SequencedKey::new(Bytes::from_static(b"abc444"), 0),
            SequencedKey::new(Bytes::from_static(b"abc444"), u64::MAX),
            SequencedKey::new(Bytes::from_static(b"abc555"), u64::MAX),
        ]
    )]
    #[case(
        BytesRange::from(..=Bytes::from_static(b"abc333")),
        KVTableInternalKeyRange {
            start_bound: Bound::Unbounded,
            end_bound: Bound::Included(SequencedKey::new(Bytes::from_static(b"abc333"), 0)),
        },
        vec![
            SequencedKey::new(Bytes::from_static(b"abc111"), 1),
            SequencedKey::new(Bytes::from_static(b"abc222"), 2),
            SequencedKey::new(Bytes::from_static(b"abc333"), 3),
            SequencedKey::new(Bytes::from_static(b"abc333"), u64::MAX),
        ],
        vec![SequencedKey::new(Bytes::from_static(b"abc444"), 4)]
    )]
    fn test_from_internal_key_range(
        #[case] range: BytesRange,
        #[case] expected: KVTableInternalKeyRange,
        #[case] should_contains: Vec<SequencedKey>,
        #[case] should_not_contains: Vec<SequencedKey>,
    ) {
        let range = KVTableInternalKeyRange::from(range);
        assert_eq!(range, expected);
        for key in should_contains {
            assert!(range.contains(&key));
        }
        for key in should_not_contains {
            assert!(!range.contains(&key));
        }
    }

    #[test]
    fn should_iterate_arbitrary_range() {
        let mut runner = proptest_util::runner::new(file!(), None);
        let runtime = Runtime::new().unwrap();
        let sample_table = sample::table(runner.rng(), 500, 10);

        let kv_table = WritableKVTable::new(None);
        let mut seq = 1;
        for (key, value) in &sample_table {
            let row_entry = RowEntry::new_value(key, value, seq);
            kv_table.put(row_entry, 0);
            seq += 1;
        }

        runner
            .run(
                &(arbitrary::nonempty_range(10), arbitrary::iteration_order()),
                |(range, ordering)| {
                    let mut kv_iter = kv_table.table.range(range.clone(), ordering);

                    runtime.block_on(test_utils::assert_ranged_kv_scan(
                        &sample_table,
                        &range,
                        ordering,
                        &mut kv_iter,
                    ));
                    Ok(())
                },
            )
            .unwrap();
    }

    // Test helper: simple merge operator that concatenates strings
    struct TestMergeOperator;

    impl crate::merge_operator::MergeOperator for TestMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operand: Bytes,
        ) -> Result<Bytes, crate::merge_operator::MergeOperatorError> {
            match existing_value {
                Some(base) => {
                    let mut merged = base.to_vec();
                    merged.extend_from_slice(&operand);
                    Ok(Bytes::from(merged))
                }
                None => Ok(operand),
            }
        }

        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Result<Bytes, crate::merge_operator::MergeOperatorError> {
            let mut result = existing_value.map(|b| b.to_vec()).unwrap_or_default();
            for operand in operands {
                result.extend_from_slice(operand);
            }
            Ok(Bytes::from(result))
        }
    }

    #[tokio::test]
    async fn should_eagerly_merge_entries_below_watermark() {
        // given: a memtable with a merge operator
        let merge_operator = Arc::new(TestMergeOperator);
        let table = WritableKVTable::new(Some(merge_operator));

        // when: insert a base value and two merge operands, all below the watermark
        table.put(RowEntry::new_value(b"key1", b"base", 1), 100);
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_merge1")),
                seq: 2,
                create_ts: None,
                expire_ts: None,
            },
            100,
        );
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_merge2")),
                seq: 3,
                create_ts: None,
                expire_ts: None,
            },
            100,
        );

        // then: the old entries should be merged into one
        let mut iter = table.table().iter();
        let entries: Vec<RowEntry> = std::iter::from_fn(|| iter.next_entry_sync()).collect();

        // Should have only 2 entries: the merged result (seq=1) and the latest merge (seq=3)
        assert_eq!(entries.len(), 2);

        // The latest entry (seq=3) should still be a merge operand
        assert_eq!(entries[0].seq, 3);
        assert!(matches!(entries[0].value, ValueDeletable::Merge(_)));

        // The older merged entry should have seq=1 (oldest) and contain merged data
        assert_eq!(entries[1].seq, 1);
        if let ValueDeletable::Value(ref v) = entries[1].value {
            assert_eq!(v.as_ref(), b"base_merge1");
        } else {
            panic!("Expected merged value");
        }
    }

    #[tokio::test]
    async fn should_not_merge_entries_above_watermark() {
        // given: a memtable with a merge operator
        let merge_operator = Arc::new(TestMergeOperator);
        let table = WritableKVTable::new(Some(merge_operator));

        // when: insert entries where some are above the watermark (recent_snapshot_min_seq=2)
        table.put(RowEntry::new_value(b"key1", b"base", 1), 2);
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_merge1")),
                seq: 2,
                create_ts: None,
                expire_ts: None,
            },
            2,
        );
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_merge2")),
                seq: 3,
                create_ts: None,
                expire_ts: None,
            },
            2,
        );

        // then: entries above watermark should not be merged
        let mut iter = table.table().iter();
        let entries: Vec<RowEntry> = std::iter::from_fn(|| iter.next_entry_sync()).collect();

        // Should have 3 separate entries since seq=2 and seq=3 are not below watermark
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq, 3);
        assert_eq!(entries[1].seq, 2);
        assert_eq!(entries[2].seq, 1);
    }

    #[tokio::test]
    async fn should_not_merge_without_merge_operator() {
        // given: a memtable without a merge operator
        let table = WritableKVTable::new(None);

        // when: insert merge operands
        table.put(RowEntry::new_value(b"key1", b"base", 1), 100);
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_merge1")),
                seq: 2,
                create_ts: None,
                expire_ts: None,
            },
            100,
        );

        // then: entries should not be merged
        let mut iter = table.table().iter();
        let entries: Vec<RowEntry> = std::iter::from_fn(|| iter.next_entry_sync()).collect();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 2);
        assert_eq!(entries[1].seq, 1);
    }

    #[tokio::test]
    async fn should_update_size_tracking_after_eager_merge() {
        // given: a memtable with a merge operator
        let merge_operator = Arc::new(TestMergeOperator);
        let table = WritableKVTable::new(Some(merge_operator));

        // when: insert entries that will be eagerly merged
        // Insert base, then two merge operands - the third insert should trigger a merge
        table.put(RowEntry::new_value(b"key1", b"base", 1), 100);
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_m1")),
                seq: 2,
                create_ts: None,
                expire_ts: None,
            },
            100,
        );
        table.put(
            RowEntry {
                key: Bytes::from_static(b"key1"),
                value: ValueDeletable::Merge(Bytes::from_static(b"_m2")),
                seq: 3,
                create_ts: None,
                expire_ts: None,
            },
            100,
        );

        // then: size should be updated correctly after merge
        let final_size = table.metadata().entries_size_in_bytes;

        // After inserting seq=3, seq=1 and seq=2 should be merged together
        // We should have: seq=3 (merge="_m2") + seq=1 (merged="base_m1")
        let merged_entry = RowEntry::new_value(b"key1", b"base_m1", 1);
        let latest_merge = RowEntry {
            key: Bytes::from_static(b"key1"),
            value: ValueDeletable::Merge(Bytes::from_static(b"_m2")),
            seq: 3,
            create_ts: None,
            expire_ts: None,
        };
        assert_eq!(
            final_size,
            merged_entry.estimated_size() + latest_merge.estimated_size()
        );
    }
}
