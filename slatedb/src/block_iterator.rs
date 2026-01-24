use std::cmp::Ordering;
use std::sync::Arc;

use crate::iter::IterationOrder;
use crate::iter::IterationOrder::Ascending;
use crate::row_codec::SstRowCodecV0;
use crate::{block::Block, error::SlateDBError, iter::KeyValueIterator, types::RowEntry};
use async_trait::async_trait;
use bytes::Bytes;
use IterationOrder::Descending;

pub(crate) trait BlockLike: Send + Sync {
    fn data(&self) -> &Bytes;
    fn restarts(&self) -> &[u16];
}

impl BlockLike for Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn restarts(&self) -> &[u16] {
        &self.restarts
    }
}

impl BlockLike for &Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn restarts(&self) -> &[u16] {
        &self.restarts
    }
}

impl BlockLike for Arc<Block> {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn restarts(&self) -> &[u16] {
        &self.restarts
    }
}

pub(crate) struct BlockIterator<B: BlockLike> {
    block: B,
    /// Byte offset within the block data for forward iteration
    entry_offset: usize,
    /// Index into the block for counting entries (used for descending iteration)
    entry_index: usize,
    /// Total number of entries in the block (computed lazily for descending)
    num_entries: Option<usize>,
    /// Current key cached for prefix restoration
    current_key: Bytes,
    /// Current restart index (which restart interval we're in)
    current_restart_idx: usize,
    ordering: IterationOrder,
}

#[async_trait]
impl<B: BlockLike> KeyValueIterator for BlockIterator<B> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match self.ordering {
            Ascending => self.next_entry_ascending(),
            Descending => self.next_entry_descending(),
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let num_restarts = self.block.restarts().len();
        if num_restarts == 0 {
            return Ok(());
        }

        // Forward-only seek: if current position is already >= target, don't go backwards
        if !self.current_key.is_empty() && self.current_key.as_ref() >= next_key {
            return Ok(());
        }

        // Binary search on restart points to find the interval containing the target key
        // Start from current restart index for forward-only seeking
        let mut low = self.current_restart_idx;
        let mut high = num_restarts;

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_key = self.decode_key_at_restart(mid)?;

            match mid_key.as_ref().cmp(next_key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Equal | Ordering::Greater => {
                    high = mid;
                }
            }
        }

        // Position at the restart point before or at the target
        // If low > current_restart_idx, we need to seek to a later restart point
        // Otherwise stay at current restart interval and scan forward
        let restart_idx = if low > self.current_restart_idx {
            if low > 0 {
                low - 1
            } else {
                0
            }
        } else {
            self.current_restart_idx
        };

        // Only seek to restart if it's ahead of our current position
        if restart_idx > self.current_restart_idx {
            self.seek_to_restart(restart_idx)?;
        }

        // Linear scan within the restart interval to find the exact position
        loop {
            if self.entry_offset >= self.block.data().len() {
                break;
            }

            // If current key is already >= target, we're done
            if self.current_key.as_ref() >= next_key {
                break;
            }

            // Decode next entry to advance position
            let mut cursor = self.block.data().slice(self.entry_offset..);
            let codec = SstRowCodecV0::new();
            let prev_key = self.current_key.clone();
            let sst_row = codec.decode(&mut cursor)?;
            let entry_len = self.block.data().len() - self.entry_offset - cursor.len();
            self.entry_offset += entry_len;
            self.entry_index += 1;
            self.current_key = sst_row.restore_full_key(&prev_key);

            // Update restart index if we've crossed into a new restart interval
            let restarts = self.block.restarts();
            while self.current_restart_idx + 1 < restarts.len()
                && self.entry_offset >= restarts[self.current_restart_idx + 1] as usize
            {
                self.current_restart_idx += 1;
            }

            // Check if we've reached or passed the target
            if self.current_key.as_ref() >= next_key {
                // Back up one entry - we need to return this one
                self.entry_offset -= entry_len;
                self.entry_index -= 1;
                self.current_key = prev_key;
                break;
            }
        }

        Ok(())
    }
}

impl<B: BlockLike> BlockIterator<B> {
    pub(crate) fn new(block: B, ordering: IterationOrder) -> Self {
        // Decode first key at first restart point (always starts at offset 0)
        let first_key = if block.data().is_empty() {
            Bytes::new()
        } else {
            Self::decode_first_key_static(block.data())
        };

        BlockIterator {
            block,
            entry_offset: 0,
            entry_index: 0,
            num_entries: None,
            current_key: first_key,
            current_restart_idx: 0,
            ordering,
        }
    }

    pub(crate) fn new_ascending(block: B) -> Self {
        Self::new(block, Ascending)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entry_offset >= self.block.data().len()
    }

    /// Get the next entry in ascending order
    fn next_entry_ascending(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.entry_offset >= self.block.data().len() {
            return Ok(None);
        }

        let mut cursor = self.block.data().slice(self.entry_offset..);
        let codec = SstRowCodecV0::new();
        let sst_row = codec.decode(&mut cursor)?;

        // Restore full key using current_key as prefix
        let full_key = sst_row.restore_full_key(&self.current_key);

        // Calculate how many bytes we consumed
        let entry_len = self.block.data().len() - self.entry_offset - cursor.len();
        self.entry_offset += entry_len;
        self.entry_index += 1;

        // Update current_key for the next entry
        self.current_key = full_key.clone();

        // Update restart index if we've crossed into a new restart interval
        let restarts = self.block.restarts();
        while self.current_restart_idx + 1 < restarts.len()
            && self.entry_offset >= restarts[self.current_restart_idx + 1] as usize
        {
            self.current_restart_idx += 1;
        }

        Ok(Some(RowEntry::new(
            full_key,
            sst_row.value,
            sst_row.seq,
            sst_row.create_ts,
            sst_row.expire_ts,
        )))
    }

    /// Get the next entry in descending order
    fn next_entry_descending(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        // For descending iteration, we need to know total number of entries
        // We compute this lazily by scanning the block once
        if self.num_entries.is_none() {
            self.num_entries = Some(self.count_entries()?);
            // Reset to scan from beginning to target position
            self.entry_offset = 0;
            self.entry_index = 0;
            self.current_key = Self::decode_first_key_static(self.block.data());
            self.current_restart_idx = 0;
        }

        let total = self.num_entries.unwrap();
        if self.entry_index >= total {
            return Ok(None);
        }

        // Target index for descending: total - 1, total - 2, etc.
        let target_idx = total - 1 - self.entry_index;

        // Reset to beginning and scan to target position
        let mut scan_offset = 0;
        let mut scan_key = Self::decode_first_key_static(self.block.data());
        let mut result_entry = None;

        let codec = SstRowCodecV0::new();
        for i in 0..=target_idx {
            let mut cursor = self.block.data().slice(scan_offset..);
            let sst_row = codec.decode(&mut cursor)?;
            let full_key = sst_row.restore_full_key(&scan_key);

            if i == target_idx {
                result_entry = Some(RowEntry::new(
                    full_key.clone(),
                    sst_row.value,
                    sst_row.seq,
                    sst_row.create_ts,
                    sst_row.expire_ts,
                ));
            }

            let entry_len = self.block.data().len() - scan_offset - cursor.len();
            scan_offset += entry_len;
            scan_key = full_key;
        }

        self.entry_index += 1;
        Ok(result_entry)
    }

    /// Count total entries in the block by scanning through
    fn count_entries(&self) -> Result<usize, SlateDBError> {
        let mut count = 0;
        let mut offset = 0;
        let mut prev_key = Self::decode_first_key_static(self.block.data());
        let codec = SstRowCodecV0::new();

        while offset < self.block.data().len() {
            let mut cursor = self.block.data().slice(offset..);
            let sst_row = codec.decode(&mut cursor)?;
            let full_key = sst_row.restore_full_key(&prev_key);
            let entry_len = self.block.data().len() - offset - cursor.len();
            offset += entry_len;
            prev_key = full_key;
            count += 1;
        }
        Ok(count)
    }

    /// Decode the first key in the block (at offset 0)
    fn decode_first_key_static(data: &Bytes) -> Bytes {
        let mut cursor = data.slice(..);
        let codec = SstRowCodecV0::new();
        // The first entry has key_prefix_len = 0, so we can decode it with empty prefix
        let sst_row = codec
            .decode(&mut cursor)
            .expect("Failed to decode first key");
        sst_row.restore_full_key(&Bytes::new())
    }

    /// Decode the key at a restart point (for binary search)
    fn decode_key_at_restart(&self, restart_idx: usize) -> Result<Bytes, SlateDBError> {
        let restart_offset = self.block.restarts()[restart_idx] as usize;
        let mut cursor = self.block.data().slice(restart_offset..);
        let codec = SstRowCodecV0::new();
        // At restart points, key_prefix_len = 0, so we can decode with empty prefix
        let sst_row = codec.decode(&mut cursor)?;
        Ok(sst_row.restore_full_key(&Bytes::new()))
    }

    /// Seek to a specific restart point
    fn seek_to_restart(&mut self, restart_idx: usize) -> Result<(), SlateDBError> {
        let restart_offset = self.block.restarts()[restart_idx] as usize;
        self.entry_offset = restart_offset;
        self.current_restart_idx = restart_idx;

        // Decode the key at this restart point
        let mut cursor = self.block.data().slice(restart_offset..);
        let codec = SstRowCodecV0::new();
        let sst_row = codec.decode(&mut cursor)?;
        self.current_key = sst_row.restore_full_key(&Bytes::new());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::block_iterator::BlockIterator;
    use crate::bytes_range::BytesRange;
    use crate::iter::KeyValueIterator;
    use crate::proptest_util::{arbitrary, sample};
    use crate::test_utils::{assert_iterator, assert_next_entry, gen_attrs, gen_empty_attrs};
    use crate::types::RowEntry;
    use crate::{proptest_util, test_utils};
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn test_iterator() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"donkey", b"kong");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_to_existing_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"kratos").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_to_nonexisting_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"ka").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_to_key_beyond_last_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"zzz").await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_to_key_skips_records_prior_to_next_key() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_empty_attrs()));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_empty_attrs()));
        assert!(block_builder.add_value(b"super", b"mario", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_next_entry(&mut iter, &RowEntry::new_value(b"donkey", b"kong", 0)).await;
        iter.seek(b"s").await.unwrap();
        assert_iterator(&mut iter, vec![RowEntry::new_value(b"super", b"mario", 0)]).await;
    }

    #[tokio::test]
    async fn test_seek_to_key_with_iterator_at_seek_point() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_empty_attrs()));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_empty_attrs()));
        assert!(block_builder.add_value(b"super", b"mario", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        assert_next_entry(&mut iter, &RowEntry::new_value(b"donkey", b"kong", 0)).await;
        iter.seek(b"kratos").await.unwrap();
        assert_iterator(
            &mut iter,
            vec![
                RowEntry::new_value(b"kratos", b"atreus", 0),
                RowEntry::new_value(b"super", b"mario", 0),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_seek_to_key_beyond_last_key_in_block() {
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"donkey", b"kong", gen_attrs(1)));
        assert!(block_builder.add_value(b"kratos", b"atreus", gen_attrs(2)));
        assert!(block_builder.add_value(b"super", b"mario", gen_attrs(3)));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);
        iter.seek(b"zelda".as_ref()).await.unwrap();
        assert_iterator(&mut iter, Vec::new()).await;
    }

    #[test]
    fn should_iterate_arbitrary_range() {
        let mut runner = proptest_util::runner::new(file!(), None);
        let runtime = Runtime::new().unwrap();
        let sample_table = sample::table(runner.rng(), 5, 10);

        let mut block_builder = BlockBuilder::new(1024);
        for (key, value) in &sample_table {
            block_builder.add_value(key, value, gen_empty_attrs());
        }
        let block = Arc::new(block_builder.build().unwrap());

        runner
            .run(&arbitrary::iteration_order(), |ordering| {
                let mut iter = BlockIterator::new(block.clone(), ordering);
                runtime.block_on(test_utils::assert_ranged_kv_scan(
                    &sample_table,
                    &BytesRange::from(..),
                    ordering,
                    &mut iter,
                ));
                Ok(())
            })
            .unwrap();
    }

    // ----- Binary search tests -----

    #[tokio::test]
    async fn should_binary_search_in_large_block() {
        // given: a block with many entries
        let mut block_builder = BlockBuilder::new(16384);
        for i in 0..100u32 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{}", i);
            assert!(block_builder.add_value(key.as_bytes(), value.as_bytes(), gen_empty_attrs()));
        }
        let block = block_builder.build().unwrap();

        // when: seeking to various keys
        // then: the correct entries are returned
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"key_00050").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"key_00050", b"value_50");

        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"key_00099").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"key_00099", b"value_99");

        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"key_00000").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"key_00000", b"value_0");
    }

    #[tokio::test]
    async fn should_seek_to_first_key_in_block() {
        // given: a block with multiple entries
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"apple", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"banana", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"cherry", b"3", gen_empty_attrs()));
        let block = block_builder.build().unwrap();

        // when: seeking to the first key
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"apple").await.unwrap();

        // then: the first entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"apple", b"1");
    }

    #[tokio::test]
    async fn should_seek_to_last_key_in_block() {
        // given: a block with multiple entries
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"apple", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"banana", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"cherry", b"3", gen_empty_attrs()));
        let block = block_builder.build().unwrap();

        // when: seeking to the last key
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"cherry").await.unwrap();

        // then: the last entry is returned and iteration ends
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"cherry", b"3");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_to_key_before_first() {
        // given: a block with entries
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"banana", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"cherry", b"3", gen_empty_attrs()));
        let block = block_builder.build().unwrap();

        // when: seeking to a key before the first entry
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"apple").await.unwrap();

        // then: the first entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"banana", b"2");
    }

    #[tokio::test]
    async fn should_seek_with_shared_prefix_keys() {
        // given: a block with keys that share prefixes (tests prefix encoding interaction)
        let mut block_builder = BlockBuilder::new(4096);
        assert!(block_builder.add_value(b"user:1000", b"alice", gen_empty_attrs()));
        assert!(block_builder.add_value(b"user:1001", b"bob", gen_empty_attrs()));
        assert!(block_builder.add_value(b"user:1002", b"carol", gen_empty_attrs()));
        assert!(block_builder.add_value(b"user:1010", b"dave", gen_empty_attrs()));
        assert!(block_builder.add_value(b"user:1020", b"eve", gen_empty_attrs()));
        let block = block_builder.build().unwrap();

        // when: seeking to various keys with shared prefixes
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"user:1001").await.unwrap();

        // then: correct entry is found
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"user:1001", b"bob");

        // when: seeking to a key between entries
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"user:1005").await.unwrap();

        // then: the next entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"user:1010", b"dave");
    }

    #[tokio::test]
    async fn should_seek_multiple_times_sequentially() {
        // given: a block with multiple entries
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"a", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"b", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"c", b"3", gen_empty_attrs()));
        assert!(block_builder.add_value(b"d", b"4", gen_empty_attrs()));
        assert!(block_builder.add_value(b"e", b"5", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);

        // when/then: multiple sequential seeks work correctly
        iter.seek(b"b").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"b", b"2");

        iter.seek(b"d").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"d", b"4");

        iter.seek(b"e").await.unwrap();
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"e", b"5");
    }

    #[tokio::test]
    async fn should_seek_forward_only_from_current_position() {
        // given: a block with entries and an iterator advanced past the first entry
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"a", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"b", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"c", b"3", gen_empty_attrs()));
        assert!(block_builder.add_value(b"d", b"4", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(block);

        // advance past "a" and "b"
        iter.next().await.unwrap();
        iter.next().await.unwrap();

        // when: seeking to a key before current position
        iter.seek(b"a").await.unwrap();

        // then: seek does not go backwards, returns current position ("c")
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"c", b"3");
    }

    #[tokio::test]
    async fn should_seek_in_single_entry_block() {
        // given: a block with only one entry
        let mut block_builder = BlockBuilder::new(1024);
        assert!(block_builder.add_value(b"only", b"one", gen_empty_attrs()));
        let block = block_builder.build().unwrap();

        // when: seeking to the exact key
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"only").await.unwrap();

        // then: the entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"only", b"one");

        // when: seeking to a key before it
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"aaa").await.unwrap();

        // then: the entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"only", b"one");

        // when: seeking to a key after it
        let mut iter = BlockIterator::new_ascending(&block);
        iter.seek(b"zzz").await.unwrap();

        // then: no entries remain
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_decode_key_at_restart_correctly() {
        // given: a block with entries where first entry is at a restart point
        let mut block_builder = BlockBuilder::new(4096);
        assert!(block_builder.add_value(b"prefix_aaa", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"prefix_bbb", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"prefix_ccc", b"3", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let iter = BlockIterator::new_ascending(&block);

        // when: decoding the key at restart point 0
        // then: first key is correctly decoded
        let key0 = iter.decode_key_at_restart(0).unwrap();
        assert_eq!(key0.as_ref(), b"prefix_aaa");

        // With restart interval of 16 and only 3 entries, there's only 1 restart point
        assert_eq!(block.restarts.len(), 1);
    }

    #[tokio::test]
    async fn should_iterate_all_entries_with_prefix_encoding() {
        // given: a block with entries that have shared prefixes
        let mut block_builder = BlockBuilder::new(4096);
        assert!(block_builder.add_value(b"prefix_aaa", b"1", gen_empty_attrs()));
        assert!(block_builder.add_value(b"prefix_bbb", b"2", gen_empty_attrs()));
        assert!(block_builder.add_value(b"prefix_ccc", b"3", gen_empty_attrs()));
        let block = block_builder.build().unwrap();
        let mut iter = BlockIterator::new_ascending(&block);

        // when: iterating through all entries
        // then: full keys are correctly reconstructed from prefix encoding
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"prefix_aaa", b"1");

        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"prefix_bbb", b"2");

        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"prefix_ccc", b"3");

        assert!(iter.next().await.unwrap().is_none());
    }
}
