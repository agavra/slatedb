use std::cmp::Ordering;
use std::sync::Arc;

use crate::iter::IterationOrder;
use crate::iter::IterationOrder::Ascending;
use crate::row_codec::SstRowCodecV0;
use crate::{block::Block, error::SlateDBError, iter::KeyValueIterator, types::RowEntry};
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use IterationOrder::Descending;

pub(crate) trait BlockLikeV1: Send + Sync {
    fn data(&self) -> &Bytes;
    fn offsets(&self) -> &[u16];
}

impl BlockLikeV1 for Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        // V1 blocks stored dense offsets in the restarts field
        &self.restarts
    }
}

impl BlockLikeV1 for &Block {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.restarts
    }
}

impl BlockLikeV1 for Arc<Block> {
    fn data(&self) -> &Bytes {
        &self.data
    }

    fn offsets(&self) -> &[u16] {
        &self.restarts
    }
}

pub(crate) struct V1BlockIterator<B: BlockLikeV1> {
    block: B,
    off_off: usize,
    // first key in the block, because slateDB does not support multi version of keys
    // so we use `Bytes` temporarily
    first_key: Bytes,
    ordering: IterationOrder,
}

#[async_trait]
impl<B: BlockLikeV1> KeyValueIterator for V1BlockIterator<B> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        Ok(())
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        let result = self.load_at_current_off();
        match result {
            Ok(None) => Ok(None),
            Ok(key_value) => {
                self.advance();
                Ok(key_value)
            }
            Err(e) => Err(e),
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        let num_entries = self.block.offsets().len();
        if num_entries == 0 {
            return Ok(());
        }

        // Binary search to find the first key >= next_key
        let mut low = self.off_off;
        let mut high = num_entries;

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_key = self.decode_key_at_index(mid)?;

            match mid_key.as_ref().cmp(next_key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Equal | Ordering::Greater => {
                    high = mid;
                }
            }
        }

        self.off_off = low;
        Ok(())
    }
}

impl<B: BlockLikeV1> V1BlockIterator<B> {
    pub(crate) fn new(block: B, ordering: IterationOrder) -> Self {
        V1BlockIterator {
            first_key: V1BlockIterator::decode_first_key(&block),
            block,
            off_off: 0,
            ordering,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_ascending(block: B) -> Self {
        Self::new(block, Ascending)
    }

    fn advance(&mut self) {
        self.off_off += 1;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.off_off >= self.block.offsets().len()
    }

    fn load_at_current_off(&self) -> Result<Option<RowEntry>, SlateDBError> {
        if self.is_empty() {
            return Ok(None);
        }
        let off_off = match self.ordering {
            Ascending => self.off_off,
            Descending => self.block.offsets().len() - 1 - self.off_off,
        };

        let off = self.block.offsets()[off_off];
        let off_usz = off as usize;
        // TODO: bounds checks to avoid panics? (paulgb)
        let mut cursor = self.block.data().slice(off_usz..);
        let codec = SstRowCodecV0::new();
        let sst_row = codec.decode(&mut cursor)?;
        Ok(Some(RowEntry::new(
            sst_row.restore_full_key(&self.first_key),
            sst_row.value,
            sst_row.seq,
            sst_row.create_ts,
            sst_row.expire_ts,
        )))
    }

    fn decode_first_key(block: &B) -> Bytes {
        let mut buf = block.data().slice(..);
        let overlap_len = buf.get_u16() as usize;
        assert_eq!(overlap_len, 0, "first key overlap should be 0");
        let key_len = buf.get_u16() as usize;
        let first_key = &buf[..key_len];
        Bytes::copy_from_slice(first_key)
    }

    /// Decodes just the key at the given offset index without parsing the full row.
    /// This is more efficient for binary search where we only need to compare keys.
    fn decode_key_at_index(&self, index: usize) -> Result<Bytes, SlateDBError> {
        let off = self.block.offsets()[index] as usize;
        let mut cursor = self.block.data().slice(off..);

        let key_prefix_len = cursor.get_u16() as usize;
        let key_suffix_len = cursor.get_u16() as usize;
        let key_suffix = &cursor[..key_suffix_len];

        // Reconstruct the full key from first_key prefix + suffix
        let mut full_key = BytesMut::with_capacity(key_prefix_len + key_suffix_len);
        full_key.extend_from_slice(&self.first_key[..key_prefix_len]);
        full_key.extend_from_slice(key_suffix);
        Ok(full_key.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;
    use crate::iter::KeyValueIterator;
    use crate::row_codec::{SstRowCodecV0, SstRowEntry};
    use crate::test_utils;
    use crate::types::ValueDeletable;
    use bytes::Bytes;

    /// Build a V1-format block with dense offsets (one offset per entry)
    /// This mimics the old block format for testing V1BlockIterator
    fn build_v1_block(entries: &[(&[u8], &[u8], u64)]) -> Block {
        let mut data = Vec::new();
        let mut offsets = Vec::new();

        let codec = SstRowCodecV0::new();
        let mut first_key = Bytes::new();

        for (i, (key, value, seq)) in entries.iter().enumerate() {
            offsets.push(data.len() as u16);

            let key_bytes = Bytes::copy_from_slice(key);
            if i == 0 {
                first_key = key_bytes.clone();
            }

            // Encode with prefix compression relative to first key
            let prefix_len = if i == 0 {
                0
            } else {
                compute_prefix(&first_key, &key_bytes)
            };

            let key_suffix = Bytes::copy_from_slice(&key_bytes[prefix_len..]);
            let entry = SstRowEntry::new(
                prefix_len,
                key_suffix,
                *seq,
                ValueDeletable::Value(Bytes::copy_from_slice(value)),
                None,
                None,
            );

            codec.encode(&mut data, &entry);
        }

        Block {
            data: Bytes::from(data),
            restarts: offsets, // V1 blocks store dense offsets in restarts field
        }
    }

    fn compute_prefix(lhs: &[u8], rhs: &[u8]) -> usize {
        lhs.iter()
            .zip(rhs.iter())
            .take_while(|(a, b)| a == b)
            .count()
    }

    #[tokio::test]
    async fn should_iterate_v1_block() {
        // given: a V1 block with dense offsets
        let block = build_v1_block(&[
            (b"donkey", b"kong", 1),
            (b"kratos", b"atreus", 2),
            (b"super", b"mario", 3),
        ]);

        // when: iterating with V1BlockIterator
        let mut iter = V1BlockIterator::new_ascending(&block);

        // then: all entries are returned correctly
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"donkey", b"kong");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"kratos", b"atreus");
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"super", b"mario");
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_seek_in_v1_block() {
        // given: a V1 block with dense offsets
        let block = build_v1_block(&[
            (b"apple", b"1", 1),
            (b"banana", b"2", 2),
            (b"cherry", b"3", 3),
        ]);

        // when: seeking to an existing key
        let mut iter = V1BlockIterator::new_ascending(&block);
        iter.seek(b"banana").await.unwrap();

        // then: the correct entry is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"banana", b"2");
    }

    #[tokio::test]
    async fn should_seek_to_nonexisting_key_in_v1_block() {
        // given: a V1 block
        let block = build_v1_block(&[(b"apple", b"1", 1), (b"cherry", b"3", 3)]);

        // when: seeking to a key between existing keys
        let mut iter = V1BlockIterator::new_ascending(&block);
        iter.seek(b"banana").await.unwrap();

        // then: the next key is returned
        let kv = iter.next().await.unwrap().unwrap();
        test_utils::assert_kv(&kv, b"cherry", b"3");
    }
}
