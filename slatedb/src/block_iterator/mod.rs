//! Block iterator implementations for V1 and V2 block formats.
//!
//! This module provides:
//! - `V1BlockIterator` - Iterator for V1 blocks with dense offsets
//! - `V2BlockIterator` - Iterator for V2 blocks with restart-based prefix encoding
//! - `VersionedBlockIterator` - Enum that routes to the appropriate iterator based on format version
//!
//! For backward compatibility, `BlockIterator` is aliased to `V2BlockIterator`
//! and `BlockLike` is aliased to `BlockLikeV2`.

mod block_iterator_v1;
mod block_iterator_v2;

use std::sync::Arc;

use async_trait::async_trait;

use crate::block::Block;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::types::RowEntry;

// V1 types (used by VersionedBlockIterator for version 1 SSTs)
#[allow(unused_imports)]
pub(crate) use block_iterator_v1::BlockLikeV1;
pub(crate) use block_iterator_v1::V1BlockIterator;

// V2 types (used by VersionedBlockIterator for version 2+ SSTs)
pub(crate) use block_iterator_v2::{BlockLikeV2, V2BlockIterator};

// Backward compatibility - existing code may use these names
#[allow(dead_code)]
pub(crate) type BlockIterator<B> = V2BlockIterator<B>;
#[allow(dead_code)]
pub(crate) type BlockLike = dyn BlockLikeV2;

/// A versioned block iterator that routes to either V1 or V2 iterator
/// based on the SST format version.
///
/// NOTE: Currently unused because the manifest doesn't store format_version.
/// Once the manifest is updated to persist format_version, this can be used
/// in sst_iter.rs for version-based iterator routing.
#[allow(dead_code)]
pub(crate) enum VersionedBlockIterator {
    V1(V1BlockIterator<Arc<Block>>),
    V2(V2BlockIterator<Arc<Block>>),
}

#[allow(dead_code)]
impl VersionedBlockIterator {
    /// Create a new versioned block iterator based on format version.
    /// - Version 1: Uses V1BlockIterator (dense offsets)
    /// - Version 2+: Uses V2BlockIterator (restart-based)
    pub(crate) fn new(block: Arc<Block>, format_version: u16) -> Self {
        use crate::iter::IterationOrder::Ascending;
        if format_version == 1 {
            VersionedBlockIterator::V1(V1BlockIterator::new(block, Ascending))
        } else {
            VersionedBlockIterator::V2(V2BlockIterator::new(block, Ascending))
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            VersionedBlockIterator::V1(iter) => iter.is_empty(),
            VersionedBlockIterator::V2(iter) => iter.is_empty(),
        }
    }
}

#[async_trait]
impl KeyValueIterator for VersionedBlockIterator {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        match self {
            VersionedBlockIterator::V1(iter) => iter.init().await,
            VersionedBlockIterator::V2(iter) => iter.init().await,
        }
    }

    async fn next_entry(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        match self {
            VersionedBlockIterator::V1(iter) => iter.next_entry().await,
            VersionedBlockIterator::V2(iter) => iter.next_entry().await,
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        match self {
            VersionedBlockIterator::V1(iter) => iter.seek(next_key).await,
            VersionedBlockIterator::V2(iter) => iter.seek(next_key).await,
        }
    }
}
