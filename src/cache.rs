use std::{convert::TryInto, future::Future, hash::BuildHasherDefault, sync::Arc};

use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use melstructs::{BlockHeight, CoinID, Header, NetID, TxHash};
use mini_moka::sync::Cache;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHasher;
use serde::{de::DeserializeOwned, Serialize};
use stdcode::StdcodeSerializeExt;
use tmelcrypt::{HashVal, Hashable};

use crate::{CoinSpendStatus, Substate};

/// Global cache
pub(crate) static GLOBAL_CACHE: Lazy<RwLock<Arc<dyn StateCache>>> =
    Lazy::new(|| RwLock::new(Arc::new(InMemoryStateCache::new(100_000_000))));

/// Sets the global state cache. Only affects [crate::Client] instances that are created after this point!
pub fn set_global_cache(cache: impl StateCache) {
    *GLOBAL_CACHE.write() = Arc::new(cache);
}

/// An in-memory state cache.
pub struct InMemoryStateCache {
    inner: Cache<Bytes, Bytes>,
}

impl InMemoryStateCache {
    /// Creates a new in-memory state cache with the given maximum size, in bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(max_bytes as u64)
                .weigher(|k: &Bytes, v: &Bytes| (k.len() + v.len() + 10) as u32)
                .build(),
        }
    }
}

#[async_trait]
impl StateCache for InMemoryStateCache {
    async fn get_blob(&self, key: &[u8]) -> Option<Bytes> {
        let key: Bytes = key.to_vec().into();
        let res = self.inner.get(&key);
        log::debug!("memcache: {:?} hit? {}", key, res.is_some());
        res
    }

    async fn insert_blob(&self, key: &[u8], value: &[u8]) {
        self.inner
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }
}

/// A trait that abstracts over a key-value cache for verified on-chain information. Only the "blob" methods are mandatory to implement.
#[async_trait]
pub trait StateCache: Send + Sync + 'static {
    /// Gets an arbitrary blob of data from the cache.
    async fn get_blob(&self, key: &[u8]) -> Option<Bytes>;

    /// Inserts an arbitrary blob of data into the cache.
    async fn insert_blob(&self, key: &[u8], value: &[u8]);

    /// Gets a historical header from the cache.
    async fn get_header(&self, network: NetID, height: BlockHeight) -> Option<Header> {
        stdcode::deserialize(
            &self
                .get_blob(&("header", network, height).stdcode())
                .await?,
        )
        .ok()
    }

    /// Gets a coin-spend status, for a *spent* coin, from the cache.
    async fn get_spend_location(&self, coin: CoinID) -> Option<(TxHash, BlockHeight)> {
        stdcode::deserialize(&self.get_blob(&("spend_location", coin).stdcode()).await?).ok()
    }

    /// Inserts a coin-spend status, for a *spent* coin, into the cache.
    async fn insert_spend_location(&self, coin: CoinID, txhash: TxHash, height: BlockHeight) {
        self.insert_blob(
            &("spend_location", coin).stdcode(),
            &(txhash, height).stdcode(),
        )
        .await;
    }

    /// Inserts a historical header from the cache.
    async fn insert_header(&self, network: NetID, height: BlockHeight, header: Header) {
        self.insert_blob(&("header", network, height).stdcode(), &header.stdcode())
            .await;
    }

    /// Gets an SMT branch from the cache.
    async fn get_smt_branch(
        &self,
        header_hash: HashVal,
        tree: Substate,
        branch: HashVal,
    ) -> Option<Bytes> {
        self.get_blob(&("smt", header_hash, tree, branch).stdcode())
            .await
    }

    /// Inserts an SMT branch into the cache.
    async fn insert_smt_branch(
        &self,
        header_hash: HashVal,
        tree: Substate,
        branch: HashVal,
        value: &[u8],
    ) {
        self.insert_blob(&("smt", header_hash, tree, branch).stdcode(), value)
            .await
    }
}
