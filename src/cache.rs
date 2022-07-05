use std::{convert::TryInto, future::Future, hash::BuildHasherDefault};

use arrayref::array_ref;
use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use rustc_hash::FxHasher;
use serde::{de::DeserializeOwned, Serialize};
use stdcode::StdcodeSerializeExt;
use tmelcrypt::{HashVal, Hashable};

const SHARDS: usize = 6;

pub struct  AsyncCache {
    inner: [Mutex<LruCache<HashVal, Bytes, BuildHasherDefault<FxHasher>>>; SHARDS],
}

impl AsyncCache {
    pub fn new(size: u64) -> Self {
        let mut vv = vec![];
        for _ in 0..SHARDS {
            vv.push(Mutex::new(LruCache::with_hasher(
                size as usize / SHARDS,
                BuildHasherDefault::<FxHasher>::default(),
            )))
        }
        Self {
            inner: vv.try_into().ok().expect("le fail"),
        }
    }

    pub async fn get_or_try_fill<K: Serialize, V: Serialize + DeserializeOwned, E>(
        &self,
        key: K,
        fallback: impl Future<Output = Result<V, E>>,
    ) -> Result<V, E> {
        let key = key.stdcode().hash();
        let bucket = (u64::from_le_bytes(*array_ref![key.0, 0, 8]) % (SHARDS as u64)) as usize;

        let b = self.inner[bucket].lock().get(&key).cloned();
        if let Some(b) = b {
            log::debug!("cache HIT for key {}", hex::encode(&key));
            Ok(stdcode::deserialize(&b).expect("badly serialized thing in cache"))
        } else {
            log::debug!("cache MISS for key {}", hex::encode(&key));
            let res = fallback.await?;
            self.inner[bucket].lock().put(key, res.stdcode().into());
            Ok(res)
        }
    }
}
