use std::{future::Future, sync::RwLock};

use bytes::Bytes;
use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};
use stdcode::StdcodeSerializeExt;

pub struct AsyncCache {
    inner: RwLock<LruCache<Bytes, Bytes>>,
}

impl AsyncCache {
    pub fn new(size: u64) -> Self {
        Self {
            inner: LruCache::new(size as usize).into(),
        }
    }

    pub async fn get_or_try_fill<K: Serialize, V: Serialize + DeserializeOwned, E>(
        &self,
        key: K,
        fallback: impl Future<Output = Result<V, E>>,
    ) -> Result<V, E> {
        let key = Bytes::from(key.stdcode());
        let b = self.inner.read().unwrap().peek(&key).cloned();
        if let Some(b) = b {
            Ok(stdcode::deserialize(&b).expect("badly serialized thing in cache"))
        } else {
            let res = fallback.await?;
            self.inner.write().unwrap().put(key, res.stdcode().into());
            Ok(res)
        }
    }
}
