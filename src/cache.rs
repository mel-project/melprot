use std::future::Future;

use bytes::Bytes;
use moka::sync::Cache;
use serde::{de::DeserializeOwned, Serialize};
use stdcode::StdcodeSerializeExt;

pub struct AsyncCache {
    inner: Cache<Bytes, Bytes>,
}

impl AsyncCache {
    pub fn new(size: u64) -> Self {
        Self {
            inner: Cache::new(size),
        }
    }

    pub async fn get_or_try_fill<K: Serialize, V: Serialize + DeserializeOwned, E>(
        &self,
        key: K,
        fallback: impl Future<Output = Result<V, E>>,
    ) -> Result<V, E> {
        if let Some(b) = self.inner.get(&Bytes::from(key.stdcode())) {
            Ok(stdcode::deserialize(&b).expect("badly serialized thing in cache"))
        } else {
            let res = fallback.await?;
            self.inner
                .insert(key.stdcode().into(), res.stdcode().into());
            Ok(res)
        }
    }
}
