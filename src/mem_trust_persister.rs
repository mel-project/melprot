use crate::{BlockHeight, TrustStore};
use std::{collections::HashMap, sync::RwLock};
use themelio_stf::NetID;
use tmelcrypt::HashVal;

/// In-memory trust store.
pub struct InMemoryTrustStore {
    inner: RwLock<HashMap<NetID, (BlockHeight, HashVal)>>,
}

impl Default for InMemoryTrustStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryTrustStore {
    /// Creates a new in-memory trust store.
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl TrustStore for InMemoryTrustStore {
    fn set(&self, netid: NetID, height: BlockHeight, header_hash: HashVal) {
        let mut inner = self.inner.write().unwrap();
        if let Some((k, _)) = inner.get(&netid) {
            if *k >= height {
                return;
            }
        }
        inner.insert(netid, (height, header_hash));
    }

    fn set_highest(
        &self,
        netid: NetID,
        height: BlockHeight,
        header_hash: HashVal)
    {
        let (t_height, t_head) = self.inner.read().unwrap().get(&netid)
            .map(|(cur_height, cur_head)|
                if height > *cur_height {
                    (height, header_hash)
                } else {
                    (cur_height.clone(), cur_head.clone())
                })
            .or(Some((height, header_hash)))
            .expect("Trust should always return Some, this is a bug");

        self.inner.write().unwrap().insert(netid, (t_height, t_head));
    }

    fn get(&self, netid: NetID) -> Option<(BlockHeight, HashVal)> {
        self.inner.read().unwrap().get(&netid).cloned()
    }
}
