use crate::{TrustStore, TrustedHeight};
use std::{collections::HashMap, sync::Arc, sync::RwLock};
use themelio_stf::NetID;

/// In-memory trust store.
#[derive(Clone)]
pub struct InMemoryTrustStore {
    inner: Arc<RwLock<HashMap<NetID, TrustedHeight>>>,
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
    fn set(&self, netid: NetID, trusted: TrustedHeight) {
        let mut inner = self.inner.write().unwrap();
        if let Some(old) = inner.get(&netid) {
            if old.height >= trusted.height {
                return;
            }
        }
        inner.insert(netid, trusted);
    }

    fn get(&self, netid: NetID) -> Option<TrustedHeight> {
        self.inner.read().unwrap().get(&netid).cloned()
    }
}
