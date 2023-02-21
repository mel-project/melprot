use themelio_structs::{Checkpoint, NetID};

use crate::TrustStore;
use std::{collections::HashMap, sync::Arc, sync::RwLock};

/// In-memory trust store.
#[derive(Clone)]
pub struct InMemoryTrustStore {
    inner: Arc<RwLock<HashMap<NetID, Checkpoint>>>,
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
    fn set(&self, netid: NetID, trusted: Checkpoint) {
        let mut inner = self.inner.write().unwrap();
        if let Some(old) = inner.get(&netid) {
            if old.height >= trusted.height {
                return;
            }
        }
        inner.insert(netid, trusted);
    }

    fn get(&self, netid: NetID) -> Option<Checkpoint> {
        self.inner.read().unwrap().get(&netid).cloned()
    }
}
