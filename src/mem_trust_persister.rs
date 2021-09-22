use tmelcrypt::HashVal;
use acidjson::{AcidJson, AcidJsonError};
use themelio_stf::NetID;
use std::{collections::BTreeMap, path::Path};
use crate::{BlockHeight, TrustedBlockPersister};

/// File mapping from network id to latest trusted block.
#[derive(Clone)]
pub struct InMemoryTrustStore(AcidJson<BTreeMap<u8, (BlockHeight, HashVal)>>);

impl InMemoryTrustStore {
    /// Opens or creates a blockstore from a given filename.
    pub fn open(path: &Path) -> Result<Self, AcidJsonError> {
        // if not exists, create
        if std::fs::read(path).is_err() {
            std::fs::write(path, "{}")
                .map_err(|e| AcidJsonError::IoError(e))?;
        }
        Ok(Self( AcidJson::open(path)? ))
    }
}

impl TrustedBlockPersister for InMemoryTrustStore {
    // Note: Allows a latest block to be rolled back to an earlier one
    fn set(
        &self,
        netid: NetID,
        height: BlockHeight,
        header_hash: HashVal)
    {
        self.0.write().insert(netid as u8, (height, header_hash));
    }

    fn set_highest(
        &self,
        netid: NetID,
        height: BlockHeight,
        header_hash: HashVal)
    {
        let (t_height, t_head) = self.0.read().get(&(netid as u8))
            .map(|(cur_height, cur_head)|
                if height > *cur_height {
                    (height, header_hash)
                } else {
                    (cur_height.clone(), cur_head.clone())
                })
            .or(Some((height, header_hash)))
            .expect("Trust should always return Some, this is a bug");

        self.0.write().insert(netid as u8, (t_height, t_head));
    }

    fn get(&self, netid: NetID)
    -> Option<(BlockHeight, HashVal)> {
        self.0.read().get(&(netid as u8)).cloned()
    }
}

