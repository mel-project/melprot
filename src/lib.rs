pub mod cache;
mod client;
mod inmemory_truststore;
#[cfg(feature = "server")]
mod server;

pub use client::*;
pub use inmemory_truststore::*;
#[cfg(feature = "server")]
pub use server::*;

use serde::{Deserialize, Serialize};
use themelio_structs::{Address, BlockHeight, ConsensusProof, Header, NetID, Transaction, TxHash};
use tmelcrypt::HashVal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSummary {
    pub netid: NetID,
    pub height: BlockHeight,
    pub header: Header,
    pub proof: ConsensusProof,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum Substate {
    History,
    Coins,
    Transactions,
    Pools,
    Stakes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NodeRequest {
    SendTx(Transaction),
    GetAbbrBlock(BlockHeight),
    GetSummary,
    GetSmtBranch(BlockHeight, Substate, HashVal),
    GetStakersRaw(BlockHeight),
    GetPartialBlock(BlockHeight, Vec<TxHash>),
    GetSomeCoins(BlockHeight, Address),
}
