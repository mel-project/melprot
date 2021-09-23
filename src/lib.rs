mod client;
mod mem_trust_persister;
mod server;

pub use client::*;
pub use mem_trust_persister::*;
pub use server::*;

use serde::{Deserialize, Serialize};
use themelio_stf::{BlockHeight, ConsensusProof, Header, NetID, Transaction, TxHash};
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
pub enum NodeRequest {
    SendTx(Transaction),
    GetAbbrBlock(BlockHeight),
    GetSummary,
    GetSmtBranch(BlockHeight, Substate, HashVal),
    GetStakersRaw(BlockHeight),
    GetPartialBlock(BlockHeight, Vec<TxHash>),
}
