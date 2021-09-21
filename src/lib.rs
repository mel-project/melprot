mod client;
mod server;
mod mem_trust_persister;

pub use client::*;
pub use server::*;
pub use mem_trust_persister::*;

use serde::{Deserialize, Serialize};
use themelio_stf::{ConsensusProof, Header, NetID, Transaction, TxHash};
use tmelcrypt::HashVal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSummary {
    pub netid: NetID,
    pub height: u64,
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
    GetAbbrBlock(u64),
    GetSummary,
    GetSmtBranch(u64, Substate, HashVal),
    GetStakersRaw(u64),
    GetPartialBlock(u64, Vec<TxHash>),
}
