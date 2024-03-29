use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
    str::FromStr,
};

use async_trait::async_trait;

use nanorpc::{nanorpc_derive, RpcTransport};
use novasmt::CompressedProof;
use serde::{Deserialize, Serialize};
use melstructs::{
    AbbrBlock, Address, Block, BlockHeight, CoinID, ConsensusProof, Header, NetID, Transaction,
    TxHash,
};
use thiserror::Error;
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

#[derive(Error, Debug, Clone)]
pub enum SubstateParseError {
    #[error("Invalid substate")]
    Invalid,
}

impl FromStr for Substate {
    type Err = SubstateParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HISTORY" => Ok(Substate::History),
            "COINS" => Ok(Substate::Coins),
            "TRANSACTIONS" => Ok(Substate::Transactions),
            "POOLS" => Ok(Substate::Pools),
            "STAKES" => Ok(Substate::Stakes),
            _ => Err(SubstateParseError::Invalid),
        }
    }
}

impl Display for Substate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = match self {
            Substate::History => "HISTORY".into(),
            Substate::Coins => "COINS".into(),
            Substate::Transactions => "TRANSACTIONS".into(),
            Substate::Pools => "POOLs".into(),
            Substate::Stakes => "STAKES".into(),
        };
        s.fmt(f)
    }
}

#[nanorpc_derive]
#[async_trait]
pub trait NodeRpcProtocol: Send + Sync {
    /// Broadcasts a transaction to the network
    async fn send_tx(&self, tx: Transaction) -> Result<(), TransactionError>;

    /// Gets an "abbreviated block"
    async fn get_abbr_block(&self, height: BlockHeight) -> Option<(AbbrBlock, ConsensusProof)>;

    /// Gets a state summary
    async fn get_summary(&self) -> StateSummary;

    /// Gets a full state
    async fn get_block(&self, height: BlockHeight) -> Option<Block>;

    /// Gets an lz4-compressed blob containing a stdcode-encoded, base64 vector of blocks, starting at the given height, of at most the given bytes. If the starting block is pruned, return None.
    async fn get_lz4_blocks(&self, height: BlockHeight, size_limit: usize) -> Option<String>;

    /// Gets an SMT branch
    async fn get_smt_branch(
        &self,
        height: BlockHeight,
        elem: Substate,
        key: HashVal,
    ) -> Option<(Vec<u8>, CompressedProof)>;

    /// Gets stakers
    async fn get_stakers_raw(&self, height: BlockHeight) -> Option<BTreeMap<HashVal, Vec<u8>>>;

    /// Gets *possibly a subset* of the list of all coins associated with a covenant hash. Can return None if the node simply doesn't index this information.
    async fn get_some_coins(&self, _height: BlockHeight, _covhash: Address) -> Option<Vec<CoinID>> {
        None
    }

    /// Gets the changes in coins owned by the given address, at the given height.
    async fn get_coin_changes(
        &self,
        height: BlockHeight,
        address: Address,
    ) -> Option<Vec<CoinChange>>;

    /// Gets the transaction hash and height for a transaction that spent a given coin ID.
    /// Returns `None` if the node could not produce a result (e.g. no coin indexer, etc.).
    async fn get_coin_spend(&self, _coin_id: CoinID) -> Option<CoinSpendStatus> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum CoinSpendStatus {
    Spent((TxHash, BlockHeight)),
    NotSpent,
}

/// Change in coins.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum CoinChange {
    Add(CoinID),
    Delete(CoinID, TxHash),
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

#[derive(Serialize, Deserialize, Error, Debug)]
pub enum TransactionError {
    #[error("Rejecting recently seen transaction")]
    RecentlySeen,
    #[error("Invalid transaction: {0}")]
    Invalid(String),
}

impl<T: RpcTransport> NodeRpcClient<T> {
    /// Gets a full block, given a function that tells known from unknown transactions.
    pub async fn get_full_block(
        &self,
        height: BlockHeight,
        get_known_tx: impl Fn(TxHash) -> Option<Transaction>,
    ) -> Result<Option<(Block, ConsensusProof)>, NodeRpcError<T::Error>> {
        let (abbr, cproof) = match self.get_abbr_block(height).await? {
            Some(v) => v,
            None => return Ok(None), // No such block
        };

        let mut known = vec![];
        let mut unknown = vec![];
        for txhash in abbr.txhashes.iter() {
            if let Some(tx) = get_known_tx(*txhash) {
                known.push(tx);
            } else {
                unknown.push(*txhash);
            }
        }

        // send off a request
        let mut response: Block = if unknown.is_empty() {
            Block {
                header: abbr.header,
                transactions: HashSet::new(),
                proposer_action: abbr.proposer_action,
            }
        } else {
            unknown.sort_unstable();
            let hvv = unknown;
            let blk_height = self.get_block(height).await?;
            if let Some(mut blk) = blk_height {
                blk.transactions
                    .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                blk
            } else {
                return Ok(None);
            }
        };

        for known in known {
            response.transactions.insert(known);
        }
        Ok(Some((response, cproof)))
    }
}
