use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use melnet::Request;
use nanorpc::{nanorpc_derive, RpcTransport};
use novasmt::CompressedProof;
use serde::{Deserialize, Serialize};
use themelio_structs::{
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

/// The LEGACY endpoint
#[async_trait]
impl<T: NodeRpcProtocol> melnet::Endpoint<NodeRequest, Vec<u8>> for NodeRpcService<T> {
    async fn respond(&self, req: Request<NodeRequest>) -> anyhow::Result<Vec<u8>> {
        let service = &self.0;
        match req.body {
            NodeRequest::SendTx(tx) => {
                let _ = service.send_tx(tx).await;
                Ok(vec![])
            }
            NodeRequest::GetSummary => {
                let summary = service.get_summary().await;
                Ok::<_, anyhow::Error>(stdcode::serialize(&summary)?)
            }
            NodeRequest::GetAbbrBlock(height) => {
                let block = service.get_abbr_block(height).await;
                Ok::<_, anyhow::Error>(stdcode::serialize(&block)?)
            }
            NodeRequest::GetSmtBranch(height, elem, key) => {
                let branch = service.get_smt_branch(height, elem, key).await;
                Ok::<_, anyhow::Error>(stdcode::serialize(&branch)?)
            }
            NodeRequest::GetStakersRaw(height) => {
                Ok(stdcode::serialize(&service.get_stakers_raw(height).await)?)
            }
            NodeRequest::GetPartialBlock(height, mut hvv) => {
                hvv.sort_unstable();
                let hvv = hvv;

                if let Some(mut blk) = service.get_block(height).await {
                    blk.transactions
                        .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                    Ok(stdcode::serialize(&blk)?)
                } else {
                    Ok(vec![])
                }
            }
            NodeRequest::GetSomeCoins(height, address) => {
                if let Some(coins) = service.get_some_coins(height, address).await {
                    return Ok(stdcode::serialize(&coins)?);
                } else {
                    Ok(vec![])
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Error, Debug)]
pub enum TransactionError {
    #[error("Rejecting recently seen transaction")]
    RecentlySeen,
    #[error("Duplicate transaction")]
    Duplicate(String),
    #[error("Storage error")]
    Storage,
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
            let blk_height = self.get_block(height).await.ok().unwrap();
            if let Some(mut blk) = blk_height {
                blk.transactions
                    .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                blk
            } else {
                // return an empty block here?
                Block {
                    header: abbr.header,
                    transactions: HashSet::new(),
                    proposer_action: abbr.proposer_action,
                }
            }
        };

        for known in known {
            response.transactions.insert(known);
        }
        Ok(Some((response, cproof)))
    }
}
