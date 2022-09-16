use async_trait::async_trait;
use melnet::Request;
use novasmt::CompressedProof;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use themelio_structs::{
    AbbrBlock, Address, Block, BlockHeight, CoinID, ConsensusProof, Transaction, TxHash,
};
use thiserror::Error;
use tmelcrypt::HashVal;

use crate::{NodeRequest, StateSummary, Substate};

use nanorpc::{nanorpc_derive, RpcTransport};
use serde::{Deserialize, Serialize};

/// This trait represents a server of Themelio's node protocol. Actual nodes should implement this.
pub trait NodeServer: Send + Sync {
    /// Broadcasts a transaction to the network
    fn send_tx(&self, state: melnet::NetState, tx: Transaction) -> anyhow::Result<()>;

    /// Gets an "abbreviated block"
    fn get_abbr_block(&self, height: BlockHeight) -> anyhow::Result<(AbbrBlock, ConsensusProof)>;

    /// Gets a state summary
    fn get_summary(&self) -> anyhow::Result<StateSummary>;

    /// Gets a full state
    fn get_block(&self, height: BlockHeight) -> anyhow::Result<Block>;

    /// Gets an SMT branch
    fn get_smt_branch(
        &self,
        height: BlockHeight,
        elem: Substate,
        key: HashVal,
    ) -> anyhow::Result<(Vec<u8>, CompressedProof)>;

    /// Gets stakers
    fn get_stakers_raw(&self, height: BlockHeight) -> anyhow::Result<BTreeMap<HashVal, Vec<u8>>>;

    /// Gets *possibly a subset* of the list of all coins associated with a covenant hash. Can return None if the node simply doesn't index this information.
    fn get_some_coins(
        &self,
        _height: BlockHeight,
        _covhash: Address,
    ) -> anyhow::Result<Option<Vec<CoinID>>> {
        Ok(None)
    }
}

/// This is a melnet responder that wraps a NodeServer.
pub struct NodeResponder<S: NodeServer + 'static> {
    server: Arc<S>,
}

impl<S: NodeServer> NodeResponder<S> {
    /// Creates a new NodeResponder from something that implements NodeServer.
    pub fn new(server: S) -> Self {
        Self {
            server: Arc::new(server),
        }
    }
}

impl<S: NodeServer> Clone for NodeResponder<S> {
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
        }
    }
}

#[async_trait]
impl<S: NodeServer> melnet::Endpoint<NodeRequest, Vec<u8>> for NodeResponder<S> {
    async fn respond(&self, req: Request<NodeRequest>) -> anyhow::Result<Vec<u8>> {
        let state = req.state.clone();
        let server = self.server.clone();

        match req.body {
            NodeRequest::SendTx(tx) => {
                server.send_tx(state, tx)?;
                Ok(vec![])
            }
            NodeRequest::GetSummary => Ok(stdcode::serialize(&server.get_summary()?)?),
            NodeRequest::GetAbbrBlock(height) => {
                Ok(stdcode::serialize(&server.get_abbr_block(height)?)?)
            }
            NodeRequest::GetSmtBranch(height, elem, key) => Ok(stdcode::serialize(
                &server.get_smt_branch(height, elem, key)?,
            )?),
            NodeRequest::GetStakersRaw(height) => {
                Ok(stdcode::serialize(&server.get_stakers_raw(height)?)?)
            }
            NodeRequest::GetPartialBlock(height, mut hvv) => {
                hvv.sort_unstable();
                let hvv = hvv;
                let mut blk: Block = server.get_block(height)?;
                blk.transactions
                    .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                Ok(stdcode::serialize(&blk)?)
            }
            NodeRequest::GetSomeCoins(height, address) => Ok(stdcode::serialize(
                &server.get_some_coins(height, address)?,
            )?),
        }
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

impl<T: RpcTransport> NodeRpcClient<T> {
    /// Gets a full block, given a function that tells known from unknown transactions.
    pub async fn get_full_block(
        &self,
        height: BlockHeight,
        get_known_tx: impl Fn(TxHash) -> Option<Transaction>,
    ) -> Option<(Block, ConsensusProof)> {
        // TODO: find a better way to do this!
        let (abbr, cproof) = self.get_abbr_block(height).await.ok().unwrap().unwrap();

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
        let new_abbr = response.abbreviate();
        if new_abbr.header != abbr.header || new_abbr.txhashes != abbr.txhashes {
            log::error!("Mismatched abbreviation");
            return None;
        }
        Some((response, cproof))
    }
}

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
