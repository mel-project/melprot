use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use melnet::Request;
use novasmt::CompressedProof;
use themelio_structs::{
    AbbrBlock, Address, Block, BlockHeight, CoinID, ConsensusProof, Transaction,
};
use tmelcrypt::HashVal;

use crate::{NodeRequest, StateSummary, Substate};

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
        height: BlockHeight,
        covhash: Address,
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
                hvv.sort();
                let hvv = hvv;
                let mut blk = server.get_block(height)?;
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
