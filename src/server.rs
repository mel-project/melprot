use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use melnet::Request;
use novasmt::{CompressedProof, ContentAddrStore};
use themelio_stf::{AbbrBlock, BlockHeight, ConsensusProof, SealedState, Transaction};
use tmelcrypt::HashVal;

use crate::{NodeRequest, StateSummary, Substate};

/// This trait represents a server of Themelio's node protocol. Actual nodes should implement this.
pub trait NodeServer<C: ContentAddrStore>: Send + Sync {
    /// Broadcasts a transaction to the network
    fn send_tx(&self, state: melnet::NetState, tx: Transaction) -> melnet::Result<()>;

    /// Gets an "abbreviated block"
    fn get_abbr_block(&self, height: BlockHeight) -> melnet::Result<(AbbrBlock, ConsensusProof)>;

    /// Gets abbreviated blocks within a large rang

    /// Gets a state summary
    fn get_summary(&self) -> melnet::Result<StateSummary>;

    /// Gets a full state
    fn get_state(&self, height: BlockHeight) -> melnet::Result<SealedState<C>>;

    /// Gets an SMT branch
    fn get_smt_branch(
        &self,
        height: BlockHeight,
        elem: Substate,
        key: HashVal,
    ) -> melnet::Result<(Vec<u8>, CompressedProof)>;

    /// Gets stakers
    fn get_stakers_raw(&self, height: BlockHeight) -> melnet::Result<BTreeMap<HashVal, Vec<u8>>>;
}

/// This is a melnet responder that wraps a NodeServer.
pub struct NodeResponder<C: ContentAddrStore, S: NodeServer<C> + 'static> {
    server: Arc<S>,
    _p: PhantomData<C>,
}

impl<C: ContentAddrStore, S: NodeServer<C>> NodeResponder<C, S> {
    /// Creates a new NodeResponder from something that implements NodeServer.
    pub fn new(server: S) -> Self {
        Self {
            server: Arc::new(server),
            _p: Default::default(),
        }
    }
}

impl<C: ContentAddrStore, S: NodeServer<C>> melnet::Endpoint<NodeRequest, Vec<u8>>
    for NodeResponder<C, S>
{
    fn respond(&self, req: Request<NodeRequest, Vec<u8>>) {
        let state = req.state.clone();
        match req.body.clone() {
            NodeRequest::SendTx(tx) => req
                .response
                .send(self.server.send_tx(state, tx).map(|_| Vec::new())),
            NodeRequest::GetSummary => req.response.send(
                self.server
                    .get_summary()
                    .map(|sum| stdcode::serialize(&sum).unwrap()),
            ),
            NodeRequest::GetAbbrBlock(height) => req.response.send(
                self.server
                    .get_abbr_block(height)
                    .map(|blk| stdcode::serialize(&blk).unwrap()),
            ),
            NodeRequest::GetSmtBranch(height, elem, key) => req.response.send(
                self.server
                    .get_smt_branch(height, elem, key)
                    .map(|v| stdcode::serialize(&v).unwrap()),
            ),
            NodeRequest::GetStakersRaw(height) => req.response.send(
                self.server
                    .get_stakers_raw(height)
                    .map(|v| stdcode::serialize(&v).unwrap()),
            ),
            NodeRequest::GetPartialBlock(height, mut hvv) => {
                let server = self.server.clone();
                hvv.sort();
                let hvv = hvv;
                let res = server.get_state(height).map(|ss| {
                    let mut blk = ss.to_block();
                    blk.transactions
                        .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                    stdcode::serialize(&blk).unwrap()
                });
                req.response.send(res);
            }
        }
    }
}
