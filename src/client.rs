use anyhow::Context;
// use anyhow::Context;

use bytes::Bytes;
use derivative::Derivative;

use melnet2::{wire::http::HttpBackhaul, Backhaul};

use nanorpc::{DynRpcTransport, RpcTransport};
use novasmt::{Database, InMemoryCas};
use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Serialize};
use smol::Task;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use stdcode::StdcodeSerializeExt;

use futures_util::{Stream, StreamExt};

use melstructs::{
    Address, Block, BlockHeight, Checkpoint, CoinDataHeight, CoinID, CoinValue, ConsensusProof,
    Header, NetID, PoolKey, PoolState, StakeDoc, Transaction, TxHash, STAKE_EPOCH,
};
use thiserror::Error;
use tmelcrypt::{Ed25519PK, HashVal, Hashable};

use crate::{
    cache::{StateCache, GLOBAL_CACHE},
    CoinChange, CoinSpendStatus, InMemoryTrustStore, NodeRpcClient, NodeRpcError, Substate,
};

/// Standard interface for persisting a trusted block.
pub trait TrustStore {
    /// Set a trusted block in persistent storage, overriding the current
    /// value only if this one has a higher block height.
    fn set(&self, netid: NetID, trusted: Checkpoint);
    /// Get the latest trusted block from persistent storage if one exists.
    fn get(&self, netid: NetID) -> Option<Checkpoint>;
}

/// A higher-level client that validates all information.
#[derive(Derivative)]
#[derivative(Debug, Clone(bound = ""))]
pub struct Client {
    netid: NetID,
    #[derivative(Debug = "ignore")]
    trust_store: Arc<dyn TrustStore + Send + Sync + 'static>,
    #[derivative(Debug = "ignore")]
    cache: Arc<dyn StateCache>,

    #[derivative(Debug = "ignore")]
    raw: Arc<NodeRpcClient<DynRpcTransport>>,
}

/// Errors that a Client may return
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("state validation error: {0}")]
    InvalidState(anyhow::Error),
    #[error("error during network communication: {0}")]
    NetworkError(anyhow::Error),
    #[error("invalid node configuration: {0}")]
    InvalidNodeConfig(anyhow::Error),
}

fn to_neterr(e: NodeRpcError<anyhow::Error>) -> ClientError {
    // TODO something a little more intelligent
    ClientError::NetworkError(anyhow::anyhow!("{}", e.to_string()))
}

impl Client {
    /// Creates a new Client, hardcoding the default, in-memory trust store.
    pub fn new<Net: RpcTransport>(netid: NetID, remote: NodeRpcClient<Net>) -> Self
    where
        Net::Error: Into<anyhow::Error>,
    {
        Self::new_with_truststore(netid, remote, InMemoryTrustStore::new())
    }

    /// Creates a new Client.
    pub fn new_with_truststore<Net: RpcTransport>(
        netid: NetID,
        remote: NodeRpcClient<Net>,
        trust_store: impl TrustStore + Send + Sync + 'static,
    ) -> Self
    where
        Net::Error: Into<anyhow::Error>,
    {
        Self {
            netid,
            trust_store: Arc::new(trust_store),
            cache: GLOBAL_CACHE.read().clone(),
            raw: NodeRpcClient(DynRpcTransport::new(remote.0)).into(),
        }
    }

    /// A convenience method for connecting to a given address.
    pub async fn connect_http(netid: NetID, addr: SocketAddr) -> anyhow::Result<Self> {
        /// Global backhaul for caching connections etc
        static BACKHAUL: Lazy<HttpBackhaul> = Lazy::new(HttpBackhaul::new);
        let rpc_client = NodeRpcClient(BACKHAUL.connect(addr.to_string().into()).await?);

        // one-off set up to "trust" a custom network.
        Ok(Self::new_with_truststore(
            netid,
            rpc_client,
            InMemoryTrustStore::new(),
        ))
    }

    pub async fn connect_http_with_truststore(
        netid: NetID,
        addr: SocketAddr,
        trust_store: impl TrustStore + Send + Sync + 'static,
    ) -> anyhow::Result<Self> {
        /// Global backhaul for caching connections etc
        static BACKHAUL: Lazy<HttpBackhaul> = Lazy::new(HttpBackhaul::new);
        let rpc_client = NodeRpcClient(BACKHAUL.connect(addr.to_string().into()).await?);

        // one-off set up to "trust" a custom network.
        Ok(Self::new_with_truststore(netid, rpc_client, trust_store))
    }

    /// A convenience method for automatically connecting a client
    pub async fn autoconnect(netid: NetID) -> anyhow::Result<Self> {
        let bootstrap_routes = melbootstrap::bootstrap_routes(netid);
        let route = *bootstrap_routes
            .first()
            .context("Error retreiving bootstrap routes")?;
        let trusted_height =
            melbootstrap::checkpoint_height(netid).context("Unable to get checkpoint height")?;
        let client = Self::connect_http(netid, route).await?;
        client.trust(trusted_height);
        Ok(client)
    }

    pub async fn autoconnect_with_truststore(
        netid: NetID,
        trust_store: impl TrustStore + Send + Sync + 'static,
    ) -> anyhow::Result<Self> {
        let bootstrap_routes = melbootstrap::bootstrap_routes(netid);
        let route = *bootstrap_routes
            .first()
            .context("Error retreiving bootstrap routes")?;
        static BACKHAUL: Lazy<HttpBackhaul> = Lazy::new(HttpBackhaul::new);
        let rpc_client = NodeRpcClient(BACKHAUL.connect(route.to_string().into()).await?);
        let melclient = Client::new_with_truststore(netid, rpc_client, trust_store);
        let trusted_height =
            melbootstrap::checkpoint_height(netid).context("Unable to get checkpoint height")?;
        melclient.trust(trusted_height);

        Ok(melclient)
    }

    /// Gets the netid.
    pub fn netid(&self) -> NetID {
        self.netid
    }

    /// Trust a height.
    pub fn trust(&self, trusted: Checkpoint) {
        self.trust_store.set(self.netid, trusted);
    }

    /// Trust the latest height COMPLETELY BLINDLY
    pub async fn dangerously_trust_latest(&self) -> Result<(), ClientError> {
        let summary = self.raw.get_summary().await.map_err(to_neterr)?;
        self.trust(Checkpoint {
            height: summary.height,
            header_hash: summary.header.hash(),
        });
        Ok(())
    }

    /// Obtains a validated snapshot based on what height was trusted.
    pub async fn latest_snapshot(&self) -> Result<Snapshot, ClientError> {
        let _c = self.raw.clone();
        static INCEPTION: Lazy<Instant> = Lazy::new(Instant::now);

        let summary = self.raw.get_summary().await.map_err(to_neterr)?;
        self.validate_height(summary.height, summary.header, summary.proof.clone())
            .await?;
        Ok(Snapshot {
            height: summary.height,
            header: summary.header,
            raw: self.raw.clone(),
            cache: self.cache.clone(),
        })
    }

    /// Convenience function to obtains a validated snapshot based on a given height.
    pub async fn snapshot(&self, height: BlockHeight) -> Result<Snapshot, ClientError> {
        let snap = self.latest_snapshot().await?;
        snap.get_older(height).await
    }

    /// Helper to validate a given block height and header.
    // #[async_recursion]
    fn validate_height(
        &self,
        height: BlockHeight,
        header: Header,
        proof: ConsensusProof,
    ) -> Task<Result<(), ClientError>> {
        let this = self.clone();
        smolscale::spawn(async move {
            let safe_stakers = {
                loop {
                    let (safe_height, safe_stakers) = this.best_staker_votes().await?;
                    if height.epoch() > safe_height.epoch() + 1 {
                        // println!("we're way behind man");
                        log::error!(
                    "OUTDATED CHECKPOINT: trusted height {} in epoch {} but remote height {} in epoch {}. Continuing with best-effort to update checkpoint",
                    safe_height,
                    safe_height.epoch(),
                    height,
                    height.epoch()
                );
                        // Okay, we must recurse to one epoch ago now
                        let old_height = BlockHeight(height.0.saturating_sub(STAKE_EPOCH));
                        let (old_block, old_proof) = this
                            .raw
                            .get_abbr_block(old_height)
                            .await
                            .map_err(to_neterr)?
                            .ok_or_else(|| {
                                ClientError::InvalidState(anyhow::anyhow!(
                                    "old block gone while validating height"
                                ))
                            })?;
                        this.validate_height(old_height, old_block.header, old_proof)
                            .await?;
                    } else if height.epoch() > safe_height.epoch()
                        && safe_height.epoch() == (safe_height + BlockHeight(1)).epoch()
                    {
                        // println!("crossing epoch boundary!");
                        // to cross the epoch, we must obtain the epoch-terminal snapshot first.
                        // this places the correct thing in the cache, which then lets this one verify too.
                        let epoch_ending_height =
                            BlockHeight((safe_height.epoch() + 1) * STAKE_EPOCH - 1);
                        let (old_block, old_proof) = this
                            .raw
                            .get_abbr_block(epoch_ending_height)
                            .await
                            .map_err(to_neterr)?
                            .context("old abbr block gone while validating height")
                            .map_err(ClientError::InvalidState)?;
                        this.validate_height(epoch_ending_height, old_block.header, old_proof)
                            .await?;
                    } else {
                        // println!("yay safe");
                        break safe_stakers;
                    }
                }
            };
            // we use the stakers to validate the latest summary
            let mut good_votes = CoinValue(0);
            let mut total_votes = CoinValue(0);
            for (pubkey, &syms_staked) in safe_stakers.iter() {
                total_votes += syms_staked;
                if let Some(sig) = proof.get(pubkey) {
                    if pubkey.verify(&header.hash(), sig) {
                        good_votes += syms_staked
                    }
                }
            }
            if good_votes < total_votes * 2 / 3 {
                return Err(ClientError::InvalidState(anyhow::anyhow!(
                    "remote height {} has insufficient votes (total_votes = {}, good_votes = {})",
                    height,
                    total_votes,
                    good_votes
                )));
            }
            // automatically update trust
            this.trust(Checkpoint {
                height,
                header_hash: header.hash(),
            });
            Ok(())
        })
    }

    /// Helper function to obtain the most recent staker votes map.
    async fn best_staker_votes(
        &self,
    ) -> Result<(BlockHeight, BTreeMap<Ed25519PK, CoinValue>), ClientError> {
        let checkpoint = self
            .trust_store
            .get(self.netid)
            .context("expected to find a trusted block while fetching trusted stakers")
            .map_err(ClientError::InvalidState)?;
        if let Some(val) = self.cache.get_staker_votes(checkpoint.height.epoch()).await {
            return Ok((checkpoint.height, val));
        }

        let temp_forest = Database::new(InMemoryCas::default());
        let stakers = self
            .raw
            .get_stakers_raw(checkpoint.height)
            .await
            .map_err(to_neterr)?
            .context(format!("server did not give us the stakers for height {}", checkpoint.height))
            .map_err(ClientError::InvalidState)?;
        // first obtain trusted SMT branch
        let (abbr_block, _) = self
            .raw
            .get_abbr_block(checkpoint.height)
            .await
            .map_err(to_neterr)?
            .context(format!("server did not give us the abbr block {}", checkpoint.height))
            .map_err(ClientError::InvalidState)?;
        if abbr_block.header.hash() != checkpoint.header_hash {
            return Err(ClientError::InvalidState(anyhow::anyhow!(
                "remote block contradicted trusted block hash: trusted {}, yet got {}:{}:{}",
                checkpoint.header_hash,
                checkpoint.height,
                abbr_block.header.hash(),
                abbr_block.header.height
            )));
        }
        let trusted_stake_hash = abbr_block.header.stakes_hash;
        let mut mapping = temp_forest.get_tree(Default::default()).unwrap();
        for (k, v) in stakers {
            mapping.insert(k.0, &v);
        }
        if mapping.root_hash() != trusted_stake_hash.0 {
            return Err(ClientError::InvalidState(anyhow::anyhow!(
                "remote staker set contradicted valid header"
            )));
        }

        let mapping: Result<Vec<_>, _> = mapping
            .iter()
            .map(|(_, doc)| {
                let doc: StakeDoc = stdcode::deserialize(&doc)
                    .context("cannot deserialize stakedoc")
                    .map_err(ClientError::InvalidState)?;

                Ok(doc)
            })
            .collect();
        let mapping = mapping?
            .into_iter()
            .fold(BTreeMap::new(), |mut accum, doc| {
                if checkpoint.height.epoch() >= doc.e_start
                    && checkpoint.height.epoch() < doc.e_post_end
                {
                    *accum.entry(doc.pubkey).or_default() += doc.syms_staked
                }
                accum
            });

        self.cache
            .insert_staker_votes(checkpoint.height.epoch(), mapping.clone())
            .await;

        Ok((checkpoint.height, mapping))
    }

    /// This returns a [futures_util::Stream] of [melstructs::Transaction]s
    /// given a starting height, transaction hash, and a closure that specifies which parent coin to follow,
    ///
    /// # Examples
    ///
    /// ```
    /// let traversal = client.traverse_back(
    ///     BlockHeight(100000),
    ///     "674735b7b7e4163f7404715bd6b8433a8db523c52279ad07e2b4e88a6708d873".parse().unwrap(),
    ///     |tx| {
    ///         Some(0)
    ///     }
    /// );
    ///
    /// while let Some(next) = traversal.next().await? {
    ///     println!("transaction: {:?}", next);
    /// }
    /// ```
    pub fn traverse_back(
        &self,
        height: BlockHeight,
        txhash: TxHash,
        picker: impl Fn(&Transaction) -> Option<usize> + Send + 'static,
    ) -> impl Stream<Item = Transaction> {
        let seed = (height, txhash);
        let this = self.clone();
        let picker = Arc::new(picker);
        futures_util::stream::unfold(seed, move |(current_height, current_txhash)| {
            let this = this.clone();
            let picker = picker.clone();
            async move {
                loop {
                    let fallible_part = async {
                        // we need a snapshot at *this* height, because only that can let us use get_transaction
                        let current_snap = this.snapshot(current_height).await?;
                        let current_tx = current_snap
                            .get_transaction(current_txhash)
                            .await?
                            .expect("somehow got stuck halfway through");
                        let next_coin = current_tx
                            .inputs
                            .get(if let Some(idx) = picker(&current_tx) {
                                idx
                            } else {
                                return anyhow::Ok(None);
                            })
                            .copied()
                            .expect("picker picked out of bounds");
                        let next_height = current_snap
                            .get_coin_spent_here(next_coin)
                            .await?
                            .expect("this coin WAS spent here, wtf")
                            .height;
                        anyhow::Ok(Some((current_tx, (next_height, next_coin.txhash))))
                    };
                    match fallible_part.await {
                        Ok(value) => return value,
                        Err(err) => {
                            log::warn!(
                                "got stuck traversing due to error: {:?}, trying again",
                                err
                            );
                        }
                    }
                }
            }
        })
    }

    /// Traverses the coin graph, forwards.
    pub fn traverse_fwd(
        &self,
        height: BlockHeight,
        txhash: TxHash,
        picker: impl Fn(&Transaction) -> Option<usize> + Send + 'static,
    ) -> Result<impl Stream<Item = Transaction>, ClientError> {
        let seed = (height, txhash);
        let this = self.clone();
        let picker = Arc::new(picker);
        Ok(futures_util::stream::unfold(
            seed,
            move |(current_height, current_txhash)| {
                let this = this.clone();
                let picker = picker.clone();
                async move {
                    loop {
                        let fallible_part = async {
                            let current_snap = this.snapshot(current_height).await?;
                            let current_tx = current_snap
                                .get_transaction(current_txhash)
                                .await?
                                .expect("failed to get current transaction");
                            let idx = if let Some(idx) = picker(&current_tx) {
                                idx
                            } else {
                                return anyhow::Ok(None);
                            };

                            let next_coin_id = CoinID::new(current_txhash, idx as u8);
                            let coin_spend_status = this.get_coin_spend(next_coin_id).await?;
                            match coin_spend_status {
                                Some(status) => match status {
                                    CoinSpendStatus::NotSpent => anyhow::Ok(None),
                                    CoinSpendStatus::Spent((next_txhash, next_height)) => {
                                        let snap = this.snapshot(next_height).await?;
                                        let next_transaction = snap
                                            .get_transaction(next_txhash)
                                            .await?
                                            .context("node lied to us about a transaction here")?;
                                        anyhow::Ok(Some((
                                            next_transaction,
                                            (next_height, next_txhash),
                                        )))
                                    }
                                },
                                None => {
                                    // TODO: have the entire function return an error instead of
                                    // retrying in a loop?
                                    anyhow::bail!("the node this client is connected to does not index coins, so it could not provide a result");
                                }
                            }
                        };
                        match fallible_part.await {
                            Ok(value) => return value,
                            Err(err) => {
                                log::warn!(
                                    "got stuck traversing due to error: {:?}, trying again",
                                    err
                                );
                            }
                        }
                    }
                }
            },
        ))
    }

    /// Gets the coin-spend status of this coin_id.
    pub async fn get_coin_spend(
        &self,
        coin_id: CoinID,
    ) -> Result<Option<CoinSpendStatus>, ClientError> {
        if let Some(v) = self.cache.get_spend_location(coin_id).await {
            return Ok(Some(CoinSpendStatus::Spent(v)));
        }
        let current_snap = self.latest_snapshot().await?;
        let coin_spend_status = self
            .raw
            .get_coin_spend(coin_id)
            .await
            .map_err(|e| ClientError::NetworkError(e.into()))?;
        match coin_spend_status {
            Some(status) => match status {
                CoinSpendStatus::NotSpent => {
                    // Check that it *really is* unspent
                    if current_snap.get_coin(coin_id).await?.is_none() {
                        return Err(ClientError::InvalidState(anyhow::anyhow!(
                            "server lied to us by saying that this coin was not spent"
                        )));
                    }
                    Ok(Some(status))
                }
                CoinSpendStatus::Spent((next_txhash, next_height)) => {
                    let snap = self.snapshot(next_height).await?;
                    snap.get_transaction(next_txhash)
                        .await?
                        .context("node lied to us about a transaction here")
                        .map_err(ClientError::InvalidState)?;
                    self.cache
                        .insert_spend_location(coin_id, next_txhash, next_height)
                        .await;
                    Ok(Some(status))
                }
            },
            None => {
                return Err(ClientError::InvalidNodeConfig(anyhow::anyhow!(
                    "not indexing coin spends"
                )))
            }
        }
    }

    /// Returns a stream of all transactions relevant to the particular address --- meaning all transactions that either create or spend coins with that address.
    pub fn stream_transactions_from(
        &self,
        height: BlockHeight,
        address: Address,
    ) -> impl Stream<Item = (Transaction, BlockHeight)> + '_ {
        self.stream_snapshots(height)
            .then(move |snapshot| async move {
                let changes = loop {
                    match snapshot.get_coin_changes(address).await {
                        Ok(changes) => break changes,
                        Err(err) => {
                            log::warn!("error getting changes for {}: {err}", snapshot.height)
                        }
                    }
                };
                futures_util::stream::iter(changes.into_iter()).then(move |change| {
                    let snapshot = snapshot.clone();
                    let current_height = snapshot.current_header().height;
                    async move {
                        loop {
                            let fallible = async {
                                match change {
                                    CoinChange::Add(coin_id) => anyhow::Ok((
                                        snapshot
                                            .get_transaction(coin_id.txhash)
                                            .await?
                                            .context("mysteriously missing transaction")?,
                                        current_height,
                                    )),
                                    CoinChange::Delete(_coin_id, txhash) => anyhow::Ok((
                                        snapshot
                                            .get_transaction(txhash)
                                            .await?
                                            .context("mysteriously missing transaction")?,
                                        current_height,
                                    )),
                                }
                            };
                            match fallible.await {
                                Err(err) => {
                                    println!("could not resolve change {:?}: {err}", change);
                                    log::warn!("could not resolve change {:?}: {err}", change)
                                }
                                Ok(val) => {
                                    println!("got txs for this height");
                                    return val;
                                }
                            }
                        }
                    }
                })
            })
            .flatten()
    }

    /// Returns a stream of all snapshots after the given height.
    pub fn stream_snapshots(&self, height: BlockHeight) -> impl Stream<Item = Snapshot> + '_ {
        futures_util::stream::iter(height.0..).then(move |height| async move {
            loop {
                match self.snapshot(height.into()).await {
                    Ok(snap) => return snap,
                    Err(err) => {
                        log::warn!("error snapping {height}: {err}, retrying in a while");
                        smol::Timer::after(Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }
}

/// A "snapshot" of the state at a given state. It essentially encapsulates a NodeClient and a trusted header.
#[derive(Clone)]
pub struct Snapshot {
    height: BlockHeight,
    header: Header,
    raw: Arc<NodeRpcClient>,
    cache: Arc<dyn StateCache>,
}

impl Snapshot {
    /// Gets a reference to the raw, unvalidating raw client.
    pub fn get_raw(&self) -> &NodeRpcClient {
        &self.raw
    }

    /// Gets a transaction by its sorted position within this block.
    pub async fn get_transaction_by_posn(
        &self,
        index: usize,
    ) -> Result<Option<TxHash>, ClientError> {
        let block = self.current_block().await?;
        let mut sorted_txx: Vec<TxHash> = block
            .transactions
            .into_iter()
            .map(|tx| tx.hash_nosigs())
            .collect();
        sorted_txx.sort_unstable();
        Ok(sorted_txx.get(index).copied())
    }

    /// Gets an older snapshot.
    pub async fn get_older(&self, old_height: BlockHeight) -> Result<Self, ClientError> {
        if old_height > self.height {
            return Err(ClientError::InvalidState(anyhow::anyhow!(
                "cannot travel into the future"
            )));
        }
        if old_height == self.height {
            return Ok(self.clone());
        }
        // Get an SMT branch
        let old_header = self
            .get_history(old_height)
            .await?
            .expect("missing older height in the history SMT");

        Ok(Self {
            height: old_height,
            header: old_header,
            raw: self.raw.clone(),
            cache: self.cache.clone(),
        })
    }

    /// Gets the header.
    pub fn current_header(&self) -> Header {
        self.header
    }

    /// Helper function to obtain the proposer reward amount.
    pub async fn get_proposer_reward(&self) -> Result<CoinValue, ClientError> {
        let reward_coin = self.get_coin(CoinID::proposer_reward(self.height)).await?;
        let reward_amount = reward_coin.map(|v| v.coin_data.value).unwrap_or_default();
        Ok(reward_amount)
    }

    /// Gets the whole block at this height.
    pub async fn current_block(&self) -> Result<Block, ClientError> {
        self.current_block_with_known(|_| None).await
    }

    /// Gets the whole block at this height, with a function that gets cached transactions.
    pub async fn current_block_with_known(
        &self,
        get_known_tx: impl Fn(TxHash) -> Option<Transaction>,
    ) -> Result<Block, ClientError> {
        dbg!(self.height);
        let header = self.current_header();
        let (block, _) = self
            .raw
            .get_full_block(self.height, get_known_tx)
            .await
            .map_err(to_neterr)?
            .context("block disappeared from under our feet")
            .map_err(ClientError::InvalidState)?;
        if block.header != header {
            return Err(ClientError::InvalidState(anyhow::anyhow!(
                "block header does not match: block header {:?} vs snapshot header {:?}",
                block.header,
                header
            )));
        }

        // TODO support dense merkle trees. Right now we assume the transactions are in a SMT
        let mut transactions_smt = novasmt::Database::new(InMemoryCas::default())
            .get_tree([0u8; 32])
            .unwrap();
        for transaction in block.transactions.iter() {
            transactions_smt.insert(
                transaction.hash_nosigs().stdcode().hash().0,
                &transaction.stdcode(),
            );
        }
        if HashVal(transactions_smt.root_hash()) != block.header.transactions_hash {
            return Err(ClientError::InvalidState(anyhow::anyhow!(
                "transactions root does not match: in-header root hash {:?} vs computed {:?}",
                block.header.transactions_hash,
                HashVal(transactions_smt.root_hash())
            )));
        }

        Ok(block)
    }

    /// Gets a historical header.
    pub async fn get_history(&self, height: BlockHeight) -> Result<Option<Header>, ClientError> {
        // Especially cache because this is invariant w.r.t the current snapshot
        if height.0 < self.header.height.0 {
            if let Some(cached) = self.cache.get_header(self.header.network, height).await {
                log::debug!("exceptional history hit!!!");
                return Ok(Some(cached));
            }
        }
        let result: Option<Header> = self.get_smt_value_serde(Substate::History, height).await?;
        match result {
            Some(val) => {
                self.cache
                    .insert_header(self.header.network, height, val)
                    .await;
                log::debug!("inserted into history cache for {height}");
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// Gets a coin.
    pub async fn get_coin(&self, coinid: CoinID) -> Result<Option<CoinDataHeight>, ClientError> {
        self.get_smt_value_serde(Substate::Coins, coinid).await
    }

    /// Gets a coin count.
    pub async fn get_coin_count(&self, covhash: Address) -> Result<Option<u64>, ClientError> {
        let val = self
            .get_smt_value(Substate::Coins, covhash.0.hash_keyed(b"coin_count"))
            .await?;
        if val.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                stdcode::deserialize(&val)
                    .context("bad error count")
                    .map_err(|e| {
                        ClientError::InvalidState(anyhow::anyhow!(
                            "bad coin count {:?} {:?}",
                            val,
                            e.to_string()
                        ))
                    })?,
            ))
        }
    }

    /// Gets all the coins associated with the given address. Return None if the server simply does not index this information.
    pub async fn get_coins(
        &self,
        covhash: Address,
    ) -> Result<Option<BTreeMap<CoinID, CoinDataHeight>>, ClientError> {
        println!("GETTING COYNES");
        let coins = self
            .raw
            .get_some_coins(self.height, covhash)
            .await
            .map_err(to_neterr)?;
        if let Some(coins) = coins {
            let coins: HashSet<CoinID> = coins.into_iter().collect();
            let count = self.get_coin_count(covhash).await?;
            if let Some(count) = count {
                dbg!(count);
                if count != coins.len() as u64 {
                    return Err(ClientError::InvalidState(anyhow::anyhow!(
                        "got incomplete list of {} coins rather than {}",
                        coins.len(),
                        count
                    )));
                }
            }
            // fill in the coins
            let mut coin_futs = BTreeMap::new();
            for coin in coins {
                let this = self.clone();
                // TODO spawn this somewhere for parallelness
                let fut_data = async move {
                    let r = this
                        .get_coin(coin)
                        .await?
                        .context("invalid data received while getting coin list")
                        .map_err(ClientError::InvalidState)?;
                    Ok::<_, ClientError>(r)
                };
                coin_futs.insert(coin, fut_data);
            }
            let mut toret = BTreeMap::new();
            for (i, (k, v)) in coin_futs.into_iter().enumerate() {
                let v = v.await?;
                log::debug!("loading coin {}", i);
                toret.insert(k, v);
            }
            Ok(Some(toret))
        } else {
            Ok(None)
        }
    }

    /// A helper function to gets the CoinDataHeight for a coin *spent* at this height. This requires special handling because if the coin was created and spent at the same height, then the coin would never appear in a confirmed coin mapping.
    pub async fn get_coin_spent_here(
        &self,
        coinid: CoinID,
    ) -> Result<Option<CoinDataHeight>, ClientError> {
        // First we try the transactions mapping in this block.
        if let Some(tx) = self.get_transaction(coinid.txhash).await? {
            // Great. Now we can reconstruct the CDH from the transaction.
            return Ok(tx
                .outputs
                .get(coinid.index as usize)
                .map(|v| CoinDataHeight {
                    coin_data: v.clone(),
                    height: self.height,
                }));
        }
        // Okay, so that didn't really work. That means that if the CDH does exist, it's in the previous block's coin mapping.
        self.get_older(self.height.0.saturating_sub(1).into())
            .await?
            .get_coin(coinid)
            .await
    }

    /// Gets a pool info.
    pub async fn get_pool(&self, denom: PoolKey) -> Result<Option<PoolState>, ClientError> {
        self.get_smt_value_serde(Substate::Pools, denom).await
    }

    /// Gets a stake info.
    pub async fn get_stake(
        &self,
        staking_txhash: HashVal,
    ) -> Result<Option<StakeDoc>, ClientError> {
        self.get_smt_value_serde(Substate::Stakes, staking_txhash)
            .await
    }

    /// Gets a transaction.
    pub async fn get_transaction(
        &self,
        txhash: TxHash,
    ) -> Result<Option<Transaction>, ClientError> {
        self.get_smt_value_serde(Substate::Transactions, txhash)
            .await
    }

    /// Gets all the coin changes relevant to this address, at this height.
    pub async fn get_coin_changes(&self, address: Address) -> Result<Vec<CoinChange>, ClientError> {
        // We first just ask the raw RPC endpoint
        let claimed_changes = self
            .raw
            .get_coin_changes(self.height, address)
            .await
            .context("failed to get coin changes")
            .map_err(ClientError::NetworkError)?
            .ok_or_else(|| {
                ClientError::InvalidNodeConfig(anyhow::anyhow!(
                    "coin index not available on the server"
                ))
            })?;
        // Then, we make sure the coin changes are legit. This involves 4 things:
        // - Every coin added must appear in this snapshot, unless it is spent within the same height.
        // - Every coin added must have a txhash pointing to a transaction that does exist in this block.
        // - Every coin deleted must NOT appear in this snapshot
        // - The "net" coins added must be reflected in the change to the coin count of this address.
        // We check these things in sequence
        let net_coins_added = claimed_changes
            .iter()
            .filter_map(|change| {
                if let CoinChange::Add(coinid) = change {
                    if !claimed_changes.iter().any(|other| match other {
                        CoinChange::Add(_) => false,
                        CoinChange::Delete(other_coinid, _) => other_coinid == coinid,
                    }) {
                        return Some(*coinid);
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        for &coin in net_coins_added.iter() {
            if self.get_coin(coin).await?.is_none() {
                return Err(ClientError::InvalidState(anyhow::anyhow!(
                    "invalid coin {coin} claimed to be added, since not found in state"
                )));
            }
        }
        for &coin in claimed_changes.iter() {
            match coin {
                CoinChange::Add(coinid) => {
                    if self.get_transaction(coinid.txhash).await?.is_none()
                        && coinid != CoinID::proposer_reward(self.height)
                    {
                        return Err(ClientError::InvalidState(anyhow::anyhow!(
                        "invalid coin {coinid} claimed to be added, since not found in transactions"
                    )));
                    }
                }
                CoinChange::Delete(coinid, txhash) => {
                    if self.get_coin(coinid).await?.is_some() {
                        return Err(ClientError::InvalidState(anyhow::anyhow!(
                            "invalid deletion of {coinid} claimed, since it's still around lol"
                        )));
                    }
                    if self.get_transaction(txhash).await?.is_none() {
                        return Err(ClientError::InvalidState(anyhow::anyhow!(
                            "invalid deletion of {coinid} claimed, since the claimed transaction that deleted it is nowhere to be found"
                        )));
                    }
                }
            }
        }
        let net_count_change = claimed_changes
            .iter()
            .map(|change| match change {
                CoinChange::Add(_) => 1i128,
                CoinChange::Delete(..) => -1i128,
            })
            .sum::<i128>();
        let previous_count = self
            .get_older(self.height.0.saturating_sub(1).into())
            .await?
            .get_coin_count(address)
            .await?
            .unwrap_or_default();
        let current_count = self.get_coin_count(address).await?.unwrap_or_default();
        if current_count as i128 - previous_count as i128 != net_count_change {
            return Err(ClientError::InvalidState(anyhow::anyhow!("claimed a coin count change of {net_count_change} when in reality it changed from {previous_count} to {current_count}")));
        }
        Ok(claimed_changes)
    }

    /// Helper for serde.
    async fn get_smt_value_serde<S: Serialize, D: DeserializeOwned + Serialize>(
        &self,
        substate: Substate,
        key: S,
    ) -> Result<Option<D>, ClientError> {
        let val = self
            .get_smt_value(
                substate,
                tmelcrypt::hash_single(&stdcode::serialize(&key).unwrap()),
            )
            .await?;
        if val.is_empty() {
            return Ok(None);
        }
        let val = stdcode::deserialize(&val)
            .context("fatal deserialization error while getting SMT value")
            .map_err(ClientError::InvalidState)?;
        Ok(Some(val))
    }

    /// Gets a local SMT branch, validated.
    pub async fn get_smt_value(
        &self,
        substate: Substate,
        key: HashVal,
    ) -> Result<Bytes, ClientError> {
        if let Some(cached) = self
            .cache
            .get_smt_branch(self.header.hash(), substate, key)
            .await
        {
            Ok(cached)
        } else {
            let verify_against = match substate {
                Substate::Coins => self.header.coins_hash,
                Substate::History => self.header.history_hash,
                Substate::Pools => self.header.pools_hash,
                Substate::Stakes => self.header.stakes_hash,
                Substate::Transactions => self.header.transactions_hash,
            };
            let (val, branch) = self
                .raw
                .get_smt_branch(self.height, substate, key)
                .await
                .map_err(to_neterr)?
                .context("smt branch suddenly absent")
                .map_err(ClientError::InvalidState)?;
            let branch = branch
                .decompress()
                .context("invalidly compressed SMT branch")
                .map_err(ClientError::InvalidState)?;
            if !branch.verify(verify_against.0, key.0, &val) {
                return Err(ClientError::InvalidState(anyhow::anyhow!(
                    "unable to verify merkle proof for height {:?}, substate {:?}, key {:?}, value {:?}, branch {:?}",
                    self.height, substate, key, val, branch
                )));
            }
            self.cache
                .insert_smt_branch(self.header.hash(), substate, key, &val)
                .await;
            Ok(val.into())
        }
    }
}
