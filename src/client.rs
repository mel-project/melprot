use anyhow::Context;
// use anyhow::Context;

use derivative::Derivative;

use melnet2::{wire::http::HttpBackhaul, Backhaul};

use nanorpc::{DynRpcTransport, RpcTransport};
use novasmt::{Database, InMemoryCas, Tree};
use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Serialize};
use smol::Task;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Instant,
};

use futures_util::Stream;

use themelio_structs::{
    Address, Block, BlockHeight, CoinDataHeight, CoinID, CoinValue, ConsensusProof, Header, NetID,
    PoolKey, PoolState, StakeDoc, Transaction, TxHash, STAKE_EPOCH,
};
use thiserror::Error;
use tmelcrypt::{HashVal, Hashable};

use crate::{cache::AsyncCache, InMemoryTrustStore, NodeRpcClient, NodeRpcError, Substate};

#[derive(Debug, Clone)]
pub struct TrustedHeight {
    pub height: BlockHeight,
    pub header_hash: HashVal,
}

#[derive(Error, Debug)]
pub enum ParseTrustedHeightError {
    #[error("expected a ':' character to split the height and header hash")]
    ParseSplitError,
    #[error("height is not an integer")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("failed to parse header hash as a hash")]
    ParseHeaderHash(#[from] hex::FromHexError),
}

impl FromStr for TrustedHeight {
    type Err = ParseTrustedHeightError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (height_str, hash_str) = s
            .split_once(':')
            .ok_or(ParseTrustedHeightError::ParseSplitError)?;

        let height = BlockHeight::from_str(height_str)?;
        let header_hash = HashVal::from_str(hash_str)?;

        Ok(Self {
            height,
            header_hash,
        })
    }
}

/// Standard interface for persisting a trusted block.
pub trait TrustStore {
    /// Set a trusted block in persistent storage, overriding the current
    /// value only if this one has a higher block height.
    fn set(&self, netid: NetID, trusted: TrustedHeight);
    /// Get the latest trusted block from persistent storage if one exists.
    fn get(&self, netid: NetID) -> Option<TrustedHeight>;
}

/// A higher-level client that validates all information.
#[derive(Derivative)]
#[derivative(Debug, Clone(bound = ""))]
pub struct ValClient {
    netid: NetID,
    #[derivative(Debug = "ignore")]
    trust_store: Arc<dyn TrustStore + Send + Sync + 'static>,
    #[derivative(Debug = "ignore")]
    cache: Arc<AsyncCache>,

    #[derivative(Debug = "ignore")]
    raw: Arc<NodeRpcClient<DynRpcTransport>>,
}

/// Errors that a ValClient may return
#[derive(Error, Debug)]
pub enum ValClientError {
    #[error("state validation error: {0}")]
    InvalidState(anyhow::Error),
    #[error("error during network communication: {0}")]
    NetworkError(anyhow::Error),
}

fn to_neterr(e: NodeRpcError<anyhow::Error>) -> ValClientError {
    // TODO something a little more intelligent
    ValClientError::NetworkError(anyhow::anyhow!("{}", e.to_string()))
}

impl ValClient {
    /// Creates a new ValClient, hardcoding the default, in-memory trust store.
    pub fn new<Net: RpcTransport>(netid: NetID, remote: NodeRpcClient<Net>) -> Self
    where
        Net::Error: Into<anyhow::Error>,
    {
        Self::new_with_truststore(netid, remote, InMemoryTrustStore::new())
    }

    /// Creates a new ValClient.
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
            cache: Arc::new(AsyncCache::new(10000)),
            raw: NodeRpcClient(DynRpcTransport::new(remote.0)).into(),
        }
    }

    /// A convenience method for connecting to a given address, using [melnet2::TcpBackhaul].
    pub async fn connect_melnet2_tcp(netid: NetID, addr: SocketAddr) -> anyhow::Result<Self> {
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

    /// Gets the netid.
    pub fn netid(&self) -> NetID {
        self.netid
    }

    /// Trust a height.
    pub fn trust(&self, trusted: TrustedHeight) {
        self.trust_store.set(self.netid, trusted);
    }

    /// NOTE: this is only used for testing (e.g. from CLI utils, etc.)
    /// Obtains the latest validated snapshot. Use this method first to get something to validate info against.
    #[deprecated]
    pub async fn insecure_latest_snapshot(&self) -> Result<ValClientSnapshot, ValClientError> {
        self.trust_latest().await?;
        self.snapshot().await
    }

    /// NOTE: this is only used for testing (e.g. from CLI utils, etc.)
    /// Trust the latest height
    #[deprecated]
    async fn trust_latest(&self) -> Result<(), ValClientError> {
        let summary = self.raw.get_summary().await.map_err(to_neterr)?;
        self.trust(TrustedHeight {
            height: summary.height,
            header_hash: summary.header.hash(),
        });
        Ok(())
    }

    /// Obtains a validated snapshot based on what height was trusted.
    pub async fn snapshot(&self) -> Result<ValClientSnapshot, ValClientError> {
        let _c = self.raw.clone();

        static INCEPTION: Lazy<Instant> = Lazy::new(Instant::now);
        // cache key: current time, divided by 10 seconds
        let cache_key = INCEPTION.elapsed().as_secs() / 10;
        let (height, header, _) = self
            .cache
            .get_or_try_fill((cache_key, "summary"), async {
                let summary = self.raw.get_summary().await.map_err(to_neterr)?;
                self.validate_height(summary.height, summary.header, summary.proof.clone())
                    .await?;
                Ok((summary.height, summary.header, summary.proof))
            })
            .await?;
        Ok(ValClientSnapshot {
            height,
            header,
            raw: self.raw.clone(),
            cache: self.cache.clone(),
        })
    }

    /// Convenience function to obtains a validated snapshot based on a given height.
    pub async fn older_snapshot(
        &self,
        height: BlockHeight,
    ) -> Result<ValClientSnapshot, ValClientError> {
        let snap = self.snapshot().await?;
        snap.get_older(height).await
    }

    /// Helper to validate a given block height and header.
    // #[async_recursion]
    fn validate_height(
        &self,
        height: BlockHeight,
        header: Header,
        proof: ConsensusProof,
    ) -> Task<Result<(), ValClientError>> {
        let this = self.clone();
        smolscale::spawn(async move {
            let safe_stakers = {
                loop {
                    let (safe_height, safe_stakers) = this.get_trusted_stakers().await?;
                    if height.epoch() > safe_height.epoch() + 1 {
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
                                ValClientError::InvalidState(anyhow::anyhow!(
                                    "old block gone while validating height"
                                ))
                            })?;
                        this.validate_height(old_height, old_block.header, old_proof)
                            .await?;
                    } else if height.epoch() > safe_height.epoch()
                        && safe_height.epoch() == (safe_height + BlockHeight(1)).epoch()
                    {
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
                            .map_err(ValClientError::InvalidState)?;
                        this.validate_height(epoch_ending_height, old_block.header, old_proof)
                            .await?;
                    } else {
                        break safe_stakers;
                    }
                }
            };
            // we use the stakers to validate the latest summary
            let mut good_votes = CoinValue(0);
            let mut total_votes = CoinValue(0);
            for (_, doc) in safe_stakers.iter() {
                let doc: StakeDoc = stdcode::deserialize(&doc)
                    .context("cannot deserialize stakedoc")
                    .map_err(ValClientError::InvalidState)?;
                if height.epoch() >= doc.e_start && height.epoch() < doc.e_post_end {
                    total_votes += doc.syms_staked;
                    if let Some(sig) = proof.get(&doc.pubkey) {
                        if doc.pubkey.verify(&header.hash(), sig) {
                            good_votes += doc.syms_staked
                        }
                    }
                }
            }

            if good_votes < total_votes * 2 / 3 {
                return Err(ValClientError::InvalidState(anyhow::anyhow!(
                    "remote height {} has insufficient votes (total_votes = {}, good_votes = {})",
                    height,
                    total_votes,
                    good_votes
                )));
            }
            // automatically update trust
            this.trust(TrustedHeight {
                height,
                header_hash: header.hash(),
            });
            Ok(())
        })
    }

    /// Helper function to obtain the trusted staker set.
    async fn get_trusted_stakers(
        &self,
    ) -> Result<(BlockHeight, Tree<InMemoryCas>), ValClientError> {
        let checkpoint = self
            .trust_store
            .get(self.netid)
            .context("expected to find a trusted block while fetching trusted stakers")
            .map_err(ValClientError::InvalidState)?;

        let temp_forest = Database::new(InMemoryCas::default());
        let stakers = self
            .raw
            .get_stakers_raw(checkpoint.height)
            .await
            .map_err(to_neterr)?
            .context("server did not give us the stakers for the height")
            .map_err(ValClientError::InvalidState)?;
        // first obtain trusted SMT branch
        let (abbr_block, _) = self
            .raw
            .get_abbr_block(checkpoint.height)
            .await
            .map_err(to_neterr)?
            .context("server did not give us the abbr block for the height")
            .map_err(ValClientError::InvalidState)?;
        if abbr_block.header.hash() != checkpoint.header_hash {
            return Err(ValClientError::InvalidState(anyhow::anyhow!(
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
            return Err(ValClientError::InvalidState(anyhow::anyhow!(
                "remote staker set contradicted valid header"
            )));
        }
        Ok((checkpoint.height, mapping))
    }

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
                        let current_snap = this.older_snapshot(current_height).await?;
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

    // pub fn traverse_fwd(&self) -> impl futures::Stream<Item = Transaction> {
    //     todo!()
    // }
}

/// A "snapshot" of the state at a given state. It essentially encapsulates a NodeClient and a trusted header.
#[derive(Clone)]
pub struct ValClientSnapshot {
    height: BlockHeight,
    header: Header,
    raw: Arc<NodeRpcClient<DynRpcTransport>>,
    cache: Arc<AsyncCache>,
}

impl ValClientSnapshot {
    /// Gets a reference to the raw, unvalidating raw client.
    pub fn get_raw(&self) -> &NodeRpcClient<DynRpcTransport> {
        &self.raw
    }

    /// Gets an older snapshot.
    pub async fn get_older(&self, old_height: BlockHeight) -> Result<Self, ValClientError> {
        if old_height > self.height {
            return Err(ValClientError::InvalidState(anyhow::anyhow!(
                "cannot travel into the future"
            )));
        }
        if old_height == self.height {
            return Ok(self.clone());
        }
        // Get an SMT branch
        let old_elem: Header = self
            .cache
            .get_or_try_fill(("header", old_height), async {
                let val = self
                    .get_smt_value(
                        Substate::History,
                        tmelcrypt::hash_single(&stdcode::serialize(&old_height).unwrap()),
                    )
                    .await?;
                let old_elem: Header = stdcode::deserialize(&val)
                    .context("cannot deserialize header")
                    .map_err(ValClientError::InvalidState)?;
                Ok::<_, ValClientError>(old_elem)
            })
            .await?;
        // this can never possibly be bad unless everything is horribly untrustworthy
        assert_eq!(old_elem.height, old_height);
        Ok(Self {
            height: old_height,
            header: old_elem,
            raw: self.raw.clone(),
            cache: self.cache.clone(),
        })
    }

    /// Gets the header.
    pub fn current_header(&self) -> Header {
        self.header
    }

    /// Helper function to obtain the proposer reward amount.
    pub async fn get_proposer_reward(&self) -> Result<CoinValue, ValClientError> {
        let reward_coin = self.get_coin(CoinID::proposer_reward(self.height)).await?;
        let reward_amount = reward_coin.map(|v| v.coin_data.value).unwrap_or_default();
        Ok(reward_amount)
    }

    /// Gets the whole block at this height.
    pub async fn current_block(&self) -> Result<Block, ValClientError> {
        self.current_block_with_known(|_| None).await
    }

    /// Gets the whole block at this height, with a function that gets cached transactions.
    pub async fn current_block_with_known(
        &self,
        get_known_tx: impl Fn(TxHash) -> Option<Transaction>,
    ) -> Result<Block, ValClientError> {
        self.cache
            .get_or_try_fill(("block", self.height), async {
                let header = self.current_header();
                let (block, _) = self
                    .raw
                    .get_full_block(self.height, get_known_tx)
                    .await
                    .map_err(to_neterr)?
                    .context("block disappeared from under our feet")
                    .map_err(ValClientError::InvalidState)?;
                if block.header != header {
                    return Err(ValClientError::InvalidState(anyhow::anyhow!(
                        "block header does not match: block header {:?} vs snapshot header {:?}",
                        block.header,
                        header
                    )));
                }
                Ok(block)
            })
            .await
    }

    /// Gets a historical header.
    pub async fn get_history(&self, height: BlockHeight) -> Result<Option<Header>, ValClientError> {
        self.get_smt_value_serde(Substate::History, height).await
    }

    /// Gets a coin.
    pub async fn get_coin(&self, coinid: CoinID) -> Result<Option<CoinDataHeight>, ValClientError> {
        self.get_smt_value_serde(Substate::Coins, coinid).await
    }

    /// Gets a coin count.
    pub async fn get_coin_count(&self, covhash: Address) -> Result<Option<u64>, ValClientError> {
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
                        ValClientError::InvalidState(anyhow::anyhow!(
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
    ) -> Result<Option<BTreeMap<CoinID, CoinDataHeight>>, ValClientError> {
        self.cache
            .get_or_try_fill(("coins", self.height, covhash), async {
                let coins = self
                    .raw
                    .get_some_coins(self.height, covhash)
                    .await
                    .map_err(to_neterr)?;
                if let Some(coins) = coins {
                    let coins: HashSet<CoinID> = coins.into_iter().collect();
                    let count = self.get_coin_count(covhash).await?;
                    if let Some(count) = count {
                        if count != coins.len() as u64 {
                            return Err(ValClientError::InvalidState(anyhow::anyhow!(
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
                                .map_err(ValClientError::InvalidState)?;
                            Ok::<_, ValClientError>(r)
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
            })
            .await
    }

    /// A helper function to gets the CoinDataHeight for a coin *spent* at this height. This requires special handling because if the coin was created and spent at the same height, then the coin would never appear in a confirmed coin mapping.
    pub async fn get_coin_spent_here(
        &self,
        coinid: CoinID,
    ) -> Result<Option<CoinDataHeight>, ValClientError> {
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
    pub async fn get_pool(&self, denom: PoolKey) -> Result<Option<PoolState>, ValClientError> {
        self.get_smt_value_serde(Substate::Pools, denom).await
    }

    /// Gets a stake info.
    pub async fn get_stake(
        &self,
        staking_txhash: HashVal,
    ) -> Result<Option<StakeDoc>, ValClientError> {
        self.get_smt_value_serde(Substate::Stakes, staking_txhash)
            .await
    }

    /// Gets a transaction.
    pub async fn get_transaction(
        &self,
        txhash: TxHash,
    ) -> Result<Option<Transaction>, ValClientError> {
        self.get_smt_value_serde(Substate::Transactions, txhash)
            .await
    }

    /// Helper for serde.
    async fn get_smt_value_serde<S: Serialize, D: DeserializeOwned + Serialize>(
        &self,
        substate: Substate,
        key: S,
    ) -> Result<Option<D>, ValClientError> {
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
            .map_err(ValClientError::InvalidState)?;
        Ok(Some(val))
    }

    /// Gets a local SMT branch, validated.
    pub async fn get_smt_value(
        &self,
        substate: Substate,
        key: HashVal,
    ) -> Result<Vec<u8>, ValClientError> {
        let verify_against = match substate {
            Substate::Coins => self.header.coins_hash,
            Substate::History => self.header.history_hash,
            Substate::Pools => self.header.pools_hash,
            Substate::Stakes => self.header.stakes_hash,
            Substate::Transactions => self.header.transactions_hash,
        };
        self.cache.get_or_try_fill((verify_against, key), async {
        let (val, branch) = self.raw.get_smt_branch(self.height, substate, key).await.map_err(to_neterr)?.context("smt branch suddenly absent").map_err(ValClientError::InvalidState)?;
        let branch = branch.decompress().context("invalidly compressed SMT branch").map_err(ValClientError::InvalidState)?;
            if !branch.verify(verify_against.0, key.0, &val) {
                return Err(ValClientError::InvalidState(anyhow::anyhow!(
                    "unable to verify merkle proof for height {:?}, substate {:?}, key {:?}, value {:?}, branch {:?}",
                    self.height, substate, key, val, branch
                )));
            }
            Ok(val)
        }).await
    }
}
