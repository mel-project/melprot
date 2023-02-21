use std::{collections::BTreeMap, net::SocketAddr};

use argh::FromArgs;

use melnet2::{wire::http::HttpBackhaul, Backhaul};
use melprot::Substate;
use melprot::{Client, NodeRpcClient, Snapshot};
use themelio_structs::{
    Address, BlockHeight, CoinDataHeight, CoinID, Header, NetID, PoolKey, PoolState, TxHash,
};
use tmelcrypt::HashVal;

fn main() {
    smolscale::block_on(async move {
        let args: Args = argh::from_env();
        let backhaul = HttpBackhaul::new();
        let rpc_client = NodeRpcClient::from(
            backhaul
                .connect(args.addr.to_string().into())
                .await
                .expect("failed to create RPC client"),
        );

        // one-off set up to "trust" a custom network.
        let client = Client::new(args.netid, rpc_client);
        let snapshot = client.insecure_latest_snapshot().await.unwrap();

        match args.client_method {
            ClientMethod::Snapshot(args) => {
                let snapshot = client.snapshot().await.expect("snapshot error");
                print_snapshot_info(snapshot, args).await;
            }
            ClientMethod::OlderSnapshot(args) => {
                if let Some(height) = args.height {
                    client.older_snapshot(height).await.expect("snapshot error");
                    print_snapshot_info(snapshot, args).await;
                }
            }
            ClientMethod::SendTxRaw(_) => {
                todo!()
            }
            ClientMethod::GetSmtBranchRaw(_) => {
                todo!()
            }
            ClientMethod::GetSummaryRaw(_) => {
                let summary = snapshot
                    .get_raw()
                    .get_summary()
                    .await
                    .expect("get_summary error");
                println!("get_summary result: {:?}", summary);
            }
            ClientMethod::GetAbbrBlockRaw(args) => {
                let abbr_block = snapshot
                    .get_raw()
                    .get_abbr_block(args.height)
                    .await
                    .expect("get_abbr_block error");
                println!("get_abbr_block result: {:?}", abbr_block);
            }
            ClientMethod::GetStakersRaw(args) => {
                let stakers = snapshot
                    .get_raw()
                    .get_stakers_raw(args.height)
                    .await
                    .expect("get_stakers_raw error");
                println!("get_stakers_raw result: {:?}", stakers);
            }
            ClientMethod::GetPartialBlockRaw(mut args) => {
                args.tx_hashes.sort_unstable();
                let hvv = args.tx_hashes;

                if let Some(mut blk) = snapshot.get_raw().get_block(args.height).await.unwrap() {
                    blk.transactions
                        .retain(|h| hvv.binary_search(&h.hash_nosigs()).is_ok());
                    println!("get_partial_block result: {:?}", &blk);
                } else {
                    println!("no results for get_partial_block");
                }
            }
            ClientMethod::GetSomeCoinsRaw(args) => {
                let coins = snapshot
                    .get_raw()
                    .get_some_coins(args.height, args.address)
                    .await
                    .unwrap();
                println!("get_some_coins result: {:?}", coins);
            }
        }
    });
}

async fn print_snapshot_info(snapshot: Snapshot, args: SnapshotArgs) {
    let current_block = snapshot.current_block().await.expect("current_block error");
    // TODO: snapshot.current_block_with_known()
    let current_header = snapshot.current_header();

    let coin: Option<CoinDataHeight> = if let Some(coin_id) = args.coin_id {
        snapshot.get_coin(coin_id).await.expect("get_coin error")
    } else {
        None
    };

    let coin_count: Option<u64> = if let Some(covhash) = args.covhash {
        snapshot
            .get_coin_count(covhash)
            .await
            .expect("get_coin_count error")
    } else {
        None
    };

    let coin_data_height: Option<CoinDataHeight> = if let Some(coin_id) = args.coin_id {
        snapshot
            .get_coin_spent_here(coin_id)
            .await
            .expect("get_coin_spent_here error")
    } else {
        None
    };

    let coins: Option<BTreeMap<CoinID, CoinDataHeight>> = if let Some(covhash) = args.covhash {
        snapshot.get_coins(covhash).await.expect("get_coins error")
    } else {
        None
    };

    let history: Option<Header> = if let Some(height) = args.height {
        snapshot
            .get_history(height)
            .await
            .expect("get_history error")
    } else {
        None
    };

    let pool: Option<PoolState> = if let Some(denom) = args.denom {
        snapshot.get_pool(denom).await.expect("get_pool error")
    } else {
        None
    };

    println!(
        "===== CURRENT_BLOCK =====
        {:#?},
        ====================\n
        ==== CURRENT_HEADER ====
        {:#?},
        ====================\n
        ==== COIN ====
        {:#?},
        ====================\n
        ==== COIN_COUNT ====
        {:#?},
        ====================\n
        ==== COIN_DATA_HEIGHT ====
        {:#?},
        ====================\n
        ==== COINS ====
        {:#?},
        ====================\n
        ==== HISTORY ====
        {:#?},
        ====================\n
        ==== POOL ====
        {:#?}",
        current_block, current_header, coin, coin_count, coin_data_height, coins, history, pool
    );
}

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command specifying the RPC method to call.
pub struct Args {
    #[argh(subcommand)]
    /// umbrella field for the RPC method to call.
    client_method: ClientMethod,

    #[argh(option)]
    /// the address of the node server to talk to.
    addr: SocketAddr,

    #[argh(option)]
    /// network ID.
    netid: NetID,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum ClientMethod {
    // ValClient methods
    Snapshot(SnapshotArgs),
    OlderSnapshot(SnapshotArgs),

    // Raw RPC methods
    SendTxRaw(SendTxArgs),
    GetAbbrBlockRaw(GetAbbrBlockArgs),
    GetSummaryRaw(GetSummaryArgs),
    GetSmtBranchRaw(GetSmtBranchArgs),
    GetStakersRaw(GetStakersRawArgs),
    GetPartialBlockRaw(GetPartialBlockArgs),
    GetSomeCoinsRaw(GetSomeCoinsArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "snapshot")]
/// Arguments for the `Snapshot` command.
/// These arguments are valid for `OlderSnapshot` as well.
struct SnapshotArgs {
    #[argh(option)]
    /// block height
    height: Option<BlockHeight>,

    #[argh(option)]
    /// coin ID
    coin_id: Option<CoinID>,

    #[argh(option)]
    /// covhash address
    covhash: Option<Address>,

    #[argh(option)]
    /// pool key `Denom`
    denom: Option<PoolKey>,

    #[argh(option)]
    /// staking transaction hash
    staking_txhash: Option<HashVal>,

    #[argh(option)]
    /// transaction hash
    txhash: Option<TxHash>,

    #[argh(option)]
    /// SMT substate
    smt_substate: Option<Substate>,

    #[argh(option)]
    /// SMT key
    smt_key: Option<HashVal>,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_trusted_stakers_args")]
/// Arguments for the `GetTrustedStakers` command.
struct GetTrustedStakerArgs {}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "send_tx_args")]
/// Arguments for the `SendTx` RPC.
struct SendTxArgs {
    #[argh(option)]
    /// transaction string to send
    transaction: String,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_summary")]
/// Arguments for the `SendTx` RPC.
struct GetSummaryArgs {}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_abbr_block")]
/// Arguments for the `GetAbbrBlock` RPC.
struct GetAbbrBlockArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_smt_branch")]
/// Arguments for the `GetSmtBranch` RPC.
struct GetSmtBranchArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,
    #[argh(option)]

    /// substate
    substate: Substate,

    #[argh(option)]
    /// hash value
    hashval: HashVal,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_stakers_raw")]
/// Arguments for the `GetStakersRaw` RPC.
struct GetStakersRawArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_partial_block")]
/// Arguments for the `GetPartialBlock` RPC.
struct GetPartialBlockArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,

    #[argh(option)]
    /// transaction hashes
    tx_hashes: Vec<TxHash>,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_some_coins")]
/// Arguments for the `GetSomeCoins` RPC.
struct GetSomeCoinsArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,

    #[argh(option)]
    /// address
    address: Address,
}
