use std::net::SocketAddr;

use argh::FromArgs;
use melnet2::{wire::tcp::TcpBackhaul, Backhaul};
use themelio_nodeprot::{NodeRpcClient, Substate, TrustedHeight, ValClient};
use themelio_structs::{Address, BlockHeight, NetID, Transaction, TxHash};
use tmelcrypt::HashVal;

fn main() {
    smolscale::block_on(async move {
        let args: Args = argh::from_env();
        println!("args: {:?}", args);
        let backhaul = TcpBackhaul::new();
        let rpc_client = NodeRpcClient(
            backhaul
                .connect(args.addr.to_string().into())
                .await
                .expect("failed to create RPC client"),
        );

        // one-off set up to test using a custom network.
        let client = ValClient::new(args.netid, rpc_client);
        let snapshot = client.insecure_latest_snapshot().await.unwrap();

        match args.rpc_method.into() {
            RpcMethod::GetSummary(_) => {
                let summary = snapshot
                    .get_raw()
                    .get_summary()
                    .await
                    .expect("get_summary error");
                println!("get_summary result: {:?}", summary);
            }
            _ => todo!(),
        }
    });
}

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command specifying the RPC method to call.
pub struct Args {
    #[argh(subcommand)]
    /// umbrella field for the RPC method to call.
    rpc_method: RpcMethod,

    #[argh(option)]
    /// the address of the node server to talk to.
    addr: SocketAddr,

    #[argh(option)]
    /// network ID.
    netid: NetID,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum RpcMethod {
    //SendTx(SendTxArgs),
    GetAbbrBlock(GetAbbrBlockArgs),
    GetSummary(GetSummaryArgs),
    //GetSmtBranch(GetSmtBranchArgs),
    GetStakersRaw(GetStakersRawArgs),
    GetPartialBlock(GetPartialBlockArgs),
    GetSomeCoins(GetSomeCoinsArgs),
}

// #[derive(FromArgs, PartialEq, Debug)]
// #[argh(subcommand, name = "send_tx_args")]
// /// Arguments for the `SendTx` RPC.
// struct SendTxArgs {
//     #[argh(option)]
//     /// transaction to send
//     transaction: Transaction,
// }

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

// #[derive(FromArgs, PartialEq, Debug)]
// #[argh(subcommand, name = "get_smt_branch")]
// /// Arguments for the `GetSmtBranch` RPC.
// struct GetSmtBranchArgs {
//     #[argh(option)]
//     /// block height
//     height: BlockHeight,
//     #[argh(option)]

//     /// substate
//     substate: Substate,

//     #[argh(option)]
//     /// hash value
//     hashval: HashVal,
// }

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
