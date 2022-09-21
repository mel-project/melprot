use argh::FromArgs;
use themelio_nodeprot::Substate;
use themelio_structs::{Address, BlockHeight, Transaction, TxHash};
use tmelcrypt::HashVal;

fn main() {
    println!("I AM CLI");
    todo!();
}

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command specifying the RPC method to call.
pub struct Args {
    #[argh(subcommand)]
    rpc_method: RpcMethod,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum RpcMethod {
    SendTx(SendTxArgs),
    GetAbbrBlock(GetAbbrBlockArgs),
    GetSummary,
    GetSmtBranch(GetSmtBranchArgs),
    GetStakersRaw(GetStakersRawArgs),
    GetPartialBlock(GetPartialBlockArgs),
    GetSomeCoins(GetSomeCoinsArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "send_tx_args")]
/// Arguments for the `SendTx` RPC.
struct SendTxArgs {
    #[argh(option)]
    /// transaction to send
    transaction: Transaction,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_abbr_block_args")]
/// Arguments for the `GetAbbrBlock` RPC.
struct GetAbbrBlockArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_smt_branch_args")]
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
#[argh(subcommand, name = "get_stakers_raw_args")]
/// Arguments for the `GetStakersRaw` RPC.
struct GetStakersRawArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "get_partial_block_args")]
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
#[argh(subcommand, name = "get_some_coins_args")]
/// Arguments for the `GetSomeCoins` RPC.
struct GetSomeCoinsArgs {
    #[argh(option)]
    /// block height
    height: BlockHeight,

    #[argh(option)]
    /// address
    address: Address,
}
