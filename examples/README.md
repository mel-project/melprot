# nodeprot-cli

This is a simple CLI utility to make RPC requests to a running node. It will mostly used for quick smoke tests.

## Running a node

In the [themelio-node](https://github.com/themeliolabs/themelio-node) repo, start up a node with the command below. Details about starting a local simnet can be found [here](https://github.com/themeliolabs/themelio-node#local-simnet-support).

```sh
themelio-node --override-genesis genesis.yml --listen 127.0.0.1:12345 --staker-cfg staker-cfg.yml --bootstrap 127.0.0.1:12345 --advertise 127.0.0.1:12345
```

In a separate terminal, navigate to the [themelio-nodeprot](https://github.com/themeliolabs/themelio-nodeprot) repo, and run the CLI tool. Use the `--help` flag to see all of the RPC options as subcommands. `CUSTOM_NETWORK` should be the same string as the one specified in the `genesis.yaml` in the command above.

```sh
cargo run --example nodeprot-cli -- --addr 127.0.0.1:12345 --netid <CUSTOM_NETWORK> --help
```

### Usage

````sh
Usage: nodeprot-cli --addr <addr> --netid <netid> <command> [<args>]

Top-level command specifying the RPC method to call.

Options:
  --addr            the address of the node server to talk to.
  --netid           network ID.
  --help            display usage information

Commands:
  snapshot          Arguments for the `Snapshot` command. These include
                    arguments for `ClientSnapshot` methods as well.
  snapshot          Arguments for the `Snapshot` command. These include
                    arguments for `ClientSnapshot` methods as well.
  send_tx_args      Arguments for the `SendTx` RPC.
  get_abbr_block    Arguments for the `GetAbbrBlock` RPC.
  get_summary       Arguments for the `SendTx` RPC.
  get_smt_branch    Arguments for the `GetSmtBranch` RPC.
  get_stakers_raw   Arguments for the `GetStakersRaw` RPC.
  get_partial_block Arguments for the `GetPartialBlock` RPC.
  get_some_coins    Arguments for the `GetSomeCoins` RPC.```

````
