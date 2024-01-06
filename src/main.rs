use std::collections::HashMap;

use clap::Arg;
use dotenv::dotenv;
use prost::Message;
use serde::{Deserialize, Serialize};
use terra_proto::generated::cosmos::base::v1beta1::Coin;
use terra_proto::generated::cosmos::crypto::secp256k1::PubKey;
use terra_proto::generated::cosmos::tx::v1beta1::Tx;
use terra_proto::generated::ibc::core::client::v1::MsgUpdateClient;
//use terra_proto::generated::ibc::applications::transfer::v1::MsgTransfer;
//use terra_proto::generated::ibc::core::channel::v1::{MsgAcknowledgement, MsgRecvPacket};
//use terra_proto::generated::ibc::core::client::v1::{MsgCreateClient, MsgUpdateClient};
//use terra_proto::generated::ibc::core::connection::v1::{MsgConnectionOpenInit,MsgConnectionOpenConfirm,MsgConnectionOpenAck,MsgConnectionOpenTry};
use terra_rust_api::{PublicKey, Terra};
use terra_rust_cli::cli_helpers;

pub const STATE_NAME: &'static str = "get_ibc_req.json";

/// VERSION number of package
pub const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
/// NAME of package
pub const NAME: Option<&'static str> = option_env!("CARGO_PKG_NAME");

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct IBCState {
    pub height: u64,
}

pub struct IBCTx {
    pub coins: Vec<Coin>,
    pub account: String,
    pub sequence: u64,
    pub client: String,
    pub memo: String,

}

fn process_tx(hex: &str) -> anyhow::Result<Option<IBCTx>> {
    //let mut count = 0;
    let b64_decoded = base64::decode(hex)?;
    let tx: Tx = Tx::decode(b64_decoded.as_slice())?;

    if let Some(body) = tx.body {
        let mut client: String = String::from("-");
        let mut has_ibc = false;
        for msg in body.messages {
            /*
               match msg.type_url.as_str() {
                   "/ibc.core.channel.v1.MsgRecvPacket" => {
                       let recv = MsgRecvPacket::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", recv);
                       count += 1
                   }
                   "/ibc.core.client.v1.MsgCreateClient" => {
                       let cc: MsgCreateClient =
                           MsgCreateClient::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", cc)
                   }
                   "/ibc.core.channel.v1.MsgUpdateClient" => {
                       let upd = MsgUpdateClient::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", upd);
                       count += 1
                   }
                   "/ibc.core.channel.v1.MsgAcknowledgement" => {
                       let ack = MsgAcknowledgement::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", ack);
                       if let Some(packet) = ack.packet {
                           log::info!("{:?}", packet);
                       }
                       count += 1
                   }
                   "/ibc.applications.transfer.v1.MsgTransfer" => {
                       let transfer = MsgTransfer::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", transfer);

                       count += 1
                   }
                   "/ibc.core.connection.v1.MsgConnectionOpenInit" => {
                       let cc: MsgConnectionOpenInit =
                           MsgConnectionOpenInit::decode(msg.value.as_slice()).unwrap();
                       log::info!("{:?}", cc)
                   }
                   //   "/terra.oracle.v1beta1.MsgAggregateExchangeRateVote" => {}
                   //   "/terra.oracle.v1beta1.MsgAggregateExchangeRatePrevote" => {}
                   "/ibc.core.connection.v1.MsgConnectionOpenConfirm" => {  has_ibc = true}
                   "/ibc.core.connection.v1.MsgConnectionOpenAck" => {  has_ibc = true}
                   "/ibc.core.connection.v1.MsgConnectionOpenTry" => {  has_ibc = true}
                   _ => {
                       if msg.type_url.starts_with("/ibc") {
                           log::info!("TYPE={}", msg.type_url.as_str())
                             has_ibc = true
                       }
                   }
               }

            */
            // Transfers are not paid by relayers. so ignore them.
            if msg.type_url.as_str().starts_with("/ibc") {
                if msg.type_url == "/ibc.core.client.v1.MsgUpdateClient" {
                    let upd = MsgUpdateClient::decode(msg.value.as_slice()).unwrap();
                    //println!("ClientID:{}", upd.client_id);
                    client = upd.client_id.clone();
                }

                if msg.type_url != "/ibc.applications.transfer.v1.MsgTransfer"
                {
                    has_ibc = true;
                }
            }
        }
        if has_ibc {
            if let Some(auth) = tx.auth_info {
                if let Some(fee) = auth.fee {
                    let mut coin_str: String = Default::default();
                    for c in &fee.amount {
                        coin_str.push_str(&format!("{}{}", c.amount, c.denom))
                    }
                    log::debug!(
                        "FEES {} Granter:{} Payer:{} / gas:{} memo:{}",
                        coin_str,
                        fee.granter,
                        fee.payer,
                        fee.gas_limit,
                        body.memo
                    );
                    // only first signer gets credit.
                    if let Some(s) = auth.signer_infos.first() {
                        if let Some(pubkey) = &s.public_key {
                            let pub_key_p: terra_proto::generated::cosmos::crypto::secp256k1::PubKey = PubKey::decode(pubkey.value.as_slice()).unwrap();
                            let account = PublicKey::from_public_key(pub_key_p.key.as_slice());

                            log::debug!("Account:{} {} {}", account.account()?, s.sequence,&client);
                            let ibc = IBCTx {
                                coins: fee.amount,
                                account: account.account()?,
                                sequence: s.sequence,
                                client,
                                memo: body.memo.clone().replace(",","."),
                            };
                            return Ok(Some(ibc));
                        }
                    }
                }
            }
        }
    }

    Ok(None)
}

fn process_block(block_height: u64, txs: &Vec<String>) -> anyhow::Result<HashMap<String, HashMap<String, u128>>> {
    let mut tally: HashMap<String, HashMap<String, u128>> = Default::default();
    for tx in txs {
        if let Some(ibc) = process_tx(tx)? {
            let coins = ibc
                .coins
                .iter()
                .map(|x| (x.denom.clone(), x.amount.parse::<u128>().unwrap()))
                .collect::<HashMap<String, u128>>();
            let coinstr = coins.iter().map(|x| { format!("{}{}", x.1, x.0) }).collect::<Vec<String>>();

            println!("{},{},{},{},{}", block_height, ibc.account, coinstr.join("|"), ibc.client, ibc.memo);

            tally
                .entry(ibc.account)
                .and_modify(|x| {
                    for c in &coins {
                        x.entry(c.0.clone())
                            .and_modify(|f| *f += c.1)
                            .or_insert(c.1.clone());
                    }
                })
                .or_insert(coins.clone());
        }
    }
    Ok(tally)
}

async fn fetch_blocks(terra: &Terra, rpc: &str) -> anyhow::Result<IBCState> {
    let mut state: IBCState = if let Ok(file) = std::fs::File::open(STATE_NAME) {
        serde_json::from_reader(file)?
        //IBCState { height: 4988972 }
    } else {
        log::info!("Starting from origin IBC block");
        IBCState { height: 8397604 }
        //IBCState { height: 4988972 }
    };

    // let terra: Terra = Terra::lcd_client_no_tx(lcd, chain_id);
    let status = terra.rpc(rpc).status().await?;
    let mut height = status.sync_info.latest_block_height;
    //let tally: HashMap<String, HashMap<String, u128>>;
    while state.height <= height {
        while state.height <= height {
            let block = terra.rpc(&rpc).block_at_height(state.height).await?;
            let block_height = block.block.header.height;
            let txs = &block.block.data.txs.unwrap_or(vec![]);
            let block_tally = process_block(block_height, txs)?;

            // process_block_emit(block_height, txs, true)?;
            //let block_results: BlockResultsResult =                terra.rpc(rpc).block_results_at_height(state.height).await?;

            if block_tally.is_empty() {
                log::debug!(
                    "Processing Block {}/ HEAD={} -- {} ",
                    block_height,
                    height,
                    txs.len()
                );
            } else {
                for entry in block_tally.iter() {
                    let coins_str = entry
                        .1
                        .iter()
                        .map(|x| format!("{}{}", x.1, x.0))
                        .collect::<Vec<_>>()
                        .join(",");
                    //         println!("{},{},{}", block_height, entry.0, coins_str)
                }
                log::debug!(
                    "Processing Block {}/ HEAD={} -- {} {:?}",
                    block_height,
                    height,
                    txs.len(),
                    block_tally
                );
            }
            serde_json::to_writer(std::fs::File::create(STATE_NAME)?, &state)?;
            state.height = block_height + 1;
        }
        let status = terra.rpc(rpc).status().await?;
        height = status.sync_info.latest_block_height;
    }
    Ok(state)
}

async fn run() -> anyhow::Result<()> {
    let cli = cli_helpers::gen_cli_read_only("get_ibc_reqs", "get_ibc_reqs")
        .arg(
            Arg::new("rpc")
                .long("rpc")
                .value_name("rpc")
                .takes_value(true)
                .help("RPC port to use"),
        )
        .get_matches();
    let terra = cli_helpers::lcd_no_tx_from_args(&cli)?;
    let rpc = cli_helpers::get_arg_value(&cli, "rpc")?;
    log::info!("RPC={}", rpc);

    //    let json: serde_json::Value = serde_json::from_str(json_str)?;
    println!("{},{},{},{},{}", "block", "account", "fees", "src_port", "src_channel");
    fetch_blocks(&terra, rpc).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    if let Err(ref err) = run().await {
        log::error!("{}", err);
        err.chain()
            .skip(1)
            .for_each(|cause| log::error!("because: {}", cause));

        // The backtrace is not always generated. Try to run this example
        // with `$env:RUST_BACKTRACE=1`.
        //    if let Some(backtrace) = e.backtrace() {
        //        log::debug!("backtrace: {:?}", backtrace);
        //    }

        ::std::process::exit(1);
    }
}
