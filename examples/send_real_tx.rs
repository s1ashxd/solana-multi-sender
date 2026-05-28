use std::str::FromStr;
use std::sync::mpsc::{self, Sender as ChanSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_system_interface::instruction::transfer;

use tx_sender::job::{Job, ProviderId, MAX_TX_LEN};
use tx_sender::prelude::IoUringMode;
use tx_sender::provider::response::JsonRpcCodec;
use tx_sender::provider::{HttpAuth, HttpEndpoint, ProviderConfig, Protocol};
use tx_sender::sink::{OutcomeKind, ProviderOutcome, ResultSink};
use tx_sender::source::TxSource;
use tx_sender::transport::real_roots_client_config;
use tx_sender::transport::sequential::Sequential;
use tx_sender::transport::Sender;

const DEFAULT_CU_PRICE: u64 = 100_000;
const DEFAULT_CU_LIMIT: u32 = 100_000;
const MAX_BODY: usize = 1644;

struct PreparedTxs {
    per_provider: Vec<Vec<u8>>,
}

impl TxSource for PreparedTxs {
    fn fill(&self, provider: ProviderId, _job: Job, out: &mut [u8; MAX_TX_LEN]) -> u16 {
        let bytes = &self.per_provider[provider.idx()];
        out[..bytes.len()].copy_from_slice(bytes);
        bytes.len() as u16
    }
}

struct PrintSink(Mutex<ChanSender<ProviderOutcome>>);

impl ResultSink for PrintSink {
    fn on_result(&self, outcome: ProviderOutcome) {
        if let Ok(tx) = self.0.lock() {
            let _ = tx.send(outcome);
        }
    }
}

fn usage_and_exit() -> ! {
    eprintln!("send_real_tx builds a real signed Solana transaction with a provider tip and");
    eprintln!("submits it through tx-sender's Sequential io_uring HTTP path over TLS.");
    eprintln!();
    eprintln!("Required environment variables:");
    eprintln!("  TXSENDER_ENDPOINT      host[:port]/path of the SWQoS/RPC endpoint");
    eprintln!("                         (e.g. mainnet.block-engine.jito.wtf/api/v1/transactions)");
    eprintln!("  TXSENDER_KEYPAIR       path to a solana id.json (64-byte JSON array)");
    eprintln!("                         or a base58-encoded 64-byte secret key");
    eprintln!("  TXSENDER_TIP_ACCOUNT   base58 pubkey of the provider tip account");
    eprintln!("  TXSENDER_TIP_LAMPORTS  tip amount in lamports (u64)");
    eprintln!();
    eprintln!("Optional environment variables:");
    eprintln!("  TXSENDER_AUTH          HTTP auth header as \"Name: value\"");
    eprintln!("  TXSENDER_CU_PRICE      compute unit price in micro-lamports (default 100000)");
    eprintln!("  TXSENDER_CU_LIMIT      compute unit limit (default 100000)");
    eprintln!("  TXSENDER_BLOCKHASH     base58 recent blockhash to sign with");
    eprintln!("  TXSENDER_BLOCKHASH_RPC RPC URL to fetch a fresh blockhash when the above is unset");
    std::process::exit(2)
}

fn require(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!("missing required environment variable: {name}");
            eprintln!();
            usage_and_exit()
        }
    }
}

fn parse_endpoint(raw: &str) -> HttpEndpoint {
    let (authority, path) = match raw.find('/') {
        Some(pos) => (&raw[..pos], &raw[pos..]),
        None => (raw, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => {
            let port = p.parse::<u16>().unwrap_or_else(|_| {
                eprintln!("invalid port in TXSENDER_ENDPOINT: {p}");
                std::process::exit(2)
            });
            (h.to_string(), port)
        }
        None => (authority.to_string(), 443),
    };
    HttpEndpoint {
        host: host.clone(),
        port,
        path: path.to_string(),
        server_name: host,
    }
}

fn load_keypair(raw: &str) -> Result<Keypair, Box<dyn std::error::Error>> {
    if let Ok(contents) = std::fs::read_to_string(raw) {
        let bytes: Vec<u8> = serde_json_array(&contents)?;
        let arr: [u8; 64] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| "keypair file must contain a 64-byte array")?;
        return Keypair::try_from(&arr[..]).map_err(|e| e.into());
    }
    Ok(Keypair::from_base58_string(raw))
}

fn serde_json_array(contents: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let trimmed = contents.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or("keypair file is not a JSON array")?;
    let mut out = Vec::with_capacity(64);
    for tok in inner.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        out.push(tok.parse::<u8>()?);
    }
    Ok(out)
}

fn resolve_auth() -> HttpAuth {
    match std::env::var("TXSENDER_AUTH") {
        Ok(raw) if !raw.is_empty() => match raw.split_once(':') {
            Some((name, value)) => HttpAuth::Header {
                name: name.trim().to_string(),
                value: value.trim().to_string(),
            },
            None => {
                eprintln!("TXSENDER_AUTH must be in the form \"Name: value\"");
                std::process::exit(2)
            }
        },
        _ => HttpAuth::None,
    }
}

fn resolve_blockhash() -> Result<Hash, Box<dyn std::error::Error>> {
    if let Ok(raw) = std::env::var("TXSENDER_BLOCKHASH") {
        if !raw.is_empty() {
            return Hash::from_str(&raw).map_err(|e| format!("invalid TXSENDER_BLOCKHASH: {e}").into());
        }
    }
    if let Ok(url) = std::env::var("TXSENDER_BLOCKHASH_RPC") {
        if !url.is_empty() {
            let client = solana_client::rpc_client::RpcClient::new(url);
            return client.get_latest_blockhash().map_err(|e| e.into());
        }
    }
    Err("set TXSENDER_BLOCKHASH (base58) or TXSENDER_BLOCKHASH_RPC (RPC URL) to obtain a recent blockhash".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint_raw = require("TXSENDER_ENDPOINT");
    let keypair_raw = require("TXSENDER_KEYPAIR");
    let tip_account_raw = require("TXSENDER_TIP_ACCOUNT");
    let tip_lamports_raw = require("TXSENDER_TIP_LAMPORTS");

    let endpoint = parse_endpoint(&endpoint_raw);
    let keypair = load_keypair(&keypair_raw)?;
    let payer = keypair.pubkey();
    let tip_account = Pubkey::from_str(&tip_account_raw)
        .map_err(|e| format!("invalid TXSENDER_TIP_ACCOUNT: {e}"))?;
    let tip_lamports: u64 = tip_lamports_raw
        .parse()
        .map_err(|e| format!("invalid TXSENDER_TIP_LAMPORTS: {e}"))?;

    let cu_price = std::env::var("TXSENDER_CU_PRICE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CU_PRICE);
    let cu_limit = std::env::var("TXSENDER_CU_LIMIT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CU_LIMIT);

    let auth = resolve_auth();
    let blockhash = resolve_blockhash()?;

    let instructions = [
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        transfer(&payer, &tip_account, tip_lamports),
    ];
    let tx = Transaction::new_signed_with_payer(&instructions, Some(&payer), &[&keypair], blockhash);
    let bytes = bincode::serialize(&tx)?;
    if bytes.len() > MAX_TX_LEN {
        return Err(format!(
            "serialized transaction is {} bytes, exceeds MAX_TX_LEN ({MAX_TX_LEN})",
            bytes.len()
        )
        .into());
    }

    let signature = tx.signatures.first().map(|s| s.to_string()).unwrap_or_default();

    eprintln!("endpoint     {}:{}{}", endpoint.host, endpoint.port, endpoint.path);
    eprintln!("payer        {payer}");
    eprintln!("tip account  {tip_account}");
    eprintln!("tip lamports {tip_lamports}");
    eprintln!("cu price     {cu_price}");
    eprintln!("cu limit     {cu_limit}");
    eprintln!("blockhash    {blockhash}");
    eprintln!("tx size      {} bytes", bytes.len());
    eprintln!("signature    {signature}");

    let (out_tx, out_rx) = mpsc::channel::<ProviderOutcome>();

    let sender = Sender::builder()
        .transport(Sequential::io_uring(IoUringMode::BatchSyscall))
        .tls(real_roots_client_config())
        .provider(ProviderConfig {
            protocol: Protocol::Http,
            endpoint,
            quic_endpoint: None,
            quic_profile: None,
            quic_auth: None,
            auth,
            max_body: MAX_BODY,
            body_template: String::new(),
            codec: Arc::new(JsonRpcCodec),
        })
        .source(Arc::new(PreparedTxs {
            per_provider: vec![bytes],
        }))
        .sink(Arc::new(PrintSink(Mutex::new(out_tx))))
        .build()
        .map_err(|e| format!("failed to build sender: {e:?}"))?;

    sender
        .trigger(Job { id: 1, ctx: 0 })
        .map_err(|e| format!("failed to trigger job: {e:?}"))?;

    match out_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(outcome) => match outcome.kind {
            OutcomeKind::Accepted { signature } => {
                println!("accepted signature {signature}");
                println!("https://solscan.io/tx/{signature}");
            }
            OutcomeKind::Rejected { code, message } => {
                eprintln!("rejected code {code}: {message}");
            }
            OutcomeKind::Transport(err) => {
                eprintln!("transport error: {err:?}");
            }
            OutcomeKind::NoResponse => {
                eprintln!("no response from endpoint");
            }
        },
        Err(_) => {
            eprintln!("timed out waiting for an outcome");
        }
    }

    sender.shutdown();
    Ok(())
}
