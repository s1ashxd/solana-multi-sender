use std::net::SocketAddr;
use std::sync::Arc;

use crate::provider::response::JsonRpcCodec;
use crate::provider::{
    HttpAuth, HttpEndpoint, Protocol, ProviderConfig, QuicAuth, QuicEndpoint, QuicProfile,
};

const MAX_BODY: usize = 1644;

fn http_cfg(
    host: &str,
    port: u16,
    path: &str,
    server_name: &str,
    auth: HttpAuth,
    body_template: &str,
) -> ProviderConfig {
    ProviderConfig {
        protocol: Protocol::Http,
        endpoint: HttpEndpoint {
            host: host.to_string(),
            port,
            path: path.to_string(),
            server_name: server_name.to_string(),
        },
        quic_endpoint: None,
        quic_profile: None,
        quic_auth: None,
        auth,
        max_body: MAX_BODY,
        body_template: body_template.to_string(),
        codec: Arc::new(JsonRpcCodec),
    }
}

pub fn jito_transaction(token: &str) -> ProviderConfig {
    let host = "mainnet.block-engine.jito.wtf";
    let path = format!("/api/v1/transactions?uuid={token}");
    let auth = HttpAuth::Header {
        name: "x-jito-auth".into(),
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64"}]}"#;
    http_cfg(host, 443, &path, host, auth, body)
}

pub fn jito_bundle(token: &str) -> ProviderConfig {
    let host = "mainnet.block-engine.jito.wtf";
    let path = format!("/api/v1/bundles?uuid={token}");
    let auth = HttpAuth::Header {
        name: "x-jito-auth".into(),
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendBundle","params":[["<<BODY>>"],{"encoding":"base64"}]}"#;
    http_cfg(host, 443, &path, host, auth, body)
}

pub fn nextblock(token: &str) -> ProviderConfig {
    let host = "fra.nextblock.io";
    let auth = HttpAuth::Header {
        name: "Authorization".into(),
        value: token.to_string(),
    };
    let body = r#"{"transaction":{"content":"<<BODY>>"},"frontRunningProtection":false}"#;
    http_cfg(host, 443, "/api/v2/submit", host, auth, body)
}

pub fn zeroslot(token: &str) -> ProviderConfig {
    let host = "de.0slot.trade";
    let auth = HttpAuth::UrlParam {
        key: "api-key",
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

pub fn temporal(token: &str) -> ProviderConfig {
    let host = "ny.temporal.xyz";
    let auth = HttpAuth::UrlParam {
        key: "c",
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64"}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

pub fn bloxroute(token: &str) -> ProviderConfig {
    let host = "virginia.solana.dex.blxrbdn.com";
    let auth = HttpAuth::Header {
        name: "Authorization".into(),
        value: token.to_string(),
    };
    let body = r#"{"transaction":{"content":"<<BODY>>"},"frontRunningProtection":false,"useStakedRPCs":true}"#;
    http_cfg(host, 443, "/api/v2/submit", host, auth, body)
}

pub fn node1(token: &str) -> ProviderConfig {
    let host = "rpc.node1.io";
    let auth = HttpAuth::Header {
        name: "api-key".into(),
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

pub fn flashblock(token: &str) -> ProviderConfig {
    let host = "fra.flashblock.io";
    let auth = HttpAuth::Header {
        name: "Authorization".into(),
        value: token.to_string(),
    };
    let body = r#"{"transactions":["<<BODY>>"]}"#;
    http_cfg(host, 443, "/api/v2/submit-batch", host, auth, body)
}

pub fn blockrazor(token: &str) -> ProviderConfig {
    let host = "solana.blockrazor.xyz";
    let auth = HttpAuth::Header {
        name: "apikey".into(),
        value: token.to_string(),
    };
    let body = r#"{"transaction":"<<BODY>>","mode":"fast"}"#;
    http_cfg(host, 443, "/sendTransaction", host, auth, body)
}

pub fn astralane(token: &str) -> ProviderConfig {
    let host = "solana-ny.astralane.io";
    let auth = HttpAuth::Header {
        name: "api_key".into(),
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true},{"mevProtect":false}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

pub fn stellium(token: &str) -> ProviderConfig {
    let host = "tx.stellium.io";
    let path = format!("/{token}");
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64"}]}"#;
    http_cfg(host, 443, &path, host, HttpAuth::None, body)
}

pub fn lightspeed(token: &str) -> ProviderConfig {
    let host = "solana.lightspeed.supply";
    let auth = HttpAuth::UrlParam {
        key: "api_key",
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true,"preflightCommitment":"processed","maxRetries":0}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

pub fn helius(token: &str) -> ProviderConfig {
    let host = "mainnet.helius-rpc.com";
    let auth = HttpAuth::UrlParam {
        key: "api-key",
        value: token.to_string(),
    };
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["<<BODY>>",{"encoding":"base64","skipPreflight":true,"maxRetries":0}]}"#;
    http_cfg(host, 443, "/", host, auth, body)
}

fn quic_cfg(
    host: &str,
    server_name: &str,
    addr: SocketAddr,
    auth: QuicAuth,
    profile: QuicProfile,
) -> ProviderConfig {
    ProviderConfig {
        protocol: Protocol::Quic,
        endpoint: HttpEndpoint {
            host: host.to_string(),
            port: addr.port(),
            path: String::new(),
            server_name: server_name.to_string(),
        },
        quic_endpoint: Some(QuicEndpoint {
            addr,
            server_name: server_name.to_string(),
        }),
        quic_profile: Some(profile),
        quic_auth: Some(auth),
        auth: HttpAuth::None,
        max_body: MAX_BODY,
        body_template: String::new(),
        codec: Arc::new(JsonRpcCodec),
    }
}

pub fn soyas(addr: SocketAddr, keypair: [u8; 64]) -> ProviderConfig {
    quic_cfg(
        "soyas-landing.solana.io",
        "soyas-landing",
        addr,
        QuicAuth::Ed25519Keypair(keypair),
        QuicProfile::soyas(),
    )
}

pub fn speedlanding(addr: SocketAddr, keypair: [u8; 64]) -> ProviderConfig {
    quic_cfg(
        "speed-landing.solana.io",
        "speed-landing",
        addr,
        QuicAuth::Ed25519Keypair(keypair),
        QuicProfile::speedlanding(),
    )
}

pub fn falcon_quic(addr: SocketAddr, api_key: String) -> ProviderConfig {
    quic_cfg(
        "fra.falcon.wtf",
        "falcon",
        addr,
        QuicAuth::FalconApiKey(api_key),
        QuicProfile::falcon(),
    )
}
