pub mod response;

#[cfg(feature = "registry")]
pub mod registry;

use std::sync::Arc;

use crate::provider::response::ResponseCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Http,
    Quic,
}

#[derive(Clone, Debug)]
pub enum HttpAuth {
    None,
    Header { name: String, value: String },
    UrlParam { key: &'static str, value: String },
    UrlPath { token: String },
}

#[derive(Clone, Debug)]
pub struct HttpEndpoint {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub server_name: String,
}

#[derive(Clone, Debug)]
pub struct QuicEndpoint {
    pub addr: std::net::SocketAddr,
    pub server_name: String,
}

#[derive(Clone)]
pub struct QuicProfile {
    pub alpn: &'static [u8],
    pub idle_timeout_ms: u64,
    pub keepalive_interval_secs: u64,
    pub max_streams_uni: u32,
}

impl QuicProfile {
    pub fn soyas() -> Self {
        Self {
            alpn: b"solana-tpu",
            idle_timeout_ms: 300_000,
            keepalive_interval_secs: 30,
            max_streams_uni: 1_000_000,
        }
    }

    pub fn speedlanding() -> Self {
        Self {
            alpn: b"solana-tpu",
            idle_timeout_ms: 300_000,
            keepalive_interval_secs: 30,
            max_streams_uni: 1_000_000,
        }
    }

    pub fn falcon() -> Self {
        Self {
            alpn: b"falcon-tx",
            idle_timeout_ms: 30_000,
            keepalive_interval_secs: 25,
            max_streams_uni: 64,
        }
    }
}

#[derive(Clone)]
pub enum QuicAuth {
    Ed25519Keypair([u8; 64]),
    FalconApiKey(String),
    NoCert,
}

#[derive(Clone)]
pub struct ProviderConfig {
    pub protocol: Protocol,
    pub endpoint: HttpEndpoint,
    pub quic_endpoint: Option<QuicEndpoint>,
    pub quic_profile: Option<QuicProfile>,
    pub quic_auth: Option<QuicAuth>,
    pub auth: HttpAuth,
    pub max_body: usize,
    pub body_template: String,
    pub codec: Arc<dyn ResponseCodec>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quic_profile_soyas_fields() {
        let p = QuicProfile::soyas();
        assert_eq!(p.alpn, b"solana-tpu");
        assert_eq!(p.max_streams_uni, 1_000_000);
    }

    #[test]
    fn quic_auth_ed25519_keypair() {
        let kp = [1u8; 64];
        let auth = QuicAuth::Ed25519Keypair(kp);
        match auth {
            QuicAuth::Ed25519Keypair(k) => assert_eq!(k[0], 1),
            _ => panic!("wrong variant"),
        }
    }
}
