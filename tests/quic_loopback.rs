use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tx_sender::job::{Job, ProviderId, MAX_TX_LEN};
use tx_sender::provider::response::JsonRpcCodec;
use tx_sender::provider::{
    HttpAuth, HttpEndpoint, Protocol, ProviderConfig, QuicAuth, QuicEndpoint, QuicProfile,
};
use tx_sender::sink::{OutcomeKind, ProviderOutcome, ResultSink};
use tx_sender::source::TxSource;
use tx_sender::transport::Sender;

const TX_BODY: [u8; 8] = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];

struct FixedTx;

impl TxSource for FixedTx {
    fn fill(&self, _p: ProviderId, _j: Job, out: &mut [u8; MAX_TX_LEN]) -> u16 {
        out[..8].copy_from_slice(&TX_BODY);
        8
    }
}

struct ChanSink(Mutex<std::sync::mpsc::Sender<ProviderOutcome>>);

impl ResultSink for ChanSink {
    fn on_result(&self, o: ProviderOutcome) {
        let _ = self.0.lock().unwrap().send(o);
    }
}

#[derive(Debug)]
struct DangerAcceptAll;

impl rustls::client::danger::ServerCertVerifier for DangerAcceptAll {
    fn verify_server_cert(
        &self,
        _: &rustls_pki_types::CertificateDer,
        _: &[rustls_pki_types::CertificateDer],
        _: &rustls_pki_types::ServerName,
        _: &[u8],
        _: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls_pki_types::CertificateDer,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

fn spawn_quic_server() -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
    let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls_pki_types::CertificateDer::from(cert.cert.der().to_vec());
    let key_der = rustls_pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    server_crypto.alpn_protocols = vec![b"solana-tpu".to_vec()];
    let quic_server_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap();
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_cfg));

    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let endpoint =
                quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
            addr_tx.send(endpoint.local_addr().unwrap()).unwrap();
            if let Some(incoming) = endpoint.accept().await {
                if let Ok(conn) = incoming.await {
                    while let Ok(mut recv) = conn.accept_uni().await {
                        if let Ok(data) = recv.read_to_end(MAX_TX_LEN).await {
                            received_clone.lock().unwrap().push(data);
                        }
                    }
                }
            }
        });
    });

    let addr = addr_rx.recv().unwrap();
    (addr, received)
}

#[test]
fn quic_send_and_noresp_outcome() {
    let (server_addr, received) = spawn_quic_server();
    let (tx_outcome, rx_outcome) = std::sync::mpsc::channel();

    let tls_for_http = Arc::new(
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(DangerAcceptAll))
            .with_no_client_auth(),
    );

    let mut quic_profile = QuicProfile::soyas();
    quic_profile.idle_timeout_ms = 5_000;
    quic_profile.keepalive_interval_secs = 2;

    let provider = ProviderConfig {
        protocol: Protocol::Quic,
        endpoint: HttpEndpoint {
            host: String::new(),
            port: 0,
            path: String::new(),
            server_name: String::new(),
        },
        quic_endpoint: Some(QuicEndpoint {
            addr: server_addr,
            server_name: "localhost".into(),
        }),
        quic_profile: Some(quic_profile),
        quic_auth: Some(QuicAuth::NoCert),
        auth: HttpAuth::None,
        max_body: 0,
        codec: Arc::new(JsonRpcCodec),
    };

    let sender = Sender::builder(tls_for_http)
        .provider(provider)
        .source(Arc::new(FixedTx))
        .sink(Arc::new(ChanSink(Mutex::new(tx_outcome))))
        .build()
        .unwrap();

    sender.trigger(Job { id: 1, ctx: 0 }).unwrap();

    let outcome = rx_outcome.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(outcome.job, 1);
    assert!(matches!(outcome.kind, OutcomeKind::NoResponse));

    std::thread::sleep(Duration::from_millis(200));
    let pkts = received.lock().unwrap();
    assert!(!pkts.is_empty(), "server received no packets");
    assert_eq!(&pkts[0][..8], &TX_BODY);

    sender.shutdown();
}
