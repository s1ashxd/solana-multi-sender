use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use tx_sender::job::{Job, ProviderId, MAX_TX_LEN};
use tx_sender::provider::response::JsonRpcCodec;
use tx_sender::provider::{HttpAuth, HttpEndpoint, Protocol, ProviderConfig};
use tx_sender::sink::{OutcomeKind, ProviderOutcome, ResultSink};
use tx_sender::source::TxSource;
use tx_sender::transport::parallel::{CoreSet, Parallel};
use tx_sender::transport::Sender;

#[derive(Debug)]
struct AcceptAll;

impl ServerCertVerifier for AcceptAll {
    fn verify_server_cert(
        &self,
        _: &CertificateDer,
        _: &[CertificateDer],
        _: &ServerName,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

struct FixedTx;

impl TxSource for FixedTx {
    fn fill(&self, _p: ProviderId, _j: Job, out: &mut [u8; MAX_TX_LEN]) -> u16 {
        out[..4].copy_from_slice(&[1, 2, 3, 4]);
        4
    }
}

struct CollectSink(Mutex<mpsc::Sender<ProviderOutcome>>);

impl ResultSink for CollectSink {
    fn on_result(&self, o: ProviderOutcome) {
        let _ = self.0.lock().unwrap().send(o);
    }
}

fn spawn_mock_server(sig: &'static str) -> u16 {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert.der().to_vec());
    let key_der = rustls_pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();
    let server_cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut conn = rustls::ServerConnection::new(server_cfg).unwrap();
        while conn.is_handshaking() {
            while conn.wants_write() {
                let mut out = Vec::new();
                conn.write_tls(&mut out).unwrap();
                sock.write_all(&out).unwrap();
            }
            if conn.is_handshaking() {
                let mut buf = [0u8; 8192];
                let n = sock.read(&mut buf).unwrap_or(0);
                if n > 0 {
                    let mut sl: &[u8] = &buf[..n];
                    conn.read_tls(&mut sl).unwrap();
                    conn.process_new_packets().unwrap();
                }
            }
        }
        while conn.wants_write() {
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            sock.write_all(&out).unwrap();
        }
        let mut req = Vec::new();
        let drain = |conn: &mut rustls::ServerConnection, req: &mut Vec<u8>| {
            let mut tmp = [0u8; 8192];
            loop {
                match conn.reader().read(&mut tmp) {
                    Ok(0) => break,
                    Ok(m) => req.extend_from_slice(&tmp[..m]),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
        };
        drain(&mut conn, &mut req);
        while req.is_empty() {
            let mut buf = [0u8; 8192];
            let n = sock.read(&mut buf).unwrap_or(0);
            if n > 0 {
                let mut sl: &[u8] = &buf[..n];
                conn.read_tls(&mut sl).unwrap();
                conn.process_new_packets().unwrap();
                drain(&mut conn, &mut req);
            }
        }
        let body = format!(r#"{{"jsonrpc":"2.0","result":"{sig}","id":1}}"#);
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        conn.writer().write_all(resp.as_bytes()).unwrap();
        while conn.wants_write() {
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            sock.write_all(&out).unwrap();
        }
        let _ = sock.flush();
    });
    port
}

fn make_provider(port: u16) -> ProviderConfig {
    ProviderConfig {
        protocol: Protocol::Http,
        endpoint: HttpEndpoint {
            host: "127.0.0.1".into(),
            port,
            path: "/".into(),
            server_name: "localhost".into(),
        },
        quic_endpoint: None,
        quic_profile: None,
        quic_auth: None,
        auth: HttpAuth::None,
        max_body: 2000,
        codec: Arc::new(JsonRpcCodec),
    }
}

fn client_cfg() -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAll))
            .with_no_client_auth(),
    )
}

#[test]
fn parallel_two_providers_both_accepted() {
    let port0 = spawn_mock_server("sig_provider_0");
    let port1 = spawn_mock_server("sig_provider_1");

    let (tx, rx) = mpsc::channel();
    let sender = Sender::builder()
        .transport(Parallel::raw_libc())
        .tls(client_cfg())
        .provider(make_provider(port0))
        .provider(make_provider(port1))
        .source(Arc::new(FixedTx))
        .sink(Arc::new(CollectSink(Mutex::new(tx))))
        .build()
        .unwrap();

    sender.trigger(Job { id: 77, ctx: 0 }).unwrap();

    let mut outcomes: Vec<ProviderOutcome> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while outcomes.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(o) = rx.recv_timeout(Duration::from_millis(100)) {
            outcomes.push(o);
        }
    }

    assert_eq!(
        outcomes.len(),
        2,
        "expected outcomes from both providers, got {}",
        outcomes.len()
    );
    for o in &outcomes {
        assert_eq!(o.job, 77);
        match &o.kind {
            OutcomeKind::Accepted { signature } => {
                let expected = format!("sig_provider_{}", o.provider.idx());
                assert_eq!(signature, &expected, "wrong sig for provider {:?}", o.provider);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }
    let seen: std::collections::HashSet<usize> = outcomes.iter().map(|o| o.provider.idx()).collect();
    assert!(seen.contains(&0) && seen.contains(&1), "missing a provider outcome: {seen:?}");

    sender.shutdown();
}

#[test]
fn parallel_affinity_and_rt_config_accepted_by_builder() {
    let port = spawn_mock_server("any_sig");
    let (tx, _rx) = mpsc::channel();

    let result = Sender::builder()
        .transport(Parallel::raw_libc().affinity(CoreSet::new(vec![0])).rt_priority(1))
        .tls(client_cfg())
        .provider(make_provider(port))
        .source(Arc::new(FixedTx))
        .sink(Arc::new(CollectSink(Mutex::new(tx))))
        .build();

    assert!(result.is_ok(), "builder with affinity+rt should succeed: {:?}", result.err());
    result.unwrap().shutdown();
}
