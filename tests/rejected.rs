use std::sync::mpsc;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

use tx_sender::error::SenderError;
use tx_sender::job::{Job, ProviderId, MAX_TX_LEN};
use tx_sender::provider::response::JsonRpcCodec;
use tx_sender::provider::{HttpAuth, HttpEndpoint, ProviderConfig, Protocol};
use tx_sender::sink::{OutcomeKind, ProviderOutcome, ResultSink};
use tx_sender::source::TxSource;
use tx_sender::transport::sequential::Sequential;
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

struct ChanSink(std::sync::Mutex<mpsc::Sender<ProviderOutcome>>);

impl ResultSink for ChanSink {
    fn on_result(&self, o: ProviderOutcome) {
        let _ = self.0.lock().unwrap().send(o);
    }
}

fn serve_connection(mut sock: std::net::TcpStream, server_cfg: Arc<rustls::ServerConfig>) {
    use std::io::{Read, Write};

    let mut conn = rustls::ServerConnection::new(server_cfg).unwrap();
    let mut buf = [0u8; 8192];

    while conn.is_handshaking() {
        while conn.wants_write() {
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            sock.write_all(&out).unwrap();
        }
        if conn.is_handshaking() {
            let n = sock.read(&mut buf).unwrap_or(0);
            if n > 0 {
                let mut slice: &[u8] = &buf[..n];
                conn.read_tls(&mut slice).unwrap();
                conn.process_new_packets().unwrap();
            }
        }
    }

    while conn.wants_write() {
        let mut out = Vec::new();
        conn.write_tls(&mut out).unwrap();
        sock.write_all(&out).unwrap();
    }

    let mut request_buf = Vec::new();

    let drain_reader = |conn: &mut rustls::ServerConnection, req: &mut Vec<u8>| {
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

    drain_reader(&mut conn, &mut request_buf);

    while request_buf.is_empty() {
        let n = sock.read(&mut buf).unwrap_or(0);
        if n > 0 {
            let mut slice: &[u8] = &buf[..n];
            conn.read_tls(&mut slice).unwrap();
            conn.process_new_packets().unwrap();
            drain_reader(&mut conn, &mut request_buf);
        }
    }

    let body = br#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"blockhash not found"},"id":1}"#;
    let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
    conn.writer().write_all(resp.as_bytes()).unwrap();
    conn.writer().write_all(body).unwrap();

    while conn.wants_write() {
        let mut out = Vec::new();
        conn.write_tls(&mut out).unwrap();
        sock.write_all(&out).unwrap();
    }
    sock.flush().unwrap();
}

fn spawn_tls_server() -> u16 {
    use std::net::TcpListener;

    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(certified.cert.der().to_vec());
    let key_der =
        rustls_pki_types::PrivateKeyDer::try_from(certified.key_pair.serialize_der()).unwrap();

    let server_cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap(),
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    std::thread::spawn(move || {
        while let Ok((sock, _)) = listener.accept() {
            let cfg = server_cfg.clone();
            std::thread::spawn(move || serve_connection(sock, cfg));
        }
    });

    port
}

#[test]
fn end_to_end_http_rejected_outcome() {
    let port = spawn_tls_server();

    let client_cfg = Arc::new(
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAll))
            .with_no_client_auth(),
    );

    let (tx, rx) = mpsc::channel();
    let provider = ProviderConfig {
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
        body_template: String::new(),
        codec: Arc::new(JsonRpcCodec),
    };

    let sender = match Sender::builder()
        .transport(Sequential::raw_libc())
        .tls(client_cfg)
        .provider(provider)
        .source(Arc::new(FixedTx))
        .sink(Arc::new(ChanSink(std::sync::Mutex::new(tx))))
        .build()
    {
        Ok(s) => s,
        Err(SenderError::Unsupported(e)) => {
            eprintln!("transport unsupported on this kernel, skipping: {e}");
            return;
        }
        Err(e) => panic!("{e:?}"),
    };

    sender.trigger(Job { id: 7, ctx: 0 }).unwrap();

    let outcome = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(outcome.job, 7);
    match outcome.kind {
        OutcomeKind::Rejected { code, message } => {
            assert_eq!(code, -32002);
            assert!(message.contains("blockhash"));
        }
        other => panic!("{other:?}"),
    }

    sender.shutdown();
}
