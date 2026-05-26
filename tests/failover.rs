use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

struct ChanSink(Mutex<mpsc::Sender<ProviderOutcome>>);

impl ResultSink for ChanSink {
    fn on_result(&self, o: ProviderOutcome) {
        let _ = self.0.lock().unwrap().send(o);
    }
}

type KillHandles = Arc<Mutex<Vec<TcpStream>>>;

fn serve_connection(mut sock: TcpStream, server_cfg: Arc<rustls::ServerConfig>) {
    let mut conn = match rustls::ServerConnection::new(server_cfg) {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut buf = [0u8; 8192];

    while conn.is_handshaking() {
        while conn.wants_write() {
            let mut out = Vec::new();
            if conn.write_tls(&mut out).is_err() || sock.write_all(&out).is_err() {
                return;
            }
        }
        if conn.is_handshaking() {
            let n = match sock.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };
            let mut slice: &[u8] = &buf[..n];
            if conn.read_tls(&mut slice).is_err() || conn.process_new_packets().is_err() {
                return;
            }
        }
    }

    while conn.wants_write() {
        let mut out = Vec::new();
        if conn.write_tls(&mut out).is_err() || sock.write_all(&out).is_err() {
            return;
        }
    }

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

    let body = br#"{"jsonrpc":"2.0","result":"okSig","id":1}"#;
    let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());

    loop {
        let mut request_buf = Vec::new();
        drain_reader(&mut conn, &mut request_buf);

        while !request_buf.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = match sock.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };
            let mut slice: &[u8] = &buf[..n];
            if conn.read_tls(&mut slice).is_err() || conn.process_new_packets().is_err() {
                return;
            }
            drain_reader(&mut conn, &mut request_buf);
        }

        if conn.writer().write_all(resp.as_bytes()).is_err()
            || conn.writer().write_all(body).is_err()
        {
            return;
        }
        while conn.wants_write() {
            let mut out = Vec::new();
            if conn.write_tls(&mut out).is_err() || sock.write_all(&out).is_err() {
                return;
            }
        }
        if sock.flush().is_err() {
            return;
        }
    }
}

fn spawn_harness() -> (u16, KillHandles) {
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

    let handles: KillHandles = Arc::new(Mutex::new(Vec::new()));
    let accept_handles = handles.clone();

    std::thread::spawn(move || {
        while let Ok((sock, _)) = listener.accept() {
            if let Ok(clone) = sock.try_clone() {
                accept_handles.lock().unwrap().push(clone);
            }
            let cfg = server_cfg.clone();
            std::thread::spawn(move || serve_connection(sock, cfg));
        }
    });

    (port, handles)
}

fn kill_active(handles: &KillHandles) {
    let mut guard = handles.lock().unwrap();
    if guard.is_empty() {
        return;
    }
    let victim = guard.remove(0);
    let _ = victim.shutdown(Shutdown::Both);
}

fn expect_accept(rx: &mpsc::Receiver<ProviderOutcome>, job: u64) {
    let outcome = rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| panic!("no outcome for job {job} within 5s"));
    assert_eq!(outcome.job, job);
    match outcome.kind {
        OutcomeKind::Accepted { signature } => assert_eq!(signature, "okSig"),
        other => panic!("job {job} got {other:?}"),
    }
}

#[test]
fn failover_then_reconnect_keeps_serving() {
    let (port, handles) = spawn_harness();

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
        .sink(Arc::new(ChanSink(Mutex::new(tx))))
        .build()
    {
        Ok(s) => s,
        Err(SenderError::Unsupported(e)) => {
            eprintln!("transport unsupported on this kernel, skipping: {e}");
            return;
        }
        Err(e) => panic!("{e:?}"),
    };

    sender.trigger(Job { id: 1, ctx: 0 }).unwrap();
    expect_accept(&rx, 1);

    kill_active(&handles);
    std::thread::sleep(Duration::from_millis(20));

    sender.trigger(Job { id: 2, ctx: 0 }).unwrap();
    expect_accept(&rx, 2);

    std::thread::sleep(Duration::from_millis(300));

    kill_active(&handles);
    std::thread::sleep(Duration::from_millis(20));

    sender.trigger(Job { id: 3, ctx: 0 }).unwrap();
    expect_accept(&rx, 3);

    sender.shutdown();
}
