#[cfg(feature = "registry")]
mod registry_tests {
    use tx_sender::protocol::http::spec_builder::{AuthPlacement, EnvelopeSpecBuilder};
    use tx_sender::provider::{registry, HttpAuth, Protocol, ProviderConfig};

    fn frame_of(cfg: &ProviderConfig) -> String {
        let auth = match &cfg.auth {
            HttpAuth::None => AuthPlacement::None,
            HttpAuth::Header { name, value } => AuthPlacement::Header {
                name: name.clone(),
                value: value.clone(),
            },
            HttpAuth::UrlParam { key, value } => AuthPlacement::UrlParam {
                key,
                value: value.clone(),
            },
            HttpAuth::UrlPath { token } => AuthPlacement::UrlPath {
                token: token.clone(),
            },
        };
        let mut b = EnvelopeSpecBuilder::new(
            "POST",
            &cfg.endpoint.path,
            &cfg.endpoint.server_name,
            cfg.max_body,
        )
        .auth_placement(auth);
        if !cfg.body_template.is_empty() {
            b = b.body_template(&cfg.body_template);
        }
        let mut tpl = b.build().compile().unwrap();
        String::from_utf8(tpl.splice(&[0x01u8; 8]).unwrap().to_vec()).unwrap()
    }

    fn assert_contains(cfg: &ProviderConfig, needle: &str) {
        let f = frame_of(cfg);
        assert!(
            f.contains(needle),
            "frame missing {needle:?}:\n{}",
            &f[..f.find("\r\n\r\n").unwrap_or(f.len())]
        );
    }

    #[test]
    fn jito_transaction_shape() {
        let c = registry::jito_transaction("tok");
        assert_eq!(c.protocol, Protocol::Http);
        assert_contains(&c, "x-jito-auth: tok");
        assert_contains(&c, "\"sendTransaction\"");
        assert_contains(&c, "/api/v1/transactions?uuid=tok");
    }

    #[test]
    fn jito_bundle_shape() {
        let c = registry::jito_bundle("tok");
        assert_contains(&c, "x-jito-auth: tok");
        assert_contains(&c, "\"sendBundle\"");
        assert_contains(&c, "/api/v1/bundles?uuid=tok");
    }

    #[test]
    fn nextblock_shape() {
        let c = registry::nextblock("auth-tok");
        assert_contains(&c, "Authorization: auth-tok");
        assert_contains(&c, "/api/v2/submit");
        assert_contains(&c, "\"transaction\"");
    }

    #[test]
    fn zeroslot_shape() {
        let c = registry::zeroslot("key456");
        assert_contains(&c, "?api-key=key456");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn temporal_shape() {
        let c = registry::temporal("cval");
        assert_contains(&c, "?c=cval");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn bloxroute_shape() {
        let c = registry::bloxroute("blx");
        assert_contains(&c, "Authorization: blx");
        assert_contains(&c, "\"useStakedRPCs\"");
    }

    #[test]
    fn node1_shape() {
        let c = registry::node1("n1");
        assert_contains(&c, "api-key: n1");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn flashblock_shape() {
        let c = registry::flashblock("fb");
        assert_contains(&c, "Authorization: fb");
        assert_contains(&c, "\"transactions\"");
    }

    #[test]
    fn blockrazor_shape() {
        let c = registry::blockrazor("br");
        assert_contains(&c, "apikey: br");
        assert_contains(&c, "\"mode\":\"fast\"");
    }

    #[test]
    fn astralane_shape() {
        let c = registry::astralane("ast");
        assert_contains(&c, "api_key: ast");
        assert_contains(&c, "\"mevProtect\"");
    }

    #[test]
    fn stellium_shape() {
        let c = registry::stellium("stel");
        assert_contains(&c, "/stel");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn lightspeed_shape() {
        let c = registry::lightspeed("ls");
        assert_contains(&c, "?api_key=ls");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn helius_shape() {
        let c = registry::helius("hel");
        assert_contains(&c, "?api-key=hel");
        assert_contains(&c, "\"sendTransaction\"");
    }

    #[test]
    fn soyas_is_quic() {
        let c = registry::soyas("127.0.0.1:443".parse().unwrap(), [7u8; 64]);
        assert_eq!(c.protocol, Protocol::Quic);
        assert!(c.quic_auth.is_some());
        assert!(c.quic_endpoint.is_some());
        let p = c.quic_profile.unwrap();
        assert_eq!(p.alpn, b"solana-tpu");
        assert_eq!(p.max_streams_uni, 1_000_000);
    }

    #[test]
    fn speedlanding_is_quic() {
        let c = registry::speedlanding("127.0.0.1:443".parse().unwrap(), [7u8; 64]);
        assert_eq!(c.protocol, Protocol::Quic);
        assert_eq!(c.quic_profile.unwrap().alpn, b"solana-tpu");
    }

    #[test]
    fn falcon_quic_is_quic() {
        let c = registry::falcon_quic("127.0.0.1:5000".parse().unwrap(), "uuid-key".into());
        assert_eq!(c.protocol, Protocol::Quic);
        let p = c.quic_profile.unwrap();
        assert_eq!(p.alpn, b"falcon-tx");
        assert_eq!(p.max_streams_uni, 64);
    }
}

#[cfg(feature = "registry")]
mod smoke {
    use std::net::TcpListener;
    use std::sync::Arc;

    use rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
    use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

    use tx_sender::job::{Job, ProviderId, MAX_TX_LEN};
    use tx_sender::provider::registry;
    use tx_sender::sink::{ProviderOutcome, ResultSink};
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

    struct DropSink;

    impl ResultSink for DropSink {
        fn on_result(&self, _o: ProviderOutcome) {}
    }

    fn serve_connection(mut sock: std::net::TcpStream, server_cfg: Arc<rustls::ServerConfig>) {
        use std::io::{Read, Write};

        let mut conn = rustls::ServerConnection::new(server_cfg).unwrap();
        let mut buf = [0u8; 8192];

        while conn.is_handshaking() {
            while conn.wants_write() {
                let mut out = Vec::new();
                conn.write_tls(&mut out).unwrap();
                if sock.write_all(&out).is_err() {
                    return;
                }
            }
            if conn.is_handshaking() {
                let n = sock.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    return;
                }
                let mut slice: &[u8] = &buf[..n];
                if conn.read_tls(&mut slice).is_err() || conn.process_new_packets().is_err() {
                    return;
                }
            }
        }

        while conn.wants_write() {
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            if sock.write_all(&out).is_err() {
                return;
            }
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
            if n == 0 {
                return;
            }
            let mut slice: &[u8] = &buf[..n];
            if conn.read_tls(&mut slice).is_err() || conn.process_new_packets().is_err() {
                return;
            }
            drain_reader(&mut conn, &mut request_buf);
        }

        let body = br#"{"jsonrpc":"2.0","result":"smoke-sig","id":1}"#;
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
        let _ = conn.writer().write_all(resp.as_bytes());
        let _ = conn.writer().write_all(body);

        while conn.wants_write() {
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            if sock.write_all(&out).is_err() {
                return;
            }
        }
        let _ = sock.flush();
    }

    fn spawn_tls_server() -> u16 {
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
    fn builds_sender_from_jito_preset() {
        let port = spawn_tls_server();

        let client_cfg = Arc::new(
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAll))
                .with_no_client_auth(),
        );

        let mut cfg = registry::jito_transaction("test-tok");
        cfg.endpoint.host = "127.0.0.1".into();
        cfg.endpoint.port = port;
        cfg.endpoint.server_name = "localhost".into();

        let sender = match Sender::builder()
            .transport(Sequential::raw_libc())
            .tls(client_cfg)
            .provider(cfg)
            .source(Arc::new(FixedTx))
            .sink(Arc::new(DropSink))
            .build()
        {
            Ok(s) => s,
            Err(tx_sender::error::SenderError::Unsupported(e)) => {
                eprintln!("transport unsupported on this kernel, skipping: {e}");
                return;
            }
            Err(e) => panic!("{e:?}"),
        };

        sender.trigger(Job { id: 1, ctx: 0 }).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        sender.shutdown();
    }
}
