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
