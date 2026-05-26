use tx_sender::error::EnvelopeError;
use tx_sender::protocol::http::envelope::EnvelopeSpec;
use tx_sender::protocol::http::spec_builder::{AuthPlacement, EnvelopeSpecBuilder};

fn assert_frame_consistent(frame: &[u8], tx: &[u8]) {
    let text = String::from_utf8_lossy(frame);
    let b64 = base64_simd::STANDARD.encode_to_string(tx);
    assert!(text.contains(&b64));
    let body_start = text.find("\r\n\r\n").unwrap() + 4;
    let body_len = frame.len() - body_start;
    assert!(
        text.contains(&format!("Content-Length: {body_len}"))
            || text.contains(&format!("Content-Length:{body_len}"))
    );
}

#[test]
fn splice_base64_body_and_patches_content_length() {
    let spec: EnvelopeSpec = EnvelopeSpecBuilder::send_transaction("/", "node.example", 2000)
        .header("authorization", "Bearer t")
        .build();
    let mut tpl = spec.compile().unwrap();

    let tx = [0xABu8; 60];
    let frame = tpl.splice(&tx).unwrap();
    assert_frame_consistent(frame, &tx);
}

#[test]
fn splice_is_repeatable() {
    let spec: EnvelopeSpec = EnvelopeSpecBuilder::send_transaction("/", "node.example", 2000)
        .header("authorization", "Bearer t")
        .build();
    let mut tpl = spec.compile().unwrap();

    let tx_a = [0x11u8; 32];
    let frame_a = tpl.splice(&tx_a).unwrap().to_vec();
    assert_frame_consistent(&frame_a, &tx_a);

    let tx_b = [0x22u8; 97];
    let frame_b = tpl.splice(&tx_b).unwrap().to_vec();
    assert_frame_consistent(&frame_b, &tx_b);

    assert_ne!(frame_a, frame_b);
}

#[test]
fn splice_rejects_oversized_tx() {
    let spec: EnvelopeSpec = EnvelopeSpecBuilder::send_transaction("/", "node.example", 16).build();
    let mut tpl = spec.compile().unwrap();

    let tx = [0x33u8; 64];
    let err = tpl.splice(&tx).unwrap_err();
    assert!(matches!(err, EnvelopeError::BodyTooLarge { .. }));
}

#[test]
fn builder_url_param_auth_embeds_token_in_path() {
    let mut tpl = EnvelopeSpecBuilder::new("POST", "/api", "rpc.example.com", 2000)
        .auth_placement(AuthPlacement::UrlParam {
            key: "api-key",
            value: "tok123".into(),
        })
        .build()
        .compile()
        .unwrap();
    let frame = String::from_utf8(tpl.splice(&[0xABu8; 4]).unwrap().to_vec()).unwrap();
    assert!(frame.starts_with("POST /api?api-key=tok123 HTTP/1.1\r\n"));
}

#[test]
fn builder_url_path_auth_appends_token_to_path() {
    let mut tpl = EnvelopeSpecBuilder::new("POST", "/route", "rpc.example.com", 2000)
        .auth_placement(AuthPlacement::UrlPath {
            token: "mytoken".into(),
        })
        .build()
        .compile()
        .unwrap();
    let frame = String::from_utf8(tpl.splice(&[0xABu8; 4]).unwrap().to_vec()).unwrap();
    assert!(frame.starts_with("POST /route/mytoken HTTP/1.1\r\n"));
}

#[test]
fn builder_header_auth_writes_header_line() {
    let mut tpl = EnvelopeSpecBuilder::new("POST", "/", "rpc.example.com", 2000)
        .auth_placement(AuthPlacement::Header {
            name: "x-api-key".into(),
            value: "secret".into(),
        })
        .build()
        .compile()
        .unwrap();
    let frame = String::from_utf8(tpl.splice(&[0xABu8; 4]).unwrap().to_vec()).unwrap();
    assert!(frame.contains("x-api-key: secret\r\n"));
}

#[test]
fn builder_custom_body_template() {
    let mut tpl = EnvelopeSpecBuilder::new("POST", "/bundles", "rpc.example.com", 2000)
        .body_template(
            r#"{"jsonrpc":"2.0","id":1,"method":"sendBundle","params":[["<<BODY>>"],{"encoding":"base64"}]}"#,
        )
        .build()
        .compile()
        .unwrap();
    let frame = String::from_utf8(tpl.splice(&[0x01u8; 4]).unwrap().to_vec()).unwrap();
    let body_start = frame.find("\r\n\r\n").unwrap() + 4;
    assert!(frame[body_start..].contains("\"sendBundle\""));
}
