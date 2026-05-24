use tx_sender::error::EnvelopeError;
use tx_sender::protocol::http::envelope::EnvelopeSpec;
use tx_sender::protocol::http::spec_builder::EnvelopeSpecBuilder;

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
