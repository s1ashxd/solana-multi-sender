use std::sync::Arc;

use quinn_proto::crypto::rustls::QuicClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::TransportError;
use crate::provider::{QuicAuth, QuicProfile};

const ED25519_PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

fn finish_client_config(
    mut rustls_cfg: rustls::ClientConfig,
    profile: &QuicProfile,
) -> Result<quinn_proto::ClientConfig, TransportError> {
    rustls_cfg.alpn_protocols = vec![profile.alpn.to_vec()];
    let quic_cfg =
        QuicClientConfig::try_from(rustls_cfg).map_err(|e| TransportError::Quic(e.to_string()))?;
    let mut cfg = quinn_proto::ClientConfig::new(Arc::new(quic_cfg));
    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
        quinn_proto::VarInt::from_u64(profile.idle_timeout_ms)
            .map_err(|e| TransportError::Quic(e.to_string()))?
            .into(),
    ));
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(
        profile.keepalive_interval_secs,
    )));
    transport.max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(profile.max_streams_uni));
    cfg.transport_config(Arc::new(transport));
    Ok(cfg)
}

fn build_ed25519_cert(
    keypair: &[u8; 64],
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TransportError> {
    let mut pkcs8_v1 = Vec::with_capacity(48);
    pkcs8_v1.extend_from_slice(&ED25519_PKCS8_PREFIX);
    pkcs8_v1.extend_from_slice(&keypair[..32]);
    let pkcs8_der = PrivatePkcs8KeyDer::from(pkcs8_v1.clone());
    let key_pair = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(&pkcs8_der, &rcgen::PKCS_ED25519)
        .map_err(|e| TransportError::Quic(format!("keypair: {e}")))?;
    let params = rcgen::CertificateParams::default();
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| TransportError::Quic(format!("self-sign: {e}")))?;
    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8_v1));
    Ok((vec![cert_der], key_der))
}

fn build_falcon_cert(
    api_key: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TransportError> {
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| TransportError::Quic(format!("keygen: {e}")))?;
    let mut dn = rcgen::DistinguishedName::new();
    dn.push(rcgen::DnType::CommonName, api_key);
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = dn;
    params.not_before = rcgen::date_time_ymd(1970, 1, 1);
    params.not_after = rcgen::date_time_ymd(4096, 1, 1);
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| TransportError::Quic(format!("self-sign: {e}")))?;
    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));
    Ok((vec![cert_der], key_der))
}

pub fn build_quic_client_config(
    profile: &QuicProfile,
    auth: &QuicAuth,
) -> Result<quinn_proto::ClientConfig, TransportError> {
    match auth {
        QuicAuth::NoCert => {
            let rustls_cfg = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();
            finish_client_config(rustls_cfg, profile)
        }
        QuicAuth::Ed25519Keypair(kp) => {
            let (certs, key) = build_ed25519_cert(kp)?;
            let rustls_cfg = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_client_auth_cert(certs, key)
                .map_err(|e| TransportError::Quic(format!("client cert: {e}")))?;
            finish_client_config(rustls_cfg, profile)
        }
        QuicAuth::FalconApiKey(api_key) => {
            let (certs, key) = build_falcon_cert(api_key)?;
            let rustls_cfg = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_client_auth_cert(certs, key)
                .map_err(|e| TransportError::Quic(format!("client cert: {e}")))?;
            finish_client_config(rustls_cfg, profile)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{QuicAuth, QuicProfile};

    #[test]
    fn build_ed25519_config_succeeds() {
        let keypair = [42u8; 64];
        let profile = QuicProfile::soyas();
        let cfg = build_quic_client_config(&profile, &QuicAuth::Ed25519Keypair(keypair));
        assert!(cfg.is_ok());
    }

    #[test]
    fn build_falcon_config_succeeds() {
        let profile = QuicProfile::falcon();
        let cfg = build_quic_client_config(&profile, &QuicAuth::FalconApiKey("test-uuid".into()));
        assert!(cfg.is_ok());
    }

    #[test]
    fn build_no_cert_config_succeeds() {
        let profile = QuicProfile::soyas();
        let cfg = build_quic_client_config(&profile, &QuicAuth::NoCert);
        assert!(cfg.is_ok());
    }
}
