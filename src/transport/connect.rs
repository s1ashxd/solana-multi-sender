use std::sync::Arc;

use rustls::ClientConfig;

use crate::conn::reconnect::ConnKind;
use crate::conn::{ConnState, DualConn, HttpConn, QuicConn};
use crate::error::SenderError;
use crate::protocol::http::spec_builder::EnvelopeSpecBuilder;
use crate::protocol::quic::QuicEngine;
use crate::provider::{HttpAuth, ProviderConfig, Protocol};

pub(super) fn connect_one<B, F>(
    providers: &[ProviderConfig],
    i: usize,
    tls: &Arc<ClientConfig>,
    make: &F,
) -> Result<ConnState<B>, SenderError>
where
    B: crate::backend::ByteIo,
    F: Fn(usize) -> Result<B, SenderError>,
{
    let p = &providers[i];
    match p.protocol {
        Protocol::Http => {
            let spec = build_envelope(p);
            let tpl = spec.compile().map_err(|e| SenderError::Connect {
                provider: i as u16,
                source: crate::error::TransportError::Envelope(e),
            })?;
            let io = make(i)?;
            let conn = HttpConn::connect_with(io, p, tls.clone(), tpl).map_err(|e| {
                SenderError::Connect {
                    provider: i as u16,
                    source: e,
                }
            })?;
            Ok(ConnState::Http(Box::new(conn)))
        }
        Protocol::Quic => {
            let quic_ep = p.quic_endpoint.as_ref().ok_or_else(|| SenderError::QuicConfig {
                provider: i as u16,
                reason: "missing quic_endpoint".into(),
            })?;
            let quic_profile = p.quic_profile.as_ref().ok_or_else(|| SenderError::QuicConfig {
                provider: i as u16,
                reason: "missing quic_profile".into(),
            })?;
            let quic_auth = p.quic_auth.as_ref().ok_or_else(|| SenderError::QuicConfig {
                provider: i as u16,
                reason: "missing quic_auth".into(),
            })?;
            let client_cfg =
                crate::protocol::quic_cert::build_quic_client_config(quic_profile, quic_auth)
                    .map_err(|e| SenderError::Connect {
                        provider: i as u16,
                        source: e,
                    })?;
            let engine = QuicEngine::connect(client_cfg, quic_ep.addr, &quic_ep.server_name)
                .map_err(|e| SenderError::Connect {
                    provider: i as u16,
                    source: e,
                })?;
            Ok(ConnState::Quic(Box::new(QuicConn::new(engine))))
        }
    }
}

pub(super) fn connect_dual_all<B, F>(
    providers: &[ProviderConfig],
    tls: &Arc<ClientConfig>,
    make: &F,
) -> Result<Vec<DualConn<ConnState<B>>>, SenderError>
where
    B: crate::backend::ByteIo,
    F: Fn(usize) -> Result<B, SenderError>,
{
    let mut duals = Vec::with_capacity(providers.len());
    for i in 0..providers.len() {
        let active = connect_one::<B, F>(providers, i, tls, make)?;
        let standby = connect_one::<B, F>(providers, i, tls, make)?;
        duals.push(DualConn::new(active, standby));
    }
    Ok(duals)
}

pub(super) struct EngineReconnectBuilder<B, F>
where
    B: crate::backend::ByteIo + Send + 'static,
    F: Fn(usize) -> Result<B, SenderError> + Send + 'static,
{
    pub(super) providers: Vec<ProviderConfig>,
    pub(super) tls: Arc<ClientConfig>,
    pub(super) make: F,
    pub(super) _marker: std::marker::PhantomData<B>,
}

impl<B, F> crate::conn::reconnect::ReconnectBuilder for EngineReconnectBuilder<B, F>
where
    B: crate::backend::ByteIo + Send + 'static,
    F: Fn(usize) -> Result<B, SenderError> + Send + 'static,
    ConnState<B>: Send + 'static,
{
    fn build(
        &self,
        provider_idx: usize,
        _kind: ConnKind,
    ) -> Result<crate::conn::reconnect::ConnBox, crate::conn::reconnect::ReconnectError> {
        let conn = connect_one::<B, _>(&self.providers, provider_idx, &self.tls, &|i| {
            (self.make)(i)
        })
        .map_err(|e| crate::conn::reconnect::ReconnectError::Build(format!("{e:?}")))?;
        Ok(Box::new(conn))
    }
}

#[cfg(feature = "libtpa")]
pub(super) struct TpaReconnectBuilder;

#[cfg(feature = "libtpa")]
impl crate::conn::reconnect::ReconnectBuilder for TpaReconnectBuilder {
    fn build(
        &self,
        _provider_idx: usize,
        _kind: ConnKind,
    ) -> Result<crate::conn::reconnect::ConnBox, crate::conn::reconnect::ReconnectError> {
        Err(crate::conn::reconnect::ReconnectError::NotSupported)
    }
}

fn build_envelope(p: &ProviderConfig) -> crate::protocol::http::envelope::EnvelopeSpec {
    use crate::protocol::http::spec_builder::AuthPlacement;
    let auth = match &p.auth {
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
        &p.endpoint.path,
        &p.endpoint.server_name,
        p.max_body,
    )
    .auth_placement(auth);
    if !p.body_template.is_empty() {
        b = b.body_template(&p.body_template);
    }
    b.build()
}

pub fn real_roots() -> rustls::RootCertStore {
    let mut store = rustls::RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    store
}

pub fn real_roots_client_config() -> std::sync::Arc<rustls::ClientConfig> {
    std::sync::Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(real_roots())
            .with_no_client_auth(),
    )
}
