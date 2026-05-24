use std::sync::Arc;

use rustls::ClientConfig;

use crate::backend::raw_libc::{RawConn, RawLibc};
use crate::backend::{ByteIo, Wire};
use crate::error::TransportError;
use crate::protocol::http::engine::HttpEngine;
use crate::protocol::http::envelope::EnvelopeTemplate;
use crate::provider::ProviderConfig;

pub struct HttpConn {
    io: RawLibc,
    raw: RawConn,
    engine: HttpEngine,
}

impl HttpConn {
    pub fn connect(
        cfg: &ProviderConfig,
        tls: Arc<ClientConfig>,
        envelope: EnvelopeTemplate,
    ) -> Result<Self, TransportError> {
        let mut io = RawLibc::new();
        let addr = format!("{}:{}", cfg.endpoint.host, cfg.endpoint.port);
        let mut raw = io.connect(&addr).map_err(TransportError::Connect)?;
        let mut engine = HttpEngine::new(tls, &cfg.endpoint.server_name, envelope)?;

        let mut rx = [0u8; 4096];
        while engine.is_handshaking() {
            let out = engine.handshake_ciphertext();
            if !out.is_empty() {
                io.submit(&mut raw, Wire::Stream, &out)
                    .map_err(TransportError::Io)?;
            }
            match io.poll_recv(&mut raw, &mut rx) {
                Ok(0) => std::hint::spin_loop(),
                Ok(n) => {
                    engine.ingest(&rx[..n])?;
                }
                Err(e) => return Err(TransportError::Io(e)),
            };
        }

        Ok(Self { io, raw, engine })
    }

    pub fn send_tx(&mut self, tx: &[u8]) -> Result<(), TransportError> {
        let cipher = self.engine.encode(tx)?.to_vec();
        self.io
            .submit(&mut self.raw, Wire::Stream, &cipher)
            .map_err(TransportError::Io)
    }

    pub fn poll_response(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut rx = [0u8; 4096];
        match self.io.poll_recv(&mut self.raw, &mut rx) {
            Ok(0) => self.engine.ingest(&[]),
            Ok(n) => self.engine.ingest(&rx[..n]),
            Err(e) => Err(TransportError::Io(e)),
        }
    }
}
