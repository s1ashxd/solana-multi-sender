pub mod reconnect;

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::ClientConfig;

use crate::backend::{ByteIo, Wire};
use crate::error::TransportError;
use crate::job::{Job, JobId};
use crate::protocol::http::engine::HttpEngine;
use crate::protocol::http::envelope::EnvelopeTemplate;
use crate::protocol::quic::QuicEngine;
use crate::provider::ProviderConfig;

pub struct HttpConn<B: ByteIo> {
    io: B,
    raw: B::Conn,
    engine: HttpEngine,
}

impl<B: ByteIo> HttpConn<B> {
    pub fn connect_with(
        mut io: B,
        cfg: &ProviderConfig,
        tls: Arc<ClientConfig>,
        envelope: EnvelopeTemplate,
    ) -> Result<Self, TransportError> {
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
            io.drive();
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
        let cipher = self.engine.encode(tx)?;
        self.io
            .submit(&mut self.raw, Wire::Stream, cipher)
            .map_err(TransportError::Io)
    }

    pub fn drive(&mut self) {
        self.io.drive();
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

pub const QUIC_RESPONSE_WINDOW_MS: u64 = 500;

pub struct QuicConn {
    pub engine: QuicEngine,
    pub inflight: Option<(JobId, u64)>,
    pub sent_at: Option<Instant>,
}

impl QuicConn {
    pub fn new(engine: QuicEngine) -> Self {
        Self {
            engine,
            inflight: None,
            sent_at: None,
        }
    }

    pub fn send_tx(&mut self, job: Job, sent_tsc: u64, tx: &[u8]) -> Result<(), TransportError> {
        let now = Instant::now();
        self.engine.send_tx(tx, now)?;
        self.inflight = Some((job.id, sent_tsc));
        self.sent_at = Some(now);
        Ok(())
    }

    pub fn poll_noresp_window(&mut self) -> Option<(JobId, u64)> {
        if let (Some((job_id, sent_tsc)), Some(sent_at)) = (self.inflight, self.sent_at) {
            if sent_at.elapsed() > Duration::from_millis(QUIC_RESPONSE_WINDOW_MS) {
                self.inflight = None;
                self.sent_at = None;
                return Some((job_id, sent_tsc));
            }
        }
        None
    }

    pub fn drive(&mut self) {
        self.engine.drive(Instant::now());
    }

    pub fn is_dead(&self) -> bool {
        self.engine.is_closed()
    }
}

pub enum ConnState<B: ByteIo> {
    Http(Box<HttpConn<B>>),
    Quic(Box<QuicConn>),
}

impl<B: ByteIo> ConnState<B> {
    pub fn drive(&mut self) {
        match self {
            ConnState::Http(h) => h.drive(),
            ConnState::Quic(q) => q.drive(),
        }
    }
}

pub struct ConnSlot<C> {
    pub conn: C,
    pub healthy: bool,
}

pub struct DualConn<C> {
    pub active: ConnSlot<C>,
    pub standby: ConnSlot<C>,
}

impl<C> DualConn<C> {
    pub fn new(active: C, standby: C) -> Self {
        Self {
            active: ConnSlot { conn: active, healthy: true },
            standby: ConnSlot { conn: standby, healthy: true },
        }
    }

    pub fn swap(&mut self) {
        std::mem::swap(&mut self.active, &mut self.standby);
        self.active.healthy = true;
    }

    pub fn replace_standby(&mut self, conn: C) {
        self.standby.conn = conn;
        self.standby.healthy = true;
    }
}

pub fn http_conn_failed_io(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        UnexpectedEof | BrokenPipe | ConnectionReset | ConnectionAborted
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::raw_libc::RawLibc;

    #[test]
    fn conn_state_variant_names() {
        fn is_http(c: &ConnState<RawLibc>) -> bool {
            matches!(c, ConnState::Http(_))
        }
        fn is_quic(c: &ConnState<RawLibc>) -> bool {
            matches!(c, ConnState::Quic(_))
        }
        let _ = (
            is_http as fn(&ConnState<RawLibc>) -> bool,
            is_quic as fn(&ConnState<RawLibc>) -> bool,
        );
    }

    struct Dummy(u8);

    #[test]
    fn swap_makes_standby_active() {
        let mut dc: DualConn<Dummy> = DualConn::new(Dummy(1), Dummy(2));
        dc.swap();
        assert_eq!(dc.active.conn.0, 2);
        assert_eq!(dc.standby.conn.0, 1);
    }

    #[test]
    fn replace_standby_resets_health() {
        let mut dc: DualConn<Dummy> = DualConn::new(Dummy(1), Dummy(2));
        dc.standby.healthy = false;
        dc.replace_standby(Dummy(3));
        assert!(dc.standby.healthy);
        assert_eq!(dc.standby.conn.0, 3);
    }

    #[test]
    fn http_failed_true_on_eof_false_on_wouldblock() {
        assert!(http_conn_failed_io(&std::io::Error::from(std::io::ErrorKind::UnexpectedEof)));
        assert!(!http_conn_failed_io(&std::io::Error::from(std::io::ErrorKind::WouldBlock)));
    }
}
