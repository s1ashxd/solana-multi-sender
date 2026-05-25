use std::net::{SocketAddr, UdpSocket};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig, Event,
};

use crate::backend::raw_libc::udp_sendto;
use crate::error::TransportError;

pub struct QuicEngine {
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Option<Connection>,
    pub(crate) conn_handle: ConnectionHandle,
    pub(crate) fd: RawFd,
    pub(crate) _sock: UdpSocket,
    pub(crate) remote: SocketAddr,
    pub(crate) send_buf: Vec<u8>,
    pub(crate) recv_buf: Vec<u8>,
    pub(crate) resp_buf: Vec<u8>,
    pub(crate) next_timeout: Option<Instant>,
}

impl QuicEngine {
    pub fn connect(
        client_cfg: ClientConfig,
        endpoint_addr: SocketAddr,
        server_name: &str,
    ) -> Result<Self, TransportError> {
        let sock = UdpSocket::bind("0.0.0.0:0")
            .map_err(|e| TransportError::Quic(format!("bind: {e}")))?;
        sock.set_nonblocking(true)
            .map_err(|e| TransportError::Quic(format!("set_nonblocking: {e}")))?;
        let fd = sock.as_raw_fd();

        let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let (conn_handle, mut connection) = endpoint
            .connect(Instant::now(), client_cfg, endpoint_addr, server_name)
            .map_err(|e| TransportError::Quic(format!("connect: {e}")))?;

        quic_handshake(
            fd,
            &mut endpoint,
            &mut connection,
            conn_handle,
            endpoint_addr,
        )?;

        let next_timeout = connection.poll_timeout();

        Ok(Self {
            endpoint,
            connection: Some(connection),
            conn_handle,
            fd,
            _sock: sock,
            remote: endpoint_addr,
            send_buf: Vec::with_capacity(1500),
            recv_buf: vec![0u8; 65535],
            resp_buf: Vec::with_capacity(1500),
            next_timeout,
        })
    }

    pub fn send_tx(&mut self, tx: &[u8], now: Instant) -> Result<(), TransportError> {
        let conn = self
            .connection
            .as_mut()
            .filter(|c| !c.is_closed() && !c.is_drained())
            .ok_or_else(|| TransportError::Quic("connection closed".into()))?;

        let stream_id = conn
            .streams()
            .open(Dir::Uni)
            .ok_or_else(|| TransportError::Quic("stream limit reached".into()))?;

        let mut bytes = BytesMut::with_capacity(tx.len());
        bytes.extend_from_slice(tx);
        let bytes = bytes.freeze();
        let tx_len = bytes.len();

        let written = conn
            .send_stream(stream_id)
            .write_chunks(&mut [bytes])
            .map_err(|e| TransportError::Quic(format!("write: {e:?}")))?;
        if written.bytes < tx_len {
            return Err(TransportError::Quic("partial write".into()));
        }
        let _ = conn.send_stream(stream_id).finish();

        self.drain_transmits(now);
        self.next_timeout = self.connection.as_mut().and_then(|c| c.poll_timeout());
        Ok(())
    }

    pub fn drive(&mut self, now: Instant) {
        self.recv_once(now);
        self.drive_timeout(now);
        self.drain_transmits(now);
        self.next_timeout = self.connection.as_mut().and_then(|c| c.poll_timeout());
    }

    pub fn is_closed(&self) -> bool {
        self.connection
            .as_ref()
            .map(|c| c.is_closed() || c.is_drained())
            .unwrap_or(true)
    }

    pub fn next_timeout(&self) -> Option<Instant> {
        self.next_timeout
    }

    fn drain_transmits(&mut self, now: Instant) {
        let Some(conn) = self.connection.as_mut() else {
            return;
        };
        self.send_buf.clear();
        while let Some(transmit) = conn.poll_transmit(now, 8, &mut self.send_buf) {
            let _ = udp_sendto(self.fd, self.remote, &self.send_buf[..transmit.size]);
            self.send_buf.clear();
        }
    }

    fn recv_once(&mut self, now: Instant) {
        let n = recv_dgram(self.fd, &mut self.recv_buf);
        if n == 0 {
            return;
        }
        let data = BytesMut::from(&self.recv_buf[..n]);
        self.resp_buf.clear();
        if let Some(event) = self
            .endpoint
            .handle(now, self.remote, None, None, data, &mut self.resp_buf)
        {
            match event {
                DatagramEvent::ConnectionEvent(_h, ev) => {
                    if let Some(conn) = self.connection.as_mut() {
                        conn.handle_event(ev);
                    }
                }
                DatagramEvent::Response(transmit) => {
                    let _ = udp_sendto(self.fd, self.remote, &self.resp_buf[..transmit.size]);
                }
                DatagramEvent::NewConnection(_) => {}
            }
        }

        let Some(conn) = self.connection.as_mut() else {
            return;
        };
        while let Some(ep_ev) = conn.poll_endpoint_events() {
            if let Some(conn_ev) = self.endpoint.handle_event(self.conn_handle, ep_ev) {
                conn.handle_event(conn_ev);
            }
        }
    }

    fn drive_timeout(&mut self, now: Instant) {
        let Some(deadline) = self.next_timeout else {
            return;
        };
        if now < deadline {
            return;
        }
        if let Some(conn) = self.connection.as_mut() {
            conn.handle_timeout(now);
        }
        self.drain_transmits(now);
    }
}

fn recv_dgram(fd: RawFd, buf: &mut [u8]) -> usize {
    let n = unsafe {
        libc::recvfrom(
            fd,
            buf.as_mut_ptr().cast(),
            buf.len(),
            libc::MSG_DONTWAIT,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if n > 0 {
        n as usize
    } else {
        0
    }
}

fn quic_handshake(
    fd: RawFd,
    endpoint: &mut Endpoint,
    connection: &mut Connection,
    handle: ConnectionHandle,
    addr: SocketAddr,
) -> Result<(), TransportError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut send_buf: Vec<u8> = Vec::with_capacity(1500);
    let mut recv_buf = vec![0u8; 65535];
    let mut resp_buf: Vec<u8> = Vec::with_capacity(1500);

    loop {
        let now = Instant::now();
        if now > deadline {
            return Err(TransportError::Quic("handshake timeout (5s)".into()));
        }

        send_buf.clear();
        while let Some(transmit) = connection.poll_transmit(now, 1, &mut send_buf) {
            let _ = udp_sendto(fd, addr, &send_buf[..transmit.size]);
            send_buf.clear();
        }

        let n = recv_dgram(fd, &mut recv_buf);
        if n > 0 {
            let data = BytesMut::from(&recv_buf[..n]);
            resp_buf.clear();
            if let Some(event) = endpoint.handle(now, addr, None, None, data, &mut resp_buf) {
                match event {
                    DatagramEvent::ConnectionEvent(_h, ev) => connection.handle_event(ev),
                    DatagramEvent::Response(transmit) => {
                        let _ = udp_sendto(fd, addr, &resp_buf[..transmit.size]);
                    }
                    DatagramEvent::NewConnection(_) => {}
                }
            }
        }

        if let Some(t) = connection.poll_timeout() {
            if now >= t {
                connection.handle_timeout(now);
            }
        }

        while let Some(ep_ev) = connection.poll_endpoint_events() {
            if let Some(cev) = endpoint.handle_event(handle, ep_ev) {
                connection.handle_event(cev);
            }
        }

        while let Some(event) = connection.poll() {
            match event {
                Event::Connected => {
                    drain_handshake(fd, connection, addr, &mut send_buf);
                    return Ok(());
                }
                Event::ConnectionLost { reason } => {
                    return Err(TransportError::Quic(format!("handshake rejected: {reason}")));
                }
                _ => {}
            }
        }

        if !connection.is_handshaking() {
            drain_handshake(fd, connection, addr, &mut send_buf);
            return Ok(());
        }

        std::hint::spin_loop();
    }
}

fn drain_handshake(
    fd: RawFd,
    connection: &mut Connection,
    addr: SocketAddr,
    send_buf: &mut Vec<u8>,
) {
    let now = Instant::now();
    send_buf.clear();
    while let Some(transmit) = connection.poll_transmit(now, 1, send_buf) {
        let _ = udp_sendto(fd, addr, &send_buf[..transmit.size]);
        send_buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn make_unconnected() -> QuicEngine {
        let sock = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        sock.set_nonblocking(true).unwrap();
        let fd = std::os::fd::AsRawFd::as_raw_fd(&sock);
        let remote: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let ep_cfg = quinn_proto::EndpointConfig::default();
        let endpoint = quinn_proto::Endpoint::new(Arc::new(ep_cfg), None, true, None);
        QuicEngine {
            endpoint,
            connection: None,
            conn_handle: quinn_proto::ConnectionHandle(0),
            fd,
            _sock: sock,
            remote,
            send_buf: Vec::with_capacity(1500),
            recv_buf: vec![0u8; 65535],
            resp_buf: Vec::with_capacity(1500),
            next_timeout: None,
        }
    }

    #[test]
    fn send_tx_without_connection_returns_error() {
        let mut engine = make_unconnected();
        let tx = [0u8; 64];
        let result = engine.send_tx(&tx, Instant::now());
        assert!(result.is_err());
    }

    #[test]
    fn is_closed_without_connection_returns_true() {
        let engine = make_unconnected();
        assert!(engine.is_closed());
    }
}
