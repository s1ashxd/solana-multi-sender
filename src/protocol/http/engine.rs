use std::io::{Read as _, Write as _};
use std::sync::Arc;

use rustls::{ClientConfig, ClientConnection};
use rustls_pki_types::ServerName;

use crate::error::TransportError;
use crate::protocol::http::envelope::EnvelopeTemplate;

const MAX_RESPONSE: usize = 64 * 1024;

pub struct HttpEngine {
    tls: ClientConnection,
    envelope: EnvelopeTemplate,
    rx_plain: Vec<u8>,
    ciphertext: Vec<u8>,
}

pub(crate) fn body_if_complete(acc: &[u8]) -> Option<&[u8]> {
    let header_end = acc.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let headers = std::str::from_utf8(&acc[..header_end]).ok()?;
    let prefix = b"content-length:";
    let cl = headers.lines().find_map(|l| {
        let bytes = l.as_bytes();
        if bytes.len() >= prefix.len() && bytes[..prefix.len()].eq_ignore_ascii_case(prefix) {
            Some(l[prefix.len()..].trim())
        } else {
            None
        }
    })?;
    let len: usize = cl.parse().ok()?;
    if acc.len() >= header_end + len {
        Some(&acc[header_end..header_end + len])
    } else {
        None
    }
}

impl HttpEngine {
    pub fn new(
        config: Arc<ClientConfig>,
        server_name: &str,
        envelope: EnvelopeTemplate,
    ) -> Result<Self, TransportError> {
        let name = ServerName::try_from(server_name.to_string())
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let tls =
            ClientConnection::new(config, name).map_err(|e| TransportError::Tls(e.to_string()))?;
        Ok(Self {
            tls,
            envelope,
            rx_plain: Vec::with_capacity(1024),
            ciphertext: Vec::with_capacity(4096),
        })
    }

    pub fn handshake_ciphertext(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        while self.tls.wants_write() {
            let _ = self.tls.write_tls(&mut out);
        }
        out
    }

    pub fn is_handshaking(&self) -> bool {
        self.tls.is_handshaking()
    }

    pub fn encode(&mut self, tx: &[u8]) -> Result<&[u8], TransportError> {
        let frame = self.envelope.splice(tx).map_err(TransportError::Envelope)?;
        self.tls
            .writer()
            .write_all(frame)
            .map_err(TransportError::Io)?;
        self.ciphertext.clear();
        while self.tls.wants_write() {
            self.tls
                .write_tls(&mut self.ciphertext)
                .map_err(TransportError::Io)?;
        }
        Ok(&self.ciphertext)
    }

    pub fn ingest(&mut self, mut cipher: &[u8]) -> Result<Option<Vec<u8>>, TransportError> {
        if !cipher.is_empty() {
            while !cipher.is_empty() {
                let n = self.tls.read_tls(&mut cipher).map_err(TransportError::Io)?;
                if n == 0 {
                    break;
                }
                self.tls
                    .process_new_packets()
                    .map_err(|e| TransportError::Tls(e.to_string()))?;
            }
            let mut tmp = [0u8; 2048];
            loop {
                match self.tls.reader().read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => self.rx_plain.extend_from_slice(&tmp[..n]),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(TransportError::Io(e)),
                }
            }
        }
        if let Some(body) = body_if_complete(&self.rx_plain) {
            let owned = body.to_vec();
            self.rx_plain.clear();
            return Ok(Some(owned));
        }
        if self.rx_plain.len() > MAX_RESPONSE {
            return Err(TransportError::Tls("response exceeds maximum size".into()));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_json_body_after_headers() {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        let mut acc = Vec::new();
        acc.extend_from_slice(resp);
        let body = body_if_complete(&acc).unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn incomplete_response_returns_none() {
        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhel";
        assert!(body_if_complete(resp).is_none());
    }

    #[test]
    fn matches_content_length_header_case_insensitively() {
        let resp = b"HTTP/1.1 200 OK\r\nCONTENT-LENGTH: 5\r\n\r\nhello";
        let body = body_if_complete(resp).unwrap();
        assert_eq!(body, b"hello");
    }
}
