use std::io;
use std::net::TcpStream;
use std::os::fd::AsRawFd;

use crate::backend::{ByteIo, Wire};

pub struct RawConn {
    stream: TcpStream,
}

pub struct RawLibc;

impl RawLibc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for RawLibc {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteIo for RawLibc {
    type Conn = RawConn;

    fn connect(&mut self, endpoint: &str) -> io::Result<RawConn> {
        let stream = TcpStream::connect(endpoint)?;
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(RawConn { stream })
    }

    fn submit(&mut self, conn: &mut RawConn, kind: Wire, bytes: &[u8]) -> io::Result<()> {
        if !matches!(kind, Wire::Stream) {
            return Err(io::Error::from(io::ErrorKind::Unsupported));
        }
        let fd = conn.stream.as_raw_fd();
        let mut sent = 0;
        while sent < bytes.len() {
            let n = unsafe {
                libc::send(
                    fd,
                    bytes[sent..].as_ptr().cast(),
                    bytes.len() - sent,
                    libc::MSG_NOSIGNAL,
                )
            };
            if n > 0 {
                sent += n as usize;
                continue;
            }
            let err = io::Error::last_os_error();
            if let Some(libc::EINTR) = err.raw_os_error() {
                continue;
            }
            if err.kind() == io::ErrorKind::WouldBlock {
                std::hint::spin_loop();
                continue;
            }
            return Err(err);
        }
        Ok(())
    }

    fn poll_recv(&mut self, conn: &mut RawConn, buf: &mut [u8]) -> io::Result<usize> {
        let fd = conn.stream.as_raw_fd();
        loop {
            let n = unsafe { libc::recv(fd, buf.as_mut_ptr().cast(), buf.len(), 0) };
            if n > 0 {
                return Ok(n as usize);
            }
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            let err = io::Error::last_os_error();
            if let Some(libc::EINTR) = err.raw_os_error() {
                continue;
            }
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(0);
            }
            return Err(err);
        }
    }
}
