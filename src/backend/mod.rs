pub mod raw_libc;

#[cfg(feature = "io-uring")]
pub mod io_uring;

#[cfg(feature = "libtpa")]
pub mod libtpa;

use std::net::SocketAddr;

pub enum Wire {
    Datagram { dst: SocketAddr },
    Stream,
}

pub trait ByteIo {
    type Conn;

    fn connect(&mut self, endpoint: &str) -> std::io::Result<Self::Conn>;
    fn submit(&mut self, conn: &mut Self::Conn, kind: Wire, bytes: &[u8]) -> std::io::Result<()>;
    fn poll_recv(&mut self, conn: &mut Self::Conn, buf: &mut [u8]) -> std::io::Result<usize>;
    fn drive(&mut self) {}
}

pub trait SeqBackend: ByteIo {}

impl SeqBackend for raw_libc::RawLibc {}

pub trait ParBackend: ByteIo {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::raw_libc::RawLibc;
    fn assert_par_backend<B: ParBackend>() {}
    #[test]
    fn raw_libc_is_par_backend() {
        assert_par_backend::<RawLibc>();
    }
}
