pub mod raw_libc;

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
