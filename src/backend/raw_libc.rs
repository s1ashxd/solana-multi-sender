use std::io;
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::os::fd::{AsRawFd, RawFd};

use crate::backend::{ByteIo, Wire};

pub struct RawConn {
    stream: TcpStream,
}

pub struct UdpConn {
    pub(crate) _sock: UdpSocket,
    pub(crate) fd: RawFd,
}

pub struct RawLibc;

impl RawLibc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn udp_connect(&mut self, bind_addr: &str) -> io::Result<UdpConn> {
        let sock = UdpSocket::bind(bind_addr)?;
        sock.set_nonblocking(true)?;
        let fd = sock.as_raw_fd();
        Ok(UdpConn { _sock: sock, fd })
    }

    pub fn poll_recv_udp(&mut self, conn: &mut UdpConn, buf: &mut [u8]) -> io::Result<usize> {
        let n = unsafe {
            libc::recvfrom(
                conn.fd,
                buf.as_mut_ptr().cast(),
                buf.len(),
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if n > 0 {
            return Ok(n as usize);
        }
        if n == 0 {
            return Ok(0);
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(0);
        }
        if let Some(libc::EINTR) = err.raw_os_error() {
            return Ok(0);
        }
        Err(err)
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

impl crate::backend::ParBackend for RawLibc {}

impl UdpConn {
    pub fn send_to(&self, dst: SocketAddr, buf: &[u8]) -> io::Result<()> {
        udp_sendto(self.fd, dst, buf)
    }
}

pub fn udp_sendto(fd: RawFd, dst: SocketAddr, buf: &[u8]) -> io::Result<()> {
    let (storage, sa_len) = sockaddr_of(dst);
    let n = unsafe {
        libc::sendto(
            fd,
            buf.as_ptr().cast(),
            buf.len(),
            libc::MSG_DONTWAIT,
            std::ptr::addr_of!(storage).cast(),
            sa_len,
        )
    };
    if n < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

pub fn udp_sendmsg_gso(fd: RawFd, dst: SocketAddr, bytes: &[u8], segment_size: u16) -> io::Result<()> {
    const UDP_SEGMENT: libc::c_int = 103;
    let (storage, sa_len) = sockaddr_of(dst);
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    };
    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];
    let mut mhdr: libc::msghdr = unsafe { std::mem::zeroed() };
    mhdr.msg_name = std::ptr::addr_of!(storage) as *mut libc::c_void;
    mhdr.msg_namelen = sa_len;
    mhdr.msg_iov = &mut iov;
    mhdr.msg_iovlen = 1;
    mhdr.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    mhdr.msg_controllen = cmsg_space as _;
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&mhdr);
        (*cmsg).cmsg_level = libc::SOL_UDP;
        (*cmsg).cmsg_type = UDP_SEGMENT;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
        std::ptr::write(libc::CMSG_DATA(cmsg) as *mut u16, segment_size);
    }
    let n = unsafe { libc::sendmsg(fd, &mhdr, libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) };
    if n < 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock {
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}

pub(crate) fn sockaddr_of(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let sa_len = match addr {
        SocketAddr::V4(v4) => {
            let sa = unsafe { &mut *std::ptr::addr_of_mut!(storage).cast::<libc::sockaddr_in>() };
            sa.sin_family = libc::AF_INET as libc::sa_family_t;
            sa.sin_port = v4.port().to_be();
            sa.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sa = unsafe { &mut *std::ptr::addr_of_mut!(storage).cast::<libc::sockaddr_in6>() };
            sa.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sa.sin6_port = v6.port().to_be();
            sa.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            sa.sin6_flowinfo = v6.flowinfo();
            sa.sin6_scope_id = v6.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    };
    (storage, sa_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn gso_single_segment_loopback() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let recv_addr = receiver.local_addr().unwrap();

        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let fd = sender.as_raw_fd();

        udp_sendmsg_gso(fd, recv_addr, b"hello", 1280).unwrap();

        let mut buf = [0u8; 64];
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match receiver.recv_from(&mut buf) {
                Ok((n, _)) => {
                    assert_eq!(&buf[..n], b"hello");
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        panic!("timed out waiting for gso datagram");
                    }
                }
                Err(e) => panic!("recv failed: {e}"),
            }
        }
    }
}
