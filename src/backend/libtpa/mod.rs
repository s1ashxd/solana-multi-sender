pub use libtpa_sys as ffi;

use std::ffi::CString;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::backend::{ByteIo, ParBackend, Wire};
use crate::error::TransportError;

static INIT_DONE: AtomicBool = AtomicBool::new(false);

pub struct TpaRuntime {
    _lock: std::fs::File,
}

impl TpaRuntime {
    pub fn try_acquire_lock_only() -> io::Result<Self> {
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open("/tmp/tx-sender-libtpa.lock")?;
        let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { _lock: lock })
    }

    pub fn start(nr_worker: i32, nr_udp_queue: i32) -> Result<Self, TransportError> {
        let runtime = Self::try_acquire_lock_only().map_err(TransportError::Io)?;
        if INIT_DONE.swap(true, Ordering::SeqCst) {
            return Err(TransportError::Tpa(-1));
        }
        let rc = unsafe { ffi::tpa_init_with_udp_queues(nr_worker, nr_udp_queue) };
        if rc != 0 {
            INIT_DONE.store(false, Ordering::SeqCst);
            return Err(TransportError::Tpa(rc));
        }
        Ok(runtime)
    }

    pub fn init_udp_ports(&mut self, ports: &mut [u16]) -> Result<(), TransportError> {
        let rc = unsafe { ffi::tpa_udp_init(ports.as_mut_ptr(), ports.len() as i32) };
        if rc != 0 {
            return Err(TransportError::Tpa(rc));
        }
        Ok(())
    }
}

pub struct TpaConn {
    pub sid: i32,
    pub dst: Option<SocketAddr>,
}

pub struct LibTpa {
    worker: *mut ffi::TpaWorker,
}

impl LibTpa {
    #[must_use]
    pub fn new() -> Self {
        Self {
            worker: std::ptr::null_mut(),
        }
    }

    #[must_use]
    pub fn with_worker(worker: *mut ffi::TpaWorker) -> Self {
        Self { worker }
    }

    pub fn attach_worker(&mut self) -> Result<(), TransportError> {
        let worker = unsafe { ffi::tpa_worker_init() };
        if worker.is_null() {
            return Err(TransportError::Tpa(-1));
        }
        self.worker = worker;
        Ok(())
    }

    #[must_use]
    pub fn worker(&self) -> *mut ffi::TpaWorker {
        self.worker
    }

    pub fn connect_tcp(&mut self, host: &str, port: u16) -> Result<TpaConn, TransportError> {
        let server = CString::new(host).map_err(|_| TransportError::Tpa(-1))?;
        let sid = unsafe { ffi::tpa_connect_to(server.as_ptr(), port, std::ptr::null()) };
        if sid < 0 {
            return Err(TransportError::Tpa(sid));
        }
        Ok(TpaConn {
            sid,
            dst: None,
        })
    }

    pub fn connect_udp(dst: SocketAddr) -> TpaConn {
        TpaConn {
            sid: -1,
            dst: Some(dst),
        }
    }

    fn send_udp(&mut self, conn: &TpaConn, dst: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        let v4 = match dst {
            SocketAddr::V4(v4) => v4,
            SocketAddr::V6(_) => return Err(io::Error::from(io::ErrorKind::Unsupported)),
        };
        let local_port = match conn.dst {
            Some(SocketAddr::V4(local)) => local.port(),
            _ => 0,
        };
        let pkt = build_udp_pkt(
            bytes.as_ptr() as *mut c_void,
            bytes.len() as u16,
            *v4.ip(),
            v4.port(),
            local_port,
        );
        let n = unsafe { ffi::tpa_udp_send_batch(self.worker, &pkt, 1) };
        if n == 1 {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::WriteZero))
        }
    }

    fn recv_udp(&mut self, _conn: &TpaConn, buf: &mut [u8]) -> io::Result<usize> {
        let mut pkt = ffi::TpaUdpPkt {
            buf: buf.as_mut_ptr().cast(),
            len: buf.len() as u16,
            remote_ip: ffi::TpaIp::zeroed(),
            remote_port: 0,
            local_port: 0,
        };
        let n = unsafe { ffi::tpa_udp_recv_batch(self.worker, &mut pkt, 1) };
        if n <= 0 {
            return Ok(0);
        }
        Ok(pkt.len as usize)
    }

    fn recv_tcp(&mut self, conn: &TpaConn, buf: &mut [u8]) -> io::Result<usize> {
        let mut ev = ffi::TpaEvent {
            events: 0,
            data: std::ptr::null_mut(),
        };
        let n = unsafe { ffi::tpa_event_poll(self.worker, &mut ev, 1) };
        if n <= 0 || ev.events & ffi::TPA_EVENT_IN == 0 {
            return Ok(0);
        }
        let mut iov = ffi::TpaIovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_phys: 0,
            iov_len: buf.len() as u32,
            iov_reserved: 0,
            iov_param: std::ptr::null_mut(),
            iov_done: None,
        };
        let got = unsafe { ffi::tpa_zreadv(conn.sid, &mut iov, 1) };
        if got < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(got as usize)
    }
}

impl Default for LibTpa {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteIo for LibTpa {
    type Conn = TpaConn;

    fn connect(&mut self, endpoint: &str) -> io::Result<TpaConn> {
        let (host, port) =
            split_host_port(endpoint).ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.connect_tcp(host, port)
            .map_err(|err| io::Error::other(format!("{err:?}")))
    }

    fn submit(&mut self, conn: &mut TpaConn, kind: Wire, bytes: &[u8]) -> io::Result<()> {
        match kind {
            Wire::Stream => {
                let mut sent = 0;
                while sent < bytes.len() {
                    let n = unsafe {
                        ffi::tpa_write(
                            conn.sid,
                            bytes[sent..].as_ptr().cast(),
                            bytes.len() - sent,
                        )
                    };
                    if n > 0 {
                        sent += n as usize;
                        continue;
                    }
                    if n == 0 {
                        unsafe { ffi::tpa_worker_run(self.worker) };
                        continue;
                    }
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }
            Wire::Datagram { dst } => self.send_udp(conn, dst, bytes),
        }
    }

    fn poll_recv(&mut self, conn: &mut TpaConn, buf: &mut [u8]) -> io::Result<usize> {
        if conn.dst.is_some() {
            self.recv_udp(conn, buf)
        } else {
            self.recv_tcp(conn, buf)
        }
    }

    fn drive(&mut self) {
        if !self.worker.is_null() {
            unsafe { ffi::tpa_worker_run(self.worker) };
        }
    }
}

impl ParBackend for LibTpa {}

fn split_host_port(endpoint: &str) -> Option<(&str, u16)> {
    let (host, port) = endpoint.rsplit_once(':')?;
    let port = port.parse().ok()?;
    Some((host, port))
}

fn tpa_ip_from_v4(addr: Ipv4Addr) -> ffi::TpaIp {
    let mut ip = ffi::TpaIp::zeroed();
    unsafe {
        ip.u32[2] = 0xffff0000u32;
        ip.u32[3] = u32::from(addr).to_be();
    }
    ip
}

fn build_udp_pkt(
    buf: *mut c_void,
    len: u16,
    remote: Ipv4Addr,
    remote_port: u16,
    local_port: u16,
) -> ffi::TpaUdpPkt {
    ffi::TpaUdpPkt {
        buf,
        len,
        remote_ip: tpa_ip_from_v4(remote),
        remote_port: remote_port.to_be(),
        local_port: local_port.to_be(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flock_second_acquire_rejected() {
        let _first = TpaRuntime::try_acquire_lock_only().expect("first");
        assert!(TpaRuntime::try_acquire_lock_only().is_err());
    }

    #[test]
    fn udp_ports_network_order() {
        let pkt = build_udp_pkt(
            std::ptr::null_mut(),
            0,
            "1.2.3.4".parse().unwrap(),
            8080,
            5000,
        );
        assert_eq!(pkt.remote_port, 8080u16.to_be());
        assert_eq!(pkt.local_port, 5000u16.to_be());
    }

    #[test]
    fn ipv4_maps_into_tpa_ip() {
        let ip = tpa_ip_from_v4(Ipv4Addr::new(1, 2, 3, 4));
        unsafe {
            assert_eq!(ip.u32[3], u32::from(Ipv4Addr::new(1, 2, 3, 4)).to_be());
            assert_eq!(ip.u32[2], 0xffff0000u32);
        }
    }

    fn _assert_par_backend<B: ParBackend>() {}

    #[test]
    fn libtpa_is_par_backend() {
        _assert_par_backend::<LibTpa>();
    }
}
