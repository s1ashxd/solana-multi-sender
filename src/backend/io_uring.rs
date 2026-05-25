use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

use io_uring::{opcode, squeue, types, IoUring as Ring};

use crate::backend::{ByteIo, SeqBackend, Wire};

const OP_SEND: u64 = 0;
const OP_RECV: u64 = 1;
const RX_CAP: usize = 4096;
const RING_ENTRIES: u32 = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoUringMode {
    BatchSyscall,
    SqeStream,
}

#[derive(Clone, Debug)]
pub struct SqpollCfg {
    pub poll_core: u32,
    pub idle_ms: u32,
}

#[derive(Clone, Debug, Default)]
pub struct IoUringTuning {
    pub sqpoll: Option<SqpollCfg>,
}

pub struct UringConn {
    _stream: TcpStream,
    fd: RawFd,
    idx: u64,
    rx_buf: Box<[u8; RX_CAP]>,
    recv_in_flight: bool,
    send_in_flight: VecDeque<Vec<u8>>,
}

pub struct IoUring {
    ring: Ring,
    mode: IoUringMode,
    sqpoll: bool,
    next_idx: u64,
    pending: VecDeque<squeue::Entry>,
    completed_sends: HashMap<u64, u32>,
    completed_recvs: HashMap<u64, i32>,
}

fn ud(idx: u64, op: u64) -> u64 {
    (idx << 1) | op
}

impl IoUring {
    pub fn new(mode: IoUringMode, tuning: IoUringTuning) -> io::Result<Self> {
        let mut builder = Ring::builder();
        let sqpoll = tuning.sqpoll.is_some();
        if let Some(cfg) = tuning.sqpoll.as_ref() {
            builder.setup_sqpoll(cfg.idle_ms);
            builder.setup_sqpoll_cpu(cfg.poll_core);
        }
        let ring = builder.build(RING_ENTRIES).map_err(map_setup_err)?;
        Ok(Self {
            ring,
            mode,
            sqpoll,
            next_idx: 0,
            pending: VecDeque::new(),
            completed_sends: HashMap::new(),
            completed_recvs: HashMap::new(),
        })
    }

    fn push_sqe(&mut self, entry: squeue::Entry) -> io::Result<()> {
        loop {
            let mut sq = self.ring.submission();
            match unsafe { sq.push(&entry) } {
                Ok(()) => {
                    drop(sq);
                    return Ok(());
                }
                Err(_) => {
                    drop(sq);
                    self.flush_submit()?;
                    self.reap();
                }
            }
        }
    }

    fn flush_submit(&mut self) -> io::Result<()> {
        if !self.sqpoll {
            self.ring.submit()?;
        }
        Ok(())
    }

    fn reap(&mut self) {
        let mut cq = self.ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let key = cqe.user_data();
            if key & 1 == OP_SEND {
                *self.completed_sends.entry(key).or_insert(0) += 1;
            } else {
                self.completed_recvs.insert(key, cqe.result());
            }
        }
    }

    fn drain_pending(&mut self) -> io::Result<()> {
        while let Some(entry) = self.pending.pop_front() {
            loop {
                let mut sq = self.ring.submission();
                match unsafe { sq.push(&entry) } {
                    Ok(()) => {
                        drop(sq);
                        break;
                    }
                    Err(_) => {
                        drop(sq);
                        self.flush_submit()?;
                        self.reap();
                    }
                }
            }
        }
        Ok(())
    }

    fn reclaim_sends(&mut self, conn: &mut UringConn) {
        let key = ud(conn.idx, OP_SEND);
        if let Some(n) = self.completed_sends.remove(&key) {
            for _ in 0..n {
                conn.send_in_flight.pop_front();
            }
        }
    }
}

fn map_setup_err(e: io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(libc::ENOSYS) | Some(libc::EPERM) => io::Error::new(io::ErrorKind::Unsupported, e),
        _ => e,
    }
}

impl ByteIo for IoUring {
    type Conn = UringConn;

    fn connect(&mut self, endpoint: &str) -> io::Result<UringConn> {
        let stream = TcpStream::connect(endpoint)?;
        stream.set_nodelay(true)?;
        let fd = stream.as_raw_fd();
        let idx = self.next_idx;
        self.next_idx += 1;
        Ok(UringConn {
            _stream: stream,
            fd,
            idx,
            rx_buf: Box::new([0u8; RX_CAP]),
            recv_in_flight: false,
            send_in_flight: VecDeque::new(),
        })
    }

    fn submit(&mut self, conn: &mut UringConn, kind: Wire, bytes: &[u8]) -> io::Result<()> {
        match kind {
            Wire::Stream => {}
            Wire::Datagram { .. } => return Err(io::Error::from(io::ErrorKind::Unsupported)),
        }
        conn.send_in_flight.push_back(bytes.to_vec());
        let staged = conn.send_in_flight.back().unwrap();
        let entry = opcode::Send::new(types::Fd(conn.fd), staged.as_ptr(), staged.len() as u32)
            .build()
            .user_data(ud(conn.idx, OP_SEND));
        match self.mode {
            IoUringMode::SqeStream => {
                self.push_sqe(entry)?;
                self.flush_submit()?;
            }
            IoUringMode::BatchSyscall => {
                self.pending.push_back(entry);
            }
        }
        Ok(())
    }

    fn poll_recv(&mut self, conn: &mut UringConn, buf: &mut [u8]) -> io::Result<usize> {
        if !conn.recv_in_flight {
            let entry = opcode::Recv::new(
                types::Fd(conn.fd),
                conn.rx_buf.as_mut_ptr(),
                conn.rx_buf.len() as u32,
            )
            .build()
            .user_data(ud(conn.idx, OP_RECV));
            self.push_sqe(entry)?;
            conn.recv_in_flight = true;
        }
        self.drive_inner()?;
        self.reclaim_sends(conn);
        let key = ud(conn.idx, OP_RECV);
        if let Some(result) = self.completed_recvs.remove(&key) {
            conn.recv_in_flight = false;
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
            let n = result as usize;
            if n == 0 {
                return Ok(0);
            }
            let copy = n.min(buf.len());
            buf[..copy].copy_from_slice(&conn.rx_buf[..copy]);
            return Ok(copy);
        }
        Ok(0)
    }

    fn drive(&mut self) {
        let _ = self.drive_inner();
    }
}

impl IoUring {
    fn drive_inner(&mut self) -> io::Result<()> {
        if self.mode == IoUringMode::BatchSyscall {
            self.drain_pending()?;
        }
        self.flush_submit()?;
        self.reap();
        Ok(())
    }
}

impl SeqBackend for IoUring {}
