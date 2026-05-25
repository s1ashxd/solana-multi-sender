#![cfg(feature = "io-uring")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use tx_sender::backend::io_uring::{IoUring, IoUringMode, IoUringTuning};
use tx_sender::backend::{ByteIo, Wire};

fn roundtrip(mode: IoUringMode) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 5];
        sock.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        sock.write_all(b"world").unwrap();
    });

    let mut io = match IoUring::new(mode, IoUringTuning::default()) {
        Ok(io) => io,
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            eprintln!("io_uring unsupported, skipping: {e}");
            server.join().unwrap();
            return;
        }
        Err(e) => panic!("{e}"),
    };
    let mut conn = io.connect(&addr.to_string()).unwrap();
    io.submit(&mut conn, Wire::Stream, b"hello").unwrap();
    io.drive();

    let mut rx = [0u8; 16];
    let mut got = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while got < 5 {
        match io.poll_recv(&mut conn, &mut rx[got..]) {
            Ok(n) if n > 0 => got += n,
            _ => {
                io.drive();
                assert!(std::time::Instant::now() < deadline, "timed out");
            }
        }
    }
    assert_eq!(&rx[..5], b"world");
    server.join().unwrap();
}

#[test]
fn batch_syscall_roundtrips() {
    roundtrip(IoUringMode::BatchSyscall);
}

#[test]
fn sqe_stream_roundtrips() {
    roundtrip(IoUringMode::SqeStream);
}
