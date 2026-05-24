use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use tx_sender::backend::raw_libc::RawLibc;
use tx_sender::backend::{ByteIo, Wire};

#[test]
fn send_and_recv_over_loopback() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 5];
        sock.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        sock.write_all(b"world").unwrap();
    });

    let mut io = RawLibc::new();
    let mut conn = io.connect(&addr.to_string()).unwrap();
    io.submit(&mut conn, Wire::Stream, b"hello").unwrap();

    let mut rx = [0u8; 16];
    let mut got = 0;
    while got < 5 {
        match io.poll_recv(&mut conn, &mut rx[got..]) {
            Ok(n) if n > 0 => got += n,
            _ => std::hint::spin_loop(),
        }
    }
    assert_eq!(&rx[..5], b"world");
    server.join().unwrap();
}
