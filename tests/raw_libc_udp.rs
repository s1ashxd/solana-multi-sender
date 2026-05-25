use std::net::UdpSocket;
use tx_sender::backend::raw_libc::{RawLibc, UdpConn};

#[test]
fn udp_send_and_recv_loopback() {
    let server = UdpSocket::bind("127.0.0.1:0").unwrap();
    let server_addr: std::net::SocketAddr = server.local_addr().unwrap();
    server.set_nonblocking(true).unwrap();

    let mut io = RawLibc::new();
    let conn = io.udp_connect("0.0.0.0:0").unwrap();

    conn.send_to(server_addr, b"ping").unwrap();

    let mut buf = [0u8; 64];
    let mut got = 0usize;
    for _ in 0..1000 {
        match server.recv_from(&mut buf) {
            Ok((n, _)) => {
                got = n;
                break;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => std::hint::spin_loop(),
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(got, 4);
    assert_eq!(&buf[..4], b"ping");
}

#[test]
fn udp_poll_recv_returns_zero_when_empty() {
    let mut io = RawLibc::new();
    let mut conn: UdpConn = io.udp_connect("0.0.0.0:0").unwrap();
    let mut buf = [0u8; 64];
    let n = io.poll_recv_udp(&mut conn, &mut buf).unwrap();
    assert_eq!(n, 0);
}
