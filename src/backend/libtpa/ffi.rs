#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub const TPA_EVENT_IN: u32 = 0x1;
pub const TPA_EVENT_OUT: u32 = 0x4;
pub const TPA_EVENT_ERR: u32 = 0x8;
pub const TPA_EVENT_HUP: u32 = 0x10;

#[repr(C)]
pub struct tpa_event {
    pub events: u32,
    pub data: *mut c_void,
}

#[repr(C)]
pub struct tpa_iovec {
    pub iov_base: *mut c_void,
    pub iov_phys: u64,
    pub iov_len: u32,
    pub iov_reserved: u32,
    pub iov_param: *mut c_void,
    pub iov_done: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
}

#[repr(C, packed)]
pub struct tpa_sock_opts {
    pub bitfield: u64,
    pub data: *mut c_void,
    pub local_port: u16,
    pub reserved: [u8; 110],
}

#[repr(C)]
pub union tpa_ip {
    pub u8_: [u8; 16],
    pub u32_: [u32; 4],
    pub u64_: [u64; 2],
}

#[repr(C)]
pub struct tpa_udp_pkt {
    pub buf: *mut c_void,
    pub len: u16,
    pub remote_ip: tpa_ip,
    pub remote_port: u16,
    pub local_port: u16,
}

#[repr(C)]
pub struct tpa_worker {
    _private: [u8; 0],
}

extern "C" {
    pub fn tpa_init(nr_worker: c_int) -> c_int;
    pub fn tpa_init_with_udp_queues(nr_worker: c_int, nr_udp_queue: c_int) -> c_int;
    pub fn tpa_worker_init() -> *mut tpa_worker;
    pub fn tpa_worker_run(worker: *mut tpa_worker);
    pub fn tpa_connect_to(server: *const c_char, port: u16, opts: *const tpa_sock_opts) -> c_int;
    pub fn tpa_close(sid: c_int);
    pub fn tpa_write(sid: c_int, buf: *const c_void, count: usize) -> isize;
    pub fn tpa_zreadv(sid: c_int, iov: *mut tpa_iovec, nr_iov: c_int) -> isize;
    pub fn tpa_event_poll(worker: *mut tpa_worker, events: *mut tpa_event, max: c_int) -> c_int;
    pub fn tpa_udp_init(listen_ports: *mut u16, nr_port: c_int) -> c_int;
    pub fn tpa_udp_send_batch(
        worker: *mut tpa_worker,
        pkts: *const tpa_udp_pkt,
        count: c_int,
    ) -> c_int;
    pub fn tpa_udp_recv_batch(
        worker: *mut tpa_worker,
        pkts: *mut tpa_udp_pkt,
        max_count: c_int,
    ) -> c_int;
}
