#![cfg(feature = "libtpa")]
use tx_sender::backend::libtpa::ffi;

#[test]
fn ffi_symbols_link() {
    let syms: &[*const ()] = &[
        ffi::tpa_init as *const (),
        ffi::tpa_init_with_udp_queues as *const (),
        ffi::tpa_worker_init as *const (),
        ffi::tpa_worker_run as *const (),
        ffi::tpa_connect_to as *const (),
        ffi::tpa_close as *const (),
        ffi::tpa_write as *const (),
        ffi::tpa_event_poll as *const (),
        ffi::tpa_udp_init as *const (),
        ffi::tpa_udp_send_batch as *const (),
        ffi::tpa_udp_recv_batch as *const (),
    ];
    assert!(syms.iter().all(|p| !p.is_null()));
}

#[test]
fn struct_sizes_sane() {
    assert_eq!(core::mem::size_of::<ffi::TpaSockOpts>(), 128);
    assert!(core::mem::size_of::<ffi::TpaUdpPkt>() >= 24);
}
