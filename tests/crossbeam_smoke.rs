#[test]
fn crossbeam_bounded_send_recv() {
    let (tx, rx) = crossbeam_channel::bounded::<u32>(4);
    tx.send(42).unwrap();
    assert_eq!(rx.recv().unwrap(), 42);
}
