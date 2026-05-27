#![cfg(feature = "dhat-heap")]

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[test]
fn hot_path_splice_is_zero_alloc() {
    use tx_sender::protocol::http::spec_builder::EnvelopeSpecBuilder;
    let _profiler = dhat::Profiler::builder().testing().build();

    let mut tpl = EnvelopeSpecBuilder::new("POST", "/", "rpc.example.com", 2000)
        .build()
        .compile()
        .unwrap();
    let tx = [0u8; 256];
    let _ = tpl.splice(&tx).unwrap();

    let before = dhat::HeapStats::get();
    for _ in 0..1000 {
        let _ = tpl.splice(&tx).unwrap();
    }
    let after = dhat::HeapStats::get();
    assert_eq!(
        after.total_blocks, before.total_blocks,
        "splice allocated on the hot path"
    );
}
