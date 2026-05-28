use criterion::{black_box, criterion_group, criterion_main, Criterion};

use low_latency_utils::spsc;
use tx_sender::job::Job;
use tx_sender::protocol::http::spec_builder::EnvelopeSpecBuilder;

fn bench_trigger_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigger_submit");

    let mut tpl = EnvelopeSpecBuilder::new("POST", "/", "rpc.example.com", 2000)
        .build()
        .compile()
        .unwrap();
    let tx = [0u8; 256];
    let _ = tpl.splice(&tx).unwrap();

    group.bench_function("envelope_splice", |b| {
        b.iter(|| {
            let _ = tpl.splice(black_box(&tx)).unwrap();
        });
    });

    let (prod, cons) = spsc::channel::<Job, 256>();
    group.bench_function("spsc_trigger_push", |b| {
        b.iter(|| {
            let _ = prod.try_push(black_box(Job { id: 1, ctx: 0 }));
            let _ = cons.try_pop();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_trigger_submit);
criterion_main!(benches);
