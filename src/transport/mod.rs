pub mod sequential;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use rustls::ClientConfig;

use crate::conn::HttpConn;
use crate::error::{SenderError, TriggerError};
use crate::job::Job;
use crate::protocol::http::spec_builder::EnvelopeSpecBuilder;
use crate::provider::response::ResponseCodec;
use crate::provider::{HttpAuth, ProviderConfig};
use crate::result_lane::{RawResp, ResultLane, RESULT_RING};
use crate::sink::ResultSink;
use crate::source::TxSource;
use crate::transport::sequential::{SequentialWorker, TRIGGER_RING};
use low_latency_utils::{SpscConsumer, SpscProducer};

pub struct SenderBuilder {
    providers: Vec<ProviderConfig>,
    source: Option<Arc<dyn TxSource>>,
    sink: Option<Arc<dyn ResultSink>>,
    tls: Arc<ClientConfig>,
}

pub struct Sender {
    trigger: SpscProducer<Job, TRIGGER_RING>,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl Sender {
    #[must_use]
    pub fn builder(tls: Arc<ClientConfig>) -> SenderBuilder {
        SenderBuilder {
            providers: Vec::new(),
            source: None,
            sink: None,
            tls,
        }
    }

    pub fn trigger(&self, job: Job) -> Result<(), TriggerError> {
        if self.trigger.try_push(job) {
            Ok(())
        } else {
            Err(TriggerError::Backpressure)
        }
    }

    pub fn shutdown(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in self.workers {
            let _ = w.join();
        }
    }
}

impl SenderBuilder {
    #[must_use]
    pub fn provider(mut self, p: ProviderConfig) -> Self {
        self.providers.push(p);
        self
    }

    #[must_use]
    pub fn source(mut self, s: Arc<dyn TxSource>) -> Self {
        self.source = Some(s);
        self
    }

    #[must_use]
    pub fn sink(mut self, s: Arc<dyn ResultSink>) -> Self {
        self.sink = Some(s);
        self
    }

    pub fn build(self) -> Result<Sender, SenderError> {
        if self.providers.is_empty() {
            return Err(SenderError::NoProviders);
        }
        let source = self.source.ok_or(SenderError::NoSource)?;
        let sink = self.sink.ok_or(SenderError::NoSink)?;

        let (trigger_tx, trigger_rx) = low_latency_utils::spsc::channel::<Job, TRIGGER_RING>();

        let mut conns = Vec::new();
        let mut result_tx = Vec::new();
        let mut result_rx: Vec<SpscConsumer<RawResp, RESULT_RING>> = Vec::new();
        let mut codecs: Vec<Arc<dyn ResponseCodec>> = Vec::new();

        for (i, p) in self.providers.iter().enumerate() {
            let spec = build_envelope(p);
            let tpl = spec.compile().expect("envelope compiles");
            let conn = HttpConn::connect(p, self.tls.clone(), tpl)
                .map_err(|e| SenderError::Connect { provider: i as u16, source: e })?;
            conns.push(conn);
            let (tx, rx) = low_latency_utils::spsc::channel::<RawResp, RESULT_RING>();
            result_tx.push(tx);
            result_rx.push(rx);
            codecs.push(p.codec.clone());
        }

        let stop = Arc::new(AtomicBool::new(false));

        let lane = ResultLane::new(result_rx, codecs, sink);
        let lane_stop = stop.clone();
        let lane_handle = std::thread::spawn(move || lane.run(&lane_stop));

        let inflight = (0..conns.len()).map(|_| None).collect();
        let worker = SequentialWorker {
            trigger: trigger_rx,
            conns,
            result_tx,
            source,
            inflight,
        };
        let worker_stop = stop.clone();
        let worker_handle = std::thread::spawn(move || worker.run(worker_stop));

        Ok(Sender {
            trigger: trigger_tx,
            stop,
            workers: vec![worker_handle, lane_handle],
        })
    }
}

fn build_envelope(p: &ProviderConfig) -> crate::protocol::http::envelope::EnvelopeSpec {
    let mut b = EnvelopeSpecBuilder::send_transaction(
        &p.endpoint.path,
        &p.endpoint.server_name,
        p.max_body,
    );
    if let HttpAuth::Header { name, value } = &p.auth {
        b = b.header(name, value);
    }
    b.build()
}
