pub mod parallel;
pub mod sequential;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use rustls::ClientConfig;

use crate::backend::raw_libc::RawLibc;
use crate::backend::SeqBackend;
use crate::conn::{ConnState, HttpConn, QuicConn};
use crate::error::{SenderError, TriggerError};
use crate::job::Job;
use crate::protocol::http::spec_builder::EnvelopeSpecBuilder;
use crate::protocol::quic::QuicEngine;
use crate::provider::response::ResponseCodec;
use crate::provider::{HttpAuth, ProviderConfig, Protocol};
use crate::result_lane::{RawResp, ResultLane, RESULT_RING};
use crate::sink::ResultSink;
use crate::source::TxSource;
use crate::transport::sequential::{SequentialWorker, TRIGGER_RING};
use low_latency_utils::{SpscConsumer, SpscProducer};

#[cfg(feature = "io-uring")]
use crate::backend::io_uring::{IoUring, IoUringMode, IoUringTuning};

#[derive(Clone)]
pub enum Engine {
    SeqRaw,
    #[cfg(feature = "io-uring")]
    SeqUring {
        mode: IoUringMode,
        tuning: IoUringTuning,
    },
    ParRaw {
        cfg: crate::transport::parallel::ParallelConfig,
    },
}

pub struct TransportSpec {
    pub(crate) engine_kind: Engine,
}

impl TransportSpec {
    #[must_use]
    pub fn affinity(mut self, cores: crate::transport::parallel::CoreSet) -> Self {
        if let Engine::ParRaw { cfg } = &mut self.engine_kind {
            cfg.affinity = Some(cores);
        }
        self
    }

    #[must_use]
    pub fn rt_priority(mut self, priority: i32) -> Self {
        if let Engine::ParRaw { cfg } = &mut self.engine_kind {
            cfg.rt.priority = Some(priority);
        }
        self
    }
}

#[cfg(feature = "io-uring")]
impl TransportSpec {
    #[must_use]
    pub fn tuning(mut self, tuning: IoUringTuning) -> Self {
        if let Engine::SeqUring { tuning: t, .. } = &mut self.engine_kind {
            *t = tuning;
        }
        self
    }
}

pub struct SenderBuilder {
    transport: Option<TransportSpec>,
    providers: Vec<ProviderConfig>,
    source: Option<Arc<dyn TxSource>>,
    sink: Option<Arc<dyn ResultSink>>,
    tls: Option<Arc<ClientConfig>>,
}

enum Dispatch {
    Single(SpscProducer<Job, TRIGGER_RING>),
    Fanout(Vec<SpscProducer<Job, TRIGGER_RING>>),
}

pub struct Sender {
    dispatch: Dispatch,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl Sender {
    #[must_use]
    pub fn builder() -> SenderBuilder {
        SenderBuilder {
            transport: None,
            providers: Vec::new(),
            source: None,
            sink: None,
            tls: None,
        }
    }

    pub fn trigger(&self, job: Job) -> Result<(), TriggerError> {
        match &self.dispatch {
            Dispatch::Single(tx) => {
                if tx.try_push(job) {
                    Ok(())
                } else {
                    Err(TriggerError::Backpressure)
                }
            }
            Dispatch::Fanout(txs) => {
                let mut ok = true;
                for tx in txs {
                    if !tx.try_push(job) {
                        ok = false;
                    }
                }
                if ok {
                    Ok(())
                } else {
                    Err(TriggerError::Backpressure)
                }
            }
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
    pub fn transport(mut self, spec: TransportSpec) -> Self {
        self.transport = Some(spec);
        self
    }

    #[must_use]
    pub fn tls(mut self, cfg: Arc<ClientConfig>) -> Self {
        self.tls = Some(cfg);
        self
    }

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
        let spec = self.transport.ok_or(SenderError::NoTransport)?;
        let tls = self.tls.ok_or(SenderError::NoTls)?;
        let source = self.source.ok_or(SenderError::NoSource)?;
        let sink = self.sink.ok_or(SenderError::NoSink)?;

        let stop = Arc::new(AtomicBool::new(false));

        let mut result_tx = Vec::new();
        let mut result_rx: Vec<SpscConsumer<RawResp, RESULT_RING>> = Vec::new();
        let mut codecs: Vec<Arc<dyn ResponseCodec>> = Vec::new();
        for p in &self.providers {
            let (tx, rx) = low_latency_utils::spsc::channel::<RawResp, RESULT_RING>();
            result_tx.push(tx);
            result_rx.push(rx);
            codecs.push(p.codec.clone());
        }

        let lane = ResultLane::new(result_rx, codecs, sink);
        let lane_stop = stop.clone();
        let lane_handle = std::thread::spawn(move || lane.run(&lane_stop));

        let (dispatch, mut workers): (Dispatch, Vec<JoinHandle<()>>) = match spec.engine_kind {
            Engine::SeqRaw => {
                let conns = connect_all(&self.providers, &tls, |_| Ok(RawLibc::new()))?;
                let (trig_tx, trig_rx) = low_latency_utils::spsc::channel::<Job, TRIGGER_RING>();
                let h = spawn_seq(conns, trig_rx, result_tx, source, stop.clone());
                (Dispatch::Single(trig_tx), vec![h])
            }
            #[cfg(feature = "io-uring")]
            Engine::SeqUring { mode, tuning } => {
                let conns = connect_all(&self.providers, &tls, move |_| {
                    IoUring::new(mode, tuning.clone()).map_err(SenderError::Unsupported)
                })?;
                let (trig_tx, trig_rx) = low_latency_utils::spsc::channel::<Job, TRIGGER_RING>();
                let h = spawn_seq(conns, trig_rx, result_tx, source, stop.clone());
                (Dispatch::Single(trig_tx), vec![h])
            }
            Engine::ParRaw { cfg } => {
                let conns = connect_all(&self.providers, &tls, |_| Ok(RawLibc::new()))?;
                spawn_parallel(conns, result_tx, source, cfg, stop.clone())
            }
        };
        workers.push(lane_handle);

        Ok(Sender {
            dispatch,
            stop,
            workers,
        })
    }
}

fn connect_all<B, F>(
    providers: &[ProviderConfig],
    tls: &Arc<ClientConfig>,
    make: F,
) -> Result<Vec<ConnState<B>>, SenderError>
where
    B: SeqBackend,
    F: Fn(usize) -> Result<B, SenderError>,
{
    let mut conns = Vec::with_capacity(providers.len());
    for (i, p) in providers.iter().enumerate() {
        let conn_state = match p.protocol {
            Protocol::Http => {
                let spec = build_envelope(p);
                let tpl = spec.compile().expect("envelope compiles");
                let io = make(i)?;
                let conn = HttpConn::connect_with(io, p, tls.clone(), tpl)
                    .map_err(|e| SenderError::Connect {
                        provider: i as u16,
                        source: e,
                    })?;
                ConnState::Http(Box::new(conn))
            }
            Protocol::Quic => {
                let quic_ep = p.quic_endpoint.as_ref().ok_or_else(|| SenderError::QuicConfig {
                    provider: i as u16,
                    reason: "missing quic_endpoint".into(),
                })?;
                let quic_profile = p.quic_profile.as_ref().ok_or_else(|| SenderError::QuicConfig {
                    provider: i as u16,
                    reason: "missing quic_profile".into(),
                })?;
                let quic_auth = p.quic_auth.as_ref().ok_or_else(|| SenderError::QuicConfig {
                    provider: i as u16,
                    reason: "missing quic_auth".into(),
                })?;
                let client_cfg =
                    crate::protocol::quic_cert::build_quic_client_config(quic_profile, quic_auth)
                        .map_err(|e| SenderError::Connect {
                            provider: i as u16,
                            source: e,
                        })?;
                let engine = QuicEngine::connect(client_cfg, quic_ep.addr, &quic_ep.server_name)
                    .map_err(|e| SenderError::Connect {
                        provider: i as u16,
                        source: e,
                    })?;
                ConnState::Quic(Box::new(QuicConn::new(engine)))
            }
        };
        conns.push(conn_state);
    }
    Ok(conns)
}

fn spawn_seq<B>(
    conns: Vec<ConnState<B>>,
    trigger: SpscConsumer<Job, TRIGGER_RING>,
    result_tx: Vec<SpscProducer<RawResp, RESULT_RING>>,
    source: Arc<dyn TxSource>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()>
where
    B: SeqBackend + Send + 'static,
    B::Conn: Send + 'static,
{
    let inflight = (0..conns.len()).map(|_| None).collect();
    let worker = SequentialWorker {
        trigger,
        conns,
        result_tx,
        source,
        inflight,
    };
    std::thread::spawn(move || worker.run(stop))
}

fn spawn_parallel<B>(
    conns: Vec<ConnState<B>>,
    result_tx: Vec<SpscProducer<RawResp, RESULT_RING>>,
    source: Arc<dyn TxSource>,
    cfg: crate::transport::parallel::ParallelConfig,
    stop: Arc<AtomicBool>,
) -> (Dispatch, Vec<JoinHandle<()>>)
where
    B: crate::backend::ParBackend + Send + 'static,
    B::Conn: Send + 'static,
{
    use crate::transport::parallel::ParWorker;

    let core_ids: Vec<usize> = cfg
        .affinity
        .as_ref()
        .map(|c| c.ids().to_vec())
        .unwrap_or_default();
    let rt_priority = cfg.rt.priority;

    let mut trigger_txs = Vec::with_capacity(conns.len());
    let mut handles = Vec::with_capacity(conns.len());

    for (i, (conn, res_tx)) in conns.into_iter().zip(result_tx).enumerate() {
        let (trig_tx, trig_rx) = low_latency_utils::spsc::channel::<Job, TRIGGER_RING>();
        trigger_txs.push(trig_tx);

        let worker = ParWorker {
            provider_idx: i,
            trigger: trig_rx,
            result_tx: res_tx,
            source: source.clone(),
            conn,
            inflight: None,
        };
        let core_id = core_ids.get(i).copied();
        let worker_stop = stop.clone();
        let handle = std::thread::spawn(move || {
            if let Some(id) = core_id {
                crate::rt::pin_current_thread(id);
            }
            if let Some(prio) = rt_priority {
                let _ = crate::rt::set_sched_fifo(prio);
            }
            worker.run(worker_stop);
        });
        handles.push(handle);
    }

    (Dispatch::Fanout(trigger_txs), handles)
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
