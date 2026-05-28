use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use rustls::ClientConfig;

use crate::backend::raw_libc::RawLibc;
use crate::conn::reconnect::{ConnKind, ReconnectThread};
use crate::error::{SenderError, TriggerError};
use crate::job::Job;
use crate::provider::response::ResponseCodec;
use crate::provider::ProviderConfig;
use crate::result_lane::{RawResp, ResultLane, RESULT_RING};
use crate::rt::spsc::Consumer as SpscConsumer;
use crate::sink::ResultSink;
use crate::source::TxSource;
use crate::transport::sequential::TRIGGER_RING;

#[cfg(feature = "io-uring")]
use crate::backend::io_uring::IoUring;

#[cfg(feature = "libtpa")]
use super::connect::TpaReconnectBuilder;
use super::connect::{connect_dual_all, EngineReconnectBuilder};
#[cfg(feature = "libtpa")]
use super::spawn::spawn_parallel_tpa;
use super::spawn::{spawn_parallel, spawn_seq};
use super::{DiagSlot, Dispatch, Engine, TransportSpec};

pub struct SenderBuilder {
    transport: Option<TransportSpec>,
    providers: Vec<ProviderConfig>,
    source: Option<Arc<dyn TxSource>>,
    sink: Option<Arc<dyn ResultSink>>,
    tls: Option<Arc<ClientConfig>>,
}

pub struct Sender {
    dispatch: Dispatch,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
    reconnect: ReconnectThread,
    diag: Vec<DiagSlot>,
    #[cfg(feature = "libtpa")]
    _tpa: Option<crate::backend::libtpa::TpaRuntime>,
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
                if tx.try_push(job).is_ok() {
                    Ok(())
                } else {
                    Err(TriggerError::Backpressure)
                }
            }
            Dispatch::Fanout(txs) => {
                let mut ok = true;
                for tx in txs {
                    if tx.try_push(job).is_err() {
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
        self.reconnect.join();
    }

    pub fn health(&self) -> crate::transport::health::ProviderHealthSnapshot {
        let providers = self
            .diag
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let w = d.read();
                crate::transport::health::ProviderHealthEntry {
                    provider_idx: i,
                    alive: w.alive,
                    jobs_sent: w.jobs_sent,
                    fill_ns: w.fill_ns,
                    submit_ns: w.submit_ns,
                }
            })
            .collect();
        crate::transport::health::ProviderHealthSnapshot { providers }
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
        let mut result_rx: Vec<SpscConsumer<RawResp>> = Vec::new();
        let mut codecs: Vec<Arc<dyn ResponseCodec>> = Vec::new();
        for p in &self.providers {
            let (tx, rx) = crate::rt::spsc::channel::<RawResp>(RESULT_RING);
            result_tx.push(tx);
            result_rx.push(rx);
            codecs.push(p.codec.clone());
        }

        let mut lane = ResultLane::new(result_rx, codecs, sink);
        let lane_stop = stop.clone();
        let lane_handle = std::thread::spawn(move || lane.run(&lane_stop));

        let n = self.providers.len();
        let diag: Vec<DiagSlot> = (0..n)
            .map(|_| {
                std::sync::Arc::new(seqlock::SeqLock::new(
                    crate::transport::health::WorkerDiag::default(),
                ))
            })
            .collect();
        let mut resp_txs = Vec::with_capacity(n);
        let mut resp_rxs = Vec::with_capacity(n);
        for _ in 0..n {
            let (t, r) = crossbeam_channel::bounded::<crate::conn::reconnect::ReconnectResp>(1);
            resp_txs.push(t);
            resp_rxs.push(r);
        }

        #[cfg(feature = "libtpa")]
        let mut tpa_runtime: Option<crate::backend::libtpa::TpaRuntime> = None;

        let (dispatch, mut workers, reconnect): (Dispatch, Vec<JoinHandle<()>>, ReconnectThread) =
            match spec.engine_kind {
                Engine::SeqRaw => {
                    let duals = connect_dual_all::<RawLibc, _>(&self.providers, &tls, &|_| {
                        Ok(RawLibc::new())
                    })?;
                    let builder = Box::new(EngineReconnectBuilder::<RawLibc, _> {
                        providers: self.providers.clone(),
                        tls: tls.clone(),
                        make: |_| Ok(RawLibc::new()),
                        _marker: std::marker::PhantomData,
                    });
                    let reconnect = ReconnectThread::spawn(builder, resp_txs, stop.clone());
                    let req_tx = reconnect.sender();
                    let (trig_tx, trig_rx) = crate::rt::spsc::channel::<Job>(TRIGGER_RING);
                    let h = spawn_seq(
                        duals,
                        trig_rx,
                        result_tx,
                        source,
                        req_tx,
                        resp_rxs,
                        ConnKind::RawLibc,
                        stop.clone(),
                        diag.clone(),
                    );
                    (Dispatch::Single(trig_tx), vec![h], reconnect)
                }
                #[cfg(feature = "io-uring")]
                Engine::SeqUring { mode, tuning } => {
                    let m = mode;
                    let warmup_tuning = tuning.clone();
                    let duals = connect_dual_all::<IoUring, _>(&self.providers, &tls, &move |_| {
                        IoUring::new(m, warmup_tuning.clone()).map_err(SenderError::Unsupported)
                    })?;
                    let builder_tuning = tuning.clone();
                    let builder = Box::new(EngineReconnectBuilder::<IoUring, _> {
                        providers: self.providers.clone(),
                        tls: tls.clone(),
                        make: move |_| {
                            IoUring::new(m, builder_tuning.clone()).map_err(SenderError::Unsupported)
                        },
                        _marker: std::marker::PhantomData,
                    });
                    let reconnect = ReconnectThread::spawn(builder, resp_txs, stop.clone());
                    let req_tx = reconnect.sender();
                    let (trig_tx, trig_rx) = crate::rt::spsc::channel::<Job>(TRIGGER_RING);
                    let h = spawn_seq(
                        duals,
                        trig_rx,
                        result_tx,
                        source,
                        req_tx,
                        resp_rxs,
                        ConnKind::IoUring,
                        stop.clone(),
                        diag.clone(),
                    );
                    (Dispatch::Single(trig_tx), vec![h], reconnect)
                }
                Engine::ParRaw { cfg } => {
                    let duals = connect_dual_all::<RawLibc, _>(&self.providers, &tls, &|_| {
                        Ok(RawLibc::new())
                    })?;
                    let builder = Box::new(EngineReconnectBuilder::<RawLibc, _> {
                        providers: self.providers.clone(),
                        tls: tls.clone(),
                        make: |_| Ok(RawLibc::new()),
                        _marker: std::marker::PhantomData,
                    });
                    let reconnect = ReconnectThread::spawn(builder, resp_txs, stop.clone());
                    let req_tx = reconnect.sender();
                    let (dispatch, handles) = spawn_parallel(
                        duals,
                        result_tx,
                        source,
                        cfg,
                        req_tx,
                        resp_rxs,
                        ConnKind::RawLibc,
                        stop.clone(),
                        diag.clone(),
                    );
                    (dispatch, handles, reconnect)
                }
                #[cfg(feature = "libtpa")]
                Engine::ParTpa { cfg } => {
                    let rt = crate::backend::libtpa::TpaRuntime::start(
                        self.providers.len() as i32,
                        0,
                    )
                    .map_err(|e| SenderError::Connect {
                        provider: 0,
                        source: e,
                    })?;
                    let builder = Box::new(TpaReconnectBuilder);
                    let reconnect = ReconnectThread::spawn(builder, resp_txs, stop.clone());
                    let req_tx = reconnect.sender();
                    let (dispatch, handles) = spawn_parallel_tpa(
                        self.providers.clone(),
                        tls.clone(),
                        source,
                        cfg,
                        result_tx,
                        req_tx,
                        resp_rxs,
                        stop.clone(),
                        diag.clone(),
                    );
                    tpa_runtime = Some(rt);
                    (dispatch, handles, reconnect)
                }
            };
        workers.push(lane_handle);

        Ok(Sender {
            dispatch,
            stop,
            workers,
            reconnect,
            diag,
            #[cfg(feature = "libtpa")]
            _tpa: tpa_runtime,
        })
    }
}
