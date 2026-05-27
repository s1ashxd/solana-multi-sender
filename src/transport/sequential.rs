use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use low_latency_utils::tsc::rdtsc;
use low_latency_utils::{SpscConsumer, SpscProducer};

use crate::backend::raw_libc::RawLibc;
use crate::backend::SeqBackend;
use crate::conn::reconnect::{ConnKind, ReconnectReq, ReconnectResp};
use crate::conn::{http_conn_failed_io, ConnState, DualConn};
use crate::error::TransportError;
use crate::job::Job;
use crate::result_lane::{RawResp, RESP_CAP, RESULT_RING};
use crate::sink::OutcomeKind;
use crate::source::TxSource;
use crate::transport::health::WorkerDiag;
use crate::transport::{DiagSlot, Engine, TransportSpec};
use crossbeam_channel::{Receiver, Sender};

pub const TRIGGER_RING: usize = 256;

const PERIODIC_MASK: u32 = 0x3FF;

pub struct SequentialWorker<B: SeqBackend> {
    pub trigger: SpscConsumer<Job, TRIGGER_RING>,
    pub conns: Vec<DualConn<ConnState<B>>>,
    pub result_tx: Vec<SpscProducer<RawResp, RESULT_RING>>,
    pub source: Arc<dyn TxSource>,
    pub inflight: Vec<Option<(Job, u64)>>,
    pub reconnect_req_tx: Sender<ReconnectReq>,
    pub reconnect_resp_rx: Vec<Receiver<ReconnectResp>>,
    pub conn_kind: ConnKind,
    tick: u32,
    diag: Vec<DiagSlot>,
    jobs_sent: Vec<u64>,
    #[cfg(feature = "profiling")]
    profiler: low_latency_utils::Profiler,
    #[cfg(feature = "profiling")]
    fill_stat: Vec<low_latency_utils::LatencyStat>,
    #[cfg(feature = "profiling")]
    submit_stat: Vec<low_latency_utils::LatencyStat>,
}

impl<B: SeqBackend + 'static> SequentialWorker<B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trigger: SpscConsumer<Job, TRIGGER_RING>,
        conns: Vec<DualConn<ConnState<B>>>,
        result_tx: Vec<SpscProducer<RawResp, RESULT_RING>>,
        source: Arc<dyn TxSource>,
        reconnect_req_tx: Sender<ReconnectReq>,
        reconnect_resp_rx: Vec<Receiver<ReconnectResp>>,
        conn_kind: ConnKind,
        diag: Vec<DiagSlot>,
    ) -> Self {
        let n = conns.len();
        let inflight = (0..n).map(|_| None).collect();
        Self {
            trigger,
            conns,
            result_tx,
            source,
            inflight,
            reconnect_req_tx,
            reconnect_resp_rx,
            conn_kind,
            tick: 0,
            diag,
            jobs_sent: vec![0; n],
            #[cfg(feature = "profiling")]
            profiler: low_latency_utils::Profiler::new(),
            #[cfg(feature = "profiling")]
            fill_stat: (0..n).map(|_| low_latency_utils::LatencyStat::new()).collect(),
            #[cfg(feature = "profiling")]
            submit_stat: (0..n).map(|_| low_latency_utils::LatencyStat::new()).collect(),
        }
    }

    pub fn run(mut self, stop: Arc<AtomicBool>) {
        let mut scratch = [0u8; crate::job::MAX_TX_LEN];
        while !stop.load(Ordering::Relaxed) {
            if let Some(job) = self.trigger.try_pop() {
                let sent = rdtsc();
                for i in 0..self.conns.len() {
                    let pid = crate::job::ProviderId(i as u16);
                    #[cfg(feature = "profiling")]
                    let t0 = self.profiler.mark();
                    let len = self.source.fill(pid, job, &mut scratch);
                    #[cfg(feature = "profiling")]
                    {
                        let t1 = self.profiler.mark();
                        self.fill_stat[i].record(self.profiler.elapsed(t0, t1));
                    }
                    let end = usize::from(len);
                    self.send_to(i, job, sent, end, &scratch, pid);
                }
                for i in 0..self.conns.len() {
                    self.conns[i].active.conn.drive();
                }
            }
            for i in 0..self.conns.len() {
                self.conns[i].active.conn.drive();
                self.reap(i, crate::job::ProviderId(i as u16));
            }
            self.tick = self.tick.wrapping_add(1);
            if self.tick & PERIODIC_MASK == 0 {
                self.check_standby_deliveries();
                self.publish_diag();
            }
            if self.trigger.is_empty() {
                unsafe {
                    low_latency_utils::wait::idle_wait(self.trigger.tail_ptr(), || {
                        self.trigger.is_empty() && !stop.load(Ordering::Relaxed)
                    });
                }
                self.check_standby_deliveries();
                self.publish_diag();
            }
        }
    }

    fn send_to(
        &mut self,
        i: usize,
        job: Job,
        sent: u64,
        end: usize,
        scratch: &[u8],
        pid: crate::job::ProviderId,
    ) {
        let tx = &scratch[..end];
        #[cfg(feature = "profiling")]
        let t0 = self.profiler.mark();
        match &mut self.conns[i].active.conn {
            ConnState::Http(h) => match h.send_tx(tx) {
                Ok(()) => {
                    self.inflight[i] = Some((job, sent));
                    self.jobs_sent[i] += 1;
                }
                Err(TransportError::Io(ref e)) if http_conn_failed_io(e) => {
                    self.do_failover(i);
                    if let ConnState::Http(h2) = &mut self.conns[i].active.conn {
                        if h2.send_tx(tx).is_ok() {
                            self.inflight[i] = Some((job, sent));
                            self.jobs_sent[i] += 1;
                        }
                    }
                }
                Err(_) => {}
            },
            ConnState::Quic(q) => {
                let _ = q.send_tx(job, sent, tx);
                self.jobs_sent[i] += 1;
            }
        }
        #[cfg(feature = "profiling")]
        {
            let t1 = self.profiler.mark();
            self.submit_stat[i].record(self.profiler.elapsed(t0, t1));
        }
        let _ = pid;
    }

    fn do_failover(&mut self, i: usize) {
        self.inflight[i] = None;
        self.conns[i].swap();
        let _ = self
            .reconnect_req_tx
            .try_send(ReconnectReq { provider_idx: i, kind: self.conn_kind });
    }

    fn reap(&mut self, i: usize, pid: crate::job::ProviderId) {
        let mut failed = false;
        match &mut self.conns[i].active.conn {
            ConnState::Http(h) => match h.poll_response() {
                Ok(Some(body)) => {
                    if let Some((job, sent)) = self.inflight[i].take() {
                        let mut resp = RawResp::new();
                        resp.job = job.id;
                        resp.provider = pid;
                        resp.sent_tsc = sent;
                        resp.settled_tsc = rdtsc();
                        let take = body.len().min(RESP_CAP);
                        resp.bytes.extend(body[..take].iter().copied());
                        let _ = self.result_tx[i].try_push(resp);
                    }
                }
                Err(TransportError::Io(ref e)) if http_conn_failed_io(e) => failed = true,
                _ => {}
            },
            ConnState::Quic(q) => {
                if q.is_dead() {
                    failed = true;
                } else if let Some((job_id, sent_tsc)) = q.poll_noresp_window() {
                    let mut resp = RawResp::new();
                    resp.job = job_id;
                    resp.provider = pid;
                    resp.sent_tsc = sent_tsc;
                    resp.settled_tsc = rdtsc();
                    resp.outcome_hint = Some(OutcomeKind::NoResponse);
                    let _ = self.result_tx[i].try_push(resp);
                }
            }
        }
        if failed {
            self.do_failover(i);
        }
    }

    fn check_standby_deliveries(&mut self) {
        for i in 0..self.reconnect_resp_rx.len() {
            if let Ok(resp) = self.reconnect_resp_rx[i].try_recv() {
                if let Ok(new_conn) = resp.conn.downcast::<ConnState<B>>() {
                    self.conns[resp.provider_idx].replace_standby(*new_conn);
                }
            }
        }
    }

    fn publish_diag(&self) {
        for i in 0..self.diag.len() {
            #[cfg(feature = "profiling")]
            let (fill_ns, submit_ns) = (self.fill_stat[i].snapshot(), self.submit_stat[i].snapshot());
            #[cfg(not(feature = "profiling"))]
            let (fill_ns, submit_ns) = (
                low_latency_utils::StatSnapshot::default(),
                low_latency_utils::StatSnapshot::default(),
            );
            self.diag[i].publish(WorkerDiag {
                alive: true,
                jobs_sent: self.jobs_sent[i],
                fill_ns,
                submit_ns,
            });
        }
    }
}

pub struct Sequential<B: SeqBackend> {
    _marker: PhantomData<B>,
}

impl Sequential<RawLibc> {
    #[must_use]
    pub fn raw_libc() -> TransportSpec {
        TransportSpec {
            engine_kind: Engine::SeqRaw,
        }
    }
}

#[cfg(feature = "io-uring")]
impl Sequential<crate::backend::io_uring::IoUring> {
    #[must_use]
    pub fn io_uring(mode: crate::backend::io_uring::IoUringMode) -> TransportSpec {
        TransportSpec {
            engine_kind: Engine::SeqUring {
                mode,
                tuning: Default::default(),
            },
        }
    }
}
