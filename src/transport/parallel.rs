use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use low_latency_utils::tsc::rdtsc;
use low_latency_utils::{SpscConsumer, SpscProducer};

use crate::backend::raw_libc::RawLibc;
use crate::backend::ParBackend;
use crate::conn::reconnect::{ConnKind, ReconnectReq, ReconnectResp};
use crate::conn::{http_conn_failed_io, ConnState, DualConn};
use crate::error::TransportError;
use crate::job::{Job, ProviderId, MAX_TX_LEN};
use crate::result_lane::{RawResp, RESP_CAP, RESULT_RING};
use crate::sink::OutcomeKind;
use crate::source::TxSource;
use crate::transport::sequential::TRIGGER_RING;
use crate::transport::{Engine, TransportSpec};
use crossbeam_channel::{Receiver, Sender};

#[derive(Clone, Default)]
pub struct CoreSet {
    ids: Vec<usize>,
}

impl CoreSet {
    #[must_use]
    pub fn new(ids: Vec<usize>) -> Self {
        Self { ids }
    }

    #[must_use]
    pub fn ids(&self) -> &[usize] {
        &self.ids
    }
}

#[derive(Clone, Default)]
pub struct RtConfig {
    pub priority: Option<i32>,
}

#[derive(Clone, Default)]
pub struct ParallelConfig {
    pub affinity: Option<CoreSet>,
    pub rt: RtConfig,
}

pub struct Parallel<B: ParBackend> {
    _marker: PhantomData<B>,
}

impl Parallel<RawLibc> {
    #[must_use]
    pub fn raw_libc() -> TransportSpec {
        TransportSpec {
            engine_kind: Engine::ParRaw {
                cfg: ParallelConfig::default(),
            },
        }
    }
}

#[cfg(feature = "libtpa")]
impl Parallel<crate::backend::libtpa::LibTpa> {
    #[must_use]
    pub fn libtpa() -> TransportSpec {
        TransportSpec {
            engine_kind: Engine::ParTpa {
                cfg: ParallelConfig::default(),
            },
        }
    }
}

const PERIODIC_MASK: u32 = 0x3FF;

pub struct ParWorker<B: ParBackend> {
    pub provider_idx: usize,
    pub trigger: SpscConsumer<Job, TRIGGER_RING>,
    pub result_tx: SpscProducer<RawResp, RESULT_RING>,
    pub source: Arc<dyn TxSource>,
    pub dual_conn: DualConn<ConnState<B>>,
    pub inflight: Option<(Job, u64)>,
    pub reconnect_req_tx: Sender<ReconnectReq>,
    pub reconnect_resp_rx: Receiver<ReconnectResp>,
    pub conn_kind: ConnKind,
    tick: u32,
}

impl<B: ParBackend + 'static> ParWorker<B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider_idx: usize,
        trigger: SpscConsumer<Job, TRIGGER_RING>,
        result_tx: SpscProducer<RawResp, RESULT_RING>,
        source: Arc<dyn TxSource>,
        dual_conn: DualConn<ConnState<B>>,
        reconnect_req_tx: Sender<ReconnectReq>,
        reconnect_resp_rx: Receiver<ReconnectResp>,
        conn_kind: ConnKind,
    ) -> Self {
        Self {
            provider_idx,
            trigger,
            result_tx,
            source,
            dual_conn,
            inflight: None,
            reconnect_req_tx,
            reconnect_resp_rx,
            conn_kind,
            tick: 0,
        }
    }

    pub fn run(mut self, stop: Arc<AtomicBool>) {
        let mut scratch = [0u8; MAX_TX_LEN];
        let pid = ProviderId(self.provider_idx as u16);
        while !stop.load(Ordering::Relaxed) {
            if let Some(job) = self.trigger.try_pop() {
                let sent = rdtsc();
                let len = self.source.fill(pid, job, &mut scratch);
                let end = usize::from(len);
                self.send_to(job, sent, end, &scratch);
            }
            self.dual_conn.active.conn.drive();
            self.reap(pid);
            self.tick = self.tick.wrapping_add(1);
            if self.tick & PERIODIC_MASK == 0 {
                self.check_standby_delivery();
            }
            if self.trigger.is_empty() {
                unsafe {
                    low_latency_utils::wait::idle_wait(self.trigger.tail_ptr(), || {
                        self.trigger.is_empty() && !stop.load(Ordering::Relaxed)
                    });
                }
                self.check_standby_delivery();
            }
        }
    }

    fn send_to(&mut self, job: Job, sent: u64, end: usize, scratch: &[u8]) {
        let tx = &scratch[..end];
        match &mut self.dual_conn.active.conn {
            ConnState::Http(h) => match h.send_tx(tx) {
                Ok(()) => self.inflight = Some((job, sent)),
                Err(TransportError::Io(ref e)) if http_conn_failed_io(e) => {
                    self.do_failover();
                    if let ConnState::Http(h2) = &mut self.dual_conn.active.conn {
                        if h2.send_tx(tx).is_ok() {
                            self.inflight = Some((job, sent));
                        }
                    }
                }
                Err(_) => {}
            },
            ConnState::Quic(q) => {
                let _ = q.send_tx(job, sent, tx);
            }
        }
    }

    fn do_failover(&mut self) {
        self.inflight = None;
        self.dual_conn.swap();
        let _ = self.reconnect_req_tx.try_send(ReconnectReq {
            provider_idx: self.provider_idx,
            kind: self.conn_kind,
        });
    }

    fn reap(&mut self, pid: ProviderId) {
        let mut failed = false;
        match &mut self.dual_conn.active.conn {
            ConnState::Http(h) => match h.poll_response() {
                Ok(Some(body)) => {
                    if let Some((job, sent)) = self.inflight.take() {
                        let mut resp = RawResp::new();
                        resp.job = job.id;
                        resp.provider = pid;
                        resp.sent_tsc = sent;
                        resp.settled_tsc = rdtsc();
                        let take = body.len().min(RESP_CAP);
                        resp.bytes.extend(body[..take].iter().copied());
                        let _ = self.result_tx.try_push(resp);
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
                    let _ = self.result_tx.try_push(resp);
                }
            }
        }
        if failed {
            self.do_failover();
        }
    }

    fn check_standby_delivery(&mut self) {
        if let Ok(resp) = self.reconnect_resp_rx.try_recv() {
            if let Ok(new_conn) = resp.conn.downcast::<ConnState<B>>() {
                self.dual_conn.replace_standby(*new_conn);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_set_stores_ids() {
        let cs = CoreSet::new(vec![2, 4, 6]);
        assert_eq!(cs.ids(), &[2usize, 4, 6]);
    }

    #[test]
    fn rt_config_default_has_no_priority() {
        let rt = RtConfig::default();
        assert!(rt.priority.is_none());
    }
}
