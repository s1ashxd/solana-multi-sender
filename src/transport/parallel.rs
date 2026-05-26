use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use low_latency_utils::tsc::rdtsc;
use low_latency_utils::{SpscConsumer, SpscProducer};

use crate::backend::raw_libc::RawLibc;
use crate::backend::ParBackend;
use crate::conn::ConnState;
use crate::job::{Job, ProviderId, MAX_TX_LEN};
use crate::result_lane::{RawResp, RESP_CAP, RESULT_RING};
use crate::sink::OutcomeKind;
use crate::source::TxSource;
use crate::transport::sequential::TRIGGER_RING;
use crate::transport::{Engine, TransportSpec};

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

pub struct ParWorker<B: ParBackend> {
    pub provider_idx: usize,
    pub trigger: SpscConsumer<Job, TRIGGER_RING>,
    pub result_tx: SpscProducer<RawResp, RESULT_RING>,
    pub source: Arc<dyn TxSource>,
    pub conn: ConnState<B>,
    pub inflight: Option<(Job, u64)>,
}

impl<B: ParBackend> ParWorker<B> {
    pub fn run(mut self, stop: Arc<AtomicBool>) {
        let mut scratch = [0u8; MAX_TX_LEN];
        let pid = ProviderId(self.provider_idx as u16);
        while !stop.load(Ordering::Relaxed) {
            if let Some(job) = self.trigger.try_pop() {
                let sent = rdtsc();
                let len = self.source.fill(pid, job, &mut scratch);
                let tx = &scratch[..usize::from(len)];
                match &mut self.conn {
                    ConnState::Http(h) => {
                        if h.send_tx(tx).is_ok() {
                            self.inflight = Some((job, sent));
                        }
                    }
                    ConnState::Quic(q) => {
                        let _ = q.send_tx(job, sent, tx);
                    }
                }
            }
            self.conn.drive();
            self.reap(pid);
            if self.trigger.is_empty() {
                unsafe {
                    low_latency_utils::wait::idle_wait(self.trigger.tail_ptr(), || {
                        self.trigger.is_empty() && !stop.load(Ordering::Relaxed)
                    });
                }
            }
        }
    }

    fn reap(&mut self, pid: ProviderId) {
        match &mut self.conn {
            ConnState::Http(h) => {
                if let Ok(Some(body)) = h.poll_response() {
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
            }
            ConnState::Quic(q) => {
                if let Some((job_id, sent_tsc)) = q.poll_noresp_window() {
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
