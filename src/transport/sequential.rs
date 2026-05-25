use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use low_latency_utils::tsc::rdtsc;
use low_latency_utils::{SpscConsumer, SpscProducer};

use crate::conn::ConnState;
use crate::job::Job;
use crate::result_lane::{RawResp, RESP_CAP, RESULT_RING};
use crate::sink::OutcomeKind;
use crate::source::TxSource;

pub const TRIGGER_RING: usize = 256;

pub struct SequentialWorker {
    pub trigger: SpscConsumer<Job, TRIGGER_RING>,
    pub conns: Vec<ConnState>,
    pub result_tx: Vec<SpscProducer<RawResp, RESULT_RING>>,
    pub source: Arc<dyn TxSource>,
    pub inflight: Vec<Option<(Job, u64)>>,
}

impl SequentialWorker {
    pub fn run(mut self, stop: Arc<AtomicBool>) {
        let mut scratch = [0u8; crate::job::MAX_TX_LEN];
        while !stop.load(Ordering::Relaxed) {
            if let Some(job) = self.trigger.try_pop() {
                let sent = rdtsc();
                for i in 0..self.conns.len() {
                    let pid = crate::job::ProviderId(i as u16);
                    let len = self.source.fill(pid, job, &mut scratch);
                    let tx = &scratch[..usize::from(len)];
                    match &mut self.conns[i] {
                        ConnState::Http(h) => {
                            if h.send_tx(tx).is_ok() {
                                self.inflight[i] = Some((job, sent));
                            }
                        }
                        ConnState::Quic(q) => {
                            let _ = q.send_tx(job, sent, tx);
                        }
                    }
                }
            }
            for i in 0..self.conns.len() {
                self.reap(i);
            }
            if self.trigger.is_empty() {
                unsafe {
                    low_latency_utils::wait::idle_wait(self.trigger.tail_ptr(), || {
                        self.trigger.is_empty() && !stop.load(Ordering::Relaxed)
                    });
                }
            }
        }
    }

    fn reap(&mut self, i: usize) {
        match &mut self.conns[i] {
            ConnState::Http(h) => {
                if let Ok(Some(body)) = h.poll_response() {
                    if let Some((job, sent)) = self.inflight[i].take() {
                        let mut resp = RawResp::new();
                        resp.job = job.id;
                        resp.provider = crate::job::ProviderId(i as u16);
                        resp.sent_tsc = sent;
                        resp.settled_tsc = rdtsc();
                        let take = body.len().min(RESP_CAP);
                        resp.bytes.extend(body[..take].iter().copied());
                        let _ = self.result_tx[i].try_push(resp);
                    }
                }
            }
            ConnState::Quic(q) => {
                q.drive();
                if let Some((job_id, sent_tsc)) = q.poll_noresp_window() {
                    let mut resp = RawResp::new();
                    resp.job = job_id;
                    resp.provider = crate::job::ProviderId(i as u16);
                    resp.sent_tsc = sent_tsc;
                    resp.settled_tsc = rdtsc();
                    resp.outcome_hint = Some(OutcomeKind::NoResponse);
                    let _ = self.result_tx[i].try_push(resp);
                }
            }
        }
    }
}
