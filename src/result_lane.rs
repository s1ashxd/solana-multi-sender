use std::sync::Arc;

use arrayvec::ArrayVec;
use crate::rt::spsc::Consumer as SpscConsumer;

use crate::job::{JobId, ProviderId};
use crate::provider::response::ResponseCodec;
use crate::sink::{OutcomeKind, ProviderOutcome, ResultSink};

pub const RESP_CAP: usize = 1024;
pub const RESULT_RING: usize = 256;

pub struct RawResp {
    pub job: JobId,
    pub provider: ProviderId,
    pub sent_tsc: u64,
    pub settled_tsc: u64,
    pub bytes: ArrayVec<u8, RESP_CAP>,
    pub outcome_hint: Option<OutcomeKind>,
}

impl RawResp {
    #[must_use]
    pub fn new() -> Self {
        Self {
            job: 0,
            provider: ProviderId(0),
            sent_tsc: 0,
            settled_tsc: 0,
            bytes: ArrayVec::new(),
            outcome_hint: None,
        }
    }
}

impl Default for RawResp {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn parse_one(resp: &RawResp, codec: &dyn ResponseCodec) -> ProviderOutcome {
    let kind = if matches!(resp.outcome_hint, Some(OutcomeKind::NoResponse)) {
        OutcomeKind::NoResponse
    } else {
        codec.parse(&resp.bytes)
    };
    ProviderOutcome {
        job: resp.job,
        provider: resp.provider,
        kind,
        sent_at: resp.sent_tsc,
        settled_at: resp.settled_tsc,
    }
}

pub struct ResultLane {
    rings: Vec<SpscConsumer<RawResp>>,
    codecs: Vec<Arc<dyn ResponseCodec>>,
    sink: Arc<dyn ResultSink>,
}

impl ResultLane {
    #[must_use]
    pub fn new(
        rings: Vec<SpscConsumer<RawResp>>,
        codecs: Vec<Arc<dyn ResponseCodec>>,
        sink: Arc<dyn ResultSink>,
    ) -> Self {
        Self { rings, codecs, sink }
    }

    pub fn run(&mut self, stop: &std::sync::atomic::AtomicBool) {
        use std::sync::atomic::Ordering;
        while !stop.load(Ordering::Relaxed) {
            let mut got = false;
            for (i, ring) in self.rings.iter_mut().enumerate() {
                while let Some(resp) = ring.try_pop() {
                    got = true;
                    let outcome = parse_one(&resp, self.codecs[i].as_ref());
                    self.sink.on_result(outcome);
                }
            }
            if !got {
                if let Some(first) = self.rings.first() {
                    unsafe {
                        crate::rt::wait::idle_wait(first.monitor_addr(), || {
                            first.is_empty() && !stop.load(Ordering::Relaxed)
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::ProviderId;
    use crate::sink::{OutcomeKind, ProviderOutcome, ResultSink};
    use std::sync::mpsc;

    struct ChanSink(std::sync::Mutex<mpsc::Sender<ProviderOutcome>>);
    impl ResultSink for ChanSink {
        fn on_result(&self, o: ProviderOutcome) {
            self.0.lock().unwrap().send(o).unwrap();
        }
    }

    #[test]
    fn parses_and_dispatches_one_response() {
        let mut resp = RawResp::new();
        resp.job = 42;
        resp.provider = ProviderId(0);
        resp.bytes.extend(br#"{"result":"sigZ"}"#.iter().copied());
        let codec = crate::provider::response::JsonRpcCodec;
        let outcome = parse_one(&resp, &codec);
        match outcome.kind {
            OutcomeKind::Accepted { signature } => assert_eq!(signature, "sigZ"),
            other => panic!("{other:?}"),
        }
        assert_eq!(outcome.job, 42);
        let _ = ChanSink(std::sync::Mutex::new(mpsc::channel().0));
    }
}
