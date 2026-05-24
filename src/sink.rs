use crate::error::TransportError;
use crate::job::{JobId, ProviderId};

#[derive(Debug)]
pub enum OutcomeKind {
    Accepted { signature: String },
    Rejected { code: i64, message: String },
    Transport(TransportError),
    NoResponse,
}

#[derive(Debug)]
pub struct ProviderOutcome {
    pub job: JobId,
    pub provider: ProviderId,
    pub kind: OutcomeKind,
    pub sent_at: u64,
    pub settled_at: u64,
}

pub trait ResultSink: Send + Sync {
    fn on_result(&self, outcome: ProviderOutcome);
}
