use crate::job::{Job, ProviderId, MAX_TX_LEN};

pub trait TxSource: Send + Sync {
    fn fill(&self, provider: ProviderId, job: Job, out: &mut [u8; MAX_TX_LEN]) -> u16;
}
