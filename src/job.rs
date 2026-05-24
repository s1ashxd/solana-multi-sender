pub const MAX_TX_LEN: usize = 1232;

pub type JobId = u64;
pub type JobCtx = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub ctx: JobCtx,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProviderId(pub u16);

impl ProviderId {
    #[must_use]
    pub const fn idx(self) -> usize {
        self.0 as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_id_index_round_trips() {
        assert_eq!(ProviderId(7).idx(), 7);
    }

    #[test]
    fn job_is_copy_and_small() {
        let j = Job { id: 1, ctx: 2 };
        let k = j;
        assert_eq!(j.id, k.id);
        assert_eq!(core::mem::size_of::<Job>(), 16);
    }
}
