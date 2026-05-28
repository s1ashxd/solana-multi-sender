pub mod health;
pub mod parallel;
pub mod sequential;

mod builder;
mod connect;
mod spawn;

#[cfg(feature = "io-uring")]
use crate::backend::io_uring::{IoUringMode, IoUringTuning};
use crate::job::Job;
use crate::rt::spsc::Producer as SpscProducer;

pub use builder::{Sender, SenderBuilder};
pub use connect::{real_roots, real_roots_client_config};

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
    #[cfg(feature = "libtpa")]
    ParTpa {
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

pub(crate) type DiagSlot =
    std::sync::Arc<seqlock::SeqLock<crate::transport::health::WorkerDiag>>;

enum Dispatch {
    Single(SpscProducer<Job>),
    Fanout(Vec<SpscProducer<Job>>),
}

#[cfg(test)]
mod tls_tests {
    use super::real_roots;

    #[test]
    fn real_roots_returns_non_empty_store() {
        assert!(!real_roots().is_empty());
    }
}
