pub mod backend;
pub mod conn;
pub mod error;
pub mod job;
pub mod protocol;
pub mod provider;
pub mod result_lane;
pub mod sink;
pub mod source;
pub mod transport;
pub mod rt;

pub mod prelude {
    pub use crate::job::{Job, JobCtx, JobId, ProviderId, MAX_TX_LEN};
    pub use crate::provider::{HttpAuth, HttpEndpoint, ProviderConfig, Protocol};
    pub use crate::sink::{OutcomeKind, ProviderOutcome, ResultSink};
    pub use crate::source::TxSource;
    pub use crate::transport::sequential::Sequential;
    pub use crate::transport::{Sender, SenderBuilder, TransportSpec};
    #[cfg(feature = "io-uring")]
    pub use crate::backend::io_uring::{IoUringMode, IoUringTuning, SqpollCfg};
}
