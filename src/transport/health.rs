/// Latency histogram snapshot — POD, Copy so it fits inside a `SeqLock`.
#[derive(Clone, Copy, Debug, Default)]
pub struct HistSnapshot {
    pub p50: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub max: u64,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerDiag {
    pub alive: bool,
    pub jobs_sent: u64,
    pub fill_ns: HistSnapshot,
    pub submit_ns: HistSnapshot,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthEntry {
    pub provider_idx: usize,
    pub alive: bool,
    pub jobs_sent: u64,
    pub fill_ns: HistSnapshot,
    pub submit_ns: HistSnapshot,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthSnapshot {
    pub providers: Vec<ProviderHealthEntry>,
}

#[cfg(feature = "profiling")]
pub(crate) fn hist_snap(h: &hdrhistogram::Histogram<u64>) -> HistSnapshot {
    HistSnapshot {
        p50: h.value_at_quantile(0.5),
        p99: h.value_at_quantile(0.99),
        p99_9: h.value_at_quantile(0.999),
        max: h.max(),
        count: h.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seqlock::SeqLock;

    #[test]
    fn diag_publish_read_roundtrip() {
        let d: SeqLock<WorkerDiag> = SeqLock::new(WorkerDiag::default());
        *d.lock_write() = WorkerDiag {
            alive: true,
            jobs_sent: 7,
            ..Default::default()
        };
        let got = d.read();
        assert!(got.alive);
        assert_eq!(got.jobs_sent, 7);
    }
}
