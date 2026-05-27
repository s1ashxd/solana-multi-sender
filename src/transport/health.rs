use low_latency_utils::StatSnapshot;

#[derive(Clone, Debug, Default)]
pub struct WorkerDiag {
    pub alive: bool,
    pub jobs_sent: u64,
    pub fill_ns: StatSnapshot,
    pub submit_ns: StatSnapshot,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthEntry {
    pub provider_idx: usize,
    pub alive: bool,
    pub jobs_sent: u64,
    pub fill_ns: StatSnapshot,
    pub submit_ns: StatSnapshot,
}

#[derive(Clone, Debug)]
pub struct ProviderHealthSnapshot {
    pub providers: Vec<ProviderHealthEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use low_latency_utils::SharedDiagnostics;

    #[test]
    fn diag_publish_read_roundtrip() {
        let d: SharedDiagnostics<WorkerDiag> = SharedDiagnostics::new(WorkerDiag::default());
        d.publish(WorkerDiag {
            alive: true,
            jobs_sent: 7,
            ..Default::default()
        });
        let got = d.read_latest().unwrap();
        assert!(got.alive);
        assert_eq!(got.jobs_sent, 7);
    }
}
