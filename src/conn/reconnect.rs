use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnKind {
    RawLibc,
    IoUring,
    LibTpa,
}

pub struct ReconnectReq {
    pub provider_idx: usize,
    pub kind: ConnKind,
}

pub type ConnBox = Box<dyn Any + Send>;

pub struct ReconnectResp {
    pub provider_idx: usize,
    pub conn: ConnBox,
}

#[derive(Debug)]
pub enum ReconnectError {
    NotSupported,
    Build(String),
}

pub trait ReconnectBuilder: Send + 'static {
    fn build(&self, provider_idx: usize, kind: ConnKind) -> Result<ConnBox, ReconnectError>;
}

pub struct ReconnectThread {
    req_tx: Sender<ReconnectReq>,
    handle: Option<JoinHandle<()>>,
}

impl ReconnectThread {
    pub fn spawn(
        builder: Box<dyn ReconnectBuilder>,
        resp_txs: Vec<Sender<ReconnectResp>>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let (req_tx, req_rx) = crossbeam_channel::bounded::<ReconnectReq>(64);
        let handle = std::thread::Builder::new()
            .name("tx-sender-reconnect".into())
            .spawn(move || run_reconnect_loop(builder, req_rx, resp_txs, stop))
            .expect("reconnect thread spawn");
        Self {
            req_tx,
            handle: Some(handle),
        }
    }

    pub fn sender(&self) -> Sender<ReconnectReq> {
        self.req_tx.clone()
    }

    pub fn join(mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn run_reconnect_loop(
    builder: Box<dyn ReconnectBuilder>,
    req_rx: Receiver<ReconnectReq>,
    resp_txs: Vec<Sender<ReconnectResp>>,
    stop: Arc<AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match req_rx.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(req) => {
                if req.kind == ConnKind::LibTpa {
                    continue;
                }
                if let Ok(conn) = builder.build(req.provider_idx, req.kind) {
                    let idx = req.provider_idx;
                    if let Some(tx) = resp_txs.get(idx) {
                        let _ = tx.try_send(ReconnectResp {
                            provider_idx: idx,
                            conn,
                        });
                    }
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_round_trips_provider_idx() {
        let req = ReconnectReq { provider_idx: 3, kind: ConnKind::RawLibc };
        assert_eq!(req.provider_idx, 3);
        assert!(matches!(req.kind, ConnKind::RawLibc));
    }

    #[test]
    fn conn_kind_libtpa_is_defined() {
        let _k = ConnKind::LibTpa;
    }

    #[test]
    fn reconnect_thread_exits_on_stop() {
        struct NoopBuilder;
        impl ReconnectBuilder for NoopBuilder {
            fn build(&self, _: usize, _: ConnKind) -> Result<ConnBox, ReconnectError> {
                Err(ReconnectError::NotSupported)
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let rt = ReconnectThread::spawn(Box::new(NoopBuilder), vec![], stop.clone());
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        rt.join();
    }
}
