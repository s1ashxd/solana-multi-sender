use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::conn::reconnect::ConnKind;
use crate::conn::{ConnState, DualConn};
use crate::job::Job;
use crate::result_lane::RawResp;
use crate::rt::spsc::{Consumer as SpscConsumer, Producer as SpscProducer};
use crate::source::TxSource;
use crate::transport::sequential::{SequentialWorker, TRIGGER_RING};

use super::{DiagSlot, Dispatch};

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_seq<B>(
    duals: Vec<DualConn<ConnState<B>>>,
    trigger: SpscConsumer<Job>,
    result_tx: Vec<SpscProducer<RawResp>>,
    source: Arc<dyn TxSource>,
    reconnect_req_tx: crossbeam_channel::Sender<crate::conn::reconnect::ReconnectReq>,
    reconnect_resp_rx: Vec<crossbeam_channel::Receiver<crate::conn::reconnect::ReconnectResp>>,
    conn_kind: ConnKind,
    stop: Arc<AtomicBool>,
    diag: Vec<DiagSlot>,
) -> JoinHandle<()>
where
    B: crate::backend::SeqBackend + Send + 'static,
    B::Conn: Send + 'static,
{
    let worker = SequentialWorker::new(
        trigger,
        duals,
        result_tx,
        source,
        reconnect_req_tx,
        reconnect_resp_rx,
        conn_kind,
        diag,
    );
    std::thread::spawn(move || worker.run(stop))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_parallel<B>(
    duals: Vec<DualConn<ConnState<B>>>,
    result_tx: Vec<SpscProducer<RawResp>>,
    source: Arc<dyn TxSource>,
    cfg: crate::transport::parallel::ParallelConfig,
    reconnect_req_tx: crossbeam_channel::Sender<crate::conn::reconnect::ReconnectReq>,
    reconnect_resp_rx: Vec<crossbeam_channel::Receiver<crate::conn::reconnect::ReconnectResp>>,
    conn_kind: ConnKind,
    stop: Arc<AtomicBool>,
    diag: Vec<DiagSlot>,
) -> (Dispatch, Vec<JoinHandle<()>>)
where
    B: crate::backend::ParBackend + Send + 'static,
    B::Conn: Send + 'static,
{
    use crate::transport::parallel::ParWorker;

    let core_ids: Vec<usize> = cfg
        .affinity
        .as_ref()
        .map(|c| c.ids().to_vec())
        .unwrap_or_default();
    let rt_priority = cfg.rt.priority;

    let mut trigger_txs = Vec::with_capacity(duals.len());
    let mut handles = Vec::with_capacity(duals.len());

    for (i, ((dual_conn, res_tx), resp_rx)) in duals
        .into_iter()
        .zip(result_tx)
        .zip(reconnect_resp_rx)
        .enumerate()
    {
        let (trig_tx, trig_rx) = crate::rt::spsc::channel::<Job>(TRIGGER_RING);
        trigger_txs.push(trig_tx);

        let worker = ParWorker::new(
            i,
            trig_rx,
            res_tx,
            source.clone(),
            dual_conn,
            reconnect_req_tx.clone(),
            resp_rx,
            conn_kind,
            diag[i].clone(),
        );
        let core_id = core_ids.get(i).copied();
        let worker_stop = stop.clone();
        let handle = std::thread::spawn(move || {
            if let Some(id) = core_id {
                crate::rt::pin_current_thread(id);
            }
            if let Some(prio) = rt_priority {
                let _ = crate::rt::set_sched_fifo(prio);
            }
            worker.run(worker_stop);
        });
        handles.push(handle);
    }

    (Dispatch::Fanout(trigger_txs), handles)
}

#[cfg(feature = "libtpa")]
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_parallel_tpa(
    providers: Vec<crate::provider::ProviderConfig>,
    tls: Arc<rustls::ClientConfig>,
    source: Arc<dyn TxSource>,
    cfg: crate::transport::parallel::ParallelConfig,
    result_tx: Vec<SpscProducer<RawResp>>,
    reconnect_req_tx: crossbeam_channel::Sender<crate::conn::reconnect::ReconnectReq>,
    reconnect_resp_rx: Vec<crossbeam_channel::Receiver<crate::conn::reconnect::ReconnectResp>>,
    stop: Arc<AtomicBool>,
    diag: Vec<DiagSlot>,
) -> (Dispatch, Vec<JoinHandle<()>>) {
    use super::connect::connect_one;
    use crate::backend::libtpa::LibTpa;
    use crate::transport::parallel::ParWorker;

    let core_ids: Vec<usize> = cfg
        .affinity
        .as_ref()
        .map(|c| c.ids().to_vec())
        .unwrap_or_default();
    let rt_priority = cfg.rt.priority;

    let mut trigger_txs = Vec::with_capacity(providers.len());
    let mut handles = Vec::with_capacity(providers.len());

    for ((i, res_tx), resp_rx) in result_tx.into_iter().enumerate().zip(reconnect_resp_rx) {
        let (trig_tx, trig_rx) = crate::rt::spsc::channel::<Job>(TRIGGER_RING);
        trigger_txs.push(trig_tx);

        let providers_t = providers.clone();
        let tls_t = tls.clone();
        let source_t = source.clone();
        let req_tx_t = reconnect_req_tx.clone();
        let stop_t = stop.clone();
        let diag_t = diag[i].clone();
        let core_id = core_ids.get(i).copied();

        let handle = std::thread::spawn(move || {
            if let Some(id) = core_id {
                crate::rt::pin_current_thread(id);
            }
            if let Some(prio) = rt_priority {
                let _ = crate::rt::set_sched_fifo(prio);
            }
            let mut probe = LibTpa::new();
            if probe.attach_worker().is_err() {
                return;
            }
            let worker_ptr = probe.worker();
            let make = move |_| Ok(LibTpa::with_worker(worker_ptr));
            let Ok(active) = connect_one::<LibTpa, _>(&providers_t, i, &tls_t, &make) else {
                return;
            };
            let Ok(standby) = connect_one::<LibTpa, _>(&providers_t, i, &tls_t, &make) else {
                return;
            };
            let dual = DualConn::new(active, standby);
            let worker = ParWorker::new(
                i,
                trig_rx,
                res_tx,
                source_t,
                dual,
                req_tx_t,
                resp_rx,
                ConnKind::LibTpa,
                diag_t,
            );
            worker.run(stop_t);
        });
        handles.push(handle);
    }

    (Dispatch::Fanout(trigger_txs), handles)
}
