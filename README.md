# tx-sender

Low-latency tx sender for Solana SWQoS / RPC providers. Same `Sender` API over
blocking `libc` sockets, `io_uring`, or DPDK (libtpa).

`Sender::builder()` wants four things:

- a `TransportSpec` (one of the modes below)
- one or more `ProviderConfig` (HTTP or QUIC)
- a `TxSource` that fills the wire payload for `(provider, job)`
- a `ResultSink` that gets parsed outcomes

`sender.trigger(job)` is a non-blocking SPSC push; the network never touches
the calling thread.

## Modes

### `Sequential::raw_libc()`

One worker thread, blocking `libc::send` / `libc::recv`. Fans the job out to
every provider in turn. Default and the most boring path.

### `Sequential::io_uring(mode)` — feature `io-uring` (default on)

Same single-worker fan-out, but the byte path runs through `io_uring`. TLS is
still rustls.

`IoUringMode`:

- `BatchSyscall` — fill the SQ for every provider, then one `io_uring_enter`.
- `SqeStream` — push one SQE at a time as each provider is filled.

`IoUringTuning::sqpoll` turns on a kernel poll thread so `io_uring_enter`
disappears from the hot path.

### `Parallel::raw_libc()`

One worker thread **per provider**, each with its own `DualConn` and trigger
queue. `sender.trigger(job)` fans the push out across them.

- `.affinity(CoreSet)` pins workers to cores.
- `.rt_priority(prio)` sets `SCHED_FIFO`. Needs `CAP_SYS_NICE`.

### `Parallel::libtpa()` — feature `libtpa` (default off)

Same shape as `Parallel::raw_libc` but the backend is
[libtpa](https://github.com/s1ashxd/libtpa), a DPDK userspace TCP/IP stack.
TCP bypasses the kernel; QUIC still goes through the regular UDP socket.

libtpa sockets are bound to the worker that opened them, so each worker
attaches its own libtpa worker and connects from inside the thread. See
[LibTpa backend](#libtpa-backend-feature-libtpa) for build/runtime setup.

## Protocol

Picked per provider, independent of the engine.

- **HTTP/1.1 over TLS (rustls).** Envelope is built once and spliced in place
  on every job — no allocs, no formatting on the hot path. Auth can be a
  header, a URL query param, or a path token.
- **QUIC** via `quinn-proto`. ed25519 and Falcon client certs for SWQoS auth.
  Handshake at connect time, sends are fire-and-forget on a single stream.

## Failover and reconnect

Every provider holds a `DualConn` (active + standby). On a transport error
the worker swaps to the standby and emits `TransportError::Failover` for the
in-flight job. A shared background thread rebuilds the dead side over a
one-shot crossbeam channel, without touching the worker.

libtpa providers do not get rebuild: the socket has to be opened on its own
worker thread. Failover still works, only reconnect-rebuild is skipped.

## Provider registry — feature `registry` (default on)

`crate::provider::registry` ships presets for 13 HTTP SWQoS endpoints and 3
QUIC endpoints (Jito, Helius, NextBlock, Bloxroute, …). TLS roots come from
`webpki-roots`, so a real sender needs only a keypair and an auth token.

## Diagnostics

- `sender.health()` → `ProviderHealthSnapshot` (`alive`, `jobs_sent`, and
  with `profiling`: `fill_ns`, `submit_ns` percentiles measured via rdtsc).
- `profiling` feature: TSC profiler + `LatencyStat` in both engines.
- `dhat-heap` feature: gates the `dhat_zero_alloc` test that asserts the hot
  path stays alloc-free after warmup.

## Example

`examples/send_real_tx.rs` signs a real Solana tx with a provider tip and
sends it through `Sequential::io_uring` to a live endpoint. Gated behind
`real-tx-example` so the default build doesn't pull Solana SDKs. Not in CI —
needs a funded keypair, an endpoint, and network.

```bash
cargo run --example send_real_tx --features real-tx-example
```

Config is read from env: `TXSENDER_ENDPOINT`, `TXSENDER_KEYPAIR`,
`TXSENDER_TIP_ACCOUNT`, `TXSENDER_TIP_LAMPORTS` (required) and
`TXSENDER_AUTH`, `TXSENDER_CU_PRICE`, `TXSENDER_CU_LIMIT`,
`TXSENDER_BLOCKHASH`, `TXSENDER_BLOCKHASH_RPC` (optional). See the file
header for details.

## LibTpa backend (feature `libtpa`)

### Build

FFI bindings live in the sibling [`libtpa-sys`](../libtpa-sys) crate and are
pulled in by the `libtpa` feature. Linking is driven by `pkg-config`:

- `libtpa.pc` reachable through `PKG_CONFIG_PATH` (defaults to
  `/usr/share/tpa` in the libtpa-sys build script).
- The .pc file is expected to advertise `tpa` along with the usual DPDK
  system libs (`numa dl pthread rt m`). Static `-l:libtpa.a` is honored.

Compile-only checks still need a resolvable libtpa.pc — running
`cargo build --lib --features libtpa` or `cargo clippy --features libtpa`
will invoke pkg-config.

### Runtime

- DPDK v20.11.3.
- Hugepages (e.g. via `nr_hugepages`).
- Mellanox NIC with flow bifurcation.
- A `tpa.cfg` for the NIC binding.
- `CAP_SYS_NICE` + `CAP_IPC_LOCK`.
- One process per NIC binding. Enforced by an exclusive `flock` on
  `/tmp/tx-sender-libtpa.lock`; a second process fails to start.

### Caveats

1. Worker-bound sockets. Connections are opened inside each worker thread,
   not on the build thread.
2. No reconnect-rebuild yet — only failover to standby works under libtpa.
3. QUIC under `Parallel::libtpa()` still uses the kernel UDP socket.
4. Not in CI: link and runtime both need the lib and the hardware. The
   `libtpa_ffi` link test and the ignored `libtpa_dpdk` scaffold only run
   where libtpa links and the NIC is present.
