# tx-sender

A low-latency transaction sender library built around pluggable transport
engines. Providers are configured over HTTP or QUIC, and transports range from
the default raw `libc` socket backend to optional kernel-bypass paths.

## Example: send a real signed transaction (`examples/send_real_tx`)

`examples/send_real_tx.rs` builds a real signed Solana transaction that carries a
provider tip and submits it through the `Sequential::io_uring` HTTP path over
TLS to a live SWQoS/RPC endpoint. The Solana dependencies it needs are isolated
behind the opt-in `real-tx-example` feature, so the default build, tests, and
clippy never compile them. The example itself is gated by `required-features`
and is **not** part of CI: it needs a funded keypair, a real endpoint, and
network access.

Run it with:

```bash
cargo run --example send_real_tx --features real-tx-example
```

Configuration is read from the environment.

Required:

- `TXSENDER_ENDPOINT` — `host[:port]/path` of the SWQoS/RPC endpoint, e.g.
  `mainnet.block-engine.jito.wtf/api/v1/transactions`. The port defaults to 443
  and `server_name` is taken from the host.
- `TXSENDER_KEYPAIR` — path to a Solana `id.json` (64-byte JSON array) or, if it
  is not a readable file, a base58-encoded 64-byte secret key.
- `TXSENDER_TIP_ACCOUNT` — base58 pubkey of the provider's tip account. The
  transaction includes a system transfer to this account; that tip is what pays
  the SWQoS provider.
- `TXSENDER_TIP_LAMPORTS` — tip amount in lamports.

Optional:

- `TXSENDER_AUTH` — HTTP auth header as `"Name: value"`.
- `TXSENDER_CU_PRICE` — compute unit price in micro-lamports (default `100000`).
- `TXSENDER_CU_LIMIT` — compute unit limit (default `100000`).
- `TXSENDER_BLOCKHASH` — base58 recent blockhash to sign with.
- `TXSENDER_BLOCKHASH_RPC` — RPC URL used to fetch a fresh blockhash when
  `TXSENDER_BLOCKHASH` is unset.

The example wires a single provider. `PreparedTxs` holds one prebuilt
transaction per provider in `per_provider`. To fan out to multiple SWQoS
providers, add a `.provider(...)` call per endpoint on the builder and build one
transaction per provider in `PreparedTxs`, each with that provider's own tip
account.

## LibTpa backend (feature `libtpa`)

The `libtpa` feature wires the [libtpa](https://github.com/Tencent/TCPDirect)
DPDK-based userspace TCP/IP stack into the parallel engine as
`Parallel::libtpa()` (`Engine::ParTpa`). It is **off by default** and is only
intended for deployments with the required hardware and toolchain.

### Build requirements

- `tpa.h` reachable for the FFI bindings.
- A built `libtpa.a` (or shared `libtpa`). Point the build at it with the
  `LIBTPA_PATH` environment variable; it defaults to `/home/s1ash/libtpa`.
- The build script links `tpa` plus the system libraries `numa`, `dl`,
  `pthread`, `rt`, and `m`. When `libtpa.a` is absent the build falls back to
  dynamic linking.

Compile-only checks (no link) are available via
`cargo build --lib --features libtpa` and `cargo clippy --features libtpa`.

### Runtime requirements

- DPDK v20.11.3.
- Hugepages configured (for example via `nr_hugepages`).
- A Mellanox NIC with flow bifurcation.
- A `tpa.cfg` describing the NIC binding.
- `CAP_SYS_NICE` and `CAP_IPC_LOCK` capabilities.
- One process per NIC binding. This is enforced at runtime by an exclusive
  `flock` on `/tmp/tx-sender-libtpa.lock`; a second process fails to start.

### Known limitations

1. **Worker-bound sockets.** A libtpa socket may only be used on the worker
   thread that initialized the worker. Connections are therefore established
   inside each worker thread rather than ahead of time on the build thread.
2. **Reconnect rebuild deferred.** Automatic reconnect-rebuild is not yet
   supported for libtpa. Failover to the standby connection works, but the
   reconnect thread skips `libtpa` providers because rebuilding a connection
   requires the worker thread. In-worker reconnect is a follow-up.
3. **QUIC is not kernel-bypassed.** QUIC providers used under
   `Parallel::libtpa()` run on the standard socket-based QUIC engine with their
   own UDP socket; they do not traverse the libtpa UDP path.
4. **Not exercised in CI.** Both the link step and the runtime path require the
   built library and the hardware above, so they are not covered by continuous
   integration. The `libtpa_ffi` symbol/link test and the ignored
   `libtpa_dpdk` integration scaffold only run where libtpa links and the
   hardware is present.
