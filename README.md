# tx-sender

A low-latency transaction sender library built around pluggable transport
engines. Providers are configured over HTTP or QUIC, and transports range from
the default raw `libc` socket backend to optional kernel-bypass paths.

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
