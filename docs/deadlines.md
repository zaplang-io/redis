# Deadline and cancellation capability audit

Status: Redis [#7](https://github.com/zaplang-io/redis/issues/7) is **incomplete**.
This audit adds a bounded runtime diagnostic and records prerequisites. It does
not implement a timeout option, change command behavior, or close #7.

## Observed behavior

The pinned Zap image
`sha256:77f68f492779b2d3c58cb6fa1f25363766c83b7ea0a7b5a25f7d92582bbd73d6`
was tested with a child parked in `net.recv` under `scope(timeout=100)`.
A peer released the read after one second so the diagnostic could terminate.
[The CI run](https://github.com/zaplang-io/redis/actions/runs/41307244271)
reported:

```text
requested_deadline_ms=100
scope_elapsed_ms=1006
scope_timed_out=True
peer_released=1
reader_completed=1
reader_cancelled=0
reader_cleaned=1
TCP_CANCEL=blocked-until-peer-release
```

The scope timer fired, but the socket read completed normally once the peer
replied. Its finally block ran then; cancellation did not interrupt the read.
The exact elapsed time varies by host. The event/cancellation flags carry the
useful result. An indefinitely stalled peer would prevent normal scope draining.

## Required core capabilities

| Capability | Audit result | Core tracking |
| --- | --- | --- |
| Public TCP read deadline | `net.recv` exposes no timeout/deadline | [zap#485](https://github.com/zaplang-io/zap/issues/485) |
| Cancellation of parked TCP connect/read/write | Scope cancellation registration covers channel waiters, not poll-descriptor waiters | [zap#558](https://github.com/zaplang-io/zap/issues/558) |
| TCP connect/write deadline and distinct timeout outcome | `net.connect`/`net.send` expose no deadline; socket parks pass deadline zero | [zap#559](https://github.com/zaplang-io/zap/issues/559) |

The internal poller already accepts an absolute monotonic deadline. That alone
is insufficient: the public calls must pass it, preserve one budget across
partial writes and address attempts, distinguish timeout from EOF/failure, and
release native scratch buffers and descriptors safely on cancellation.
Synchronous name resolution also needs an explicit bounding policy; a connect
budget must not silently claim to cover DNS if the resolver can outlive it.

## Library work after the prerequisites

Redis owns separate connect and total command budgets, option validation,
exception mapping, and connection state. The total command budget must cover
all socket writes and reply reads for one operation, including fragmented input;
restarting the full budget on every chunk is insufficient. Session-initialization
coverage and DNS coverage must be stated explicitly when the core API is chosen.
No unjoined timer/worker or live socket may survive an operation returning.

Timeout/cancellation after sending any command bytes invalidates the connection,
discards buffered replies, and permits only an explicit fresh reconnect. Never
replay an operation that might already have executed. Local argument failures
and rejected concurrent operations keep their existing behavior. A caller's
Cancelled exception must remain distinguishable from a Redis timeout or server
response error.

The completed feature needs stalled connect, stalled read, backpressured write,
fragmented-reply budget, AUTH/session initialization, external scope cancellation,
readiness/cancellation races, failed cleanup, and reconnect/no-stale-reply tests.
Scope completion must await cleanup; process-wide grace termination is not the
normal timeout mechanism. Keep the existing live Redis and offline suites green.

## Run the bounded diagnostic

```sh
zap run tools/audit/net-cancel-probe.zap
```

It needs no Redis service. A channel handshake confirms the loopback peer has
received the request before starting the timed scope. The peer's one-second
delay is an intentional diagnostic failsafe, not a startup/readiness sleep or a
client timeout implementation. The parent also closes its socket after joining
the scoped reader. Both supported and unsupported cancellation behavior terminate.

CI runs this probe alongside the existing offline suite and prints its result.
A green diagnostic job verifies that the probe ran and cleaned up; it does **not**
mean TCP cancellation is supported or that Redis #7 is implemented. Once core
support ships, update the pinned toolchain, require the interruptible outcome,
and replace this audit gate with the actual deadline regression suite.
