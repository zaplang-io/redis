# Connection and command deadlines

Requires the pinned Zap toolchain containing [core #575](https://github.com/zaplang-io/zap/pull/575).
The earlier cancellation audit is retained in Git history; its unsupported-runtime
probe is no longer a CI gate. CI now runs executable library deadline regressions.

```zap
auto cfg = redis.options(
    host="cache.example",
    resolved_ip="192.0.2.10",
    connect_timeout_ms=1000,
    command_timeout_ms=500,
    secure=True,
)
auto conn = redis.connect_options(cfg)
try:
    conn.ping()
finally:
    conn.disconnect()
```

Both settings default to zero (disabled), reject negative values, and are copied
into the connection's reconnect policy. `connect()` accepts the same trailing
keyword arguments. URL query keys `connect_timeout_ms`, `command_timeout_ms`,
and `resolved_ip` are supported; repeated/unknown keys remain errors. Duration
arithmetic saturates at the largest monotonic deadline instead of overflowing.

## Connect and resolver scope

A positive `connect_timeout_ms` uses one absolute monotonic budget across TCP
connect, verified TLS handshake when enabled, and all AUTH, SELECT, and CLIENT
SETNAME exchanges. It raises `redis.ConnectTimeout`, including setup timeouts.
It never returns a partially initialized connection. Reconnect uses a fresh
budget and the original validated options. The command timeout does not replace
or restart the setup budget.

Bounded dialing requires a numeric IPv4/IPv6 `host` or an explicit numeric
`resolved_ip`. With `resolved_ip`, `host` remains the TLS verification/SNI name;
no DNS lookup occurs in the dial. Callers resolve names before connecting and
own that resolver's duration and address-selection policy. This API does not
perform failover or try other addresses automatically. Invalid numeric literals
are rejected by the core numeric dial and reported as connection failures.

Without a connect budget or resolved address, existing hostname connections use
the core synchronous resolver. **That DNS lookup is unbounded and may delay
scope cancellation until it returns.** Command deadlines still work after the
connection opens. No resolver worker or socket is abandoned to emulate a timeout.

## Commands, pipelines, and ownership

A positive `command_timeout_ms` starts after local encoding and exclusive
connection acquisition, immediately before network I/O. One absolute budget
covers every partial write, fragmented reply read, and all replies in a pipeline.
It is checked while consuming already-buffered replies too. It applies to raw,
typed, scripting, and blocking commands through `execute`/`execute_raw`, and to
pipeline execution. It does not bound local encoding, queue construction, or the
caller processing returned values. Idle time does not consume the next operation's
budget; successful operations clear the socket deadline before releasing ownership.

Timeout raises `redis.CommandTimeout`. A structured scope's `Cancelled` propagates
as `Cancelled`, with cleanup completed before scope exit. Native I/O releases its
scratch and partial resources; the library closes its owned socket, discards the
decoder, and marks the connection closed on any incomplete exchange. This includes
a failure before the first write, which keeps the recovery rule conservative.
A timeout does not imply that Redis did not execute the command. Never retry or
replay automatically. Explicit `reconnect()` opens and initializes a new session;
it cannot recover WATCH/MULTI/subscription state.

A complete server error remains `redis.ResponseError` (or a raw `Reply.Error`),
and leaves the connection usable. Local validation and concurrent-owner rejection
also preserve the current owner/socket. Only the owning operation closes on failure;
`disconnect()` continues to reject attempts to interrupt another owner.

## Regression coverage

- Saturated loopback connects and stalled AUTH initialization.
- Stalled and fragmented reads, backpressured writes, and whole-pipeline budgets.
- Clearing/resetting budgets, explicit reconnect, and no stale reply/replay.
- External scope cancellation of TCP and TLS reads/writes and partial TLS handshakes.
- Repeated TCP cancellation with Linux descriptor counts and peer-observed EOF.
- Verified resolved TLS connect/handshake budgets with generated certificates.
- Live pipelines and blocking-command timeout/reconnect on both pinned Redis versions.

Runtime runners retain an outer process timeout as a test-harness failsafe. It is
not the implementation of operation cancellation or the normal test completion path.
