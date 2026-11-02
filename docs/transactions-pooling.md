# Transactions and connection pools

## Exclusive transaction sessions

`conn.transaction()` acquires the connection without sending a command. Every
normal connection operation is rejected with `BusyError` until this session
finishes. A session allows one method call at a time; concurrent calls are rejected.

```zap
auto tx = conn.transaction()
try:
    tx.watch([b"balance"])
    auto current = tx.execute([b"GET", b"balance"])
    # Caller computes the next value. Nothing is automatically replayed.
    tx.multi()
    auto queued = tx.queue([b"SET", b"balance", b"42"])
    match tx.exec():
        case redis.TransactionResult.Committed(replies):
            print(len(replies))
        case redis.TransactionResult.Conflict:
            print("WATCH conflict; decide whether to retry")
        case redis.TransactionResult.Aborted(error):
            print(str(error))
finally:
    tx.discard()
```

- `watch(keys)` and `unwatch()` operate before MULTI. Empty key lists are rejected.
- `execute(args)` reads or executes ordinary commands before MULTI. A complete
  server error raises `ResponseError` and leaves the session available for cleanup.
- `multi()` enters queueing mode. `queue(args)` sends a command immediately and
  returns either `Reply.Simple(b"QUEUED")` or the complete `Reply.Error` from a
  queue-time failure. A queue error is distinct from an error inside EXEC.
- `exec()` finishes the session. An array becomes `Committed(replies)`, including
  any runtime `Reply.Error` values in their original order. A null array becomes
  `Conflict`; an EXECABORT error becomes `Aborted(error)` with its original bytes.
  Unexpected reply shapes/counts invalidate the connection. Other EXEC errors
  raise `ResponseError` and conservatively close the connection.
- `discard()` is idempotent after completion. Before MULTI it sends UNWATCH;
  during MULTI it sends DISCARD. It must run in `finally` when caller code fails
  or abandons a session. It can raise a transport/timeout/cancellation error;
  its cleanup still closes uncertain connections and releases ownership.

Transaction-control and connection-mode commands must use session methods rather
than `execute`/`queue`; session-changing commands such as AUTH, SELECT, CLIENT,
HELLO, subscriptions, and MONITOR are rejected locally in these methods. Use the
raw Connection API on a separately owned connection when such control is needed.

Each network exchange uses the connection's command budget; caller computation
and the entire transaction lifetime do not share a new transaction-wide timer.
An enclosing structured scope can bound the session's I/O through cancellation.
Timeout, cancellation, EOF, or malformed framing closes/discards the connection
before releasing ownership. A finished session cannot operate on a reused socket.
EXEC may have executed before a transport failure; no outcome is automatically
retried. Redis transactions do not roll back runtime errors inside EXEC.

The existing per-reply parser limits apply to EXEC's whole reply array. Commands
are queued on Redis one at a time; there is no separate client-side transaction
queue or implicit pipeline. See the [Redis transaction specification](https://redis.io/docs/latest/develop/using-commands/transactions/).

## Bounded pools and guarded leases

```zap
auto pool = redis.pool(
    redis.options(command_timeout_ms=1000, connect_timeout_ms=1000),
    capacity=8,
    acquire_timeout_ms=500,
    reset_timeout_ms=1000,
)
try:
    auto lease = pool.acquire()
    try:
        auto reply = lease.execute([b"PING"])
    finally:
        lease.release()
finally:
    pool.shutdown()
```

The constructor validates/snapshots Options and opens no sockets. Capacity
(default 8, minimum 1) includes sockets being initialized and closed, as well as
idle and borrowed connections. Every new connection receives identical
AUTH/database/client-name and TCP/TLS configuration. `size()` includes those
reservations; a failed setup restores its capacity before returning an error.

`acquire_timeout_ms` (default 1000, zero disables) bounds **waiting for capacity**.
It does not replace the connection's connect/session budget or promise bounded
DNS. Set a positive `connect_timeout_ms` with a numeric host/resolved address to
bound dialing/setup as described in [deadlines](deadlines.md). Exhaustion raises
`PoolTimeout`; an external scope's `Cancelled` remains distinguishable.
Waiters park on broadcast notifications. A timed notification helper is always
joined, owns no connection, and never survives an acquisition returning. Wakeups
are competitive; no FIFO fairness is promised.

A lease provides `execute`, `execute_raw`, `execute_pipeline(batch)`, and
`transaction()`. It does not expose a Connection accessor. One operation owns the
lease at a time. A transaction holds that guard for its entire lifetime, so a
lease cannot be returned while WATCH/MULTI ownership is active. Finish or discard
the transaction before releasing the lease. Retained old leases cannot touch a
socket after release; double release/use raises `LeaseError`. Overlapping calls
raise `BusyError` rather than waiting. Callers still own their lease lifetime and
must release/discard in `finally`.

## Reuse and session isolation

`release()` runs [RESET](https://redis.io/docs/latest/commands/reset/) and reapplies
the original AUTH/database/client-name setup under one finite `reset_timeout_ms`
budget (default 1000, minimum 1). RESET clears MULTI/WATCH and other server-side
connection context, and returns the protocol/database to RESP2/database zero
before setup is reapplied. This requires Redis 6.2+ and permission for RESET and
the configured setup commands to **reuse** sockets. If reset/setup is denied,
times out, or fails, the socket is discarded; it is never returned with uncertain
session state. Ordinary complete command errors can still be reused after a
successful reset. Known broken/protocol-damaged connections are discarded.
The pool does not probe idle sockets or retry operations if the server closed one.

`release()` returns true when the pool accepts the clean connection for reuse,
false when it discards it. Ordinary reset errors are absorbed after discarding;
structured `Cancelled` propagates after closing and returning capacity. Use
`discard()` to skip reset and close deliberately. This makes cleanup safe even
when ordinary command budgets are disabled.

Blocking list/sorted-set and streaming/protocol commands (BLPOP/BRPOP/BLMOVE
variants, XREAD with BLOCK, SUBSCRIBE variants, MONITOR, HELLO, replication stream commands) require `acquire(dedicated=True)` and are rejected locally on
ordinary leases, including pipeline batches. Dedicated leases always close on
release and reserve capacity for their lifetime. They provide exclusive ownership.
The dedicated `pubsub()` pull session and blocking `xread()` are documented in
[Pub/Sub and Streams](pubsub-streams.md); RESP3 negotiation remains separate.
Use them for stateful sessions that must never return to the reusable pool.

`shutdown()` is idempotent: it stops admission, wakes waiters with `PoolClosed`,
and synchronously closes idle connections. Existing leases remain valid and are
closed when released; shutdown does not interrupt another owner's operation or
wait for caller-held leases. A connection whose initialization was in flight is
closed/rejected when initialization returns. `is_shutdown()` reports admission
state; `size()` reaches zero after all active reservations/leases finish.
Keep all leases inside the lifetime managed by your application.

## Verification

Offline fixtures check ownership, malformed EXEC replies, transaction cancellation,
capacity-wait timeouts, cancelled waiters, stale handles, initialization/reset failure,
leased transaction cancellation, dedicated discard, and shutdown with active leases.
Generated-certificate tests cover verified TLS transaction ownership and pool reuse.
Live tests run commit/runtime-error/queue-error/conflict/discard scenarios, reset
MULTI/WATCH/database/name state, replace broken connections, and run concurrent
borrowers against both pinned Redis versions. They never replay user operations.
