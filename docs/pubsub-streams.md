# Pub/Sub sessions and basic Streams

## Pull-based RESP2 Pub/Sub

A subscriber owns one connection exclusively. Create a session with
`conn.pubsub()` on a standalone Connection, or `lease.pubsub()` on a dedicated
pool lease. The session keeps the ownership guard until it is disconnected,
even after its subscription count reaches zero. Ordinary commands, transactions,
reconnect, and lease release cannot share that connection while the session lives.
Use a separate connection/lease to publish.

```zap
auto lease = pool.acquire(dedicated=True)
try:
    auto sub = lease.pubsub()
    try:
        sub.subscribe([b"events"])
        match sub.next_event():
            case redis.PubSubEvent.Subscribed(topic, count):
                print(count)
            case _:
                raise ValueError("expected subscription acknowledgement")
        # A separate publisher can now publish to this acknowledged subscription.
        match sub.next_event(0):
            case redis.PubSubEvent.Message(topic, payload):
                print(hex.encode(payload))
            case _:
                raise ValueError("handle other events in the application's loop")
    finally:
        sub.disconnect()
finally:
    lease.release()
```

`subscribe(channels)` and `psubscribe(patterns)` require at least one binary name.
`unsubscribe(channels)` and `punsubscribe(patterns)` accept an empty list to
unsubscribe all names of that kind. These methods **send only**: callers consume
the acknowledgements with `next_event()`. They never hide/drop messages that
arrive between a control command and its acknowledgement. Literal and pattern
subscriptions are independent; unsubscribing both kinds can be necessary to
reach a total count of zero.

`PubSubEvent` has these variants:

| Event | Payload |
| --- | --- |
| `Subscribed` | binary topic, total count |
| `PatternSubscribed` | binary pattern, total count |
| `Unsubscribed` | Optional binary topic, total count |
| `PatternUnsubscribed` | Optional binary pattern, total count |
| `Message` | binary topic, binary payload |
| `PatternMessage` | binary pattern, actual binary topic, binary payload |

A null unsubscribe target means Redis had no subscription of that kind.
`subscription_count()` reports the most recently consumed acknowledgement's
count, not a prediction based on pending commands; it becomes zero on close.
Acknowledgements and messages are read in wire order. Overlapping channel and
pattern subscriptions may intentionally yield more than one event for a publish.
`conn.publish(topic, payload)` / `lease.publish(topic, payload)` return Redis's
integer recipient count, including zero; an ordinary lease may publish.

## Backpressure, cancellation, and shutdown

There is **no background receiver, callback dispatcher, or unbounded application
queue**. Each `next_event()` reads one frame through the bounded connection
parser. A caller that stops pulling leaves data in socket/Redis buffers; Redis's
configured output-buffer policy can disconnect a slow subscriber. No messages
are transparently replayed or subscriptions transparently restored.

One session operation runs at a time. Concurrent read/control/disconnect attempts
raise `BusyError`. To shut down a reader blocked in `next_event`, cancel and join
its structured scope, then call `disconnect()` in finally; do not free a TLS
session beneath another active reader. `disconnect()` is idempotent and always
closes/discards the connection, without waiting for unsubscribe acknowledgements.
A dedicated lease must still be released in finally so pool capacity returns.
Pool shutdown retains its existing rule: active leases finish/release themselves.

Control writes use the connection's command budget. `next_event(timeout_ms=-1)`
uses that configured budget; zero disables the event timer, and a positive value
sets a per-event millisecond budget. The control write and later acknowledgement
read are separate operations, not one combined request/reply budget. A read
without expected messages/acknowledgements can wait indefinitely when unbounded.
Timeout, cancellation, EOF, server error, or malformed event closes the session;
Cancelled remains distinct from CommandTimeout. Server-error bytes remain
available through the original Connection's last_error().

Redis Pub/Sub has **at-most-once delivery**: disconnects and processing failures
can lose messages. There is no acknowledgement/retry layer, resubscription, or
history backfill. See [Redis Pub/Sub semantics](https://redis.io/docs/latest/develop/pubsub/).

## Stream types and commands

Stream IDs stay bytes. Canonical decimal ID components are validated as unsigned
64-bit tokens by length/lexicographic bounds, never narrowed to signed int64.
Returned entries contain full `milliseconds-sequence` IDs. Redis still enforces
monotonic insertion and rejects IDs invalid for the current stream.

| Type | Fields |
| --- | --- |
| `StreamField` | `bytes field`, `bytes value` |
| `StreamEntry` | `bytes id`, `list[StreamField] fields` |
| `StreamCursor` | `bytes key`, `bytes after_id` |
| `StreamBatch` | `bytes key`, `list[StreamEntry] entries` |

Fields are ordered pairs, preserving binary/empty names and values and repeated
field names; no dict conversion occurs.

| Connection method | Result / options |
| --- | --- |
| `xadd(key, fields, id=b"*")` | Full ID bytes. `*` generates an ID; explicit IDs must be canonical full tokens. At least one pair required. |
| `xlen(key)` | Non-negative length; missing stream returns zero. |
| `xrange(key, start=b"-", end=b"+", count=0)` | Entries in increasing order. Zero omits COUNT. |
| `xrevrange(key, end=b"+", start=b"-", count=0)` | Entries in decreasing order; end precedes start in the API/wire. |
| `xdel(key, ids)` | Number deleted. At least one full canonical ID required. |
| `xread(streams, count=0, block_ms=-1)` | Ordered StreamBatch list for matching requested streams; null/missing/timeout becomes an empty list. |

Range boundaries accept `-`, `+`, canonical full IDs, or a canonical unsigned
millisecond component. Prefix an ID with `(` for an exclusive boundary. COUNT
must be nonnegative; zero omits it. No trimming, NOMKSTREAM, ms-* generated IDs,
Redis 8.x IDMP/MAXCOUNT/MAXSIZE, or consumer-group options are added here.

`xread` requires at least one StreamCursor. Its IDs accept full canonical tokens,
a millisecond component (`b"0"` starts at the beginning), or `b"$"` for new
entries after the command starts. Returned batches/fields retain wire order;
COUNT applies per stream. It does not maintain or advance cursors for the caller.
After an initial `$` read, use returned last-entry IDs for later reads: repeatedly
using `$` can skip entries that arrived between calls.

## Blocking XREAD

`block_ms=-1` omits BLOCK and performs a nonblocking read; zero means an indefinite
Redis-side block; positive values specify Redis's wait in milliseconds. A Redis
server timeout returns an empty batch list and preserves the connection. Client
command deadlines and structured cancellation remain separate: incomplete reads
discard the connection, and no cursor is advanced or command replayed automatically.

Standalone Connections support both modes. `lease.xread(...)` permits nonblocking
reads on ordinary leases but requires `dedicated=True` when BLOCK is present.
Ordinary lease raw commands/pipelines also reject XREAD's BLOCK option; a key
literally named BLOCK after STREAMS is not confused with that option. Dedicated
leases close on release, including after a healthy server timeout. Request/reply
parser limits and the whole-command budget apply to all Streams helpers.

Consumer groups and pending/acknowledgement management are separate (#22). This
API does not claim durable consumer processing or exactly-once effects. See
[Redis XREAD](https://redis.io/docs/latest/commands/xread/) and
[XRANGE](https://redis.io/docs/latest/commands/xrange/).

The [complete executable example](../examples/pubsub_streams.zap) runs from both
installed dependency types on both pinned Redis versions in CI. Focused fixtures
cover fragmented/coalesced events, delayed pull consumers, server disconnects,
malformed events/entries, cancellation, binary/repeated fields, uint64 IDs,
range boundaries, multi-stream reads, blocking wakeups/timeouts, and verified TLS.
