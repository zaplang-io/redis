# redis

A pure-Zap Redis client, official non-core package. Tracks
[zaplang-io/zap#322](https://github.com/zaplang-io/zap/issues/322).

The package includes a binary-safe RESP2 codec, TCP connections, typed errors,
explicit reconnect, URL configuration, verified TLS, and session initialization. Typed command
helpers are still planned.

## TCP connection

```zap
import redis

def main():
    auto conn = redis.connect(host="127.0.0.1", port=6379)
    try:
        auto reply = conn.execute([b"ECHO", bytes("hello")])
        match reply:
            case redis.Reply.Bulk(data):
                print(str(data))
            case _:
                print("unexpected reply")
    finally:
        conn.disconnect()
```

`connect()` returns an open `Connection`. `execute(list[bytes]) -> Reply` sends
one command and reads one complete RESP2 reply. Reads preserve arbitrary bytes;
fragmented replies are assembled and surplus bytes retained for the next reply.
A top-level Redis server error raises `redis.ResponseError`. Use
`execute_raw(list[bytes])` to receive `Reply.Error` values instead. Nested errors
inside arrays remain values in either API. Empty commands raise `ValueError`
before touching the socket.

Use `disconnect()` to close the connection (Zap reserves the word `close`). It
is idempotent when idle or already closed. `is_closed()` reports whether it has
been closed. One active operation owns the connection: overlapping `execute` or
`disconnect`/`reconnect` calls raise `redis.BusyError` instead of queuing or
sharing replies.
The connection fields and direct construction are internal; use `connect()`.

Transport failures, EOF before a complete reply, and invalid framing close the
connection and discard buffered bytes. Commands are never automatically retried.
A failed operation might already have executed on the server. Pooling and
configurable deadlines have separate backlog issues. Use a dedicated connection for stateful
commands and manage its lifetime with `try/finally`.

`max_frame` (default 16 MiB) and `max_depth` (default 64) apply to each incoming
reply. The receive buffer is bounded by `max_frame`; outgoing command size is
not limited by this option. `read_size` (default 4096, valid range 1–65536) controls
read chunk size. Large values work across multiple reads. Host, port, and limits
are validated before connecting.

## Connection options and URLs

```zap
# Direct options; nonempty credentials enable authentication automatically.
auto conn = redis.connect(host="localhost", port=6379, database=2,
    username="alice", password="secret", client_name="worker-1")

# Options can be parsed/validated without opening a socket.
auto cfg = redis.parse_url("redis://alice:p%40ss@localhost:6379/1?db=2&client_name=worker-1")
print(redis.describe_options(cfg))  # redis://***@localhost:6379/2
# Close each live connection with disconnect() in a try/finally.
```

`redis.options(...) -> Options` provides host, port, database, username, password,
client_name, authenticate, max_frame, max_depth, read_size, and secure. Use
`redis.connect_options(cfg)` to connect; it takes a validated snapshot, so later
mutations of `cfg` cannot change reconnect behavior. The existing first five
positional arguments to `connect()` (host, port, max_frame, max_depth, read_size)
remain compatible; session options are trailing keyword arguments.

`connect_url(address)` connects using `parse_url(address)`. The supported form is
`redis://[[username]:password@]host[:port][/db][?db=N&client_name=NAME]`.
Defaults are 127.0.0.1:6379, database 0, and no authentication. IPv6 addresses use
brackets, for example `redis://[::1]:6379/2`. Credentials use percent-encoding;
`+` is literal, not a space. Decoded text must be valid UTF-8. Password-only URLs
use `redis://:password@host`; `redis://:@host` explicitly sends an empty password.
Direct options can request this with `authenticate=True`.

URL parsing uses Zap's `=~`/`!~` regex operators and named capture groups. The
query `db` takes precedence over the path database; all supplied values are
validated. There is no implicit merge with a second options object: parse the
URL, edit the returned options explicitly if needed, then call connect_options.
Unknown/duplicate query keys, fragments, malformed escapes, invalid ports or
databases, and unescaped whitespace/control characters are rejected. `rediss://` enables verified TLS with the same parsing and session options. Client names must contain printable ASCII
without spaces. Databases must be nonnegative int32 values; the server validates
that the configured database actually exists.

A new session sends AUTH (password-only or ACL username/password), SELECT for a
nonzero database, then optional CLIENT SETNAME. The same sequence runs on every
explicit reconnect, under exclusive ownership. Every step requires a simple
`OK`; any server, framing, or I/O failure closes the new socket. Initialization
error messages identify only the failed stage; server replies that could echo
credentials are not retained in last_error(). General command error handling
continues to preserve raw server messages as documented below.

Use `describe_options()` for diagnostics; it omits both credentials. Avoid logging
the input URL, password field, or AUTH command arguments yourself. Offline session tests use deterministic loopback protocol fixtures. The live
Redis harness also checks ACL authentication, selected database isolation, client
names, and reconnect against real servers.

## Verified TLS

```zap
auto conn = redis.connect_url("rediss://redis.example.com:6380/0")
try:
    auto reply = conn.execute([b"PING"])
finally:
    conn.disconnect()
```

Use `rediss://` URLs or `secure=True` in direct options/connect calls. Plain
`redis://` and existing calls remain TCP. `describe_options()` preserves the
scheme while redacting credentials. Reconnect retains the selected transport
and repeats AUTH/SELECT/client-name setup over it. Binary payloads, parser
limits, ownership rejection, server errors, EOF handling, and disconnect follow
the same connection contract for both transports.

TLS calls Zap's verified `tls.connect`: certificate-chain and DNS hostname
verification are always on. Redis exposes no insecure/skip-verification option
and never retries a failed TLS connection as plaintext. TLS failures raise
`redis.ConnectionError`; protocol errors still raise `redis.ProtocolError`.
Use the certificate's DNS hostname for the connection endpoint. Numeric-IP-only
certificate identities are not part of this slice's compatibility guarantee:
the current stdlib configures OpenSSL's DNS hostname checker.

Trust comes from OpenSSL's default trust paths. For a private CA, set
`SSL_CERT_FILE` (PEM CA bundle) and/or `SSL_CERT_DIR` (hashed CA directory) before
starting the process or opening connections. These are process-wide trust
settings, as described by [OpenSSL's default verify paths](https://docs.openssl.org/3.0/man3/SSL_CTX_load_verify_locations/).
There are no per-connection CA settings or client-certificate/private-key
options in the current Zap TLS client API; mutual TLS is not supported by this
Redis slice. Do not change global trust settings around concurrent connections
to imitate isolated CA configuration. Core support is tracked in [zap#560](https://github.com/zaplang-io/zap/issues/560).

TLS handshakes and reads/writes currently share the runtime deadline/cancellation
limitations documented for #7. This feature does not add bounded handshakes or
command deadlines.

### TLS tests

```sh
ZAP=/path/to/zap tools/run-tls-tests.sh
```

The runner requires the OpenSSL 3 command-line tool, generates an ephemeral test
CA and server keys in a temporary directory, and removes them after the suite.
No certificate private keys are checked into the repository. It runs verified
loopback TLS command/session tests with 64 KiB binary data and reconnect, EOF and
malformed-frame cleanup, rejection of an untrusted chain, and rejection of a
trusted certificate for the wrong hostname. Linux CI also checks descriptor
counts across repeated failed handshakes. It needs no external Redis server.
`tools/run-tls-tests.sh --check` only checks the frontend and needs no certificates.
Runtime suites require GNU `timeout` (`coreutils` on macOS supplies `gtimeout`)
and fail after 60 seconds, with forced termination five seconds later.

## Errors and explicit reconnect

- `redis.ResponseError`: a complete top-level server error. Its UTF-8 message is
  preserved verbatim, including its prefix (WRONGTYPE, NOAUTH, ERR, or an unknown
  future prefix). The connection remains usable.
- `redis.ProtocolError`: invalid or oversized RESP framing. The connection is
  closed and its reply buffer discarded.
- `redis.ConnectionError`: connect/send/receive failure, EOF before a complete
  reply, or use after close. The connection is closed after I/O failures.
- `redis.BusyError`: another operation owns the connection. The owner proceeds
  unchanged; the rejected operation does not touch its socket or buffer.
- `ValueError`: invalid local arguments. The existing connection is unchanged.

Zap's `except Error as message` binds a string. `conn.last_error()` retains the
exact bytes of the most recently consumed top-level server error, including
non-UTF-8 payloads. Success preserves it; another server error replaces it and
reconnect clears it. `redis.error_prefix(raw)` returns the bytes before the
first ASCII space. Non-UTF-8 server errors use a lossless hex exception message.
Use `execute_raw()` when response errors should be handled as values directly.

```zap
try:
    conn.execute([b"GET", b"a-list-key"])
except redis.ResponseError as message:
    print(message)
    print(str(redis.error_prefix(conn.last_error())))
except redis.ConnectionError as message:
    print(message)
    conn.reconnect()
    # Start a new operation explicitly. The failed command is never replayed.
```

`conn.reconnect()` closes the old socket, discards all old-session reply bytes,
and connects again with the original host, port, and parser limits. It works on
an idle or closed connection. A failed reconnect leaves the connection closed;
a later explicit reconnect can try again. It cannot run during another operation.
Reconnecting creates a fresh Redis session and reapplies the configured AUTH,
SELECT, and client name. WATCH/MULTI state, subscriptions, and other session state
are not restored. Callers must decide how to recover after an ambiguously executed
operation. Cancellation that reaches the operation propagates and closes the
socket during cleanup. The current runtime does not wake a TCP read merely
because its enclosing scope was cancelled; see the deadline audit below.

## Codec

```zap
import redis

def main():
    auto command = redis.encode_command([b"GET", bytes("my-key")])
    auto reply = redis.decode(b"$5\r\nhello\r\n")
    if reply.status == "ok":
        match reply.value:
            case redis.Reply.Bulk(data):
                print(str(data))
            case _:
                print("unexpected reply")
```

`encode_command(list[bytes]) -> bytes` encodes a command and arguments as an
array of bulk strings. Empty commands raise `ValueError`. Arguments preserve
arbitrary bytes, including NUL, CRLF, and invalid UTF-8. Use `bytes(text)` to
encode text explicitly.

`decode(bytes, max_frame=16777216, max_depth=64) -> Decoded` decodes one reply:

- `status == "ok"`: `value` is a `Reply`; `consumed` is the number of bytes to
  remove from the front of the buffer. Any following replies remain untouched.
- `status == "incomplete"`: retain the entire buffer and append the next chunk.
- `status == "invalid"`: `message` explains the protocol or limit error. Stop
  parsing this stream. There is no automatic resynchronization.

Incomplete and invalid results have `consumed == 0`; their `value` is a
placeholder and must not be inspected. A Redis error is a successfully decoded
`Reply.Error`, distinct from an invalid wire frame. The future client layer will
translate server errors into exceptions.

`Reply` variants are `Simple(bytes)`, `Error(bytes)`, `Integer(int64)`,
`Bulk(bytes)`, `Array(list[Reply])`, `NullBulk`, and `NullArray`. Empty values
remain distinct from nulls, and nested errors remain values within arrays.
Payloads never undergo implicit UTF-8 decoding.

The byte limit includes all headers and payloads in one frame, rather than the
whole input buffer. Depth counts non-null arrays, including empty arrays.
Oversized declared lengths are rejected before allocation. This stateless parser
reparses an incomplete frame on each call; callers should bound buffering and
batch reads. It does not provide a transport timeout or total allocation budget.
RESP3, inline commands, and streaming aggregates are outside this slice.

Framing follows the [Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).

## Build and test

Requires a Zap toolchain with LLVM code generation and the `bytes` type.

```sh
ZAP=/path/to/zap tools/run-tests.sh
```

Or run with the published toolchain image:

```sh
docker run --rm -v "$PWD":/work -w /work ghcr.io/zaplang-io/zap:latest \
  sh tools/run-tests.sh
```

A frontend-only installation can check parsing, types, and IR lowering with
`ZAP=/path/to/zap tools/run-tests.sh --check`; this does not execute tests.
The suite uses real module imports and needs no Redis server or Python runtime.
Transport and error tests start loopback fake servers and cover one-byte reads, 64 KiB
binary commands/replies, coalesced frames, EOF, malformed/oversized replies,
connection reuse, exclusive ownership, and cleanup. Zap net.send currently
handles partial OS writes internally; the client also checks write progress.

## Development

Use medium-sized individual issues. Issues for this library belong in
`zaplang-io/redis`. Follow-up slices: TCP connection lifecycle and incremental
buffering; command helpers and integration tests against Redis. Keep this codec
independent of transport and command policy.

## Live integration tests

The declared minimum Redis server branch is **7.4**. CI tests the pinned patch
versions **7.4.12** and **8.10.3**, alongside an independent offline job. Redis
7.4 is an extended release and 8.10 is a current GA branch in the
[Redis version policy](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/).
Older server branches and Redis-compatible alternatives are currently untested.
The server images, Zap toolchain, and checkout action are pinned by digest/SHA in
`.github/workflows/test.yml`; update the server version and image digest together.
The live suites verify the actual server version when CI supplies the expected
patch version. CI runs once per pull request and on pushes to main only.

The existing `tools/run-tests.sh` remains offline and does not start or connect
to Redis. To run the live basic suite, supply an explicit endpoint:

```sh
REDIS_TEST_URL=redis://127.0.0.1:6379/0 ZAP=/path/to/zap \
  tools/run-integration-tests.sh
```

There is no implicit localhost connection. The endpoint must permit PING, INFO,
ECHO, GET/SET/DEL, LPUSH (for WRONGTYPE checks), and CLIENT ID. Credentials can be
supplied in the URL using the usual percent-encoding rules. The runner does not
print the URL. Basic tests use unique random key prefixes, delete only their
registered keys in finally cleanup, and set a 60-second fallback TTL. They do
not flush or enumerate the database.

For a **disposable, isolated** instance with administrative ACL permissions and
at least two databases, also run the session suite:

```sh
REDIS_TEST_URL=redis://127.0.0.1:6379/0 ZAP=/path/to/zap \
  tools/run-integration-tests.sh --isolated
```

That explicit mode tests database isolation and reconnect, creates a randomly
named ACL user limited to the test namespace and required commands, checks good
and bad authentication and client-name restoration, and removes the user and
keys during cleanup. It never changes the default user or global server config.
CI supplies a fresh service container per matrix job. Readiness is gated by
`redis-cli ping` health checks; tests have no startup sleeps. Destroying the
service container also removes any state left after a forcibly terminated job.

`tools/run-integration-tests.sh --check` validates both live suites without a
server or endpoint. Reusable setup/assertion/cleanup helpers live in
`tests/integration/integration_fixtures.zap`; add future live suites under that
directory with a matching `.expected` output file. Any suite that mutates ACLs
or otherwise needs administrative isolation must explicitly require isolated
mode and be selected accordingly in the runner.

## Deadline support status

Connection and total command deadlines ([#7](https://github.com/zaplang-io/redis/issues/7))
are blocked by missing core socket primitives. No timeout options are currently
exposed. A scope timeout alone does not interrupt the pinned runtime's parked
TCP read: the bounded audit requested 100 ms but the scope drained only after
the peer replied roughly one second later. Normal failure/EOF cleanup still
works; it cannot substitute for interruptible I/O.

See [the deadline capability audit](docs/deadlines.md) for the reproducer,
upstream prerequisites, and the acceptance checks required before #7 can close.

## Typed key and string commands

These `Connection` methods use RESP2 and accept keys/values as `bytes`:

| Method | Result |
| --- | --- |
| `ping()` | `bool`, true for PONG |
| `get(key)` | `Optional[bytes]`; None for missing, Some(b"") for empty |
| `set_value(key, value, nx=False, xx=False, ex=0, px=0)` | `bool`: written or condition not met |
| `mget(keys)` | `list[Optional[bytes]]` in input order, including duplicates |
| `mset(pairs)` | `bool`, true for OK; pairs are `redis.KeyValue(key, value)` |
| `delete(keys)`, `unlink(keys)` | `int64` number removed |
| `exists(keys)` | `int64` count; duplicate existing keys count repeatedly |
| `key_type(key)` | `bytes` status, e.g. b"string" or b"none" |
| `incr(key)`, `decr(key)` | `int64` new value |
| `incrby(key, amount)`, `decrby(key, amount)` | `int64` new value; signed amounts allowed |
| `expire(key, seconds)`, `pexpire(key, milliseconds)` | `bool`, whether expiration was applied |
| `ttl(key)`, `pttl(key)` | `int64` remaining seconds/milliseconds, -1 for no expiry, -2 for missing |
| `persist(key)` | `bool`, whether an expiration was removed |

Zap reserves `set`, so SET uses `set_value`; DEL uses `delete` and TYPE uses
`key_type`. SET supports only NX/XX and EX/PX in this slice. Expiry arguments are `int64` durations, with zero meaning omitted, for example:

```zap
auto lifetime = 60
auto written = conn.set_value(b"key", b"value", nx=True, ex=lifetime)
auto result = conn.get(b"key")
match result:
    case Optional.Some(data):
        print(str(data))  # explicit UTF-8 conversion; may raise on binary data
    case _:
        print("missing")
```

NX and XX conflict, EX and PX conflict, and negative SET expiry durations are rejected. Zero omits that expiry option;
a positive value sends EX or PX.
Invalid combinations and empty multi-key/pair lists raise `ValueError` before
network I/O. EXPIRE/PEXPIRE retain Redis semantics: zero or negative durations
delete existing keys immediately. SET GET, KEEPTTL, absolute expirations, and
conditional EXPIRE flags remain available through `execute`.

MGET returns None for both absent and non-string keys, as Redis specifies;
GET on a non-string key raises `ResponseError`. MSET accepts a nonempty ordered
list of pairs and follows Redis overwrite/expiry semantics. Counter overflow
and non-integer values raise `ResponseError` without replaying the command.
No helper implicitly converts payload bytes to text.

Helpers validate reply shapes and raise `ProtocolError` for unexpected types,
status values, MGET lengths, or non-boolean integer flags. A complete valid
RESP frame has already been consumed in these cases; the connection remains
usable. Transport/framing failures still invalidate the connection as described
above. Helpers inherit the connection's single-owner contract.

References: [SET](https://redis.io/docs/latest/commands/set/),
[MGET](https://redis.io/docs/latest/commands/mget/),
[TTL](https://redis.io/docs/latest/commands/ttl/).

## Typed hash commands

Hash helpers accept binary keys, fields, and values. `redis.HashField(field,
value)` represents a field/value pair without text conversion.

| Connection method | Result |
| --- | --- |
| `hset(key, fields)` | `int64` number of newly added fields; updates do not count |
| `hget(key, field)` | `Optional[bytes]`; None for missing key/field, Some(b"") for empty |
| `hmget(key, fields)` | `list[Optional[bytes]]` in requested order, including duplicates |
| `hgetall(key)` | `list[redis.HashField]`, retaining field/value associations |
| `hdel(key, fields)` | `int64` number of fields removed |
| `hexists(key, field)` | `bool`, whether the field exists |
| `hlen(key)` | `int64` field count; zero for a missing hash |
| `hincrby(key, field, amount)` | `int64` new field value; signed increments allowed |

```zap
conn.hset(b"profile", [redis.HashField(b"name", b"Ada"), redis.HashField(b"note", b"")])
auto values = conn.hmget(b"profile", [b"note", b"missing", b"name"])
for entry in conn.hgetall(b"profile"):
    print(str(entry.field))  # explicit text conversion when the data is UTF-8
```

HSET accepts a nonempty ordered list of pairs, including repeated fields;
Redis applies the assignments in order. HMGET and HDEL require nonempty field
lists. Empty requests raise `ValueError` before network I/O. Empty byte fields
and values are valid. Missing hashes yield an empty HGETALL list; Redis removes
a hash when its last field is deleted. HGETALL ordering is unspecified, so
callers should locate fields by their bytes rather than list position.

HINCRBY creates missing hashes/fields with an initial value of zero before
applying the increment. Invalid integers, overflow, and operations on keys of
the wrong type raise `ResponseError` and retain the server error bytes through
`last_error()`. Commands are not retried automatically.

HGETALL requires a non-null RESP2 array containing an even number of non-null
bulk strings. Odd lengths, nulls, and incorrectly typed fields/values raise
`ProtocolError`. HMGET also checks result count and element types. These complete
frames have already been consumed, so the connection remains usable under the
same typed-reply validation contract as the key/string helpers. Transport and
RESP framing errors still invalidate it. HSCAN and hash field expiration are
outside this slice; raw `execute` remains available.

References: [HSET](https://redis.io/docs/latest/commands/hset/),
[HMGET](https://redis.io/docs/latest/commands/hmget/),
[HGETALL](https://redis.io/docs/latest/commands/hgetall/).

## Non-blocking list commands

These `Connection` helpers accept binary keys and elements:

| Method | Result |
| --- | --- |
| `lpush(key, values)`, `rpush(key, values)` | `int64` new list length |
| `lpop(key)`, `rpop(key)` | `Optional[bytes]` single head/tail element |
| `llen(key)` | `int64` length, zero for a missing list |
| `lrange(key, start, stop)` | `list[bytes]` in list order |
| `lindex(key, index)` | `Optional[bytes]` element, None for missing/out-of-range |
| `lset(key, index, value)` | `bool`, true for OK; missing/out-of-range raises |
| `ltrim(key, start, stop)` | `bool`, true for OK, including a missing list |

```zap
conn.rpush(b"queue", [b"first", b"second"])
conn.lpush(b"queue", [b"a", b"b"])
# The list is now [b"b", b"a", b"first", b"second"].
auto last = conn.lindex(b"queue", -1)
auto tail = conn.lrange(b"queue", -2, -1)
conn.ltrim(b"queue", 0, 1)  # retain the first two elements
```

Pushes require a nonempty `list[bytes]`; an empty list raises `ValueError`
before network I/O. LPUSH inserts each supplied element at the head, so their
order is reversed in the resulting list. RPUSH appends in supplied order.
Empty byte elements are valid and pop/index results distinguish Some(b"")
from None. Pops use only the single-element form; no count argument is sent.

Indexes are signed `int64`, zero-based from the head; -1 is the last element.
LRANGE and LTRIM have **inclusive** stop indexes. Bounds are passed directly to
Redis, which clips ranges to the available elements. An empty selected range
returns an empty LRANGE result or removes the list with LTRIM. The final pop
also removes the list key. LSET errors for missing keys or out-of-range indexes
remain `ResponseError`; all helpers preserve WRONGTYPE errors and server bytes
in `last_error()`.

LRANGE validates a non-null array of non-null bulk elements. Unexpected typed
replies raise `ProtocolError` after consuming the complete frame, following the
same connection recovery contract as other helpers. No command is replayed.
Blocking list operations are tracked separately in #15, and depend on core
cancellation/deadline support. This slice adds no blocking command or multi-pop
variant; raw `execute` remains available for unsupported commands.

References: [LPUSH](https://redis.io/docs/latest/commands/lpush/),
[LRANGE](https://redis.io/docs/latest/commands/lrange/),
[LSET](https://redis.io/docs/latest/commands/lset/).

## Set commands

Set helpers use binary keys/members and retain Redis deduplication semantics:

| Connection method | Result |
| --- | --- |
| `sadd(key, members)` | `int64` number of newly added members |
| `srem(key, members)` | `int64` number of members removed |
| `sismember(key, member)` | `bool`, whether the member exists |
| `scard(key)` | `int64` cardinality, zero for a missing set |
| `smembers(key)` | `list[bytes]` members, in unspecified order |
| `spop(key)` | `Optional[bytes]` one removed member, None for a missing/empty set |
| `sunion(keys)` | `list[bytes]` union of the sets |
| `sinter(keys)` | `list[bytes]` intersection of the sets |
| `sdiff(keys)` | `list[bytes]` first set minus every following set |

```zap
conn.sadd(b"first", [b"shared", b"first-only", b"shared"])
conn.sadd(b"second", [b"shared", b"second-only"])
auto common = conn.sinter([b"first", b"second"])
auto only_first = conn.sdiff([b"first", b"second"])
```

All array results are unordered; compare membership rather than list positions.
SADD/SREM require a nonempty member list, and set algebra requires at least one
key. Empty argument lists raise `ValueError` before network I/O. Duplicate
members are sent unchanged and Redis counts actual additions/removals. Empty
bytes are valid members and remain distinct from a nil SPOP result.

Missing keys act as empty sets. SDIFF respects input key order; the other
algebra operations do not promise an output order. These helpers read their
operands without storing a destination set. Removing/popping the final member
removes the key. SPOP uses only the single-member form with no count argument;
its result is chosen by Redis, with no promise of a particular random member.

Wrong key types raise `ResponseError`, preserving the raw server error through
`last_error()`. Array replies must be non-null arrays of non-null bulk strings;
unexpected types raise `ProtocolError` after consuming the complete frame.
The existing connection recovery and no-replay contracts apply. SSCAN belongs
to #14; count-based SPOP and STORE variants remain available via raw `execute`.

References: [SADD](https://redis.io/docs/latest/commands/sadd/),
[SPOP](https://redis.io/docs/latest/commands/spop/),
[SDIFF](https://redis.io/docs/latest/commands/sdiff/).

## Sorted-set commands

`redis.ScoredMember(member, score)` pairs binary member bytes with a `float64`
score. These `Connection` helpers use basic ZADD and rank-based ZRANGE forms:

| Method | Result |
| --- | --- |
| `zadd(key, members)` | `int64` newly added members; score updates do not count |
| `zrem(key, members)` | `int64` members removed |
| `zcard(key)` | `int64` cardinality, zero for a missing set |
| `zscore(key, member)` | `Optional[float64]`, None for a missing key/member |
| `zincrby(key, member, amount)` | `float64` new score; creates missing key/member |
| `zrank(key, member)`, `zrevrank(key, member)` | `Optional[int64]` zero-based forward/reverse rank |
| `zrange(key, start, stop)` | `list[bytes]` in ascending score/rank order |
| `zrange_withscores(key, start, stop)` | `list[redis.ScoredMember]`, using WITHSCORES |

```zap
conn.zadd(b"scores", [redis.ScoredMember(b"Ada", 1.5), redis.ScoredMember(b"Lin", 2.0)])
conn.zincrby(b"scores", b"Ada", 1.0)
auto rank = conn.zrank(b"scores", b"Ada")
for entry in conn.zrange_withscores(b"scores", 0, -1):
    print(str(entry.member))  # explicit text conversion for known UTF-8 data
    print(entry.score)
```

ZADD and ZREM require nonempty lists; empty lists raise `ValueError` before
network I/O. Duplicate ZADD members are sent in order and the final score wins.
Scores use `format(score, ".17g")` for float64 round trips, rather than the
shorter display precision of `str(float)`. Positive/negative infinity are
supported as `math.inf`/`-math.inf` and sent as `inf`/`-inf`. NaN inputs raise
`ValueError` locally, including a NaN later in a multi-member request. A valid
increment whose result would be NaN is rejected by Redis as `ResponseError`.

Score replies accept decimal/exponent notation and explicit `inf`, `+inf`, or
`-inf`. Invalid UTF-8, whitespace, hexadecimal forms, NaN, trailing data,
overflowing decimal values, and nonzero values that underflow to zero raise
`ProtocolError`. Finite scores retain float64 precision, including subnormals;
zero is a present score, distinct from None.

Ranges use signed rank bounds with an **inclusive** stop. Negative ranks count
from the end (-1 is the last member), and Redis clips out-of-range bounds.
Equal scores use binary lexicographic member ordering; reverse ranks reverse
that order too. Missing members have no rank/score, while missing or empty
ranges return empty lists. Removing the final member removes the key.

WITHSCORES replies must contain an even-length non-null array of alternating
non-null bulk member/score strings. Helpers validate malformed shapes and
negative ranks, and use the existing complete-frame reply recovery contract.
Wrong key types remain `ResponseError` with raw bytes in `last_error()`.
Commands are never replayed automatically.

This slice does not expose ZADD NX/XX/GT/LT/CH/INCR options or ZRANGE
BYSCORE/BYLEX/REV/LIMIT options. ZRANK WITHSCORE and ZSCAN are also outside this
API; use raw `execute` for other forms. Cursor iteration is tracked in #14.

References: [ZADD](https://redis.io/docs/latest/commands/zadd/),
[ZRANGE](https://redis.io/docs/latest/commands/zrange/).
