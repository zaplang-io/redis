# redis

A pure-Zap Redis client, official non-core package. Tracks
[zaplang-io/zap#322](https://github.com/zaplang-io/zap/issues/322).

The package includes a binary-safe RESP2 codec, TCP connections, typed errors,
explicit reconnect, URL configuration, verified TLS, and session initialization. Typed command
helpers are still planned.

## Installation and support

Version 0.1.0 is an **unreleased candidate**. Use a reviewed checkout with a Zap
path dependency (`zap add redis --path ../redis`, then `zap zenv create --no-venv`)
and activate that environment before `import redis`. Do not copy the library
module beside your application. CI verifies external path and static-registry
consumers and executes the [complete examples](examples).

See [installation and release checks](docs/RELEASING.md), the exact
[Zap/Redis support matrix](docs/SUPPORT.md), and [changelog](CHANGELOG.md).
No public tag or hosted-registry publication is implied by the manifest version.

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
A failed operation might already have executed on the server. Pooling and configurable deadlines are described below. Use a dedicated connection for stateful commands and manage its lifetime with `try/finally`.

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

TLS supports the connect and command budgets described below. For a bounded
hostname connection, supply `resolved_ip` while retaining the certificate hostname
in `host`; verification and SNI continue to use that hostname.

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
socket during cleanup. The pinned runtime wakes parked TCP/TLS operations on
scope cancellation; synchronous hostname resolution remains unbounded.

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

Use medium-sized issues in `zaplang-io/redis` and group two compatible issues
per implementation PR. Notify the reviewer when ready; merge only after that
specific PR is reviewed/approved. Keep CI on PRs and main pushes. The remaining
optional roadmap is listed in [SUPPORT.md](docs/SUPPORT.md).

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
ECHO, GET/SET/DEL, LPUSH (for WRONGTYPE checks), BLPOP (for timeout checks),
and CLIENT ID. Credentials can be
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

## Deadlines and cancellation

`connect()`, `options()`, and `connect_options()` support `connect_timeout_ms`
and `command_timeout_ms` (zero disables; negative values are rejected). A connect
budget includes TCP, verified TLS handshake, and AUTH/SELECT/CLIENT SETNAME.
Bounded connects require a numeric `host` or `resolved_ip`; with a resolved address,
`host` remains the TLS certificate/SNI name. DNS is a caller-owned prerequisite.
The existing unbounded hostname path remains available when the connect budget
is zero and no resolved address is supplied.

The command budget covers the full write and reply sequence, including fragmented
reads and every reply of a pipeline. It starts after local encoding/acquiring the
connection, resets for each operation, and does not run while the connection is idle.
Timeouts raise `redis.ConnectTimeout` or `redis.CommandTimeout`; scope cancellation
propagates as `Cancelled`. Incomplete exchanges close/discard the connection before
returning. Reconnect explicitly, and never automatically replay ambiguous operations.

```zap
auto conn = redis.connect(connect_timeout_ms=1000, command_timeout_ms=500)
try:
    conn.ping()
finally:
    conn.disconnect()
```

URL queries accept `connect_timeout_ms`, `command_timeout_ms`, and `resolved_ip`.
For example: `rediss://localhost:6379?resolved_ip=127.0.0.1&connect_timeout_ms=1000`.
See [deadline semantics and regression coverage](docs/deadlines.md), including the
unbounded synchronous DNS limitation and ownership rules.

## Bounded pipelines

```zap
auto batch = redis.pipeline(max_commands=128, max_bytes=1048576)
batch.queue([b"SET", b"key", b"value"])
batch.queue([b"GET", b"key"])
auto replies = batch.execute(conn)
```

Queueing snapshots each command's encoded bytes. Limits count the complete RESP
request, including framing, and are checked before encoding/appending that command.
Defaults are 1024 commands and 16 MiB of encoded requests. Invalid/over-limit queue
attempts leave previously queued commands intact and perform no network I/O.

Execution sends the batch together, consumes exactly one reply per command in
order, and returns `list[redis.Reply]`. Server errors remain `Reply.Error` values,
including a middle-command error followed by success; `last_error()` retains the
last consumed top-level error. All replies are drained before reuse. An empty batch
returns an empty list without sending. Every reply retains the connection's
`max_frame`/`max_depth` limits; the command count also bounds the result list.
The request-byte limit is not an aggregate response-byte limit.

The builder is for one caller and seals on the first execution attempt, including
failure or `BusyError`; it cannot be queued or executed again. Transport, protocol,
timeout, or cancellation failure returns no partial result and discards the
connection. One command budget covers the entire batch. This is non-transactional
pipelining: commands can partially execute and are not automatically retried.
Use an exclusive transaction session or a dedicated lease for stateful commands.

## Transactions and pooling

`conn.transaction()` provides an exclusive WATCH/MULTI session. Read application
state with `tx.execute()` before `tx.multi()`, send commands with `tx.queue()`,
and finish with `tx.exec()`. Results distinguish committed replies, WATCH
conflicts, and EXECABORT; runtime errors remain values inside committed replies.
Always call `tx.discard()` in `finally` to clean up abandoned sessions. No
transaction or caller side effect is automatically replayed.

`redis.pool(config, capacity=8, acquire_timeout_ms=1000, reset_timeout_ms=1000)`
creates a lazy bounded pool. `pool.acquire()` returns a guarded lease with
`execute`, `execute_raw`, `execute_pipeline`, and `transaction` methods. Release
it in `finally`; release resets/reinitializes the session or discards it. A
transaction prevents lease release until it finishes. Dedicated leases always
close on release, and retained old handles cannot use a released connection.
`pool.shutdown()` stops admission and closes idle sockets; active leases close
when returned. Acquisition timeout bounds capacity waiting; connect options
separately govern setup and DNS scope.

See [transactions, lease ownership, session reset, and shutdown semantics](docs/transactions-pooling.md).

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
Blocking list helpers are described below. Multi-pop variants remain available
through raw commands on a standalone connection or dedicated lease.

References: [LPUSH](https://redis.io/docs/latest/commands/lpush/),
[LRANGE](https://redis.io/docs/latest/commands/lrange/),
[LSET](https://redis.io/docs/latest/commands/lset/).

## Blocking list consumers

| Method | Result |
| --- | --- |
| `blpop(keys, server_timeout_seconds=0.0)` | `Optional[redis.KeyValue]` with binary key/value |
| `brpop(keys, server_timeout_seconds=0.0)` | Same, popping the rightmost value |
| `blmove(source, destination, source_side="LEFT", destination_side="RIGHT", server_timeout_seconds=0.0)` | `Optional[bytes]` moved value |

Sides accept LEFT/RIGHT case-insensitively. Pop calls require at least one key (an empty byte key is valid); values and keys remain bytes. Negative/non-finite timeouts and invalid
sides are local ValueErrors. Server timeouts use seconds, including fractional
seconds: zero asks Redis to wait indefinitely. A server timeout returns None and
keeps the connection reusable. Positive client `command_timeout_ms` and structured
scope cancellation independently bound the whole network exchange; either closes
and discards an incomplete connection, without replay. A popped/moved value may
already have changed Redis when the transport fails.

Use a standalone connection or `pool.acquire(dedicated=True)`. Typed helpers,
raw blocking list/sorted-set commands, and pipeline batches containing those
commands are rejected on ordinary pool leases. Dedicated leases reserve capacity
for their lifetime and close on release, including server-timeout success.
No timer/socket worker survives a cancelled operation. Always release/disconnect
in `finally`; see the [complete producer/consumer example](examples/blocking.zap).

These methods use Redis [BLPOP](https://redis.io/docs/latest/commands/blpop/),
[BRPOP](https://redis.io/docs/latest/commands/brpop/), and
[BLMOVE](https://redis.io/docs/latest/commands/blmove/). BLPOP/BRPOP support fractional
timeouts on Redis 6.0+; BLMOVE requires 6.2+. The tested server matrix is narrower
and is recorded in [SUPPORT.md](docs/SUPPORT.md).

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

## Cursor-based scan pages

Each scan method fetches **one page** and keeps its cursor as opaque decimal
`bytes`, so cursors above signed-int64 range round-trip without truncation.
Start at `b"0"`, process each returned page, and stop only when `page.done()`
is true (its cursor is exactly `b"0"`). An empty page may still be nonterminal;
a terminal page may still contain results.

| Connection method | Result |
| --- | --- |
| `scan(cursor=b"0", pattern=b"*", count=0)` | `redis.ScanPage` with `.cursor` and `.items: list[bytes]` keys |
| `hscan(key, cursor=b"0", pattern=b"*", count=0)` | `redis.HashScanPage` with `.cursor` and `.fields: list[HashField]` |
| `sscan(key, cursor=b"0", pattern=b"*", count=0)` | `redis.ScanPage` with `.cursor` and `.items: list[bytes]` members |
| `zscan(key, cursor=b"0", pattern=b"*", count=0)` | `redis.SortedScanPage` with `.cursor` and `.members: list[ScoredMember]` |

All page types provide `done()`. Hash values and sorted-set scores use the same
validated representations as HGETALL and ZRANGE WITHSCORES. ZSCAN page members
are **not** guaranteed to appear in score order.

```zap
auto cursor = b"0"
auto pages = 0
while pages < 100:
    auto page = conn.scan(cursor, pattern=b"application:*", count=100)
    for key in page.items:
        # Process this page here; no whole-database collection is required.
        print(str(key))  # explicit conversion only for known UTF-8 keys
    pages = pages + 1
    cursor = page.cursor
    if page.done():
        break
# If cursor is nonzero after the budget, retain it to resume this same scan.
```

MATCH is always sent, defaulting to `b"*"`; patterns and results remain binary.
An empty pattern is sent unchanged. Positive `count` sends COUNT, zero omits
it, and negative hints raise `ValueError` locally. COUNT is a **work hint**,
not a page-size limit: Redis may return fewer, more, or no entries. Existing
RESP byte/depth limits still apply to individual replies.

Cursors must contain canonical unsigned decimal ASCII bytes (`b"0"` or a
nonzero first digit followed by digits). Invalid local cursors raise
`ValueError` before I/O. Tokens are not converted to integers or incremented;
Redis determines their numeric range. Resume with the returned cursor for the
same scan family/key and database; cursors are not portable positions or
snapshot bookmarks.

Scanning is not a snapshot. Results may repeat, and changes during a scan have
Redis's documented weak guarantees. Consumers that require uniqueness must
handle duplicates themselves. Pages preserve duplicates without collecting,
filtering, or sorting the entire dataset. Missing hashes/sets/sorted sets yield
empty terminal pages. Wrong key types remain `ResponseError`.

Malformed outer arrays, cursors, payload types, hash pairs, or member/score
pairs raise `ProtocolError` after consuming the complete frame. The existing
reply recovery/no-replay contract applies. This page API issues SCAN commands;
it does not use KEYS. No eager collect-all or implicit iterator is provided.
SCAN TYPE and HSCAN NOVALUES are outside this slice; use raw `execute` for
those options.

Reference: [Redis SCAN family and guarantees](https://redis.io/docs/latest/commands/scan/).

## Lua scripts

`eval(source, keys, arguments)` and `evalsha(sha, keys, arguments)` return raw
`redis.Reply` values, preserving nested arrays, nulls, integers, binary bulk
strings, and nested error values. Top-level server errors raise `ResponseError`.
The client derives `numkeys` from `len(keys)` and writes all keys before all
arguments; callers cannot supply a mismatched or negative key count.

`script_load(source) -> bytes` validates Redis's SHA1 against the exact source.
`script_exists(shas) -> list[bool]` preserves input order, including repeated
SHAs. SHA arguments must contain 40 ASCII hex bytes; empty SCRIPT EXISTS lists
and invalid SHAs fail locally. Source, keys, and arguments are binary bytes.

```zap
auto script = redis.register_script(b"return redis.call('GET', KEYS[1])")
list[bytes] arguments = []
auto reply = script.run(conn, [b"application:key"], arguments)
auto sha = script.sha()
```

Registration computes SHA1 locally and performs no I/O. Each `run` first uses
EVALSHA. Only a top-level error whose exact prefix is NOSCRIPT causes one EVAL
fallback with the original source, keys, and arguments. Redis caches that
source for later calls. An EVAL error is propagated without another fallback.
Other errors and transport/protocol failures are never retried or automatically
reconnected; a failed reply can follow a completed side effect. Bare EVALSHA
never falls back. Do not use NOSCRIPT as an application-defined script error
code; it is the registered helper's cache-miss signal.

Redis Functions and automatic script warmup are outside this API. Current
the connection's cancellation/deadline policy applies. Tests simulate cache
loss and interrupted execution without issuing global SCRIPT FLUSH.

References: [EVAL](https://redis.io/docs/latest/commands/eval/),
[EVALSHA](https://redis.io/docs/latest/commands/evalsha/).

## Generated parser checks and performance

The deterministic RESP2 suite uses seed 322 for generated valid trees, every
proper prefix, arbitrary byte inputs, hostile headers, wide/deep arrays, and
fragmented binary bulk strings. Live tests compare generated nested Lua replies
from Redis with the decoder's representation. Decoder/connection `max_depth`
is now restricted to 0–64 (default 64), keeping recursive parsing within a
fixed stack budget. `decode` returns an invalid diagnostic for unsupported
limits; connection options raise `ValueError` before connecting.

Command encoding uses balanced byte-fragment concatenation, reducing copying
for many-argument commands. Diagnostic timings and the copied-byte model run
with the offline suite. See [the reproducible workload](tools/bench/resp2.zap)
and [parser performance notes](docs/parser-performance.md) for scope and limits.

## Resumable RESP2 decoder

Connections now use a persistent `redis.Decoder` to frame incoming RESP2 data.
Headers and aggregate progress survive reads, bulk payloads are skipped by
length, and completed children are not repeatedly turned into reply objects.
Chunks merge in logarithmic size classes; unread coalesced data uses an offset,
so draining replies does not repeatedly copy the remaining receive buffer.
The complete frame is decoded into a `Reply` tree once.

```zap
auto parser = redis.decoder(max_frame=1048576, max_depth=64)
auto result = parser.feed(b"$5\r\nhe")  # incomplete
auto complete = parser.feed(b"llo\r\n+NEXT\r\n")  # first reply
# Drain any coalesced replies before adding another input chunk.
auto next_reply = parser.feed(b"")
```

`feed(chunk) -> Decoded` returns at most one reply. Its `consumed` is the
complete frame length, including prior chunks, not a count within the latest
chunk. When `has_pending()` is true, drain with `feed(b"")`; supplying more data
raises `ValueError` without losing that pending input. Incomplete results have
zero consumption. Malformed input poisons the decoder; create a new decoder
for a new stream. `buffered_bytes()` reports the current incomplete frame size.

`processed_bytes`, `copied_bytes`, and `frames_decoded` are cumulative diagnostic
counters. The first counts bytes examined by framing, the second counts retained
slice/merge payload copies (not RSS or all runtime allocations), and the third
counts final tree builds. Framing is linear and retained copies are bounded by
size-class merging rather than growing-buffer quadratic concatenation.

Limits apply to each frame, including across chunks. A single input chunk can
contain many frames; the caller already owns that chunk and should keep chunk
sizes bounded. Connections use their configured read size. No background read,
automatic retry, deadline, or cancellation primitive is added by this decoder.
The stateless `decode` API remains supported. Performance results are in
[parser performance notes](docs/parser-performance.md).

## Decoder-only RESP3 values

`decode3(data, max_frame=16777216, max_depth=64) -> Decoded3` decodes one
fixed-length RESP3 value with the same status/consumption/diagnostic convention.
The separate `redis.Resp3` enum preserves the existing RESP2 `Reply` API and
exhaustive matches. **Connections remain RESP2**; no public HELLO negotiation or
push delivery is introduced. Those session features remain in #26.

RESP3 supports Simple/Error/Integer/Bulk/Array (including legacy NullBulk and
NullArray), Null, Boolean, Double, BigNumber, BlobError, Verbatim, Map, Set,
Attributed, and Push variants. Bulk/error payloads and members remain bytes.
Big numbers retain their signed decimal bytes rather than narrowing to int64.
Doubles use float64, including infinities and NaN; malformed syntax and decimal
overflow/underflow are rejected before native conversion.

Map and attribute entries are ordered `Resp3Pair(key, value)` lists, preserving
binary/arbitrary keys and duplicate entries. `Resp3.Attributed(attributes,
value)` associates metadata with the following value, even when nested; stacked
attributes remain nested wrappers. Attribute bytes and their following value
share the frame/depth budget. A Verbatim value retains its three-byte format
and data separately. Sets/pushes retain their received item order without
claiming set ordering or interpreting push messages as command replies.

Streamed strings/aggregates (`$?`, `*?`, `%?`, chunk/terminator forms) are explicitly
unsupported. Resource limits and invalid-frame diagnostics apply to every
nested value and all fragment boundaries. Live decoder tests explicitly
negotiate RESP3 only on a separate test socket; the library connection and typed
command helpers continue to use RESP2.

Reference: [Redis RESP3 data types](https://redis.io/docs/latest/develop/reference/protocol-spec/).
