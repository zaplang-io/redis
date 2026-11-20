# redis

A pure-Zap Redis client, official non-core package. Tracks
[zaplang-io/zap#322](https://github.com/zaplang-io/zap/issues/322).

The package includes a binary-safe RESP2 codec, opt-in RESP3 connections, typed
commands, explicit reconnect, URL configuration, verified TLS, and session
initialization.

## Installation and support

Version 0.1.0 is an **unreleased candidate**. Use a reviewed checkout with a Zap
path dependency (`zap add redis --path ../redis`, then `zap zenv create --no-venv`)
and activate that environment before `import redis`. Do not copy the library
module beside your application. CI verifies external path and static-registry
consumers and executes the [complete examples](examples).

See [installation and release checks](docs/RELEASING.md), the exact
[Zap/Redis support matrix](docs/SUPPORT.md), and [changelog](CHANGELOG.md).
No public tag or hosted-registry publication is implied by the manifest version.

Sentinel primary discovery and Redis Cluster routing are documented in
[High availability](docs/high-availability.md), including the supported
multi-key command subset and redirection behavior.

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
one command and reads one complete reply using the negotiated protocol. Reads preserve arbitrary bytes;
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
socket during cleanup. The Zap runtime wakes parked TCP/TLS operations on
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
CI uses the latest published Zap image. Redis server images and the checkout
action use digests or SHAs in `.github/workflows/test.yml`. Update each Redis
server version and image digest together.
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
at least two databases, also run the isolated session and generic-key cases:

```sh
REDIS_TEST_URL=redis://127.0.0.1:6379/0 ZAP=/path/to/zap \
  tools/run-integration-tests.sh --isolated
```

That explicit mode tests database isolation and reconnect, creates a randomly
named ACL user limited to the test namespace and required commands, checks good
and bad authentication and client-name restoration, and removes the user and
keys during cleanup. It never changes the default user or global server config.
The isolated run also covers namespace-scoped `KEYS`, `RANDOMKEY`,
cross-database `COPY`, and the default eviction policy's idle-time metadata.

CI supplies a fresh service container per matrix job. Readiness is gated by
`redis-cli ping` health checks; tests have no startup sleeps. Destroying the
service container also removes any state left after a forcibly terminated job.

`tools/run-integration-tests.sh --check` validates the live suites without a
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

These `Connection` methods accept keys/values as `bytes` and work on RESP2 or
explicitly negotiated RESP3 connections:

| Method | Result |
| --- | --- |
| `ping()` | `bool`, true for PONG |
| `get(key)` | `Optional[bytes]`; None for missing, Some(b"") for empty |
| `set_value(name, value, ex=0, px=0, nx=False, xx=False, keepttl=False, get=False, exat=0, pxat=0)` | `redis.SetResult` for one Redis SET command |
| `getdel(name)` | `Optional[bytes]` containing the previous value, or None for a missing key |
| `getex(name, ex=0, px=0, exat=0, pxat=0, persist=False)` | `Optional[bytes]` containing the value, or None for a missing key |
| `setrange(name, offset, value)` | `int64` byte length after writing at a nonnegative offset |
| `getrange(key, start, end)` | `bytes` from the inclusive range, or empty bytes for a missing key or empty span |
| `append(key, value)` | `int64` byte length after appending |
| `strlen(name)` | `int64` byte length, zero for a missing key |
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

Zap reserves `set`, so SET uses `set_value`. DEL uses `delete` and TYPE
uses `key_type`. SET options follow redis-py's argument order through PXAT.
Use keywords to make the conditions and units explicit.

- `redis.SetResult.Written` is Redis's successful SET status when `get=False`.
- `redis.SetResult.Previous(value)` contains the previous bytes when
  `get=True`, including a present empty string. NX with GET can return the
  old value even when Redis skips the write.
- `redis.SetResult.Null` means a skipped condition with `get=False`, or an
  absent previous value with `get=True`. With GET, a null reply can follow
  either a write or a skipped condition.

```zap
auto result = conn.set_value(b"key", b"value", xx=True, keepttl=True, get=True)
match result:
    case redis.SetResult.Previous(data):
        print(len(data))
    case redis.SetResult.Null:
        print("no previous value")
    case redis.SetResult.Written:
        print("written")
```

Callers migrating from the boolean setter can use
`conn.set_value(...).is_written()` for status-only calls, and
`.is_null()` to check for a skipped conditional write. The first key keyword
is now `name`. Use named options when migrating calls that passed
`nx`, `xx`, `ex`, or `px` positionally. See [package decisions](DECISIONS.md).

SET and GETEX accept one of EX/PX (relative seconds/milliseconds) or
EXAT/PXAT (absolute Unix seconds/milliseconds). Expiry arguments are
`int64` values and zero omits the option. SET KEEPTTL and GETEX PERSIST
each conflict with a supplied expiry. SET NX and XX are mutually exclusive.
Plain GETEX keeps the current expiry, and PERSIST removes it. GETEX and GETDEL
preserve the difference between empty bytes and a missing key.

GETRANGE accepts negative indexes and clips endpoints. SETRANGE fills a gap
with zero bytes. An empty SETRANGE leaves the value and length unchanged.
APPEND creates a missing string. Both SETRANGE and APPEND retain the expiry
of an existing key. Commands that read or modify an existing string report
`ResponseError` on a different key type. Plain SET can replace other key
types. SET with GET requires a string to return the old value.

Invalid option combinations, negative string expiry options, negative
SETRANGE offsets, and empty multi-key/pair lists raise `ValueError` before
network I/O. Relative EXPIRE/PEXPIRE durations of zero or less delete an
existing key immediately. Relative expiry conditions are available through
`execute`.

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

### Generic key-space methods

| Method | Result |
| --- | --- |
| `rename(src, dst)` | `bool` for OK. Replaces the destination and preserves the source expiry. Redis raises `ResponseError` for a missing source. |
| `renamenx(src, dst)` | `bool`. False if the destination already exists. |
| `copy(source, destination, destination_db=-1, replace=False)` | `bool`. False when the source is missing or an existing destination prevents copying. The default uses the current database. |
| `keys(pattern=b"*")` | `list[bytes]` matching a binary Redis glob, in server order. |
| `randomkey()` | `Optional[bytes]`. None if the selected database is empty. |
| `touch(keys)` | `int64` count of existing keys touched. Repeated keys count repeatedly. Requires a nonempty list. |
| `dump(name)` | `Optional[bytes]` containing the opaque Redis serialization, or None for a missing key. |
| `restore(name, ttl, value, replace=False, absttl=False, idletime=-1, frequency=-1)` | `bool` for OK. Redis validates the serialized payload and reports collisions or corrupt data as `ResponseError`. |
| `expireat(name, time, nx=False, xx=False, gt=False, lt=False)`, `pexpireat(...)` | `bool` when the absolute expiration was applied. Pass Unix seconds or milliseconds respectively. |
| `expiretime(name)`, `pexpiretime(name)` | `int64` Unix seconds or milliseconds, respectively. Both return -1 for persistent keys and -2 for missing keys. |
| `object(infotype, key)` | `redis.ObjectInfo.Encoding(bytes)`, `.Refcount(int64)`, `.IdleTime(int64)`, or `.Missing`. The selector is case-insensitive. |

Use [SCAN pages](#cursor-based-scan-pages) for production traversal. Redis
scans the whole selected database for KEYS, even with a narrow pattern.

RESTORE uses a millisecond TTL. Zero creates a persistent key. With
`absttl=True`, a nonzero TTL is an absolute Unix timestamp in milliseconds.
`idletime` is in seconds, `frequency` must be 0–255, and -1 omits either
option. Supply at most one eviction metadata option. Redis's eviction policy
determines which metadata applies. OBJECT IDLETIME can fail under LFU.

For absolute expiration, NX requires a persistent key. XX requires an existing
expiry. GT and LT compare with the previous expiry and may each be combined
with XX. NX conflicts with the other flags, and GT conflicts with LT. A past
expiry deletes the key if the condition is satisfied.

```zap
auto snapshot = conn.dump(b"source")
match snapshot:
    case Optional.Some(payload):
        conn.restore(b"restored", 60000, payload, replace=True)
    case _:
        print("source key is missing")
```

## Bitmap commands

| Method | Result |
| --- | --- |
| `setbit(name, offset, value)` | Previous bit as `int64`, zero or one |
| `getbit(name, offset)` | Bit as `int64`, zero for a missing or unset bit |
| `bitcount(key, start=None, end=None, mode="")` | Number of set bits |
| `bitpos(key, bit, start=None, end=None, mode="")` | Absolute bit position, or -1 if the bit is absent from the search range |
| `bitop(operation, dest, keys)` | Byte length of the stored result |
| `bitfield(key, default_overflow="WRAP")` | Reusable `redis.BitFieldOperation` builder |

Keys are bytes. SETBIT and GETBIT use zero-based bit offsets from 0 through
4294967295, with offset zero at the most significant bit of the first byte.
BITCOUNT accepts either both range endpoints or neither. BITPOS also
accepts a start-only range. Endpoints use `Optional.Some(index)`, are
inclusive, and may be negative. The default unit is BYTE. Supply both
endpoints to use the explicit BYTE or BIT mode. An open-ended BITPOS
search for zero can include zero padding after the last byte.

BITOP takes a list of source keys. AND, OR, XOR, and NOT are available on
both supported Redis versions. NOT requires exactly one source. Redis
validates the operator against the connected server version.

```zap
conn.setbit(b"flags", 9, 1)
auto count = conn.bitcount(b"flags", Optional.Some(8), Optional.Some(15), mode="BIT")
auto fields = conn.bitfield(b"counters")
fields.set_value("u8", "#1", 255).get("u8", "8")
fields.incrby("u8", "#1", 1, overflow="FAIL")
auto results = fields.execute()
```

BITFIELD offsets are text tokens such as `"8"` for a bit offset or
`"#1"` for a field-width multiplier. GET, SET, and INCRBY each occupy
one slot in the ordered `list[Optional[int64]]` returned by `execute()`.
In the example the results are the previous value 0, the new value 255,
and None for the increment that would overflow. OVERFLOW changes the mode
for subsequent writes and adds no result slot. The modes are WRAP, SAT,
and FAIL. Execution sends the queued operations in one command and resets
the builder to its default mode. Use `reset()` to discard queued work.

See the [Redis bitmap reference](https://redis.io/docs/latest/develop/data-types/bitmaps/)
and the [BITFIELD command reference](https://redis.io/docs/latest/commands/bitfield/).

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

HGETALL requires a non-null array containing an even number of non-null
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
| `lpushx(name, values)`, `rpushx(name, values)` | `int64` new length, or zero if the list was absent |
| `lpos(name, value, rank=1, count=-1, maxlen=0)` | `redis.ListPosition.Index(int64)`, `.Missing`, or `.Positions(list[int64])` |
| `linsert(name, where, refvalue, value)` | `int64` new length, zero for an absent list, or -1 for an absent pivot |
| `lrem(name, count, value)` | `int64` number of elements removed |
| `rpoplpush(src, dst)` | `Optional[bytes]` moved tail element, or None for an absent source |
| `lmpop(keys, direction, count=1)` | `Optional[redis.ListPopBatch]` with the selected binary `.key` and `.values` |
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
Conditional pushes leave a missing key absent. Empty byte elements are valid
and pop/index results distinguish Some(b"") from None. LPOP and RPOP use the
single-element form. LMPOP selects the first nonempty list in input order.
Its count must be positive, and its LEFT/RIGHT direction is case-insensitive.
The reply preserves pop order and can contain fewer elements than requested.

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
LPOS indexes are always zero-based from the head. A positive rank searches
from the head and a negative rank searches from the tail. Rank zero raises
`ValueError`. A positive `maxlen` caps the number of inspected elements
in the search direction. Zero leaves the search uncapped. An omitted count
returns `.Index` or `.Missing`. An explicit count returns `.Positions`,
including an empty list when no match exists. Count zero finds all matches
from the selected rank.

LINSERT accepts BEFORE or AFTER case-insensitively and uses the first
matching pivot. LREM scans from the head for a positive count and from the
tail for a negative count. A zero count removes every matching value.
RPOPLPUSH atomically moves the source tail to the destination head. With
the same key as source and destination, it rotates the list.

References [LPOS](https://redis.io/docs/latest/commands/lpos/),
[LINSERT](https://redis.io/docs/latest/commands/linsert/),
[LREM](https://redis.io/docs/latest/commands/lrem/), and
[LMPOP](https://redis.io/docs/latest/commands/lmpop/).

## Blocking list consumers

| Method | Result |
| --- | --- |
| `blpop(keys, server_timeout_seconds=0.0)` | `Optional[redis.KeyValue]` with binary key/value |
| `brpop(keys, server_timeout_seconds=0.0)` | Same, popping the rightmost value |
| `blmove(source, destination, source_side="LEFT", destination_side="RIGHT", server_timeout_seconds=0.0)` | `Optional[bytes]` moved value |
| `brpoplpush(src, dst, timeout=0.0)` | `Optional[bytes]` moved source tail |
| `blmpop(timeout, keys, direction, count=1)` | `Optional[redis.ListPopBatch]` with the selected key and values |

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

Redis deprecated RPOPLPUSH and BRPOPLPUSH in 6.2. The helpers expose those
commands for compatibility. BLMOVE with RIGHT and LEFT provides the equivalent
blocking move. LMPOP and BLMPOP require Redis 7.0 or newer. See the tested
versions in [SUPPORT.md](docs/SUPPORT.md).
[BRPOPLPUSH reference](https://redis.io/docs/latest/commands/brpoplpush/),
[BLMPOP reference](https://redis.io/docs/latest/commands/blmpop/).

## Set commands

Set helpers accept binary keys and members, including empty bytes.

| Connection method | Result |
| --- | --- |
| `sadd(key, members)`, `srem(key, members)` | `int64` count of added or removed members |
| `sismember(key, member)` | `bool` membership |
| `smismember(name, values)` | `list[bool]` in request order, including duplicates |
| `smove(src, dst, value)` | `bool` indicating whether the source held the member |
| `scard(key)` | `int64` cardinality, zero for a missing set |
| `smembers(key)` | `list[bytes]` members in unspecified order |
| `srandmember(name, count=None)`, `spop(name, count=None)` | `redis.SetSelection` with one member, a missing result, or a list of members |
| `sunion(keys)`, `sinter(keys)`, `sdiff(keys)` | `list[bytes]` union, intersection, or difference |
| `sintercard(keys, limit=0)` | `int64` intersection cardinality, capped by a positive limit |
| `sunionstore(dest, keys)`, `sinterstore(dest, keys)`, `sdiffstore(dest, keys)` | `int64` resulting destination cardinality |

SADD and SREM require a nonempty member list. SMISMEMBER requires at least
one value and returns false for each value when the set is missing. Set
algebra requires at least one key. A missing input is an empty set, and
SDIFF uses the first input as its source. SINTERCARD derives the wire key
count from the list. A zero limit requests the full cardinality.

SMOVE atomically removes the member from the source and adds it to the
destination. When the destination already holds the member, Redis still
removes it from the source and returns true. The STORE commands replace the
destination and its prior expiry. An empty result removes the destination.

```zap
conn.sadd(b"first", [b"shared", b"first-only"])
conn.sadd(b"second", [b"shared", b"second-only"])
auto shared = conn.sinterstore(b"both", [b"first", b"second"])
match conn.srandmember(b"first", count=Optional.Some(-3)):
    case redis.SetSelection.Members(values):
        print(len(values))
    case _:
        print("expected a counted sample")
```

With count omitted, `SetSelection.Member(value)` holds the chosen bytes
and `.Missing` represents an absent set. An explicit `Optional.Some(count)`
returns `.Members(values)`, with an empty list for a missing set or zero
count. Positive counts are bounded by cardinality and select distinct
members. A negative SRANDMEMBER count samples with replacement and returns
its magnitude in values if the set exists. SRANDMEMBER preserves the set.
SPOP rejects negative counts and removes the selected members. Removing
the final member deletes the key.

The scalar helpers `.is_some()`, `.is_none()`, and `.unwrap()` support
existing uncounted SPOP flows. Callers that explicitly typed the old
`Optional[bytes]` result can switch to `SetSelection` or match its cases.
For keyword calls, use `name=` for the set key. Set-member results have unspecified order. SMISMEMBER follows input order.

Empty argument lists, negative SINTERCARD limits, invalid selection counts,
and the most negative int64 SRANDMEMBER count raise `ValueError` locally.
Wrong key types raise `ResponseError`, preserving server bytes in
`last_error()`. An unexpected typed reply raises `ProtocolError` after
consuming its complete frame. See the existing recovery and no-replay contract.

References [SMISMEMBER](https://redis.io/docs/latest/commands/smismember/),
[SINTERCARD](https://redis.io/docs/latest/commands/sintercard/),
[SMOVE](https://redis.io/docs/latest/commands/smove/),
[SRANDMEMBER](https://redis.io/docs/latest/commands/srandmember/), and
[SPOP](https://redis.io/docs/latest/commands/spop/).

## HyperLogLog commands

These methods accept byte keys and binary elements:

| Method | Result |
| --- | --- |
| `pfadd(name, values=[])` | `int64`, one if Redis creates the sketch or changes a register, zero otherwise |
| `pfcount(sources)` | `int64` estimate of the distinct elements across one or more sketches |
| `pfmerge(dest, sources=[])` | `bool`, true when Redis acknowledges the merge |

```zap
conn.pfadd(b"visitors:today", [b"alice", b"bob"])
conn.pfadd(b"visitors:yesterday", [b"bob", b"carol"])
auto estimate = conn.pfcount([b"visitors:today", b"visitors:yesterday"])
conn.pfmerge(b"visitors:rolling", [b"visitors:today", b"visitors:yesterday"])
```

Redis reports a standard error of 0.81% for HyperLogLog estimates.
PFADD's return value describes a register update or creation. Use PFCOUNT
for a count estimate. PFADD without elements creates an empty sketch if
the key is missing. PFCOUNT requires at least one key and treats missing
keys as empty sketches. A multi-key count estimates the union, so overlap
and duplicate keys do not add to the result. PFMERGE includes an existing
destination in the union and creates an empty sketch when all keys are
missing. A string that does not contain a valid HyperLogLog can raise
`ResponseError`, as can a key of the wrong Redis type.

References [PFADD](https://redis.io/docs/latest/commands/pfadd/),
[PFCOUNT](https://redis.io/docs/latest/commands/pfcount/),
and [PFMERGE](https://redis.io/docs/latest/commands/pfmerge/).

## Geospatial commands

| Method | Result |
| --- | --- |
| `geoadd(name, values, nx=False, xx=False, ch=False)` | Added members, or added and updated members with CH |
| `geodist(name, place1, place2, unit="")` | `Optional[float64]` distance, with meters as the default |
| `geopos(name, values)` | `list[Optional[GeoPosition]]` in request order |
| `geohash(name, values)` | `list[Optional[bytes]]` of geohash strings in request order |
| `geosearch(name, ...)` | `GeoSearchResult.Members` or `GeoSearchResult.Hits` |
| `geosearchstore(dest, name, ...)` | Number of members stored |

Keys and members are bytes. Each GEOADD entry is a
`redis.GeoEntry(longitude, latitude, member)`. Redis accepts longitudes
from -180 through 180 and latitudes from -85.05112878 through 85.05112878.
GEOPOS uses longitude followed by latitude. Missing members have a None
slot in GEOPOS and GEOHASH, including when the input has repeated members.
Coordinates are quantized by Redis, so retrieved positions can differ
slightly from the values supplied to GEOADD.

```zap
conn.geoadd(b"places", [
    redis.GeoEntry(13.361389, 38.115556, b"Palermo"),
    redis.GeoEntry(15.087269, 37.502669, b"Catania"),
])
auto nearby = conn.geosearch(
    b"places", member=Optional.Some(b"Palermo"),
    unit="km", radius=Optional.Some(200.0), sort="ASC", withdist=True,
)
match nearby:
    case redis.GeoSearchResult.Hits(hits):
        for hit in hits:
            print(hit.member, hit.distance.unwrap())
    case redis.GeoSearchResult.Members(members):
        print(len(members))
```

Supply either `member` or both `longitude` and `latitude` for the origin,
and either `radius` or both `width` and `height` for the shape. Wrap supplied
values with `Optional.Some`. The units are `m`, `km`, `mi`, and `ft`.
Use `sort="ASC"` or `"DESC"` for ordering by distance. A positive `count`
limits the result, and `any=True` with a count lets Redis stop when it
finds enough matches.

GEOSEARCH without WITH flags returns `Members(list[bytes])`. With flags it
returns `Hits(list[GeoHit])`. Each hit includes the member and optional
`distance` in the chosen unit, integer `hash` from WITHHASH, and
`position` from WITHCOORD. The raw integer hash is the internal geo
score. GEOHASH returns the textual hash.

GEOSEARCHSTORE takes the same origin, shape, unit, sort, and count options.
It overwrites the destination with the matching indices, or removes the
destination when there are no matches. Use `storedist=True` to store the
distances as sorted-set scores in the requested unit.

[Redis geospatial command reference](https://redis.io/docs/latest/develop/data-types/geospatial/)

## Sorted-set commands

Sorted-set keys and members are binary `bytes`. `redis.ScoredMember(member,
score)` pairs a member with its `float64` score in typed replies.

| Method | Result |
| --- | --- |
| `zadd(name, mapping, nx=False, xx=False, ch=False, incr=False, gt=False, lt=False)` | `redis.ZAddResult`. The mapping is `dict[bytes, float64]`. |
| `zrem(key, members)`, `zcard(key)` | `int64` removed members or cardinality |
| `zscore(key, member)`, `zincrby(key, member, amount)` | `Optional[float64]` current score, or `float64` new score |
| `zrank(key, member)`, `zrevrank(key, member)` | `Optional[int64]` zero-based forward/reverse rank |
| `zrange(key, start, stop)`, `zrange_withscores(key, start, stop)` | `list[bytes]` or `list[redis.ScoredMember]` in rank order |
| `zrangebyscore(name, min, max, start=-1, num=-1, withscores=False)`, `zrevrangebyscore(name, max, min, ...)` | `redis.SortedSetResult` with member or scored-member lists |
| `zrangebylex(name, min, max, start=-1, num=-1)`, `zrevrangebylex(name, max, min, ...)` | `list[bytes]` in lexicographic order |
| `zcount(name, min, max)`, `zlexcount(name, min, max)` | `int64` count within the score or lex interval |
| `zrangestore(dest, name, start, end, byscore=False, bylex=False, desc=False, offset=-1, num=-1)` | `int64` number stored |
| `zunion(keys, aggregate="", withscores=False, weights=[])`, `zinter(keys, ...)`, `zdiff(keys, withscores=False)` | `redis.SortedSetResult` for union, intersection, or difference |
| `zunionstore(dest, keys, aggregate="", weights=[])`, `zinterstore(dest, keys, ...)`, `zdiffstore(dest, keys)` | `int64` number stored |
| `zpopmin(name, count=-1)`, `zpopmax(name, count=-1)` | `list[redis.ScoredMember]` in pop order |
| `bzpopmin(keys, timeout=0.0)`, `bzpopmax(keys, timeout=0.0)` | `Optional[redis.SortedPop]` with the selected key, member, and score |
| `zmpop(keys, min=False, max=False, count=1)`, `bzmpop(timeout, keys, min=False, max=False, count=1)` | `Optional[redis.SortedPopBatch]` with the selected key and its scored members |

### Adding and incrementing scores

```zap
auto added = conn.zadd(b"scores", {b"Ada": 1.5, b"Lin": 2.0}).count()
match conn.zadd(b"scores", {b"Ada": 1.0}, xx=True, incr=True):
    case redis.ZAddResult.Score(score):
        print(score)
    case redis.ZAddResult.Null:
        print("condition skipped")
    case redis.ZAddResult.Count(count):
        print(count)
```

ZADD takes a nonempty member-to-score dictionary. Migrate earlier calls by
replacing the list of `ScoredMember` inputs with a dictionary and using
`.count()` for the count result. Each dictionary key has one score. Ordinary
ZADD counts added members. CH counts additions and changed scores. NX inserts
only new members, and XX updates only existing members. GT and LT compare
against the existing score and allow new members unless combined with XX.
NX conflicts with XX, GT, or LT, and GT conflicts with LT.

INCR requires one mapping entry. Its value is the score delta, and its
result is `ZAddResult.Score` or `.Null` when the condition skips the write.
Use `.is_null()` to check for that result. Score zero is a present score.

Scores and weights use 17-digit float64 formatting. Infinity is supported.
NaN input raises `ValueError` before I/O, and an increment that would produce
NaN raises `ResponseError`. Numeric score replies are validated and retain
float64 precision, including subnormal values.

### Ranges and combinations

Rank bounds are inclusive and allow negative indexes. Score and lex bounds
use Redis tokens as bytes. For scores, `b"1.5"` is inclusive and `b"(1.5"`
is exclusive. Use `b"-inf"` and `b"+inf"` for infinities. For lex ranges, use
`b"[" + member` or `b"(" + member` for an inclusive or exclusive member.
`b"-"` and `b"+"` bound the full lex range. Use lex ranges on sets whose
members share a score. Reversed ranges take the upper bound first.

A nonnegative `start` (or `offset` for ZRANGESTORE) enables LIMIT. A zero
`num` returns no entries and a negative `num` takes the remaining entries.
ZRANGESTORE uses byte tokens for rank bounds too, such as `b"0", b"-1"`.
Its LIMIT applies to score and lex modes. An empty store result removes
the destination key.

With `withscores=True`, score ranges and set combinations return
`SortedSetResult.WithScores(list[ScoredMember])`. Otherwise they return
`SortedSetResult.Members(list[bytes])`. For union and intersection,
`weights` follows the order of `keys` and defaults to all ones.
Aggregate modes are SUM (default), MIN, MAX, and COUNT (Redis 8.8+).
Difference uses the first input as the source and subtracts later inputs.

### Blocking pops

Pop commands select the first nonempty set in key order. ZPOPMIN/MAX use one
member when count is omitted and return an empty list when count is zero
or the set is missing. ZMPOP/BZMPOP derive the Redis key count from the
keys list and require exactly one of `min` and `max` and a positive count.
An empty ZMPOP result or a server timeout returns None.

The three blocking helpers also work on dedicated pool leases. Redis
`timeout` is in seconds, with zero waiting indefinitely. The independent
client command deadline and scope cancellation close an incomplete
connection and discard its lease; a server timeout leaves the session
usable. See [deadlines](docs/deadlines.md) and
[lease ownership](docs/transactions-pooling.md).

Malformed replies raise `ProtocolError`. The connection remains usable
when a complete frame was consumed. The helpers decode RESP2 and RESP3
score pairs into the same result types.

References [ZADD](https://redis.io/docs/latest/commands/zadd/),
[ZRANGE](https://redis.io/docs/latest/commands/zrange/),
[ZUNION](https://redis.io/docs/latest/commands/zunion/), and
[BZMPOP](https://redis.io/docs/latest/commands/bzmpop/).

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

## Pub/Sub subscriptions

`conn.publish(topic, payload)` returns Redis's recipient count. Subscribe through
`conn.pubsub()` or a dedicated lease's `pubsub()` session, which exclusively owns
its connection until disconnected. `subscribe`, `psubscribe`, `unsubscribe`, and
`punsubscribe` send controls; `next_event()` returns typed acknowledgement,
literal-message, and pattern-message events in wire order. Empty unsubscribe
lists mean all names of that kind.

Messages and topics remain bytes. The pull API creates no background receiver
or unbounded queue. Slow callers can be disconnected by Redis's output-buffer
policy. Delivery is at-most-once; there is no replay or transparent resubscription.
Cancel/join a blocked reader's scope before disconnecting. Always disconnect the
session and release its dedicated lease in finally.

## Streams

Connection helpers `xadd`, `xlen`, `xrange`, `xrevrange`, `xdel`, and `xread` use
byte-token IDs and ordered `StreamField` pairs, preserving repeated binary fields.
XREAD takes `StreamCursor(key, after_id)` values and returns `StreamBatch` values
containing `StreamEntry` objects. IDs above signed int64 stay exact bytes.

`xadd` returns `Optional[bytes]`. Use `.unwrap()` when the command can
create the stream. With `nomkstream=True` an absent stream returns None.
Both `xadd` and `xtrim` support MAXLEN or MINID, exact or approximate
trimming, and LIMIT with approximate trimming.

`xread(..., block_ms=-1)` is nonblocking; zero blocks indefinitely at Redis and
positive values set a server wait in milliseconds. Server timeout/missing data
returns an empty list. Client deadlines/cancellation independently discard
incomplete connections. Blocking XREAD requires a dedicated pool lease; ordinary
leases support nonblocking `xread` and reject blocking raw/pipelined XREAD.
Consumer groups use `xgroup_create`, `xgroup_destroy`, `xreadgroup`, `xack`,
`xpending`, `xclaim`, and `xautoclaim`. Group reads use `StreamCursor` identities:
`b">"` requests entries never delivered to this consumer, while a full stream
ID resumes this consumer's pending entries. `xpending` returns a summary with
the total, ID range, and per-consumer pending counts. `xautoclaim` returns the
continuation ID, reclaimed entries or JUSTID results, and (on Redis 7+)
IDs removed during the claim scan. Delivery is at least once. Process an
entry before acknowledging it, and make consumers idempotent where needed.
Blocking XREADGROUP requires a dedicated pool lease.

Use `xinfo_stream`, `xinfo_groups`, and `xinfo_consumers` to inspect typed
metadata. `xinfo_stream(full=True)` includes entries and pending details.
`xsetid` updates an existing stream's last ID and optional bookkeeping fields.

See [API, delivery/backpressure, ID, timeout, and shutdown semantics](docs/pubsub-streams.md)
and the [complete installed-package example](examples/pubsub_streams.zap).

## Server administration

| Methods | Result |
| --- | --- |
| `info(section=b"", sections=[])` | `ServerInfo` containing ordered `InfoSection` and `InfoField` records |
| `config_get(pattern=b"*", patterns=[])` | Matching configuration names and values as `list[KeyValue]` |
| `config_set(name, value, additional=[])`, `config_resetstat()` | True on Redis OK |
| `dbsize()` | Number of keys in the selected database |
| `flushdb(asynchronous=False)`, `flushall(asynchronous=False)` | True on Redis OK |
| `client_id()`, `client_getname()`, `client_setname(name)` | Numeric connection ID, optional binary name, and name-change status |
| `client_list(_type=b"", client_id=[])` | `list[ClientInfo]` with IDs and ordered binary fields |
| `client_kill(address)`, `client_kill_filter(...)` | Status for the legacy address form and match count for the filter form |
| `client_no_evict(mode)` | True on Redis OK for ON or OFF on the current connection |
| `slowlog_get(num=None)`, `slowlog_reset()` | Ordered `SlowLogEntry` records and reset status |
| `memory_usage(key, samples=None)` | Estimated bytes as `Optional[int64]`, None for an absent key |
| `command_count()`, `command_docs(names=[])` | Command count and named documentation records |

INFO values are tagged as `InfoValue.Integer`, `.Decimal`, `.Text`, or
`.Attributes`. Text and compound attribute values remain bytes. Unknown
section and field names remain available, and integers outside signed
64-bit range stay exact as text. COMMAND DOCS preserves heterogeneous field
values as `Reply`. Slow log entries include client address and name when
supplied by Redis, and the original argument count on Redis 8.10+.

**FLUSHDB deletes all keys in the currently selected database. FLUSHALL
deletes all keys in every database on the connected server.** Both use
explicit SYNC by default. `asynchronous=True` requests ASYNC, which makes
the affected keys inaccessible while Redis reclaims memory in the
background. Confirm which server and database the connection targets
before calling either method.

CONFIG SET changes live server configuration. CONFIG RESETSTAT and
SLOWLOG RESET affect server-wide state. CLIENT KILL disconnects connections
matching the supplied address or filters. Prefer a known client ID for
targeted disconnects. These commands require the corresponding Redis ACL
permissions. CLIENT SETNAME and NO-EVICT apply to the active session.
A reconnect uses the original connection options.

The `--disposable-admin` integration flag enables global reset and flush
cases plus DEBUG SLEEP. Use it only with a dedicated disposable Redis
instance with two empty databases, administrative access, and DEBUG
enabled at startup. Regular integration runs exercise the read-only and
connection-local admin cases.

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

The connection's cancellation and deadline policy applies. Tests simulate cache
loss and interrupted execution without issuing global SCRIPT FLUSH.

References: [EVAL](https://redis.io/docs/latest/commands/eval/),
[EVALSHA](https://redis.io/docs/latest/commands/evalsha/).

## Redis Functions

| Method | Result |
| --- | --- |
| `function_load(code, replace=False)`, `function_delete(library)` | Loaded library name and deletion status |
| `function_list(library=b"*", withcode=False)` | `FunctionLibrary` records with engine, functions, flags, and optional source |
| `function_dump()`, `function_flush(mode="SYNC")` | Binary dump and flush status |
| `function_stats()` | Optional running function and per-engine library and function counts |
| `fcall(function, keys, arguments)`, `fcall_ro(function, keys, arguments)` | Raw `Reply` with the key count derived from the list |

Source, function and library names, keys, and arguments are bytes. FCALL_RO
uses Redis's read-only function rules. Redis errors propagate as
`ResponseError`. FUNCTION FLUSH deletes all function libraries on the
connected server. Call it only when clearing that server is intentional.

## Sharded Pub/Sub

Use `sub.ssubscribe(channels)` and `sub.sunsubscribe(channels=[])` on a
dedicated Pub/Sub session. Read `ShardSubscribed`, `ShardUnsubscribed`,
and `ShardMessage` events with `next_event()`. Publish through a separate
connection or lease with `spublish(channel, message)`.

Cluster clients expose `cluster.ssubscribe(channels)` and
`cluster.spublish(channel, message)`. The returned subscription owns a
separate socket. Read its first acknowledgement with `next_event()` and
disconnect that session when finished. Use channels with matching hash
tags for one session. [Cluster shard routing](docs/high-availability.md)

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

## RESP3 connections

`decode3(data, max_frame=16777216, max_depth=64) -> Decoded3` decodes one
fixed-length RESP3 value with the same status/consumption/diagnostic convention.
The separate `redis.Resp3` enum preserves the existing RESP2 `Reply` API and
exhaustive matches. Connections remain RESP2 by default. Use
`connect_resp3_options(options(...))` or `connect_resp3_url(url)` to opt into
HELLO 3. Authentication, client name, database selection, reconnect, TLS, and
configured timeouts use the same connection options as RESP2.

On an opted-in connection, `execute` converts ordinary RESP3 values to the
existing `Reply` shapes. `execute3` returns the full RESP3 value, while
`execute3_raw` returns server errors as values. `last_attributes()` returns the
attributes on the most recently consumed reply. Interleaved push frames are
held in a bounded FIFO and read with `next_push(timeout_ms)`; exceeding its
64-frame limit poisons the connection. RESP3 Pub/Sub uses the same push path
through `PubSub.next_event`. There is no background reader, so a connection
must have one active owner at a time.

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

Streamed strings/aggregates (`$?`, `*?`, `%?`, chunk/terminator forms) are
explicitly unsupported. Resource limits and invalid-frame diagnostics apply to
every nested value and all fragment boundaries. Existing typed command helpers
work on either protocol through the compatibility conversion.

Reference: [Redis RESP3 data types](https://redis.io/docs/latest/develop/reference/protocol-spec/).
