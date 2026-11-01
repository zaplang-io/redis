# redis

A pure-Zap Redis client, official non-core package. Tracks
[zaplang-io/zap#322](https://github.com/zaplang-io/zap/issues/322).

The first implementation slice ([#1](https://github.com/zaplang-io/redis/issues/1))
is a binary-safe RESP2 wire codec. The next slice ([#3](https://github.com/zaplang-io/redis/issues/3))
adds a TCP connection and raw command execution. Typed command helpers are still
planned.

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
            case redis.Reply.Error(data):
                print("Redis error: " + str(data))
            case _:
                print("unexpected reply")
    finally:
        conn.disconnect()
```

`connect()` returns an open `Connection`. `execute(list[bytes]) -> Reply` sends
one command and reads one complete RESP2 reply. Reads preserve arbitrary bytes;
fragmented replies are assembled and surplus bytes retained for the next reply.
Redis server errors currently return `Reply.Error`; typed exceptions are a later
slice. Empty commands raise `ValueError` before touching the socket.

Use `disconnect()` to close the connection (Zap reserves the word `close`). It
is idempotent when idle or already closed. `is_closed()` reports whether it has
been closed. One active operation owns the connection: overlapping `execute` or
`disconnect` calls raise `RuntimeError` instead of queuing or sharing replies.
The connection fields and direct construction are internal; use `connect()`.

Transport failures, EOF before a complete reply, and invalid framing close the
connection and discard buffered bytes. Commands are never automatically retried.
A failed operation might already have executed on the server. There is no
reconnect, authentication, pooling, TLS, or configurable deadline in this slice;
these have separate backlog issues. Use a dedicated connection for stateful
commands and manage its lifetime with `try/finally`.

`max_frame` (default 16 MiB) and `max_depth` (default 64) apply to each incoming
reply. The receive buffer is bounded by `max_frame`; outgoing command size is
not limited by this option. `read_size` (default 4096, valid range 1–65536) controls
read chunk size. Large values work across multiple reads. Host, port, and limits
are validated before connecting.

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
Transport tests start loopback fake servers and cover one-byte reads, 64 KiB
binary commands/replies, coalesced frames, EOF, malformed/oversized replies,
connection reuse, exclusive ownership, and cleanup. Zap net.send currently
handles partial OS writes internally; the client also checks write progress.

## Development

Use medium-sized individual issues. Issues for this library belong in
`zaplang-io/redis`. Follow-up slices: TCP connection lifecycle and incremental
buffering; command helpers and integration tests against Redis. Keep this codec
independent of transport and command policy.
