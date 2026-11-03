# RESP2 wire codec. All payloads stay bytes; text decoding is a caller choice.
# Importing this module performs no sockets or implicit command execution.
enum Reply:
    Simple(bytes data)
    Error(bytes data)
    Integer(int64 value)
    Bulk(bytes data)
    Array(list[Reply] items)
    NullBulk
    NullArray

# A successful decode consumes exactly one reply. Incomplete and invalid
# results always have consumed=0; callers retain the entire input on incomplete.
struct Decoded:
    str status
    Reply value
    int64 consumed
    str message

# Balanced concatenation bounds copying to O(total bytes * log(fragment count))
# instead of repeatedly copying the entire growing command for each argument.
def _join_bytes(list[bytes] parts, int64 start, int64 stop) -> bytes:
    if stop <= start:
        return b""
    if stop - start == 1:
        return parts[start]
    auto middle = start + (stop - start) // 2
    return _join_bytes(parts, start, middle) + _join_bytes(parts, middle, stop)

# Encode command + arguments as an array of bulk strings (not inline commands).
def encode_command(list[bytes] args) -> bytes:
    if len(args) == 0:
        raise ValueError("a Redis command must have at least one argument")
    list[bytes] parts = [bytes("*" + str(len(args)) + "\r\n")]
    auto i = 0
    while i < len(args):
        bytes arg = args[i]
        parts.append(bytes("$" + str(len(arg)) + "\r\n"))
        parts.append(arg)
        parts.append(b"\r\n")
        i = i + 1
    return _join_bytes(parts, 0, len(parts))

def _incomplete() -> Decoded:
    return Decoded("incomplete", Reply.NullBulk, 0, "")

def _invalid(str message) -> Decoded:
    return Decoded("invalid", Reply.NullBulk, 0, message)

def _ok(Reply value, int64 end) -> Decoded:
    return Decoded("ok", value, end, "")

# Parse a signed int64 without wrapping arithmetic. Accumulate negatively so
# -9223372036854775808 is representable throughout the conversion.
def _number(bytes raw) -> int64:
    auto i = 0
    auto negative = False
    if len(raw) == 0:
        raise ValueError("empty number")
    if raw[0] == 45:
        negative = True
        i = 1
    elif raw[0] == 43:
        i = 1
    if i == len(raw):
        raise ValueError("missing digits")
    auto value = 0
    auto last = 7
    if negative:
        last = 8
    while i < len(raw):
        auto digit = raw[i] - 48
        if digit < 0 or digit > 9:
            raise ValueError("invalid decimal number")
        if value < -922337203685477580:
            raise ValueError("integer overflow")
        if value == -922337203685477580:
            if digit > last:
                raise ValueError("integer overflow")
        value = value * 10 - digit
        i = i + 1
    if negative:
        return value
    return -value

# Decode one reply from the start of data. Limits apply to ONE frame, so a
# coalesced buffer may exceed max_frame if its first reply fits within it.
# max_depth counts nested non-null arrays (a scalar needs depth zero).
def decode(bytes data, int64 max_frame = 16777216, int64 max_depth = 64) -> Decoded:
    if max_frame < 3 or max_depth < 0 or max_depth > 64:
        return _invalid("invalid decoder limits")
    return _parse(data, 0, max_frame, max_depth)

def _parse(bytes data, int64 start, int64 limit, int64 depth) -> Decoded:
    if start >= limit:
        return _invalid("frame exceeds byte limit")
    if start >= len(data):
        return _incomplete()
    auto tag = data[start]
    if tag != 43 and tag != 45 and tag != 58 and tag != 36 and tag != 42:
        return _invalid("unknown RESP2 type")
    auto end = start + 1
    while True:
        if end >= limit:
            return _invalid("frame exceeds byte limit")
        if end >= len(data):
            return _incomplete()
        if data[end] == 10:
            return _invalid("bare LF in header")
        if data[end] == 13:
            if end + 1 >= limit:
                return _invalid("frame exceeds byte limit")
            if end + 1 >= len(data):
                return _incomplete()
            if data[end + 1] != 10:
                return _invalid("CR must be followed by LF")
            break
        end = end + 1
    auto raw = data[start + 1:end]
    auto pos = end + 2
    if tag == 43:
        return _ok(Reply.Simple(raw), pos)
    if tag == 45:
        return _ok(Reply.Error(raw), pos)
    auto count = 0
    try:
        count = _number(raw)
    except ValueError as message:
        return _invalid(message)
    if tag == 58:
        return _ok(Reply.Integer(count), pos)
    # Lengths are unsigned decimal, except the exact null sentinel "-1".
    if raw == b"-1":
        if tag == 36:
            return _ok(Reply.NullBulk, pos)
        return _ok(Reply.NullArray, pos)
    if raw[0] == 45 or raw[0] == 43:
        return _invalid("invalid length")
    if tag == 36:
        # Subtract before adding to avoid overflow from hostile lengths.
        if count > limit - pos - 2:
            return _invalid("bulk string exceeds byte limit")
        auto tail = pos + count
        if tail + 2 > len(data):
            return _incomplete()
        if data[tail] != 13 or data[tail + 1] != 10:
            return _invalid("bulk string missing CRLF")
        return _ok(Reply.Bulk(data[pos:tail]), tail + 2)
    if depth == 0:
        return _invalid("array nesting exceeds depth limit")
    # Every child requires at least three bytes (e.g. +\r\n).
    if count > (limit - pos) // 3:
        return _invalid("array exceeds byte limit")
    list[Reply] items = []
    auto i = 0
    while i < count:
        auto child = _parse(data, pos, limit, depth - 1)
        if child.status != "ok":
            return child
        items.append(child.value)
        pos = child.consumed
        i = i + 1
    return _ok(Reply.Array(items), pos)

# Additive RESP2 incremental framer. Input bytes are scanned once, chunks are
# merged in logarithmic size classes, and a complete tree is decoded once.
struct Decoder:
    int64 _max_frame
    int64 _max_depth
    list[bytes] _chunks
    list[int64] _levels
    list[int64] _stack
    bytes _pending
    int64 _offset
    int64 _used
    int64 _state
    int64 _tag
    int64 _number
    int64 _digits
    int64 _sign
    int64 _left
    str _error
    int64 processed_bytes
    int64 copied_bytes
    int64 frames_decoded
    int64 _chunk_count
    int64 _stack_count

    def buffered_bytes(self) -> int64:
        return self._used

    def has_pending(self) -> bool:
        return self._offset < len(self._pending)

    def _fail(self, str message) -> int64:
        self._error = message
        return -1

    def _complete(self) -> int64:
        self._state = 0
        while self._stack_count > 0:
            self._stack_count = self._stack_count - 1
            auto remaining = self._stack[self._stack_count] - 1
            if remaining > 0:
                self._stack[self._stack_count] = remaining
                self._stack_count = self._stack_count + 1
                return 0
        return 1

    def _header_done(self) -> int64:
        if self._tag == 43 or self._tag == 45:
            return self._complete()
        if self._digits == 0:
            return self._fail("missing numeric digits")
        if self._tag == 58:
            return self._complete()
        if self._sign == -1 and self._digits == 1 and self._number == -1:
            return self._complete()
        if self._sign != 0:
            return self._fail("invalid length")
        auto count = -self._number
        if self._tag == 36:
            if count > self._max_frame - self._used - 2:
                return self._fail("bulk string exceeds byte limit")
            self._left = count
            self._state = 3
            if count == 0:
                self._state = 4
            return 0
        if self._stack_count >= self._max_depth:
            return self._fail("array nesting exceeds depth limit")
        if count > (self._max_frame - self._used) // 3:
            return self._fail("array exceeds byte limit")
        if count == 0:
            return self._complete()
        if self._stack_count == len(self._stack):
            self._stack.append(count)
        else:
            self._stack[self._stack_count] = count
        self._stack_count = self._stack_count + 1
        self._state = 0
        return 0

    def _step(self, int64 byte) -> int64:
        if self._state == 0:
            if byte != 43 and byte != 45 and byte != 58 and byte != 36 and byte != 42:
                return self._fail("unknown RESP2 type")
            self._tag = byte
            self._state = 1
            self._number = 0
            self._digits = 0
            self._sign = 0
            return 0
        if self._state == 2:
            if byte != 10:
                return self._fail("CR must be followed by LF")
            return self._header_done()
        if self._state == 4:
            if byte != 13:
                return self._fail("bulk string missing CRLF")
            self._state = 5
            return 0
        if self._state == 5:
            if byte != 10:
                return self._fail("bulk string missing CRLF")
            return self._complete()
        if byte == 13:
            self._state = 2
            return 0
        if byte == 10:
            return self._fail("bare LF in header")
        if self._tag == 43 or self._tag == 45:
            return 0
        if self._digits == 0 and self._sign == 0 and (byte == 43 or byte == 45):
            self._sign = 1
            if byte == 45:
                self._sign = -1
            return 0
        auto digit = byte - 48
        if digit < 0 or digit > 9:
            return self._fail("invalid decimal number")
        auto last = 7
        if self._sign == -1:
            last = 8
        if self._number < -922337203685477580 or self._number == -922337203685477580 and digit > last:
            return self._fail("integer overflow")
        self._number = self._number * 10 - digit
        self._digits = self._digits + 1
        return 0

    def _retain(self, bytes data):
        if len(data) == 0:
            return
        self.copied_bytes = self.copied_bytes + len(data)
        auto current = data
        auto level = _chunk_level(len(current))
        while self._chunk_count > 0:
            auto last = self._chunk_count - 1
            if self._levels[last] > level:
                break
            self._chunk_count = self._chunk_count - 1
            bytes previous = self._chunks[self._chunk_count]
            self._chunks[self._chunk_count] = b""
            self.copied_bytes = self.copied_bytes + len(previous) + len(current)
            current = previous + current
            level = _chunk_level(len(current))
        if self._chunk_count == len(self._chunks):
            self._chunks.append(current)
            self._levels.append(level)
        else:
            self._chunks[self._chunk_count] = current
            self._levels[self._chunk_count] = level
        self._chunk_count = self._chunk_count + 1

    def _finish(self) -> Decoded:
        auto frame = b""
        while self._chunk_count > 0:
            self._chunk_count = self._chunk_count - 1
            bytes previous = self._chunks[self._chunk_count]
            self._chunks[self._chunk_count] = b""
            self.copied_bytes = self.copied_bytes + len(previous) + len(frame)
            frame = previous + frame
        auto result = decode(frame, self._max_frame, self._max_depth)
        if result.status != "ok":
            self._error = "incremental frame validation failed: " + result.message
            return _invalid(self._error)
        self.frames_decoded = self.frames_decoded + 1
        self._used = 0
        self._state = 0
        return result

    # Drain coalesced frames with feed(b"") before supplying another chunk.
    # Errors poison this decoder; create a new one to start another stream.
    def feed(self, bytes chunk) -> Decoded:
        if len(self._error) > 0:
            return _invalid(self._error)
        if len(chunk) > 0:
            if self.has_pending():
                raise ValueError(
                    "Drain buffered replies before supplying another chunk",
                )
            self._pending = chunk
            self._offset = 0
        auto start = self._offset
        while self._offset < len(self._pending):
            if self._used >= self._max_frame:
                self._error = "frame exceeds byte limit"
                return _invalid(self._error)
            if self._state == 3:
                auto count = len(self._pending) - self._offset
                if count > self._left:
                    count = self._left
                if count > self._max_frame - self._used:
                    count = self._max_frame - self._used
                self._offset = self._offset + count
                self._used = self._used + count
                self.processed_bytes = self.processed_bytes + count
                self._left = self._left - count
                if self._left == 0:
                    self._state = 4
            else:
                auto byte = self._pending[self._offset]
                self._offset = self._offset + 1
                self._used = self._used + 1
                self.processed_bytes = self.processed_bytes + 1
                auto status = self._step(byte)
                if status < 0:
                    return _invalid(self._error)
                if status == 1:
                    self._retain(self._pending[start:self._offset])
                    return self._finish()
        self._retain(self._pending[start:self._offset])
        self._pending = b""
        self._offset = 0
        if self._used >= self._max_frame:
            self._error = "frame exceeds byte limit"
            return _invalid(self._error)
        return _incomplete()

def _chunk_level(int64 size) -> int64:
    auto level = 0
    while size > 1:
        size = (size + 1) // 2
        level = level + 1
    return level

def decoder(int64 max_frame = 16777216, int64 max_depth = 64) -> Decoder:
    if max_frame < 3 or max_depth < 0 or max_depth > 64:
        raise ValueError("invalid decoder limits")
    list[bytes] chunks = []
    list[int64] levels = []
    list[int64] stack = []
    return Decoder(
        max_frame,
        max_depth,
        chunks,
        levels,
        stack,
        b"",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "",
        0,
        0,
        0,
        0,
        0,
    )

# RESP3 values are separate from Reply so RESP2 consumers and exhaustive matches
# retain their existing contract. Attributes belong to their following value.
enum Resp3:
    Simple(bytes data)
    Error(bytes data)
    Integer(int64 value)
    Bulk(bytes data)
    Array(list[Resp3] items)
    NullBulk
    NullArray
    Null
    Boolean(bool value)
    Double(float64 value)
    BigNumber(bytes decimal)
    BlobError(bytes data)
    Verbatim(bytes format, bytes data)
    Map(list[Resp3Pair] pairs)
    Set(list[Resp3] items)
    Attributed(list[Resp3Pair] attributes, Resp3 value)
    Push(list[Resp3] items)

struct Resp3Pair:
    Resp3 key
    Resp3 value

struct Decoded3:
    str status
    Resp3 value
    int64 consumed
    str message

struct _Header3:
    str status
    int64 tag
    bytes raw
    int64 end
    str message

def _invalid3(str message) -> Decoded3:
    return Decoded3("invalid", Resp3.Null, 0, message)

def _incomplete3() -> Decoded3:
    return Decoded3("incomplete", Resp3.Null, 0, "")

def _ok3(Resp3 value, int64 end) -> Decoded3:
    return Decoded3("ok", value, end, "")

def _header3(bytes data, int64 start, int64 limit) -> _Header3:
    if start >= limit:
        return _Header3("invalid", 0, b"", 0, "frame exceeds byte limit")
    if start >= len(data):
        return _Header3("incomplete", 0, b"", 0, "")
    auto tag = data[start]
    if tag != 43 and tag != 45 and tag != 58 and tag != 36 and tag != 42 and tag != 95 and tag != 35 and tag != 44 and tag != 40 and tag != 33 and tag != 61 and tag != 37 and tag != 126 and tag != 124 and tag != 62:
        return _Header3(
            "invalid",
            tag,
            b"",
            0,
            "unknown RESP3 type or streamed encoding",
        )
    auto end = start + 1
    while True:
        if end >= limit:
            return _Header3("invalid", tag, b"", 0, "frame exceeds byte limit")
        if end >= len(data):
            return _Header3("incomplete", tag, b"", 0, "")
        if data[end] == 10:
            return _Header3("invalid", tag, b"", 0, "bare LF in header")
        if data[end] == 13:
            if end + 1 >= limit:
                return _Header3("invalid", tag, b"", 0, "frame exceeds byte limit")
            if end + 1 >= len(data):
                return _Header3("incomplete", tag, b"", 0, "")
            if data[end + 1] != 10:
                return _Header3("invalid", tag, b"", 0, "CR must be followed by LF")
            return _Header3("ok", tag, data[start + 1:end], end + 2, "")
        end = end + 1

def decode3(bytes data, int64 max_frame = 16777216, int64 max_depth = 64) -> Decoded3:
    if max_frame < 3 or max_depth < 0 or max_depth > 64:
        return _invalid3("invalid decoder limits")
    return _parse3(data, 0, max_frame, max_depth)

def _parse3(bytes data, int64 start, int64 limit, int64 depth) -> Decoded3:
    auto header = _header3(data, start, limit)
    if header.status == "invalid":
        return _invalid3(header.message)
    if header.status == "incomplete":
        return _incomplete3()
    auto tag = header.tag
    auto raw = header.raw
    auto pos = header.end
    if tag == 43:
        return _ok3(Resp3.Simple(raw), pos)
    if tag == 45:
        return _ok3(Resp3.Error(raw), pos)
    if tag == 95:
        if len(raw) != 0:
            return _invalid3("RESP3 null has a payload")
        return _ok3(Resp3.Null, pos)
    if tag == 35:
        if raw == b"t":
            return _ok3(Resp3.Boolean(True), pos)
        if raw == b"f":
            return _ok3(Resp3.Boolean(False), pos)
        return _invalid3("invalid RESP3 boolean")
    if tag == 44:
        if raw == b"nan":
            return _ok3(Resp3.Double(math.nan), pos)
        auto number = 0.0
        try:
            number = _score_value(Reply.Bulk(raw))
        except ProtocolError as message:
            return _invalid3(message)
        return _ok3(Resp3.Double(number), pos)
    if tag == 40:
        auto text = ""
        try:
            text = str(raw)
        except Exception:
            return _invalid3("invalid RESP3 big-number text")
        if text !~ m/\A[+-]?[0-9]+\z/:
            return _invalid3("invalid RESP3 big number")
        return _ok3(Resp3.BigNumber(raw), pos)
    auto count = 0
    try:
        count = _number(raw)
    except ValueError as message:
        if raw == b"?":
            return _invalid3("streamed encodings are unsupported")
        return _invalid3(message)
    if tag == 58:
        return _ok3(Resp3.Integer(count), pos)
    if raw == b"-1":
        if tag == 36:
            return _ok3(Resp3.NullBulk, pos)
        if tag == 42:
            return _ok3(Resp3.NullArray, pos)
    if len(raw) == 0 or raw[0] == 45 or raw[0] == 43:
        return _invalid3("invalid unsigned RESP3 length")
    if tag == 36 or tag == 33 or tag == 61:
        return _blob3(data, tag, count, pos, limit)
    return _aggregate3(data, tag, count, pos, limit, depth)

def _blob3(bytes data, int64 tag, int64 count, int64 pos, int64 limit) -> Decoded3:
    if count > limit - pos - 2:
        return _invalid3("blob exceeds byte limit")
    auto tail = pos + count
    if tail + 2 > len(data):
        return _incomplete3()
    if data[tail] != 13 or data[tail + 1] != 10:
        return _invalid3("blob missing CRLF")
    auto payload = data[pos:tail]
    if tag == 36:
        return _ok3(Resp3.Bulk(payload), tail + 2)
    if tag == 33:
        return _ok3(Resp3.BlobError(payload), tail + 2)
    if count < 4 or payload[3] != 58:
        return _invalid3("verbatim string requires a three-byte format and colon")
    return _ok3(Resp3.Verbatim(payload[0:3], payload[4:len(payload)]), tail + 2)

def _aggregate3(
    bytes data,
    int64 tag,
    int64 count,
    int64 pos,
    int64 limit,
    int64 depth,
) -> Decoded3:
    if depth == 0:
        return _invalid3("aggregate nesting exceeds depth limit")
    auto children = count
    if tag == 37 or tag == 124:
        if count > (limit - pos) // 6:
            return _invalid3("map exceeds byte limit")
        children = count * 2
    if children > (limit - pos) // 3:
        return _invalid3("aggregate exceeds byte limit")
    list[Resp3] items = []
    auto i = 0
    while i < children:
        auto child = _parse3(data, pos, limit, depth - 1)
        if child.status != "ok":
            return child
        items.append(child.value)
        pos = child.consumed
        i = i + 1
    if tag == 42:
        return _ok3(Resp3.Array(items), pos)
    if tag == 126:
        return _ok3(Resp3.Set(items), pos)
    if tag == 62:
        return _ok3(Resp3.Push(items), pos)
    list[Resp3Pair] pairs = []
    i = 0
    while i < children:
        pairs.append(Resp3Pair(items[i], items[i + 1]))
        i = i + 2
    if tag == 37:
        return _ok3(Resp3.Map(pairs), pos)
    auto value = _parse3(data, pos, limit, depth - 1)
    if value.status != "ok":
        return value
    return _ok3(Resp3.Attributed(pairs, value.value), value.consumed)

# RESP3 command frames can be interleaved with asynchronous pushes. Keep any
# incomplete suffix in a bounded byte buffer and grow socket reads geometrically
# so a large fragmented frame is decoded in O(n), not reparsed per small chunk.
struct Resp3Reader:
    int64 _max_frame
    int64 _max_depth
    bytes _buffer

    def buffered_bytes(self) -> int64:
        return len(self._buffer)

    def peek(self) -> Decoded3:
        return decode3(self._buffer, self._max_frame, self._max_depth)

    def append(self, bytes chunk):
        self._buffer = self._buffer + chunk

    def consume(self, int64 count):
        if count < 0 or count > len(self._buffer):
            raise ValueError("invalid RESP3 reader consumption")
        self._buffer = self._buffer[count:len(self._buffer)]

def resp3_reader(int64 max_frame = 16777216, int64 max_depth = 64) -> Resp3Reader:
    if max_frame < 3 or max_depth < 0 or max_depth > 64:
        raise ValueError("invalid RESP3 reader limits")
    return Resp3Reader(max_frame, max_depth, b"")

# Zap user exceptions carry a string message and have Exception as their parent.
struct ResponseError:
    str message

struct ProtocolError:
    str message

struct ConnectionError:
    str message

struct ConnectTimeout:
    str message

struct CommandTimeout:
    str message

struct BusyError:
    str message

# Redis prefixes are the bytes before the first ASCII space. Preserve the wire
# spelling, including unknown future prefixes, rather than classifying by text.
def error_prefix(bytes raw) -> bytes:
    auto i = 0
    while i < len(raw):
        if raw[i] == 32:
            break
        i = i + 1
    return raw[0:i]

def _error_text(bytes raw) -> str:
    try:
        return str(raw)
    except Exception:
        return "Redis server error (hex): " + hex.encode(raw)

# Convert relative budgets once; partial I/O never restarts the clock.
def _budget(int64 milliseconds) -> int64:
    if milliseconds == 0:
        return 0
    auto now = time.monotonic_ns()
    if milliseconds > (9223372036854775807 - now) // 1000000:
        return 9223372036854775807
    return now + milliseconds * 1000000

def _check_budget(int64 deadline):
    if deadline > 0 and time.monotonic_ns() >= deadline:
        raise CommandTimeout("Redis command deadline exceeded")

# TLS uses the verified stdlib entry point only. Failure never retries as TCP.
def _dial(Options cfg, int64 deadline) -> bytes:
    try:
        if deadline > 0 or len(cfg.resolved_ip) > 0:
            auto ip = cfg.resolved_ip
            if len(ip) == 0:
                ip = cfg.host
            if cfg.secure:
                return tls.connect_resolved(cfg.host, ip, cfg.port, deadline)
            return net.connect_ip_deadline(ip, cfg.port, deadline)
        if cfg.secure:
            return tls.connect(cfg.host, cfg.port)
        return net.connect(cfg.host, cfg.port)
    except TimeoutError:
        raise ConnectTimeout("Redis connect deadline exceeded")
    except Exception as e:
        if cfg.secure:
            raise ConnectionError("Redis TLS connect failed: " + e)
        raise ConnectionError("Redis connect failed: " + e)

def _close_socket(bytes socket, bool secure):
    if secure:
        tls.close_conn(socket)
    else:
        net.close_conn(socket)

def _send_socket(bytes socket, bytes data, bool secure) -> int64:
    if secure:
        return tls.send(socket, data)
    return net.send(socket, data)

def _receive_socket(bytes socket, int64 count, bool secure) -> bytes:
    if secure:
        return bytes(tls.recv(socket, count))
    return bytes(net.recv(socket, count))

# Public value configuration. connect_options snapshots this object, so later
# caller mutations cannot change an established connection's reconnect policy.
struct Options:
    str host
    int64 port
    int64 database
    str username
    str password
    str client_name
    bool authenticate
    int64 max_frame
    int64 max_depth
    int64 read_size
    bool secure
    int64 connect_timeout_ms
    int64 command_timeout_ms
    str resolved_ip

struct RedisEndpoint:
    bytes host
    int64 port

struct ClusterSlotRange:
    int64 first
    int64 last
    RedisEndpoint primary

struct ClusterRedirect:
    bytes kind
    int64 slot
    RedisEndpoint endpoint

struct ClusterSlotOverride:
    int64 slot
    RedisEndpoint primary

# Diagnostics deliberately omit BOTH credentials, even when authentication is off.
def describe_options(Options cfg) -> str:
    auto host = cfg.host
    if host.find(":") >= 0:
        host = "[" + host + "]"
    auto auth = ""
    if cfg.authenticate or len(cfg.username) > 0 or len(cfg.password) > 0:
        auth = "***@"
    auto scheme = "redis://"
    if cfg.secure:
        scheme = "rediss://"
    return scheme + auth + host + ":" + str(cfg.port) + "/" + str(cfg.database)

def options(
    str host = "127.0.0.1",
    int64 port = 6379,
    int64 database = 0,
    str username = "",
    str password = "",
    str client_name = "",
    bool authenticate = False,
    int64 max_frame = 16777216,
    int64 max_depth = 64,
    int64 read_size = 4096,
    bool secure = False,
    int64 connect_timeout_ms = 0,
    int64 command_timeout_ms = 0,
    str resolved_ip = "",
) -> Options:
    if len(host) == 0 or port < 1 or port > 65535:
        raise ValueError("invalid Redis host or port")
    if host =~ m{[\x00-\x20\/@?#\[\]\x7f]}:
        raise ValueError("invalid Redis host")
    if database < 0 or database > 2147483647:
        raise ValueError("invalid Redis database")
    if max_frame < 3 or max_depth < 0 or max_depth > 64 or read_size < 1 or read_size > 65536:
        raise ValueError("invalid Redis connection limits")
    if client_name !~ m/\A[\x21-\x7e]*\z/:
        raise ValueError("invalid Redis client name")
    if connect_timeout_ms < 0 or command_timeout_ms < 0:
        raise ValueError("Redis timeouts must not be negative")
    # Core validates numeric addresses without DNS. Reject hostname-only bounded
    # dials here instead of silently placing synchronous DNS outside the budget.
    if connect_timeout_ms > 0 and len(resolved_ip) == 0:
        if host !~ m/\A(?:[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+|[0-9a-fA-F]*:[0-9a-fA-F:.]+)\z/:
            raise ValueError(
                "Bounded Redis connect requires numeric host or resolved_ip",
            )
    if len(resolved_ip) > 0 and resolved_ip !~ m/\A(?:[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+|[0-9a-fA-F]*:[0-9a-fA-F:.]+)\z/:
        raise ValueError("Redis resolved_ip must be numeric")
    auto auth = authenticate or len(username) > 0 or len(password) > 0
    return Options(
        host,
        port,
        database,
        username,
        password,
        client_name,
        auth,
        max_frame,
        max_depth,
        read_size,
        secure,
        connect_timeout_ms,
        command_timeout_ms,
        resolved_ip,
    )

def _unquote(str value) -> str:
    if value !~ m/\A(?:[^%]|%[0-9A-Fa-f]{2})*\z/s:
        raise ValueError("invalid Redis URL percent escape")
    try:
        return str(bytes(url.unquote(value)))
    except Exception:
        raise ValueError("invalid Redis URL UTF-8")

def _uint(str value, int64 maximum) -> int64:
    if value !~ m/\A[0-9]+\z/:
        raise ValueError("invalid Redis URL number")
    auto number = 0
    try:
        number = _number(bytes(value))
    except ValueError:
        raise ValueError("invalid Redis URL number")
    if number > maximum:
        raise ValueError("invalid Redis URL number")
    return number

# Named regex captures separate URL structure without decoding delimiters first.
# Query db overrides path db; every supplied value is validated. Unsupported or
# repeated query keys, fragments, and implicit TLS are rejected.
def parse_url(str address) -> Options:
    if address =~ m/[\x00-\x20\x7f]/:
        raise ValueError("invalid Redis URL characters")
    auto parsed = address =~ m{\A(?<scheme>rediss?):\/\/(?<authority>[^\/?#]*)(?:\/(?<database>[^?#]*))?(?:\?(?<query>[^#]*))?\z}
    if not parsed:
        raise ValueError("invalid Redis URL or unsupported scheme")
    auto authority = parsed.group("authority")
    auto endpoint = authority =~ m/\A(?:(?<username>[^:@]*):(?<password>[^@]*)@)?(?<host>\[[^\]]+\]|[^:\[\]@]*)(?::(?<port>[0-9]+))?\z/
    if not endpoint:
        raise ValueError("invalid Redis URL authority")
    auto host = "127.0.0.1"
    auto raw_host = bytes(endpoint.group("host"))
    auto ipv6 = False
    if len(raw_host) > 0:
        if raw_host[0] == 91:
            ipv6 = True
            raw_host = raw_host[1:len(raw_host) - 1]
        host = _unquote(str(raw_host))
        if ipv6:
            if host.find(":") < 0:
                raise ValueError("invalid Redis URL IPv6 host")
        elif host.find(":") >= 0:
            raise ValueError("Redis URL IPv6 hosts require brackets")
    auto port = 6379
    if len(endpoint.group("port")) > 0:
        port = _uint(endpoint.group("port"), 65535)
    auto database = 0
    if len(parsed.group("database")) > 0:
        database = _uint(_unquote(parsed.group("database")), 2147483647)
    auto username = _unquote(endpoint.group("username"))
    auto password = _unquote(endpoint.group("password"))
    auto authenticate = authority.find("@") >= 0
    auto client_name = ""
    auto connect_timeout_ms = 0
    auto command_timeout_ms = 0
    auto resolved_ip = ""
    if address.find("?") >= 0:
        auto pairs = parsed.group("query").split("&")
        dict[str, bool] seen = {}
        for pair in pairs:
            auto option = pair =~ m/\A(?<key>[^=]+)=(?<value>.*)\z/s
            if not option:
                raise ValueError("invalid Redis URL query option")
            auto key = _unquote(option.group("key"))
            auto value = _unquote(option.group("value"))
            if key in seen:
                raise ValueError("duplicate Redis URL query option")
            seen[key] = True
            if key == "db":
                database = _uint(value, 2147483647)
            elif key == "client_name":
                client_name = value
            elif key == "connect_timeout_ms":
                connect_timeout_ms = _uint(value, 9223372036854775807)
            elif key == "command_timeout_ms":
                command_timeout_ms = _uint(value, 9223372036854775807)
            elif key == "resolved_ip":
                resolved_ip = value
            else:
                raise ValueError("unsupported Redis URL query option")
    auto cfg = options(
        host,
        port,
        database,
        username,
        password,
        client_name,
        authenticate,
        connect_timeout_ms=connect_timeout_ms,
        command_timeout_ms=command_timeout_ms,
        resolved_ip=resolved_ip,
    )
    cfg.secure = parsed.group("scheme") == "rediss"
    return cfg

def _endpoint_options(Options config, RedisEndpoint endpoint) -> Options:
    auto cfg = options(
        str(endpoint.host),
        endpoint.port,
        config.database,
        config.username,
        config.password,
        config.client_name,
        config.authenticate,
        config.max_frame,
        config.max_depth,
        config.read_size,
        config.secure,
        config.connect_timeout_ms,
        config.command_timeout_ms,
        "",
    )
    return cfg

def _redis_endpoint_equal(RedisEndpoint left, RedisEndpoint right) -> bool:
    return left.host == right.host and left.port == right.port

def _sentinel_master_reply(Reply reply) -> RedisEndpoint:
    match reply:
        case Reply.Array(parts):
            if len(parts) != 2:
                raise ProtocolError("Sentinel primary reply must contain host and port")
            auto host = b""
            try:
                host = _required_bulk(parts[0])
            except Exception:
                raise ProtocolError("Sentinel returned an invalid primary host")
            auto port_text = ""
            try:
                port_text = str(_required_bulk(parts[1]))
            except Exception:
                raise ProtocolError("Sentinel returned an invalid primary port")
            auto port = _uint(port_text, 65535)
            if len(host) == 0 or port == 0:
                raise ProtocolError("Sentinel returned an invalid primary endpoint")
            return RedisEndpoint(host, port)
        case Reply.NullArray:
            raise ProtocolError("Sentinel service has no known primary")
        case _:
            raise ProtocolError("Expected Sentinel primary address array")

def _sentinel_discover(list[Options] seeds, bytes service) -> RedisEndpoint:
    if len(seeds) == 0:
        raise ValueError("Sentinel requires at least one seed")
    for seed in seeds:
        try:
            auto sentinel = connect_options(seed)
            try:
                return _sentinel_master_reply(
                    sentinel.execute(
                        [b"SENTINEL", b"GET-MASTER-ADDR-BY-NAME", service],
                    ),
                )
            finally:
                sentinel.disconnect()
        except Exception:
            auto ignored = 0
    raise ConnectionError("Redis Sentinel could not discover a primary")

def sentinel_connect(
    list[Options] sentinel_seeds,
    bytes service,
    Options data_options,
) -> SentinelClient:
    if len(service) == 0:
        raise ValueError("Sentinel service name must not be empty")
    list[Options] seeds = []
    for seed in sentinel_seeds:
        if seed.connect_timeout_ms <= 0 or seed.command_timeout_ms <= 0:
            raise ValueError(
                "Sentinel seeds require positive connect and command timeouts",
            )
        seeds.append(_snapshot_options(seed))
    if data_options.connect_timeout_ms <= 0:
        raise ValueError("Redis data options require a positive connect timeout")
    auto data = _snapshot_options(data_options)
    auto endpoint = _sentinel_discover(seeds, service)
    return SentinelClient(
        seeds,
        service,
        data,
        endpoint,
        connect_options(_endpoint_options(data, endpoint)),
        Atomic(0),
    )

def _xor16(int64 left, int64 right) -> int64:
    auto result = 0
    auto place = 1
    for _ in range(0, 16):
        if left % 2 != right % 2:
            result = result + place
        left = left // 2
        right = right // 2
        place = place * 2
    return result

def _cluster_hash_key(bytes key) -> bytes:
    auto open = -1
    auto close = -1
    auto i = 0
    while i < len(key):
        if key[i] == 123:
            open = i
            break
        i = i + 1
    if open >= 0:
        i = open + 1
        while i < len(key):
            if key[i] == 125:
                close = i
                break
            i = i + 1
    if open >= 0 and close > open + 1:
        return key[open + 1:close]
    return key

def cluster_slot(bytes key) -> int64:
    auto tagged = _cluster_hash_key(key)
    auto crc = 0
    auto i = 0
    while i < len(tagged):
        crc = _xor16(crc, tagged[i] * 256)
        for _ in range(0, 8):
            auto high = crc >= 32768
            crc = crc * 2 % 65536
            if high:
                crc = _xor16(crc, 4129)
        i = i + 1
    return crc % 16384

# MSET preserves caller ordering and permits binary keys and values.
struct KeyValue:
    bytes key
    bytes value

# Redis server blocking timeouts are seconds, independent of client deadlines.
# Zero means wait indefinitely at Redis; reject non-finite/negative values before
# encoding or acquiring the connection. Keep sub-second precision on the wire.
def _blocking_timeout(float64 seconds) -> bytes:
    if math.isnan(seconds):
        raise ValueError("Redis server timeout must be finite and non-negative")
    if math.isinf(seconds):
        raise ValueError("Redis server timeout must be finite and non-negative")
    if seconds < 0.0:
        raise ValueError("Redis server timeout must be finite and non-negative")
    if seconds == 0.0:
        return b"0"
    return bytes(format(seconds, ".17g"))

def _requested_key(list[bytes] keys, bytes key) -> bool:
    for candidate in keys:
        if bytes(candidate) == key:
            return True
    return False

def _blocking_pop_reply(Reply reply, list[bytes] keys) -> Optional[KeyValue]:
    match reply:
        case Reply.NullArray:
            return None
        case Reply.Array(items):
            if len(items) != 2:
                raise ProtocolError("Expected blocking pop key/value pair")
            bytes key = _required_bulk(items[0])
            bytes value = _required_bulk(items[1])
            if not _requested_key(keys, key):
                raise ProtocolError("Blocking pop returned an unrequested key")
            return Optional.Some(KeyValue(key, value))
        case _:
            raise ProtocolError("Expected blocking pop pair or null array")

# Redis's blocked-list timeout path emits a null array even for BLMOVE,
# whose successful reply is bulk bytes. Keep ordinary bulk helpers strict.
def _blocking_move_reply(Reply reply) -> Optional[bytes]:
    match reply:
        case Reply.NullArray:
            return None
        case _:
            return _bulk_value(reply)

def _list_side(str side) -> bytes:
    auto normalized = side.upper()
    if normalized != "LEFT" and normalized != "RIGHT":
        raise ValueError("Redis list side must be LEFT or RIGHT")
    return bytes(normalized)

def _requires_dedicated(list[bytes] args) -> bool:
    auto name = _command_name(args)
    if _stream_command(args):
        return True
    if name == "XREAD":
        return _xread_blocks(args)
    if name == "XREADGROUP":
        return _xreadgroup_blocks(args)
    return name == "BLPOP" or name == "BRPOP" or name == "BLMOVE" or name == "BLMPOP" or name == "BRPOPLPUSH" or name == "BZPOPMIN" or name == "BZPOPMAX" or name == "BZMPOP"

# Hash fields stay separate from keys, with no implicit text conversion.
struct HashField:
    bytes field
    bytes value

# Float64 scores use round-trip formatting, independently of str(float)'s
# display precision. Members never undergo text conversion.
struct ScoredMember:
    bytes member
    float64 score

def _score_argument(float64 score) -> bytes:
    if math.isnan(score):
        raise ValueError("Redis sorted-set scores must not be NaN")
    if math.isinf(score):
        if score < 0.0:
            return b"-inf"
        return b"inf"
    return bytes(format(score, ".17g"))

def _score_value(Reply reply) -> float64:
    auto raw = _required_bulk(reply)
    auto text = ""
    try:
        text = str(raw)
    except Exception:
        raise ProtocolError("Redis score is not UTF-8 decimal text")
    if text == "inf" or text == "+inf":
        return math.inf
    if text == "-inf":
        return -math.inf
    if text !~ m/\A[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\z/:
        raise ProtocolError("Invalid Redis score syntax")
    # Validate before float(): malformed conversions abort in the pinned core.
    auto value = float(text)
    if math.isinf(value) or math.isnan(value):
        raise ProtocolError("Redis score is outside float64 range")
    if value == 0.0 and text !~ m/\A[+-]?(?:0+(?:\.0*)?|\.0+)(?:[eE][+-]?[0-9]+)?\z/:
        raise ProtocolError("Redis score underflows float64")
    return value

def _rank_value(Reply reply) -> Optional[int64]:
    match reply:
        case Reply.NullBulk:
            return None
        case Reply.Integer(value):
            if value < 0:
                raise ProtocolError("Redis rank must not be negative")
            return Optional.Some(value)
        case _:
            raise ProtocolError("Expected Redis integer or null rank reply")

def _required_bulk(Reply reply) -> bytes:
    match reply:
        case Reply.Bulk(data):
            return data
        case _:
            raise ProtocolError("Expected Redis non-null bulk reply")

def _bulk_array(Reply reply) -> list[bytes]:
    match reply:
        case Reply.Array(items):
            list[bytes] values = []
            for item in items:
                values.append(_required_bulk(item))
            return values
        case _:
            raise ProtocolError("Expected Redis non-null bulk array reply")

def _hash_pairs(Reply reply) -> list[HashField]:
    match reply:
        case Reply.Array(items):
            if len(items) % 2 != 0:
                raise ProtocolError("Hash reply has an odd number of items")
            list[HashField] fields = []
            auto i = 0
            while i < len(items):
                fields.append(
                    HashField(_required_bulk(items[i]), _required_bulk(items[i + 1])),
                )
                i = i + 2
            return fields
        case _:
            raise ProtocolError("Expected hash field/value array reply")

def _scored_members(Reply reply) -> list[ScoredMember]:
    match reply:
        case Reply.Array(items):
            if len(items) % 2 != 0:
                raise ProtocolError("Scored reply has an odd number of items")
            list[ScoredMember] members = []
            auto i = 0
            while i < len(items):
                members.append(
                    ScoredMember(_required_bulk(items[i]), _score_value(items[i + 1])),
                )
                i = i + 2
            return members
        case _:
            raise ProtocolError("Expected member/score array reply")

# Cursors stay decimal bytes, never narrowed to signed machine integers.
struct ScanPage:
    bytes cursor
    list[bytes] items

    def done(self) -> bool:
        return self.cursor == b"0"

struct HashScanPage:
    bytes cursor
    list[HashField] fields

    def done(self) -> bool:
        return self.cursor == b"0"

struct SortedScanPage:
    bytes cursor
    list[ScoredMember] members

    def done(self) -> bool:
        return self.cursor == b"0"

struct _ScanReply:
    bytes cursor
    Reply payload

def _valid_cursor(bytes cursor) -> bool:
    if len(cursor) == 0:
        return False
    if cursor[0] == 48:
        return len(cursor) == 1
    auto i = 0
    while i < len(cursor):
        if cursor[i] < 48 or cursor[i] > 57:
            return False
        i = i + 1
    return True

def _scan_args(
    list[bytes] args,
    bytes cursor,
    bytes pattern,
    int64 count,
) -> list[bytes]:
    if not _valid_cursor(cursor):
        raise ValueError("Scan cursor must be canonical unsigned decimal bytes")
    if count < 0:
        raise ValueError("Scan COUNT hint must not be negative")
    args.append(cursor)
    args.append(b"MATCH")
    args.append(pattern)
    if count > 0:
        args.append(b"COUNT")
        args.append(bytes(str(count)))
    return args

def _scan_reply(Reply reply) -> _ScanReply:
    match reply:
        case Reply.Array(items):
            if len(items) != 2:
                raise ProtocolError("Scan reply must contain cursor and page")
            auto cursor = _required_bulk(items[0])
            if not _valid_cursor(cursor):
                raise ProtocolError("Invalid scan reply cursor")
            return _ScanReply(cursor, items[1])
        case _:
            raise ProtocolError("Expected scan cursor/page array reply")

def _hash_command(bytes name, bytes key, list[bytes] fields) -> list[bytes]:
    if len(fields) == 0:
        raise ValueError("Redis hash field list must not be empty")
    list[bytes] args = [name, key]
    for field in fields:
        args.append(field)
    return args

def _bulk_value(Reply reply) -> Optional[bytes]:
    match reply:
        case Reply.Bulk(data):
            return Optional.Some(data)
        case Reply.NullBulk:
            return None
        case _:
            raise ProtocolError("Expected Redis bulk or null reply")

def _integer_value(Reply reply) -> int64:
    match reply:
        case Reply.Integer(value):
            return value
        case _:
            raise ProtocolError("Expected Redis integer reply")

def _status_value(Reply reply, bytes expected) -> bool:
    match reply:
        case Reply.Simple(data):
            if data == expected:
                return True
        case _:
            raise ProtocolError("Expected Redis status reply")
    raise ProtocolError("Unexpected Redis status value")

def _boolean_value(Reply reply) -> bool:
    auto value = _integer_value(reply)
    if value != 0 and value != 1:
        raise ProtocolError("Expected Redis zero or one reply")
    return value == 1

def _resp3_unwrap(Resp3 value) -> Resp3:
    match value:
        case Resp3.Attributed(attributes, nested):
            return _resp3_unwrap(nested)
        case _:
            return value

def _resp3_attributes(Resp3 value) -> list[Resp3Pair]:
    match value:
        case Resp3.Attributed(attributes, nested):
            return attributes
        case _:
            list[Resp3Pair] empty = []
            return empty

def _resp3_error_bytes(Resp3 value) -> Optional[bytes]:
    match _resp3_unwrap(value):
        case Resp3.Error(data):
            return Optional.Some(data)
        case Resp3.BlobError(data):
            return Optional.Some(data)
        case _:
            return None

def _resp3_is_null(Resp3 value) -> bool:
    match _resp3_unwrap(value):
        case Resp3.Null:
            return True
        case _:
            return False

def _none_bytes() -> Optional[bytes]:
    return None

def _resp3_reply(Resp3 value) -> Reply:
    match _resp3_unwrap(value):
        case Resp3.Simple(data):
            return Reply.Simple(data)
        case Resp3.Error(data):
            return Reply.Error(data)
        case Resp3.Integer(number):
            return Reply.Integer(number)
        case Resp3.Bulk(data):
            return Reply.Bulk(data)
        case Resp3.Array(items):
            list[Reply] values = []
            for item in items:
                values.append(_resp3_reply(item))
            return Reply.Array(values)
        case Resp3.NullBulk:
            return Reply.NullBulk
        case Resp3.NullArray:
            return Reply.NullArray
        case Resp3.Null:
            return Reply.NullBulk
        case Resp3.Boolean(value):
            if value:
                return Reply.Integer(1)
            return Reply.Integer(0)
        case Resp3.Double(value):
            if math.isnan(value):
                return Reply.Bulk(b"nan")
            if math.isinf(value):
                if value < 0.0:
                    return Reply.Bulk(b"-inf")
                return Reply.Bulk(b"inf")
            return Reply.Bulk(bytes(format(value, ".17g")))
        case Resp3.BigNumber(decimal):
            return Reply.Bulk(decimal)
        case Resp3.BlobError(data):
            return Reply.Error(data)
        case Resp3.Verbatim(format, data):
            return Reply.Bulk(data)
        case Resp3.Map(pairs):
            list[Reply] values = []
            for pair in pairs:
                values.append(_resp3_reply(pair.key))
                values.append(_resp3_reply(pair.value))
            return Reply.Array(values)
        case Resp3.Set(items):
            list[Reply] values = []
            for item in items:
                values.append(_resp3_reply(item))
            return Reply.Array(values)
        case Resp3.Push(items):
            raise ProtocolError("RESP3 push has no command reply representation")
        case _:
            raise ProtocolError("Unknown RESP3 value")

def _resp3_push(Resp3 value) -> bool:
    match _resp3_unwrap(value):
        case Resp3.Push(items):
            return True
        case _:
            return False

def _resp3_bulk(Resp3 value) -> bytes:
    match _resp3_unwrap(value):
        case Resp3.Bulk(data):
            return data
        case Resp3.Simple(data):
            return data
        case Resp3.Verbatim(format, data):
            return data
        case _:
            raise ProtocolError("Expected RESP3 string value")

def _resp3_integer(Resp3 value) -> int64:
    match _resp3_unwrap(value):
        case Resp3.Integer(number):
            return number
        case _:
            raise ProtocolError("Expected RESP3 integer value")

def _resp3_map_integer(Resp3 value, bytes key) -> int64:
    match _resp3_unwrap(value):
        case Resp3.Map(pairs):
            for pair in pairs:
                if _resp3_bulk(pair.key) == key:
                    return _resp3_integer(pair.value)
            raise ProtocolError("HELLO reply omitted protocol version")
        case _:
            raise ProtocolError("Expected HELLO map")

def _pubsub_event3(Resp3 value) -> PubSubEvent:
    match _resp3_unwrap(value):
        case Resp3.Push(items):
            if len(items) < 3:
                raise ProtocolError("Incomplete RESP3 Pub/Sub push")
            bytes kind = _resp3_bulk(items[0])
            if kind == b"pmessage":
                if len(items) != 4:
                    raise ProtocolError("Expected four-part pattern push")
                return PubSubEvent.PatternMessage(
                    _resp3_bulk(items[1]),
                    _resp3_bulk(items[2]),
                    _resp3_bulk(items[3]),
                )
            if len(items) != 3:
                raise ProtocolError("Expected three-part Pub/Sub push")
            if kind == b"message":
                return PubSubEvent.Message(_resp3_bulk(items[1]), _resp3_bulk(items[2]))
            auto count = _resp3_integer(items[2])
            if count < 0:
                raise ProtocolError("Negative RESP3 subscription count")
            if kind == b"subscribe":
                return PubSubEvent.Subscribed(_resp3_bulk(items[1]), count)
            if kind == b"psubscribe":
                return PubSubEvent.PatternSubscribed(_resp3_bulk(items[1]), count)
            if kind == b"unsubscribe":
                if _resp3_is_null(items[1]):
                    return PubSubEvent.Unsubscribed(_none_bytes(), count)
                return PubSubEvent.Unsubscribed(
                    Optional.Some(_resp3_bulk(items[1])),
                    count,
                )
            if kind == b"punsubscribe":
                if _resp3_is_null(items[1]):
                    return PubSubEvent.PatternUnsubscribed(_none_bytes(), count)
                return PubSubEvent.PatternUnsubscribed(
                    Optional.Some(_resp3_bulk(items[1])),
                    count,
                )
            raise ProtocolError("Unknown RESP3 Pub/Sub push")
        case _:
            raise ProtocolError("Expected RESP3 push event")

def _keys_command(bytes name, list[bytes] keys) -> list[bytes]:
    if len(keys) == 0:
        raise ValueError("Redis key list must not be empty")
    list[bytes] args = [name]
    for key in keys:
        args.append(key)
    return args

def _validate_sha(bytes sha):
    if len(sha) != 40:
        raise ValueError("Redis script SHA must contain 40 hexadecimal bytes")
    auto i = 0
    while i < len(sha):
        auto c = sha[i]
        if not (c >= 48 and c <= 57 or c >= 97 and c <= 102 or c >= 65 and c <= 70):
            raise ValueError("Redis script SHA must contain 40 hexadecimal bytes")
        i = i + 1

def _script_command(
    bytes name,
    bytes script,
    list[bytes] keys,
    list[bytes] arguments,
) -> list[bytes]:
    list[bytes] command = [name, script, bytes(str(len(keys)))]
    for key in keys:
        command.append(key)
    for argument in arguments:
        command.append(argument)
    return command

# A registered script never retries transport/protocol errors or ordinary server
# errors. An explicit NOSCRIPT means EVALSHA did not execute the script.
struct RegisteredScript:
    bytes _source
    bytes _sha

    def sha(self) -> bytes:
        return self._sha

    def run(
        self,
        Connection connection,
        list[bytes] keys,
        list[bytes] arguments,
    ) -> Reply:
        auto result = connection.execute_raw(
            _script_command(b"EVALSHA", self._sha, keys, arguments),
        )
        match result:
            case Reply.Error(data):
                if error_prefix(data) == b"NOSCRIPT":
                    return connection.eval(self._source, keys, arguments)
                raise ResponseError(_error_text(data))
            case _:
                return result

def register_script(bytes source) -> RegisteredScript:
    return RegisteredScript(source, bytes(crypto.sha1(source)))

# A builder is local to one caller. Execution seals it before touching the
# connection; failures never leave a replayable queue. Replies retain errors.
struct Pipeline:
    list[bytes] _commands
    int64 _bytes
    int64 _max_commands
    int64 _max_bytes
    bool _sealed
    bool _dedicated_commands

    def queue(self, list[bytes] args):
        if self._sealed:
            raise ValueError("Redis pipeline has already been executed")
        if len(self._commands) >= self._max_commands:
            raise ValueError("Redis pipeline command limit exceeded")
        if len(args) == 0:
            raise ValueError("a Redis command must have at least one argument")
        auto remaining = self._max_bytes - self._bytes
        auto size = len(str(len(args))) + 3
        for arg in args:
            auto header = len(str(len(arg))) + 5
            if size > remaining or header > remaining - size:
                raise ValueError("Redis pipeline byte limit exceeded")
            size = size + header
            if len(arg) > remaining - size:
                raise ValueError("Redis pipeline byte limit exceeded")
            size = size + len(arg)
        if size > remaining:
            raise ValueError("Redis pipeline byte limit exceeded")
        auto dedicated = _requires_dedicated(args)
        self._commands.append(encode_command(args))
        self._dedicated_commands = self._dedicated_commands or dedicated
        self._bytes = self._bytes + size

    def execute(self, Connection connection) -> list[Reply]:
        if self._sealed:
            raise ValueError("Redis pipeline has already been executed")
        self._sealed = True
        auto count = len(self._commands)
        auto wire = _join_bytes(self._commands, 0, count)
        list[bytes] empty = []
        self._commands = empty
        self._bytes = 0
        return connection._pipeline(wire, count)

def pipeline(int64 max_commands = 1024, int64 max_bytes = 16777216) -> Pipeline:
    if max_commands < 1 or max_bytes < 1:
        raise ValueError("invalid Redis pipeline limits")
    list[bytes] commands = []
    return Pipeline(commands, 0, max_commands, max_bytes, False, False)

# A session holds the Connection ownership token across caller code as well as
# network operations. Its per-method token rejects concurrent session calls.
enum TransactionResult:
    Committed(list[Reply] replies)
    Conflict
    Aborted(bytes error)

def _command_name(list[bytes] args) -> str:
    if len(args) == 0:
        raise ValueError("a Redis command must have at least one argument")
    try:
        return str(args[0]).upper()
    except Exception:
        return ""

def _stream_command(list[bytes] args) -> bool:
    auto name = _command_name(args)
    return name == "SUBSCRIBE" or name == "PSUBSCRIBE" or name == "SSUBSCRIBE" or name == "MONITOR" or name == "HELLO" or name == "SYNC" or name == "PSYNC" or name == "REPLCONF"

def _transaction_args(list[bytes] args):
    auto name = _command_name(args)
    if _stream_command(args) or name == "MULTI" or name == "EXEC" or name == "DISCARD" or name == "WATCH" or name == "UNWATCH" or name == "RESET" or name == "AUTH" or name == "SELECT" or name == "CLIENT" or name == "QUIT" or name == "ASKING" or name == "READONLY" or name == "READWRITE":
        raise ValueError(
            "Use the transaction session methods for transaction control; session-changing commands are unsupported",
        )

struct Transaction:
    Connection _connection
    Atomic[int64] _state
    Atomic[int64] _lease_guard
    bool _multi
    int64 _queued

    def _enter(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ValueError("Redis transaction session has finished")
            raise BusyError("Redis transaction session is in use")

    def _leave(self):
        self._state.compare_and_swap(1, 0)

    def _finish(self, bool healthy):
        if self._state.load() == 2:
            return
        if healthy:
            self._connection._state.store(0)
        else:
            self._connection._invalidate()
        self._state.store(2)
        self._lease_guard.store(0)

    def _request(self, bytes wire) -> Reply:
        auto healthy = False
        try:
            auto deadline = _budget(self._connection._config.command_timeout_ms)
            net.set_deadline_ns(self._connection._socket, deadline)
            auto reply = self._connection._wire_reply(wire, deadline)
            net.set_deadline_ns(self._connection._socket, 0)
            healthy = True
            match reply:
                case Reply.Error(data):
                    self._connection._last_error = data
                case _:
                    return reply
            return reply
        finally:
            if not healthy:
                self._finish(False)

    def _transaction_ok(self, bytes wire):
        auto reply = self._request(wire)
        match reply:
            case Reply.Error(data):
                raise ResponseError(_error_text(data))
            case Reply.Simple(data):
                if data != b"OK":
                    self._finish(False)
                    raise ProtocolError("Expected transaction OK status")
            case _:
                self._finish(False)
                raise ProtocolError("Expected transaction OK status")

    def watch(self, list[bytes] keys):
        auto wire = encode_command(_keys_command(b"WATCH", keys))
        self._enter()
        try:
            if self._multi:
                raise ValueError("WATCH must precede MULTI")
            self._transaction_ok(wire)
        finally:
            self._leave()

    def unwatch(self):
        self._enter()
        try:
            if self._multi:
                raise ValueError("UNWATCH must precede MULTI")
            self._transaction_ok(encode_command([b"UNWATCH"]))
        finally:
            self._leave()

    # Read application state between WATCH and MULTI. Complete server errors
    # preserve the session so the caller can retry locally or discard explicitly.
    def execute(self, list[bytes] args) -> Reply:
        _transaction_args(args)
        auto wire = encode_command(args)
        self._enter()
        try:
            if self._multi:
                raise ValueError("Use queue after MULTI")
            auto reply = self._request(wire)
            match reply:
                case Reply.Error(data):
                    raise ResponseError(_error_text(data))
                case _:
                    return reply
        finally:
            self._leave()

    def multi(self):
        self._enter()
        try:
            if self._multi:
                raise ValueError("MULTI has already started")
            self._transaction_ok(encode_command([b"MULTI"]))
            self._multi = True
        finally:
            self._leave()

    # Queue errors remain Reply.Error values. EXEC then reports EXECABORT;
    # errors produced while executing valid commands remain inside the result.
    def queue(self, list[bytes] args) -> Reply:
        _transaction_args(args)
        auto wire = encode_command(args)
        self._enter()
        try:
            if not self._multi:
                raise ValueError("MULTI must precede queue")
            auto reply = self._request(wire)
            match reply:
                case Reply.Simple(data):
                    if data != b"QUEUED":
                        self._finish(False)
                        raise ProtocolError("Expected transaction QUEUED status")
                    self._queued = self._queued + 1
                case Reply.Error(data):
                    return reply
                case _:
                    self._finish(False)
                    raise ProtocolError("Expected transaction QUEUED status")
            return reply
        finally:
            self._leave()

    def exec(self) -> TransactionResult:
        self._enter()
        try:
            if not self._multi:
                raise ValueError("MULTI must precede EXEC")
            auto reply = self._request(encode_command([b"EXEC"]))
            match reply:
                case Reply.Array(items):
                    if len(items) != self._queued:
                        self._finish(False)
                        raise ProtocolError(
                            "EXEC result count differs from queued commands",
                        )
                    self._finish(True)
                    return TransactionResult.Committed(items)
                case Reply.NullArray:
                    self._finish(True)
                    return TransactionResult.Conflict
                case Reply.Error(data):
                    if error_prefix(data) == b"EXECABORT":
                        self._finish(True)
                        return TransactionResult.Aborted(data)
                    self._finish(False)
                    raise ResponseError(_error_text(data))
                case _:
                    self._finish(False)
                    raise ProtocolError("Expected EXEC array, conflict, or EXECABORT")
        finally:
            self._leave()

    # Idempotent cleanup for try/finally. A failure or cancellation while cleaning
    # discards the socket; uncertain transaction state is never returned to idle.
    def discard(self):
        if self._state.load() == 2:
            return
        self._enter()
        auto healthy = False
        try:
            auto command = b"UNWATCH"
            if self._multi:
                command = b"DISCARD"
            self._transaction_ok(encode_command([command]))
            healthy = True
        finally:
            self._finish(healthy)

# Stream IDs remain byte tokens; each component is uint64 and can exceed int64.
# Ordered pairs preserve binary and repeated field names without a dict.
struct StreamField:
    bytes field
    bytes value

struct StreamEntry:
    bytes id
    list[StreamField] fields

struct StreamCursor:
    bytes key
    bytes after_id

struct StreamBatch:
    bytes key
    list[StreamEntry] entries

struct StreamPendingConsumer:
    bytes name
    int64 pending

struct StreamPendingSummary:
    int64 total
    Optional[bytes] smallest_id
    Optional[bytes] largest_id
    list[StreamPendingConsumer] consumers

struct StreamAutoClaim:
    bytes next_id
    list[StreamEntry] entries
    list[bytes] deleted_ids

def _stream_component(str part) -> bool:
    if part !~ m/\A(?:0|[1-9][0-9]*)\z/:
        return False
    if len(part) > 20:
        return False
    if len(part) == 20 and part > "18446744073709551615":
        return False
    return True

def _stream_id_valid(bytes raw) -> bool:
    auto text = ""
    try:
        text = str(raw)
    except Exception:
        return False
    auto parsed = text =~ m/\A(?<ms>0|[1-9][0-9]*)-(?<seq>0|[1-9][0-9]*)\z/
    if not parsed:
        return False
    if not _stream_component(parsed.group("ms")):
        return False
    return _stream_component(parsed.group("seq"))

def _stream_position(bytes raw, bool allow_dollar):
    if allow_dollar and raw == b"$":
        return
    if _stream_id_valid(raw):
        return
    auto text = ""
    try:
        text = str(raw)
    except Exception:
        raise ValueError("Invalid Redis stream ID")
    if not _stream_component(text):
        raise ValueError("Invalid Redis stream ID")

def _stream_boundary(bytes raw):
    if raw == b"-" or raw == b"+":
        return
    auto token = raw
    if len(token) > 0 and token[0] == 40:
        token = token[1:len(token)]
    _stream_position(token, False)

def _stream_entry(Reply reply) -> StreamEntry:
    match reply:
        case Reply.Array(parts):
            if len(parts) != 2:
                raise ProtocolError("Expected stream ID/fields entry")
            bytes id = _required_bulk(parts[0])
            if not _stream_id_valid(id):
                raise ProtocolError("Invalid returned Redis stream ID")
            match parts[1]:
                case Reply.Array(values):
                    if len(values) == 0 or len(values) % 2 != 0:
                        raise ProtocolError("Expected ordered stream field/value pairs")
                    list[StreamField] fields = []
                    auto i = 0
                    while i < len(values):
                        fields.append(
                            StreamField(
                                _required_bulk(values[i]),
                                _required_bulk(values[i + 1]),
                            ),
                        )
                        i = i + 2
                    return StreamEntry(id, fields)
                case _:
                    raise ProtocolError("Expected stream field array")
        case _:
            raise ProtocolError("Expected stream entry array")

def _stream_entries(Reply reply) -> list[StreamEntry]:
    match reply:
        case Reply.Array(items):
            list[StreamEntry] entries = []
            for item in items:
                entries.append(_stream_entry(item))
            return entries
        case _:
            raise ProtocolError("Expected stream entries array")

def _stream_batches(Reply reply, list[bytes] requested) -> list[StreamBatch]:
    list[StreamBatch] batches = []
    match reply:
        case Reply.NullArray:
            return batches
        case Reply.NullBulk:
            # RESP3 null is normalized to the existing null-bulk variant.
            return batches
        case Reply.Array(items):
            for item in items:
                match item:
                    case Reply.Array(parts):
                        if len(parts) != 2:
                            raise ProtocolError("Expected stream key/entries batch")
                        bytes key = _required_bulk(parts[0])
                        if not _requested_key(requested, key):
                            raise ProtocolError("XREAD returned an unrequested stream")
                        batches.append(StreamBatch(key, _stream_entries(parts[1])))
                    case _:
                        raise ProtocolError("Expected XREAD stream array")
            return batches
        case _:
            raise ProtocolError("Expected XREAD array or null array")

def _stream_group_batches(Reply reply, list[bytes] requested) -> list[StreamBatch]:
    return _stream_batches(reply, requested)

def _stream_optional_id(Reply reply) -> Optional[bytes]:
    match reply:
        case Reply.NullBulk:
            return None
        case Reply.Bulk(data):
            if not _stream_id_valid(data):
                raise ProtocolError("Invalid pending stream ID")
            return Optional.Some(data)
        case _:
            raise ProtocolError("Expected stream ID or null")

def _stream_reply_has_id(Reply reply) -> bool:
    match reply:
        case Reply.NullBulk:
            return False
        case Reply.Bulk(_):
            return True
        case _:
            raise ProtocolError("Expected stream ID or null")

def _stream_pending_summary(Reply reply) -> StreamPendingSummary:
    match reply:
        case Reply.Array(items):
            if len(items) != 4:
                raise ProtocolError("XPENDING summary must contain four fields")
            auto total = _stream_nonnegative(items[0])
            auto has_smallest = _stream_reply_has_id(items[1])
            auto has_largest = _stream_reply_has_id(items[2])
            auto smallest = _stream_optional_id(items[1])
            auto largest = _stream_optional_id(items[2])
            list[StreamPendingConsumer] consumers = []
            match items[3]:
                case Reply.Array(entries):
                    for entry in entries:
                        match entry:
                            case Reply.Array(parts):
                                if len(parts) != 2:
                                    raise ProtocolError(
                                        "Invalid XPENDING consumer entry",
                                    )
                                consumers.append(
                                    StreamPendingConsumer(
                                        _required_bulk(parts[0]),
                                        _stream_pending_count(parts[1]),
                                    ),
                                )
                            case _:
                                raise ProtocolError("Invalid XPENDING consumer entry")
                case Reply.NullArray:
                    auto ignored = 0
                case _:
                    raise ProtocolError("Expected XPENDING consumer array")
            if total == 0 and (has_smallest or has_largest or len(consumers) != 0):
                raise ProtocolError("Empty XPENDING summary contains pending state")
            if total > 0 and (not has_smallest or not has_largest):
                raise ProtocolError("Non-empty XPENDING summary omits its ID range")
            return StreamPendingSummary(total, smallest, largest, consumers)
        case _:
            raise ProtocolError("Expected XPENDING summary array")

def _stream_autoclaim(Reply reply) -> StreamAutoClaim:
    match reply:
        case Reply.Array(parts):
            if len(parts) != 2 and len(parts) != 3:
                raise ProtocolError("XAUTOCLAIM reply must contain two or three fields")
            auto next_id = _required_bulk(parts[0])
            if not _stream_id_valid(next_id):
                raise ProtocolError("Invalid XAUTOCLAIM continuation ID")
            auto entries = _stream_entries(parts[1])
            list[bytes] deleted = []
            if len(parts) == 3:
                deleted = _bulk_array(parts[2])
                for id in deleted:
                    if not _stream_id_valid(id):
                        raise ProtocolError("Invalid XAUTOCLAIM deleted ID")
            return StreamAutoClaim(next_id, entries, deleted)
        case _:
            raise ProtocolError("Expected XAUTOCLAIM array")

def _stream_group_id(bytes raw):
    if raw == b">":
        return
    _stream_position(raw, False)

def _stream_count(list[bytes] args, int64 count):
    if count < 0:
        raise ValueError("Redis stream COUNT must not be negative")
    if count > 0:
        args.append(b"COUNT")
        args.append(bytes(str(count)))

def _stream_nonnegative(Reply reply) -> int64:
    auto value = _integer_value(reply)
    if value < 0:
        raise ProtocolError("Expected non-negative stream integer")
    return value

def _stream_pending_count(Reply reply) -> int64:
    match reply:
        case Reply.Integer(value):
            if value < 0:
                raise ProtocolError("Expected non-negative stream integer")
            return value
        case Reply.Bulk(data):
            auto text = ""
            try:
                text = str(data)
            except Exception:
                raise ProtocolError("Invalid XPENDING consumer count")
            if not _stream_component(text):
                raise ProtocolError("Invalid XPENDING consumer count")
            try:
                return _uint(text, 9223372036854775807)
            except ValueError:
                raise ProtocolError("Invalid XPENDING consumer count")
        case _:
            raise ProtocolError("Expected XPENDING consumer count")

# Look only at XREAD options, never at binary keys/IDs following STREAMS.
def _xread_blocks(list[bytes] args) -> bool:
    auto i = 1
    while i < len(args):
        auto option = _command_name([args[i]])
        if option == "STREAMS":
            return False
        if option == "BLOCK":
            return True
        if option == "COUNT":
            i = i + 2
        else:
            i = i + 1
    return False

def _xreadgroup_blocks(list[bytes] args) -> bool:
    auto i = 1
    while i < len(args):
        auto option = _command_name([args[i]])
        if option == "STREAMS":
            return False
        if option == "BLOCK":
            return True
        if option == "GROUP" or option == "COUNT":
            i = i + 2
        else:
            i = i + 1
    return False

# RESP2 Pub/Sub is a separate pull session. No receiver coroutine or message
# queue is created; messages remain in the bounded parser/socket/server buffers.
enum PubSubEvent:
    Subscribed(bytes topic, int64 count)
    PatternSubscribed(bytes pattern, int64 count)
    Unsubscribed(Optional[bytes] topic, int64 count)
    PatternUnsubscribed(Optional[bytes] pattern, int64 count)
    Message(bytes topic, bytes payload)
    PatternMessage(bytes pattern, bytes topic, bytes payload)

def _pubsub_count(Reply reply) -> int64:
    auto count = _integer_value(reply)
    if count < 0:
        raise ProtocolError("Negative Pub/Sub subscription count")
    return count

def _pubsub_event(Reply reply) -> PubSubEvent:
    match reply:
        case Reply.Array(items):
            if len(items) < 3:
                raise ProtocolError("Incomplete Pub/Sub event")
            bytes kind = _required_bulk(items[0])
            if kind == b"pmessage":
                if len(items) != 4:
                    raise ProtocolError("Expected four-part pattern message")
                return PubSubEvent.PatternMessage(
                    _required_bulk(items[1]),
                    _required_bulk(items[2]),
                    _required_bulk(items[3]),
                )
            if len(items) != 3:
                raise ProtocolError("Expected three-part Pub/Sub event")
            if kind == b"message":
                return PubSubEvent.Message(
                    _required_bulk(items[1]),
                    _required_bulk(items[2]),
                )
            auto count = _pubsub_count(items[2])
            if kind == b"subscribe":
                return PubSubEvent.Subscribed(_required_bulk(items[1]), count)
            if kind == b"psubscribe":
                return PubSubEvent.PatternSubscribed(_required_bulk(items[1]), count)
            if kind == b"unsubscribe":
                return PubSubEvent.Unsubscribed(_bulk_value(items[1]), count)
            if kind == b"punsubscribe":
                return PubSubEvent.PatternUnsubscribed(_bulk_value(items[1]), count)
            raise ProtocolError("Unknown RESP2 Pub/Sub event")
        case _:
            raise ProtocolError("Expected Pub/Sub array event")

def _pubsub_ack_count(PubSubEvent event) -> int64:
    match event:
        case PubSubEvent.Subscribed(name, count):
            return count
        case PubSubEvent.PatternSubscribed(name, count):
            return count
        case PubSubEvent.Unsubscribed(name, count):
            return count
        case PubSubEvent.PatternUnsubscribed(name, count):
            return count
        case _:
            return -1

def _unsubscribe_wire(bytes command, list[bytes] names) -> bytes:
    list[bytes] args = [command]
    for name in names:
        args.append(name)
    return encode_command(args)

struct PubSub:
    Connection _connection
    Atomic[int64] _state
    Atomic[int64] _lease_guard
    Atomic[int64] _count

    def _enter(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis Pub/Sub session is closed")
            raise BusyError("Redis Pub/Sub session is in use")

    def _leave(self):
        self._state.compare_and_swap(1, 0)

    def _close_session(self):
        if self._state.load() == 2:
            return
        self._connection._invalidate()
        self._count.store(0)
        self._state.store(2)
        self._lease_guard.store(0)

    def is_closed(self) -> bool:
        return self._state.load() == 2

    # This is the count from the last consumed acknowledgement, not a prediction
    # based on commands whose acknowledgements have not been read yet.
    def subscription_count(self) -> int64:
        return self._count.load()

    def _send_control(self, bytes wire):
        self._enter()
        auto healthy = False
        try:
            auto deadline = _budget(self._connection._config.command_timeout_ms)
            net.set_deadline_ns(self._connection._socket, deadline)
            self._connection._write_wire(wire, deadline)
            _check_budget(deadline)
            net.set_deadline_ns(self._connection._socket, 0)
            healthy = True
        finally:
            if not healthy:
                self._close_session()
            self._leave()

    # Send control commands without silently consuming/dropping intervening
    # messages. Acknowledgements are events read with next_event().
    def subscribe(self, list[bytes] channels):
        self._send_control(encode_command(_keys_command(b"SUBSCRIBE", channels)))

    def psubscribe(self, list[bytes] patterns):
        self._send_control(encode_command(_keys_command(b"PSUBSCRIBE", patterns)))

    # An empty list asks Redis to unsubscribe all of this kind. Patterns and
    # literal channels are independent; total count reaches zero after both.
    def unsubscribe(self, list[bytes] channels):
        self._send_control(_unsubscribe_wire(b"UNSUBSCRIBE", channels))

    def punsubscribe(self, list[bytes] patterns):
        self._send_control(_unsubscribe_wire(b"PUNSUBSCRIBE", patterns))

    def next_event(self, int64 timeout_ms = -1) -> PubSubEvent:
        if timeout_ms < -1:
            raise ValueError("Pub/Sub timeout must be -1, zero, or positive")
        auto budget_ms = timeout_ms
        if budget_ms == -1:
            budget_ms = self._connection._config.command_timeout_ms
        self._enter()
        auto healthy = False
        try:
            auto deadline = _budget(budget_ms)
            net.set_deadline_ns(self._connection._socket, deadline)
            if self._connection._protocol3:
                auto frame = self._connection._next_push(deadline)
                auto event = _pubsub_event3(frame)
                auto count = _pubsub_ack_count(event)
                if count >= 0:
                    self._count.store(count)
                net.set_deadline_ns(self._connection._socket, 0)
                healthy = True
                return event
            auto reply = self._connection._read_reply(deadline)
            match reply:
                case Reply.Error(data):
                    self._connection._last_error = data
                    raise ResponseError(_error_text(data))
                case _:
                    auto event = _pubsub_event(reply)
                    auto count = _pubsub_ack_count(event)
                    if count >= 0:
                        self._count.store(count)
                    net.set_deadline_ns(self._connection._socket, 0)
                    healthy = True
                    return event
        finally:
            if not healthy:
                self._close_session()
            self._leave()

    # Cancel/join a blocked reader's scope first; concurrent close is rejected
    # instead of freeing an SSL/parser while an operation still owns it.
    def disconnect(self):
        if self._state.load() == 2:
            return
        self._enter()
        self._close_session()

# One connection has one active owner. The atomic state protects both socket
# access and the reply buffer: 0=idle, 1=owned, 2=closed. Sharing requires callers
# to serialize access; a concurrent execute/disconnect is rejected, never queued.
struct Connection:
    bytes _socket
    Decoder _reader
    Resp3Reader _reader3
    Atomic[int64] _state
    int64 _max_frame
    int64 _max_depth
    int64 _read_size
    Options _config
    bytes _last_error
    bool _protocol3
    list[Resp3] _pushes
    int64 _push_head
    int64 _push_limit
    list[Resp3Pair] _last_attributes

    def _invalidate(self):
        _close_socket(self._socket, self._config.secure)
        self._reader = decoder(self._max_frame, self._max_depth)
        self._reader3 = resp3_reader(self._max_frame, self._max_depth)
        list[Resp3] empty = []
        self._pushes = empty
        self._push_head = 0
        list[Resp3Pair] no_attributes = []
        self._last_attributes = no_attributes
        self._state.store(2)

    def protocol(self) -> str:
        if self._protocol3:
            return "RESP3"
        return "RESP2"

    def last_attributes(self) -> list[Resp3Pair]:
        return self._last_attributes

    def _queue_push(self, Resp3 value):
        if len(self._pushes) - self._push_head >= self._push_limit:
            raise ProtocolError("RESP3 push queue limit exceeded")
        self._pushes.append(value)

    def _take_push(self) -> Resp3:
        if self._push_head >= len(self._pushes):
            raise ProtocolError("RESP3 push queue is empty")
        auto value = self._pushes[self._push_head]
        self._push_head = self._push_head + 1
        if self._push_head == len(self._pushes):
            list[Resp3] empty = []
            self._pushes = empty
            self._push_head = 0
        return value

    def _next_push(self, int64 deadline) -> Resp3:
        if self._push_head < len(self._pushes):
            return self._take_push()
        while True:
            auto value = self._read_resp3_frame(deadline)
            if not _resp3_push(value):
                raise ProtocolError("Expected RESP3 push while awaiting push")
            return value

    def next_push(self, int64 timeout_ms = -1) -> Resp3:
        if not self._protocol3:
            raise ValueError(
                "RESP3 push reads require an explicitly negotiated connection",
            )
        if timeout_ms < -1:
            raise ValueError("RESP3 push timeout must be -1, zero, or positive")
        auto budget_ms = timeout_ms
        if budget_ms == -1:
            budget_ms = self._config.command_timeout_ms
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        auto healthy = False
        try:
            auto deadline = _budget(budget_ms)
            net.set_deadline_ns(self._socket, deadline)
            auto value = self._next_push(deadline)
            net.set_deadline_ns(self._socket, 0)
            healthy = True
            return value
        finally:
            if healthy:
                self._state.store(0)
            else:
                self._invalidate()

    def pubsub(self) -> PubSub:
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        return PubSub(self, Atomic(0), Atomic(0), Atomic(0))

    def publish(self, bytes topic, bytes payload) -> int64:
        auto count = _integer_value(self.execute([b"PUBLISH", topic, payload]))
        if count < 0:
            raise ProtocolError("Negative PUBLISH recipient count")
        return count

    def transaction(self) -> Transaction:
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        return Transaction(self, Atomic(0), Atomic(0), False, 0)

    # Reset has its own finite budget, even when ordinary command timeouts are
    # disabled. It is only called while the pool lease is exclusively owned.
    def _pool_reset(self, int64 timeout_ms):
        if not self._state.compare_and_swap(0, 1):
            raise BusyError("Redis connection is not idle for pool reset")
        auto healthy = False
        try:
            auto deadline = _budget(timeout_ms)
            net.set_deadline_ns(self._socket, deadline)
            auto reply = self._wire_reply(encode_command([b"RESET"]), deadline)
            _status_value(reply, b"RESET")
            self._initialize(deadline)
            net.set_deadline_ns(self._socket, 0)
            self._last_error = b""
            healthy = True
        finally:
            if healthy:
                self._state.store(0)
            else:
                self._invalidate()

    def is_closed(self) -> bool:
        return self._state.load() == 2

    def disconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                return
            raise BusyError("Redis connection is in use")
        self._invalidate()

    # The last consumed top-level server error is retained losslessly until a
    # subsequent server error or an explicit reconnect. Success does not clear it.
    def last_error(self) -> bytes:
        return self._last_error

    def reconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if not self._state.compare_and_swap(2, 1):
                raise BusyError("Redis connection is in use")
        _close_socket(self._socket, self._config.secure)
        self._reader = decoder(self._max_frame, self._max_depth)
        self._reader3 = resp3_reader(self._max_frame, self._max_depth)
        list[Resp3] empty_pushes = []
        self._pushes = empty_pushes
        self._push_head = 0
        list[Resp3Pair] empty_attributes = []
        self._last_attributes = empty_attributes
        self._last_error = b""
        auto opened = False
        try:
            auto deadline = _budget(self._config.connect_timeout_ms)
            self._socket = _dial(self._config, deadline)
            net.set_deadline_ns(self._socket, deadline)
            self._initialize(deadline)
            net.set_deadline_ns(self._socket, 0)
            opened = True
        finally:
            if opened:
                self._state.store(0)
            else:
                _close_socket(self._socket, self._config.secure)
                self._reader = decoder(self._max_frame, self._max_depth)
                self._reader3 = resp3_reader(self._max_frame, self._max_depth)
                list[Resp3Pair] empty_attributes = []
                self._last_attributes = empty_attributes
                self._last_error = b""
                self._state.store(2)

    def _session_ok(self, list[bytes] args, str stage, int64 deadline):
        auto reply = Reply.NullBulk
        try:
            reply = self._wire_reply(encode_command(args), deadline)
        except CommandTimeout:
            raise ConnectTimeout("Redis session initialization deadline exceeded")
        match reply:
            case Reply.Simple(data):
                if data != b"OK":
                    raise ProtocolError("Unexpected Redis session reply: " + stage)
            case Reply.Error(data):
                raise ResponseError("Redis session initialization failed: " + stage)
            case _:
                raise ProtocolError("Unexpected Redis session reply: " + stage)

    def _initialize(self, int64 deadline):
        if self._protocol3:
            list[bytes] hello = [b"HELLO", b"3"]
            if self._config.authenticate:
                hello.append(b"AUTH")
                if len(self._config.username) > 0:
                    hello.append(bytes(self._config.username))
                else:
                    hello.append(b"default")
                hello.append(bytes(self._config.password))
            if len(self._config.client_name) > 0:
                hello.append(b"SETNAME")
                hello.append(bytes(self._config.client_name))
            auto negotiated = Resp3.Null
            try:
                negotiated = self._wire_resp3(encode_command(hello), deadline)
            except CommandTimeout:
                raise ConnectTimeout("Redis RESP3 negotiation deadline exceeded")
            match _resp3_unwrap(negotiated):
                case Resp3.Error(data):
                    self._last_error = data
                    raise ResponseError(
                        "Redis RESP3 negotiation failed: " + _error_text(data),
                    )
                case Resp3.BlobError(data):
                    self._last_error = data
                    raise ResponseError(
                        "Redis RESP3 negotiation failed: " + _error_text(data),
                    )
                case Resp3.Map(pairs):
                    if _resp3_map_integer(negotiated, b"proto") != 3:
                        raise ProtocolError("Redis HELLO did not negotiate RESP3")
                case _:
                    raise ProtocolError("Redis HELLO returned an invalid RESP3 reply")
            if self._config.database != 0:
                self._session_ok(
                    [b"SELECT", bytes(str(self._config.database))],
                    "SELECT",
                    deadline,
                )
            return
        if self._config.authenticate:
            if len(self._config.username) > 0:
                self._session_ok(
                    [
                        b"AUTH",
                        bytes(self._config.username),
                        bytes(self._config.password),
                    ],
                    "AUTH",
                    deadline,
                )
            else:
                self._session_ok(
                    [b"AUTH", bytes(self._config.password)],
                    "AUTH",
                    deadline,
                )
        if self._config.database != 0:
            self._session_ok(
                [b"SELECT", bytes(str(self._config.database))],
                "SELECT",
                deadline,
            )
        if len(self._config.client_name) > 0:
            self._session_ok(
                [b"CLIENT", b"SETNAME", bytes(self._config.client_name)],
                "CLIENT SETNAME",
                deadline,
            )

    def ping(self) -> bool:
        return _status_value(self.execute([b"PING"]), b"PONG")

    def get(self, bytes key) -> Optional[bytes]:
        return _bulk_value(self.execute([b"GET", key]))

    def set_value(
        self,
        bytes key,
        bytes value,
        bool nx = False,
        bool xx = False,
        int64 ex = 0,
        int64 px = 0,
    ) -> bool:
        if nx and xx:
            raise ValueError("SET NX and XX are mutually exclusive")
        list[bytes] args = [b"SET", key, value]
        if ex < 0 or px < 0:
            raise ValueError("SET expiry durations must not be negative")
        if ex > 0 and px > 0:
            raise ValueError("SET EX and PX are mutually exclusive")
        if ex > 0:
            args.append(b"EX")
            args.append(bytes(str(ex)))
        if px > 0:
            args.append(b"PX")
            args.append(bytes(str(px)))
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        auto reply = self.execute(args)
        match reply:
            case Reply.NullBulk:
                if nx or xx:
                    return False
                raise ProtocolError("Unexpected null SET reply")
            case _:
                return _status_value(reply, b"OK")

    def mget(self, list[bytes] keys) -> list[Optional[bytes]]:
        auto reply = self.execute(_keys_command(b"MGET", keys))
        match reply:
            case Reply.Array(items):
                if len(items) != len(keys):
                    raise ProtocolError("MGET reply length differs from key count")
                list[Optional[bytes]] values = []
                for item in items:
                    values.append(_bulk_value(item))
                return values
            case _:
                raise ProtocolError("Expected MGET array reply")

    def mset(self, list[KeyValue] pairs) -> bool:
        if len(pairs) == 0:
            raise ValueError("MSET pairs must not be empty")
        list[bytes] args = [b"MSET"]
        for pair in pairs:
            args.append(pair.key)
            args.append(pair.value)
        return _status_value(self.execute(args), b"OK")

    def delete(self, list[bytes] keys) -> int64:
        return _integer_value(self.execute(_keys_command(b"DEL", keys)))

    def unlink(self, list[bytes] keys) -> int64:
        return _integer_value(self.execute(_keys_command(b"UNLINK", keys)))

    def exists(self, list[bytes] keys) -> int64:
        return _integer_value(self.execute(_keys_command(b"EXISTS", keys)))

    def key_type(self, bytes key) -> bytes:
        match self.execute([b"TYPE", key]):
            case Reply.Simple(data):
                return data
            case _:
                raise ProtocolError("Expected TYPE status reply")

    def incr(self, bytes key) -> int64:
        return _integer_value(self.execute([b"INCR", key]))

    def decr(self, bytes key) -> int64:
        return _integer_value(self.execute([b"DECR", key]))

    def incrby(self, bytes key, int64 amount) -> int64:
        return _integer_value(self.execute([b"INCRBY", key, bytes(str(amount))]))

    def decrby(self, bytes key, int64 amount) -> int64:
        return _integer_value(self.execute([b"DECRBY", key, bytes(str(amount))]))

    def expire(self, bytes key, int64 seconds) -> bool:
        return _boolean_value(self.execute([b"EXPIRE", key, bytes(str(seconds))]))

    def pexpire(self, bytes key, int64 milliseconds) -> bool:
        return _boolean_value(self.execute([b"PEXPIRE", key, bytes(str(milliseconds))]))

    def ttl(self, bytes key) -> int64:
        return _integer_value(self.execute([b"TTL", key]))

    def pttl(self, bytes key) -> int64:
        return _integer_value(self.execute([b"PTTL", key]))

    def persist(self, bytes key) -> bool:
        return _boolean_value(self.execute([b"PERSIST", key]))

    def hset(self, bytes key, list[HashField] fields) -> int64:
        if len(fields) == 0:
            raise ValueError("HSET field/value list must not be empty")
        list[bytes] args = [b"HSET", key]
        for entry in fields:
            args.append(entry.field)
            args.append(entry.value)
        return _integer_value(self.execute(args))

    def hget(self, bytes key, bytes field) -> Optional[bytes]:
        return _bulk_value(self.execute([b"HGET", key, field]))

    def hmget(self, bytes key, list[bytes] fields) -> list[Optional[bytes]]:
        auto reply = self.execute(_hash_command(b"HMGET", key, fields))
        match reply:
            case Reply.Array(items):
                if len(items) != len(fields):
                    raise ProtocolError("HMGET reply length differs from field count")
                list[Optional[bytes]] values = []
                for item in items:
                    values.append(_bulk_value(item))
                return values
            case _:
                raise ProtocolError("Expected HMGET array reply")

    def hgetall(self, bytes key) -> list[HashField]:
        return _hash_pairs(self.execute([b"HGETALL", key]))

    def hdel(self, bytes key, list[bytes] fields) -> int64:
        return _integer_value(self.execute(_hash_command(b"HDEL", key, fields)))

    def hexists(self, bytes key, bytes field) -> bool:
        return _boolean_value(self.execute([b"HEXISTS", key, field]))

    def hlen(self, bytes key) -> int64:
        return _integer_value(self.execute([b"HLEN", key]))

    def hincrby(self, bytes key, bytes field, int64 amount) -> int64:
        return _integer_value(
            self.execute([b"HINCRBY", key, field, bytes(str(amount))]),
        )

    def _list_push(self, bytes command, bytes key, list[bytes] values) -> int64:
        if len(values) == 0:
            raise ValueError("Redis list push requires at least one element")
        list[bytes] args = [command, key]
        for value in values:
            args.append(value)
        return _integer_value(self.execute(args))

    def lpush(self, bytes key, list[bytes] values) -> int64:
        return self._list_push(b"LPUSH", key, values)

    def rpush(self, bytes key, list[bytes] values) -> int64:
        return self._list_push(b"RPUSH", key, values)

    def lpop(self, bytes key) -> Optional[bytes]:
        return _bulk_value(self.execute([b"LPOP", key]))

    def rpop(self, bytes key) -> Optional[bytes]:
        return _bulk_value(self.execute([b"RPOP", key]))

    def llen(self, bytes key) -> int64:
        return _integer_value(self.execute([b"LLEN", key]))

    def lrange(self, bytes key, int64 start, int64 stop) -> list[bytes]:
        auto reply = self.execute([b"LRANGE", key, bytes(str(start)), bytes(str(stop))])
        match reply:
            case Reply.Array(items):
                list[bytes] values = []
                for item in items:
                    values.append(_required_bulk(item))
                return values
            case _:
                raise ProtocolError("Expected LRANGE array reply")

    def lindex(self, bytes key, int64 index) -> Optional[bytes]:
        return _bulk_value(self.execute([b"LINDEX", key, bytes(str(index))]))

    def lset(self, bytes key, int64 index, bytes value) -> bool:
        return _status_value(
            self.execute([b"LSET", key, bytes(str(index)), value]),
            b"OK",
        )

    def ltrim(self, bytes key, int64 start, int64 stop) -> bool:
        return _status_value(
            self.execute([b"LTRIM", key, bytes(str(start)), bytes(str(stop))]),
            b"OK",
        )

    def _blocking_pop(
        self,
        bytes command,
        list[bytes] keys,
        float64 server_timeout_seconds,
    ) -> Optional[KeyValue]:
        auto args = _keys_command(command, keys)
        args.append(_blocking_timeout(server_timeout_seconds))
        return _blocking_pop_reply(self.execute(args), keys)

    def blpop(
        self,
        list[bytes] keys,
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[KeyValue]:
        return self._blocking_pop(b"BLPOP", keys, server_timeout_seconds)

    def brpop(
        self,
        list[bytes] keys,
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[KeyValue]:
        return self._blocking_pop(b"BRPOP", keys, server_timeout_seconds)

    def blmove(
        self,
        bytes source,
        bytes destination,
        str source_side = "LEFT",
        str destination_side = "RIGHT",
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[bytes]:
        auto args = [
            b"BLMOVE",
            source,
            destination,
            _list_side(source_side),
            _list_side(destination_side),
            _blocking_timeout(server_timeout_seconds),
        ]
        return _blocking_move_reply(self.execute(args))

    def xadd(self, bytes key, list[StreamField] fields, bytes id = b"*") -> bytes:
        if id != b"*" and not _stream_id_valid(id):
            raise ValueError("XADD requires * or a full canonical stream ID")
        if len(fields) == 0:
            raise ValueError("XADD requires at least one field/value pair")
        list[bytes] args = [b"XADD", key, id]
        for field in fields:
            args.append(field.field)
            args.append(field.value)
        bytes created = _required_bulk(self.execute(args))
        if not _stream_id_valid(created):
            raise ProtocolError("Invalid XADD returned ID")
        return created

    def xlen(self, bytes key) -> int64:
        return _stream_nonnegative(self.execute([b"XLEN", key]))

    def xrange(
        self,
        bytes key,
        bytes start = b"-",
        bytes end = b"+",
        int64 count = 0,
    ) -> list[StreamEntry]:
        _stream_boundary(start)
        _stream_boundary(end)
        list[bytes] args = [b"XRANGE", key, start, end]
        _stream_count(args, count)
        return _stream_entries(self.execute(args))

    def xrevrange(
        self,
        bytes key,
        bytes end = b"+",
        bytes start = b"-",
        int64 count = 0,
    ) -> list[StreamEntry]:
        _stream_boundary(end)
        _stream_boundary(start)
        list[bytes] args = [b"XREVRANGE", key, end, start]
        _stream_count(args, count)
        return _stream_entries(self.execute(args))

    def xdel(self, bytes key, list[bytes] ids) -> int64:
        if len(ids) == 0:
            raise ValueError("XDEL requires at least one ID")
        list[bytes] args = [b"XDEL", key]
        for id in ids:
            if not _stream_id_valid(bytes(id)):
                raise ValueError("XDEL requires full canonical stream IDs")
            args.append(id)
        return _stream_nonnegative(self.execute(args))

    # -1 omits BLOCK, zero asks Redis to block indefinitely, positive values
    # bound Redis's wait in milliseconds. Client command deadlines are separate.
    def xread(
        self,
        list[StreamCursor] streams,
        int64 count = 0,
        int64 block_ms = -1,
    ) -> list[StreamBatch]:
        if len(streams) == 0 or block_ms < -1:
            raise ValueError("Invalid XREAD streams or BLOCK value")
        list[bytes] args = [b"XREAD"]
        _stream_count(args, count)
        if block_ms >= 0:
            args.append(b"BLOCK")
            args.append(bytes(str(block_ms)))
        args.append(b"STREAMS")
        list[bytes] keys = []
        for stream in streams:
            _stream_position(stream.after_id, True)
            args.append(stream.key)
            keys.append(stream.key)
        for stream in streams:
            args.append(stream.after_id)
        return _stream_batches(self.execute(args), keys)

    def xgroup_create(
        self,
        bytes key,
        bytes group,
        bytes start_id = b"$",
        bool mkstream = False,
    ):
        _stream_position(start_id, True)
        list[bytes] args = [b"XGROUP", b"CREATE", key, group, start_id]
        if mkstream:
            args.append(b"MKSTREAM")
        _status_value(self.execute(args), b"OK")

    def xgroup_destroy(self, bytes key, bytes group) -> bool:
        auto removed = _stream_nonnegative(
            self.execute([b"XGROUP", b"DESTROY", key, group]),
        )
        if removed != 0 and removed != 1:
            raise ProtocolError("XGROUP DESTROY result must be zero or one")
        return removed == 1

    def xreadgroup(
        self,
        bytes group,
        bytes consumer,
        list[StreamCursor] streams,
        int64 count = 0,
        int64 block_ms = -1,
        bool noack = False,
    ) -> list[StreamBatch]:
        if len(streams) == 0 or block_ms < -1:
            raise ValueError("Invalid XREADGROUP streams or BLOCK value")
        list[bytes] args = [b"XREADGROUP", b"GROUP", group, consumer]
        _stream_count(args, count)
        if block_ms >= 0:
            args.append(b"BLOCK")
            args.append(bytes(str(block_ms)))
        if noack:
            args.append(b"NOACK")
        args.append(b"STREAMS")
        list[bytes] keys = []
        for stream in streams:
            _stream_group_id(stream.after_id)
            args.append(stream.key)
            keys.append(stream.key)
        for stream in streams:
            args.append(stream.after_id)
        return _stream_group_batches(self.execute(args), keys)

    def xack(self, bytes key, bytes group, list[bytes] ids) -> int64:
        if len(ids) == 0:
            raise ValueError("XACK requires at least one stream ID")
        list[bytes] args = [b"XACK", key, group]
        for id in ids:
            if not _stream_id_valid(bytes(id)):
                raise ValueError("XACK requires full canonical stream IDs")
            args.append(id)
        return _stream_nonnegative(self.execute(args))

    def xpending(self, bytes key, bytes group) -> StreamPendingSummary:
        return _stream_pending_summary(self.execute([b"XPENDING", key, group]))

    def xautoclaim(
        self,
        bytes key,
        bytes group,
        bytes consumer,
        int64 min_idle_ms,
        bytes start_id = b"0-0",
        int64 count = 0,
    ) -> StreamAutoClaim:
        if min_idle_ms < 0 or count < 0:
            raise ValueError("XAUTOCLAIM idle time and COUNT must be non-negative")
        if not _stream_id_valid(start_id):
            raise ValueError("XAUTOCLAIM requires a full canonical start ID")
        list[bytes] args = [
            b"XAUTOCLAIM",
            key,
            group,
            consumer,
            bytes(str(min_idle_ms)),
            start_id,
        ]
        if count > 0:
            args.append(b"COUNT")
            args.append(bytes(str(count)))
        return _stream_autoclaim(self.execute(args))

    def _set_members(self, bytes command, bytes key, list[bytes] members) -> int64:
        if len(members) == 0:
            raise ValueError("Redis set mutation requires at least one member")
        list[bytes] args = [command, key]
        for member in members:
            args.append(member)
        return _integer_value(self.execute(args))

    def sadd(self, bytes key, list[bytes] members) -> int64:
        return self._set_members(b"SADD", key, members)

    def srem(self, bytes key, list[bytes] members) -> int64:
        return self._set_members(b"SREM", key, members)

    def sismember(self, bytes key, bytes member) -> bool:
        return _boolean_value(self.execute([b"SISMEMBER", key, member]))

    def scard(self, bytes key) -> int64:
        return _integer_value(self.execute([b"SCARD", key]))

    def smembers(self, bytes key) -> list[bytes]:
        return _bulk_array(self.execute([b"SMEMBERS", key]))

    def spop(self, bytes key) -> Optional[bytes]:
        return _bulk_value(self.execute([b"SPOP", key]))

    def sunion(self, list[bytes] keys) -> list[bytes]:
        return _bulk_array(self.execute(_keys_command(b"SUNION", keys)))

    def sinter(self, list[bytes] keys) -> list[bytes]:
        return _bulk_array(self.execute(_keys_command(b"SINTER", keys)))

    def sdiff(self, list[bytes] keys) -> list[bytes]:
        return _bulk_array(self.execute(_keys_command(b"SDIFF", keys)))

    def zadd(self, bytes key, list[ScoredMember] members) -> int64:
        if len(members) == 0:
            raise ValueError("ZADD requires at least one score/member pair")
        list[bytes] args = [b"ZADD", key]
        for entry in members:
            args.append(_score_argument(entry.score))
            args.append(entry.member)
        return _integer_value(self.execute(args))

    def zrem(self, bytes key, list[bytes] members) -> int64:
        if len(members) == 0:
            raise ValueError("ZREM requires at least one member")
        list[bytes] args = [b"ZREM", key]
        for member in members:
            args.append(member)
        return _integer_value(self.execute(args))

    def zcard(self, bytes key) -> int64:
        return _integer_value(self.execute([b"ZCARD", key]))

    def zscore(self, bytes key, bytes member) -> Optional[float64]:
        auto reply = self.execute([b"ZSCORE", key, member])
        match reply:
            case Reply.NullBulk:
                return None
            case _:
                return Optional.Some(_score_value(reply))

    def zincrby(self, bytes key, bytes member, float64 amount) -> float64:
        return _score_value(
            self.execute([b"ZINCRBY", key, _score_argument(amount), member]),
        )

    def zrank(self, bytes key, bytes member) -> Optional[int64]:
        return _rank_value(self.execute([b"ZRANK", key, member]))

    def zrevrank(self, bytes key, bytes member) -> Optional[int64]:
        return _rank_value(self.execute([b"ZREVRANK", key, member]))

    def zrange(self, bytes key, int64 start, int64 stop) -> list[bytes]:
        return _bulk_array(
            self.execute([b"ZRANGE", key, bytes(str(start)), bytes(str(stop))]),
        )

    def zrange_withscores(
        self,
        bytes key,
        int64 start,
        int64 stop,
    ) -> list[ScoredMember]:
        return _scored_members(
            self.execute(
                [b"ZRANGE", key, bytes(str(start)), bytes(str(stop)), b"WITHSCORES"],
            ),
        )

    def scan(
        self,
        bytes cursor = b"0",
        bytes pattern = b"*",
        int64 count = 0,
    ) -> ScanPage:
        auto page = _scan_reply(
            self.execute(_scan_args([b"SCAN"], cursor, pattern, count)),
        )
        return ScanPage(page.cursor, _bulk_array(page.payload))

    def hscan(
        self,
        bytes key,
        bytes cursor = b"0",
        bytes pattern = b"*",
        int64 count = 0,
    ) -> HashScanPage:
        auto page = _scan_reply(
            self.execute(_scan_args([b"HSCAN", key], cursor, pattern, count)),
        )
        return HashScanPage(page.cursor, _hash_pairs(page.payload))

    def sscan(
        self,
        bytes key,
        bytes cursor = b"0",
        bytes pattern = b"*",
        int64 count = 0,
    ) -> ScanPage:
        auto page = _scan_reply(
            self.execute(_scan_args([b"SSCAN", key], cursor, pattern, count)),
        )
        return ScanPage(page.cursor, _bulk_array(page.payload))

    def zscan(
        self,
        bytes key,
        bytes cursor = b"0",
        bytes pattern = b"*",
        int64 count = 0,
    ) -> SortedScanPage:
        auto page = _scan_reply(
            self.execute(_scan_args([b"ZSCAN", key], cursor, pattern, count)),
        )
        return SortedScanPage(page.cursor, _scored_members(page.payload))

    def eval(self, bytes source, list[bytes] keys, list[bytes] arguments) -> Reply:
        return self.execute(_script_command(b"EVAL", source, keys, arguments))

    def evalsha(self, bytes sha, list[bytes] keys, list[bytes] arguments) -> Reply:
        _validate_sha(sha)
        return self.execute(_script_command(b"EVALSHA", sha, keys, arguments))

    def script_load(self, bytes source) -> bytes:
        auto sha = _required_bulk(self.execute([b"SCRIPT", b"LOAD", source]))
        try:
            _validate_sha(sha)
        except ValueError:
            raise ProtocolError("SCRIPT LOAD returned an invalid SHA")
        if sha != bytes(crypto.sha1(source)):
            raise ProtocolError("SCRIPT LOAD SHA differs from source digest")
        return sha

    def script_exists(self, list[bytes] shas) -> list[bool]:
        if len(shas) == 0:
            raise ValueError("SCRIPT EXISTS requires at least one SHA")
        list[bytes] args = [b"SCRIPT", b"EXISTS"]
        for sha in shas:
            _validate_sha(sha)
            args.append(sha)
        match self.execute(args):
            case Reply.Array(items):
                if len(items) != len(shas):
                    raise ProtocolError(
                        "SCRIPT EXISTS result count differs from SHA count",
                    )
                list[bool] exists = []
                for item in items:
                    exists.append(_boolean_value(item))
                return exists
            case _:
                raise ProtocolError("Expected SCRIPT EXISTS array reply")

    def execute(self, list[bytes] args) -> Reply:
        return self._exchange(args, True)

    # For callers implementing command-specific error policy, preserve errors as
    # Reply.Error values. Both entry points consume exactly one complete reply.
    def execute_raw(self, list[bytes] args) -> Reply:
        return self._exchange(args, False)

    # No automatic retry: a failed write/read may already have executed remotely.
    def _exchange(self, list[bytes] args, bool raise_response_errors) -> Reply:
        # Local argument failures do not damage or acquire the connection.
        auto wire = encode_command(args)
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        auto healthy = False
        try:
            auto deadline = _budget(self._config.command_timeout_ms)
            net.set_deadline_ns(self._socket, deadline)
            auto result = self._wire_reply(wire, deadline)
            net.set_deadline_ns(self._socket, 0)
            healthy = True
            match result:
                case Reply.Error(data):
                    self._last_error = data
                    if raise_response_errors:
                        raise ResponseError(_error_text(data))
                case _:
                    return result
            return result
        finally:
            if healthy:
                self._state.store(0)
            else:
                self._invalidate()

    # All replies, including server errors, are returned in submission order.
    # Ownership spans the entire write and drain. Never return a partial result.
    def _pipeline(self, bytes wire, int64 count) -> list[Reply]:
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        auto healthy = False
        list[Reply] replies = []
        try:
            auto deadline = _budget(self._config.command_timeout_ms)
            net.set_deadline_ns(self._socket, deadline)
            if count > 0:
                self._write_wire(wire, deadline)
                for i in range(0, count):
                    auto result = Reply.NullBulk
                    if self._protocol3:
                        auto frame = self._read_resp3_frame(deadline)
                        while _resp3_push(frame):
                            self._queue_push(frame)
                            frame = self._read_resp3_frame(deadline)
                        result = _resp3_reply(frame)
                    else:
                        result = self._read_reply(deadline)
                    match result:
                        case Reply.Error(data):
                            self._last_error = data
                            replies.append(result)
                        case _:
                            replies.append(result)
            net.set_deadline_ns(self._socket, 0)
            healthy = True
            return replies
        finally:
            if healthy:
                self._state.store(0)
            else:
                self._invalidate()

    # Caller holds the ownership state for the entire command/session setup.
    def _wire_reply(self, bytes wire, int64 deadline) -> Reply:
        if self._protocol3:
            return _resp3_reply(self._wire_resp3(wire, deadline))
        self._write_wire(wire, deadline)
        return self._read_reply(deadline)

    # Rich RESP3 entry point. Push frames are delivered through next_push and
    # do not consume the command's reply slot.
    def execute3(self, list[bytes] args) -> Resp3:
        return self._exchange3(args, True)

    def execute3_raw(self, list[bytes] args) -> Resp3:
        return self._exchange3(args, False)

    def _exchange3(self, list[bytes] args, bool raise_response_errors) -> Resp3:
        if not self._protocol3:
            raise ValueError(
                "execute3 requires an explicitly negotiated RESP3 connection",
            )
        auto wire = encode_command(args)
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis connection is closed")
            raise BusyError("Redis connection is in use")
        auto healthy = False
        try:
            auto deadline = _budget(self._config.command_timeout_ms)
            net.set_deadline_ns(self._socket, deadline)
            auto result = self._wire_resp3(wire, deadline)
            net.set_deadline_ns(self._socket, 0)
            healthy = True
            match _resp3_unwrap(result):
                case Resp3.Error(data):
                    self._last_error = data
                    if raise_response_errors:
                        raise ResponseError(_error_text(data))
                case Resp3.BlobError(data):
                    self._last_error = data
                    if raise_response_errors:
                        raise ResponseError(_error_text(data))
                case _:
                    auto ignored = 0
            return result
        finally:
            if healthy:
                self._state.store(0)
            else:
                self._invalidate()

    def _wire_resp3(self, bytes wire, int64 deadline) -> Resp3:
        self._write_wire(wire, deadline)
        while True:
            auto value = self._read_resp3_frame(deadline)
            if _resp3_push(value):
                self._queue_push(value)
                continue
            match value:
                case Resp3.Attributed(attributes, child):
                    self._last_attributes = attributes
                case _:
                    list[Resp3Pair] empty_attributes = []
                    self._last_attributes = empty_attributes
            return value

    def _read_resp3_frame(self, int64 deadline) -> Resp3:
        while True:
            auto decoded = self._reader3.peek()
            match decoded.status:
                case "ok":
                    self._reader3.consume(decoded.consumed)
                    return decoded.value
                case "invalid":
                    raise ProtocolError("Invalid RESP3 frame: " + decoded.message)
                case _:
                    auto ignored = 0
            _check_budget(deadline)
            auto buffered = self._reader3.buffered_bytes()
            auto room = self._max_frame - buffered
            if room <= 0:
                raise ProtocolError("Redis reply exceeds byte limit")
            auto count = self._read_size
            if count < buffered:
                count = buffered
            if count > room:
                count = room
            auto chunk = b""
            try:
                chunk = _receive_socket(self._socket, count, self._config.secure)
            except TimeoutError:
                raise CommandTimeout("Redis command deadline exceeded during receive")
            except Exception as e:
                raise ConnectionError("Redis receive failed: " + e)
            if len(chunk) == 0:
                if buffered == 0:
                    raise ConnectionError("Redis peer closed before replying")
                raise ConnectionError("Redis peer closed during a reply")
            self._reader3.append(chunk)

    def _write_wire(self, bytes wire, int64 deadline):
        auto sent = 0
        while sent < len(wire):
            _check_budget(deadline)
            # Zap net.send writes all bytes today; retain the loop and the
            # progress checks for transports that can return short writes.
            auto n = 0
            try:
                n = _send_socket(
                    self._socket,
                    wire[sent:len(wire)],
                    self._config.secure,
                )
            except TimeoutError:
                raise CommandTimeout("Redis command deadline exceeded during send")
            except Exception as e:
                raise ConnectionError("Redis send failed: " + e)
            if n <= 0 or n > len(wire) - sent:
                raise ConnectionError("Redis command write made invalid progress")
            sent = sent + n
    def _read_reply(self, int64 deadline) -> Reply:
        auto result = self._reader.feed(b"")
        while True:
            _check_budget(deadline)
            if result.status == "ok":
                return result.value
            if result.status == "invalid":
                raise ProtocolError("Invalid Redis reply: " + result.message)
            auto room = self._max_frame - self._reader.buffered_bytes()
            if room <= 0:
                raise ProtocolError("Redis reply exceeds byte limit")
            auto count = self._read_size
            if count > room:
                count = room
            auto chunk = b""
            try:
                chunk = _receive_socket(self._socket, count, self._config.secure)
            except TimeoutError:
                raise CommandTimeout("Redis command deadline exceeded during receive")
            except Exception as e:
                raise ConnectionError("Redis receive failed: " + e)
            if len(chunk) == 0:
                if self._reader.buffered_bytes() == 0:
                    raise ConnectionError("Redis peer closed before replying")
                raise ConnectionError("Redis peer closed during a reply")
            result = self._reader.feed(chunk)

# Snapshot and validate options before touching the network. Session setup is
# performed with exclusive ownership and closes the socket on ANY failure.
def _snapshot_options(Options config) -> Options:
    return options(
        config.host,
        config.port,
        config.database,
        config.username,
        config.password,
        config.client_name,
        config.authenticate,
        config.max_frame,
        config.max_depth,
        config.read_size,
        config.secure,
        config.connect_timeout_ms,
        config.command_timeout_ms,
        config.resolved_ip,
    )

def connect_options(Options config) -> Connection:
    return _connect_options_mode(config, False)

def connect_resp3_options(Options config) -> Connection:
    return _connect_options_mode(config, True)

def _connect_options_mode(Options config, bool protocol3) -> Connection:
    auto cfg = _snapshot_options(config)
    auto deadline = _budget(cfg.connect_timeout_ms)
    auto socket = _dial(cfg, deadline)
    Atomic[int64] state = Atomic(1)
    auto conn = Connection(
        socket,
        decoder(cfg.max_frame, cfg.max_depth),
        resp3_reader(cfg.max_frame, cfg.max_depth),
        state,
        cfg.max_frame,
        cfg.max_depth,
        cfg.read_size,
        cfg,
        b"",
        protocol3,
        [],
        0,
        64,
        [],
    )
    auto ready = False
    try:
        net.set_deadline_ns(socket, deadline)
        conn._initialize(deadline)
        net.set_deadline_ns(socket, 0)
        ready = True
        return conn
    finally:
        if ready:
            state.store(0)
        else:
            _close_socket(socket, cfg.secure)
            state.store(2)

# Preserve the existing positional host/port/parser-limit API, adding session
# options as trailing arguments. An explicit authenticate=True supports AUTH "".
def connect(
    str host = "127.0.0.1",
    int64 port = 6379,
    int64 max_frame = 16777216,
    int64 max_depth = 64,
    int64 read_size = 4096,
    int64 database = 0,
    str username = "",
    str password = "",
    str client_name = "",
    bool authenticate = False,
    bool secure = False,
    int64 connect_timeout_ms = 0,
    int64 command_timeout_ms = 0,
    str resolved_ip = "",
) -> Connection:
    return connect_options(
        options(
            host,
            port,
            database,
            username,
            password,
            client_name,
            authenticate,
            max_frame,
            max_depth,
            read_size,
            secure,
            connect_timeout_ms,
            command_timeout_ms,
            resolved_ip,
        ),
    )

def connect_url(str address) -> Connection:
    return connect_options(parse_url(address))

def connect_resp3(
    str host = "127.0.0.1",
    int64 port = 6379,
    int64 max_frame = 16777216,
    int64 max_depth = 64,
    int64 read_size = 4096,
    int64 database = 0,
    str username = "",
    str password = "",
    str client_name = "",
    bool authenticate = False,
    bool secure = False,
    int64 connect_timeout_ms = 0,
    int64 command_timeout_ms = 0,
    str resolved_ip = "",
) -> Connection:
    return connect_resp3_options(
        options(
            host,
            port,
            database,
            username,
            password,
            client_name,
            authenticate,
            max_frame,
            max_depth,
            read_size,
            secure,
            connect_timeout_ms,
            command_timeout_ms,
            resolved_ip,
        ),
    )

def connect_resp3_url(str address) -> Connection:
    return connect_resp3_options(parse_url(address))

def _cluster_slot_reply(Reply reply, RedisEndpoint seed) -> list[ClusterSlotRange]:
    match reply:
        case Reply.Array(parts):
            list[ClusterSlotRange] ranges = []
            auto next_slot = 0
            for item in parts:
                match item:
                    case Reply.Array(slot):
                        if len(slot) < 3:
                            raise ProtocolError("Invalid CLUSTER SLOTS entry")
                        auto first = _integer_value(slot[0])
                        auto last = _integer_value(slot[1])
                        if first != next_slot or last < first or last > 16383:
                            raise ProtocolError(
                                "CLUSTER SLOTS ranges are incomplete or overlapping",
                            )
                        match slot[2]:
                            case Reply.Array(node):
                                if len(node) < 2:
                                    raise ProtocolError("Invalid CLUSTER SLOTS primary")
                                auto host = bytes(seed.host)
                                match node[0]:
                                    case Reply.Bulk(address):
                                        if len(address) > 0:
                                            host = address
                                    case Reply.NullBulk:
                                        auto ignored = 0
                                    case _:
                                        raise ProtocolError(
                                            "Invalid CLUSTER SLOTS primary host",
                                        )
                                auto port = _integer_value(node[1])
                                if len(host) == 0 or port < 1 or port > 65535:
                                    raise ProtocolError(
                                        "Invalid CLUSTER SLOTS primary endpoint",
                                    )
                                ranges.append(
                                    ClusterSlotRange(
                                        first,
                                        last,
                                        RedisEndpoint(host, port),
                                    ),
                                )
                                next_slot = last + 1
                            case _:
                                raise ProtocolError("Expected CLUSTER SLOTS primary")
                    case _:
                        raise ProtocolError("Expected CLUSTER SLOTS entry")
            if next_slot != 16384:
                raise ProtocolError("CLUSTER SLOTS does not cover every hash slot")
            return ranges
        case _:
            raise ProtocolError("Expected CLUSTER SLOTS array")

def _cluster_discover(list[Options] seeds) -> list[ClusterSlotRange]:
    if len(seeds) == 0:
        raise ValueError("Redis Cluster requires at least one seed")
    for seed in seeds:
        try:
            auto conn = connect_options(seed)
            try:
                return _cluster_slot_reply(
                    conn.execute([b"CLUSTER", b"SLOTS"]),
                    RedisEndpoint(bytes(seed.host), seed.port),
                )
            finally:
                conn.disconnect()
        except Exception:
            auto ignored = 0
    raise ConnectionError("Redis Cluster could not discover slot topology")

def _cluster_redirect(bytes error, int64 expected_slot) -> ClusterRedirect:
    auto text = ""
    try:
        text = str(error)
    except Exception:
        raise ProtocolError("Malformed Redis Cluster redirection")
    if text !~ m/\A(?:MOVED|ASK)(?:\s|\z)/:
        raise ResponseError(_error_text(error))
    auto parsed = text =~ m/\A(?<kind>MOVED|ASK) (?<slot>[0-9]+) (?<host>\[[^\]]+\]|[^:\s]+):(?<port>[0-9]+)\z/
    if not parsed:
        raise ProtocolError("Malformed Redis Cluster redirection")
    auto slot_text = parsed.group("slot")
    if not _stream_component(slot_text):
        raise ProtocolError("Malformed Redis Cluster redirection slot")
    auto slot = _uint(slot_text, 16383)
    if slot != expected_slot:
        raise ProtocolError("Redis Cluster redirection slot does not match key")
    auto port_text = parsed.group("port")
    auto port = 0
    try:
        port = _uint(port_text, 65535)
    except ValueError:
        raise ProtocolError("Malformed Redis Cluster redirection port")
    if port == 0:
        raise ProtocolError("Malformed Redis Cluster redirection port")
    auto host_text = parsed.group("host")
    auto bracketed = host_text =~ m/\A\[(?<host>[^\]]+)\]\z/
    if bracketed:
        host_text = bracketed.group("host")
    auto host = bytes(host_text)
    if len(host) == 0:
        raise ProtocolError("Malformed Redis Cluster redirection host")
    return ClusterRedirect(bytes(parsed.group("kind")), slot, RedisEndpoint(host, port))

def _cluster_command_keys(bytes command, list[bytes] args) -> list[bytes]:
    list[bytes] keys = []
    if command == b"MGET" or command == b"DEL" or command == b"UNLINK" or command == b"EXISTS" or command == b"SUNION" or command == b"SINTER" or command == b"SDIFF":
        if len(args) < 2:
            raise ValueError(
                "Redis Cluster multi-key command requires at least one key",
            )
        auto i = 1
        while i < len(args):
            keys.append(bytes(args[i]))
            i = i + 1
        return keys
    if command == b"MSET":
        if len(args) < 3 or len(args) % 2 == 0:
            raise ValueError("Redis Cluster MSET requires key/value pairs")
        auto i = 1
        while i < len(args):
            keys.append(bytes(args[i]))
            i = i + 2
        return keys
    if command == b"RENAME" or command == b"RENAMENX" or command == b"RPOPLPUSH" or command == b"SMOVE":
        auto expected = 3
        if command == b"SMOVE":
            expected = 4
        if len(args) != expected:
            raise ValueError("Redis Cluster multi-key command has invalid arity")
        keys.append(bytes(args[1]))
        keys.append(bytes(args[2]))
        return keys
    if command == b"BITOP":
        if len(args) < 4:
            raise ValueError(
                "Redis Cluster BITOP requires a destination and source keys",
            )
        auto i = 2
        while i < len(args):
            keys.append(bytes(args[i]))
            i = i + 1
        return keys
    raise ValueError("Redis Cluster command is not a supported multi-key operation")

def cluster_connect(list[Options] seeds) -> ClusterClient:
    if len(seeds) == 0:
        raise ValueError("Redis Cluster requires at least one seed")
    list[Options] copied_seeds = []
    for seed in seeds:
        if seed.database != 0:
            raise ValueError("Redis Cluster supports database zero only")
        if seed.connect_timeout_ms <= 0 or seed.command_timeout_ms <= 0:
            raise ValueError(
                "Redis Cluster seeds require positive connect and command timeouts",
            )
        copied_seeds.append(_snapshot_options(seed))
    auto slots = _cluster_discover(copied_seeds)
    return ClusterClient(copied_seeds, copied_seeds[0], slots, [], [], [], Atomic(0))

struct SentinelClient:
    list[Options] _seeds
    bytes _service
    Options _data_config
    RedisEndpoint _primary
    Connection _connection
    Atomic[int64] _state

    def primary(self) -> RedisEndpoint:
        return self._primary

    def is_closed(self) -> bool:
        return self._state.load() == 2 or self._connection.is_closed()

    def execute(self, list[bytes] args) -> Reply:
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis Sentinel client is closed")
            raise BusyError("Redis Sentinel client is in use")
        try:
            return self._connection.execute(args)
        finally:
            if self._state.load() != 2:
                self._state.store(0)

    def reconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if not self._state.compare_and_swap(2, 1):
                raise BusyError("Redis Sentinel client is in use")
        auto ready = False
        try:
            auto endpoint = _sentinel_discover(self._seeds, self._service)
            if not self._connection.is_closed():
                self._connection.disconnect()
            self._connection = connect_options(
                _endpoint_options(self._data_config, endpoint),
            )
            self._primary = endpoint
            ready = True
        finally:
            if ready:
                self._state.store(0)
            else:
                if not self._connection.is_closed():
                    self._connection.disconnect()
                self._state.store(2)

    def disconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                return
            raise BusyError("Redis Sentinel client is in use")
        try:
            self._connection.disconnect()
        finally:
            self._state.store(2)

struct ClusterClient:
    list[Options] _seeds
    Options _data_config
    list[ClusterSlotRange] _slots
    list[ClusterSlotOverride] _moved_slots
    list[RedisEndpoint] _endpoints
    list[Connection] _connections
    Atomic[int64] _state

    def _primary(self, int64 slot) -> RedisEndpoint:
        for item in self._moved_slots:
            if item.slot == slot:
                return item.primary
        for item in self._slots:
            if slot >= item.first and slot <= item.last:
                return item.primary
        raise ProtocolError("Redis Cluster slot is missing from topology")

    def _connection_for(self, RedisEndpoint endpoint) -> Connection:
        auto i = 0
        while i < len(self._endpoints):
            if _redis_endpoint_equal(self._endpoints[i], endpoint):
                if self._connections[i].is_closed():
                    self._connections[i].reconnect()
                return self._connections[i]
            i = i + 1
        auto connection = connect_options(
            _endpoint_options(self._data_config, endpoint),
        )
        self._endpoints.append(endpoint)
        self._connections.append(connection)
        return self._connections[len(self._connections) - 1]

    def execute_for(self, bytes key, list[bytes] args) -> Reply:
        if len(args) < 2:
            raise ValueError("Redis Cluster command must include a single key")
        auto command = _command_name(args)
        if bytes(args[1]) != key or not _cluster_single_key_command(bytes(command)):
            raise ValueError(
                "Redis Cluster command is not a supported single-key operation",
            )
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis Cluster client is closed")
            raise BusyError("Redis Cluster client is in use")
        try:
            return self._execute_routed(cluster_slot(key), args)
        finally:
            if self._state.load() != 2:
                self._state.store(0)

    def execute_multi(self, list[bytes] args) -> Reply:
        if len(args) == 0:
            raise ValueError("Redis Cluster command must not be empty")
        auto command = bytes(_command_name(args))
        auto keys = _cluster_command_keys(command, args)
        auto slot = cluster_slot(keys[0])
        for key in keys:
            if cluster_slot(key) != slot:
                raise ValueError(
                    "CROSSSLOT keys in request do not hash to the same slot",
                )
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis Cluster client is closed")
            raise BusyError("Redis Cluster client is in use")
        try:
            return self._execute_routed(slot, args)
        finally:
            if self._state.load() != 2:
                self._state.store(0)

    # Redirect retries occur only for complete MOVED/ASK server errors. Any
    # transport failure escapes without replay because command execution may be
    # ambiguous. Caller owns _state for this whole loop.
    def _execute_routed(self, int64 slot, list[bytes] args) -> Reply:
        auto endpoint = self._primary(slot)
        auto asking = False
        auto redirects = 0
        while redirects <= 5:
            auto connection = self._connection_for(endpoint)
            if asking:
                _status_value(connection.execute([b"ASKING"]), b"OK")
            try:
                return connection.execute(args)
            except ResponseError:
                auto redirect = _cluster_redirect(connection.last_error(), slot)
                redirects = redirects + 1
                if redirects > 5:
                    raise ProtocolError("Redis Cluster redirection limit exceeded")
                if redirect.kind == b"MOVED":
                    self._refresh_topology()
                    self._moved_slots.append(
                        ClusterSlotOverride(slot, redirect.endpoint),
                    )
                    asking = False
                else:
                    asking = True
                endpoint = redirect.endpoint
        raise ProtocolError("Redis Cluster redirection limit exceeded")

    def _refresh_topology(self):
        auto slots = _cluster_discover(self._seeds)
        list[RedisEndpoint] retained_endpoints = []
        list[Connection] retained_connections = []
        for i in range(0, len(self._endpoints)):
            auto keep = False
            for item in slots:
                if _redis_endpoint_equal(self._endpoints[i], item.primary):
                    keep = True
            if keep:
                retained_endpoints.append(self._endpoints[i])
                retained_connections.append(self._connections[i])
            else:
                self._connections[i].disconnect()
        self._endpoints = retained_endpoints
        self._connections = retained_connections
        self._slots = slots
        list[ClusterSlotOverride] no_overrides = []
        self._moved_slots = no_overrides

    def refresh(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                raise ConnectionError("Redis Cluster client is closed")
            raise BusyError("Redis Cluster client is in use")
        try:
            self._refresh_topology()
        finally:
            self._state.store(0)

    def disconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                return
            raise BusyError("Redis Cluster client is in use")
        try:
            for connection in self._connections:
                connection.disconnect()
        finally:
            self._state.store(2)

def _cluster_single_key_command(bytes command) -> bool:
    return command == b"APPEND" or command == b"DECR" or command == b"DECRBY" or command == b"EXPIRE" or command == b"EXPIREAT" or command == b"GET" or command == b"GETDEL" or command == b"GETEX" or command == b"GETRANGE" or command == b"GETSET" or command == b"HDEL" or command == b"HEXISTS" or command == b"HGET" or command == b"HGETALL" or command == b"HINCRBY" or command == b"HINCRBYFLOAT" or command == b"HKEYS" or command == b"HLEN" or command == b"HMGET" or command == b"HSET" or command == b"HSETNX" or command == b"HSTRLEN" or command == b"HVALS" or command == b"INCR" or command == b"INCRBY" or command == b"INCRBYFLOAT" or command == b"LINDEX" or command == b"LINSERT" or command == b"LLEN" or command == b"LPOP" or command == b"LPOS" or command == b"LPUSH" or command == b"LPUSHX" or command == b"LRANGE" or command == b"LREM" or command == b"LSET" or command == b"LTRIM" or command == b"PERSIST" or command == b"PEXPIRE" or command == b"PEXPIREAT" or command == b"PTTL" or command == b"RESTORE" or command == b"RPOP" or command == b"RPUSH" or command == b"RPUSHX" or command == b"SADD" or command == b"SCARD" or command == b"SET" or command == b"SETEX" or command == b"SETNX" or command == b"SETRANGE" or command == b"SISMEMBER" or command == b"SMEMBERS" or command == b"SMISMEMBER" or command == b"SPOP" or command == b"SRANDMEMBER" or command == b"SREM" or command == b"STRLEN" or command == b"SUBSTR" or command == b"TTL" or command == b"TYPE" or command == b"XADD" or command == b"XDEL" or command == b"XLEN" or command == b"XPENDING" or command == b"XRANGE" or command == b"XREVRANGE" or command == b"ZADD" or command == b"ZCARD" or command == b"ZCOUNT" or command == b"ZINCRBY" or command == b"ZLEXCOUNT" or command == b"ZMSCORE" or command == b"ZPOPMAX" or command == b"ZPOPMIN" or command == b"ZRANGE" or command == b"ZRANGEBYLEX" or command == b"ZRANGEBYSCORE" or command == b"ZRANK" or command == b"ZREM" or command == b"ZREVRANGE" or command == b"ZREVRANGEBYLEX" or command == b"ZREVRANGEBYSCORE" or command == b"ZREVRANK" or command == b"ZSCAN" or command == b"ZSCORE"

struct PoolClosed:
    str message

struct PoolTimeout:
    str message

struct LeaseError:
    str message

def _pool_waiter(channel[bool] changed):
    auto notified = <-changed

# The only helper coroutine owns a notification wait, never a socket or a lease.
# The scope always joins it before timeout/cancellation can leave this function.
def _pool_wait(channel[bool] changed, int64 deadline):
    if deadline == 0:
        auto notified = <-changed
    else:
        auto remaining = deadline - time.monotonic_ns()
        if remaining <= 0:
            raise PoolTimeout("Redis pool acquisition timed out")
        auto milliseconds = remaining // 1000000
        if remaining % 1000000 > 0:
            milliseconds = milliseconds + 1
        try:
            scope(timeout=milliseconds):
                spawn _pool_waiter(changed)
        except ScopeTimeout:
            raise PoolTimeout("Redis pool acquisition timed out")
    checkpoint()
    if deadline > 0 and time.monotonic_ns() >= deadline:
        raise PoolTimeout("Redis pool acquisition timed out")

struct Pool:
    Options _config
    int64 _capacity
    int64 _acquire_timeout_ms
    int64 _reset_timeout_ms
    bytes _mutex
    list[Connection] _idle
    int64 _idle_count
    int64 _size
    bool _closed
    channel[bool] _changed

    # All state changes occur under the mutex. Closing a generation broadcasts
    # without a cancellable channel send, so finally cleanup cannot lose slots.
    def _notify(self):
        auto previous = self._changed
        self._changed = channel[bool]()
        close(previous)

    def size(self) -> int64:
        sync.lock(self._mutex)
        auto size = self._size
        sync.unlock(self._mutex)
        return size

    def is_shutdown(self) -> bool:
        sync.lock(self._mutex)
        auto closed = self._closed
        sync.unlock(self._mutex)
        return closed

    def _return(self, Connection connection, bool reusable) -> bool:
        sync.lock(self._mutex)
        auto keep = reusable and not self._closed
        if keep:
            if self._idle_count == len(self._idle):
                self._idle.append(connection)
            else:
                self._idle[self._idle_count] = connection
            self._idle_count = self._idle_count + 1
            self._notify()
        sync.unlock(self._mutex)
        if not keep:
            try:
                connection.disconnect()
            finally:
                self._failed_creation()
        return keep

    def _failed_creation(self):
        sync.lock(self._mutex)
        self._size = self._size - 1
        self._notify()
        sync.unlock(self._mutex)

    def acquire(self, bool dedicated = False) -> Lease:
        auto deadline = _budget(self._acquire_timeout_ms)
        while True:
            checkpoint()
            sync.lock(self._mutex)
            if self._closed:
                sync.unlock(self._mutex)
                raise PoolClosed("Redis pool has shut down")
            list[Connection] selected = []
            auto reserved = False
            if self._idle_count > 0:
                self._idle_count = self._idle_count - 1
                selected.append(self._idle[self._idle_count])
                reserved = True
            elif self._size < self._capacity:
                self._size = self._size + 1
                reserved = True
            auto changed = self._changed
            sync.unlock(self._mutex)
            if reserved:
                auto lent = False
                try:
                    if len(selected) == 0:
                        selected.append(connect_options(self._config))
                    checkpoint()
                    sync.lock(self._mutex)
                    auto stopped = self._closed
                    sync.unlock(self._mutex)
                    if stopped:
                        raise PoolClosed(
                            "Redis pool shut down during connection initialization",
                        )
                    auto lease = Lease(self, selected[0], Atomic(0), dedicated)
                    lent = True
                    return lease
                finally:
                    if not lent:
                        try:
                            if len(selected) > 0:
                                selected[0].disconnect()
                        finally:
                            self._failed_creation()
            _pool_wait(changed, deadline)

    # Stop admission and close idle sockets now. Existing leases retain exclusive
    # ownership; they are discarded on release. Pending creation is also rejected.
    def shutdown(self):
        sync.lock(self._mutex)
        if self._closed:
            sync.unlock(self._mutex)
            return
        self._closed = True
        list[Connection] idle = []
        for i in range(0, self._idle_count):
            idle.append(self._idle[i])
        self._size = self._size - self._idle_count
        self._idle_count = 0
        self._notify()
        sync.unlock(self._mutex)
        for connection in idle:
            connection.disconnect()

# No Connection accessor: a retained old lease must not operate on a socket
# after another borrower acquires it. The guard also spans transaction lifetime.
struct Lease:
    Pool _pool
    Connection _connection
    Atomic[int64] _guard
    bool _dedicated

    def _enter(self):
        if not self._guard.compare_and_swap(0, 1):
            if self._guard.load() == 2:
                raise LeaseError("Redis lease has already been released")
            raise BusyError("Redis lease is in use")

    def execute(self, list[bytes] args) -> Reply:
        if not self._dedicated and _requires_dedicated(args):
            raise ValueError(
                "Blocking/streaming/protocol commands require a dedicated lease",
            )
        self._enter()
        try:
            return self._connection.execute(args)
        finally:
            self._guard.store(0)

    def execute_raw(self, list[bytes] args) -> Reply:
        if not self._dedicated and _requires_dedicated(args):
            raise ValueError(
                "Blocking/streaming/protocol commands require a dedicated lease",
            )
        self._enter()
        try:
            return self._connection.execute_raw(args)
        finally:
            self._guard.store(0)

    def execute_pipeline(self, Pipeline batch) -> list[Reply]:
        if not self._dedicated and batch._dedicated_commands:
            raise ValueError(
                "Blocking/streaming/protocol commands require a dedicated lease",
            )
        self._enter()
        try:
            return batch.execute(self._connection)
        finally:
            self._guard.store(0)

    def pubsub(self) -> PubSub:
        if not self._dedicated:
            raise ValueError("Pub/Sub requires a dedicated lease")
        self._enter()
        auto started = False
        try:
            auto session = self._connection.pubsub()
            session._lease_guard = self._guard
            started = True
            return session
        finally:
            if not started:
                self._guard.store(0)

    def publish(self, bytes topic, bytes payload) -> int64:
        self._enter()
        try:
            return self._connection.publish(topic, payload)
        finally:
            self._guard.store(0)

    def transaction(self) -> Transaction:
        self._enter()
        auto started = False
        try:
            auto session = self._connection.transaction()
            session._lease_guard = self._guard
            started = True
            return session
        finally:
            if not started:
                self._guard.store(0)

    def blpop(
        self,
        list[bytes] keys,
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[KeyValue]:
        if not self._dedicated:
            raise ValueError("Blocking list helpers require a dedicated lease")
        self._enter()
        try:
            return self._connection.blpop(keys, server_timeout_seconds)
        finally:
            self._guard.store(0)

    def brpop(
        self,
        list[bytes] keys,
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[KeyValue]:
        if not self._dedicated:
            raise ValueError("Blocking list helpers require a dedicated lease")
        self._enter()
        try:
            return self._connection.brpop(keys, server_timeout_seconds)
        finally:
            self._guard.store(0)

    def blmove(
        self,
        bytes source,
        bytes destination,
        str source_side = "LEFT",
        str destination_side = "RIGHT",
        float64 server_timeout_seconds = 0.0,
    ) -> Optional[bytes]:
        if not self._dedicated:
            raise ValueError("Blocking list helpers require a dedicated lease")
        self._enter()
        try:
            return self._connection.blmove(
                source,
                destination,
                source_side,
                destination_side,
                server_timeout_seconds,
            )
        finally:
            self._guard.store(0)

    def xread(
        self,
        list[StreamCursor] streams,
        int64 count = 0,
        int64 block_ms = -1,
    ) -> list[StreamBatch]:
        if block_ms >= 0 and not self._dedicated:
            raise ValueError("Blocking XREAD requires a dedicated lease")
        self._enter()
        try:
            return self._connection.xread(streams, count, block_ms)
        finally:
            self._guard.store(0)

    def xgroup_create(
        self,
        bytes key,
        bytes group,
        bytes start_id = b"$",
        bool mkstream = False,
    ):
        self._enter()
        try:
            self._connection.xgroup_create(key, group, start_id, mkstream)
        finally:
            self._guard.store(0)

    def xgroup_destroy(self, bytes key, bytes group) -> bool:
        self._enter()
        try:
            return self._connection.xgroup_destroy(key, group)
        finally:
            self._guard.store(0)

    def xreadgroup(
        self,
        bytes group,
        bytes consumer,
        list[StreamCursor] streams,
        int64 count = 0,
        int64 block_ms = -1,
        bool noack = False,
    ) -> list[StreamBatch]:
        if block_ms >= 0 and not self._dedicated:
            raise ValueError("Blocking XREADGROUP requires a dedicated lease")
        self._enter()
        try:
            return self._connection.xreadgroup(
                group,
                consumer,
                streams,
                count,
                block_ms,
                noack,
            )
        finally:
            self._guard.store(0)

    def xack(self, bytes key, bytes group, list[bytes] ids) -> int64:
        self._enter()
        try:
            return self._connection.xack(key, group, ids)
        finally:
            self._guard.store(0)

    def xpending(self, bytes key, bytes group) -> StreamPendingSummary:
        self._enter()
        try:
            return self._connection.xpending(key, group)
        finally:
            self._guard.store(0)

    def xautoclaim(
        self,
        bytes key,
        bytes group,
        bytes consumer,
        int64 min_idle_ms,
        bytes start_id = b"0-0",
        int64 count = 0,
    ) -> StreamAutoClaim:
        self._enter()
        try:
            return self._connection.xautoclaim(
                key,
                group,
                consumer,
                min_idle_ms,
                start_id,
                count,
            )
        finally:
            self._guard.store(0)

    def _release(self, bool reuse) -> bool:
        self._enter()
        auto reusable = False
        auto kept = False
        try:
            if reuse and not self._dedicated and not self._connection.is_closed() and not self._pool.is_shutdown():
                try:
                    self._connection._pool_reset(self._pool._reset_timeout_ms)
                    reusable = True
                except Exception:
                    reusable = False
        finally:
            self._guard.store(2)
            kept = self._pool._return(self._connection, reusable)
        return kept

    def release(self) -> bool:
        return self._release(True)

    def discard(self):
        self._release(False)

# No eager sockets; every new connection receives the same validated snapshot.
# Acquisition timeout bounds capacity waiting; dialing/session setup use Options'
# connect budget (and retain its documented synchronous-DNS limitation).
def pool(
    Options config,
    int64 capacity = 8,
    int64 acquire_timeout_ms = 1000,
    int64 reset_timeout_ms = 1000,
) -> Pool:
    if capacity < 1 or acquire_timeout_ms < 0 or reset_timeout_ms < 1:
        raise ValueError("invalid Redis pool limits")
    list[Connection] idle = []
    return Pool(
        _snapshot_options(config),
        capacity,
        acquire_timeout_ms,
        reset_timeout_ms,
        sync.mutex(),
        idle,
        0,
        0,
        False,
        channel[bool](),
    )
