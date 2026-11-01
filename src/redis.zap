# RESP2 wire codec. All payloads stay bytes; text decoding is a caller choice.
# No sockets or implicit command execution in this module.
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

# Encode command + arguments as an array of bulk strings (not inline commands).
def encode_command(list[bytes] args) -> bytes:
    if len(args) == 0:
        raise ValueError("a Redis command must have at least one argument")
    auto out = bytes("*" + str(len(args)) + "\r\n")
    auto i = 0
    while i < len(args):
        bytes arg = args[i]
        out = out + bytes("$" + str(len(arg)) + "\r\n") + arg + b"\r\n"
        i = i + 1
    return out

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
    if max_frame < 3 or max_depth < 0:
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

# Zap user exceptions carry a string message and have Exception as their parent.
struct ResponseError:
    str message

struct ProtocolError:
    str message

struct ConnectionError:
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

def _dial(str host, int64 port) -> bytes:
    try:
        return net.connect(host, port)
    except Exception as e:
        raise ConnectionError("Redis connect failed: " + e)

# One connection has one active owner. The atomic state protects both socket
# access and the reply buffer: 0=idle, 1=owned, 2=closed. Sharing requires callers
# to serialize access; a concurrent execute/disconnect is rejected, never queued.
struct Connection:
    bytes _socket
    bytes _buffer
    Atomic[int64] _state
    int64 _max_frame
    int64 _max_depth
    int64 _read_size
    str _host
    int64 _port
    bytes _last_error

    def is_closed(self) -> bool:
        return self._state.load() == 2

    def disconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                return
            raise BusyError("Redis connection is in use")
        net.close_conn(self._socket)
        self._buffer = b""
        self._state.store(2)

    # The last consumed top-level server error is retained losslessly until a
    # subsequent server error or an explicit reconnect. Success does not clear it.
    def last_error(self) -> bytes:
        return self._last_error

    def reconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if not self._state.compare_and_swap(2, 1):
                raise BusyError("Redis connection is in use")
        net.close_conn(self._socket)
        self._buffer = b""
        self._last_error = b""
        auto opened = False
        try:
            self._socket = _dial(self._host, self._port)
            opened = True
        finally:
            if opened:
                self._state.store(0)
            else:
                self._state.store(2)

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
            auto sent = 0
            while sent < len(wire):
                # Zap net.send writes all bytes today; retain the loop and the
                # progress checks for transports that can return short writes.
                auto n = 0
                try:
                    n = net.send(self._socket, wire[sent:len(wire)])
                except Exception as e:
                    raise ConnectionError("Redis send failed: " + e)
                if n <= 0 or n > len(wire) - sent:
                    raise ConnectionError("Redis command write made invalid progress")
                sent = sent + n
            while True:
                auto result = decode(self._buffer, self._max_frame, self._max_depth)
                if result.status == "ok":
                    self._buffer = self._buffer[result.consumed:len(self._buffer)]
                    healthy = True
                    match result.value:
                        case Reply.Error(data):
                            self._last_error = data
                            if raise_response_errors:
                                raise ResponseError(_error_text(data))
                        case _:
                            return result.value
                    return result.value
                if result.status == "invalid":
                    raise ProtocolError("Invalid Redis reply: " + result.message)
                auto room = self._max_frame - len(self._buffer)
                if room <= 0:
                    raise ProtocolError("Redis reply exceeds byte limit")
                auto count = self._read_size
                if count > room:
                    count = room
                # net.recv returns an opaque length-prefixed str buffer. bytes()
                # relabels it without UTF-8 decoding; do not call str(bytes).
                auto chunk = b""
                try:
                    chunk = bytes(net.recv(self._socket, count))
                except Exception as e:
                    raise ConnectionError("Redis receive failed: " + e)
                if len(chunk) == 0:
                    if len(self._buffer) == 0:
                        raise ConnectionError("Redis peer closed before replying")
                    raise ConnectionError("Redis peer closed during a reply")
                self._buffer = self._buffer + chunk
        finally:
            if healthy:
                self._state.store(0)
            else:
                net.close_conn(self._socket)
                self._buffer = b""
                self._state.store(2)

# Connect only after validating options. Limits bound each reply and the receive
# buffer; read_size bounds individual reads, not the size of a Redis value.
def connect(
    str host = "127.0.0.1",
    int64 port = 6379,
    int64 max_frame = 16777216,
    int64 max_depth = 64,
    int64 read_size = 4096,
) -> Connection:
    if len(host) == 0 or port < 1 or port > 65535:
        raise ValueError("invalid Redis host or port")
    if max_frame < 3 or max_depth < 0 or read_size < 1 or read_size > 65536:
        raise ValueError("invalid Redis connection limits")
    auto socket = _dial(host, port)
    Atomic[int64] state = Atomic(0)
    return Connection(
        socket,
        b"",
        state,
        max_frame,
        max_depth,
        read_size,
        host,
        port,
        b"",
    )
