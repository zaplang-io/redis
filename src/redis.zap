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

# TLS uses the verified stdlib entry point only. Failure never retries as TCP.
def _dial(str host, int64 port, bool secure) -> bytes:
    try:
        if secure:
            return tls.connect(host, port)
        return net.connect(host, port)
    except Exception as e:
        if secure:
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
) -> Options:
    if len(host) == 0 or port < 1 or port > 65535:
        raise ValueError("invalid Redis host or port")
    if host =~ m{[\x00-\x20\/@?#\[\]\x7f]}:
        raise ValueError("invalid Redis host")
    if database < 0 or database > 2147483647:
        raise ValueError("invalid Redis database")
    if max_frame < 3 or max_depth < 0 or read_size < 1 or read_size > 65536:
        raise ValueError("invalid Redis connection limits")
    if client_name !~ m/\A[\x21-\x7e]*\z/:
        raise ValueError("invalid Redis client name")
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
    )
    cfg.secure = parsed.group("scheme") == "rediss"
    return cfg

# MSET preserves caller ordering and permits binary keys and values.
struct KeyValue:
    bytes key
    bytes value

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

def _keys_command(bytes name, list[bytes] keys) -> list[bytes]:
    if len(keys) == 0:
        raise ValueError("Redis key list must not be empty")
    list[bytes] args = [name]
    for key in keys:
        args.append(key)
    return args

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
    Options _config
    bytes _last_error

    def is_closed(self) -> bool:
        return self._state.load() == 2

    def disconnect(self):
        if not self._state.compare_and_swap(0, 1):
            if self._state.load() == 2:
                return
            raise BusyError("Redis connection is in use")
        _close_socket(self._socket, self._config.secure)
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
        _close_socket(self._socket, self._config.secure)
        self._buffer = b""
        self._last_error = b""
        auto opened = False
        try:
            self._socket = _dial(
                self._config.host,
                self._config.port,
                self._config.secure,
            )
            self._initialize()
            opened = True
        finally:
            if opened:
                self._state.store(0)
            else:
                _close_socket(self._socket, self._config.secure)
                self._buffer = b""
                self._last_error = b""
                self._state.store(2)

    def _session_ok(self, list[bytes] args, str stage):
        auto reply = self._wire_reply(encode_command(args))
        match reply:
            case Reply.Simple(data):
                if data != b"OK":
                    raise ProtocolError("Unexpected Redis session reply: " + stage)
            case Reply.Error(data):
                raise ResponseError("Redis session initialization failed: " + stage)
            case _:
                raise ProtocolError("Unexpected Redis session reply: " + stage)

    def _initialize(self):
        if self._config.authenticate:
            if len(self._config.username) > 0:
                self._session_ok(
                    [
                        b"AUTH",
                        bytes(self._config.username),
                        bytes(self._config.password),
                    ],
                    "AUTH",
                )
            else:
                self._session_ok([b"AUTH", bytes(self._config.password)], "AUTH")
        if self._config.database != 0:
            self._session_ok([b"SELECT", bytes(str(self._config.database))], "SELECT")
        if len(self._config.client_name) > 0:
            self._session_ok(
                [b"CLIENT", b"SETNAME", bytes(self._config.client_name)],
                "CLIENT SETNAME",
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
            auto result = self._wire_reply(wire)
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
                _close_socket(self._socket, self._config.secure)
                self._buffer = b""
                self._state.store(2)

    # Caller holds the ownership state for the entire command/session setup.
    def _wire_reply(self, bytes wire) -> Reply:
        auto sent = 0
        while sent < len(wire):
            # Zap net.send writes all bytes today; retain the loop and the
            # progress checks for transports that can return short writes.
            auto n = 0
            try:
                n = _send_socket(
                    self._socket,
                    wire[sent:len(wire)],
                    self._config.secure,
                )
            except Exception as e:
                raise ConnectionError("Redis send failed: " + e)
            if n <= 0 or n > len(wire) - sent:
                raise ConnectionError("Redis command write made invalid progress")
            sent = sent + n
        while True:
            auto result = decode(self._buffer, self._max_frame, self._max_depth)
            if result.status == "ok":
                self._buffer = self._buffer[result.consumed:len(self._buffer)]
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
                chunk = _receive_socket(self._socket, count, self._config.secure)
            except Exception as e:
                raise ConnectionError("Redis receive failed: " + e)
            if len(chunk) == 0:
                if len(self._buffer) == 0:
                    raise ConnectionError("Redis peer closed before replying")
                raise ConnectionError("Redis peer closed during a reply")
            self._buffer = self._buffer + chunk

# Snapshot and validate options before touching the network. Session setup is
# performed with exclusive ownership and closes the socket on ANY failure.
def connect_options(Options config) -> Connection:
    auto cfg = options(
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
    )
    auto socket = _dial(cfg.host, cfg.port, cfg.secure)
    Atomic[int64] state = Atomic(1)
    auto conn = Connection(
        socket,
        b"",
        state,
        cfg.max_frame,
        cfg.max_depth,
        cfg.read_size,
        cfg,
        b"",
    )
    auto ready = False
    try:
        conn._initialize()
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
        ),
    )

def connect_url(str address) -> Connection:
    return connect_options(parse_url(address))
