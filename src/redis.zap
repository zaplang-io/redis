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
