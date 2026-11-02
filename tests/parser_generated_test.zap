import redis

def check(bool ok, str label):
    if not ok:
        raise ValueError("FAIL: " + label)

# Reproducible bounded generator: seed 322, recurrence modulo 65521.
def next_seed(int64 seed) -> int64:
    return (seed * 251 + 13849) % 65521

def tree(int64 seed, int64 depth) -> redis.Reply:
    auto kind = seed % 7
    if depth == 0:
        kind = seed % 6
    if kind == 0:
        return redis.Reply.Simple(bytes("s" + str(seed)))
    if kind == 1:
        return redis.Reply.Error(bytes("ERR e" + str(seed)))
    if kind == 2:
        return redis.Reply.Integer(seed - 32760)
    if kind == 3:
        return redis.Reply.Bulk(bytes([0, 255, 13, 10, seed % 256]))
    if kind == 4:
        return redis.Reply.NullBulk
    if kind == 5:
        return redis.Reply.NullArray
    list[redis.Reply] items = []
    auto i = 0
    while i < seed % 4:
        seed = next_seed(seed)
        items.append(tree(seed, depth - 1))
        i = i + 1
    return redis.Reply.Array(items)

# An independent test encoder is the structural oracle for generated replies.
def wire(redis.Reply reply) -> bytes:
    match reply:
        case redis.Reply.Simple(data):
            return b"+" + data + b"\r\n"
        case redis.Reply.Error(data):
            return b"-" + data + b"\r\n"
        case redis.Reply.Integer(value):
            return bytes(":" + str(value) + "\r\n")
        case redis.Reply.Bulk(data):
            return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"
        case redis.Reply.NullBulk:
            return b"$-1\r\n"
        case redis.Reply.NullArray:
            return b"*-1\r\n"
        case redis.Reply.Array(items):
            auto out = bytes("*" + str(len(items)) + "\r\n")
            for item in items:
                out = out + wire(item)
            return out

def invalid(bytes data):
    auto result = redis.decode(data)
    check(result.status == "invalid", "malformed frame rejected")
    check(result.consumed == 0 and len(result.message) > 0, "malformed diagnostic")

def main():
    auto seed = 322
    for i in range(0, 256):
        seed = next_seed(seed)
        auto expected = wire(tree(seed, 5))
        auto decoded = redis.decode(expected)
        check(
            decoded.status == "ok" and decoded.consumed == len(expected),
            "generated valid frame",
        )
        check(wire(decoded.value) == expected, "generated structural round trip")
        auto coalesced = redis.decode(expected + b"+NEXT\r\n", max_frame=len(expected))
        check(
            coalesced.status == "ok" and coalesced.consumed == len(expected),
            "per-frame byte limit",
        )
        auto j = 0
        while j < len(expected):
            auto prefix = redis.decode(expected[0:j])
            check(
                prefix.status == "incomplete" and prefix.consumed == 0,
                "every generated fragment",
            )
            j = j + 1
        invalid(b"?" + expected[1:len(expected)])
    for bad in [b"$9223372036854775808\r\n", b"*9223372036854775807\r\n", b":-9223372036854775809\r\n", b"$+1\r\nx\r\n", b"*-2\r\n", b"$1\nx\r\n", b"$1\r\nx??", b":+\r\n"]:
        invalid(bad)
    # Arbitrary-byte corpus must never crash or report out-of-range consumption.
    for i in range(0, 512):
        seed = next_seed(seed)
        auto data = b""
        auto count = seed % 80
        for j in range(0, count):
            seed = next_seed(seed)
            data = data + bytes([seed % 256])
        auto result = redis.decode(data, max_frame=128, max_depth=8)
        check(
            result.status == "ok" or result.status == "incomplete" or result.status == "invalid",
            "fuzz status",
        )
        check(result.consumed >= 0 and result.consumed <= len(data), "fuzz bounds")
        if result.status == "invalid":
            check(len(result.message) > 0, "fuzz diagnostic")
        if result.status != "ok":
            check(result.consumed == 0, "fuzz failure consumes nothing")
    auto deep = b"+x\r\n"
    for i in range(0, 256):
        deep = b"*1\r\n" + deep
    check(redis.decode(deep, max_depth=256).status == "ok", "safe maximum depth")
    check(redis.decode(deep, max_depth=255).status == "invalid", "nested depth limit")
    check(
        redis.decode(deep, max_depth=257).status == "invalid",
        "unsafe configured depth rejected",
    )
    auto wide = b"*2048\r\n"
    for i in range(0, 2048):
        wide = wide + b":1\r\n"
    check(redis.decode(wide).status == "ok", "wide array")
    check(
        redis.decode(wide, max_frame=len(wide) - 1).status == "invalid",
        "wide byte limit",
    )
    auto payload = bytes([0, 255, 13, 10])
    for i in range(0, 14):
        payload = payload + payload
    auto bulk = bytes("$" + str(len(payload)) + "\r\n") + payload + b"\r\n"
    auto end = 0
    while end < len(bulk):
        check(
            redis.decode(bulk[0:end]).status == "incomplete",
            "fragmented 64 KiB bulk",
        )
        end = end + 257
    check(redis.decode(bulk).status == "ok", "complete 64 KiB bulk")
    print("Generated RESP2 corpus seed 322: PASS")
