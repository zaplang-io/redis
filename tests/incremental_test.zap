import redis

def check(bool ok, str label):
    if not ok:
        raise ValueError("FAIL: " + label)

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


def fragments(bytes frame, int64 chunk_size):
    auto expected = redis.decode(frame)
    check(expected.status == "ok", "stateless oracle")
    auto parser = redis.decoder(max_frame=len(frame))
    auto pos = 0
    auto result = parser.feed(b"")
    while pos < len(frame):
        auto end = pos + chunk_size
        if end > len(frame):
            end = len(frame)
        result = parser.feed(frame[pos:end])
        pos = end
        if pos < len(frame):
            check(result.status == "incomplete", "proper fragment incomplete")
            check(parser.frames_decoded == 0, "no partial trees allocated")
        else:
            check(result.status == "ok", "complete incremental frame")
    check(wire(result.value) == wire(expected.value), "incremental oracle equality")
    check(result.consumed == len(frame), "incremental exact consumption")
    check(parser.processed_bytes == len(frame), "each input byte framed once")
    check(parser.frames_decoded == 1 and parser.buffered_bytes() == 0, "one final tree")
    auto levels = 0
    auto size = len(frame)
    while size > 1:
        size = (size + 1) // 2
        levels = levels + 1
    check(
        parser.copied_bytes <= len(frame) * (3 * levels + 3),
        "bounded chunk copy volume",
    )

def main():
    auto seed = 322
    for i in range(0, 128):
        seed = next_seed(seed)
        auto frame = wire(tree(seed, 5))
        fragments(frame, 1)
        fragments(frame, 7)
        fragments(frame, 128)
    for frame in [b":-9223372036854775808\r\n", b":+9223372036854775807\r\n", b"$000001\r\nx\r\n", b"*0001\r\n$-1\r\n"]:
        fragments(frame, 1)
    auto parser = redis.decoder(max_frame=3)
    auto coalesced = b""
    for i in range(0, 500):
        coalesced = coalesced + b"+\r\n"
    auto result = parser.feed(coalesced)
    auto count = 0
    while result.status == "ok":
        count = count + 1
        check(result.consumed == 3, "coalesced per-frame byte budget")
        result = parser.feed(b"")
    check(count == 500 and result.status == "incomplete", "all coalesced replies")
    check(parser.processed_bytes == len(coalesced), "coalesced input framed once")
    check(parser.copied_bytes <= 3 * len(coalesced), "coalesced tail is not recopied")
    auto pending = redis.decoder()
    pending.feed(b"+one\r\n+two\r\n")
    auto rejected = False
    try:
        pending.feed(b"+three\r\n")
    except ValueError:
        rejected = True
    check(rejected and pending.has_pending(), "new chunk cannot overwrite unread frame")
    check(pending.feed(b"").status == "ok", "pending frame survives misuse")
    for malformed in [b"?", b":9223372036854775808\r\n", b"$-01\r\n", b"$+1\r\nx\r\n", b"*-2\r\n", b"$1\r\nx??", b":--1\r\n", b"*999999999999\r\n"]:
        auto broken = redis.decoder()
        auto error = broken.feed(malformed)
        check(
            error.status == "invalid" and len(error.message) > 0,
            "malformed incremental diagnostic",
        )
        check(broken.feed(b"+OK\r\n").status == "invalid", "decoder remains poisoned")
    auto too_large = redis.decoder(max_frame=8)
    check(
        too_large.feed(b"$100\r\n").status == "invalid",
        "hostile blob rejected before payload",
    )
    auto too_deep = redis.decoder(max_depth=1)
    check(
        too_deep.feed(b"*1\r\n*0\r\n").status == "invalid",
        "depth limit across headers",
    )
    auto across = redis.decoder(max_depth=1)
    check(across.feed(b"*1\r\n").status == "incomplete", "partial aggregate")
    check(across.feed(b"*0\r\n").status == "invalid", "depth limit across fragments")
    auto bulk = bytes([0, 255, 13, 10])
    for i in range(0, 14):
        bulk = bulk + bulk
    fragments(bytes("$" + str(len(bulk)) + "\r\n") + bulk + b"\r\n", 128)
    auto wide = b"*2048\r\n"
    for i in range(0, 2048):
        wide = wide + b":1\r\n"
    fragments(wide, 1)
    fragments(wide, 128)
    print("Resumable RESP2 framing seed 322: PASS")
