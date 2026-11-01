import redis

struct Step:
    list[bytes] args
    bytes reply

def check(bool ok, str label):
    if not ok:
        raise ValueError("FAIL: " + label)

def server(bytes listener, list[Step] steps):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        for step in steps:
            auto want = redis.encode_command(step.args)
            auto got = b""
            while len(got) < len(want):
                auto chunk = bytes(net.recv(socket, len(want) - len(got)))
                check(len(chunk) > 0, "command complete")
                got = got + chunk
            check(got == want, "exact helper wire command")
            net.send(socket, step.reply)
    finally:
        net.close_conn(socket)

def contains(list[bytes] members, bytes want) -> bool:
    auto i = 0
    while i < len(members):
        bytes member = members[i]
        if member == want:
            return True
        i = i + 1
    return False

def membership(list[bytes] actual, list[bytes] expected):
    check(len(actual) == len(expected), "set cardinality")
    for member in expected:
        check(contains(actual, member), "set member bytes")

def invalid_options(redis.Connection conn):
    list[bytes] empty = []
    auto errors = 0
    try:
        conn.sadd(b"k", empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.srem(b"k", empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.sunion(empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.sinter(empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.sdiff(empty)
    except ValueError:
        errors = errors + 1
    check(errors == 5, "empty set arguments rejected before I/O")

def main():
    auto value = bytes([128, 255, 0, 13, 10])
    list[Step] steps = [
        Step([b"SADD", b"k", value, b"", value], b":2\r\n"),
        Step([b"SREM", b"k", value, value], b":1\r\n"),
        Step([b"SISMEMBER", b"k", b""], b":1\r\n"),
        Step([b"SISMEMBER", b"k", value], b":0\r\n"),
        Step([b"SCARD", b"k"], b":2\r\n"),
        Step([b"SMEMBERS", b"k"], b"*2\r\n$0\r\n\r\n$5\r\n" + value + b"\r\n"),
        Step([b"SMEMBERS", b"missing"], b"*0\r\n"),
        Step([b"SPOP", b"k"], b"$5\r\n" + value + b"\r\n"),
        Step([b"SPOP", b"k"], b"$0\r\n\r\n"),
        Step([b"SPOP", b"k"], b"$-1\r\n"),
        Step(
            [b"SUNION", b"a", b"b", b"a"],
            b"*2\r\n$5\r\n" + value + b"\r\n$0\r\n\r\n",
        ),
        Step([b"SINTER", b"a", b"b"], b"*1\r\n$5\r\n" + value + b"\r\n"),
        Step([b"SDIFF", b"b", b"a"], b"*0\r\n"),
    ]
    steps.append(Step([b"SADD", b"k", b"v"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SREM", b"k", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SISMEMBER", b"k", b"v"], b":2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCARD", b"k"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SPOP", b"k"], b"*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SMEMBERS", b"k"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SMEMBERS", b"k"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SMEMBERS", b"k"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SMEMBERS", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SUNION", b"k"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SUNION", b"k"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SUNION", b"k"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SUNION", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SINTER", b"k"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SINTER", b"k"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SINTER", b"k"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SINTER", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SDIFF", b"k"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SDIFF", b"k"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SDIFF", b"k"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SDIFF", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(
            conn.sadd(b"k", [value, b"", value]) == 2,
            "SADD exact binary and duplicates",
        )
        check(conn.srem(b"k", [value, value]) == 1, "SREM exact duplicates")
        check(conn.sismember(b"k", b""), "SISMEMBER true")
        check(not conn.sismember(b"k", value), "SISMEMBER false")
        check(conn.scard(b"k") == 2, "SCARD")
        membership(conn.smembers(b"k"), [value, b""])
        check(len(conn.smembers(b"missing")) == 0, "empty SMEMBERS")
        check(conn.spop(b"k").unwrap() == value, "SPOP binary")
        auto empty = conn.spop(b"k")
        check(empty.is_some() and empty.unwrap() == b"", "empty SPOP")
        check(conn.spop(b"k").is_none(), "nil SPOP")
        membership(conn.sunion([b"a", b"b", b"a"]), [b"", value])
        membership(conn.sinter([b"a", b"b"]), [value])
        check(len(conn.sdiff([b"b", b"a"])) == 0, "empty SDIFF")
        auto errors = 0
        try:
            conn.sadd(b"k", [b"v"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.srem(b"k", [b"v"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sismember(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scard(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.spop(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.smembers(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.smembers(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.smembers(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.smembers(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sunion([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sunion([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sunion([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sunion([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sinter([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sinter([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sinter([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sinter([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sdiff([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sdiff([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sdiff([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sdiff([b"k"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        check(errors == 21, "all malformed set replies rejected")
    finally:
        conn.disconnect()
    print("Set command wire and validation: PASS")
