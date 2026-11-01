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

def invalid_options(redis.Connection conn):
    list[redis.ScoredMember] no_pairs = []
    list[bytes] no_members = []
    auto errors = 0
    try:
        conn.zadd(b"k", no_pairs)
    except ValueError:
        errors = errors + 1
    try:
        conn.zrem(b"k", no_members)
    except ValueError:
        errors = errors + 1
    try:
        conn.zadd(b"k", [redis.ScoredMember(b"v", math.nan)])
    except ValueError:
        errors = errors + 1
    try:
        conn.zadd(
            b"k",
            [redis.ScoredMember(b"first", 1.0), redis.ScoredMember(b"bad", math.nan)],
        )
    except ValueError:
        errors = errors + 1
    try:
        conn.zincrby(b"k", b"v", math.nan)
    except ValueError:
        errors = errors + 1
    check(errors == 5, "invalid sorted-set arguments rejected before I/O")

def main():
    auto value = bytes([255, 0, 128, 13, 10])
    list[Step] steps = [
        Step(
            [
                b"ZADD",
                b"k",
                b"1.2345678901234567",
                value,
                b"-inf",
                b"low",
                b"inf",
                b"high",
            ],
            b":3\r\n",
        ),
        Step([b"ZREM", b"k", value, value], b":1\r\n"),
        Step([b"ZCARD", b"k"], b":2\r\n"),
        Step([b"ZSCORE", b"k", value], b"$-1\r\n"),
        Step([b"ZSCORE", b"k", value], b"$1\r\n0\r\n"),
        Step([b"ZSCORE", b"k", value], b"$7\r\n1.25e+2\r\n"),
        Step([b"ZINCRBY", b"k", b"-1.5", value], b"$4\r\n-2.5\r\n"),
        Step([b"ZINCRBY", b"k", b"inf", value], b"$3\r\ninf\r\n"),
        Step([b"ZRANK", b"k", value], b":0\r\n"),
        Step([b"ZREVRANK", b"k", value], b":2\r\n"),
        Step([b"ZRANK", b"k", b"missing"], b"$-1\r\n"),
        Step([b"ZREVRANK", b"k", b"missing"], b"$-1\r\n"),
        Step(
            [b"ZRANGE", b"k", b"-2", b"-1"],
            b"*2\r\n$0\r\n\r\n$5\r\n" + value + b"\r\n",
        ),
        Step([b"ZRANGE", b"missing", b"0", b"-1"], b"*0\r\n"),
        Step(
            [b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"],
            b"*4\r\n$5\r\n" + value + b"\r\n$4\r\n-1.5\r\n$0\r\n\r\n$3\r\ninf\r\n",
        ),
        Step([b"ZRANGE", b"missing", b"0", b"-1", b"WITHSCORES"], b"*0\r\n"),
    ]
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$0\r\n\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$3\r\nNaN\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$3\r\nnan\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$2\r\n1x\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$2\r\n 1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$2\r\n1\n\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$5\r\n0x1p2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$2\r\n1e\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$6\r\n1e9999\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$7\r\n1e-9999\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$1\r\n\x00\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$1\r\n\xff\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b":1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"+1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZINCRBY", b"k", b"1", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZRANK", b"k", b"v"], b":-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZREVRANK", b"k", b"v"], b"$1\r\n1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZADD", b"k", b"1", b"v"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZREM", b"k", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZCARD", b"k"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZRANGE", b"k", b"0", b"-1"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZRANGE", b"k", b"0", b"-1"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZRANGE", b"k", b"0", b"-1"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step([b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"], b"*1\r\n$1\r\nv\r\n"),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"],
            b"*2\r\n$-1\r\n$1\r\n1\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step([b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"], b"*2\r\n$1\r\nv\r\n:1\r\n"),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"ZRANGE", b"k", b"0", b"-1", b"WITHSCORES"],
            b"*2\r\n$1\r\nv\r\n$3\r\nnan\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(
            conn.zadd(b"k", [redis.ScoredMember(value, 1.2345678901234567), redis.ScoredMember(b"low", -math.inf), redis.ScoredMember(b"high", math.inf)]) == 3,
            "exact score formatting",
        )
        check(conn.zrem(b"k", [value, value]) == 1, "ZREM duplicates")
        check(conn.zcard(b"k") == 2, "ZCARD")
        check(conn.zscore(b"k", value).is_none(), "nil score")
        check(conn.zscore(b"k", value).unwrap() == 0.0, "zero score")
        check(conn.zscore(b"k", value).unwrap() == 125.0, "exponent score")
        check(conn.zincrby(b"k", value, -1.5) == -2.5, "signed increment")
        check(conn.zincrby(b"k", value, math.inf) == math.inf, "infinite increment")
        check(conn.zrank(b"k", value).unwrap() == 0, "rank zero")
        check(conn.zrevrank(b"k", value).unwrap() == 2, "reverse rank")
        check(conn.zrank(b"k", b"missing").is_none(), "nil rank")
        check(conn.zrevrank(b"k", b"missing").is_none(), "nil reverse rank")
        list[bytes] values = conn.zrange(b"k", -2, -1)
        check(len(values) == 2, "member range count")
        bytes first = values[0]
        bytes second = values[1]
        check(first == b"", "empty member")
        check(second == value, "binary member")
        check(len(conn.zrange(b"missing", 0, -1)) == 0, "empty range")
        auto scored = conn.zrange_withscores(b"k", 0, -1)
        check(len(scored) == 2, "scored count")
        check(
            scored[0].member == value and scored[0].score == -1.5,
            "binary scored member",
        )
        check(
            scored[1].member == b"" and scored[1].score == math.inf,
            "empty infinite member",
        )
        check(len(conn.zrange_withscores(b"missing", 0, -1)) == 0, "empty scored range")
        auto errors = 0
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscore(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zincrby(b"k", b"v", 1.0)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrank(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrevrank(b"k", b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zadd(b"k", [redis.ScoredMember(b"v", 1.0)])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrem(b"k", [b"v"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zcard(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange_withscores(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange_withscores(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange_withscores(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange_withscores(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zrange_withscores(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        check(errors == 28, "all malformed sorted-set replies rejected")
    finally:
        conn.disconnect()
    print("Sorted-set command wire and validation: PASS")
