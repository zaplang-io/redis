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
        auto number = 1
        for step in steps:
            auto want = redis.encode_command(step.args)
            auto got = b""
            while len(got) < len(want):
                auto chunk = bytes(net.recv(socket, len(want) - len(got)))
                check(len(chunk) > 0, "command complete")
                got = got + chunk
            check(
                got == want,
                "exact sorted-set wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def invalid_options(redis.Connection conn):
    dict[bytes, float64] no_pairs = {}
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
        conn.zadd(b"k", {b"v": math.nan})
    except ValueError:
        errors = errors + 1
    try:
        conn.zadd(b"k", {b"first": 1.0, b"bad": math.nan})
    except ValueError:
        errors = errors + 1
    try:
        conn.zincrby(b"k", b"v", math.nan)
    except ValueError:
        errors = errors + 1
    check(errors == 5, "invalid sorted-set arguments rejected before I/O")

def breadth_options(redis.Connection conn):
    list[bytes] empty_keys = []
    auto caught = 0
    try:
        conn.zadd(b"k", {b"m": 1.0}, nx=True, xx=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zadd(b"k", {b"m": 1.0}, nx=True, gt=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zadd(b"k", {b"m": 1.0}, nx=True, lt=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zadd(b"k", {b"m": 1.0}, gt=True, lt=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zadd(b"k", {b"m": 1.0, b"n": 2.0}, incr=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zrangebyscore(b"k", b"(nan", b"+inf")
    except ValueError:
        caught = caught + 1
    try:
        conn.zcount(b"k", b"", b"1")
    except ValueError:
        caught = caught + 1
    try:
        conn.zlexcount(b"k", b"x", b"+")
    except ValueError:
        caught = caught + 1
    try:
        conn.zrangebylex(b"k", b"-", b"+", num=1)
    except ValueError:
        caught = caught + 1
    try:
        conn.zrevrangebyscore(b"k", b"1", b"0", start=-2)
    except ValueError:
        caught = caught + 1
    try:
        conn.zrangestore(b"d", b"k", b"0", b"1", byscore=True, bylex=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zrangestore(b"d", b"k", b"0", b"1", offset=0, num=1)
    except ValueError:
        caught = caught + 1
    try:
        conn.zunion(empty_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.zinter([b"k", b"other"], weights=[2.0])
    except ValueError:
        caught = caught + 1
    try:
        conn.zunionstore(b"d", [b"k"], weights=[math.nan])
    except ValueError:
        caught = caught + 1
    try:
        conn.zinterstore(b"d", [b"k"], aggregate="median")
    except ValueError:
        caught = caught + 1
    try:
        conn.zpopmin(b"k", -2)
    except ValueError:
        caught = caught + 1
    try:
        conn.bzpopmax(empty_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.bzpopmin([b"k"], math.nan)
    except ValueError:
        caught = caught + 1
    try:
        conn.bzmpop(math.inf, [b"k"], max=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zmpop([b"k"], min=True, max=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.zmpop([b"k"])
    except ValueError:
        caught = caught + 1
    try:
        conn.bzmpop(0.0, [b"k"], max=True, count=0)
    except ValueError:
        caught = caught + 1
    check(caught == 23, "sorted-set argument validation happens before I/O")

def breadth_replies(redis.Connection conn):
    auto caught = 0
    try:
        conn.zadd(b"k", {b"m": 1.0})
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zadd(b"k", {b"m": 1.0}, incr=True)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zrangebyscore(b"k", b"0", b"1", withscores=True)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zrangestore(b"d", b"k", b"0", b"-1")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zpopmin(b"k")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.bzpopmin([b"k"])
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zmpop([b"k"], min=True)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.zmpop([b"k"], min=True)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.bzmpop(0.0, [b"k"], max=True)
    except redis.ProtocolError:
        caught = caught + 1
    check(
        caught == 9 and conn.ping(),
        "sorted-set malformed replies leave next reply usable",
    )

def breadth_wire():
    list[Step] steps = [
        Step([b"ZADD", b"k", b"XX", b"GT", b"CH", b"2", b"m"], b":1\r\n"),
        Step([b"ZADD", b"k", b"LT", b"INCR", b"-0.5", b"m"], b"$3\r\n1.5\r\n"),
        Step([b"ZADD", b"k", b"NX", b"INCR", b"1", b"m"], b"$-1\r\n"),
        Step(
            [
                b"ZREVRANGEBYSCORE",
                b"k",
                b"+inf",
                b"(0",
                b"LIMIT",
                b"1",
                b"-1",
                b"WITHSCORES",
            ],
            b"*2\r\n$1\r\nm\r\n$3\r\n1.5\r\n",
        ),
        Step(
            [
                b"ZRANGESTORE",
                b"d",
                b"k",
                b"+",
                b"-",
                b"BYLEX",
                b"REV",
                b"LIMIT",
                b"0",
                b"1",
            ],
            b":1\r\n",
        ),
        Step(
            [
                b"ZUNION",
                b"2",
                b"k",
                b"k2",
                b"WEIGHTS",
                b"-2",
                b"0.5",
                b"AGGREGATE",
                b"MIN",
            ],
            b"*1\r\n$1\r\nm\r\n",
        ),
        Step([b"ZDIFFSTORE", b"d", b"2", b"k", b"k"], b":0\r\n"),
        Step(
            [b"BZMPOP", b"0.25", b"2", b"k", b"k2", b"MAX", b"COUNT", b"1"],
            b"*-1\r\n",
        ),
        Step([b"ZADD", b"k", b"1", b"m"], b":-1\r\n"),
        Step([b"ZADD", b"k", b"INCR", b"1", b"m"], b":1\r\n"),
        Step([b"ZRANGEBYSCORE", b"k", b"0", b"1", b"WITHSCORES"], b"*1\r\n$1\r\nm\r\n"),
        Step([b"ZRANGESTORE", b"d", b"k", b"0", b"-1"], b":-1\r\n"),
        Step([b"ZPOPMIN", b"k"], b"*4\r\n$1\r\nm\r\n$1\r\n1\r\n$1\r\nn\r\n$1\r\n2\r\n"),
        Step([b"BZPOPMIN", b"k", b"0"], b"*3\r\n$5\r\nwrong\r\n$1\r\nm\r\n$1\r\n1\r\n"),
        Step(
            [b"ZMPOP", b"1", b"k", b"MIN", b"COUNT", b"1"],
            b"*2\r\n$1\r\nk\r\n*0\r\n",
        ),
        Step(
            [b"ZMPOP", b"1", b"k", b"MIN", b"COUNT", b"1"],
            b"*2\r\n$1\r\nk\r\n*1\r\n*1\r\n$1\r\nm\r\n",
        ),
        Step([b"BZMPOP", b"0", b"1", b"k", b"MAX", b"COUNT", b"1"], b"$-1\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    try:
        breadth_options(conn)
        check(
            conn.zadd(b"k", {b"m": 2.0}, xx=True, gt=True, ch=True).count() == 1,
            "ZADD XX GT CH",
        )
        match conn.zadd(b"k", {b"m": -0.5}, lt=True, incr=True):
            case redis.ZAddResult.Score(value):
                check(value == 1.5, "ZADD LT INCR score")
            case _:
                raise ValueError("expected ZADD score")
        check(
            conn.zadd(b"k", {b"m": 1.0}, nx=True, incr=True).is_null(),
            "ZADD NX INCR null",
        )
        match conn.zrevrangebyscore(b"k", b"+inf", b"(0", start=1, num=-1, withscores=True):
            case redis.SortedSetResult.WithScores(values):
                check(
                    len(values) == 1 and values[0].member == b"m" and values[0].score == 1.5,
                    "score range wire",
                )
            case _:
                raise ValueError("expected scored range")
        check(
            conn.zrangestore(b"d", b"k", b"+", b"-", bylex=True, desc=True, offset=0, num=1) == 1,
            "ZRANGESTORE BYLEX REV LIMIT",
        )
        match conn.zunion([b"k", b"k2"], aggregate="min", weights=[-2.0, 0.5]):
            case redis.SortedSetResult.Members(values):
                check(len(values) == 1, "weighted ZUNION wire")
            case _:
                raise ValueError("expected members")
        check(conn.zdiffstore(b"d", [b"k", b"k"]) == 0, "ZDIFFSTORE duplicate keys")
        check(
            conn.bzmpop(0.25, [b"k", b"k2"], max=True).is_none(),
            "BZMPOP fractional time and null",
        )
        breadth_replies(conn)
    finally:
        conn.disconnect()

def main():
    auto value = bytes([255, 0, 128, 13, 10])
    list[Step] steps = [
        Step([b"ZADD", b"k", b"1.2345678901234567", value], b":1\r\n"),
        Step([b"ZADD", b"k", b"-inf", b"low"], b":1\r\n"),
        Step([b"ZADD", b"k", b"inf", b"high"], b":1\r\n"),
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
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$1\r\n" + bytes([0]) + b"\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"ZSCORE", b"k", b"v"], b"$1\r\n" + bytes([255]) + b"\r\n"))
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
            conn.zadd(b"k", {value: 1.2345678901234567}).count() == 1,
            "exact score formatting",
        )
        check(
            conn.zadd(b"k", {b"low": -math.inf}).count() == 1,
            "negative infinity formatting",
        )
        check(
            conn.zadd(b"k", {b"high": math.inf}).count() == 1,
            "positive infinity formatting",
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
            conn.zadd(b"k", {b"v": 1.0})
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
    breadth_wire()
    print("Sorted-set command wire and validation: PASS")
