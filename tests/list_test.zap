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
                "exact list wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def invalid_options(redis.Connection conn):
    list[bytes] empty = []
    auto errors = 0
    try:
        conn.lpush(b"k", empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.rpush(b"k", empty)
    except ValueError:
        errors = errors + 1
    check(errors == 2, "empty pushes rejected locally")

def breadth_options(redis.Connection conn):
    auto caught = 0
    list[bytes] empty = []
    try:
        conn.lpushx(b"k", empty)
    except ValueError:
        caught = caught + 1
    try:
        conn.rpushx(b"k", empty)
    except ValueError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", rank=0)
    except ValueError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", count=-2)
    except ValueError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", maxlen=-1)
    except ValueError:
        caught = caught + 1
    try:
        conn.linsert(b"k", "middle", b"a", b"b")
    except ValueError:
        caught = caught + 1
    try:
        conn.lmpop(empty, "LEFT")
    except ValueError:
        caught = caught + 1
    try:
        conn.lmpop([b"k"], "LEFT", count=0)
    except ValueError:
        caught = caught + 1
    try:
        conn.lmpop([b"k"], "middle")
    except ValueError:
        caught = caught + 1
    try:
        conn.blmpop(0.0, [b"k"], "RIGHT", count=-1)
    except ValueError:
        caught = caught + 1
    try:
        conn.blmpop(math.nan, [b"k"], "LEFT")
    except ValueError:
        caught = caught + 1
    try:
        conn.brpoplpush(b"k", b"dst", timeout=math.inf)
    except ValueError:
        caught = caught + 1
    try:
        conn.brpoplpush(b"k", b"dst", timeout=-1.0)
    except ValueError:
        caught = caught + 1
    check(caught == 13, "list options rejected before I/O")

def breadth_replies(redis.Connection conn):
    auto caught = 0
    try:
        conn.lpos(b"k", b"m")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", count=0)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", count=1)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lpos(b"k", b"m", count=2)
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.linsert(b"k", "before", b"m", b"v")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lrem(b"k", 0, b"m")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lpushx(b"k", [b"v"])
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.rpoplpush(b"k", b"dst")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lmpop([b"k"], "left")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lmpop([b"k"], "left")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.lmpop([b"k"], "left")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.blmpop(0.0, [b"k"], "right")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.blmpop(0.0, [b"k"], "right")
    except redis.ProtocolError:
        caught = caught + 1
    check(
        caught == 13 and conn.ping(),
        "typed list replies validate shapes and preserve connection",
    )

def breadth_wire():
    auto key = bytes([0, 13, 10, 255])
    auto element = bytes([255, 128, 0])
    auto key_bulk = b"$4\r\n" + key + b"\r\n"
    auto element_bulk = b"$3\r\n" + element + b"\r\n"
    list[Step] steps = [
        Step([b"LPUSHX", key, b"", element], b":0\r\n"),
        Step([b"RPUSHX", key, b"", element], b":5\r\n"),
        Step([b"LPOS", key, element], b":1\r\n"),
        Step(
            [b"LPOS", key, element, b"RANK", b"-2", b"COUNT", b"0", b"MAXLEN", b"9"],
            b"*2\r\n:4\r\n:1\r\n",
        ),
        Step([b"LPOS", key, b""], b"$-1\r\n"),
        Step([b"LPOS", key, b"", b"COUNT", b"0"], b"*0\r\n"),
        Step([b"LINSERT", key, b"AFTER", element, b""], b":6\r\n"),
        Step([b"LINSERT", key, b"BEFORE", element, b""], b":-1\r\n"),
        Step([b"LREM", key, b"-3", element], b":2\r\n"),
        Step([b"RPOPLPUSH", key, b"dst"], element_bulk),
        Step([b"BRPOPLPUSH", key, b"dst", b"0.25"], b"$0\r\n\r\n"),
        Step([b"BRPOPLPUSH", key, b"dst", b"0"], b"*-1\r\n"),
        Step(
            [b"LMPOP", b"2", b"other", key, b"LEFT", b"COUNT", b"3"],
            b"*2\r\n" + key_bulk + b"*2\r\n$0\r\n\r\n" + element_bulk,
        ),
        Step([b"BLMPOP", b"0.25", b"1", key, b"RIGHT", b"COUNT", b"1"], b"*-1\r\n"),
        Step([b"LPOS", b"k", b"m"], b":-1\r\n"),
        Step([b"LPOS", b"k", b"m", b"COUNT", b"0"], b"$-1\r\n"),
        Step([b"LPOS", b"k", b"m", b"COUNT", b"1"], b"*2\r\n:0\r\n:1\r\n"),
        Step([b"LPOS", b"k", b"m", b"COUNT", b"2"], b"*2\r\n:0\r\n$-1\r\n"),
        Step([b"LINSERT", b"k", b"BEFORE", b"m", b"v"], b":-2\r\n"),
        Step([b"LREM", b"k", b"0", b"m"], b":-1\r\n"),
        Step([b"LPUSHX", b"k", b"v"], b":-1\r\n"),
        Step([b"RPOPLPUSH", b"k", b"dst"], b"*-1\r\n"),
        Step([b"LMPOP", b"1", b"k", b"LEFT", b"COUNT", b"1"], b"*1\r\n$1\r\nk\r\n"),
        Step(
            [b"LMPOP", b"1", b"k", b"LEFT", b"COUNT", b"1"],
            b"*2\r\n$1\r\nx\r\n*1\r\n$1\r\nm\r\n",
        ),
        Step(
            [b"LMPOP", b"1", b"k", b"LEFT", b"COUNT", b"1"],
            b"*2\r\n$1\r\nk\r\n*0\r\n",
        ),
        Step(
            [b"BLMPOP", b"0", b"1", b"k", b"RIGHT", b"COUNT", b"1"],
            b"*2\r\n$1\r\nk\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        ),
        Step([b"BLMPOP", b"0", b"1", b"k", b"RIGHT", b"COUNT", b"1"], b"$-1\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    try:
        breadth_options(conn)
        check(conn.lpushx(key, [b"", element]) == 0, "LPUSHX absent")
        check(conn.rpushx(key, [b"", element]) == 5, "binary RPUSHX")
        match conn.lpos(key, element):
            case redis.ListPosition.Index(value):
                check(value == 1, "LPOS index")
            case _:
                raise ValueError("expected LPOS index")
        match conn.lpos(name=key, value=element, rank=-2, count=0, maxlen=9):
            case redis.ListPosition.Positions(values):
                check(
                    len(values) == 2 and values[0] == 4 and values[1] == 1,
                    "LPOS ranked and bounded count",
                )
            case _:
                raise ValueError("expected counted LPOS positions")
        match conn.lpos(key, b""):
            case redis.ListPosition.Missing:
                check(True, "LPOS missing")
            case _:
                raise ValueError("expected missing position")
        match conn.lpos(key, b"", count=0):
            case redis.ListPosition.Positions(values):
                check(len(values) == 0, "LPOS empty array")
            case _:
                raise ValueError("expected empty positions")
        check(
            conn.linsert(name=key, where="after", refvalue=element, value=b"") == 6,
            "LINSERT case normalization",
        )
        check(conn.linsert(key, "BEFORE", element, b"") == -1, "LINSERT pivot missing")
        check(conn.lrem(key, -3, element) == 2, "LREM signed count")
        check(
            conn.rpoplpush(src=key, dst=b"dst").unwrap() == element,
            "binary RPOPLPUSH",
        )
        check(
            conn.brpoplpush(key, b"dst", timeout=0.25).unwrap() == b"",
            "BRPOPLPUSH empty bytes",
        )
        check(conn.brpoplpush(key, b"dst").is_none(), "BRPOPLPUSH null array")
        auto popped = conn.lmpop([b"other", key], "left", count=3).unwrap()
        list[bytes] expected = [b"", element]
        check(
            popped.key == key and len(popped.values) == 2 and popped.values[0] == expected[0] and popped.values[1] == expected[1],
            "LMPOP binary batch",
        )
        check(conn.blmpop(0.25, [key], "right").is_none(), "BLMPOP null array")
        breadth_replies(conn)
    finally:
        conn.disconnect()

def main():
    auto value = bytes([255, 0, 128, 13, 10])
    list[Step] steps = [
        Step([b"LPUSH", b"k", b"a", value], b":2\r\n"),
        Step([b"RPUSH", b"k", b"", b"b"], b":4\r\n"),
        Step([b"LPOP", b"k"], b"$5\r\n" + value + b"\r\n"),
        Step([b"RPOP", b"k"], b"$0\r\n\r\n"),
        Step([b"LPOP", b"absent"], b"$-1\r\n"),
        Step([b"RPOP", b"absent"], b"$-1\r\n"),
        Step([b"LLEN", b"k"], b":2\r\n"),
        Step(
            [b"LRANGE", b"k", b"-2", b"-1"],
            b"*2\r\n$5\r\n" + value + b"\r\n$0\r\n\r\n",
        ),
        Step([b"LRANGE", b"absent", b"0", b"-1"], b"*0\r\n"),
        Step([b"LINDEX", b"k", b"-1"], b"$0\r\n\r\n"),
        Step([b"LINDEX", b"k", b"99"], b"$-1\r\n"),
        Step([b"LSET", b"k", b"-1", value], b"+OK\r\n"),
        Step([b"LTRIM", b"k", b"1", b"-1"], b"+OK\r\n"),
    ]
    steps.append(Step([b"LPUSH", b"k", b"v"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"RPUSH", b"k", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LPOP", b"k"], b"*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"RPOP", b"k"], b":1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LLEN", b"k"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LINDEX", b"k", b"0"], b"+v\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LSET", b"k", b"0", b"v"], b"+NO\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LTRIM", b"k", b"0", b"-1"], b":1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"*1\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"*1\r\n+v\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"LRANGE", b"k", b"0", b"-1"], b"*1\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(conn.lpush(b"k", [b"a", value]) == 2, "LPUSH exact order and binary")
        check(conn.rpush(b"k", [b"", b"b"]) == 4, "RPUSH exact order")
        check(conn.lpop(b"k").unwrap() == value, "binary LPOP")
        check(conn.rpop(b"k").unwrap() == b"", "empty RPOP")
        check(
            conn.lpop(b"absent").is_none() and conn.rpop(b"absent").is_none(),
            "nil pops",
        )
        check(conn.llen(b"k") == 2, "LLEN")
        list[bytes] values = conn.lrange(b"k", -2, -1)
        check(len(values) == 2, "LRANGE binary order")
        check(len(conn.lrange(b"absent", 0, -1)) == 0, "empty LRANGE")
        check(conn.lindex(b"k", -1).unwrap() == b"", "negative LINDEX")
        check(conn.lindex(b"k", 99).is_none(), "out-of-range LINDEX")
        check(conn.lset(b"k", -1, value), "negative binary LSET")
        check(conn.ltrim(b"k", 1, -1), "LTRIM bounds")
        auto errors = 0
        try:
            conn.lpush(b"k", [b"v"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.rpush(b"k", [b"v"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lpop(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.rpop(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.llen(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lindex(b"k", 0)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lset(b"k", 0, b"v")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.ltrim(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.lrange(b"k", 0, -1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        check(errors == 14, "all malformed list replies rejected")
    finally:
        conn.disconnect()
    breadth_wire()
    print("List command wire and validation: PASS")
