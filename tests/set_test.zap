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
                "exact set wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
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

def breadth_options(redis.Connection conn):
    auto rejected = 0
    list[bytes] empty = []
    try:
        conn.smismember(b"k", empty)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.sintercard(empty)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.sintercard([b"k"], limit=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.sdiffstore(b"dst", empty)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.sinterstore(b"dst", empty)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.sunionstore(b"dst", empty)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 6, "set option validation happens before I/O")

def breadth_replies(redis.Connection conn):
    auto rejected = 0
    try:
        conn.smismember(b"k", [b"m", b"n"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.smismember(b"k", [b"m"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.smismember(b"k", [b"m"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.smismember(b"k", [b"m"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.smove(b"k", b"dst", b"m")
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.sintercard([b"k"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.sintercard([b"k"], limit=1)
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.sdiffstore(b"dst", [b"k"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.sinterstore(b"dst", [b"k"])
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.sunionstore(b"dst", [b"k"])
    except redis.ProtocolError:
        rejected = rejected + 1
    check(
        rejected == 10 and conn.ping(),
        "set reply validation consumes complete frames",
    )

def breadth_wire():
    auto key = bytes([0, 255, 13, 10])
    auto member = bytes([255, 0, 128])
    list[Step] steps = [
        Step([b"SMISMEMBER", key, member, b"", member], b"*3\r\n:1\r\n:0\r\n:1\r\n"),
        Step([b"SMOVE", key, b"dst", member], b":1\r\n"),
        Step([b"SMOVE", key, b"dst", b""], b":0\r\n"),
        Step([b"SINTERCARD", b"2", key, b"k"], b":3\r\n"),
        Step([b"SINTERCARD", b"2", key, b"k", b"LIMIT", b"1"], b":1\r\n"),
        Step([b"SDIFFSTORE", b"dst", key, b"k"], b":2\r\n"),
        Step([b"SINTERSTORE", b"dst", key, key], b":0\r\n"),
        Step([b"SUNIONSTORE", key, key, b"k"], b":3\r\n"),
        Step([b"SMISMEMBER", b"k", b"m", b"n"], b"*1\r\n:1\r\n"),
        Step([b"SMISMEMBER", b"k", b"m"], b"*1\r\n:2\r\n"),
        Step([b"SMISMEMBER", b"k", b"m"], b"*1\r\n$-1\r\n"),
        Step([b"SMISMEMBER", b"k", b"m"], b"$-1\r\n"),
        Step([b"SMOVE", b"k", b"dst", b"m"], b":-1\r\n"),
        Step([b"SINTERCARD", b"1", b"k"], b":-1\r\n"),
        Step([b"SINTERCARD", b"1", b"k", b"LIMIT", b"1"], b":2\r\n"),
        Step([b"SDIFFSTORE", b"dst", b"k"], b":-1\r\n"),
        Step([b"SINTERSTORE", b"dst", b"k"], b"+OK\r\n"),
        Step([b"SUNIONSTORE", b"dst", b"k"], b"$-1\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    try:
        breadth_options(conn)
        auto flags = conn.smismember(name=key, values=[member, b"", member])
        check(
            len(flags) == 3 and flags[0] and not flags[1] and flags[2],
            "SMISMEMBER flag order and duplicates",
        )
        check(conn.smove(src=key, dst=b"dst", value=member), "SMOVE binary input")
        check(not conn.smove(key, b"dst", b""), "SMOVE still distinguishes false")
        check(conn.sintercard([key, b"k"]) == 3, "SINTERCARD derived key count")
        check(conn.sintercard([key, b"k"], limit=1) == 1, "SINTERCARD passes LIMIT")
        check(
            conn.sdiffstore(b"dst", [key, b"k"]) == 2,
            "SDIFFSTORE keys in input order",
        )
        check(
            conn.sinterstore(b"dst", [key, key]) == 0,
            "SINTERSTORE forwards repeated keys",
        )
        check(
            conn.sunionstore(key, [key, b"k"]) == 3,
            "SUNIONSTORE forwards aliased destination",
        )
        breadth_replies(conn)
    finally:
        conn.disconnect()

def selection_options(redis.Connection conn):
    auto rejected = 0
    try:
        conn.spop(b"k", Optional.Some(-1))
    except ValueError:
        rejected = rejected + 1
    auto smallest = -9223372036854775807 - 1
    try:
        conn.spop(b"k", Optional.Some(smallest))
    except ValueError:
        rejected = rejected + 1
    try:
        conn.srandmember(b"k", Optional.Some(smallest))
    except ValueError:
        rejected = rejected + 1
    check(rejected == 3, "invalid set selection counts rejected before I/O")

def selection_replies(redis.Connection conn):
    auto rejected = 0
    try:
        conn.srandmember(b"k")
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.spop(b"k", Optional.Some(1))
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.spop(b"k", Optional.Some(1))
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.srandmember(b"k", Optional.Some(0))
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.srandmember(b"k", Optional.Some(-2))
    except redis.ProtocolError:
        rejected = rejected + 1
    check(rejected == 5 and conn.ping(), "bad sampling replies leave next reply usable")

def selection_wire():
    auto key = bytes([0, 13, 10, 255])
    auto binary = bytes([255, 0, 128])
    auto bulk = b"$3\r\n" + binary + b"\r\n"
    list[Step] steps = [
        Step([b"SRANDMEMBER", key], bulk),
        Step([b"SPOP", key], b"$-1\r\n"),
        Step([b"SRANDMEMBER", key, b"-2"], b"*2\r\n" + bulk + bulk),
        Step([b"SRANDMEMBER", key, b"0"], b"*0\r\n"),
        Step([b"SPOP", key, b"0"], b"*0\r\n"),
        Step([b"SPOP", key, b"9223372036854775807"], b"*2\r\n" + bulk + b"$0\r\n\r\n"),
        Step([b"SRANDMEMBER", b"k"], b"*0\r\n"),
        Step([b"SPOP", b"k", b"1"], b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"),
        Step([b"SPOP", b"k", b"1"], b"*1\r\n$-1\r\n"),
        Step([b"SRANDMEMBER", b"k", b"0"], b"$-1\r\n"),
        Step([b"SRANDMEMBER", b"k", b"-2"], b"*1\r\n$1\r\nm\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    try:
        selection_options(conn)
        check(conn.srandmember(key).unwrap() == binary, "SRANDMEMBER scalar binary")
        check(conn.spop(name=key, count=None).is_none(), "SPOP explicit omitted count")
        match conn.srandmember(key, count=Optional.Some(-2)):
            case redis.SetSelection.Members(values):
                list[bytes] expected = [binary, binary]
                check(
                    len(values) == 2 and values[0] == expected[0] and values[1] == expected[1],
                    "SRANDMEMBER duplicate wire values",
                )
            case _:
                raise ValueError("expected replacement sample")
        match conn.srandmember(key, Optional.Some(0)):
            case redis.SetSelection.Members(values):
                check(len(values) == 0, "SRANDMEMBER explicit zero")
            case _:
                raise ValueError("expected empty counted SRANDMEMBER")
        match conn.spop(key, Optional.Some(0)):
            case redis.SetSelection.Members(values):
                check(len(values) == 0, "SPOP explicit zero")
            case _:
                raise ValueError("expected empty counted SPOP")
        match conn.spop(key, Optional.Some(9223372036854775807)):
            case redis.SetSelection.Members(values):
                list[bytes] expected = [binary, b""]
                check(
                    len(values) == 2 and values[0] == expected[0] and values[1] == expected[1],
                    "SPOP full int64 and binary reply",
                )
            case _:
                raise ValueError("expected counted SPOP")
        selection_replies(conn)
    finally:
        conn.disconnect()

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
    breadth_wire()
    selection_wire()
    print("Set command wire and validation: PASS")
