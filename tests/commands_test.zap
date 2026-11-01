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
    auto caught = 0
    Optional[int64] zero = Optional.Some(0)
    Optional[int64] negative = Optional.Some(-1)
    Optional[int64] one = Optional.Some(1)
    list[bytes] no_keys = []
    list[redis.KeyValue] no_pairs = []
    try:
        conn.set_value(b"k", b"v", nx=True, xx=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", ex=zero)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", px=negative)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", ex=one, px=one)
    except ValueError:
        caught = caught + 1
    try:
        conn.mget(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.mset(no_pairs)
    except ValueError:
        caught = caught + 1
    try:
        conn.delete(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.unlink(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.exists(no_keys)
    except ValueError:
        caught = caught + 1
    check(caught == 9, "all invalid options rejected locally")

def main():
    list[Step] steps = [
        Step([b"PING"], b"+PONG\r\n"),
        Step([b"GET", b"k"], b"$-1\r\n"),
        Step([b"GET", b"k"], b"$0\r\n\r\n"),
        Step([b"SET", b"k", b"v", b"EX", b"60", b"NX"], b"+OK\r\n"),
        Step([b"SET", b"k", b"v", b"PX", b"1000", b"XX"], b"$-1\r\n"),
        Step([b"MGET", b"a", b"b"], b"*2\r\n$1\r\nv\r\n$-1\r\n"),
        Step([b"MSET", b"a", b"v", b"b", b""], b"+OK\r\n"),
        Step([b"DEL", b"a", b"b"], b":2\r\n"),
        Step([b"UNLINK", b"a"], b":1\r\n"),
        Step([b"EXISTS", b"a", b"a"], b":2\r\n"),
        Step([b"TYPE", b"k"], b"+string\r\n"),
        Step([b"INCR", b"k"], b":1\r\n"),
        Step([b"INCRBY", b"k", b"-2"], b":-1\r\n"),
        Step([b"DECR", b"k"], b":-2\r\n"),
        Step([b"DECRBY", b"k", b"3"], b":-5\r\n"),
        Step([b"EXPIRE", b"k", b"60"], b":1\r\n"),
        Step([b"PEXPIRE", b"k", b"1000"], b":0\r\n"),
        Step([b"TTL", b"k"], b":-1\r\n"),
        Step([b"PTTL", b"k"], b":-2\r\n"),
        Step([b"PERSIST", b"k"], b":1\r\n"),
    ]
    steps.append(Step([b"GET", b"k"], b":1\r\n"))
    steps.append(Step([b"MGET", b"k"], b"*0\r\n"))
    steps.append(Step([b"MGET", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"SET", b"k", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PERSIST", b"k"], b":2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(conn.ping(), "ping")
        check(conn.get(b"k").is_none(), "missing")
        check(conn.get(b"k").unwrap() == b"", "empty")
        check(conn.set_value(b"k", b"v", nx=True, ex=Optional.Some(60)), "SET EX NX")
        check(
            not conn.set_value(b"k", b"v", xx=True, px=Optional.Some(1000)),
            "SET PX XX",
        )
        auto values = conn.mget([b"a", b"b"])
        check(values[0].unwrap() == b"v" and values[1].is_none(), "MGET")
        check(
            conn.mset([redis.KeyValue(b"a", b"v"), redis.KeyValue(b"b", b"")]),
            "MSET",
        )
        check(conn.delete([b"a", b"b"]) == 2, "DEL")
        check(conn.unlink([b"a"]) == 1, "UNLINK")
        check(conn.exists([b"a", b"a"]) == 2, "EXISTS")
        check(conn.key_type(b"k") == b"string", "TYPE")
        check(conn.incr(b"k") == 1, "incr")
        check(conn.incrby(b"k", -2) == -1, "incrby")
        check(conn.decr(b"k") == -2, "decr")
        check(conn.decrby(b"k", 3) == -5, "decrby")
        check(conn.expire(b"k", 60) == True, "expire")
        check(conn.pexpire(b"k", 1000) == False, "pexpire")
        check(conn.ttl(b"k") == -1, "ttl")
        check(conn.pttl(b"k") == -2, "pttl")
        check(conn.persist(b"k") == True, "persist")
        auto mismatches = 0
        try:
            conn.get(b"k")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.mget([b"k"])
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.mget([b"k"])
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.set_value(b"k", b"v")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.persist(b"k")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        check(
            mismatches == 5 and conn.ping(),
            "typed reply validation consumes complete replies",
        )
    finally:
        conn.disconnect()
    print("Typed command wire and validation: PASS")
