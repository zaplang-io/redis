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
    print("List command wire and validation: PASS")
