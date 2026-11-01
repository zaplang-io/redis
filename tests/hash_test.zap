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
    list[redis.HashField] no_pairs = []
    list[bytes] no_fields = []
    auto errors = 0
    try:
        conn.hset(b"k", no_pairs)
    except ValueError:
        errors = errors + 1
    try:
        conn.hmget(b"k", no_fields)
    except ValueError:
        errors = errors + 1
    try:
        conn.hdel(b"k", no_fields)
    except ValueError:
        errors = errors + 1
    check(errors == 3, "empty hash arguments rejected before I/O")

def main():
    auto field = bytes([255, 0, 13, 10])
    auto value = bytes([128, 255, 0])
    list[Step] steps = [
        Step([b"PING"], b"+PONG\r\n"),
        Step([b"HSET", b"k", field, value, b"", b""], b":2\r\n"),
        Step([b"HGET", b"k", field], b"$3\r\n" + value + b"\r\n"),
        Step([b"HGET", b"k", b"missing"], b"$-1\r\n"),
        Step([b"HGET", b"k", b""], b"$0\r\n\r\n"),
        Step(
            [b"HMGET", b"k", b"", b"missing", field, field],
            b"*4\r\n$0\r\n\r\n$-1\r\n$3\r\n" + value + b"\r\n$3\r\n" + value + b"\r\n",
        ),
        Step(
            [b"HGETALL", b"k"],
            b"*4\r\n$4\r\n" + field + b"\r\n$3\r\n" + value + b"\r\n$0\r\n\r\n$0\r\n\r\n",
        ),
        Step([b"HGETALL", b"absent"], b"*0\r\n"),
        Step([b"HDEL", b"k", field, field, b"missing"], b":1\r\n"),
        Step([b"HEXISTS", b"k", b""], b":1\r\n"),
        Step([b"HEXISTS", b"k", b"missing"], b":0\r\n"),
        Step([b"HLEN", b"k"], b":1\r\n"),
        Step([b"HINCRBY", b"k", b"n", b"-3"], b":-3\r\n"),
    ]
    steps.append(Step([b"HGETALL", b"k"], b"*1\r\n$1\r\nf\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"*2\r\n:1\r\n$1\r\nv\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"*2\r\n$1\r\nf\r\n$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"*2\r\n$-1\r\n$1\r\nv\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"*2\r\n$1\r\nf\r\n+v\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGETALL", b"k"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HMGET", b"k", b"f"], b"*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HMGET", b"k", b"f"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HMGET", b"k", b"f"], b"*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HMGET", b"k", b"f"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HGET", b"k", b"f"], b":1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HEXISTS", b"k", b"f"], b":2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HLEN", b"k"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HSET", b"k", b"f", b"v"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HDEL", b"k", b"f"], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"HINCRBY", b"k", b"f", b"1"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(conn.ping(), "local validation preserved stream")
        check(
            conn.hset(b"k", [redis.HashField(field, value), redis.HashField(b"", b"")]) == 2,
            "HSET exact bytes",
        )
        check(conn.hget(b"k", field).unwrap() == value, "HGET binary")
        check(conn.hget(b"k", b"missing").is_none(), "HGET missing")
        check(conn.hget(b"k", b"").unwrap() == b"", "HGET empty")
        auto values = conn.hmget(b"k", [b"", b"missing", field, field])
        check(len(values) == 4, "HMGET count")
        check(values[0].unwrap() == b"" and values[1].is_none(), "HMGET empty/missing")
        check(
            values[2].unwrap() == value and values[3].unwrap() == value,
            "HMGET ordering and duplicates",
        )
        auto fields = conn.hgetall(b"k")
        check(len(fields) == 2, "HGETALL pair count")
        check(
            fields[0].field == field and fields[0].value == value,
            "HGETALL binary pair",
        )
        check(fields[1].field == b"" and fields[1].value == b"", "HGETALL empty pair")
        check(len(conn.hgetall(b"absent")) == 0, "HGETALL absent")
        check(conn.hdel(b"k", [field, field, b"missing"]) == 1, "HDEL")
        check(conn.hexists(b"k", b""), "HEXISTS true")
        check(not conn.hexists(b"k", b"missing"), "HEXISTS false")
        check(conn.hlen(b"k") == 1, "HLEN")
        check(conn.hincrby(b"k", b"n", -3) == -3, "HINCRBY")
        auto errors = 0
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hgetall(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hmget(b"k", [b"f"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hmget(b"k", [b"f"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hmget(b"k", [b"f"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hmget(b"k", [b"f"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hget(b"k", b"f")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hexists(b"k", b"f")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hlen(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hset(b"k", [redis.HashField(b"f", b"v")])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hdel(b"k", [b"f"])
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hincrby(b"k", b"f", 1)
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        check(errors == 17, "all malformed hash replies rejected")
    finally:
        conn.disconnect()
    print("Hash command wire and validation: PASS")
