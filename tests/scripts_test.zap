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
        conn.evalsha(b"short", empty, empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.script_exists(empty)
    except ValueError:
        errors = errors + 1
    try:
        conn.script_exists([b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"])
    except ValueError:
        errors = errors + 1
    check(errors == 3, "invalid SHA arguments fail before I/O")

def main():
    auto source = b"return ARGV[1]"
    auto script = redis.register_script(source)
    auto sha = script.sha()
    auto binary = bytes([0, 255, 13, 10])
    list[bytes] empty = []
    auto sha_reply = b"$40\r\n" + sha + b"\r\n"
    list[Step] steps = [
        Step([b"SCRIPT", b"LOAD", source], sha_reply),
        Step([b"SCRIPT", b"EXISTS", sha, sha], b"*2\r\n:1\r\n:0\r\n"),
        Step(
            [b"EVAL", source, b"2", binary, b"k", binary],
            b"$4\r\n" + binary + b"\r\n",
        ),
        Step([b"EVALSHA", sha, b"0"], b"$-1\r\n"),
        Step(
            [b"EVALSHA", sha, b"1", binary, binary],
            b"-NOSCRIPT No matching script. Please use EVAL.\r\n",
        ),
        Step([b"EVAL", source, b"1", binary, binary], b":1\r\n"),
        Step([b"EVALSHA", sha, b"0"], b":2\r\n"),
        Step(
            [b"EVALSHA", sha, b"0"],
            b"-NOSCRIPT No matching script. Please use EVAL.\r\n",
        ),
        Step([b"EVAL", source, b"0"], b":3\r\n"),
        Step([b"EVALSHA", sha, b"0"], b"-ERR after side effect\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
        Step([b"EVALSHA", sha, b"0"], b"-NOSCRIPTISH not cache miss\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
        Step([b"EVALSHA", sha, b"0"], b"-NOSCRIPT missing\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    steps.append(Step([b"SCRIPT", b"LOAD", source], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCRIPT", b"LOAD", source], b"$3\r\nabc\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"SCRIPT", b"LOAD", source],
            b"$40\r\n0000000000000000000000000000000000000000\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCRIPT", b"EXISTS", sha], b"*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCRIPT", b"EXISTS", sha], b"*1\r\n:2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCRIPT", b"EXISTS", sha], b"$-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(conn.script_load(source) == sha, "SCRIPT LOAD digest")
        auto exists = conn.script_exists([sha, sha])
        check(len(exists) == 2 and exists[0] and not exists[1], "EXISTS order")
        match conn.eval(source, [binary, b"k"], [binary]):
            case redis.Reply.Bulk(data):
                check(data == binary, "binary script arguments")
            case _:
                raise ValueError("expected binary script result")
        match conn.evalsha(sha, empty, empty):
            case redis.Reply.NullBulk:
                check(True, "null result")
            case _:
                raise ValueError("expected null script result")
        auto result = script.run(conn, [binary], [binary])
        match result:
            case redis.Reply.Integer(n):
                check(n == 1, "NOSCRIPT fallback once")
            case _:
                raise ValueError("expected integer")
        script.run(conn, empty, empty)
        script.run(conn, empty, empty)
        auto errors = 0
        try:
            script.run(conn, empty, empty)
        except redis.ResponseError:
            errors = errors + 1
        check(conn.ping(), "no retry after ordinary script error")
        try:
            script.run(conn, empty, empty)
        except redis.ResponseError:
            errors = errors + 1
        check(conn.ping(), "prefix must be exact NOSCRIPT")
        try:
            conn.evalsha(sha, empty, empty)
        except redis.ResponseError:
            errors = errors + 1
        check(errors == 3 and conn.ping(), "bare EVALSHA never falls back")
        auto malformed = 0
        try:
            conn.script_load(source)
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        try:
            conn.script_load(source)
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        try:
            conn.script_load(source)
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        try:
            conn.script_exists([sha])
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        try:
            conn.script_exists([sha])
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        try:
            conn.script_exists([sha])
        except redis.ProtocolError:
            malformed = malformed + 1
        check(conn.ping(), "complete typed script reply consumed")
        check(malformed == 6, "all malformed script replies rejected")
    finally:
        conn.disconnect()
    interrupted(False)
    interrupted(True)
    print("Scripting wire and safe fallback: PASS")

def expect(bytes socket, list[bytes] args):
    auto wanted = redis.encode_command(args)
    auto got = b""
    while len(got) < len(wanted):
        auto chunk = bytes(net.recv(socket, len(wanted) - len(got)))
        check(len(chunk) > 0, "interrupted server receives command")
        got = got + chunk
    check(got == wanted, "interrupted command bytes")

def interrupted_server(
    bytes listener,
    redis.RegisteredScript script,
    bool fallback,
    channel[int64] done,
):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    expect(socket, [b"EVALSHA", script.sha(), b"0"])
    auto executed = 0
    if fallback:
        net.send(socket, b"-NOSCRIPT missing\r\n")
        expect(socket, [b"EVAL", b"return 1", b"0"])
        executed = 1
    net.close_conn(socket)
    done <- executed

def interrupted(bool fallback):
    auto script = redis.register_script(b"return 1")
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[int64]()
    spawn interrupted_server(listener, script, fallback, done)
    auto conn = redis.connect(port=net.port(listener))
    list[bytes] empty = []
    auto failed = False
    try:
        script.run(conn, empty, empty)
    except redis.ConnectionError:
        failed = True
    check(
        failed and conn.is_closed(),
        "ambiguous script failure invalidates connection",
    )
    auto observed = <-done
    check(
        fallback and observed == 1 or not fallback and observed == 0,
        "no retry after ambiguous execution",
    )
    conn.disconnect()
