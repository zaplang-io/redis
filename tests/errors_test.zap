import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete command")
        got = got + chunk
    check(got == want, "exact command")

def simple(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Simple(data):
            check(data == want, "simple payload")
        case _:
            raise ValueError("expected simple reply")

def error_server(bytes listener, list[bytes] errors):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        auto i = 0
        while i < len(errors):
            command(socket, [b"BAD"])
            bytes error = errors[i]
            net.send(socket, b"-" + error + b"\r\n")
            command(socket, [b"PING"])
            net.send(socket, b"+PONG\r\n")
            i = i + 1
        command(socket, [b"RAW"])
        net.send(socket, b"-CUSTOM raw error\r\n")
        command(socket, [b"NESTED"])
        net.send(socket, b"*2\r\n:1\r\n-WRONGTYPE nested\r\n")
    finally:
        net.close_conn(socket)

def response_errors():
    list[bytes] errors = [
        b"WRONGTYPE Operation against a key holding the wrong kind of value",
        b"NOAUTH Authentication required.",
        b"ERR unknown command 'BAD'",
        b"FUTURE opaque new error",
        b"ERR invalid utf8 " + bytes([255, 0]),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn error_server(listener, errors)
    auto conn = redis.connect(port=net.port(listener), read_size=1)
    auto i = 0
    while i < len(errors):
        auto message = ""
        try:
            conn.execute([b"BAD"])
        except redis.ResponseError as e:
            message = e
        bytes want = errors[i]
        check(conn.last_error() == want, "lossless server error")
        if i < 4:
            check(message == str(want), "original server message")
        else:
            check(
                message == "Redis server error (hex): " + hex.encode(want),
                "binary diagnostic",
            )
        check(not conn.is_closed(), "server error leaves connection usable")
        simple(conn.execute([b"PING"]), b"PONG")
        check(conn.last_error() == want, "success preserves diagnostic")
        i = i + 1
    auto raw = conn.execute_raw([b"RAW"])
    match raw:
        case redis.Reply.Error(data):
            check(data == b"CUSTOM raw error", "raw error reply")
        case _:
            raise ValueError("expected raw error value")
    auto nested = conn.execute([b"NESTED"])
    match nested:
        case redis.Reply.Array(items):
            check(len(items) == 2, "nested result count")
            match items[1]:
                case redis.Reply.Error(data):
                    check(data == b"WRONGTYPE nested", "nested error remains a value")
                case _:
                    raise ValueError("expected nested error")
        case _:
            raise ValueError("expected array")
    check(
        conn.last_error() == b"CUSTOM raw error",
        "nested error does not replace top-level diagnostic",
    )
    conn.disconnect()
    check(redis.error_prefix(b"WRONGTYPE a message") == b"WRONGTYPE", "known prefix")
    check(redis.error_prefix(b"FUTURE") == b"FUTURE", "prefix without message")
    check(redis.error_prefix(b"") == b"", "empty prefix")

def reconnect_server(bytes listener, bool malformed):
    auto first = net.accept(listener)
    command(first, [b"FIRST"])
    if malformed:
        net.send(first, b"?")
    else:
        net.send(first, b"-ERR old session\r\n+stale\r\n")
    check(len(bytes(net.recv(first, 1))) == 0, "old connection closed")
    net.close_conn(first)
    auto second = net.accept(listener)
    net.close_listener(listener)
    try:
        command(second, [b"PING"])
        net.send(second, b"+fresh\r\n")
    finally:
        net.close_conn(second)

def reconnect(bool malformed):
    auto listener = net.listen("127.0.0.1", 0)
    spawn reconnect_server(listener, malformed)
    auto conn = redis.connect(port=net.port(listener))
    auto rejected = False
    try:
        conn.execute([b"FIRST"])
    except redis.ResponseError:
        check(not malformed, "server error category")
        check(conn.last_error() == b"ERR old session", "old diagnostic")
        check(not conn.is_closed(), "idle reconnect begins open")
        rejected = True
    except redis.ProtocolError:
        check(malformed, "protocol error category")
        check(conn.is_closed(), "malformed reply closed")
        rejected = True
    check(rejected, "first session failed as expected")
    conn.reconnect()
    check(not conn.is_closed(), "reconnect opens fresh session")
    check(conn.last_error() == b"", "reconnect clears diagnostic")
    simple(conn.execute([b"PING"]), b"fresh")
    conn.disconnect()

def no_retry_server(
    bytes listener,
    Atomic[int64] requests,
    bool before_write,
    channel[bool] closed,
):
    auto first = net.accept(listener)
    if not before_write:
        command(first, [b"INCR", b"counter"])
        requests.add(1)
    net.close_conn(first)
    closed <- True
    auto second = net.accept(listener)
    net.close_listener(listener)
    try:
        command(second, [b"PING"])
        requests.add(1)
        net.send(second, b"+fresh\r\n")
    finally:
        net.close_conn(second)

def no_retry(bool before_write):
    auto listener = net.listen("127.0.0.1", 0)
    Atomic[int64] requests = Atomic(0)
    auto closed = channel[bool]()
    spawn no_retry_server(listener, requests, before_write, closed)
    auto conn = redis.connect(port=net.port(listener))
    if before_write:
        check(<-closed, "peer closed before write")
    auto rejected = False
    try:
        conn.execute([b"INCR", b"counter"])
    except redis.ConnectionError:
        rejected = True
    check(rejected, "transport error category")
    if not before_write:
        check(<-closed, "peer closed after write")
    check(conn.is_closed(), "transport failure closes connection")
    rejected = False
    try:
        conn.execute([b"INCR", b"counter"])
    except redis.ConnectionError:
        rejected = True
    check(rejected, "closed connection rejects subsequent command")
    auto want = 1
    if before_write:
        want = 0
    check(requests.load() == want, "no automatic command replay")
    conn.reconnect()
    simple(conn.execute([b"PING"]), b"fresh")
    check(
        requests.load() == want + 1,
        "explicit reconnect sends only requested command",
    )
    conn.disconnect()

def close_listener_server(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"PING"])
    net.send(socket, b"+OK\r\n")
    net.close_conn(socket)
    done <- True

def reconnect_failure():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn close_listener_server(listener, done)
    auto conn = redis.connect(port=net.port(listener))
    simple(conn.execute([b"PING"]), b"OK")
    check(<-done, "listener stopped")
    auto rejected = False
    try:
        conn.reconnect()
    except redis.ConnectionError as e:
        rejected = e.find("connect failed") >= 0
    check(rejected, "failed reconnect error category")
    check(conn.is_closed(), "failed reconnect stays closed")
    conn.disconnect()

def main():
    response_errors()
    reconnect(False)
    reconnect(True)
    no_retry(False)
    no_retry(True)
    reconnect_failure()
    print("Client errors and recovery: PASS")
