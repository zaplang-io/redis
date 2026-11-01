import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def listener() -> bytes:
    auto certs = os.getenv("REDIS_TLS_TEST_CERTS")
    check(len(certs) > 0, "TLS fixture directory required")
    return tls.listen("127.0.0.1", 0, certs + "/server.pem", certs + "/server.key")

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto count = len(want) - len(got)
        if count > 4096:
            count = 4096
        auto chunk = bytes(tls.recv(socket, count))
        check(len(chunk) > 0, "complete TLS command")
        got = got + chunk
    check(got == want, "binary TLS command bytes")

def simple(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Simple(data):
            check(data == want, "simple TLS reply")
        case _:
            raise ValueError("expected simple TLS reply")

def bulk(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Bulk(data):
            check(data == want, "binary TLS reply")
        case _:
            raise ValueError("expected bulk TLS reply")

def server(bytes listen, bytes payload):
    auto i = 0
    while i < 2:
        auto socket = tls.accept(listen)
        try:
            command(socket, [b"AUTH", b"alice", b"SECRET"])
            tls.send(socket, b"+OK\r\n")
            command(socket, [b"SELECT", b"1"])
            tls.send(socket, b"+OK\r\n")
            command(socket, [b"CLIENT", b"SETNAME", b"tls-worker"])
            tls.send(socket, b"+OK\r\n")
            command(socket, [b"PING"])
            tls.send(socket, b"+PO")
            tls.send(socket, b"NG\r\n")
            command(socket, [b"ECHO", payload])
            tls.send(
                socket,
                bytes("$" + str(len(payload)) + "\r\n") + payload + b"\r\n",
            )
            command(socket, [b"BAD"])
            tls.send(socket, b"-ERR tls fixture\r\n")
            command(socket, [b"PING"])
            tls.send(socket, b"+PONG\r\n")
            check(len(bytes(tls.recv(socket, 1))) == 0, "TLS session disconnected")
        finally:
            tls.close_conn(socket)
        i = i + 1
    tls.close_listener(listen)

def round_trip():
    auto listen = listener()
    auto port = tls.port(listen)
    auto payload = bytes([0, 255, 13, 10])
    for i in range(0, 14):
        payload = payload + payload
    spawn server(listen, payload)
    auto cfg = redis.parse_url(
        "rediss://alice:SECRET@localhost:" + str(port) + "/1?client_name=tls-worker",
    )
    cfg.read_size = 257
    auto conn = redis.connect_options(cfg)
    try:
        auto i = 0
        while i < 2:
            if i > 0:
                conn.reconnect()
                check(conn.last_error() == b"", "TLS reconnect clears last error")
            simple(conn.execute([b"PING"]), b"PONG")
            bulk(conn.execute([b"ECHO", payload]), payload)
            auto rejected = False
            try:
                conn.execute([b"BAD"])
            except redis.ResponseError as message:
                rejected = message == "ERR tls fixture"
            check(rejected, "TLS server errors preserved")
            check(not conn.is_closed(), "TLS server error leaves connection usable")
            simple(conn.execute([b"PING"]), b"PONG")
            i = i + 1
    finally:
        conn.disconnect()
    conn.disconnect()
    check(conn.is_closed(), "idempotent TLS close")

def failure_server(bytes listen, bytes reply, bool close_peer, channel[bool] done):
    auto socket = tls.accept(listen)
    tls.close_listener(listen)
    command(socket, [b"PING"])
    if len(reply) > 0:
        tls.send(socket, reply)
    if close_peer:
        tls.close_conn(socket)
        done <- True
    else:
        auto closed = len(bytes(tls.recv(socket, 1))) == 0
        tls.close_conn(socket)
        done <- closed

def failure(bytes reply, bool close_peer):
    auto listen = listener()
    auto port = tls.port(listen)
    auto done = channel[bool]()
    spawn failure_server(listen, reply, close_peer, done)
    auto conn = redis.connect(host="localhost", port=port, secure=True)
    auto rejected = False
    try:
        conn.execute([b"PING"])
    except redis.ConnectionError:
        check(close_peer, "TLS EOF category")
        rejected = True
    except redis.ProtocolError:
        check(not close_peer, "TLS framing category")
        rejected = True
    check(rejected, "TLS failure propagated")
    check(conn.is_closed(), "TLS failure invalidates connection")
    conn.disconnect()
    check(<-done, "TLS failure socket cleaned up")

def main():
    round_trip()
    failure(b"", True)
    failure(b"$5\r\nab", True)
    failure(b"?", False)
    print("Verified TLS connection: PASS")
