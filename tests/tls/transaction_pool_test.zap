import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(tls.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete TLS session request")
        got = got + chunk
    check(got == want, "TLS command sequence")

def peer(bytes listener, channel[bool] done):
    auto socket = tls.accept(listener)
    tls.close_listener(listener)
    try:
        command(socket, [b"MULTI"])
        tls.send(socket, b"+OK\r\n")
        command(socket, [b"PING"])
        tls.send(socket, b"+QUEUED\r\n")
        command(socket, [b"EXEC"])
        tls.send(socket, b"*1\r\n+PONG\r\n")
        command(socket, [b"RESET"])
        tls.send(socket, b"+RESET\r\n")
        command(socket, [b"WATCH", b"key"])
        tls.send(socket, b"+OK\r\n")
        command(socket, [b"UNWATCH"])
        tls.send(socket, b"+OK\r\n")
        command(socket, [b"RESET"])
        tls.send(socket, b"+RESET\r\n")
        check(len(bytes(tls.recv(socket, 1))) == 0, "pooled TLS socket closed")
    finally:
        tls.close_conn(socket)
    done <- True

def main():
    auto certs = os.getenv("REDIS_TLS_TEST_CERTS")
    auto listener = tls.listen(
        "127.0.0.1",
        0,
        certs + "/server.pem",
        certs + "/server.key",
    )
    auto done = channel[bool]()
    spawn peer(listener, done)
    auto cfg = redis.options(
        host="localhost",
        port=tls.port(listener),
        secure=True,
        resolved_ip="127.0.0.1",
        connect_timeout_ms=1000,
        command_timeout_ms=1000,
    )
    auto pool = redis.pool(cfg, capacity=1)
    auto lease = pool.acquire()
    auto tx = lease.transaction()
    try:
        tx.multi()
        tx.queue([b"PING"])
        match tx.exec():
            case redis.TransactionResult.Committed(replies):
                check(len(replies) == 1, "TLS transaction committed")
            case _:
                raise ValueError("unexpected TLS transaction outcome")
    finally:
        tx.discard()
    check(lease.release(), "TLS session reusable")
    lease = pool.acquire()
    tx = lease.transaction()
    tx.watch([b"key"])
    tx.discard()
    check(lease.release(), "TLS WATCH session cleared")
    pool.shutdown()
    check(<-done, "TLS pool shutdown")
    print("Verified TLS transactions and pooling: PASS")
