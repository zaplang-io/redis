import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(tls.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete TLS request")
        got = got + chunk
    check(got == want, "TLS request order")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def peer(bytes listener, channel[bool] done):
    auto topic = bytes([0, 255])
    auto payload = bytes([255, 0, 13, 10])
    auto socket = tls.accept(listener)
    try:
        command(socket, [b"SUBSCRIBE", topic])
        tls.send(
            socket,
            b"*3\r\n" + bulk(b"subscribe") + bulk(topic) + b":1\r\n" + b"*3\r\n" + bulk(b"message") + bulk(topic) + bulk(payload),
        )
        command(socket, [b"UNSUBSCRIBE"])
        tls.send(socket, b"*3\r\n" + bulk(b"unsubscribe") + bulk(topic) + b":0\r\n")
        check(len(bytes(tls.recv(socket, 1))) == 0, "TLS subscriber closed")
    finally:
        tls.close_conn(socket)
    socket = tls.accept(listener)
    tls.close_listener(listener)
    try:
        command(socket, [b"XADD", b"key", b"*", topic, payload])
        tls.send(socket, bulk(b"1-0"))
        command(socket, [b"XREAD", b"STREAMS", b"key", b"0"])
        tls.send(
            socket,
            b"*1\r\n*2\r\n" + bulk(b"key") + b"*1\r\n*2\r\n" + bulk(b"1-0") + b"*2\r\n" + bulk(topic) + bulk(payload),
        )
        check(len(bytes(tls.recv(socket, 1))) == 0, "TLS stream connection closed")
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
    auto lease = pool.acquire(dedicated=True)
    auto sub = lease.pubsub()
    auto topic = bytes([0, 255])
    auto payload = bytes([255, 0, 13, 10])
    try:
        sub.subscribe([topic])
        sub.next_event()
        match sub.next_event():
            case redis.PubSubEvent.Message(name, data):
                check(name == topic and data == payload, "TLS binary message")
            case _:
                raise ValueError("expected TLS message")
        list[bytes] all = []
        sub.unsubscribe(all)
        sub.next_event()
        check(sub.subscription_count() == 0, "TLS unsubscribed")
    finally:
        sub.disconnect()
        lease.release()
        pool.shutdown()
    auto conn = redis.connect_options(cfg)
    check(
        conn.xadd(b"key", [redis.StreamField(topic, payload)]) == b"1-0",
        "TLS XADD ID",
    )
    auto batches = conn.xread([redis.StreamCursor(b"key", b"0")])
    check(
        len(batches) == 1 and batches[0].entries[0].fields[0].value == payload,
        "TLS XREAD binary field",
    )
    conn.disconnect()
    check(<-done, "TLS sessions cleaned")
    print("Verified TLS Pub/Sub and Streams: PASS")
