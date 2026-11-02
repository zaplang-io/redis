import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def listener() -> bytes:
    auto certs = os.getenv("REDIS_TLS_TEST_CERTS")
    return tls.listen("127.0.0.1", 0, certs + "/server.pem", certs + "/server.key")

def open_client(int64 port, int64 command_ms = 100) -> redis.Connection:
    return redis.connect(
        host="localhost",
        port=port,
        secure=True,
        resolved_ip="127.0.0.1",
        connect_timeout_ms=1000,
        command_timeout_ms=command_ms,
    )

def stalled_peer(
    bytes listen,
    channel[bool] entered,
    channel[bool] done,
    bool write,
    bool delayed,
):
    auto socket = tls.accept(listen)
    tls.close_listener(listen)
    try:
        check(len(bytes(tls.recv(socket, 1))) == 1, "TLS operation began")
        entered <- True
        if delayed:
            time.sleep(0.3)
        try:
            auto chunk = bytes(tls.recv(socket, 65536))
            while len(chunk) > 0:
                chunk = bytes(tls.recv(socket, 65536))
        except Exception:
            # An aborted backpressured write can end mid-TLS-record. The peer
            # may report a truncation error instead of clean close_notify/EOF.
            # Client timeout/cancellation and closed state are asserted below.
            check(write, "only an aborted writer may truncate TLS records")
    finally:
        tls.close_conn(socket)
    done <- True

def release(channel[bool] entered):
    auto started = <-entered

def fail_after(channel[bool] entered):
    auto started = <-entered
    raise ValueError("cancel TLS Redis owner")

def owner(redis.Connection conn, bytes payload, bool write, Atomic[int64] cancelled):
    try:
        if write:
            auto p = redis.pipeline(max_bytes=67108864)
            p.queue([b"ECHO", payload])
            p.execute(conn)
        else:
            conn.ping()
    except Cancelled:
        check(conn.is_closed(), "TLS cleaned before propagation")
        cancelled.add(1)

def cancelled(bool write, bytes payload):
    auto listen = listener()
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled_peer(listen, entered, done, write, False)
    auto conn = open_client(tls.port(listen), 0)
    Atomic[int64] count = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn owner(conn, payload, write, count)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(failed and count.load() == 1 and conn.is_closed(), "TLS cancellation drained")
    check(<-done, "TLS cancellation socket EOF")

def timeout(bool write, bytes payload):
    auto listen = listener()
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled_peer(listen, entered, done, write, True)
    spawn release(entered)
    auto conn = open_client(tls.port(listen))
    auto timed_out = False
    try:
        if write:
            auto p = redis.pipeline(max_bytes=67108864)
            p.queue([b"ECHO", payload])
            p.execute(conn)
        else:
            conn.ping()
    except redis.CommandTimeout:
        timed_out = True
    check(timed_out and conn.is_closed(), "TLS command deadline")
    check(<-done, "TLS timed-out socket EOF")

def handshake_peer(bytes listen, channel[bool] entered, channel[bool] done):
    auto socket = net.accept(listen)
    net.close_listener(listen)
    check(len(bytes(net.recv(socket, 1))) == 1, "ClientHello began")
    entered <- True
    auto chunk = bytes(net.recv(socket, 65536))
    while len(chunk) > 0:
        chunk = bytes(net.recv(socket, 65536))
    net.close_conn(socket)
    done <- True

def dialing(int64 port, Atomic[int64] count):
    try:
        auto conn = open_client(port)
        conn.disconnect()
    except Cancelled:
        count.add(1)

def handshake(bool cancel):
    auto listen = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn handshake_peer(listen, entered, done)
    if cancel:
        Atomic[int64] count = Atomic(0)
        auto failed = False
        try:
            scope:
                spawn dialing(net.port(listen), count)
                spawn fail_after(entered)
        except Exception:
            failed = True
        check(failed and count.load() == 1, "TLS handshake cancellation")
    else:
        spawn release(entered)
        auto timed_out = False
        try:
            redis.connect(
                host="localhost",
                port=net.port(listen),
                secure=True,
                resolved_ip="127.0.0.1",
                connect_timeout_ms=100,
            )
        except redis.ConnectTimeout:
            timed_out = True
        check(timed_out, "TLS handshake shares connect budget")
    check(<-done, "partial TLS handshake closed before scope exit")

def main():
    auto payload = b"x"
    for i in range(0, 25):
        payload = payload + payload
    io.write(2, "TLS deadline: stalled handshake\n")
    handshake(False)
    io.write(2, "TLS deadline: cancelled handshake\n")
    handshake(True)
    io.write(2, "TLS deadline: read timeout\n")
    timeout(False, b"")
    io.write(2, "TLS deadline: write timeout\n")
    timeout(True, payload)
    io.write(2, "TLS deadline: read cancellation\n")
    cancelled(False, b"")
    io.write(2, "TLS deadline: write cancellation\n")
    cancelled(True, payload)
    print("TLS connect and command deadlines/cancellation: PASS")
