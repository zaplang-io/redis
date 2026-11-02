import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def request(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete request")
        got = got + chunk
    check(got == want, "request matches")

def stall(bytes listener, channel[bool] done, bool fragment, bool auth):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        if auth:
            request(socket, [b"AUTH", b"secret"])
        else:
            request(socket, [b"PING"])
        if fragment:
            net.send(socket, b"+P")
            time.sleep(0.075)
            net.send(socket, b"O")
            time.sleep(0.075)
            try:
                net.send(socket, b"NG\r\n")
            except Exception:
                auto ignored = True
        check(len(bytes(net.recv(socket, 1))) == 0, "deadline closes peer")
    finally:
        net.close_conn(socket)
    done <- True

def read_timeout(bool fragment):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn stall(listener, done, fragment, False)
    auto conn = redis.connect(
        port=net.port(listener),
        command_timeout_ms=100,
        read_size=1,
    )
    auto timed_out = False
    try:
        conn.ping()
    except redis.CommandTimeout:
        timed_out = True
    check(timed_out and conn.is_closed(), "total command budget discards socket")
    check(<-done, "timeout peer cleaned")

def auth_timeout():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn stall(listener, done, False, True)
    auto timed_out = False
    try:
        redis.connect(
            port=net.port(listener),
            password="secret",
            connect_timeout_ms=100,
        )
    except redis.ConnectTimeout:
        timed_out = True
    check(timed_out, "connect budget covers AUTH")
    check(<-done, "failed setup closed socket")

def cancel_peer(
    bytes listener,
    channel[bool] entered,
    channel[bool] done,
    bool writing,
):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        if writing:
            check(len(bytes(net.recv(socket, 1))) == 1, "backpressure writer began")
        else:
            request(socket, [b"PING"])
        entered <- True
        # Drain on cancellation. A writer can leave already-buffered bytes.
        auto chunk = bytes(net.recv(socket, 65536))
        while len(chunk) > 0:
            chunk = bytes(net.recv(socket, 65536))
    finally:
        net.close_conn(socket)
    done <- True

def fail_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel Redis owner")

def owner(
    redis.Connection conn,
    bool writing,
    bool batch,
    bytes payload,
    Atomic[int64] cancelled,
):
    try:
        if batch:
            auto p = redis.pipeline(max_bytes=67108864)
            if writing:
                p.queue([b"ECHO", payload])
            else:
                p.queue([b"PING"])
                p.queue([b"PING"])
            p.execute(conn)
        elif writing:
            conn.execute([b"ECHO", payload])
        else:
            conn.ping()
    except Cancelled:
        check(conn.is_closed(), "cleanup before cancellation observed")
        cancelled.add(1)

def cancellation(bool writing, bool batch, bytes payload):
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn cancel_peer(listener, entered, done, writing)
    auto conn = redis.connect(port=net.port(listener))
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn owner(conn, writing, batch, payload, cancelled)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and conn.is_closed(),
        "scope joins cancellation cleanup",
    )
    check(<-done, "cancelled owner leaves no live socket")

def write_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    check(len(bytes(net.recv(socket, 1))) == 1, "write began")
    time.sleep(0.3)
    auto chunk = bytes(net.recv(socket, 65536))
    while len(chunk) > 0:
        chunk = bytes(net.recv(socket, 65536))
    net.close_conn(socket)
    done <- True

def write_timeout(bytes payload):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn write_peer(listener, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=100)
    auto timed_out = False
    auto p = redis.pipeline(max_bytes=67108864)
    p.queue([b"ECHO", payload])
    try:
        p.execute(conn)
    except redis.CommandTimeout:
        timed_out = True
    check(timed_out and conn.is_closed(), "backpressured pipeline write times out")
    check(<-done, "write timeout cleaned")

def reconnect_peer(bytes listener, channel[bool] done):
    auto first = net.accept(listener)
    request(first, [b"PING"])
    check(len(bytes(net.recv(first, 1))) == 0, "old command socket discarded")
    net.close_conn(first)
    auto second = net.accept(listener)
    net.close_listener(listener)
    request(second, [b"PING"])
    net.send(second, b"+PONG\r\n")
    request(second, [b"PING"])
    net.send(second, b"+PONG\r\n")
    check(len(bytes(net.recv(second, 1))) == 0, "reconnected socket closed")
    net.close_conn(second)
    done <- True

def reconnect():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn reconnect_peer(listener, done)
    auto conn = redis.connect(
        port=net.port(listener),
        connect_timeout_ms=1000,
        command_timeout_ms=100,
    )
    auto timed_out = False
    try:
        conn.ping()
    except redis.CommandTimeout:
        timed_out = True
    check(timed_out and conn.is_closed(), "failed operation not replayed")
    conn.reconnect()
    check(conn.ping(), "fresh connection has no stale reply")
    time.sleep(0.15)
    check(conn.ping(), "command budget resets and idle socket deadline clears")
    conn.disconnect()
    check(<-done, "reconnect finished")

def validation():
    auto rejected = 0
    try:
        redis.options(connect_timeout_ms=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(command_timeout_ms=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(host="example.invalid", connect_timeout_ms=1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(resolved_ip="example.invalid")
    except ValueError:
        rejected = rejected + 1
    check(rejected == 4, "validation before network")
    auto cfg = redis.parse_url(
        "rediss://localhost?resolved_ip=127.0.0.1&connect_timeout_ms=100&command_timeout_ms=200",
    )
    check(cfg.secure and cfg.resolved_ip == "127.0.0.1", "resolved TLS URL")
    check(
        cfg.connect_timeout_ms == 100 and cfg.command_timeout_ms == 200,
        "URL budgets",
    )

def aggregate_peer(bytes listener, channel[bool] done, bool session):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        if session:
            request(socket, [b"AUTH", b"secret"])
        else:
            request(socket, [b"PING"])
            request(socket, [b"PING"])
        time.sleep(0.075)
        net.send(socket, b"+OK\r\n")
        if session:
            request(socket, [b"SELECT", b"1"])
        time.sleep(0.075)
        try:
            net.send(socket, b"+OK\r\n")
        except Exception:
            auto ignored = True
        check(len(bytes(net.recv(socket, 1))) == 0, "aggregate deadline closes socket")
    finally:
        net.close_conn(socket)
    done <- True

def aggregate(bool session):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn aggregate_peer(listener, done, session)
    auto timed_out = False
    if session:
        try:
            redis.connect(
                port=net.port(listener),
                password="secret",
                database=1,
                connect_timeout_ms=100,
            )
        except redis.ConnectTimeout:
            timed_out = True
    else:
        auto conn = redis.connect(port=net.port(listener), command_timeout_ms=100)
        auto p = redis.pipeline()
        p.queue([b"PING"])
        p.queue([b"PING"])
        try:
            p.execute(conn)
        except redis.CommandTimeout:
            timed_out = True
        check(conn.is_closed(), "partial pipeline discarded")
    check(timed_out, "one budget across all replies and setup steps")
    check(<-done, "aggregate timeout cleaned")

def connect_timeout():
    auto listener = net.listen("127.0.0.1", 0)
    list[redis.Connection] connections = []
    auto timed_out = False
    try:
        for i in range(0, 512):
            try:
                connections.append(
                    redis.connect(port=net.port(listener), connect_timeout_ms=100),
                )
            except redis.ConnectTimeout:
                timed_out = True
                break
        check(timed_out, "saturated connect budget")
    finally:
        for conn in connections:
            conn.disconnect()
        net.close_listener(listener)

def main():
    validation()
    connect_timeout()
    read_timeout(False)
    read_timeout(True)
    auth_timeout()
    aggregate(True)
    aggregate(False)
    reconnect()
    auto payload = b"x"
    for i in range(0, 25):
        payload = payload + payload
    write_timeout(payload)
    auto baseline = 0
    for i in range(0, 4):
        cancellation(False, False, b"")
        cancellation(False, True, b"")
        cancellation(True, False, payload)
        cancellation(True, True, payload)
        if os.path_exists("/proc/self/fd"):
            auto descriptors = len(os.listdir("/proc/self/fd"))
            if i == 0:
                baseline = descriptors
            else:
                check(descriptors <= baseline + 1, "no descriptor growth")
    print("Deadlines and cancellation-safe cleanup: PASS")
