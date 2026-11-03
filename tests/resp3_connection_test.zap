import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete RESP3 command")
        got = got + chunk
    check(got == want, "RESP3 wire command")

def hello(bytes socket):
    command(socket, [b"HELLO", b"3"])
    net.send(socket, b"%1\r\n+proto\r\n:3\r\n")

def peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    hello(socket)
    command(socket, [b"PING"])
    net.send(
        socket,
        b">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n"
        + b"|1\r\n+ttl\r\n:10\r\n+PONG\r\n",
    )
    command(socket, [b"ECHO", b"x"])
    net.send(socket, b"$1\r\nx\r\n")
    command(socket, [b"SUBSCRIBE", b"t"])
    net.send(
        socket,
        b">3\r\n+subscribe\r\n$1\r\nt\r\n:1\r\n"
        + b">3\r\n+message\r\n$1\r\nt\r\n$2\r\n"
        + bytes([0, 255]) + b"\r\n",
    )
    check(len(bytes(net.recv(socket, 1))) == 0, "RESP3 Pub/Sub closed")
    net.close_conn(socket)

    socket = net.accept(listener)
    hello(socket)
    command(socket, [b"PING"])
    net.send(socket, b"+PONG\r\n")
    check(len(bytes(net.recv(socket, 1))) == 0, "reconnected RESP3 socket closed")
    net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def refused_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"HELLO", b"3"])
    net.send(socket, b"-NOPROTO unsupported protocol version\r\n")
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def auth_refused_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"HELLO", b"3", b"AUTH", b"default", b"secret"])
    net.send(socket, b"-WRONGPASS invalid username-password pair\r\n")
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def backpressure_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    hello(socket)
    command(socket, [b"PING"])
    auto push = b">1\r\n+invalidate\r\n"
    for i in range(0, 65):
        net.send(socket, push)
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def cancel_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    hello(socket)
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def waiting_push(redis.Connection conn, channel[bool] entered, Atomic[int64] cancelled):
    try:
        entered <- True
        conn.next_push(0)
    except Cancelled:
        cancelled.add(1)

def fail_after(channel[bool] entered):
    auto ready = <-entered
    time.sleep(0.01)
    raise ValueError("cancel RESP3 push wait")

def cancellation():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn cancel_peer(listener, done)
    auto conn = redis.connect_resp3(port=net.port(listener), command_timeout_ms=0)
    auto entered = channel[bool]()
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn waiting_push(conn, entered, cancelled)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and conn.is_closed(),
        "cancelled RESP3 push wait closes owned connection",
    )
    check(<-done, "cancelled RESP3 peer cleaned")

def main():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn peer(listener, done)
    auto conn = redis.connect_resp3(port=net.port(listener), command_timeout_ms=1000)
    check(conn.protocol() == "RESP3", "explicit protocol mode")
    match conn.execute([b"PING"]):
        case redis.Reply.Simple(payload):
            check(payload == b"PONG", "RESP3 command compatibility reply")
        case _:
            raise ValueError("expected RESP3 compatible PING reply")
    auto attrs = conn.last_attributes()
    check(len(attrs) == 1, "RESP3 reply attributes retained")
    match attrs[0].key:
        case redis.Resp3.Simple(key):
            check(key == b"ttl", "RESP3 attribute key")
        case _:
            raise ValueError("expected simple attribute key")
    match conn.next_push(1000):
        case redis.Resp3.Push(items):
            check(len(items) == 2, "interleaved push routed separately")
        case _:
            raise ValueError("expected queued RESP3 push")
    match conn.execute3([b"ECHO", b"x"]):
        case redis.Resp3.Bulk(payload):
            check(payload == b"x", "rich RESP3 command reply")
        case _:
            raise ValueError("expected RESP3 bulk reply")
    auto sub = conn.pubsub()
    sub.subscribe([b"t"])
    match sub.next_event(1000):
        case redis.PubSubEvent.Subscribed(topic, count):
            check(topic == b"t" and count == 1, "RESP3 subscribe acknowledgement")
        case _:
            raise ValueError("expected RESP3 subscribe event")
    match sub.next_event(1000):
        case redis.PubSubEvent.Message(topic, payload):
            check(topic == b"t" and payload == bytes([0, 255]), "RESP3 binary message")
        case _:
            raise ValueError("expected RESP3 message event")
    sub.disconnect()
    conn.reconnect()
    check(conn.protocol() == "RESP3", "reconnect retains explicit RESP3 mode")
    match conn.execute3([b"PING"]):
        case redis.Resp3.Simple(payload):
            check(payload == b"PONG", "RESP3 reconnect command")
        case _:
            raise ValueError("expected RESP3 reconnect reply")
    conn.disconnect()
    check(<-done, "RESP3 peer cleaned")

    auto refused_listener = net.listen("127.0.0.1", 0)
    auto refused_done = channel[bool]()
    spawn refused_peer(refused_listener, refused_done)
    auto refused = False
    try:
        redis.connect_resp3(port=net.port(refused_listener), command_timeout_ms=1000)
    except redis.ResponseError:
        refused = True
    check(refused and <-refused_done, "HELLO refusal fails connection setup")

    auto auth_listener = net.listen("127.0.0.1", 0)
    auto auth_done = channel[bool]()
    spawn auth_refused_peer(auth_listener, auth_done)
    auto auth_failed = False
    try:
        redis.connect_resp3(
            port=net.port(auth_listener),
            password="secret",
            authenticate=True,
            command_timeout_ms=1000,
        )
    except redis.ResponseError:
        auth_failed = True
    check(auth_failed and <-auth_done, "RESP3 authentication failure closes socket")

    auto busy_listener = net.listen("127.0.0.1", 0)
    auto busy_done = channel[bool]()
    spawn backpressure_peer(busy_listener, busy_done)
    auto busy = redis.connect_resp3(port=net.port(busy_listener), command_timeout_ms=1000)
    auto overflow = False
    try:
        busy.execute([b"PING"])
    except redis.ProtocolError:
        overflow = True
    check(overflow and busy.is_closed(), "bounded push queue overflow poisons socket")
    check(<-busy_done, "backpressure peer cleaned")
    cancellation()
    print("RESP3 negotiation, push dispatch, and reconnect: PASS")
