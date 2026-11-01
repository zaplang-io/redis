import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "session command complete")
        got = got + chunk
    check(got == want, "session command bytes and ordering")

def ok(bytes socket):
    net.send(socket, b"+OK\r\n")

def simple(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Simple(data):
            check(data == want, "simple response")
        case _:
            raise ValueError("expected simple response")

def setup(bytes socket, redis.Options cfg):
    if cfg.authenticate:
        if len(cfg.username) > 0:
            command(socket, [b"AUTH", bytes(cfg.username), bytes(cfg.password)])
        else:
            command(socket, [b"AUTH", bytes(cfg.password)])
        ok(socket)
    if cfg.database != 0:
        command(socket, [b"SELECT", bytes(str(cfg.database))])
        ok(socket)
    if len(cfg.client_name) > 0:
        command(socket, [b"CLIENT", b"SETNAME", bytes(cfg.client_name)])
        ok(socket)

def session_server(bytes listener, redis.Options cfg, int64 sessions):
    auto i = 0
    while i < sessions:
        auto socket = net.accept(listener)
        setup(socket, cfg)
        command(socket, [b"PING"])
        net.send(socket, b"+PONG\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "session socket closed")
        net.close_conn(socket)
        i = i + 1
    net.close_listener(listener)

def sessions():
    auto listener = net.listen("127.0.0.1", 0)
    auto port = net.port(listener)
    auto expected = redis.options(
        port=port,
        database=2,
        username="user@name",
        password="p:ss/%+",
        client_name="worker-1",
    )
    spawn session_server(listener, expected, 2)
    auto cfg = redis.parse_url(
        "redis://user%40name:p%3Ass%2F%25%2B@127.0.0.1:" + str(port) + "/1?db=2&client_name=worker-1",
    )
    auto conn = redis.connect_options(cfg)
    simple(conn.execute([b"PING"]), b"PONG")
    # The live connection must retain a validated snapshot, not the caller object.
    cfg.database = 9
    cfg.username = "changed"
    cfg.password = "changed"
    cfg.client_name = "changed"
    conn.reconnect()
    simple(conn.execute([b"PING"]), b"PONG")
    conn.disconnect()

def password_only(bool empty):
    auto listener = net.listen("127.0.0.1", 0)
    auto port = net.port(listener)
    auto password = "secret"
    if empty:
        password = ""
    auto cfg = redis.options(port=port, password=password, authenticate=True)
    spawn session_server(listener, cfg, 1)
    auto conn = redis.connect(port=port, password=password, authenticate=True)
    simple(conn.execute([b"PING"]), b"PONG")
    conn.disconnect()

def failure_server(bytes listener, str stage, bool malformed, channel[bool] closed):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"AUTH", b"alice", b"SECRET"])
    if stage != "AUTH":
        ok(socket)
        command(socket, [b"SELECT", b"2"])
        if stage != "SELECT":
            ok(socket)
            command(socket, [b"CLIENT", b"SETNAME", b"worker"])
    if malformed:
        net.send(socket, b"$2\r\nOK\r\n")
    else:
        # Even an adversarial server response must not echo credentials in errors.
        net.send(socket, b"-WRONGPASS SECRET alice\r\n")
    auto eof = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    closed <- eof

def failed_initialization(str stage, bool malformed):
    auto listener = net.listen("127.0.0.1", 0)
    auto port = net.port(listener)
    auto closed = channel[bool]()
    spawn failure_server(listener, stage, malformed, closed)
    auto message = ""
    try:
        redis.connect(
            port=port,
            database=2,
            username="alice",
            password="SECRET",
            client_name="worker",
        )
    except redis.ResponseError as e:
        check(not malformed, "response failure category")
        message = e
    except redis.ProtocolError as e:
        check(malformed, "unexpected response category")
        message = e
    check(message.find(stage) >= 0, "initialization stage diagnostic")
    check(message.find("SECRET") < 0, "password redacted")
    check(message.find("alice") < 0, "username redacted")
    check(<-closed, "initialization failure closes socket")

def retry_server(bytes listener, channel[bool] failed_closed):
    auto cfg = redis.options(password="SECRET")
    auto first = net.accept(listener)
    setup(first, cfg)
    command(first, [b"PING"])
    ok(first)
    check(len(bytes(net.recv(first, 1))) == 0, "first session closed on reconnect")
    net.close_conn(first)
    auto second = net.accept(listener)
    command(second, [b"AUTH", b"SECRET"])
    net.send(second, b"-ERR SECRET\r\n")
    auto eof = len(bytes(net.recv(second, 1))) == 0
    net.close_conn(second)
    failed_closed <- eof
    auto third = net.accept(listener)
    net.close_listener(listener)
    setup(third, cfg)
    command(third, [b"PING"])
    ok(third)
    net.close_conn(third)

def failed_reconnect():
    auto listener = net.listen("127.0.0.1", 0)
    auto port = net.port(listener)
    auto failed_closed = channel[bool]()
    spawn retry_server(listener, failed_closed)
    auto conn = redis.connect(port=port, password="SECRET")
    simple(conn.execute([b"PING"]), b"OK")
    auto rejected = False
    try:
        conn.reconnect()
    except redis.ResponseError as e:
        rejected = e == "Redis session initialization failed: AUTH"
    check(rejected, "reconnect reauthenticates")
    check(conn.is_closed(), "failed setup leaves connection closed")
    check(conn.last_error() == b"", "credentials absent from raw diagnostics")
    check(<-failed_closed, "failed reconnect socket closed")
    conn.reconnect()
    simple(conn.execute([b"PING"]), b"OK")
    conn.disconnect()

def main():
    sessions()
    password_only(False)
    password_only(True)
    failed_initialization("AUTH", False)
    failed_initialization("SELECT", False)
    failed_initialization("CLIENT SETNAME", False)
    failed_initialization("AUTH", True)
    failed_reconnect()
    print("Session initialization and reconnect: PASS")
