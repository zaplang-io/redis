import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def read_exact(bytes socket, int64 count) -> bytes:
    auto out = b""
    while len(out) < count:
        auto n = count - len(out)
        if n > 97:
            n = 97
        auto chunk = bytes(net.recv(socket, n))
        check(len(chunk) > 0, "server received complete command")
        out = out + chunk
    return out

def expect_command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    check(read_exact(socket, len(want)) == want, "exact command bytes")

def expect_simple(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Simple(data):
            check(data == want, "simple reply payload")
        case _:
            raise ValueError("expected simple reply")

def expect_bulk(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Bulk(data):
            check(data == want, "bulk reply payload")
        case _:
            raise ValueError("expected bulk reply")

def echo_server(bytes listener, bytes payload):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        expect_command(socket, [b"PING"])
        net.send(socket, b"+PO")
        net.send(socket, b"NG\r\n")
        expect_command(socket, [b"ECHO", payload])
        net.send(socket, bytes("$" + str(len(payload)) + "\r\n") + payload + b"\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "client disconnected")
    finally:
        net.close_conn(socket)

def round_trip(bytes payload, int64 read_size):
    auto listener = net.listen("127.0.0.1", 0)
    spawn echo_server(listener, payload)
    auto conn = redis.connect(port=net.port(listener), read_size=read_size)
    check(not conn.is_closed(), "new connection open")
    auto rejected = False
    try:
        list[bytes] empty = []
        conn.execute(empty)
    except ValueError:
        rejected = True
    check(rejected, "local argument error")
    check(not conn.is_closed(), "argument error preserves connection")
    expect_simple(conn.execute([b"PING"]), b"PONG")
    expect_bulk(conn.execute([b"ECHO", payload]), payload)
    conn.disconnect()
    conn.disconnect()
    check(conn.is_closed(), "idempotent disconnect")
    rejected = False
    try:
        conn.execute([b"PING"])
    except redis.ConnectionError:
        rejected = True
    check(rejected, "use after disconnect rejected")

def coalesced_server(bytes listener):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        expect_command(socket, [b"ONE"])
        # Deliberately pre-send a second reply to exercise surplus buffering.
        net.send(socket, b"+one\r\n+two\r\n")
        expect_command(socket, [b"TWO"])
        expect_command(socket, [b"THREE"])
        net.send(socket, b"+three\r\n")
    finally:
        net.close_conn(socket)

def coalesced():
    auto listener = net.listen("127.0.0.1", 0)
    spawn coalesced_server(listener)
    auto conn = redis.connect(port=net.port(listener))
    expect_simple(conn.execute([b"ONE"]), b"one")
    expect_simple(conn.execute([b"TWO"]), b"two")
    expect_simple(conn.execute([b"THREE"]), b"three")
    conn.disconnect()

def failure_server(bytes listener, bytes reply, bool peer_eof, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    expect_command(socket, [b"PING"])
    if len(reply) > 0:
        net.send(socket, reply)
    if peer_eof:
        net.close_conn(socket)
        done <- True
    else:
        auto closed = len(bytes(net.recv(socket, 1))) == 0
        net.close_conn(socket)
        done <- closed

def failure(bytes reply, bool peer_eof, int64 max_frame, str want):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn failure_server(listener, reply, peer_eof, done)
    auto conn = redis.connect(port=net.port(listener), max_frame=max_frame)
    auto message = ""
    try:
        conn.execute([b"PING"])
    except redis.ConnectionError as e:
        message = e
    except redis.ProtocolError as e:
        message = e
    check(message.find(want) >= 0, "failure diagnostic: " + want)
    check(conn.is_closed(), "failure invalidates connection")
    conn.disconnect()
    check(<-done, "failed connection socket cleaned up")

def owned_server(bytes listener, channel[bool] entered, channel[bool] release):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        expect_command(socket, [b"PING"])
        entered <- True
        auto go = <-release
        net.send(socket, b"+OK\r\n")
        expect_command(socket, [b"PING"])
        net.send(socket, b"+OK\r\n")
    finally:
        net.close_conn(socket)

def owner(redis.Connection conn, channel[bool] finished):
    expect_simple(conn.execute([b"PING"]), b"OK")
    finished <- True

def exclusive_owner():
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto release = channel[bool]()
    auto finished = channel[bool]()
    spawn owned_server(listener, entered, release)
    auto conn = redis.connect(port=net.port(listener))
    spawn owner(conn, finished)
    check(<-entered, "owner has sent command")
    auto rejected = False
    try:
        conn.execute([b"PING"])
    except redis.BusyError as e:
        rejected = e.find("in use") >= 0
    check(rejected, "concurrent execute rejected")
    rejected = False
    try:
        conn.disconnect()
    except redis.BusyError as e:
        rejected = e.find("in use") >= 0
    check(rejected, "concurrent disconnect rejected")
    rejected = False
    try:
        conn.reconnect()
    except redis.BusyError:
        rejected = True
    check(rejected, "concurrent reconnect rejected")
    release <- True
    check(<-finished, "original owner completed")
    expect_simple(conn.execute([b"PING"]), b"OK")
    conn.disconnect()

def invalid_options():
    auto rejected = 0
    try:
        redis.connect(host="")
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(port=0)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(port=65536)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(max_frame=2)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(max_depth=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(read_size=0)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.connect(read_size=65537)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 7, "options rejected before network access")

def main():
    io.write(2, "start options\n")
    invalid_options()
    io.write(2, "start binary round trip\n")
    round_trip(bytes([0, 255, 13, 10, 128]), 1)
    io.write(2, "start coalesced\n")
    coalesced()
    io.write(2, "start EOF tests\n")
    failure(b"", True, 4096, "before replying")
    failure(b"$5\r\nab", True, 4096, "during a reply")
    failure(b"?", False, 4096, "unknown RESP2 type")
    failure(b"$100\r\n", False, 32, "exceeds byte limit")
    io.write(2, "start ownership\n")
    exclusive_owner()
    auto large = bytes([0, 255, 13, 10])
    auto i = 0
    while i < 14:
        large = large + large
        i = i + 1
    io.write(2, "start large round trip\n")
    round_trip(large, 4096)
    print("TCP connection: PASS")
