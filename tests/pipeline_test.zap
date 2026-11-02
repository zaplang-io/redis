import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def read_exact(bytes socket, int64 count) -> bytes:
    auto out = b""
    while len(out) < count:
        auto chunk = bytes(net.recv(socket, count - len(out)))
        check(len(chunk) > 0, "complete pipeline request")
        out = out + chunk
    return out

def server(bytes listener, bytes wire, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        # Must receive ALL commands before replying: sequential execution stalls.
        check(read_exact(socket, len(wire)) == wire, "queued snapshot and order")
        net.send(socket, b"+fir")
        net.send(socket, b"st\r\n-ERR middle\r\n:3\r\n")
        auto ping = redis.encode_command([b"PING"])
        check(read_exact(socket, len(ping)) == ping, "post-drain reuse")
        net.send(socket, b"+PONG\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "closed")
    finally:
        net.close_conn(socket)
    done <- True

def round_trip():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    auto wire = redis.encode_command([b"ONE", b"original"])
    wire = wire + redis.encode_command([b"BAD"]) + redis.encode_command([b"THREE"])
    spawn server(listener, wire, done)
    auto conn = redis.connect(
        port=net.port(listener),
        command_timeout_ms=1000,
        read_size=1,
    )
    auto p = redis.pipeline(max_commands=3, max_bytes=len(wire))
    list[bytes] args = [b"ONE", b"original"]
    p.queue(args)
    args[1] = b"mutated"
    p.queue([b"BAD"])
    p.queue([b"THREE"])
    auto replies = p.execute(conn)
    check(len(replies) == 3, "one reply per command")
    match replies[0]:
        case redis.Reply.Simple(data):
            check(data == b"first", "fragmented first reply")
        case _:
            raise ValueError("first reply")
    match replies[1]:
        case redis.Reply.Error(data):
            check(data == b"ERR middle", "middle error retained")
        case _:
            raise ValueError("middle error")
    match replies[2]:
        case redis.Reply.Integer(value):
            check(value == 3, "success after error drained")
        case _:
            raise ValueError("third reply")
    check(conn.last_error() == b"ERR middle", "lossless last error")
    check(len(redis.pipeline().execute(conn)) == 0, "empty pipeline")
    check(conn.ping(), "reuse after entire pipeline drained")
    auto rejected = 0
    try:
        p.execute(conn)
    except ValueError:
        rejected = rejected + 1
    try:
        p.queue([b"PING"])
    except ValueError:
        rejected = rejected + 1
    check(rejected == 2, "sealed builder cannot replay")
    conn.disconnect()
    check(<-done, "peer cleaned")

def limits():
    auto p = redis.pipeline(max_commands=1, max_bytes=14)
    auto rejected = 0
    try:
        p.queue([b"PING", b"too large"])
    except ValueError:
        rejected = rejected + 1
    list[bytes] empty = []
    try:
        p.queue(empty)
    except ValueError:
        rejected = rejected + 1
    p.queue([b"PING"])
    try:
        p.queue([b"PING"])
    except ValueError:
        rejected = rejected + 1
    try:
        redis.pipeline(max_commands=0)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.pipeline(max_bytes=0)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 5, "limits and empty command rejected locally")

def broken_server(bytes listener, bytes reply, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    auto wire = redis.encode_command([b"ONE"]) + redis.encode_command([b"TWO"])
    check(read_exact(socket, len(wire)) == wire, "failure batch complete")
    net.send(socket, reply)
    net.close_conn(socket)
    done <- True

def broken(bytes reply, bool protocol):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn broken_server(listener, reply, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    auto p = redis.pipeline()
    p.queue([b"ONE"])
    p.queue([b"TWO"])
    auto rejected = False
    try:
        p.execute(conn)
    except redis.ConnectionError:
        rejected = not protocol
    except redis.ProtocolError:
        rejected = protocol
    check(rejected and conn.is_closed(), "partial pipeline discarded")
    check(<-done, "broken peer closed")

def main():
    limits()
    round_trip()
    broken(b"+first\r\n$5\r\nab", False)
    broken(b"-ERR first\r\n?", True)
    print("Bounded ordered pipelines: PASS")
