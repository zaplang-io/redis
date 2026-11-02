import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete stream request")
        got = got + chunk
    check(got == want, "stream wire order/options")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def entry(bytes id, bytes field, bytes value) -> bytes:
    return b"*2\r\n" + bulk(id) + b"*4\r\n" + bulk(field) + bulk(value) + bulk(field) + bulk(b"second")

def peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    auto id = b"9223372036854775808-18446744073709551615"
    auto max_id = b"18446744073709551615-0"
    auto field = bytes([0, 255])
    auto value = bytes([255, 0, 13, 10])
    auto record = entry(id, field, value)
    try:
        command(socket, [b"XADD", b"key", b"*", field, value, field, b"second"])
        net.send(socket, bulk(id))
        command(socket, [b"XADD", b"key", max_id, field, value])
        net.send(socket, bulk(max_id))
        command(socket, [b"XLEN", b"key"])
        net.send(socket, b":2\r\n")
        command(socket, [b"XRANGE", b"key", b"(" + id, b"+", b"COUNT", b"2"])
        net.send(socket, b"*1\r\n" + record)
        command(socket, [b"XREVRANGE", b"key", b"+", b"-", b"COUNT", b"1"])
        net.send(socket, b"*1\r\n" + record)
        command(socket, [b"XDEL", b"key", id, max_id])
        net.send(socket, b":2\r\n")
        command(
            socket,
            [b"XREAD", b"COUNT", b"2", b"STREAMS", b"key", b"other", b"0", id],
        )
        net.send(
            socket,
            b"*2\r\n*2\r\n" + bulk(b"key") + b"*1\r\n" + record + b"*2\r\n" + bulk(b"other") + b"*0\r\n",
        )
        command(socket, [b"XREAD", b"BLOCK", b"25", b"STREAMS", b"key", b"$"])
        net.send(socket, b"*-1\r\n")
        command(socket, [b"XRANGE", b"missing", b"-", b"+"])
        net.send(socket, b"*0\r\n")
        command(socket, [b"XRANGE", b"bad", b"-", b"+"])
        net.send(socket, b"*1\r\n*2\r\n$3\r\n1-0\r\n*1\r\n$1\r\nx\r\n")
        command(socket, [b"XADD", b"bad", b"*", field, value])
        net.send(socket, bulk(b"18446744073709551616-0"))
        command(socket, [b"PING"])
        net.send(socket, b"+PONG\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "stream peer closed")
    finally:
        net.close_conn(socket)
    done <- True

def wire():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn peer(listener, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    auto field = bytes([0, 255])
    auto value = bytes([255, 0, 13, 10])
    auto id = b"9223372036854775808-18446744073709551615"
    auto max_id = b"18446744073709551615-0"
    list[redis.StreamField] fields = [redis.StreamField(field, value)]
    auto rejected = 0
    try:
        conn.xadd(b"key", fields, b"18446744073709551616-0")
    except ValueError:
        rejected = rejected + 1
    list[redis.StreamField] empty_fields = []
    try:
        conn.xadd(b"key", empty_fields)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.xrange(b"key", b"(+")
    except ValueError:
        rejected = rejected + 1
    try:
        conn.xdel(b"key", [b"1"])
    except ValueError:
        rejected = rejected + 1
    try:
        conn.xread([redis.StreamCursor(b"key", b"*")])
    except ValueError:
        rejected = rejected + 1
    try:
        conn.xread([redis.StreamCursor(b"key", b"0")], block_ms=-2)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.xrange(b"key", count=-1)
    except ValueError:
        rejected = rejected + 1
    check(
        rejected == 7 and not conn.is_closed(),
        "local stream validation preserves connection",
    )
    check(
        conn.xadd(b"key", [redis.StreamField(field, value), redis.StreamField(field, b"second")]) == id,
        "generated ID kept as bytes",
    )
    check(
        conn.xadd(b"key", fields, max_id) == max_id and conn.xlen(b"key") == 2,
        "uint64 IDs not narrowed",
    )
    auto entries = conn.xrange(b"key", b"(" + id, b"+", 2)
    check(len(entries) == 1 and entries[0].id == id, "range ID token")
    check(
        len(entries[0].fields) == 2 and entries[0].fields[0].field == field and entries[0].fields[1].field == field,
        "ordered duplicate field names",
    )
    check(
        entries[0].fields[0].value == value and entries[0].fields[1].value == b"second",
        "binary field values",
    )
    check(len(conn.xrevrange(b"key", count=1)) == 1, "reverse range")
    check(conn.xdel(b"key", [id, max_id]) == 2, "XDEL integer")
    auto batches = conn.xread(
        [redis.StreamCursor(b"key", b"0"), redis.StreamCursor(b"other", id)],
        count=2,
    )
    check(
        len(batches) == 2 and batches[0].key == b"key" and batches[1].key == b"other",
        "multi-stream result order",
    )
    check(
        len(batches[0].entries) == 1 and len(batches[1].entries) == 0,
        "batch entries",
    )
    check(
        len(conn.xread([redis.StreamCursor(b"key", b"$")], block_ms=25)) == 0,
        "server timeout maps to empty batches",
    )
    check(len(conn.xrange(b"missing")) == 0, "missing range empty")
    rejected = 0
    try:
        conn.xrange(b"bad")
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.xadd(b"bad", fields)
    except redis.ProtocolError:
        rejected = rejected + 1
    check(rejected == 2 and conn.ping(), "bad complete typed replies are consumed")
    conn.disconnect()
    check(<-done, "wire streams cleaned")

def shared_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"XREAD", b"STREAMS", b"BLOCK", b"0"])
    net.send(socket, b"*-1\r\n")
    check(len(bytes(net.recv(socket, 1))) == 0, "shared stream socket closed")
    net.close_conn(socket)
    done <- True

def shared():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn shared_peer(listener, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire()
    auto rejected = 0
    try:
        lease.xread([redis.StreamCursor(b"key", b"0")], block_ms=0)
    except ValueError:
        rejected = rejected + 1
    try:
        lease.execute(
            [b"XREAD", b"COUNT", b"1", b"BLOCK", b"0", b"STREAMS", b"key", b"$"],
        )
    except ValueError:
        rejected = rejected + 1
    auto batch = redis.pipeline()
    batch.queue([b"XREAD", b"BLOCK", b"0", b"STREAMS", b"key", b"$"])
    try:
        lease.execute_pipeline(batch)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 3, "shared leases reject typed/raw/pipeline blocking XREAD")
    lease.execute([b"XREAD", b"STREAMS", b"BLOCK", b"0"])
    lease.discard()
    pool.shutdown()
    check(<-done, "literal BLOCK key is not parsed as an option")

def stalled(bytes listener, channel[bool] entered, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"XREAD", b"BLOCK", b"0", b"STREAMS", b"key", b"$"])
    entered <- True
    check(len(bytes(net.recv(socket, 1))) == 0, "cancelled XREAD EOF")
    net.close_conn(socket)
    done <- True

def owner(redis.Lease lease, Atomic[int64] cancelled):
    try:
        lease.xread([redis.StreamCursor(b"key", b"$")], block_ms=0)
    except Cancelled:
        cancelled.add(1)
    finally:
        lease.release()

def fail_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel XREAD")

def cancellation():
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled(listener, entered, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire(dedicated=True)
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn owner(lease, cancelled)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and pool.size() == 0,
        "cancelled XREAD joined and discarded",
    )
    pool.shutdown()
    check(<-done, "cancelled XREAD socket cleaned")

def main():
    wire()
    shared()
    cancellation()
    print("Streams wire tokens, fields, reads, and cancellation: PASS")
