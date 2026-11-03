import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete group request")
        got = got + chunk
    check(got == want, "group wire arguments")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def entry(bytes id, bytes field, bytes value) -> bytes:
    return b"*2\r\n" + bulk(id) + b"*4\r\n" + bulk(field) + bulk(value) + bulk(field) + bulk(b"again")

def optional_id_is(Optional[bytes] actual) -> bool:
    match actual:
        case Optional.Some(_):
            return True
        case _:
            return False

def peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    auto key = bytes([0, 255])
    auto group = bytes([255, 0])
    auto consumer = bytes([0, 10])
    auto field = bytes([0, 255])
    auto value = bytes([255, 13, 10])
    auto id = b"18446744073709551615-1"
    auto record = entry(id, field, value)
    try:
        command(socket, [b"XGROUP", b"CREATE", key, group, b"$"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"XGROUP", b"CREATE", key, group, b"0-0", b"MKSTREAM"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"XGROUP", b"CREATE", key, group, b"$"])
        net.send(socket, b"-BUSYGROUP Consumer Group name already exists\r\n")
        command(
            socket,
            [
                b"XREADGROUP",
                b"GROUP",
                group,
                consumer,
                b"COUNT",
                b"2",
                b"BLOCK",
                b"10",
                b"NOACK",
                b"STREAMS",
                key,
                b">",
            ],
        )
        net.send(socket, b"*1\r\n*2\r\n" + bulk(key) + b"*1\r\n" + record)
        command(
            socket,
            [b"XREADGROUP", b"GROUP", group, consumer, b"STREAMS", key, b"0-0"],
        )
        net.send(socket, b"*1\r\n*2\r\n" + bulk(key) + b"*1\r\n" + record)
        command(socket, [b"XACK", key, group, id])
        net.send(socket, b":1\r\n")
        command(socket, [b"XPENDING", key, group])
        net.send(
            socket,
            b"*4\r\n:1\r\n" + bulk(id) + bulk(id) + b"*1\r\n*2\r\n" + bulk(consumer) + b":1\r\n",
        )
        command(
            socket,
            [b"XAUTOCLAIM", key, group, consumer, b"25", b"0-0", b"COUNT", b"2"],
        )
        net.send(
            socket,
            b"*3\r\n" + bulk(b"0-0") + b"*1\r\n" + record + b"*1\r\n" + bulk(id),
        )
        command(socket, [b"XPENDING", key, group])
        net.send(socket, b"*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n")
        command(socket, [b"XGROUP", b"DESTROY", key, group])
        net.send(socket, b":1\r\n")
        command(socket, [b"XACK", key, group, id])
        net.send(socket, b":0\r\n")
        command(
            socket,
            [b"XREADGROUP", b"GROUP", group, consumer, b"STREAMS", key, b">"],
        )
        net.send(socket, b"-NOGROUP No such key or consumer group\r\n")
        command(socket, [b"PING"])
        net.send(socket, b"+PONG\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "group peer closed")
    finally:
        net.close_conn(socket)
    done <- True

def main():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn peer(listener, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    auto key = bytes([0, 255])
    auto group = bytes([255, 0])
    auto consumer = bytes([0, 10])
    auto field = bytes([0, 255])
    auto value = bytes([255, 13, 10])
    auto id = b"18446744073709551615-1"
    conn.xgroup_create(key, group)
    conn.xgroup_create(key, group, b"0-0", True)
    auto duplicate = False
    try:
        conn.xgroup_create(key, group)
    except redis.ResponseError:
        duplicate = True
    check(duplicate and not conn.is_closed(), "BUSYGROUP remains a response error")
    list[redis.StreamCursor] streams = [redis.StreamCursor(key, b">")]
    auto fresh = conn.xreadgroup(group, consumer, streams, 2, 10, True)
    check(len(fresh) == 1 and fresh[0].key == key, "new group delivery")
    check(
        len(fresh[0].entries) == 1 and fresh[0].entries[0].id == id,
        "group entry ID preserved",
    )
    check(
        fresh[0].entries[0].fields[0].field == field and fresh[0].entries[0].fields[0].value == value and fresh[0].entries[0].fields[1].field == field,
        "binary repeated entry fields preserved",
    )
    list[redis.StreamCursor] pending_streams = [redis.StreamCursor(key, b"0-0")]
    auto pending = conn.xreadgroup(group, consumer, pending_streams)
    check(pending[0].entries[0].id == id, "pending group delivery")
    check(conn.xack(key, group, [id]) == 1, "XACK count")
    auto summary = conn.xpending(key, group)
    check(
        summary.total == 1 and optional_id_is(summary.smallest_id) and optional_id_is(summary.largest_id) and summary.consumers[0].name == consumer and summary.consumers[0].pending == 1,
        "XPENDING summary",
    )
    auto claimed = conn.xautoclaim(key, group, consumer, 25, b"0-0", 2)
    check(
        claimed.next_id == b"0-0" and claimed.entries[0].id == id and str(claimed.deleted_ids[0]) == str(id),
        "XAUTOCLAIM entries and deleted IDs",
    )
    auto empty_summary = conn.xpending(key, group)
    check(
        empty_summary.total == 0 and len(empty_summary.consumers) == 0,
        "XPENDING accepts a null consumer list when empty",
    )
    check(conn.xgroup_destroy(key, group), "XGROUP DESTROY")
    check(conn.xack(key, group, [id]) == 0, "missing pending entry")
    auto missing = False
    try:
        conn.xreadgroup(group, consumer, streams)
    except redis.ResponseError:
        missing = True
    check(missing and conn.ping(), "missing group error preserves alignment")
    conn.disconnect()
    check(<-done, "group peer cleaned")
    print("Streams consumer group commands: PASS")
