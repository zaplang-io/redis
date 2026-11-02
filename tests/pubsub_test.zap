import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete Pub/Sub control")
        got = got + chunk
    check(got == want, "Pub/Sub control ordering")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def ack(bytes kind, bytes name, int64 count) -> bytes:
    return b"*3\r\n" + bulk(kind) + bulk(name) + bytes(":" + str(count) + "\r\n")

def message(bytes topic, bytes payload) -> bytes:
    return b"*3\r\n" + bulk(b"message") + bulk(topic) + bulk(payload)

def peer(bytes listener, bytes topic, bytes payload, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        command(socket, [b"SUBSCRIBE", topic, b"other"])
        auto frame = ack(b"subscribe", topic, 1) + ack(b"subscribe", b"other", 2)
        frame = frame + message(topic, payload)
        net.send(socket, frame[0:3])
        net.send(socket, frame[3:len(frame)])
        command(socket, [b"PSUBSCRIBE", b"*"])
        net.send(
            socket,
            ack(b"psubscribe", b"*", 3) + b"*4\r\n" + bulk(b"pmessage") + bulk(b"*") + bulk(topic) + bulk(payload),
        )
        command(socket, [b"UNSUBSCRIBE", topic])
        net.send(socket, message(topic, b"before-ack") + ack(b"unsubscribe", topic, 2))
        command(socket, [b"UNSUBSCRIBE"])
        net.send(socket, ack(b"unsubscribe", b"other", 1))
        command(socket, [b"PUNSUBSCRIBE"])
        net.send(socket, ack(b"punsubscribe", b"*", 0))
        command(socket, [b"UNSUBSCRIBE"])
        net.send(socket, b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n")
        check(
            len(bytes(net.recv(socket, 1))) == 0,
            "session closes even after unsubscribe-to-zero",
        )
    finally:
        net.close_conn(socket)
    done <- True

def acknowledgements():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    auto topic = bytes([0, 255])
    auto payload = bytes([255, 0, 13, 10])
    spawn peer(listener, topic, payload, done)
    auto conn = redis.connect(
        port=net.port(listener),
        command_timeout_ms=1000,
        read_size=7,
    )
    auto sub = conn.pubsub()
    auto busy = 0
    try:
        conn.ping()
    except redis.BusyError:
        busy = busy + 1
    try:
        conn.disconnect()
    except redis.BusyError:
        busy = busy + 1
    check(busy == 2, "ordinary commands cannot share subscribed connection")
    sub.subscribe([topic, b"other"])
    for expected in range(1, 3):
        match sub.next_event():
            case redis.PubSubEvent.Subscribed(name, count):
                check(count == expected, "ack count")
            case _:
                raise ValueError("expected subscribe ack")
    match sub.next_event():
        case redis.PubSubEvent.Message(name, data):
            check(
                name == topic and data == payload,
                "fragmented/coalesced binary message",
            )
        case _:
            raise ValueError("expected message")
    sub.psubscribe([b"*"])
    match sub.next_event():
        case redis.PubSubEvent.PatternSubscribed(pattern, count):
            check(pattern == b"*" and count == 3, "pattern ack count includes channels")
        case _:
            raise ValueError("expected pattern ack")
    match sub.next_event():
        case redis.PubSubEvent.PatternMessage(pattern, name, data):
            check(
                pattern == b"*" and name == topic and data == payload,
                "pattern binary event",
            )
        case _:
            raise ValueError("expected pattern message")
    sub.unsubscribe([topic])
    match sub.next_event():
        case redis.PubSubEvent.Message(name, data):
            check(
                data == b"before-ack",
                "control call never drops intervening messages",
            )
        case _:
            raise ValueError("expected intervening message")
    match sub.next_event():
        case redis.PubSubEvent.Unsubscribed(name, count):
            check(count == 2, "unsubscribe count")
        case _:
            raise ValueError("expected unsubscribe ack")
    list[bytes] all = []
    sub.unsubscribe(all)
    sub.next_event()
    sub.punsubscribe(all)
    sub.next_event()
    check(
        sub.subscription_count() == 0 and not sub.is_closed(),
        "zero count keeps session exclusively owned",
    )
    sub.unsubscribe(all)
    match sub.next_event():
        case redis.PubSubEvent.Unsubscribed(name, count):
            match name:
                case Optional.None:
                    check(count == 0, "null-target no-subscriptions ack")
                case _:
                    raise ValueError("expected null topic")
        case _:
            raise ValueError("expected empty unsubscribe ack")
    sub.disconnect()
    sub.disconnect()
    check(
        conn.is_closed() and sub.is_closed(),
        "explicit close discards session connection",
    )
    check(<-done, "ack peer closed")

def failure_peer(bytes listener, bytes reply, channel[bool] done, bool eof):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"SUBSCRIBE", b"topic"])
    if len(reply) > 0:
        net.send(socket, reply)
    if not eof:
        check(len(bytes(net.recv(socket, 1))) == 0, "failed session socket closed")
    net.close_conn(socket)
    done <- True

def failure(bytes reply, bool timeout, bool eof = False):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn failure_peer(listener, reply, done, eof)
    auto conn = redis.connect(port=net.port(listener))
    auto sub = conn.pubsub()
    sub.subscribe([b"topic"])
    auto failed = False
    try:
        sub.next_event(50)
    except redis.ProtocolError:
        failed = not timeout
    except redis.ResponseError:
        failed = not timeout
    except redis.CommandTimeout:
        failed = timeout
    except redis.ConnectionError:
        failed = eof
    check(
        failed and conn.is_closed() and sub.is_closed(),
        "invalid/error/timeout closes session",
    )
    check(<-done, "failed session cleaned")

def cancel_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"SUBSCRIBE", b"topic"])
    net.send(socket, ack(b"subscribe", b"topic", 1))
    check(len(bytes(net.recv(socket, 1))) == 0, "cancelled read EOF")
    net.close_conn(socket)
    done <- True

def consumer(redis.PubSub sub, channel[bool] entered, Atomic[int64] cancelled):
    try:
        sub.subscribe([b"topic"])
        sub.next_event()
        entered <- True
        sub.next_event(0)
    except Cancelled:
        check(sub.is_closed(), "cancel propagated after close")
        cancelled.add(1)
    finally:
        sub.disconnect()

def fail_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel subscriber")

def cancellation():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn cancel_peer(listener, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire(dedicated=True)
    auto sub = lease.pubsub()
    auto entered = channel[bool]()
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn consumer(sub, entered, cancelled)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and sub.is_closed(),
        "scope joined subscriber cleanup",
    )
    check(not lease.release() and pool.size() == 0, "dedicated lease capacity returned")
    pool.shutdown()
    check(<-done, "cancelled subscriber peer closed")

def main():
    acknowledgements()
    failure(b"*3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:-1\r\n", False)
    failure(b"*3\r\n$7\r\nmessage\r\n$1\r\nx\r\n:1\r\n", False)
    failure(b"-NOPERM subscribe denied\r\n", False)
    failure(b"", True)
    failure(b"", False, True)
    for i in range(0, 4):
        cancellation()
    print("Dedicated Pub/Sub events and cancellation: PASS")
