import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete Cluster redirect request")
        got = got + chunk
    check(got == want, "Cluster redirect command arguments")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def seed_peer(
    bytes listener,
    int64 seed_port,
    int64 primary_port,
    int64 slot,
    channel[bool] done,
):
    auto discovery = net.accept(listener)
    print("Cluster redirect fixture: seed accepted discovery")
    command(discovery, [b"CLUSTER", b"SLOTS"])
    print("Cluster redirect fixture: discovery command received")
    net.send(
        discovery,
        b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" + bulk(b"127.0.0.1") + bytes(":" + str(seed_port) + "\r\n") + bulk(b"node"),
    )
    print("Cluster redirect fixture: topology sent")
    auto discovery_closed = len(bytes(net.recv(discovery, 1))) == 0
    check(discovery_closed, "client closes the discovery connection")
    net.close_conn(discovery)

    auto original = net.accept(listener)
    command(original, [b"GET", b"account:{42}"])
    net.send(
        original,
        bytes("-MOVED " + str(slot) + " 127.0.0.1:" + str(primary_port) + "\r\n"),
    )
    auto original_closed = len(bytes(net.recv(original, 1))) == 0
    net.close_conn(original)

    # The first refresh installs the moved primary; five more refreshes are
    # triggered by the deliberate redirect loop, followed by one public refresh.
    for _ in range(0, 7):
        auto refresh = net.accept(listener)
        command(refresh, [b"CLUSTER", b"SLOTS"])
        net.send(
            refresh,
            b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" + bulk(b"127.0.0.1") + bytes(":" + str(primary_port) + "\r\n") + bulk(b"node"),
        )
        net.close_conn(refresh)
    net.close_listener(listener)
    done <- original_closed

def primary_peer(bytes listener, int64 slot, int64 ask_port, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"GET", b"account:{42}"])
    net.send(socket, bulk(b"moved"))
    command(socket, [b"MGET", b"account:{42}", b"session:{42}"])
    net.send(socket, b"*2\r\n" + bulk(b"one") + bulk(b"two"))
    command(socket, [b"GET", b"ask:{42}"])
    net.send(
        socket,
        bytes("-ASK " + str(slot) + " 127.0.0.1:" + str(ask_port) + "\r\n"),
    )
    for _ in range(0, 6):
        command(socket, [b"GET", b"loop:{42}"])
        net.send(
            socket,
            bytes(
                "-MOVED " + str(slot) + " 127.0.0.1:" + str(net.port(listener)) + "\r\n",
            ),
        )
    command(socket, [b"GET", b"malformed:{42}"])
    net.send(socket, b"-MOVED invalid redirect\r\n")
    command(socket, [b"SET", b"account:{42}", b"value"])
    net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def asking_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"ASKING"])
    net.send(socket, b"+OK\r\n")
    command(socket, [b"GET", b"ask:{42}"])
    net.send(socket, bulk(b"asked"))
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def main():
    auto seed_listener = net.listen("127.0.0.1", 0)
    auto primary_listener = net.listen("127.0.0.1", 0)
    auto asking_listener = net.listen("127.0.0.1", 0)
    auto seed_done = channel[bool]()
    auto primary_done = channel[bool]()
    auto asking_done = channel[bool]()
    auto slot = redis.cluster_slot(b"account:{42}")
    spawn seed_peer(
        seed_listener,
        net.port(seed_listener),
        net.port(primary_listener),
        slot,
        seed_done,
    )
    auto cluster = redis.cluster_connect(
        [
            redis.options(
                "127.0.0.1",
                net.port(seed_listener),
                connect_timeout_ms=5000,
                command_timeout_ms=5000,
            ),
        ],
    )
    spawn primary_peer(primary_listener, slot, net.port(asking_listener), primary_done)
    spawn asking_peer(asking_listener, asking_done)
    match cluster.execute_for(b"account:{42}", [b"GET", b"account:{42}"]):
        case redis.Reply.Bulk(value):
            check(value == b"moved", "MOVED refreshes and retries at new primary")
        case _:
            raise ValueError("expected MOVED command reply")
    match cluster.execute_multi([b"MGET", b"account:{42}", b"session:{42}"]):
        case redis.Reply.Array(values):
            auto first_matches = False
            auto second_matches = False
            if len(values) == 2:
                match values[0]:
                    case redis.Reply.Bulk(value):
                        first_matches = value == b"one"
                    case _:
                        auto ignored = 0
                match values[1]:
                    case redis.Reply.Bulk(value):
                        second_matches = value == b"two"
                    case _:
                        auto ignored = 0
            check(first_matches and second_matches, "same-slot multi-key command")
        case _:
            raise ValueError("expected same-slot MGET array")
    match cluster.execute_for(b"ask:{42}", [b"GET", b"ask:{42}"]):
        case redis.Reply.Bulk(value):
            check(value == b"asked", "ASKING precedes command on temporary primary")
        case _:
            raise ValueError("expected ASK command reply")
    auto loop_rejected = False
    try:
        cluster.execute_for(b"loop:{42}", [b"GET", b"loop:{42}"])
    except redis.ProtocolError:
        loop_rejected = True
    check(loop_rejected, "Cluster redirect loop is bounded")
    auto malformed_rejected = False
    try:
        cluster.execute_for(b"malformed:{42}", [b"GET", b"malformed:{42}"])
    except redis.ProtocolError:
        malformed_rejected = True
    check(malformed_rejected, "malformed redirect is a protocol error")
    auto ambiguous_write_not_replayed = False
    try:
        cluster.execute_for(b"account:{42}", [b"SET", b"account:{42}", b"value"])
    except redis.ConnectionError:
        ambiguous_write_not_replayed = True
    check(ambiguous_write_not_replayed, "ambiguous node failure is not retried")
    cluster.refresh()
    auto cross_slot_rejected = False
    try:
        cluster.execute_multi([b"MGET", b"account:{42}", b"other"])
    except ValueError:
        cross_slot_rejected = True
    check(cross_slot_rejected, "cross-slot multi-key command rejected locally")
    cluster.disconnect()
    check(<-seed_done, "seed topology fixture cleaned")
    check(<-primary_done, "redirected primary fixture cleaned")
    check(<-asking_done, "ASKING fixture cleaned")
    print("Cluster redirections and slot validation: PASS")
