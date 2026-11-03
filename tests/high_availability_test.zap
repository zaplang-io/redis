import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete high-availability command")
        got = got + chunk
    check(got == want, "high-availability command arguments")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def sentinel_peer(
    bytes listener,
    int64 first_port,
    int64 second_port,
    channel[bool] done,
):
    for port in [first_port, second_port]:
        auto socket = net.accept(listener)
        command(socket, [b"AUTH", b"sentinel-secret"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"SENTINEL", b"GET-MASTER-ADDR-BY-NAME", b"orders"])
        net.send(socket, b"*2\r\n" + bulk(b"127.0.0.1") + bulk(bytes(str(port))))
        net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def data_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"AUTH", b"data-secret"])
    net.send(socket, b"+OK\r\n")
    command(socket, [b"PING"])
    net.send(socket, b"+PONG\r\n")
    auto closed = len(bytes(net.recv(socket, 1))) == 0
    net.close_conn(socket)
    net.close_listener(listener)
    done <- closed

def cluster_peer(bytes listener, int64 port, channel[bool] done):
    auto discovery = net.accept(listener)
    command(discovery, [b"CLUSTER", b"SLOTS"])
    net.send(
        discovery,
        b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" + bulk(b"127.0.0.1") + bytes(":" + str(port) + "\r\n") + bulk(b"node-a"),
    )
    net.close_conn(discovery)

    auto data = net.accept(listener)
    command(data, [b"GET", b"account:{42}"])
    net.send(data, bulk(b"first"))
    command(data, [b"GET", b"session:{42}"])
    net.send(data, bulk(b"second"))
    auto refresh = net.accept(listener)
    command(refresh, [b"CLUSTER", b"SLOTS"])
    net.send(
        refresh,
        b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" + bulk(b"127.0.0.1") + bytes(":" + str(port) + "\r\n") + bulk(b"node-a"),
    )
    net.close_conn(refresh)
    auto closed = len(bytes(net.recv(data, 1))) == 0
    net.close_conn(data)
    net.close_listener(listener)
    done <- closed

def main():
    check(redis.cluster_slot(b"123456789") == 12739, "CRC16/XMODEM vector")
    check(redis.cluster_slot(b"foo") == 12182, "plain Redis Cluster slot")
    check(
        redis.cluster_slot(b"foo{bar}zap") == redis.cluster_slot(b"bar"),
        "binary-safe hash-tag selection",
    )

    auto sentinel_listener = net.listen("127.0.0.1", 0)
    auto first_data_listener = net.listen("127.0.0.1", 0)
    auto second_data_listener = net.listen("127.0.0.1", 0)
    auto sentinel_done = channel[bool]()
    auto first_data_done = channel[bool]()
    auto second_data_done = channel[bool]()
    spawn sentinel_peer(
        sentinel_listener,
        net.port(first_data_listener),
        net.port(second_data_listener),
        sentinel_done,
    )
    spawn data_peer(first_data_listener, first_data_done)
    spawn data_peer(second_data_listener, second_data_done)
    list[redis.Options] sentinel_seeds = [
        redis.options(
            "127.0.0.1",
            net.port(sentinel_listener),
            password="sentinel-secret",
            connect_timeout_ms=1000,
            command_timeout_ms=1000,
        ),
    ]
    auto data_config = redis.options(
        "127.0.0.1",
        6379,
        password="data-secret",
        connect_timeout_ms=1000,
        command_timeout_ms=1000,
    )
    auto sentinel = redis.sentinel_connect(sentinel_seeds, b"orders", data_config)
    check(
        sentinel.primary().port == net.port(first_data_listener),
        "initial Sentinel primary",
    )
    match sentinel.execute([b"PING"]):
        case redis.Reply.Simple(value):
            check(value == b"PONG", "Sentinel data command")
        case _:
            raise ValueError("expected Sentinel PING response")
    sentinel.reconnect()
    check(
        sentinel.primary().port == net.port(second_data_listener),
        "Sentinel primary refresh",
    )
    match sentinel.execute([b"PING"]):
        case redis.Reply.Simple(value):
            check(value == b"PONG", "command after Sentinel reconnect")
        case _:
            raise ValueError("expected reconnected Sentinel PING response")
    sentinel.disconnect()
    check(<-sentinel_done, "Sentinel fixture cleaned")
    check(<-first_data_done, "old primary connection replaced")
    check(<-second_data_done, "new primary connection closed")

    auto cluster_listener = net.listen("127.0.0.1", 0)
    auto cluster_done = channel[bool]()
    spawn cluster_peer(cluster_listener, net.port(cluster_listener), cluster_done)
    auto cluster = redis.cluster_connect(
        [
            redis.options(
                "127.0.0.1",
                net.port(cluster_listener),
                connect_timeout_ms=1000,
                command_timeout_ms=1000,
            ),
        ],
    )
    match cluster.execute_for(b"account:{42}", [b"GET", b"account:{42}"]):
        case redis.Reply.Bulk(value):
            check(value == b"first", "Cluster routes a single-key command")
        case _:
            raise ValueError("expected Cluster GET response")
    match cluster.execute_for(b"session:{42}", [b"GET", b"session:{42}"]):
        case redis.Reply.Bulk(value):
            check(value == b"second", "Cluster reuses the primary connection")
        case _:
            raise ValueError("expected reused Cluster GET response")
    cluster.refresh()
    auto multi_key_rejected = False
    try:
        cluster.execute_for(b"account:{42}", [b"MGET", b"account:{42}", b"other"])
    except ValueError:
        multi_key_rejected = True
    check(multi_key_rejected, "Cluster rejects multi-key commands locally")
    cluster.disconnect()
    check(<-cluster_done, "Cluster fixture cleaned")
    print("Sentinel discovery and Cluster single-key routing: PASS")
