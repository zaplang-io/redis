import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete Cluster shard request")
        got = got + chunk
    check(got == want, "Cluster shard command arguments")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def range_wire(int64 first, int64 last, int64 port) -> bytes:
    return bytes("*3\r\n:" + str(first) + "\r\n:" + str(last) + "\r\n*3\r\n") + bulk(b"127.0.0.1") + bytes(":" + str(port) + "\r\n") + bulk(b"node")

def ack(bytes kind, bytes name, int64 count) -> bytes:
    return b"*3\r\n" + bulk(kind) + bulk(name) + bytes(":" + str(count) + "\r\n")

def redirect_edge(bytes kind, int64 slot, int64 port) -> bytes:
    return b"-" + kind + bytes(" " + str(slot) + " 127.0.0.1:" + str(port) + "\r\n")

def seed_peer(bytes listener, int64 primary_port, bytes first, bytes second, channel[bool] done):
    auto port = net.port(listener)
    auto slot = redis.cluster_slot(first)
    auto discovery = net.accept(listener)
    command(discovery, [b"CLUSTER", b"SLOTS"])
    net.send(discovery, b"*1\r\n" + range_wire(0, 16383, port))
    net.close_conn(discovery)
    auto original = net.accept(listener)
    command(original, [b"SSUBSCRIBE", first, second])
    net.send(original, redirect_edge(b"MOVED", slot, primary_port))
    check(len(bytes(net.recv(original, 1))) == 0, "MOVED subscription socket closed")
    net.close_conn(original)
    auto refresh = net.accept(listener)
    command(refresh, [b"CLUSTER", b"SLOTS"])
    net.send(refresh, b"*3\r\n" + range_wire(0, slot - 1, port) + range_wire(slot, slot, primary_port) + range_wire(slot + 1, 16383, port))
    net.close_conn(refresh)
    auto public = net.accept(listener)
    command(public, [b"SPUBLISH", b"another-slot", b"no listeners"])
    net.send(public, b":0\r\n")
    check(len(bytes(net.recv(public, 1))) == 0, "cached seed command connection closed")
    net.close_conn(public)
    net.close_listener(listener)
    done <- True

def primary_peer(bytes listener, int64 ask_port, bytes first, bytes second, bytes payload, channel[bool] done):
    auto sub = net.accept(listener)
    command(sub, [b"SSUBSCRIBE", first, second])
    net.send(sub, ack(b"ssubscribe", first, 1) + ack(b"ssubscribe", second, 2))
    auto publisher = net.accept(listener)
    command(publisher, [b"SPUBLISH", second, payload])
    net.send(publisher, b":1\r\n")
    net.send(sub, b"*3\r\n" + bulk(b"smessage") + bulk(second) + bulk(payload))
    command(sub, [b"SUNSUBSCRIBE", first, second])
    net.send(sub, ack(b"sunsubscribe", first, 1) + ack(b"sunsubscribe", second, 0))
    check(len(bytes(net.recv(sub, 1))) == 0, "subscription is closed by its owner")
    net.close_conn(sub)
    auto moved = net.accept(listener)
    command(moved, [b"SSUBSCRIBE", b"ask:{events}"])
    net.send(moved, redirect_edge(b"ASK", redis.cluster_slot(first), ask_port))
    check(len(bytes(net.recv(moved, 1))) == 0, "ASK source socket closed")
    net.close_conn(moved)
    command(publisher, [b"SPUBLISH", second, b"cached"])
    net.send(publisher, b":0\r\n")
    auto invalid = net.accept(listener)
    command(invalid, [b"SSUBSCRIBE", b"invalid:{events}"])
    net.send(invalid, ack(b"ssubscribe", b"wrong:{events}", 1))
    check(len(bytes(net.recv(invalid, 1))) == 0, "malformed acknowledgement socket closed")
    net.close_conn(invalid)
    check(len(bytes(net.recv(publisher, 1))) == 0, "independent publish socket closed")
    net.close_conn(publisher)
    net.close_listener(listener)
    done <- True

def asking_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    command(socket, [b"ASKING"])
    net.send(socket, b"+OK\r\n")
    command(socket, [b"SSUBSCRIBE", b"ask:{events}"])
    net.send(socket, ack(b"ssubscribe", b"ask:{events}", 1))
    command(socket, [b"SUNSUBSCRIBE"])
    net.send(socket, ack(b"sunsubscribe", b"ask:{events}", 0))
    check(len(bytes(net.recv(socket, 1))) == 0, "temporary subscription socket closed")
    net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def subscribed(redis.PubSub sub, bytes wanted, int64 expected):
    match sub.next_event():
        case redis.PubSubEvent.ShardSubscribed(topic, count):
            check(topic == wanted and count == expected, "ack remains in wire order")
        case _:
            raise ValueError("Expected shard acknowledgement")

def unsubscribed(redis.PubSub sub, bytes wanted, int64 expected):
    match sub.next_event():
        case redis.PubSubEvent.ShardUnsubscribed(topic, count):
            check(topic.unwrap() == wanted and count == expected, "unsubscribe ack")
        case _:
            raise ValueError("Expected shard unsubscription")

def main():
    auto seed = net.listen("127.0.0.1", 0)
    auto primary = net.listen("127.0.0.1", 0)
    auto asking = net.listen("127.0.0.1", 0)
    auto seed_done = channel[bool]()
    auto primary_done = channel[bool]()
    auto asking_done = channel[bool]()
    auto first = bytes([255, 0]) + b"one:{events}"
    auto second = b"two:{events}"
    auto payload = bytes([0, 255, 13, 10])
    check(redis.cluster_slot(first) == redis.cluster_slot(second) and redis.cluster_slot(first) != redis.cluster_slot(b"another-slot"), "fixture key tags")
    spawn seed_peer(seed, net.port(primary), first, second, seed_done)
    spawn primary_peer(primary, net.port(asking), first, second, payload, primary_done)
    spawn asking_peer(asking, asking_done)
    auto cluster = redis.cluster_connect([redis.options(port=net.port(seed), connect_timeout_ms=3000, command_timeout_ms=3000)])
    try:
        auto rejected = 0
        try:
            cluster.ssubscribe([])
        except ValueError:
            rejected = rejected + 1
        try:
            cluster.ssubscribe([first, b"another-slot"])
        except ValueError:
            rejected = rejected + 1
        check(rejected == 2, "empty and cross-slot requests rejected before I/O")
        auto sub = cluster.ssubscribe([first, second])
        try:
            check(sub.subscription_count() == 0, "first acknowledgement counted only when delivered to caller")
            subscribed(sub, first, 1)
            subscribed(sub, second, 2)
            check(sub.subscription_count() == 2, "both shard subscriptions acknowledged")
            rejected = 0
            try:
                sub.ssubscribe([b"another-slot"])
            except ValueError:
                rejected = rejected + 1
            try:
                sub.sunsubscribe([b"another-slot"])
            except ValueError:
                rejected = rejected + 1
            check(rejected == 2, "bound session rejects other slots")
            check(cluster.spublish(second, payload) == 1, "MOVED primary used for publishing")
            match sub.next_event():
                case redis.PubSubEvent.ShardMessage(topic, data):
                    check(topic == second and data == payload, "message on dedicated socket")
                case _:
                    raise ValueError("Expected shard message")
            sub.sunsubscribe([first, second])
            unsubscribed(sub, first, 1)
            unsubscribed(sub, second, 0)
        finally:
            sub.disconnect()
        auto temporary = cluster.ssubscribe([b"ask:{events}"])
        try:
            subscribed(temporary, b"ask:{events}", 1)
            temporary.sunsubscribe()
            unsubscribed(temporary, b"ask:{events}", 0)
        finally:
            temporary.disconnect()
        check(cluster.spublish(second, b"cached") == 0, "ASK does not change the cached slot owner")
        auto invalid = False
        try:
            cluster.ssubscribe([b"invalid:{events}"])
        except redis.ProtocolError:
            invalid = True
        check(invalid, "unexpected first shard acknowledgement is rejected")
        check(cluster.spublish(b"another-slot", b"no listeners") == 0, "different shard uses its own primary")
    finally:
        cluster.disconnect()
    check(<-seed_done, "seed fixture finished")
    check(<-primary_done, "primary fixture finished")
    check(<-asking_done, "temporary fixture finished")
    print("Cluster sharded Pub/Sub routing: PASS")
