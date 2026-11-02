import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete blocking command")
        got = got + chunk
    check(got == want, "blocking wire arguments")

def bulk(bytes data) -> bytes:
    return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"

def wire_peer(bytes listener, bytes key, bytes value, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        command(socket, [b"BLPOP", b"first", key, b"0.25"])
        net.send(socket, b"*2\r\n" + bulk(key) + bulk(value))
        command(socket, [b"BRPOP", key, b"0"])
        net.send(socket, b"*-1\r\n")
        command(socket, [b"BLMOVE", key, b"dest", b"RIGHT", b"LEFT", b"0.25"])
        net.send(socket, bulk(value))
        command(socket, [b"BLMOVE", key, b"dest", b"LEFT", b"RIGHT", b"0"])
        net.send(socket, b"$-1\r\n")
        command(socket, [b"BLPOP", key, b"0"])
        net.send(socket, b"*1\r\n" + bulk(value))
        command(socket, [b"BLPOP", key, b"0"])
        net.send(socket, b"*2\r\n" + bulk(b"unrequested") + bulk(value))
        command(socket, [b"BRPOP", key, b"0"])
        net.send(socket, b"-WRONGTYPE fixture\r\n")
        command(socket, [b"PING"])
        net.send(socket, b"+PONG\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "wire peer EOF")
    finally:
        net.close_conn(socket)
    done <- True

def wire():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    auto key = bytes([0, 255])
    auto value = bytes([13, 10, 0, 255])
    spawn wire_peer(listener, key, value, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    auto rejected = 0
    list[bytes] empty = []
    try:
        conn.blpop(empty)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.brpop([key], -1.0)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.blpop([key], math.nan)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.blmove(key, b"dest", server_timeout_seconds=math.inf)
    except ValueError:
        rejected = rejected + 1
    try:
        conn.blmove(key, b"dest", source_side="MIDDLE")
    except ValueError:
        rejected = rejected + 1
    check(rejected == 5 and not conn.is_closed(), "local options preserve connection")
    match conn.blpop([b"first", key], 0.25):
        case Optional.Some(item):
            check(item.key == key and item.value == value, "binary pop pair")
        case Optional.None:
            raise ValueError("expected pop result")
    match conn.brpop([key]):
        case Optional.None:
            check(not conn.is_closed(), "server null keeps connection")
        case _:
            raise ValueError("expected null array timeout")
    match conn.blmove(key, b"dest", "right", "left", 0.25):
        case Optional.Some(data):
            check(data == value, "binary moved value")
        case _:
            raise ValueError("expected moved value")
    match conn.blmove(key, b"dest"):
        case Optional.None:
            check(True, "null bulk timeout")
        case _:
            raise ValueError("expected null bulk timeout")
    rejected = 0
    for i in range(0, 2):
        try:
            conn.blpop([key])
        except redis.ProtocolError:
            rejected = rejected + 1
    check(rejected == 2, "shape and returned-key validation")
    rejected = 0
    try:
        conn.brpop([key])
    except redis.ResponseError:
        rejected = 1
    check(rejected == 1 and conn.ping(), "complete error does not leave stale replies")
    conn.disconnect()
    check(<-done, "wire test cleaned")

def stalled(
    bytes listener,
    int64 mode,
    channel[bool] entered,
    channel[bool] done,
    bool signal,
):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    if mode == 0:
        command(socket, [b"BLPOP", b"key", b"0"])
    elif mode == 1:
        command(socket, [b"BRPOP", b"key", b"0"])
    else:
        command(socket, [b"BLMOVE", b"key", b"dest", b"LEFT", b"RIGHT", b"0"])
    if signal:
        entered <- True
    check(len(bytes(net.recv(socket, 1))) == 0, "blocked socket discarded")
    net.close_conn(socket)
    done <- True

def fail_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel blocking consumer")

def owner(redis.Lease lease, int64 mode, Atomic[int64] cancelled):
    try:
        if mode == 0:
            lease.blpop([b"key"])
        elif mode == 1:
            lease.brpop([b"key"])
        else:
            lease.blmove(b"key", b"dest")
    except Cancelled:
        cancelled.add(1)
    finally:
        lease.release()

def cancellation(int64 mode):
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled(listener, mode, entered, done, True)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire(dedicated=True)
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn owner(lease, mode, cancelled)
            spawn fail_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and pool.size() == 0,
        "cancelled consumer joined and released capacity",
    )
    check(<-done, "cancelled blocking socket closed")
    pool.shutdown()

def deadline(int64 mode):
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled(listener, mode, entered, done, False)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=50)
    auto failed = False
    try:
        if mode == 0:
            conn.blpop([b"key"])
        elif mode == 1:
            conn.brpop([b"key"])
        else:
            conn.blmove(b"key", b"dest")
    except redis.CommandTimeout:
        failed = True
    check(failed and conn.is_closed(), "client deadline overrides infinite server wait")
    check(<-done, "timed-out blocking socket closed")

def shared_lease():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    # Reuse the mode-independent peer below, expecting only the final PING.
    spawn shared_peer(listener, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire()
    auto rejected = 0
    try:
        lease.blpop([b"key"])
    except ValueError:
        rejected = rejected + 1
    try:
        lease.execute([b"brpop", b"key", b"0"])
    except ValueError:
        rejected = rejected + 1
    auto batch = redis.pipeline()
    batch.queue([b"BLMOVE", b"key", b"dest", b"LEFT", b"RIGHT", b"0"])
    try:
        lease.execute_pipeline(batch)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 3, "shared lease rejects typed/raw/pipelined blockers before I/O")
    lease.execute([b"PING"])
    lease.discard()
    pool.shutdown()
    check(<-done, "ordinary lease remains usable")

def shared_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"PING"])
    net.send(socket, b"+PONG\r\n")
    check(len(bytes(net.recv(socket, 1))) == 0, "shared socket closed")
    net.close_conn(socket)
    done <- True

def main():
    wire()
    shared_lease()
    for mode in range(0, 3):
        deadline(mode)
        cancellation(mode)
    print("Blocking lists, deadlines, and dedicated ownership: PASS")
