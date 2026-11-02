import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete transaction request")
        got = got + chunk
    check(got == want, "transaction command order")

def peer(bytes listener, bytes final_reply, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        command(socket, [b"WATCH", b"key"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"GET", b"key"])
        net.send(socket, b"$1\r\nx\r\n")
        command(socket, [b"UNWATCH"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"MULTI"])
        net.send(socket, b"+OK\r\n")
        command(socket, [b"INCR", b"key"])
        net.send(socket, b"+QUEUED\r\n")
        command(socket, [b"EXEC"])
        net.send(socket, final_reply)
        check(len(bytes(net.recv(socket, 1))) == 0, "finished socket closed")
    finally:
        net.close_conn(socket)
    done <- True

def outcome(bytes final_reply, bool valid):
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn peer(listener, final_reply, done)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    auto tx = conn.transaction()
    auto busy = 0
    try:
        conn.ping()
    except redis.BusyError:
        busy = busy + 1
    try:
        conn.disconnect()
    except redis.BusyError:
        busy = busy + 1
    try:
        conn.transaction()
    except redis.BusyError:
        busy = busy + 1
    check(busy == 3, "transaction exclusively owns connection")
    tx.watch([b"key"])
    tx.execute([b"GET", b"key"])
    tx.unwatch()
    tx.multi()
    tx.queue([b"INCR", b"key"])
    auto rejected = 0
    try:
        tx.execute([b"GET", b"key"])
    except ValueError:
        rejected = rejected + 1
    try:
        tx.watch([b"key"])
    except ValueError:
        rejected = rejected + 1
    check(rejected == 2, "invalid stage changes no wire state")
    auto failed = False
    try:
        auto result = tx.exec()
        match result:
            case redis.TransactionResult.Committed(replies):
                check(len(replies) == 1, "one committed reply")
            case redis.TransactionResult.Conflict:
                check(final_reply == b"*-1\r\n", "null EXEC is a conflict")
            case redis.TransactionResult.Aborted(data):
                check(redis.error_prefix(data) == b"EXECABORT", "explicit abort")
    except redis.ProtocolError:
        failed = True
    check(failed == (not valid), "malformed EXEC discarded")
    tx.discard()
    if valid:
        check(not conn.is_closed(), "completed transaction releases connection")
    else:
        check(conn.is_closed(), "bad EXEC closes connection")
    auto finished = False
    try:
        tx.multi()
    except ValueError:
        finished = True
    check(finished, "finished session cannot affect reused connection")
    conn.disconnect()
    check(<-done, "transaction peer closed")

def stalled(bytes listener, channel[bool] entered, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"MULTI"])
    net.send(socket, b"+OK\r\n")
    command(socket, [b"GET", b"key"])
    entered <- True
    check(len(bytes(net.recv(socket, 1))) == 0, "cancelled transaction EOF")
    net.close_conn(socket)
    done <- True

def stop_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel transaction")

def cancelled_owner(redis.Connection conn, Atomic[int64] cancelled):
    auto tx = conn.transaction()
    try:
        tx.multi()
        tx.queue([b"GET", b"key"])
    except Cancelled:
        check(conn.is_closed(), "cancelled transaction discarded")
        cancelled.add(1)
    finally:
        tx.discard()

def cancellation():
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn stalled(listener, entered, done)
    auto conn = redis.connect(port=net.port(listener))
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn cancelled_owner(conn, cancelled)
            spawn stop_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and conn.is_closed(),
        "scope joins transaction cleanup",
    )
    check(<-done, "cancelled transaction no socket leak")

def main():
    outcome(b"*1\r\n:1\r\n", True)
    outcome(b"*1\r\n-ERR runtime\r\n", True)
    outcome(b"*-1\r\n", True)
    outcome(b"-EXECABORT dirty\r\n", True)
    outcome(b"*0\r\n", False)
    outcome(b"+WRONG\r\n", False)
    cancellation()
    print("Exclusive transaction sessions and cleanup: PASS")
