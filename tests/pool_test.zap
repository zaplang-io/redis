import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def command(bytes socket, list[bytes] args):
    auto want = redis.encode_command(args)
    auto got = b""
    while len(got) < len(want):
        auto chunk = bytes(net.recv(socket, len(want) - len(got)))
        check(len(chunk) > 0, "complete pool request")
        got = got + chunk
    check(got == want, "pool command sequence")

def reuse_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        for i in range(0, 2):
            command(socket, [b"PING"])
            net.send(socket, b"+PONG\r\n")
            command(socket, [b"RESET"])
            net.send(socket, b"+RESET\r\n")
        check(len(bytes(net.recv(socket, 1))) == 0, "shutdown closes idle socket")
    finally:
        net.close_conn(socket)
    done <- True

def waiter(redis.Pool pool, channel[bool] entered, Atomic[int64] cancelled):
    try:
        entered <- True
        auto lease = pool.acquire()
        lease.discard()
        raise ValueError("capacity exceeded")
    except Cancelled:
        cancelled.add(1)

def stop_after(channel[bool] entered):
    auto ready = <-entered
    raise ValueError("cancel pool waiter")

def reuse():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn reuse_peer(listener, done)
    auto cfg = redis.options(port=net.port(listener), command_timeout_ms=1000)
    auto pool = redis.pool(cfg, capacity=1, acquire_timeout_ms=50)
    cfg.port = 1
    check(pool.size() == 0, "lazy creation")
    auto first = pool.acquire()
    check(pool.size() == 1, "bounded reservation")
    auto timed_out = False
    try:
        pool.acquire()
    except redis.PoolTimeout:
        timed_out = True
    check(timed_out and pool.size() == 1, "acquisition timeout loses no slot")
    auto entered = channel[bool]()
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn waiter(pool, entered, cancelled)
            spawn stop_after(entered)
    except Exception:
        failed = True
    check(failed and cancelled.load() == 1, "cancelled capacity waiter joined")
    first.execute([b"PING"])
    check(first.release(), "healthy lease reset and reused")
    auto second = pool.acquire()
    auto stale = 0
    try:
        first.execute([b"PING"])
    except redis.LeaseError:
        stale = stale + 1
    try:
        first.release()
    except redis.LeaseError:
        stale = stale + 1
    check(stale == 2, "retained handles and double release rejected")
    second.execute([b"PING"])
    check(second.release(), "second release reuses same socket")
    pool.shutdown()
    pool.shutdown()
    check(pool.size() == 0 and pool.is_shutdown(), "deterministic idle shutdown")
    check(<-done, "idle socket closed")

def shutdown_peer(bytes listener, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"PING"])
    net.send(socket, b"+PONG\r\n")
    check(len(bytes(net.recv(socket, 1))) == 0, "active lease discarded on release")
    net.close_conn(socket)
    done <- True

def closed_waiter(redis.Pool pool, channel[bool] entered, channel[bool] done):
    entered <- True
    auto rejected = False
    try:
        auto lease = pool.acquire()
        lease.discard()
    except redis.PoolClosed:
        rejected = True
    done <- rejected

def active_shutdown():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn shutdown_peer(listener, done)
    auto pool = redis.pool(
        redis.options(port=net.port(listener)),
        capacity=1,
        acquire_timeout_ms=0,
    )
    auto lease = pool.acquire()
    auto entered = channel[bool]()
    auto waiter_done = channel[bool]()
    spawn closed_waiter(pool, entered, waiter_done)
    check(<-entered, "borrower entered")
    pool.shutdown()
    check(<-waiter_done, "shutdown wakes/rejects borrowers")
    lease.execute([b"PING"])
    check(
        not lease.release() and pool.size() == 0,
        "active lease allowed to finish then closed",
    )
    check(<-done, "active socket closed")

def init_peer(bytes listener, channel[bool] done):
    for i in range(0, 2):
        auto socket = net.accept(listener)
        command(socket, [b"AUTH", b"secret"])
        if i == 0:
            net.send(socket, b"-ERR initialization denied\r\n")
        else:
            net.send(socket, b"+OK\r\n")
            command(socket, [b"PING"])
            net.send(socket, b"+PONG\r\n")
        check(
            len(bytes(net.recv(socket, 1))) == 0,
            "failed or dedicated connection closed",
        )
        net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def initialization():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn init_peer(listener, done)
    auto cfg = redis.options(
        port=net.port(listener),
        password="secret",
        connect_timeout_ms=1000,
    )
    auto pool = redis.pool(cfg, capacity=1)
    auto failed = False
    try:
        pool.acquire()
    except redis.ResponseError:
        failed = True
    check(failed and pool.size() == 0, "initialization failure releases reservation")
    auto lease = pool.acquire(dedicated=True)
    lease.execute([b"PING"])
    check(not lease.release() and pool.size() == 0, "dedicated sessions never reused")
    pool.shutdown()
    check(<-done, "initialization sockets cleaned")

def cancel_peer(bytes listener, channel[bool] entered, channel[bool] done):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    command(socket, [b"MULTI"])
    net.send(socket, b"+OK\r\n")
    command(socket, [b"PING"])
    entered <- True
    check(len(bytes(net.recv(socket, 1))) == 0, "cancelled leased transaction EOF")
    net.close_conn(socket)
    done <- True

def leased_owner(redis.Pool pool, Atomic[int64] cancelled):
    auto lease = pool.acquire()
    try:
        auto tx = lease.transaction()
        try:
            tx.multi()
            tx.queue([b"PING"])
        finally:
            tx.discard()
    except Cancelled:
        cancelled.add(1)
    finally:
        lease.release()

def leased_cancellation():
    auto listener = net.listen("127.0.0.1", 0)
    auto entered = channel[bool]()
    auto done = channel[bool]()
    spawn cancel_peer(listener, entered, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    Atomic[int64] cancelled = Atomic(0)
    auto failed = False
    try:
        scope:
            spawn leased_owner(pool, cancelled)
            spawn stop_after(entered)
    except Exception:
        failed = True
    check(
        failed and cancelled.load() == 1 and pool.size() == 0,
        "cancelled leased transaction returns slot after cleanup",
    )
    check(<-done, "cancelled lease no live socket")
    pool.shutdown()

def reset_peer(bytes listener, channel[bool] done):
    for i in range(0, 2):
        auto socket = net.accept(listener)
        command(socket, [b"PING"])
        net.send(socket, b"+PONG\r\n")
        if i == 0:
            command(socket, [b"RESET"])
            net.send(socket, b"-NOPERM reset denied\r\n")
        check(
            len(bytes(net.recv(socket, 1))) == 0,
            "reset failure or discard closes socket",
        )
        net.close_conn(socket)
    net.close_listener(listener)
    done <- True

def reset_failure():
    auto listener = net.listen("127.0.0.1", 0)
    auto done = channel[bool]()
    spawn reset_peer(listener, done)
    auto pool = redis.pool(redis.options(port=net.port(listener)), capacity=1)
    auto lease = pool.acquire()
    lease.execute([b"PING"])
    check(
        not lease.release() and pool.size() == 0,
        "reset rejection discards connection",
    )
    lease = pool.acquire()
    lease.execute([b"PING"])
    lease.discard()
    check(pool.size() == 0, "replacement reservation released")
    pool.shutdown()
    check(<-done, "reset failure cleaned")

def main():
    auto rejected = 0
    try:
        redis.pool(redis.options(), capacity=0)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.pool(redis.options(), acquire_timeout_ms=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.pool(redis.options(), reset_timeout_ms=0)
    except ValueError:
        rejected = rejected + 1
    check(rejected == 3, "pool limits validated before I/O")
    reuse()
    active_shutdown()
    initialization()
    leased_cancellation()
    reset_failure()
    print("Bounded pools and guarded leases: PASS")
