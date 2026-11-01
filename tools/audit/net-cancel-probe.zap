# Diagnostic for redis#7: does scope cancellation wake a stalled TCP read?
# No Redis server required. The peer eventually releases the read so even a
# runtime without I/O cancellation terminates; never use this delay as a fix.
def peer(bytes listener, channel[int64] entered, Atomic[int64] released):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        auto request = net.recv(socket, 1)
        entered <- 1
        time.sleep(1)
        released.store(1)
        try:
            net.send(socket, b"x")
        except Exception:
            released.store(1)
    finally:
        net.close_conn(socket)

def reader(
    bytes socket,
    Atomic[int64] completed,
    Atomic[int64] cancelled,
    Atomic[int64] cleaned,
):
    try:
        auto reply = net.recv(socket, 1)
        completed.store(1)
    except Cancelled:
        cancelled.store(1)
    finally:
        net.close_conn(socket)
        cleaned.store(1)

def main():
    auto listener = net.listen("127.0.0.1", 0)
    auto port = net.port(listener)
    auto entered = channel[int64]()
    Atomic[int64] released = Atomic(0)
    Atomic[int64] completed = Atomic(0)
    Atomic[int64] cancelled = Atomic(0)
    Atomic[int64] cleaned = Atomic(0)
    spawn peer(listener, entered, released)
    auto socket = net.connect("127.0.0.1", port)
    net.send(socket, b"x")
    auto started = <-entered
    auto timed_out = False
    auto start = time.monotonic_ns()
    try:
        scope(timeout=100):
            spawn reader(socket, completed, cancelled, cleaned)
    except ScopeTimeout:
        timed_out = True
    finally:
        net.close_conn(socket)
    auto elapsed = (time.monotonic_ns() - start) // 1000000
    print("requested_deadline_ms=100")
    print("scope_elapsed_ms=" + str(elapsed))
    print("scope_timed_out=" + str(timed_out))
    print("peer_released=" + str(released.load()))
    print("reader_completed=" + str(completed.load()))
    print("reader_cancelled=" + str(cancelled.load()))
    print("reader_cleaned=" + str(cleaned.load()))
    if not timed_out:
        raise ValueError("probe did not observe a scope deadline")
    if cleaned.load() != 1:
        raise ValueError("reader failed to clean up")
    if released.load() == 1:
        print("TCP_CANCEL=blocked-until-peer-release")
    elif cancelled.load() == 1 and completed.load() == 0:
        print("TCP_CANCEL=interruptible")
    else:
        raise ValueError("unexpected cancellation outcome")
