import redis
import integration_fixtures

def queued(redis.Reply reply):
    integration_fixtures.simple(reply, b"QUEUED")

def committed(redis.TransactionResult result) -> list[redis.Reply]:
    match result:
        case redis.TransactionResult.Committed(replies):
            return replies
        case _:
            raise ValueError("expected committed transaction")

def transactions(integration_fixtures.Fixture f):
    auto conn = f.open()
    auto other = f.open()
    auto key = f.key("transaction")
    try:
        f.put(conn, key, b"before")
        auto tx = conn.transaction()
        try:
            tx.watch([key])
            integration_fixtures.bulk(tx.execute([b"GET", key]), b"before")
            tx.multi()
            queued(tx.queue([b"SET", key, b"after", b"PX", b"60000"]))
            queued(tx.queue([b"GET", key]))
            auto replies = committed(tx.exec())
            integration_fixtures.check(len(replies) == 2, "atomic result count")
            integration_fixtures.simple(replies[0], b"OK")
            integration_fixtures.bulk(replies[1], b"after")
        finally:
            tx.discard()
        tx = conn.transaction()
        try:
            tx.multi()
            queued(tx.queue([b"LPUSH", key, b"wrong-type"]))
            queued(tx.queue([b"GET", key]))
            auto replies = committed(tx.exec())
            match replies[0]:
                case redis.Reply.Error(data):
                    integration_fixtures.check(
                        redis.error_prefix(data) == b"WRONGTYPE",
                        "runtime error retained inside EXEC",
                    )
                case _:
                    raise ValueError("expected runtime error")
            integration_fixtures.bulk(replies[1], b"after")
        finally:
            tx.discard()
        tx = conn.transaction()
        try:
            tx.multi()
            auto reply = tx.queue([b"INCR"])
            match reply:
                case redis.Reply.Error(data):
                    integration_fixtures.check(
                        redis.error_prefix(data) == b"ERR",
                        "queue-time error retained",
                    )
                case _:
                    raise ValueError("expected queue error")
            match tx.exec():
                case redis.TransactionResult.Aborted(data):
                    integration_fixtures.check(
                        redis.error_prefix(data) == b"EXECABORT",
                        "dirty transaction abort distinct",
                    )
                case _:
                    raise ValueError("expected EXECABORT")
        finally:
            tx.discard()
        tx = conn.transaction()
        try:
            tx.watch([key])
            f.put(other, key, b"external")
            tx.multi()
            queued(tx.queue([b"SET", key, b"must-not-run", b"PX", b"60000"]))
            match tx.exec():
                case redis.TransactionResult.Conflict:
                    integration_fixtures.bulk(conn.execute([b"GET", key]), b"external")
                case _:
                    raise ValueError("expected WATCH conflict")
        finally:
            tx.discard()
        tx = conn.transaction()
        tx.watch([key])
        tx.unwatch()
        tx.multi()
        queued(tx.queue([b"SET", key, b"discarded"]))
        tx.discard()
        tx.discard()
        integration_fixtures.bulk(conn.execute([b"GET", key]), b"external")
        integration_fixtures.check(
            conn.ping(),
            "connection clean after all transaction outcomes",
        )
    finally:
        conn.disconnect()
        other.disconnect()

struct Borrowers:
    bytes mutex
    dict[str, bool] active
    dict[str, bool] seen

    def work(self, redis.Pool pool):
        auto lease = pool.acquire()
        auto id = str(integration_fixtures.integer(lease.execute([b"CLIENT", b"ID"])))
        sync.lock(self.mutex)
        if id in self.active:
            integration_fixtures.check(
                not self.active[id],
                "connection never leased twice",
            )
        self.active[id] = True
        self.seen[id] = True
        sync.unlock(self.mutex)
        try:
            integration_fixtures.simple(lease.execute([b"PING"]), b"PONG")
        finally:
            sync.lock(self.mutex)
            self.active[id] = False
            sync.unlock(self.mutex)
            lease.release()

def borrower(Borrowers borrowers, redis.Pool pool):
    borrowers.work(pool)

def pooling(integration_fixtures.Fixture f):
    auto cfg = f.config
    cfg.client_name = "pool-worker"
    auto pool = redis.pool(cfg, capacity=2, acquire_timeout_ms=5000)
    auto key = f.key("pool-reset")
    auto conn = f.open()
    f.put(conn, key, b"original")
    conn.disconnect()
    try:
        auto lease = pool.acquire()
        auto id = integration_fixtures.integer(lease.execute([b"CLIENT", b"ID"]))
        lease.execute([b"CLIENT", b"SETNAME", b"dirty"])
        lease.execute([b"WATCH", key])
        lease.execute([b"MULTI"])
        queued(lease.execute([b"SET", key, b"leaked"]))
        integration_fixtures.check(lease.release(), "RESET discarded MULTI/WATCH")
        lease = pool.acquire()
        integration_fixtures.check(
            integration_fixtures.integer(lease.execute([b"CLIENT", b"ID"])) == id,
            "same socket reused",
        )
        integration_fixtures.bulk(
            lease.execute([b"CLIENT", b"GETNAME"]),
            b"pool-worker",
        )
        integration_fixtures.bulk(lease.execute([b"GET", key]), b"original")
        lease.execute([b"SELECT", b"1"])
        lease.release()
        lease = pool.acquire()
        integration_fixtures.bulk(lease.execute([b"GET", key]), b"original")
        auto tx = lease.transaction()
        auto busy = False
        try:
            lease.release()
        except redis.BusyError:
            busy = True
        integration_fixtures.check(busy, "active transaction prevents pool return")
        try:
            tx.multi()
            queued(tx.queue([b"GET", key]))
            integration_fixtures.bulk(committed(tx.exec())[0], b"original")
        finally:
            tx.discard()
        auto batch = redis.pipeline()
        batch.queue([b"PING"])
        integration_fixtures.simple(lease.execute_pipeline(batch)[0], b"PONG")
        lease.release()
        auto dedicated = pool.acquire(dedicated=True)
        auto old_id = integration_fixtures.integer(
            dedicated.execute([b"CLIENT", b"ID"]),
        )
        dedicated.execute([b"WATCH", key])
        integration_fixtures.check(
            not dedicated.release(),
            "dedicated WATCH session discarded",
        )
        auto fresh = pool.acquire()
        integration_fixtures.check(
            integration_fixtures.integer(fresh.execute([b"CLIENT", b"ID"])) != old_id,
            "dedicated state not reused",
        )
        fresh.execute([b"QUIT"])
        auto failed = False
        try:
            fresh.execute([b"PING"])
        except redis.ConnectionError:
            failed = True
        integration_fixtures.check(
            failed and not fresh.release(),
            "broken connection dropped",
        )
        dict[str, bool] active = {}
        dict[str, bool] seen = {}
        auto borrowers = Borrowers(sync.mutex(), active, seen)
        scope:
            for i in range(0, 16):
                spawn borrower(borrowers, pool)
        integration_fixtures.check(
            len(borrowers.seen) <= 2 and pool.size() <= 2,
            "concurrent capacity bound",
        )
    finally:
        pool.shutdown()
    integration_fixtures.check(pool.size() == 0, "shutdown closes idle connections")

def main():
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 2000
    try:
        io.write(2, "transaction/pool: transaction outcomes\n")
        transactions(f)
        for round in range(0, 4):
            auto stats = gc.stats()
            io.write(
                2,
                "transaction/pool: borrower round " + str(round) + " gc=" + str(stats.gc_count) + "\n",
            )
            pooling(f)
        io.write(2, "transaction/pool: rounds complete\n")
    except Exception as message:
        # Preserve the primary failure before cleanup, which previously hid an
        # intermittent failure behind a native crash in this fixture's finally.
        io.write(2, "transaction/pool: failure before cleanup: " + message + "\n")
        raise ValueError("transaction/pool regression: " + message)
    finally:
        io.write(2, "transaction/pool: fixture cleanup\n")
        f.cleanup()
    print("Live transactions, WATCH, and bounded pooling: PASS")
