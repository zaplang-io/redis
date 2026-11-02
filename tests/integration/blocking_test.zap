import redis
import integration_fixtures

def wait_blocked(redis.Connection observer, int64 id):
    auto deadline = time.monotonic_ns() + 3000000000
    while time.monotonic_ns() < deadline:
        auto reply = observer.execute([b"CLIENT", b"LIST", b"ID", bytes(str(id))])
        match reply:
            case redis.Reply.Bulk(data):
                if str(data) =~ m/(?:\A| )flags=[^ ]*b/:
                    return
            case _:
                raise ValueError("expected CLIENT LIST text")
        time.sleep(0.001)
    raise ValueError("consumer did not block on Redis")

def producer(redis.Options cfg, int64 id, bytes key, bytes value):
    auto conn = redis.connect_options(cfg)
    try:
        wait_blocked(conn, id)
        conn.rpush(key, [value])
        conn.execute([b"PEXPIRE", key, b"60000"])
    finally:
        conn.disconnect()

def pop_pair(Optional[redis.KeyValue] result, bytes key, bytes value):
    match result:
        case Optional.Some(item):
            integration_fixtures.check(
                item.key == key and item.value == value,
                "binary key/value wakeup",
            )
        case Optional.None:
            raise ValueError("expected producer wakeup")

def optional_value(Optional[bytes] result, bytes value):
    match result:
        case Optional.Some(data):
            integration_fixtures.check(data == value, "binary moved value")
        case Optional.None:
            raise ValueError("expected moved value")

def wakeups(integration_fixtures.Fixture f):
    auto conn = f.open()
    auto id = integration_fixtures.integer(conn.execute([b"CLIENT", b"ID"]))
    auto value = bytes([0, 255, 13, 10])
    try:
        for mode in range(0, 3):
            auto key = f.binary_key(bytes([0, 255]) + bytes(str(mode)))
            auto destination = f.key("moved-" + str(mode))
            scope:
                spawn producer(f.config, id, key, value)
                if mode == 0:
                    pop_pair(conn.blpop([f.key("empty"), key], 1.0), key, value)
                elif mode == 1:
                    pop_pair(conn.brpop([key], 1.0), key, value)
                else:
                    optional_value(
                        conn.blmove(key, destination, "RIGHT", "LEFT", 1.0),
                        value,
                    )
            if mode == 2:
                conn.execute([b"PEXPIRE", destination, b"60000"])
                optional_value(conn.lpop(destination), value)
        auto empty = f.key("server-timeout")
        match conn.blpop([empty], 0.01):
            case Optional.None:
                integration_fixtures.check(
                    conn.ping(),
                    "server BLPOP timeout preserves connection",
                )
            case _:
                raise ValueError("expected server BLPOP timeout")
        match conn.brpop([empty], 0.01):
            case Optional.None:
                integration_fixtures.check(
                    conn.ping(),
                    "server BRPOP timeout preserves connection",
                )
            case _:
                raise ValueError("expected server BRPOP timeout")
        match conn.blmove(empty, f.key("empty-destination"), server_timeout_seconds=0.01):
            case Optional.None:
                integration_fixtures.check(
                    conn.ping(),
                    "server BLMOVE timeout preserves connection",
                )
            case _:
                raise ValueError("expected server BLMOVE timeout")
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto failed = False
        try:
            conn.blpop([wrong], 0.01)
        except redis.ResponseError:
            failed = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
        integration_fixtures.check(
            failed and conn.ping(),
            "blocking command server error is reusable",
        )
    finally:
        conn.disconnect()

def deadlines(integration_fixtures.Fixture f):
    f.config.command_timeout_ms = 50
    for mode in range(0, 3):
        auto conn = f.open()
        auto failed = False
        auto key = f.key("client-timeout-" + str(mode))
        try:
            if mode == 0:
                conn.blpop([key])
            elif mode == 1:
                conn.brpop([key])
            else:
                conn.blmove(key, f.key("client-destination"))
        except redis.CommandTimeout:
            failed = True
        finally:
            conn.disconnect()
        integration_fixtures.check(
            failed and conn.is_closed(),
            "client timeout discards infinite server wait",
        )
    f.config.command_timeout_ms = 2000

def main():
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 2000
    try:
        wakeups(f)
        deadlines(f)
    finally:
        f.config.command_timeout_ms = 2000
        f.cleanup()
    print("Live blocking wakeups and separate server/client timeouts: PASS")
