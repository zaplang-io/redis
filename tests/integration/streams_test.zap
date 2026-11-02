import redis
import integration_fixtures

def wait_blocked(redis.Connection observer, int64 id):
    auto deadline = time.monotonic_ns() + 3000000000
    while time.monotonic_ns() < deadline:
        match observer.execute([b"CLIENT", b"LIST", b"ID", bytes(str(id))]):
            case redis.Reply.Bulk(data):
                if str(data) =~ m/(?:\A| )flags=[^ ]*b/:
                    return
            case _:
                raise ValueError("expected CLIENT LIST text")
        time.sleep(0.001)
    raise ValueError("XREAD did not block")

def producer(redis.Options cfg, int64 id, bytes key):
    auto conn = redis.connect_options(cfg)
    try:
        wait_blocked(conn, id)
        conn.xadd(key, [redis.StreamField(b"event", bytes([0, 255]))])
        conn.execute([b"PEXPIRE", key, b"60000"])
    finally:
        conn.disconnect()

def commands(integration_fixtures.Fixture f):
    auto conn = f.open()
    auto key = f.binary_key(bytes([0, 255]))
    auto other = f.key("other-stream")
    auto value = bytes([0, 255, 13, 10])
    try:
        auto id = conn.xadd(
            key,
            [
                redis.StreamField(b"field", value),
                redis.StreamField(b"field", b"second"),
            ],
            b"1-0",
        )
        conn.execute([b"PEXPIRE", key, b"60000"])
        integration_fixtures.check(id == b"1-0", "explicit ID")
        conn.xadd(key, [redis.StreamField(b"empty", b"")], b"1-1")
        auto generated = conn.xadd(key, [redis.StreamField(b"generated", b"yes")])
        integration_fixtures.check(
            generated != b"*" and conn.xlen(key) == 3,
            "generated ID and length",
        )
        auto entries = conn.xrange(key, b"1-0", b"1-1")
        integration_fixtures.check(
            len(entries) == 2 and len(entries[0].fields) == 2,
            "range boundaries and field count",
        )
        integration_fixtures.check(
            entries[0].fields[0].field == b"field" and entries[0].fields[1].field == b"field",
            "repeated fields preserved",
        )
        integration_fixtures.check(
            entries[0].fields[0].value == value and entries[0].fields[1].value == b"second",
            "binary field order",
        )
        integration_fixtures.check(
            len(conn.xrange(key, b"(1-0", b"1-1")) == 1,
            "exclusive boundary",
        )
        integration_fixtures.check(len(conn.xrange(key, count=1)) == 1, "range COUNT")
        auto reverse = conn.xrevrange(key, count=1)
        integration_fixtures.check(
            len(reverse) == 1 and reverse[0].id == generated,
            "reverse ordering",
        )
        auto large = b"9223372036854775808-9223372036854775809"
        integration_fixtures.check(
            conn.xadd(other, [redis.StreamField(bytes([255]), value)], large) == large,
            "IDs above signed int64 remain exact",
        )
        conn.execute([b"PEXPIRE", other, b"60000"])
        auto batches = conn.xread(
            [redis.StreamCursor(key, b"0"), redis.StreamCursor(other, b"0")],
            count=2,
        )
        integration_fixtures.check(
            len(batches) == 2 and batches[0].key == key and batches[1].key == other,
            "multiple streams in request order",
        )
        integration_fixtures.check(
            len(batches[0].entries) == 2 and batches[1].entries[0].id == large,
            "per-stream COUNT and exact large ID",
        )
        integration_fixtures.check(
            conn.xdel(key, [b"1-0", b"1-1"]) == 2 and conn.xlen(key) == 1,
            "delete explicit IDs",
        )
        integration_fixtures.check(
            len(conn.xread([redis.StreamCursor(key, generated)])) == 0,
            "read after last ID is empty",
        )
        integration_fixtures.check(
            len(conn.xrange(f.key("missing"))) == 0,
            "missing range empty",
        )
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto failed = False
        try:
            conn.xlen(wrong)
        except redis.ResponseError:
            failed = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
        integration_fixtures.check(
            failed and conn.ping(),
            "stream WRONGTYPE preserves connection",
        )
    finally:
        conn.disconnect()

def blocking(integration_fixtures.Fixture f):
    auto conn = f.open()
    auto key = f.key("blocking-stream")
    auto id = integration_fixtures.integer(conn.execute([b"CLIENT", b"ID"]))
    try:
        scope:
            spawn producer(f.config, id, key)
            auto batches = conn.xread([redis.StreamCursor(key, b"$")], block_ms=1000)
            integration_fixtures.check(
                len(batches) == 1 and len(batches[0].entries) == 1,
                "blocking wakeup",
            )
            integration_fixtures.check(
                batches[0].entries[0].fields[0].value == bytes([0, 255]),
                "blocking binary payload",
            )
        integration_fixtures.check(
            len(conn.xread([redis.StreamCursor(key, b"$")], block_ms=10)) == 0 and conn.ping(),
            "server BLOCK timeout preserves connection",
        )
    finally:
        conn.disconnect()
    f.config.command_timeout_ms = 50
    auto timed = f.open()
    auto failed = False
    try:
        timed.xread([redis.StreamCursor(key, b"$")], block_ms=0)
    except redis.CommandTimeout:
        failed = True
    finally:
        timed.disconnect()
    integration_fixtures.check(
        failed and timed.is_closed(),
        "client timeout discards blocking XREAD",
    )
    f.config.command_timeout_ms = 2000

def main():
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 2000
    try:
        commands(f)
        blocking(f)
    finally:
        f.config.command_timeout_ms = 2000
        f.cleanup()
    print("Live Streams IDs, ordered fields, ranges, and XREAD: PASS")
