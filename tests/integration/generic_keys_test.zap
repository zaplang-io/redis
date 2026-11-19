import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def key_moves(redis.Connection conn, integration_fixtures.Fixture f):
    auto source = f.binary_key(bytes([0, 255, 13, 10]))
    auto dest = f.key("rename-destination")
    auto copied = f.key("copy")
    auto missing = f.key("missing")
    auto data = bytes([255, 0, 128, 13, 10])
    f.put(conn, source, data)
    f.put(conn, dest, b"existing")
    auto expires = conn.pexpiretime(source)
    check(not conn.renamenx(source, dest), "RENAMENX existing destination")
    check(conn.get(dest).unwrap() == b"existing", "RENAMENX preserves destination")
    check(conn.rename(source, dest), "RENAME replaces destination")
    check(
        conn.get(source).is_none() and conn.get(dest).unwrap() == data,
        "RENAME moves binary value",
    )
    check(conn.pexpiretime(dest) == expires, "RENAME preserves source expiry")
    check(conn.renamenx(dest, source), "RENAMENX missing destination")
    auto missing_error = False
    try:
        conn.rename(missing, dest)
    except redis.ResponseError:
        missing_error = redis.error_prefix(conn.last_error()) == b"ERR"
    check(
        missing_error and conn.ping(),
        "RENAME missing source leaves connection usable",
    )

    check(conn.copy(source, copied), "COPY missing destination")
    check(conn.get(copied).unwrap() == data, "COPY preserves binary value")
    check(conn.pexpiretime(copied) == expires, "COPY preserves source expiry")
    check(not conn.copy(source, copied), "COPY existing destination")
    f.put(conn, copied, b"old")
    check(conn.copy(source, copied, replace=True), "COPY REPLACE")
    check(not conn.copy(missing, copied, replace=True), "COPY missing source")
    check(conn.get(copied).unwrap() == data, "missing source preserves destination")
    check(
        conn.touch([source, missing, source, copied]) == 3,
        "TOUCH counts duplicate existing keys",
    )
    check(conn.touch([missing]) == 0, "TOUCH absent key")

def serialized_values(redis.Connection conn, integration_fixtures.Fixture f):
    auto source = f.key("dump-source")
    auto restored = f.binary_key(bytes([2, 255, 13, 10]))
    auto missing = f.key("dump-missing")
    auto data = bytes([255, 0, 128, 13, 10])
    f.put(conn, source, data)
    auto serialized = conn.dump(source).unwrap()
    check(conn.dump(missing).is_none(), "DUMP missing key")
    check(conn.restore(restored, 60000, serialized), "RESTORE binary dump")
    check(conn.get(restored).unwrap() == data, "binary DUMP/RESTORE round trip")

    auto busy = False
    try:
        conn.restore(restored, 0, serialized)
    except redis.ResponseError:
        busy = redis.error_prefix(conn.last_error()) == b"BUSYKEY"
    check(
        busy and conn.get(restored).unwrap() == data,
        "RESTORE collision preserves destination",
    )
    auto corrupt = False
    try:
        conn.restore(missing, 0, b"bad dump")
    except redis.ResponseError:
        corrupt = redis.error_prefix(conn.last_error()) == b"ERR"
    check(
        corrupt and conn.get(missing).is_none(),
        "RESTORE corrupt payload creates no key",
    )

    f.put(conn, restored, b"old")
    auto deadline = conn.pexpiretime(source)
    check(
        conn.restore(restored, deadline, serialized, replace=True, absttl=True),
        "RESTORE REPLACE with ABSTTL",
    )
    check(
        conn.pexpiretime(restored) == deadline,
        "RESTORE preserves exact absolute milliseconds",
    )
    check(conn.get(restored).unwrap() == data, "RESTORE REPLACE replaces data")

def expiration_and_metadata(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("expiry")
    auto absent = f.key("missing-expiry")
    f.put(conn, key, b"data")
    match conn.object("encoding", key):
        case redis.ObjectInfo.Encoding(name):
            check(len(name) > 0, "OBJECT encoding")
        case _:
            raise ValueError("expected OBJECT encoding")
    match conn.object("REFCOUNT", key):
        case redis.ObjectInfo.Refcount(count):
            check(count >= 1, "OBJECT refcount")
        case _:
            raise ValueError("expected OBJECT refcount")
    match conn.object("IDLETIME", absent):
        case redis.ObjectInfo.Missing:
            check(True, "OBJECT missing")
        case _:
            raise ValueError("expected missing OBJECT metadata")

    auto seconds = conn.expiretime(key)
    auto millis = seconds * 1000 + 123
    check(
        conn.expireat(key, seconds) and conn.expiretime(key) == seconds,
        "EXPIREAT uses Unix seconds",
    )
    check(
        conn.pexpireat(key, millis) and conn.pexpiretime(key) == millis,
        "PEXPIREAT uses Unix milliseconds",
    )
    check(not conn.pexpireat(key, millis + 876, nx=True), "NX rejects existing expiry")
    check(not conn.pexpireat(key, millis, gt=True), "GT rejects equal expiry")
    check(not conn.pexpireat(key, millis + 1, lt=True), "LT rejects greater expiry")
    check(conn.pexpiretime(key) == millis, "skipped conditions preserve expiry")
    check(
        conn.pexpireat(key, millis + 2, xx=True, gt=True),
        "XX and GT can be combined",
    )
    check(conn.pexpireat(key, millis, lt=True), "LT shortens expiry")
    check(
        conn.persist(key) and conn.expiretime(key) == -1,
        "persistent expiry sentinel",
    )
    check(not conn.expireat(key, seconds, xx=True), "XX requires existing expiry")
    check(conn.expireat(key, seconds, nx=True), "NX sets first expiry")
    check(conn.pexpireat(key, 1), "past absolute expiry deletes key")
    check(
        conn.expiretime(key) == -2 and conn.pexpiretime(key) == -2,
        "expired key sentinel",
    )
    check(
        not conn.expireat(absent, seconds) and not conn.pexpireat(absent, millis),
        "expiry of missing key",
    )

def isolated_operations(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([1, 255, 13, 10]))
    auto data = bytes([128, 0, 255])
    f.put(conn, key, data)
    auto scoped = conn.keys(bytes(f.prefix) + b"*")
    check(
        len(scoped) == 1 and conn.get(scoped[0]).unwrap() == data,
        "KEYS returns binary fixture key",
    )
    check(len(conn.keys(bytes(f.prefix) + b"no-such-key")) == 0, "KEYS empty match")
    check(
        conn.exists([conn.randomkey().unwrap()]) == 1,
        "RANDOMKEY returns a present key",
    )

    auto snapshot = conn.dump(key).unwrap()
    auto restored = f.key("restore-idle")
    check(conn.restore(restored, 60000, snapshot, idletime=5), "RESTORE IDLETIME")
    match conn.object("idletime", restored):
        case redis.ObjectInfo.IdleTime(seconds):
            check(seconds >= 5, "OBJECT reports restored idle seconds")
        case _:
            raise ValueError("expected OBJECT idle time")

    auto other = integration_fixtures.other_database(f)
    auto copied = other.key("other-db")
    try:
        check(
            conn.copy(key, copied, destination_db=other.config.database),
            "COPY explicit destination database",
        )
        check(
            conn.get(copied).is_none(),
            "COPY leaves source database destination absent",
        )
        auto second = other.open()
        try:
            check(
                second.get(copied).unwrap() == data,
                "COPY wrote binary data into target database",
            )
        finally:
            second.disconnect()
    finally:
        other.cleanup()

def run(bool resp3):
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    if resp3:
        conn.disconnect()
        conn = redis.connect_resp3_options(f.config)
    try:
        f.verify_version(conn)
        if os.getenv("REDIS_TEST_ISOLATED") == "1":
            isolated_operations(conn, f)
        key_moves(conn, f)
        serialized_values(conn, f)
        expiration_and_metadata(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()

def main():
    run(False)
    run(True)
    print("Live Redis generic key command coverage: PASS")
