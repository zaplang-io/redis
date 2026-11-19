import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def setter_options(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([255, 0, 13, 10]))
    auto missing = f.key("set-missing")
    auto data = bytes([0, 255, 128, 13, 10])
    check(
        conn.set_value(key, data, nx=True, ex=60).is_written(),
        "SET NX with EX inserts binary value",
    )
    auto expiry = conn.pexpiretime(key)
    auto seconds = conn.expiretime(key)
    auto millis = seconds * 1000 + 123
    check(expiry > 0 and conn.pttl(key) <= 60000, "SET EX installs a seconds expiry")
    check(
        conn.set_value(key, b"ignored", nx=True, pxat=millis).is_null(),
        "SET NX skips existing key",
    )
    check(
        conn.set_value(missing, data, xx=True, exat=seconds).is_null(),
        "SET XX skips absent key",
    )
    check(conn.get(missing).is_none(), "SET XX skip creates no key")
    check(
        conn.get(key).unwrap() == data and conn.pexpiretime(key) == expiry,
        "SET NX skip preserves bytes and TTL",
    )

    check(
        conn.set_value(key, b"", xx=True, keepttl=True).is_written(),
        "SET XX KEEPTTL updates to empty",
    )
    check(
        conn.get(key).unwrap() == b"" and conn.pexpiretime(key) == expiry,
        "KEEPTTL preserves exact expiry",
    )
    auto invalid = False
    try:
        conn.set_value(key, data, px=60000, keepttl=True)
    except ValueError:
        invalid = True
    check(
        invalid and conn.get(key).unwrap() == b"" and conn.pexpiretime(key) == expiry,
        "conflicting SET expiry leaves key unchanged",
    )
    check(
        conn.set_value(key, data, exat=seconds).is_written() and conn.expiretime(key) == seconds,
        "SET EXAT uses Unix seconds",
    )
    check(
        conn.set_value(key, data, pxat=millis).is_written() and conn.pexpiretime(key) == millis,
        "SET PXAT uses Unix milliseconds",
    )
    check(
        conn.set_value(key, data).is_written() and conn.pttl(key) == -1,
        "plain SET clears expiry",
    )
    check(
        conn.set_value(key, data, px=60000).is_written() and conn.pttl(key) > 0,
        "SET PX installs millisecond duration",
    )
    check(
        conn.set_value(missing, data, nx=True, keepttl=True).is_written(),
        "SET KEEPTTL on absent key",
    )
    check(conn.pttl(missing) == -1, "absent key has no expiry to keep")
    conn.pexpire(missing, 60000)
    check(
        conn.set_value(key, data, pxat=1).is_written() and conn.get(key).is_none(),
        "past SET PXAT leaves key absent",
    )

def check_previous(redis.SetResult result, bytes expected, str label):
    match result:
        case redis.SetResult.Previous(value):
            check(value == expected, label)
        case _:
            raise ValueError("expected previous SET value: " + label)

def setter_get(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([255, 1, 13, 10]))
    auto absent = f.key("set-get-absent")
    auto previous = bytes([255, 128, 0, 13, 10])
    auto updated = bytes([0, 10, 255])
    check(
        conn.set_value(absent, updated, xx=True, get=True).is_null(),
        "SET XX GET absent",
    )
    check(conn.get(absent).is_none(), "SET XX GET skip creates no key")
    check(
        conn.set_value(key, previous, px=60000, nx=True, get=True).is_null(),
        "SET NX GET inserts missing key",
    )
    check(conn.get(key).unwrap() == previous, "SET NX GET inserted binary value")
    auto deadline = conn.pexpiretime(key)
    check_previous(
        conn.set_value(key, updated, ex=60, nx=True, get=True),
        previous,
        "SET NX GET returns old value even when write is skipped",
    )
    check(
        conn.get(key).unwrap() == previous and conn.pexpiretime(key) == deadline,
        "SET NX GET preserves value and expiry",
    )
    check_previous(
        conn.set_value(key, b"", xx=True, keepttl=True, get=True),
        previous,
        "SET XX GET returns replaced binary value",
    )
    check(conn.pexpiretime(key) == deadline, "SET GET KEEPTTL preserves exact expiry")
    check_previous(
        conn.set_value(key, updated, get=True, pxat=deadline),
        b"",
        "SET GET preserves distinction between empty and missing",
    )
    check(
        conn.get(key).unwrap() == updated and conn.pexpiretime(key) == deadline,
        "SET GET PXAT updates value and expiry",
    )
    check(
        conn.set_value(absent, updated, get=True, px=60000).is_null(),
        "unconditional SET GET missing returns null",
    )
    check(
        conn.get(absent).unwrap() == updated,
        "unconditional SET GET creates missing key",
    )

def fetch_and_expire(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("getex")
    auto missing = f.key("getex-missing")
    auto data = bytes([255, 0, 13, 10])
    f.put(conn, key, data)
    auto before = conn.pexpiretime(key)
    auto seconds = conn.expiretime(key)
    auto millis = seconds * 1000 + 123
    check(
        conn.getex(key).unwrap() == data and conn.pexpiretime(key) == before,
        "plain GETEX keeps TTL",
    )
    check(
        conn.getex(key, exat=seconds).unwrap() == data and conn.expiretime(key) == seconds,
        "GETEX EXAT",
    )
    check(
        conn.getex(key, pxat=millis).unwrap() == data and conn.pexpiretime(key) == millis,
        "GETEX PXAT",
    )
    check(
        conn.getex(key, persist=True).unwrap() == data and conn.pttl(key) == -1,
        "GETEX PERSIST",
    )
    check(
        conn.getex(key, persist=True).unwrap() == data,
        "GETEX PERSIST on persistent key",
    )
    check(conn.getex(key, ex=60).unwrap() == data, "GETEX EX returns bytes")
    auto relative_seconds = conn.pttl(key)
    check(
        relative_seconds > 0 and relative_seconds <= 60000,
        "GETEX EX sets relative seconds",
    )
    check(conn.getex(key, px=30000).unwrap() == data, "GETEX PX returns bytes")
    auto relative_ms = conn.pttl(key)
    check(
        relative_ms > 0 and relative_ms <= 30000,
        "GETEX PX sets relative milliseconds",
    )
    check(
        conn.getex(missing, ex=60).is_none() and conn.get(missing).is_none(),
        "GETEX missing key without creation",
    )
    check(
        conn.getdel(key).unwrap() == data and conn.get(key).is_none(),
        "GETDEL removes binary value",
    )
    check(conn.getdel(key).is_none(), "GETDEL absent")
    f.put(conn, key, b"")
    check(conn.getex(key).unwrap() == b"", "GETEX present empty value")
    check(conn.getdel(key).unwrap() == b"", "GETDEL present empty value")
    f.put(conn, key, data)
    check(
        conn.getex(key, pxat=1).unwrap() == data and conn.get(key).is_none(),
        "GETEX past expiry returns old value",
    )

def ranges_and_lengths(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("range")
    auto data = bytes([255, 0, 128, 10])
    check(
        conn.strlen(key) == 0 and conn.getrange(key, 0, -1) == b"",
        "missing string lengths",
    )
    check(
        conn.setrange(key, 5, b"") == 0 and conn.get(key).is_none(),
        "empty SETRANGE does not create a key",
    )
    check(conn.setrange(key, 2, data) == 6, "SETRANGE extends with zero bytes")
    conn.pexpire(key, 60000)
    auto expiry = conn.pexpiretime(key)
    check(
        conn.getrange(key, 0, 1) == bytes([0, 0]),
        "GETRANGE inclusive end and zero padding",
    )
    check(conn.getrange(key, -4, -1) == data, "GETRANGE negative indexes")
    check(
        conn.getrange(key, -99, 99) == bytes([0, 0]) + data,
        "GETRANGE clips both bounds",
    )
    check(
        conn.getrange(key, 4, 2) == b"" and conn.getrange(key, 99, 100) == b"",
        "GETRANGE empty spans",
    )
    check(conn.setrange(key, 1, b"AB") == 6, "SETRANGE overwrites without shrinking")
    check(
        conn.getrange(key, 0, 3) == bytes([0, 65, 66, 0]),
        "SETRANGE preserves surrounding bytes",
    )
    check(conn.setrange(key, 99, b"") == 6, "empty SETRANGE preserves existing length")
    check(
        conn.append(key, data) == 10 and conn.strlen(key) == 10,
        "APPEND returns new byte count",
    )
    check(
        conn.append(key, b"") == 10 and conn.pexpiretime(key) == expiry,
        "range and append preserve exact expiry",
    )
    auto invalid = False
    try:
        conn.setrange(key, -1, b"x")
    except ValueError:
        invalid = True
    check(
        invalid and conn.strlen(key) == 10,
        "negative SETRANGE offset leaves length unchanged",
    )
    auto append_key = f.key("append-missing")
    check(conn.append(append_key, data) == len(data), "APPEND creates missing key")
    conn.pexpire(append_key, 60000)
    check(conn.get(append_key).unwrap() == data, "APPEND preserves binary input")

def wrong_types(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("wrongtype-list")
    conn.execute([b"LPUSH", key, b"item"])
    conn.pexpire(key, 60000)
    auto before = conn.pexpiretime(key)
    auto wrong = 0
    try:
        conn.getdel(key)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.getex(key, persist=True)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.setrange(key, 0, b"x")
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.getrange(key, 0, -1)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.append(key, b"x")
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.strlen(key)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    try:
        conn.set_value(key, b"x", get=True, keepttl=True)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            wrong = wrong + 1
    check(
        wrong == 7 and conn.ping(),
        "string commands report WRONGTYPE and leave socket usable",
    )
    check(
        conn.llen(key) == 1 and conn.pexpiretime(key) == before,
        "wrong-type commands preserve list and expiry",
    )

def run(bool resp3):
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    if resp3:
        conn.disconnect()
        conn = redis.connect_resp3_options(f.config)
    try:
        f.verify_version(conn)
        setter_options(conn, f)
        setter_get(conn, f)
        fetch_and_expire(conn, f)
        ranges_and_lengths(conn, f)
        wrong_types(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()

def main():
    run(False)
    run(True)
    print("Live Redis extended string command coverage: PASS")
