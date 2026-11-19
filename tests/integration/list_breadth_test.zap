import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def values_equal(list[bytes] actual, list[bytes] expected, str label):
    check(len(actual) == len(expected), label + ": length")
    for i in range(0, len(actual)):
        check(actual[i] == expected[i], label + ": order and bytes")

def position(redis.ListPosition result, int64 expected, str label):
    match result:
        case redis.ListPosition.Index(value):
            check(value == expected, label)
        case _:
            raise ValueError("expected scalar LPOS index: " + label)

def positions(redis.ListPosition result, list[int64] expected, str label):
    match result:
        case redis.ListPosition.Positions(values):
            check(len(values) == len(expected), label + ": length")
            for i in range(0, len(values)):
                check(values[i] == expected[i], label + ": index")
        case _:
            raise ValueError("expected LPOS COUNT result: " + label)

def missing_position(redis.ListPosition result, str label):
    match result:
        case redis.ListPosition.Missing:
            return
        case _:
            raise ValueError("expected missing LPOS result: " + label)

def positions_and_insert(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([0, 255, 13, 10]))
    auto absent = f.key("position-absent")
    auto binary = bytes([255, 0, 13, 10])
    conn.rpush(key, [binary, b"a", binary, b"b", binary, b""])
    conn.pexpire(key, 60000)
    auto expiry = conn.pexpiretime(key)
    position(conn.lpos(key, binary), 0, "LPOS default head match")
    position(conn.lpos(key, binary, rank=-2), 2, "LPOS second match from tail")
    position(conn.lpos(key, b""), 5, "LPOS empty element")
    positions(conn.lpos(key, binary, count=0), [0, 2, 4], "LPOS COUNT zero matches all")
    positions(
        conn.lpos(key, binary, rank=2, count=0),
        [2, 4],
        "LPOS COUNT starts at RANK",
    )
    positions(
        conn.lpos(key, binary, rank=-1, count=2),
        [4, 2],
        "LPOS reverse COUNT order",
    )
    positions(
        conn.lpos(key, binary, count=2, maxlen=2),
        [0],
        "LPOS bounded head search",
    )
    positions(
        conn.lpos(key, binary, rank=-1, count=0, maxlen=2),
        [4],
        "LPOS bounded tail search",
    )
    missing_position(conn.lpos(key, b"b", maxlen=2), "MAXLEN excludes later match")
    missing_position(conn.lpos(key, binary, rank=4), "RANK beyond matches")
    missing_position(conn.lpos(absent, binary), "missing list scalar form")
    positions(conn.lpos(absent, binary, count=0), [], "missing list counted form")
    positions(conn.lpos(key, b"absent", count=1), [], "no match counted form")

    check(conn.linsert(absent, "after", binary, b"x") == 0, "LINSERT absent list")
    check(
        conn.linsert(key, "before", b"not-a-pivot", b"x") == -1,
        "LINSERT absent pivot",
    )
    check(
        conn.linsert(key, "bEfOrE", binary, b"head") == 7,
        "LINSERT before first duplicate",
    )
    check(
        conn.linsert(key, "AFTER", binary, b"after") == 8,
        "LINSERT after first duplicate",
    )
    values_equal(
        conn.lrange(key, 0, -1),
        [b"head", binary, b"after", b"a", binary, b"b", binary, b""],
        "LINSERT position",
    )
    check(
        conn.pexpiretime(key) == expiry and conn.exists([absent]) == 0,
        "LINSERT keeps expiry and missing keys",
    )

def conditional_push_and_remove(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("remove")
    auto missing = f.key("push-missing")
    auto binary = bytes([0, 255, 128])
    check(conn.lpushx(missing, [binary, b""]) == 0, "LPUSHX skips missing list")
    check(conn.rpushx(missing, [binary, b""]) == 0, "RPUSHX skips missing list")
    check(
        conn.lrem(missing, 0, binary) == 0 and conn.exists([missing]) == 0,
        "LREM missing list",
    )
    conn.rpush(key, [binary, b"a", binary, b"b", binary])
    conn.pexpire(key, 60000)
    auto expiry = conn.pexpiretime(key)
    check(conn.lpushx(key, [b"low", b"high"]) == 7, "LPUSHX multiple elements")
    check(conn.rpushx(key, [b"", b"tail"]) == 9, "RPUSHX multiple elements")
    values_equal(
        conn.lrange(key, 0, -1),
        [b"high", b"low", binary, b"a", binary, b"b", binary, b"", b"tail"],
        "conditional push order",
    )
    check(conn.lrem(key, 1, binary) == 1, "LREM positive count removes from head")
    values_equal(
        conn.lrange(key, 2, 5),
        [b"a", binary, b"b", binary],
        "after head removal",
    )
    check(conn.lrem(key, -1, binary) == 1, "LREM negative count removes from tail")
    values_equal(
        conn.lrange(key, 2, 5),
        [b"a", binary, b"b", b""],
        "after tail removal",
    )
    check(
        conn.lrem(key, 0, binary) == 1 and conn.lrem(key, 0, binary) == 0,
        "LREM zero removes all remaining matches",
    )
    check(conn.pexpiretime(key) == expiry, "push and removal keep expiry")
    auto final = f.key("remove-final")
    conn.rpush(final, [b"", b""])
    check(
        conn.lrem(final, 0, b"") == 2 and conn.exists([final]) == 0,
        "removing every empty element deletes key",
    )

def multi_pop_and_move(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("multi-a")
    auto b = f.key("multi-b")
    auto absent = f.key("multi-absent")
    auto src = f.key("move-src")
    auto dst = f.key("move-dst")
    auto binary = bytes([255, 0, 13, 10])
    conn.rpush(a, [binary, b""])
    conn.pexpire(a, 60000)
    conn.rpush(b, [b"first", b"second", b"third"])
    conn.pexpire(b, 60000)
    auto first = conn.lmpop([absent, a, b], direction="lEfT", count=4).unwrap()
    check(first.key == a, "LMPOP chooses first nonempty key")
    values_equal(first.values, [binary, b""], "LMPOP fewer elements than requested")
    check(
        conn.exists([a]) == 0 and conn.llen(b) == 3,
        "LMPOP deletes only exhausted first key",
    )
    auto second = conn.blmpop(0.25, [absent, b], direction="RIGHT", count=2).unwrap()
    check(second.key == b, "BLMPOP finds ready key")
    values_equal(second.values, [b"third", b"second"], "BLMPOP right order")
    values_equal(
        conn.lmpop([b], direction="left").unwrap().values,
        [b"first"],
        "LMPOP default count",
    )
    check(conn.lmpop([a, b], direction="LEFT").is_none(), "LMPOP no ready key")
    check(
        conn.blmpop(0.01, [absent], direction="RIGHT").is_none() and conn.ping(),
        "BLMPOP server timeout preserves connection",
    )

    conn.rpush(src, [b"", binary])
    conn.pexpire(src, 60000)
    auto expiry = conn.pexpiretime(src)
    check(conn.rpoplpush(src, src).unwrap() == binary, "RPOPLPUSH rotates same key")
    values_equal(conn.lrange(src, 0, -1), [binary, b""], "rotation keeps all values")
    check(conn.pexpiretime(src) == expiry, "rotation keeps expiry")
    check(
        conn.rpoplpush(src, dst).unwrap() == b"",
        "RPOPLPUSH moves present empty element",
    )
    conn.pexpire(dst, 60000)
    check(
        conn.brpoplpush(src, dst, timeout=0.25).unwrap() == binary,
        "BRPOPLPUSH moves binary tail",
    )
    values_equal(conn.lrange(dst, 0, -1), [binary, b""], "move prepends to destination")
    check(
        conn.rpoplpush(src, dst).is_none() and conn.exists([src]) == 0,
        "RPOPLPUSH missing source",
    )
    check(
        conn.brpoplpush(src, dst, timeout=0.01).is_none() and conn.ping(),
        "BRPOPLPUSH timeout preserves connection",
    )

def wrong_types(redis.Connection conn, integration_fixtures.Fixture f):
    auto wrong = f.key("wrong-type")
    auto src = f.key("move-source")
    f.put(conn, wrong, b"string")
    conn.rpush(src, [b"value"])
    conn.pexpire(src, 60000)
    auto expiry = conn.pexpiretime(src)
    auto failures = 0
    try:
        conn.rpushx(wrong, [b"new"])
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            failures = failures + 1
    try:
        conn.lpos(wrong, b"value", count=0)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            failures = failures + 1
    try:
        conn.lmpop([wrong, src], direction="LEFT")
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            failures = failures + 1
    try:
        conn.rpoplpush(src, wrong)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            failures = failures + 1
    try:
        conn.brpoplpush(src, wrong, timeout=0.25)
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            failures = failures + 1
    check(failures == 5 and conn.ping(), "typed list operations propagate WRONGTYPE")
    check(
        conn.get(wrong).unwrap() == b"string",
        "failed operations preserve destination",
    )
    check(
        conn.lindex(src, 0).unwrap() == b"value" and conn.pexpiretime(src) == expiry,
        "failed pops preserve source and expiry",
    )

def run(bool resp3):
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 3000
    auto conn = f.open()
    if resp3:
        conn.disconnect()
        conn = redis.connect_resp3_options(f.config)
    try:
        f.verify_version(conn)
        positions_and_insert(conn, f)
        conditional_push_and_remove(conn, f)
        multi_pop_and_move(conn, f)
        wrong_types(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()

def main():
    run(False)
    run(True)
    print("Live Redis extended list commands: PASS")
