import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def sequence(list[bytes] actual, list[bytes] expected):
    check(len(actual) == len(expected), "list length")
    auto i = 0
    while i < len(expected):
        check(actual[i] == expected[i], "list element order and bytes")
        i = i + 1

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto key = f.binary_key(bytes([255, 0, 13, 10]))
        auto value = bytes([0, 128, 255, 13, 10])
        check(conn.llen(key) == 0, "missing LLEN")
        check(conn.lpop(key).is_none() and conn.rpop(key).is_none(), "missing pops")
        check(conn.lindex(key, 0).is_none(), "missing LINDEX")
        check(len(conn.lrange(key, 0, -1)) == 0, "missing LRANGE")
        check(conn.ltrim(key, 0, -1), "missing LTRIM succeeds")
        auto missing_set = False
        try:
            conn.lset(key, 0, b"v")
        except redis.ResponseError:
            missing_set = True
        check(missing_set, "LSET missing is an error")
        check(conn.rpush(key, [b"a", b"", value]) == 3, "RPUSH multi length")
        conn.pexpire(key, 60000)
        check(conn.lpush(key, [b"b", b"c"]) == 5, "LPUSH multi length")
        sequence(conn.lrange(key, 0, -1), [b"c", b"b", b"a", b"", value])
        sequence(conn.lrange(key, 1, 3), [b"b", b"a", b""])
        sequence(conn.lrange(key, -2, -1), [b"", value])
        sequence(conn.lrange(key, -100, 100), [b"c", b"b", b"a", b"", value])
        check(len(conn.lrange(key, 3, 1)) == 0, "reversed range is empty")
        check(len(conn.lrange(key, 100, 200)) == 0, "out-of-range LRANGE")
        check(conn.lindex(key, -1).unwrap() == value, "negative LINDEX")
        check(
            conn.lindex(key, 3).is_some() and conn.lindex(key, 3).unwrap() == b"",
            "empty LINDEX",
        )
        check(
            conn.lindex(key, 100).is_none() and conn.lindex(key, -100).is_none(),
            "out-of-range LINDEX",
        )
        check(conn.lset(key, -2, value), "negative LSET")
        check(conn.lindex(key, -2).unwrap() == value, "binary LSET")
        auto out_of_range = False
        try:
            conn.lset(key, 100, b"v")
        except redis.ResponseError:
            out_of_range = True
        check(out_of_range and conn.llen(key) == 5, "LSET range error preserves list")
        check(conn.ltrim(key, 1, -2), "negative inclusive LTRIM")
        sequence(conn.lrange(key, 0, -1), [b"b", b"a", value])
        check(conn.lpop(key).unwrap() == b"b", "LPOP head")
        check(conn.rpop(key).unwrap() == value, "RPOP tail binary")
        check(conn.lpop(key).unwrap() == b"a", "LPOP last")
        check(conn.key_type(key) == b"none", "last pop removes list")
        check(conn.lpop(key).is_none(), "empty pop is missing")
        check(conn.lpush(key, [b""]) == 1, "push empty element")
        conn.pexpire(key, 60000)
        auto empty = conn.rpop(key)
        check(empty.is_some() and empty.unwrap() == b"", "empty pop distinct from nil")
        check(conn.rpush(key, [b"a", b"b"]) == 2, "recreate list")
        conn.pexpire(key, 60000)
        check(conn.ltrim(key, 2, 1), "empty trim range succeeds")
        check(
            conn.llen(key) == 0 and conn.key_type(key) == b"none",
            "trim removes entire list",
        )
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto errors = 0
        try:
            conn.lpush(wrong, [b"v"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.rpush(wrong, [b"v"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.lpop(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.rpop(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.llen(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.lrange(wrong, 0, -1)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.lindex(wrong, 0)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.lset(wrong, 0, b"v")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.ltrim(wrong, 0, -1)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "list WRONGTYPE",
            )
            errors = errors + 1
        check(errors == 9 and conn.ping(), "all WRONGTYPE errors preserve connection")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis list commands: PASS")
