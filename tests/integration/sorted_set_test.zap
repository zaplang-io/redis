import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def sequence(list[bytes] actual, list[bytes] expected):
    check(len(actual) == len(expected), "sorted range length")
    auto i = 0
    while i < len(expected):
        check(actual[i] == expected[i], "sorted member order and bytes")
        i = i + 1

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto key = f.binary_key(bytes([255, 0, 13, 10]))
        auto binary = bytes([0, 255, 128, 13, 10])
        check(conn.zcard(key) == 0, "missing ZCARD")
        check(conn.zscore(key, binary).is_none(), "missing ZSCORE")
        check(conn.zrank(key, binary).is_none(), "missing ZRANK")
        check(conn.zrevrank(key, binary).is_none(), "missing ZREVRANK")
        check(len(conn.zrange(key, 0, -1)) == 0, "missing ZRANGE")
        check(len(conn.zrange_withscores(key, 0, -1)) == 0, "missing WITHSCORES")
        check(conn.zrem(key, [binary]) == 0, "missing ZREM")
        check(
            conn.zadd(key, [redis.ScoredMember(b"b", 2.0), redis.ScoredMember(b"a", 2.0), redis.ScoredMember(binary, -1.5), redis.ScoredMember(b"", 0.0)]) == 4,
            "multi ZADD",
        )
        conn.pexpire(key, 60000)
        sequence(conn.zrange(key, 0, -1), [binary, b"", b"a", b"b"])
        check(
            conn.zscore(key, b"").is_some() and conn.zscore(key, b"").unwrap() == 0.0,
            "zero score distinct from nil",
        )
        check(conn.zscore(key, binary).unwrap() == -1.5, "binary score")
        check(conn.zscore(key, b"absent").is_none(), "missing member score")
        check(conn.zrank(key, b"a").unwrap() == 2, "tied forward rank")
        check(conn.zrevrank(key, b"a").unwrap() == 1, "tied reverse rank")
        check(
            conn.zrank(key, b"absent").is_none() and conn.zrevrank(key, b"absent").is_none(),
            "missing member ranks",
        )
        sequence(conn.zrange(key, -2, -1), [b"a", b"b"])
        sequence(conn.zrange(key, 1, 2), [b"", b"a"])
        sequence(conn.zrange(key, -99, 99), [binary, b"", b"a", b"b"])
        check(len(conn.zrange(key, 3, 1)) == 0, "reversed range")
        check(len(conn.zrange(key, 99, 100)) == 0, "out-of-range")
        auto scored = conn.zrange_withscores(key, 1, -1)
        check(len(scored) == 3, "scored range count")
        check(scored[0].member == b"" and scored[0].score == 0.0, "empty scored member")
        check(scored[1].member == b"a" and scored[1].score == 2.0, "first tied score")
        check(scored[2].member == b"b" and scored[2].score == 2.0, "second tied score")
        check(
            conn.zadd(key, [redis.ScoredMember(b"a", 3.0)]) == 0,
            "ZADD update not added count",
        )
        check(conn.zrank(key, b"a").unwrap() == 3, "score update changes rank")
        check(conn.zincrby(key, b"a", -4.5) == -1.5, "signed ZINCRBY")
        sequence(conn.zrange(key, 0, 1), [binary, b"a"])
        check(
            conn.zadd(key, [redis.ScoredMember(b"dup", 1.0), redis.ScoredMember(b"dup", 4.0)]) == 1,
            "duplicate ZADD last score wins",
        )
        check(conn.zscore(key, b"dup").unwrap() == 4.0, "duplicate score")
        auto precise = 1.2345678901234567
        conn.zadd(key, [redis.ScoredMember(b"precise", precise)])
        check(
            conn.zscore(key, b"precise").unwrap() == precise,
            "17-digit score round trip",
        )
        conn.zadd(
            key,
            [
                redis.ScoredMember(b"tiny", 5e-324),
                redis.ScoredMember(b"huge", 1.7976931348623157e308),
            ],
        )
        check(
            conn.zscore(key, b"tiny").unwrap() == 5e-324,
            "subnormal score round trip",
        )
        check(
            conn.zscore(key, b"huge").unwrap() == 1.7976931348623157e308,
            "max finite score",
        )
        conn.zadd(
            key,
            [
                redis.ScoredMember(b"low", -math.inf),
                redis.ScoredMember(b"high", math.inf),
            ],
        )
        check(conn.zscore(key, b"low").unwrap() == -math.inf, "negative infinity")
        check(conn.zscore(key, b"high").unwrap() == math.inf, "positive infinity")
        auto invalid_sum = False
        try:
            conn.zincrby(key, b"high", -math.inf)
        except redis.ResponseError:
            invalid_sum = True
        check(
            invalid_sum and conn.zscore(key, b"high").unwrap() == math.inf,
            "NaN resulting increment rejected by Redis",
        )
        check(conn.zrem(key, [binary, binary, b"missing"]) == 1, "ZREM duplicate count")
        auto created = f.key("increment")
        check(conn.zincrby(created, binary, 0.25) == 0.25, "ZINCRBY creates key/member")
        conn.pexpire(created, 60000)
        check(
            conn.zrem(created, [binary]) == 1 and conn.key_type(created) == b"none",
            "final ZREM removes key",
        )
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto errors = 0
        try:
            conn.zadd(wrong, [redis.ScoredMember(b"v", 1.0)])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zrem(wrong, [b"v"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zcard(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zscore(wrong, b"v")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zincrby(wrong, b"v", 1.0)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zrank(wrong, b"v")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zrevrank(wrong, b"v")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zrange(wrong, 0, -1)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.zrange_withscores(wrong, 0, -1)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "sorted-set WRONGTYPE",
            )
            errors = errors + 1
        check(errors == 9 and conn.ping(), "all WRONGTYPE errors preserve connection")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis sorted-set commands: PASS")
