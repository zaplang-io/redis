import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def contains(list[bytes] members, bytes want) -> bool:
    auto i = 0
    while i < len(members):
        bytes member = members[i]
        if member == want:
            return True
        i = i + 1
    return False

# No assertion depends on Redis set iteration order or a particular random pop.
def membership(list[bytes] actual, list[bytes] expected):
    check(len(actual) == len(expected), "set result cardinality")
    for member in expected:
        check(contains(actual, member), "expected binary member")

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto a = f.binary_key(bytes([255, 0, 13, 10]))
        auto b = f.key("b")
        auto missing = f.key("missing")
        auto binary = bytes([128, 255, 0, 13, 10])
        check(conn.scard(missing) == 0, "missing SCARD")
        check(not conn.sismember(missing, binary), "missing SISMEMBER")
        check(len(conn.smembers(missing)) == 0, "missing SMEMBERS")
        check(conn.spop(missing).is_none(), "missing SPOP")
        check(conn.srem(missing, [binary]) == 0, "missing SREM")
        check(conn.sadd(a, [binary, b"", b"common", binary]) == 3, "SADD deduplicates")
        conn.pexpire(a, 60000)
        check(conn.sadd(a, [binary, b""]) == 0, "existing members not counted")
        check(conn.scard(a) == 3, "SCARD")
        check(
            conn.sismember(a, binary) and conn.sismember(a, b""),
            "binary and empty membership",
        )
        check(not conn.sismember(a, b"absent"), "absent member")
        membership(conn.smembers(a), [binary, b"", b"common"])
        check(conn.sadd(b, [binary, b"common", b"other"]) == 3, "second set")
        conn.pexpire(b, 60000)
        membership(conn.sunion([a, b]), [binary, b"", b"common", b"other"])
        membership(conn.sinter([a, b]), [binary, b"common"])
        membership(conn.sdiff([a, b]), [b""])
        membership(conn.sdiff([b, a]), [b"other"])
        membership(conn.sunion([a, missing, a]), [binary, b"", b"common"])
        check(len(conn.sinter([a, missing])) == 0, "missing intersection operand")
        membership(conn.sdiff([a, missing]), [binary, b"", b"common"])
        check(len(conn.sdiff([missing, a])) == 0, "missing first difference operand")
        membership(conn.sunion([a]), [binary, b"", b"common"])
        membership(conn.sinter([a, a]), [binary, b"", b"common"])
        check(len(conn.sdiff([a, a])) == 0, "self difference")
        check(len(conn.sunion([missing])) == 0, "all missing union")
        check(
            conn.srem(a, [binary, binary, b"absent"]) == 1,
            "SREM duplicate removal count",
        )
        membership(conn.smembers(a), [b"", b"common"])
        check(conn.srem(a, [b"", b"common"]) == 2, "remove final members")
        check(conn.key_type(a) == b"none", "final SREM removes key")
        check(conn.spop(a).is_none(), "empty set SPOP nil")
        check(conn.sadd(a, [b""]) == 1, "singleton empty member")
        conn.pexpire(a, 60000)
        auto empty = conn.spop(a)
        check(empty.is_some() and empty.unwrap() == b"", "empty pop distinct from nil")
        check(conn.key_type(a) == b"none", "final SPOP removes key")
        check(conn.sadd(a, [binary, b"one", b"two"]) == 3, "random-pop set")
        conn.pexpire(a, 60000)
        list[bytes] popped = []
        auto i = 0
        while i < 3:
            bytes item = conn.spop(a).unwrap()
            check(contains([binary, b"one", b"two"], item), "pop is original member")
            check(not contains(popped, item), "popped member not repeated")
            popped.append(item)
            check(not conn.sismember(a, item), "pop removed member")
            i = i + 1
        membership(popped, [binary, b"one", b"two"])
        check(conn.spop(a).is_none() and conn.scard(a) == 0, "drained set")
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto errors = 0
        try:
            conn.sadd(wrong, [b"v"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.srem(wrong, [b"v"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.sismember(wrong, b"v")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.scard(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.smembers(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.spop(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.sunion([b, wrong])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.sinter([b, wrong])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.sdiff([b, wrong])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "set WRONGTYPE",
            )
            errors = errors + 1
        check(errors == 9 and conn.ping(), "all WRONGTYPE errors preserve connection")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis set commands: PASS")
