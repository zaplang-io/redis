import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto key = f.binary_key(bytes([0, 255, 13, 10]))
        auto field = bytes([255, 0, 13, 10])
        auto value = bytes([128, 255, 0, 13, 10])
        check(conn.hget(key, field).is_none(), "missing hash HGET")
        check(conn.hlen(key) == 0, "missing hash HLEN")
        check(len(conn.hgetall(key)) == 0, "missing hash HGETALL")
        check(not conn.hexists(key, field), "missing hash HEXISTS")
        check(conn.hdel(key, [field]) == 0, "missing hash HDEL")
        auto absent = conn.hmget(key, [field, b"missing"])
        check(
            len(absent) == 2 and absent[0].is_none() and absent[1].is_none(),
            "missing hash HMGET",
        )
        check(
            conn.hset(key, [redis.HashField(field, value), redis.HashField(b"", b"")]) == 2,
            "binary multi HSET",
        )
        conn.pexpire(key, 60000)
        check(conn.hget(key, field).unwrap() == value, "binary HGET")
        check(
            conn.hget(key, b"").is_some() and conn.hget(key, b"").unwrap() == b"",
            "empty field and value",
        )
        check(conn.hget(key, b"missing").is_none(), "missing field")
        check(
            conn.hset(key, [redis.HashField(field, b"new"), redis.HashField(field, value)]) == 0,
            "duplicate updates count no new fields",
        )
        auto values = conn.hmget(key, [b"", b"missing", field, field])
        check(len(values) == 4, "HMGET length")
        check(
            values[0].unwrap() == b"" and values[1].is_none(),
            "empty versus missing HMGET",
        )
        check(
            values[2].unwrap() == value and values[3].unwrap() == value,
            "HMGET ordered duplicates",
        )
        auto all_fields = conn.hgetall(key)
        check(len(all_fields) == 2, "HGETALL length")
        auto found_binary = False
        auto found_empty = False
        for entry in all_fields:
            if entry.field == field:
                check(entry.value == value, "HGETALL binary pair")
                found_binary = True
            elif entry.field == b"":
                check(entry.value == b"", "HGETALL empty pair")
                found_empty = True
            else:
                raise ValueError("unexpected hash field")
        check(found_binary and found_empty, "all hash pairs without order assumption")
        check(conn.hexists(key, field) and conn.hexists(key, b""), "HEXISTS present")
        check(conn.hlen(key) == 2, "HLEN")
        check(conn.hincrby(key, b"counter", 7) == 7, "HINCRBY creates field")
        check(conn.hincrby(key, b"counter", -9) == -2, "HINCRBY signed delta")
        auto invalid_number = False
        try:
            conn.hincrby(key, field, 1)
        except redis.ResponseError:
            invalid_number = True
        check(invalid_number, "non-integer hash counter")
        check(
            conn.hget(key, field).unwrap() == value,
            "failed increment preserves binary value",
        )
        conn.hset(key, [redis.HashField(b"max", b"9223372036854775807")])
        auto overflow = False
        try:
            conn.hincrby(key, b"max", 1)
        except redis.ResponseError:
            overflow = True
        check(overflow and conn.ping(), "counter overflow preserves connection")
        check(
            conn.hget(key, b"max").unwrap() == b"9223372036854775807",
            "overflow leaves value unchanged",
        )
        check(conn.hdel(key, [field, field, b"absent"]) == 1, "HDEL duplicate count")
        check(conn.hdel(key, [b"", b"counter", b"max"]) == 3, "HDEL final fields")
        check(conn.key_type(key) == b"none", "Redis removes empty hash")
        check(len(conn.hgetall(key)) == 0, "empty hash reads as absent")
        auto counter_key = f.key("new-counter")
        check(conn.hincrby(counter_key, b"n", -1) == -1, "HINCRBY creates hash")
        conn.pexpire(counter_key, 60000)
        auto wrong = f.key("wrong-type")
        f.put(conn, wrong, b"string")
        auto errors = 0
        try:
            conn.hset(wrong, [redis.HashField(b"f", b"v")])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hget(wrong, b"f")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hmget(wrong, [b"f"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hgetall(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hdel(wrong, [b"f"])
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hexists(wrong, b"f")
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hlen(wrong)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        try:
            conn.hincrby(wrong, b"f", 1)
        except redis.ResponseError:
            check(
                redis.error_prefix(conn.last_error()) == b"WRONGTYPE",
                "hash WRONGTYPE",
            )
            errors = errors + 1
        check(
            errors == 8 and conn.ping(),
            "all hash WRONGTYPE errors preserve connection",
        )
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis hash commands: PASS")
