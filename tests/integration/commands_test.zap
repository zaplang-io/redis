import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        check(conn.ping(), "typed ping")
        auto key = f.binary_key(bytes([0, 255, 13, 10]))
        auto value = bytes([255, 0, 128, 13, 10])
        auto missing = f.key("missing")
        auto empty = f.key("empty")
        check(conn.get(missing).is_none(), "GET missing")
        check(conn.set_value(key, value, px=Optional.Some(60000)), "binary SET")
        check(conn.get(key).unwrap() == value, "binary GET")
        check(not conn.set_value(key, b"wrong", nx=True), "NX existing skipped")
        check(not conn.set_value(missing, b"wrong", xx=True), "XX missing skipped")
        check(conn.set_value(empty, b"", nx=True, ex=Optional.Some(60)), "NX insert")
        check(conn.get(empty).is_some(), "empty is present")
        check(conn.get(empty).unwrap() == b"", "empty bytes")
        check(conn.set_value(key, value, xx=True, ex=Optional.Some(60)), "XX update")
        auto values = conn.mget([empty, missing, key, key])
        check(len(values) == 4, "MGET length")
        check(values[0].unwrap() == b"" and values[1].is_none(), "MGET null ordering")
        check(
            values[2].unwrap() == value and values[3].unwrap() == value,
            "MGET duplicates",
        )
        check(conn.exists([key, missing, key]) == 2, "EXISTS counts duplicate keys")
        check(conn.key_type(key) == b"string", "TYPE string")
        check(conn.key_type(missing) == b"none", "TYPE absent")
        auto counter = f.key("counter")
        f.put(conn, counter, b"0")
        check(conn.incr(counter) == 1, "INCR")
        check(conn.incrby(counter, 9) == 10, "INCRBY")
        check(conn.decr(counter) == 9, "DECR")
        check(conn.decrby(counter, 12) == -3, "DECRBY")
        check(conn.incrby(counter, -2) == -5, "negative delta")
        auto bad_number = False
        try:
            conn.incr(key)
        except redis.ResponseError:
            bad_number = True
        check(bad_number, "counter non-integer error")
        auto overflow = f.key("overflow")
        f.put(conn, overflow, b"9223372036854775807")
        auto overflowed = False
        try:
            conn.incr(overflow)
        except redis.ResponseError:
            overflowed = True
        check(overflowed and conn.ping(), "overflow preserves usable connection")
        check(
            conn.ttl(missing) == -2 and conn.pttl(missing) == -2,
            "missing TTL sentinel",
        )
        check(conn.persist(key), "remove TTL")
        check(conn.ttl(key) == -1 and conn.pttl(key) == -1, "persistent TTL sentinel")
        check(not conn.persist(key), "already persistent")
        check(not conn.expire(missing, 60), "EXPIRE missing")
        check(not conn.pexpire(missing, 60000), "PEXPIRE missing")
        check(not conn.persist(missing), "PERSIST missing")
        check(conn.expire(key, 60), "EXPIRE")
        auto seconds = conn.ttl(key)
        check(seconds > 0 and seconds <= 60, "TTL bounded range")
        check(conn.pexpire(key, 60000), "PEXPIRE")
        auto millis = conn.pttl(key)
        check(millis > 0 and millis <= 60000, "PTTL bounded range")
        check(conn.expire(empty, 0), "zero EXPIRE deletes")
        check(conn.get(empty).is_none(), "immediate expiration")
        auto a = f.key("a")
        auto b = f.key("b")
        check(conn.mset([redis.KeyValue(a, value), redis.KeyValue(b, b"")]), "MSET")
        # MSET clears previous TTLs; cleanup owns both keys from registration.
        check(
            conn.get(a).unwrap() == value and conn.get(b).unwrap() == b"",
            "MSET values",
        )
        check(conn.delete([a, a, missing]) == 1, "DEL actual count")
        check(conn.unlink([b, missing]) == 1, "UNLINK actual count")
        auto list_key = f.key("list")
        conn.execute([b"LPUSH", list_key, b"item"])
        conn.pexpire(list_key, 60000)
        check(conn.mget([list_key])[0].is_none(), "MGET non-string is null")
        auto wrong_type = False
        try:
            conn.get(list_key)
        except redis.ResponseError:
            wrong_type = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
        check(wrong_type and conn.ping(), "GET wrong-type error preserved")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis typed commands: PASS")
