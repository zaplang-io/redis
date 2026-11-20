import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def estimate(int64 observed, int64 expected, str label):
    check(
        observed * 100 >= expected * 97 and observed * 100 <= expected * 103,
        label + ": expected within 3% of " + str(expected) + ", got " + str(observed),
    )

def small(redis.Connection conn, integration_fixtures.Fixture f, str protocol):
    auto key = f.binary_key(bytes(protocol) + bytes([255, 0, 13, 10]))
    auto other = f.key(protocol + "-other")
    auto missing = f.key(protocol + "-missing")
    auto dest = f.key(protocol + "-dest")
    auto merged_missing = f.key(protocol + "-merged-missing")
    auto member = bytes([255, 0, 128, 13, 10])
    check(conn.pfcount([missing]) == 0, "missing sketch counts zero")
    check(conn.pfcount([missing, other]) == 0 and conn.key_type(other) == b"none", "missing union counts zero without creating keys")
    check(conn.pfadd(key) == 1 and conn.key_type(key) == b"string", "PFADD creates an empty sketch")
    conn.pexpire(key, 60000)
    check(conn.pfadd(key, []) == 0 and conn.pfcount([key]) == 0, "empty add leaves an existing sketch empty")
    check(conn.pfadd(key, [member, member, b"", b"a"]) == 1, "binary and empty elements")
    auto count = conn.pfcount([key])
    check(count >= 2 and count <= 4, "small sketch estimates three distinct elements")
    check(conn.pfadd(key, [b"", b"a", member, member]) == 0, "repeat input does not change registers")
    check(conn.pfcount([key, missing, key]) == count, "repeating sketch and missing inputs do not inflate union")
    check(conn.pfadd(other, [member, b"b"]) == 1, "second sketch overlaps first")
    conn.pexpire(other, 60000)
    auto union_count = conn.pfcount([key, other, missing])
    check(union_count >= 3 and union_count <= 5, "small union estimates four distinct elements")
    check(conn.pfmerge(dest), "PFMERGE with only a missing destination")
    conn.pexpire(dest, 60000)
    check(conn.key_type(dest) == b"string" and conn.pfcount([dest]) == 0, "destination-only merge creates an empty sketch")
    check(conn.pfmerge(dest, [other, key, missing, key]), "PFMERGE accepts multiple, missing and repeated sources")
    conn.pexpire(dest, 60000)
    check(conn.pfcount([dest]) == union_count, "merge and count estimate same union")
    check(conn.pfmerge(dest) and conn.pfcount([dest]) == union_count, "destination-only merge preserves existing data")
    check(conn.pfmerge(merged_missing, [missing]), "merge with only missing sources succeeds")
    conn.pexpire(merged_missing, 60000)
    check(conn.key_type(merged_missing) == b"string" and conn.pfcount([merged_missing]) == 0, "missing sources produce an empty destination")
    check(conn.pfcount([key]) == count and conn.key_type(missing) == b"none", "merges preserve sources and do not create missing source")

def bulk_add(redis.Connection conn, bytes key, int64 start, int64 end):
    auto i = start
    auto prefix = bytes([255, 0, 128, 13, 10])
    while i < end:
        list[bytes] elements = []
        auto batch_end = i + 500
        if batch_end > end:
            batch_end = end
        while i < batch_end:
            elements.append(prefix + bytes(str(i)))
            i = i + 1
        conn.pfadd(key, elements)
        conn.pexpire(key, 60000)

def large(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("large-a")
    auto b = f.key("large-b")
    auto dest = f.key("large-destination")
    auto union_key = f.key("large-union")
    bulk_add(conn, a, 0, 4000)
    bulk_add(conn, b, 2000, 6000)
    bulk_add(conn, dest, 5500, 7500)
    auto count_a = conn.pfcount([a])
    auto count_b = conn.pfcount([b])
    auto count_dest = conn.pfcount([dest])
    estimate(count_a, 4000, "first input")
    estimate(count_b, 4000, "second input")
    estimate(count_dest, 2000, "pre-existing destination")
    auto union_count = conn.pfcount([a, b])
    estimate(union_count, 6000, "overlapping two-key union")
    check(conn.pfcount([b, a, b]) == union_count, "union order and duplicate keys")
    check(conn.pfmerge(union_key, [a, b]), "store large two-key union")
    conn.pexpire(union_key, 60000)
    check(conn.pfcount([union_key]) == union_count, "stored union matches PFCOUNT")
    auto all_count = conn.pfcount([dest, a, b])
    estimate(all_count, 7500, "union including pre-existing destination")
    check(conn.pfmerge(dest, [a, b]), "merge with existing destination")
    conn.pexpire(dest, 60000)
    check(conn.pfcount([dest]) == all_count, "existing destination participates without explicit alias")
    check(conn.pfmerge(dest, [dest, b]) and conn.pfcount([dest]) == all_count, "explicit alias and repeat merge")
    check(conn.pfcount([a]) == count_a and conn.pfcount([b]) == count_b, "large inputs preserved")

def server_errors(redis.Connection conn, integration_fixtures.Fixture f):
    auto good = f.key("error-good")
    auto wrong_type = f.key("error-list")
    auto wrong_string = f.key("error-string")
    auto absent = f.key("error-dest-absent")
    conn.pfadd(good, [b"good", b"valid"])
    conn.pexpire(good, 60000)
    conn.rpush(wrong_type, [b"list"])
    conn.pexpire(wrong_type, 60000)
    auto plain = bytes([255, 0, 128, 13, 10])
    f.put(conn, wrong_string, plain)
    auto before = conn.pfcount([good])
    auto raw = conn.get(good).unwrap()
    auto errors = 0
    for wrong in [wrong_type, wrong_string]:
        try:
            conn.pfadd(wrong, [b"v"])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFADD wrong type")
            errors = errors + 1
        try:
            conn.pfcount([wrong])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFCOUNT invalid sketch")
            errors = errors + 1
        try:
            conn.pfcount([good, wrong])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFCOUNT union invalid source")
            errors = errors + 1
        try:
            conn.pfmerge(good, [good, wrong])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFMERGE invalid source")
            errors = errors + 1
        try:
            conn.pfmerge(wrong, [good])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFMERGE invalid destination")
            errors = errors + 1
        try:
            conn.pfmerge(absent, [good, wrong])
        except redis.ResponseError:
            check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "PFMERGE invalid source for missing destination")
            errors = errors + 1
    check(errors == 12 and conn.ping(), "server errors do not close the connection")
    check(conn.get(good).unwrap() == raw and conn.pfcount([good]) == before, "failed merges preserve existing destination bytes")
    check(conn.key_type(absent) == b"none", "failed merge does not create destination")
    check(conn.get(wrong_string).unwrap() == plain and conn.llen(wrong_type) == 1, "invalid keys are unchanged")

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        small(conn, f, "resp2")
        large(conn, f)
        server_errors(conn, f)
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            small(resp3, f, "resp3")
        finally:
            resp3.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis HyperLogLog commands: PASS")
