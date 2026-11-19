import redis
import integration_fixtures

def keys(redis.Connection conn, integration_fixtures.Fixture f):
    auto source = f.binary_key(bytes([0, 255, 13, 10]))
    auto dest = f.key("rename-destination")
    auto copied = f.key("copy")
    auto restored = f.key("restored")
    auto missing = f.key("missing")
    auto data = bytes([255, 0, 128, 13, 10])
    f.put(conn, source, data)
    f.put(conn, dest, b"existing")
    integration_fixtures.count(
        conn.execute([b"RENAMENX", source, dest]),
        0,
        "RENAMENX existing",
    )
    integration_fixtures.bulk(conn.execute([b"GET", dest]), b"existing")
    integration_fixtures.simple(conn.execute([b"RENAME", source, dest]), b"OK")
    integration_fixtures.missing(conn.execute([b"GET", source]))
    integration_fixtures.bulk(conn.execute([b"GET", dest]), data)
    integration_fixtures.count(
        conn.execute([b"RENAMENX", dest, source]),
        1,
        "RENAMENX missing",
    )
    integration_fixtures.rejected(
        conn,
        [b"RENAME", missing, dest],
        b"ERR",
        "RENAME missing",
    )
    integration_fixtures.count(
        conn.execute([b"COPY", source, copied]),
        1,
        "COPY missing destination",
    )
    integration_fixtures.count(
        conn.execute([b"COPY", source, copied]),
        0,
        "COPY existing destination",
    )
    f.put(conn, copied, b"old")
    integration_fixtures.count(
        conn.execute([b"COPY", source, copied, b"REPLACE"]),
        1,
        "COPY REPLACE",
    )
    integration_fixtures.bulk(conn.execute([b"GET", copied]), data)
    integration_fixtures.count(
        conn.execute([b"COPY", missing, copied, b"REPLACE"]),
        0,
        "COPY missing source",
    )
    integration_fixtures.count(
        conn.execute([b"TOUCH", source, missing, source, copied]),
        3,
        "TOUCH duplicates",
    )
    auto dump = integration_fixtures.payload(
        conn.execute([b"DUMP", source]),
        "serialized value",
    )
    integration_fixtures.missing(conn.execute([b"DUMP", missing]))
    integration_fixtures.simple(
        conn.execute([b"RESTORE", restored, b"60000", dump]),
        b"OK",
    )
    integration_fixtures.bulk(conn.execute([b"GET", restored]), data)
    integration_fixtures.rejected(
        conn,
        [b"RESTORE", restored, b"0", dump],
        b"BUSYKEY",
        "RESTORE existing",
    )
    integration_fixtures.rejected(
        conn,
        [b"RESTORE", missing, b"0", b"bad dump"],
        b"ERR",
        "RESTORE corrupt",
    )
    integration_fixtures.missing(conn.execute([b"GET", missing]))
    f.put(conn, restored, b"old")
    integration_fixtures.simple(
        conn.execute([b"RESTORE", restored, b"60000", dump, b"REPLACE"]),
        b"OK",
    )
    integration_fixtures.bulk(conn.execute([b"GET", restored]), data)
    integration_fixtures.check(
        len(integration_fixtures.payload(conn.execute([b"OBJECT", b"ENCODING", source]), "OBJECT ENCODING")) > 0,
        "OBJECT encoding is present",
    )
    integration_fixtures.check(
        integration_fixtures.integer(conn.execute([b"OBJECT", b"REFCOUNT", source])) >= 1,
        "OBJECT REFCOUNT",
    )
    integration_fixtures.missing(conn.execute([b"OBJECT", b"IDLETIME", missing]))
    auto expiry_seconds = integration_fixtures.integer(
        conn.execute([b"EXPIRETIME", source]),
    )
    auto expiry_ms = expiry_seconds * 1000 + 123
    integration_fixtures.count(
        conn.execute([b"EXPIREAT", source, bytes(str(expiry_seconds))]),
        1,
        "EXPIREAT",
    )
    integration_fixtures.count(
        conn.execute([b"EXPIRETIME", source]),
        expiry_seconds,
        "EXPIRETIME seconds",
    )
    integration_fixtures.count(
        conn.execute([b"PEXPIREAT", source, bytes(str(expiry_ms))]),
        1,
        "PEXPIREAT",
    )
    integration_fixtures.count(
        conn.execute([b"PEXPIRETIME", source]),
        expiry_ms,
        "PEXPIRETIME exact milliseconds",
    )
    integration_fixtures.count(
        conn.execute([b"PEXPIREAT", source, bytes(str(expiry_ms + 876)), b"NX"]),
        0,
        "PEXPIREAT NX existing",
    )
    integration_fixtures.count(
        conn.execute([b"PEXPIRETIME", source]),
        expiry_ms,
        "rejected expiry preserves timestamp",
    )
    integration_fixtures.count(
        conn.execute([b"PEXPIREAT", source, b"1"]),
        1,
        "past expiration",
    )
    integration_fixtures.count(
        conn.execute([b"EXPIRETIME", source]),
        -2,
        "expired key sentinel",
    )
    conn.persist(restored)
    integration_fixtures.count(
        conn.execute([b"EXPIRETIME", restored]),
        -1,
        "persistent sentinel",
    )

def key_enumeration(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([1, 255, 13, 10]))
    f.put(conn, key, b"data")
    auto scoped = integration_fixtures.items(
        conn.execute([b"KEYS", bytes(f.prefix) + b"*"]),
        "KEYS namespaced glob",
    )
    integration_fixtures.check(
        len(scoped) == 1,
        "KEYS matches only populated fixture key",
    )
    integration_fixtures.bulk(scoped[0], key)
    auto random_key = integration_fixtures.payload(
        conn.execute([b"RANDOMKEY"]),
        "RANDOMKEY populated database",
    )
    integration_fixtures.check(
        conn.exists([random_key]) == 1,
        "RANDOMKEY refers to a present key",
    )

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        if os.getenv("REDIS_TEST_ISOLATED") == "1":
            key_enumeration(conn, f)
        keys(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis generic key command coverage: PASS")
