import redis
import integration_fixtures

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        integration_fixtures.simple(conn.execute([b"PING"]), b"PONG")
        auto key = f.binary_key(bytes([0, 255, 13, 10]))
        auto value = bytes([0, 255, 128, 13, 10]) + bytes("café")
        f.put(conn, key, value)
        integration_fixtures.bulk(conn.execute([b"GET", key]), value)
        auto missing = f.key("missing")
        integration_fixtures.missing(conn.execute([b"GET", missing]))
        auto empty = f.key("empty")
        f.put(conn, empty, b"")
        integration_fixtures.bulk(conn.execute([b"GET", empty]), b"")
        integration_fixtures.bulk(conn.execute([b"ECHO", value]), value)

        auto rejected = False
        try:
            conn.execute([b"LPUSH", key, b"item"])
        except redis.ResponseError:
            rejected = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
        integration_fixtures.check(rejected, "real WRONGTYPE error")
        rejected = False
        try:
            conn.execute([b"ZAP_TEST_NO_SUCH_COMMAND"])
        except redis.ResponseError:
            rejected = redis.error_prefix(conn.last_error()) == b"ERR"
        integration_fixtures.check(rejected, "real unknown-command error")
        integration_fixtures.simple(conn.execute([b"PING"]), b"PONG")

        auto old_id = integration_fixtures.integer(conn.execute([b"CLIENT", b"ID"]))
        conn.disconnect()
        conn.disconnect()
        integration_fixtures.check(conn.is_closed(), "explicit close")
        conn.reconnect()
        auto new_id = integration_fixtures.integer(conn.execute([b"CLIENT", b"ID"]))
        integration_fixtures.check(
            new_id != old_id,
            "reconnect establishes a new session",
        )
        integration_fixtures.bulk(conn.execute([b"GET", key]), value)
    finally:
        conn.disconnect()
        f.cleanup()

    # Verify deterministic key deletion rather than relying on the fallback TTL.
    auto verifier = f.open()
    try:
        auto i = 0
        while i < len(f.keys):
            integration_fixtures.missing(verifier.execute([b"GET", f.keys[i]]))
            i = i + 1
    finally:
        verifier.disconnect()
    print("Live Redis basic: PASS")
