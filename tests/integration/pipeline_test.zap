import redis
import integration_fixtures

def main():
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 5000
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto key = f.key("pipeline")
        auto value = bytes([0, 255, 13, 10])
        auto p = redis.pipeline()
        p.queue([b"SET", key, value, b"PX", b"60000"])
        p.queue([b"LPUSH", key, b"wrong-type"])
        p.queue([b"GET", key])
        p.queue([b"PING"])
        auto replies = p.execute(conn)
        integration_fixtures.check(len(replies) == 4, "pipeline count")
        integration_fixtures.simple(replies[0], b"OK")
        match replies[1]:
            case redis.Reply.Error(data):
                integration_fixtures.check(
                    redis.error_prefix(data) == b"WRONGTYPE",
                    "real middle-command error",
                )
            case _:
                raise ValueError("expected middle pipeline error")
        integration_fixtures.bulk(replies[2], value)
        integration_fixtures.simple(replies[3], b"PONG")
        integration_fixtures.check(conn.ping(), "reusable after errors drained")
        # A raw blocking command proves total command timeouts on a real server.
        auto empty = f.key("deadline-empty-list")
        f.config.command_timeout_ms = 100
        auto blocked = f.open()
        auto timed_out = False
        try:
            blocked.execute([b"BLPOP", empty, b"0"])
        except redis.CommandTimeout:
            timed_out = True
        finally:
            blocked.disconnect()
        integration_fixtures.check(
            timed_out and blocked.is_closed(),
            "real blocking command bounded and discarded",
        )
        blocked.reconnect()
        integration_fixtures.check(blocked.ping(), "explicit reconnect after timeout")
        blocked.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live deadlines and ordered pipelines: PASS")
