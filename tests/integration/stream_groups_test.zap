import redis
import integration_fixtures

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    auto key = f.binary_key(bytes([0, 255]))
    auto group = bytes([255, 0])
    auto consumer = bytes([0, 10])
    auto claimant = bytes([255, 10])
    auto field = bytes([0, 255])
    auto value = bytes([255, 0, 13, 10])
    try:
        auto id = conn.xadd(
            key,
            [redis.StreamField(field, value), redis.StreamField(field, b"second")],
        )
        conn.xgroup_create(key, group, b"0-0", True)
        auto duplicate = False
        try:
            conn.xgroup_create(key, group, b"0-0")
        except redis.ResponseError:
            duplicate = redis.error_prefix(conn.last_error()) == b"BUSYGROUP"
        integration_fixtures.check(duplicate, "BUSYGROUP reported")

        list[redis.StreamCursor] fresh_cursor = [redis.StreamCursor(key, b">")]
        auto fresh = conn.xreadgroup(group, consumer, fresh_cursor, count=2)
        integration_fixtures.check(
            len(fresh) == 1 and len(fresh[0].entries) == 1,
            "new group delivery",
        )
        integration_fixtures.check(
            fresh[0].entries[0].id == id and fresh[0].entries[0].fields[0].field == field and fresh[0].entries[0].fields[0].value == value and fresh[0].entries[0].fields[1].field == field,
            "binary entry and repeated fields",
        )
        auto pending = conn.xpending(key, group)
        integration_fixtures.check(
            pending.total == 1 and pending.consumers[0].name == consumer,
            "pending total and consumer identity",
        )
        auto claimed = conn.xautoclaim(key, group, claimant, 0, b"0-0", 2)
        integration_fixtures.check(
            len(claimed.entries) == 1 and claimed.entries[0].id == id,
            "abandoned entry claimed",
        )
        pending = conn.xpending(key, group)
        integration_fixtures.check(
            pending.total == 1 and pending.consumers[0].name == claimant,
            "claim transfers pending ownership",
        )
        integration_fixtures.check(
            conn.xack(key, group, [id]) == 1,
            "acknowledge claimed entry",
        )
        integration_fixtures.check(
            conn.xpending(key, group).total == 0,
            "pending entry acknowledged",
        )
        integration_fixtures.check(conn.xgroup_destroy(key, group), "destroy group")
        auto missing = False
        try:
            conn.xreadgroup(group, consumer, fresh_cursor)
        except redis.ResponseError:
            missing = redis.error_prefix(conn.last_error()) == b"NOGROUP"
        integration_fixtures.check(
            missing and conn.ping(),
            "missing group preserves connection",
        )
    finally:
        conn.disconnect()

    f.config.command_timeout_ms = 50
    auto blocked = f.open()
    auto blocked_key = f.binary_key(bytes([0, 254]))
    auto timed_out = False
    try:
        blocked.xgroup_create(blocked_key, group, b"0-0", True)
        list[redis.StreamCursor] new_cursor = [redis.StreamCursor(blocked_key, b">")]
        blocked.xreadgroup(group, consumer, new_cursor, block_ms=0)
    except redis.CommandTimeout:
        timed_out = True
    finally:
        blocked.disconnect()
    integration_fixtures.check(
        timed_out and blocked.is_closed(),
        "client timeout discards blocked group read",
    )
    f.cleanup()
    print("Live Streams consumer groups: PASS")
