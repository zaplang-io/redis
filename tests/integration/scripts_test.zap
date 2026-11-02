import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    list[bytes] no_keys = []
    list[bytes] no_args = []
    try:
        f.verify_version(conn)
        auto key = f.binary_key(bytes([255, 0, 13, 10]))
        auto value = bytes([0, 255, 128, 13, 10])
        auto source = b"return {KEYS[1], ARGV[1], false, 7}"
        match conn.eval(source, [key], [value]):
            case redis.Reply.Array(items):
                check(len(items) == 4, "Lua array count")
                integration_fixtures.bulk(items[0], key)
                integration_fixtures.bulk(items[1], value)
                integration_fixtures.missing(items[2], "Lua false null")
                check(integration_fixtures.integer(items[3]) == 7, "Lua integer")
            case _:
                raise ValueError("expected Lua array")
        check(
            integration_fixtures.integer(conn.eval(b"return 42", no_keys, no_args)) == 42,
            "scalar EVAL",
        )
        integration_fixtures.missing(conn.eval(b"return false", no_keys, no_args))
        auto sha = conn.script_load(source)
        auto exists = conn.script_exists(
            [sha, bytes("0000000000000000000000000000000000000000"), sha],
        )
        check(
            len(exists) == 3 and exists[0] and not exists[1] and exists[2],
            "SCRIPT EXISTS order",
        )
        match conn.evalsha(sha, [key], [value]):
            case redis.Reply.Array(items):
                integration_fixtures.bulk(items[1], value)
            case _:
                raise ValueError("expected cached Lua array")
        # Unique source guarantees an initially absent cache entry without
        # flushing any other user's scripts from the Redis process.
        auto counter = f.key("counter")
        f.put(conn, counter, b"0")
        auto script = redis.register_script(
            bytes("-- " + f.prefix + "\n") + b"return redis.call('INCR', KEYS[1])",
        )
        check(
            not conn.script_exists([script.sha()])[0],
            "registered source initially uncached",
        )
        check(
            integration_fixtures.integer(script.run(conn, [counter], no_args)) == 1,
            "NOSCRIPT fallback executes exactly once",
        )
        check(conn.script_exists([script.sha()])[0], "fallback caches source")
        check(
            integration_fixtures.integer(script.run(conn, [counter], no_args)) == 2,
            "cached SHA path",
        )
        integration_fixtures.bulk(conn.execute([b"GET", counter]), b"2")
        auto failed = False
        try:
            conn.eval(
                b"return redis.error_reply('ERR script fixture')",
                no_keys,
                no_args,
            )
        except redis.ResponseError:
            failed = redis.error_prefix(conn.last_error()) == b"ERR"
        check(failed and conn.ping(), "script server error preserves connection")
        auto mutation = redis.register_script(
            bytes("-- error " + f.prefix + "\n") + b"redis.call('INCR', KEYS[1]); return redis.error_reply('ERR after side effect')",
        )
        failed = False
        try:
            mutation.run(conn, [counter], no_args)
        except redis.ResponseError:
            failed = True
        check(failed, "fallback EVAL error propagated without a second retry")
        integration_fixtures.bulk(conn.execute([b"GET", counter]), b"3")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis scripting: PASS")
