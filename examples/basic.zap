import redis

def main():
    auto address = os.getenv("REDIS_EXAMPLE_URL")
    if len(address) == 0:
        raise ValueError("Set REDIS_EXAMPLE_URL explicitly")
    auto cfg = redis.parse_url(address)
    cfg.command_timeout_ms = 2000
    auto conn = redis.connect_options(cfg)
    auto prefix = bytes("zap-redis-example:" + crypto.token_hex(16) + ":")
    auto key = prefix + b"value"
    try:
        conn.set_value(key, bytes("hello"), px=60000)
        match conn.get(key):
            case Optional.Some(value):
                if str(value) != "hello":
                    raise ValueError("unexpected UTF-8 value")
            case Optional.None:
                raise ValueError("missing example value")
        # Redis payloads stay bytes. Decode as UTF-8 only when the application
        # knows that is the format; arbitrary binary values need no conversion.
        auto script = redis.register_script(b"return ARGV[1]")
        list[bytes] no_keys = []
        script.run(conn, no_keys, [bytes([0, 255])])
        auto cursor = b"0"
        auto pages = 0
        while True:
            auto page = conn.scan(cursor, prefix + b"*", 16)
            pages = pages + 1
            if page.done():
                break
            if pages >= 1000:
                raise ValueError("example scan exceeded its page budget")
            cursor = page.cursor
    finally:
        try:
            conn.execute([b"DEL", key])
        finally:
            conn.disconnect()
    print("Basic bytes, scripting, and scan example: PASS")
