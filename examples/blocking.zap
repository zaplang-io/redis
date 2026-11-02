import redis

def produce(redis.Options cfg, bytes key):
    auto conn = redis.connect_options(cfg)
    try:
        conn.rpush(key, [bytes([0, 255, 13, 10])])
        conn.execute([b"PEXPIRE", key, b"60000"])
    finally:
        conn.disconnect()

def main():
    auto address = os.getenv("REDIS_EXAMPLE_URL")
    if len(address) == 0:
        raise ValueError("Set REDIS_EXAMPLE_URL explicitly")
    auto cfg = redis.parse_url(address)
    cfg.command_timeout_ms = 2000
    auto key = bytes("zap-redis-example:" + crypto.token_hex(16))
    auto pool = redis.pool(cfg, capacity=1)
    auto conn = redis.connect_options(cfg)
    try:
        auto lease = pool.acquire(dedicated=True)
        try:
            scope:
                spawn produce(cfg, key)
                match lease.blpop([key], 1.0):
                    case Optional.Some(item):
                        if item.key != key or item.value != bytes([0, 255, 13, 10]):
                            raise ValueError("unexpected blocking result")
                    case Optional.None:
                        raise ValueError("producer did not supply a value")
        finally:
            lease.release()
    finally:
        pool.shutdown()
        try:
            conn.execute([b"DEL", key])
        finally:
            conn.disconnect()
    print("Dedicated blocking consumer example: PASS")
