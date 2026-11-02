import redis

def main():
    auto address = os.getenv("REDIS_EXAMPLE_URL")
    if len(address) == 0:
        raise ValueError("Set REDIS_EXAMPLE_URL explicitly")
    auto cfg = redis.parse_url(address)
    cfg.command_timeout_ms = 2000
    auto pool = redis.pool(cfg, capacity=2, acquire_timeout_ms=500)
    try:
        auto lease = pool.acquire()
        try:
            lease.execute([b"PING"])
        finally:
            lease.release()
    finally:
        pool.shutdown()
    print("Guarded pool lease example: PASS")
