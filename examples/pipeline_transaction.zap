import redis

def main():
    auto address = os.getenv("REDIS_EXAMPLE_URL")
    if len(address) == 0:
        raise ValueError("Set REDIS_EXAMPLE_URL explicitly")
    auto cfg = redis.parse_url(address)
    cfg.command_timeout_ms = 2000
    auto conn = redis.connect_options(cfg)
    auto key = bytes("zap-redis-example:" + crypto.token_hex(16))
    try:
        conn.set_value(key, b"before", px=60000)
        auto batch = redis.pipeline(max_commands=2)
        batch.queue([b"GET", key])
        batch.queue([b"PING"])
        auto replies = batch.execute(conn)
        if len(replies) != 2:
            raise ValueError("unexpected pipeline reply count")
        auto tx = conn.transaction()
        try:
            tx.watch([key])
            tx.execute([b"GET", key])
            tx.multi()
            tx.queue([b"SET", key, b"after", b"PX", b"60000"])
            match tx.exec():
                case redis.TransactionResult.Committed(results):
                    if len(results) != 1:
                        raise ValueError("unexpected EXEC reply count")
                case redis.TransactionResult.Conflict:
                    # An application decides if retrying its logic is safe.
                    raise ValueError("unexpected WATCH conflict in isolated example")
                case redis.TransactionResult.Aborted(error):
                    raise ValueError("example transaction aborted")
        finally:
            tx.discard()
    finally:
        try:
            conn.execute([b"DEL", key])
        finally:
            conn.disconnect()
    print("Pipeline and transaction example: PASS")
