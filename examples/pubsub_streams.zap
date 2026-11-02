import redis

def main():
    auto address = os.getenv("REDIS_EXAMPLE_URL")
    if len(address) == 0:
        raise ValueError("Set REDIS_EXAMPLE_URL explicitly")
    auto cfg = redis.parse_url(address)
    cfg.command_timeout_ms = 2000
    auto publisher = redis.connect_options(cfg)
    auto pool = redis.pool(cfg, capacity=1)
    auto topic = bytes("zap-redis-example:" + crypto.token_hex(16))
    auto payload = bytes([0, 255, 13, 10])
    try:
        auto lease = pool.acquire(dedicated=True)
        try:
            auto sub = lease.pubsub()
            try:
                sub.subscribe([topic])
                match sub.next_event():
                    case redis.PubSubEvent.Subscribed(name, count):
                        if name != topic or count != 1:
                            raise ValueError("unexpected subscription acknowledgement")
                    case _:
                        raise ValueError("expected subscribe acknowledgement")
                publisher.publish(topic, payload)
                match sub.next_event():
                    case redis.PubSubEvent.Message(name, data):
                        if name != topic or data != payload:
                            raise ValueError("unexpected binary message")
                    case _:
                        raise ValueError("expected published message")
                list[bytes] all = []
                sub.unsubscribe(all)
                sub.next_event()
            finally:
                sub.disconnect()
        finally:
            lease.release()
        auto id = publisher.xadd(
            topic,
            [
                redis.StreamField(b"event", payload),
                redis.StreamField(b"event", b"repeated"),
            ],
        )
        publisher.execute([b"PEXPIRE", topic, b"60000"])
        auto batches = publisher.xread([redis.StreamCursor(topic, b"0")], count=1)
        if len(batches) != 1 or batches[0].entries[0].id != id:
            raise ValueError("unexpected stream ID")
        if len(batches[0].entries[0].fields) != 2:
            raise ValueError("ordered repeated stream fields were lost")
    finally:
        pool.shutdown()
        try:
            publisher.execute([b"DEL", topic])
        finally:
            publisher.disconnect()
    print("Dedicated Pub/Sub and Streams example: PASS")
