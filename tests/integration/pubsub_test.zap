import redis
import integration_fixtures

def channel_ack(redis.PubSubEvent event, bytes expected, int64 total):
    match event:
        case redis.PubSubEvent.Subscribed(topic, count):
            integration_fixtures.check(
                topic == expected and count == total,
                "channel ack target/count",
            )
        case _:
            raise ValueError("expected subscribe acknowledgement")

def subscriptions(integration_fixtures.Fixture f):
    auto conn = f.open()
    auto publisher = f.open()
    auto sub = conn.pubsub()
    auto topic = bytes(f.prefix) + bytes([0, 255])
    auto second = bytes(f.prefix) + b"second"
    auto pattern = bytes(f.prefix) + b"*"
    auto payload = bytes([255, 0, 13, 10])
    try:
        sub.subscribe([topic, second])
        channel_ack(sub.next_event(), topic, 1)
        channel_ack(sub.next_event(), second, 2)
        sub.psubscribe([pattern])
        match sub.next_event():
            case redis.PubSubEvent.PatternSubscribed(name, count):
                integration_fixtures.check(
                    name == pattern and count == 3,
                    "pattern ack includes both channels",
                )
            case _:
                raise ValueError("expected pattern ack")
        integration_fixtures.check(
            publisher.publish(topic, payload) > 0,
            "PUBLISH recipient integer",
        )
        auto direct = False
        auto matched = False
        for i in range(0, 2):
            match sub.next_event():
                case redis.PubSubEvent.Message(name, data):
                    integration_fixtures.check(
                        name == topic and data == payload,
                        "binary literal delivery",
                    )
                    direct = True
                case redis.PubSubEvent.PatternMessage(name, actual, data):
                    integration_fixtures.check(
                        name == pattern and actual == topic and data == payload,
                        "binary pattern delivery",
                    )
                    matched = True
                case _:
                    raise ValueError("expected published event")
        integration_fixtures.check(
            direct and matched,
            "overlapping subscriptions produce distinct events",
        )
        sub.punsubscribe([pattern])
        match sub.next_event():
            case redis.PubSubEvent.PatternUnsubscribed(name, count):
                integration_fixtures.check(count == 2, "pattern unsubscribe count")
            case _:
                raise ValueError("expected pattern unsubscribe")
        sub.unsubscribe([second])
        sub.next_event()
        # Publish a burst while this caller deliberately does not read. There is
        # no library receiver/queue; subsequent pull calls must preserve order.
        for i in range(0, 32):
            integration_fixtures.check(
                publisher.publish(topic, bytes(str(i)) + payload) == 1,
                "one literal subscriber",
            )
        for i in range(0, 32):
            match sub.next_event():
                case redis.PubSubEvent.Message(name, data):
                    integration_fixtures.check(
                        name == topic and data == bytes(str(i)) + payload,
                        "slow pull consumer order",
                    )
                case _:
                    raise ValueError("unexpected burst event")
        list[bytes] all = []
        sub.unsubscribe(all)
        match sub.next_event():
            case redis.PubSubEvent.Unsubscribed(name, count):
                integration_fixtures.check(count == 0, "unsubscribe-to-zero")
            case _:
                raise ValueError("expected final unsubscribe")
        integration_fixtures.check(sub.subscription_count() == 0, "last consumed count")
        auto busy = False
        try:
            conn.ping()
        except redis.BusyError:
            busy = True
        integration_fixtures.check(
            busy,
            "zero subscriptions still owns the session connection",
        )
        integration_fixtures.check(
            publisher.publish(topic, payload) == 0,
            "no automatic resubscription",
        )
    finally:
        sub.disconnect()
        publisher.disconnect()
    integration_fixtures.check(conn.is_closed(), "Pub/Sub close discards connection")

def pooling(integration_fixtures.Fixture f):
    auto pool = redis.pool(f.config, capacity=1)
    auto topic = bytes(f.prefix) + b"lease"
    try:
        auto ordinary = pool.acquire()
        auto rejected = False
        try:
            ordinary.pubsub()
        except ValueError:
            rejected = True
        integration_fixtures.check(
            rejected and ordinary.publish(topic, b"none") == 0,
            "ordinary lease can publish but cannot subscribe",
        )
        ordinary.release()
        auto lease = pool.acquire(dedicated=True)
        auto sub = lease.pubsub()
        try:
            sub.subscribe([topic])
            channel_ack(sub.next_event(), topic, 1)
            rejected = False
            try:
                lease.release()
            except redis.BusyError:
                rejected = True
            integration_fixtures.check(
                rejected,
                "active subscription prevents lease release",
            )
        finally:
            sub.disconnect()
        integration_fixtures.check(
            not lease.release() and pool.size() == 0,
            "dedicated subscription socket not reused",
        )
    finally:
        pool.shutdown()

def main():
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 2000
    subscriptions(f)
    pooling(f)
    print("Live Pub/Sub acknowledgements, patterns, and slow consumers: PASS")
