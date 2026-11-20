import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def shard_ack(redis.PubSub session, bytes wanted, int64 expected):
    match session.next_event():
        case redis.PubSubEvent.ShardSubscribed(topic, count):
            check(topic == wanted and count == expected, "shard subscribe acknowledgement")
        case _:
            raise ValueError("Expected shard subscription")

def shard_unsub(redis.PubSub session, Optional[bytes] wanted, int64 expected):
    match session.next_event():
        case redis.PubSubEvent.ShardUnsubscribed(topic, count):
            check(topic.is_some() == wanted.is_some() and count == expected, "shard unsubscribe count and optional topic")
            if wanted.is_some():
                check(topic.unwrap() == wanted.unwrap(), "shard unsubscribe topic")
        case _:
            raise ValueError("Expected shard unsubscription")

def exercise(redis.Connection publisher, redis.Connection subscriber, bytes first, bytes second):
    auto payload = bytes([255, 0, 13, 10])
    auto session = subscriber.pubsub()
    try:
        session.subscribe([first])
        match session.next_event():
            case redis.PubSubEvent.Subscribed(topic, count):
                check(topic == first and count == 1, "ordinary subscription")
            case _:
                raise ValueError("Expected ordinary subscription")
        auto empty_rejected = False
        try:
            session.ssubscribe([])
        except ValueError:
            empty_rejected = True
        check(empty_rejected and session.subscription_count() == 1, "empty shard subscribe rejected locally")
        session.ssubscribe([first, second])
        check(session.subscription_count() == 1, "sending control does not pre-count acknowledgements")
        shard_ack(session, first, 1)
        shard_ack(session, second, 2)
        check(session.subscription_count() == 3, "ordinary and shard counts are distinct")
        session.ssubscribe([second])
        shard_ack(session, second, 2)
        check(session.subscription_count() == 3, "duplicate subscription does not inflate count")
        check(publisher.publish(first, b"plain") == 1, "ordinary publish recipients")
        check(publisher.spublish(topic=first, message=payload) == 1, "shard publish recipients")
        match session.next_event():
            case redis.PubSubEvent.Message(topic, value):
                check(topic == first and value == b"plain", "ordinary and shard messages are distinct")
            case _:
                raise ValueError("Expected ordinary message")
        match session.next_event():
            case redis.PubSubEvent.ShardMessage(topic, value):
                check(topic == first and value == payload, "binary shard message")
            case _:
                raise ValueError("Expected shard message")
        session.sunsubscribe([first])
        shard_unsub(session, Optional.Some(first), 1)
        check(publisher.spublish(first, payload) == 0 and session.subscription_count() == 2, "explicit shard unsubscribe leaves ordinary subscription")
        session.sunsubscribe()
        shard_unsub(session, Optional.Some(second), 0)
        session.sunsubscribe()
        shard_unsub(session, None, 0)
        check(session.subscription_count() == 1 and publisher.spublish(second, b"") == 0, "unsubscribe all and nil acknowledgement")
        session.unsubscribe([first])
        match session.next_event():
            case redis.PubSubEvent.Unsubscribed(topic, count):
                check(topic.unwrap() == first and count == 0, "ordinary unsubscribe")
            case _:
                raise ValueError("Expected ordinary unsubscription")
        check(session.subscription_count() == 0, "total subscription count returns to zero")
    finally:
        session.disconnect()

def lease_publish(integration_fixtures.Fixture f):
    auto pool = redis.pool(f.config, capacity=2)
    auto sub_lease = pool.acquire(dedicated=True)
    auto publisher = pool.acquire()
    try:
        auto sharded = sub_lease.pubsub()
        try:
            auto topic = f.binary_key(bytes([0, 128, 255]))
            sharded.ssubscribe([topic])
            shard_ack(sharded, topic, 1)
            check(publisher.spublish(topic, b"") == 1, "publish through ordinary pool lease")
            match sharded.next_event():
                case redis.PubSubEvent.ShardMessage(received_topic, payload):
                    check(received_topic == topic and payload == b"", "lease shard payload")
                case _:
                    raise ValueError("Expected shard message")
        finally:
            sharded.disconnect()
    finally:
        try:
            publisher.release()
        finally:
            try:
                sub_lease.release()
            finally:
                pool.shutdown()

def main():
    auto f = integration_fixtures.fixture()
    auto publisher = f.open()
    try:
        f.verify_version(publisher)
        auto resp2 = f.open()
        try:
            exercise(publisher, resp2, f.binary_key(bytes([0, 255, 13, 10])), f.key("second-resp2"))
        finally:
            resp2.disconnect()
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            exercise(publisher, resp3, f.binary_key(bytes([128, 0, 255])), f.key("second-resp3"))
        finally:
            resp3.disconnect()
        lease_publish(f)
    finally:
        publisher.disconnect()
    print("Live Redis sharded Pub/Sub: PASS")
