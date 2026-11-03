import redis
import integration_fixtures

# Raw RESP3 value coverage plus the public opt-in RESP3 connection path.
struct Raw3:
    bytes socket
    bytes buffer
    bool secure

    def execute(self, list[bytes] args) -> redis.Resp3:
        if self.secure:
            tls.send(self.socket, redis.encode_command(args))
        else:
            net.send(self.socket, redis.encode_command(args))
        while True:
            auto result = redis.decode3(self.buffer, max_frame=65536)
            if result.status == "ok":
                self.buffer = self.buffer[result.consumed:len(self.buffer)]
                return result.value
            integration_fixtures.check(
                result.status == "incomplete",
                "valid live RESP3 frame",
            )
            auto chunk = b""
            if self.secure:
                chunk = bytes(tls.recv(self.socket, 257))
            else:
                chunk = bytes(net.recv(self.socket, 257))
            integration_fixtures.check(len(chunk) > 0, "complete live RESP3 frame")
            self.buffer = self.buffer + chunk

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    auto hash = f.key("resp3-hash")
    auto set_key = f.key("resp3-set")
    auto sorted = f.key("resp3-sorted")
    auto value = bytes([0, 255, 128, 13, 10])
    try:
        f.verify_version(conn)
        conn.hset(hash, [redis.HashField(b"field", value)])
        conn.pexpire(hash, 60000)
        conn.sadd(set_key, [b"", value])
        conn.pexpire(set_key, 60000)
        conn.zadd(sorted, [redis.ScoredMember(value, 1.25)])
        conn.pexpire(sorted, 60000)
        auto socket = b""
        if f.config.secure:
            socket = tls.connect(f.config.host, f.config.port)
        else:
            socket = net.connect(f.config.host, f.config.port)
        auto raw = Raw3(socket, b"", f.config.secure)
        try:
            list[bytes] hello = [b"HELLO", b"3"]
            if f.config.authenticate:
                auto username = f.config.username
                if len(username) == 0:
                    username = "default"
                hello.append(b"AUTH")
                hello.append(bytes(username))
                hello.append(bytes(f.config.password))
            match raw.execute(hello):
                case redis.Resp3.Map(pairs):
                    auto confirmed = False
                    for pair in pairs:
                        match pair.key:
                            case redis.Resp3.Bulk(key):
                                if key == b"proto":
                                    match pair.value:
                                        case redis.Resp3.Integer(version):
                                            confirmed = version == 3
                                        case _:
                                            raise ValueError(
                                                "expected protocol integer",
                                            )
                            case _:
                                raise ValueError("expected HELLO bulk key")
                    integration_fixtures.check(
                        confirmed,
                        "raw fixture protocol is RESP3",
                    )
                case _:
                    raise ValueError("expected HELLO map")
            raw.execute([b"SELECT", bytes(str(f.config.database))])
            match raw.execute([b"HGETALL", hash]):
                case redis.Resp3.Map(pairs):
                    integration_fixtures.check(len(pairs) == 1, "live RESP3 map count")
                    match pairs[0].value:
                        case redis.Resp3.Bulk(data):
                            integration_fixtures.check(
                                data == value,
                                "live RESP3 binary map",
                            )
                        case _:
                            raise ValueError("expected binary map value")
                case _:
                    raise ValueError("expected HGETALL map")
            match raw.execute([b"SMEMBERS", set_key]):
                case redis.Resp3.Set(items):
                    integration_fixtures.check(len(items) == 2, "live RESP3 set")
                case _:
                    raise ValueError("expected SMEMBERS set")
            match raw.execute([b"ZSCORE", sorted, value]):
                case redis.Resp3.Double(score):
                    integration_fixtures.check(score == 1.25, "live RESP3 double")
                case _:
                    raise ValueError("expected ZSCORE double")
            match raw.execute([b"GET", f.key("resp3-missing")]):
                case redis.Resp3.Null:
                    integration_fixtures.check(True, "live RESP3 null")
                case _:
                    raise ValueError("expected RESP3 null")
            match raw.execute([b"INFO", b"server"]):
                case redis.Resp3.Verbatim(format, data):
                    integration_fixtures.check(
                        format == b"txt" and len(data) > 0,
                        "live RESP3 verbatim",
                    )
                case _:
                    raise ValueError("expected INFO verbatim")
        finally:
            if raw.secure:
                tls.close_conn(raw.socket)
            else:
                net.close_conn(raw.socket)

        auto public3 = redis.connect_resp3_options(f.config)
        try:
            integration_fixtures.check(
                public3.protocol() == "RESP3" and public3.ping(),
                "public HELLO 3 command transport",
            )
            public3.reconnect()
            integration_fixtures.check(
                public3.protocol() == "RESP3" and public3.ping(),
                "public RESP3 reconnect renegotiates",
            )
        finally:
            public3.disconnect()

        auto subscriber = redis.connect_resp3_options(f.config)
        auto topic = f.key("resp3-pubsub")
        try:
            auto sub = subscriber.pubsub()
            try:
                sub.subscribe([topic])
                match sub.next_event():
                    case redis.PubSubEvent.Subscribed(name, count):
                        integration_fixtures.check(
                            name == topic and count == 1,
                            "public RESP3 subscription acknowledgement",
                        )
                    case _:
                        raise ValueError("expected public RESP3 subscribe push")
                conn.publish(topic, value)
                match sub.next_event():
                    case redis.PubSubEvent.Message(name, payload):
                        integration_fixtures.check(
                            name == topic and payload == value,
                            "public RESP3 binary Pub/Sub push",
                        )
                    case _:
                        raise ValueError("expected public RESP3 message push")
            finally:
                sub.disconnect()
        finally:
            subscriber.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis RESP3 decoder and connections: PASS")
