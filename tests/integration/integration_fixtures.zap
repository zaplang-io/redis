import redis

# Each suite owns a random namespace and an explicit list of keys to delete.
# A short TTL is a fallback if a crashed test cannot run its finally cleanup.
struct Fixture:
    redis.Options config
    str prefix
    list[bytes] keys

    def open(self) -> redis.Connection:
        return redis.connect_options(self.config)

    def key(self, str suffix) -> bytes:
        return self.binary_key(bytes(suffix))

    def binary_key(self, bytes suffix) -> bytes:
        auto key = bytes(self.prefix) + suffix
        self.keys.append(key)
        return key

    def put(self, redis.Connection conn, bytes key, bytes value):
        simple(conn.execute([b"SET", key, value, b"PX", b"60000"]), b"OK")

    def cleanup(self):
        if len(self.keys) > 0:
            auto conn = self.open()
            try:
                list[bytes] args = [b"DEL"]
                auto i = 0
                while i < len(self.keys):
                    args.append(self.keys[i])
                    i = i + 1
                conn.execute(args)
            finally:
                conn.disconnect()

    def verify_version(self, redis.Connection conn):
        auto want = os.getenv("REDIS_TEST_SERVER_VERSION")
        if len(want) > 0:
            auto reply = conn.execute([b"INFO", b"server"])
            match reply:
                case redis.Reply.Bulk(data):
                    auto info = str(data)
                    auto version = info =~ m/^redis_version:(?<version>[^\r\n]+)\r?$/m
                    if not version:
                        raise ValueError("server version is missing")
                    check(version.group("version") == want, "pinned server version")
                case _:
                    raise ValueError("expected server INFO response")

def fixture() -> Fixture:
    auto address = os.getenv("REDIS_TEST_URL")
    if len(address) == 0:
        raise ValueError("set REDIS_TEST_URL explicitly before running live tests")
    auto cfg = redis.parse_url(address)
    list[bytes] keys = []
    return Fixture(cfg, "zap-redis-test:" + crypto.token_hex(16) + ":", keys)

# The same key namespace on a different database lets the isolation suite prove
# SELECT routing without touching or enumerating any other test's keys.
def other_database(Fixture source) -> Fixture:
    auto old = source.config
    auto db = 0
    if old.database == 0:
        db = 1
    auto cfg = redis.options(
        old.host,
        old.port,
        db,
        old.username,
        old.password,
        old.client_name,
        old.authenticate,
        old.max_frame,
        old.max_depth,
        old.read_size,
        old.secure,
    )
    list[bytes] keys = []
    return Fixture(cfg, source.prefix, keys)

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def simple(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Simple(data):
            check(data == want, "simple reply")
        case _:
            raise ValueError("expected simple reply")

def bulk(redis.Reply reply, bytes want):
    match reply:
        case redis.Reply.Bulk(data):
            check(data == want, "bulk reply bytes")
        case _:
            raise ValueError("expected bulk reply")

def missing(redis.Reply reply, str label = "missing-key null"):
    match reply:
        case redis.Reply.NullBulk:
            return
        case _:
            raise ValueError("expected " + label)

def integer(redis.Reply reply) -> int64:
    match reply:
        case redis.Reply.Integer(value):
            return value
        case _:
            raise ValueError("expected integer reply")
