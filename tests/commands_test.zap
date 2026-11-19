import redis

struct Step:
    list[bytes] args
    bytes reply

def check(bool ok, str label):
    if not ok:
        raise ValueError("FAIL: " + label)

def server(bytes listener, list[Step] steps):
    auto socket = net.accept(listener)
    net.close_listener(listener)
    try:
        for step in steps:
            auto want = redis.encode_command(step.args)
            auto got = b""
            while len(got) < len(want):
                auto chunk = bytes(net.recv(socket, len(want) - len(got)))
                check(len(chunk) > 0, "command complete")
                got = got + chunk
            check(got == want, "exact helper wire command")
            net.send(socket, step.reply)
    finally:
        net.close_conn(socket)

def invalid_options(redis.Connection conn):
    auto caught = 0
    list[bytes] no_keys = []
    list[redis.KeyValue] no_pairs = []
    try:
        conn.set_value(b"k", b"v", nx=True, xx=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", ex=-1)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", px=-1)
    except ValueError:
        caught = caught + 1
    try:
        conn.set_value(b"k", b"v", ex=1, px=1)
    except ValueError:
        caught = caught + 1
    try:
        conn.mget(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.mset(no_pairs)
    except ValueError:
        caught = caught + 1
    try:
        conn.delete(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.unlink(no_keys)
    except ValueError:
        caught = caught + 1
    try:
        conn.exists(no_keys)
    except ValueError:
        caught = caught + 1
    check(caught == 9, "all invalid options rejected locally")

def invalid_generic_options(redis.Connection conn):
    list[bytes] empty = []
    auto caught = 0
    try:
        conn.touch(empty)
    except ValueError:
        caught = caught + 1
    try:
        conn.copy(b"s", b"d", destination_db=-2)
    except ValueError:
        caught = caught + 1
    try:
        conn.copy(b"s", b"d", destination_db=2147483648)
    except ValueError:
        caught = caught + 1
    try:
        conn.restore(b"s", -1, b"opaque")
    except ValueError:
        caught = caught + 1
    try:
        conn.restore(b"s", 0, b"opaque", idletime=-2)
    except ValueError:
        caught = caught + 1
    try:
        conn.restore(b"s", 0, b"opaque", frequency=-2)
    except ValueError:
        caught = caught + 1
    try:
        conn.restore(b"s", 0, b"opaque", frequency=256)
    except ValueError:
        caught = caught + 1
    try:
        conn.restore(b"s", 0, b"opaque", idletime=0, frequency=0)
    except ValueError:
        caught = caught + 1
    try:
        conn.object("HELP", b"s")
    except ValueError:
        caught = caught + 1
    try:
        conn.expireat(b"s", 5, nx=True, xx=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.expireat(b"s", 5, nx=True, gt=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.pexpireat(b"s", 5, nx=True, lt=True)
    except ValueError:
        caught = caught + 1
    try:
        conn.pexpireat(b"s", 5, gt=True, lt=True)
    except ValueError:
        caught = caught + 1
    check(caught == 13, "generic key options rejected before I/O")

def invalid_generic_replies(redis.Connection conn):
    auto caught = 0
    try:
        conn.rename(b"s", b"d")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.renamenx(b"s", b"d")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.keys()
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.keys()
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.touch([b"s"])
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.dump(b"s")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.expiretime(b"s")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.object("ENCODING", b"s")
    except redis.ProtocolError:
        caught = caught + 1
    try:
        conn.object("IDLETIME", b"s")
    except redis.ProtocolError:
        caught = caught + 1
    check(
        caught == 9 and conn.ping(),
        "generic keys validate typed replies and preserve connection",
    )

def generic_key_commands():
    auto key = bytes([255, 0, 13, 10])
    auto serialized = bytes([0, 128, 255, 13, 10])
    list[Step] steps = [
        Step([b"RENAME", key, b"d"], b"+OK\r\n"),
        Step([b"RENAMENX", key, b"d"], b":0\r\n"),
        Step([b"COPY", key, b"d", b"DB", b"0", b"REPLACE"], b":1\r\n"),
        Step([b"KEYS", b"*"], b"*0\r\n"),
        Step([b"KEYS", b""], b"*0\r\n"),
        Step([b"RANDOMKEY"], b"$-1\r\n"),
        Step([b"RANDOMKEY"], b"$0\r\n\r\n"),
        Step([b"TOUCH", key, key], b":2\r\n"),
        Step([b"DUMP", key], b"$5\r\n" + serialized + b"\r\n"),
        Step([b"RESTORE", key, b"0", serialized, b"IDLETIME", b"0"], b"+OK\r\n"),
        Step([b"RESTORE", key, b"0", serialized, b"FREQ", b"255"], b"+OK\r\n"),
        Step([b"EXPIREAT", key, b"-1", b"NX"], b":1\r\n"),
        Step([b"PEXPIREAT", key, b"4294967296123", b"XX", b"LT"], b":0\r\n"),
        Step([b"EXPIRETIME", key], b":-1\r\n"),
        Step([b"PEXPIRETIME", key], b":4294967296123\r\n"),
        Step([b"OBJECT", b"ENCODING", key], b"$3\r\nraw\r\n"),
        Step([b"OBJECT", b"REFCOUNT", key], b":42\r\n"),
        Step([b"OBJECT", b"IDLETIME", key], b":0\r\n"),
        Step([b"OBJECT", b"ENCODING", key], b"$-1\r\n"),
        Step([b"RENAME", b"s", b"d"], b"+QUEUED\r\n"),
        Step([b"RENAMENX", b"s", b"d"], b":2\r\n"),
        Step([b"KEYS", b"*"], b"*-1\r\n"),
        Step([b"KEYS", b"*"], b"*1\r\n$-1\r\n"),
        Step([b"TOUCH", b"s"], b":2\r\n"),
        Step([b"DUMP", b"s"], b":3\r\n"),
        Step([b"EXPIRETIME", b"s"], b":-3\r\n"),
        Step([b"OBJECT", b"ENCODING", b"s"], b":1\r\n"),
        Step([b"OBJECT", b"IDLETIME", b"s"], b":-1\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=1000)
    try:
        invalid_generic_options(conn)
        check(conn.rename(key, b"d"), "RENAME")
        check(not conn.renamenx(key, b"d"), "RENAMENX skips")
        check(conn.copy(key, b"d", destination_db=0, replace=True), "COPY DB 0 REPLACE")
        check(
            len(conn.keys()) == 0 and len(conn.keys(b"")) == 0,
            "KEYS default and explicit empty pattern",
        )
        check(conn.randomkey().is_none(), "RANDOMKEY missing")
        check(conn.randomkey().unwrap() == b"", "RANDOMKEY empty binary key")
        check(conn.touch([key, key]) == 2, "TOUCH duplicate binary keys")
        check(conn.dump(key).unwrap() == serialized, "DUMP opaque binary data")
        check(conn.restore(key, 0, serialized, idletime=0), "RESTORE zero idle time")
        check(
            conn.restore(key, 0, serialized, frequency=255),
            "RESTORE upper frequency bound",
        )
        check(conn.expireat(key, -1, nx=True), "EXPIREAT negative timestamp")
        check(
            not conn.pexpireat(key, 4294967296123, xx=True, lt=True),
            "PEXPIREAT full int64 and combined flags",
        )
        check(conn.expiretime(key) == -1, "EXPIRETIME persistent sentinel")
        check(conn.pexpiretime(key) == 4294967296123, "PEXPIRETIME full int64")
        match conn.object("encoding", key):
            case redis.ObjectInfo.Encoding(name):
                check(name == b"raw", "typed OBJECT encoding")
            case _:
                raise ValueError("expected OBJECT encoding")
        match conn.object("refcount", key):
            case redis.ObjectInfo.Refcount(count):
                check(count == 42, "typed OBJECT refcount")
            case _:
                raise ValueError("expected OBJECT refcount")
        match conn.object("idletime", key):
            case redis.ObjectInfo.IdleTime(seconds):
                check(seconds == 0, "typed OBJECT idle time")
            case _:
                raise ValueError("expected OBJECT idle time")
        match conn.object("encoding", key):
            case redis.ObjectInfo.Missing:
                check(True, "typed OBJECT missing key")
            case _:
                raise ValueError("expected missing OBJECT")
        invalid_generic_replies(conn)
    finally:
        conn.disconnect()

def main():
    list[Step] steps = [
        Step([b"PING"], b"+PONG\r\n"),
        Step([b"GET", b"k"], b"$-1\r\n"),
        Step([b"GET", b"k"], b"$0\r\n\r\n"),
        Step([b"SET", b"k", b"v", b"EX", b"60", b"NX"], b"+OK\r\n"),
        Step([b"SET", b"k", b"v", b"PX", b"1000", b"XX"], b"$-1\r\n"),
        Step([b"MGET", b"a", b"b"], b"*2\r\n$1\r\nv\r\n$-1\r\n"),
        Step([b"MSET", b"a", b"v", b"b", b""], b"+OK\r\n"),
        Step([b"DEL", b"a", b"b"], b":2\r\n"),
        Step([b"UNLINK", b"a"], b":1\r\n"),
        Step([b"EXISTS", b"a", b"a"], b":2\r\n"),
        Step([b"TYPE", b"k"], b"+string\r\n"),
        Step([b"INCR", b"k"], b":1\r\n"),
        Step([b"INCRBY", b"k", b"-2"], b":-1\r\n"),
        Step([b"DECR", b"k"], b":-2\r\n"),
        Step([b"DECRBY", b"k", b"3"], b":-5\r\n"),
        Step([b"EXPIRE", b"k", b"60"], b":1\r\n"),
        Step([b"PEXPIRE", b"k", b"1000"], b":0\r\n"),
        Step([b"TTL", b"k"], b":-1\r\n"),
        Step([b"PTTL", b"k"], b":-2\r\n"),
        Step([b"PERSIST", b"k"], b":1\r\n"),
    ]
    steps.append(Step([b"GET", b"k"], b":1\r\n"))
    steps.append(Step([b"MGET", b"k"], b"*0\r\n"))
    steps.append(Step([b"MGET", b"k"], b"*1\r\n:1\r\n"))
    steps.append(Step([b"SET", b"k", b"v"], b"$-1\r\n"))
    steps.append(Step([b"PERSIST", b"k"], b":2\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        check(conn.ping(), "ping")
        check(conn.get(b"k").is_none(), "missing")
        check(conn.get(b"k").unwrap() == b"", "empty")
        check(conn.set_value(b"k", b"v", nx=True, ex=60), "SET EX NX")
        check(not conn.set_value(b"k", b"v", xx=True, px=1000), "SET PX XX")
        auto values = conn.mget([b"a", b"b"])
        check(values[0].unwrap() == b"v" and values[1].is_none(), "MGET")
        check(
            conn.mset([redis.KeyValue(b"a", b"v"), redis.KeyValue(b"b", b"")]),
            "MSET",
        )
        check(conn.delete([b"a", b"b"]) == 2, "DEL")
        check(conn.unlink([b"a"]) == 1, "UNLINK")
        check(conn.exists([b"a", b"a"]) == 2, "EXISTS")
        check(conn.key_type(b"k") == b"string", "TYPE")
        check(conn.incr(b"k") == 1, "incr")
        check(conn.incrby(b"k", -2) == -1, "incrby")
        check(conn.decr(b"k") == -2, "decr")
        check(conn.decrby(b"k", 3) == -5, "decrby")
        check(conn.expire(b"k", 60) == True, "expire")
        check(conn.pexpire(b"k", 1000) == False, "pexpire")
        check(conn.ttl(b"k") == -1, "ttl")
        check(conn.pttl(b"k") == -2, "pttl")
        check(conn.persist(b"k") == True, "persist")
        auto mismatches = 0
        try:
            conn.get(b"k")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.mget([b"k"])
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.mget([b"k"])
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.set_value(b"k", b"v")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        try:
            conn.persist(b"k")
        except redis.ProtocolError:
            mismatches = mismatches + 1
        check(
            mismatches == 5 and conn.ping(),
            "typed reply validation consumes complete replies",
        )
    finally:
        conn.disconnect()
    generic_key_commands()
    print("Typed command wire and validation: PASS")
