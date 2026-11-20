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
        auto number = 1
        for step in steps:
            auto want = redis.encode_command(step.args)
            auto got = b""
            while len(got) < len(want):
                auto chunk = bytes(net.recv(socket, len(want) - len(got)))
                check(len(chunk) > 0, "command complete")
                got = got + chunk
            check(
                got == want,
                "exact bitmap wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def bad_arguments(redis.Connection conn):
    auto errors = 0
    try:
        conn.setbit(b"k", -1, 1)
    except ValueError:
        errors = errors + 1
    try:
        conn.getbit(b"k", 4294967296)
    except ValueError:
        errors = errors + 1
    try:
        conn.setbit(b"k", 0, 2)
    except ValueError:
        errors = errors + 1
    try:
        conn.bitcount(b"k", start=Optional.Some(0))
    except ValueError:
        errors = errors + 1
    try:
        conn.bitcount(b"k", end=Optional.Some(0))
    except ValueError:
        errors = errors + 1
    try:
        conn.bitcount(b"k", Optional.Some(0), Optional.Some(1), mode="invalid")
    except ValueError:
        errors = errors + 1
    try:
        conn.bitpos(b"k", -1)
    except ValueError:
        errors = errors + 1
    try:
        conn.bitpos(b"k", 1, end=Optional.Some(1))
    except ValueError:
        errors = errors + 1
    try:
        conn.bitpos(b"k", 1, start=Optional.Some(0), mode="BIT")
    except ValueError:
        errors = errors + 1
    try:
        conn.bitcount(b"k", mode="BYTE")
    except ValueError:
        errors = errors + 1
    try:
        conn.bitop("xor", b"dst", [])
    except ValueError:
        errors = errors + 1
    try:
        conn.bitop("not", b"dst", [b"a", b"b"])
    except ValueError:
        errors = errors + 1
    try:
        conn.bitfield(b"k", default_overflow="invalid")
    except ValueError:
        errors = errors + 1
    auto builder = conn.bitfield(b"k")
    try:
        builder.incrby("u8", "0", 1, overflow="invalid")
    except ValueError:
        errors = errors + 1
    check(errors == 14, "invalid bitmap arguments rejected before I/O")

def scalar_commands(redis.Connection conn, bytes key):
    check(conn.setbit(name=key, offset=0, value=1) == 0, "SETBIT old bit")
    check(conn.getbit(name=key, offset=4294967295) == 1, "GETBIT last valid offset")
    check(conn.bitcount(key) == 8, "BITCOUNT whole key")
    check(conn.bitcount(key, Optional.Some(-1), Optional.Some(-1)) == 4, "BITCOUNT implicit BYTE")
    check(conn.bitcount(key, Optional.Some(-8), Optional.Some(-1), mode="bit") == 4, "BITCOUNT explicit BIT")
    check(conn.bitpos(key, 0) == -1, "BITPOS no match")
    check(conn.bitpos(key, 1, start=Optional.Some(2)) == 17, "BITPOS start only")
    check(conn.bitpos(key, 1, Optional.Some(0), Optional.Some(2), mode="byte") == 17, "BITPOS explicit BYTE")
    check(conn.bitpos(key, 1, Optional.Some(0), Optional.Some(2), mode="BIT") == 1, "BITPOS explicit BIT")
    check(conn.bitop(operation="and", dest=key, keys=[b"a", key, b"a"]) == 4, "BITOP binary keys, order and duplicates")

def field_commands(redis.Connection conn, bytes key):
    auto builder = conn.bitfield(key, default_overflow="sat")
    builder.overflow("SAT")
    builder.set_value(fmt="i8", offset="#1", value=-2).get("i8", "#1")
    builder.incrby("u8", "0", -1, overflow="fail")
    auto values = builder.execute()
    check(len(values) == 3, "one result per operation")
    check(values[0].unwrap() == 0, "SET old field")
    check(values[1].unwrap() == -2, "signed GET")
    check(values[2].is_none(), "FAIL overflow is nil")
    builder.get("u8", "0")
    builder.reset()
    check(len(builder.execute()) == 0, "explicit reset discards work")
    builder.overflow("wrap").incrby("u8", "0", -1)
    check(builder.execute()[0].unwrap() == 255, "WRAP override after reset")
    builder.incrby("u8", "0", 1)
    check(builder.execute()[0].unwrap() == 255, "execute restores default SAT")

def bad_replies(redis.Connection conn):
    auto errors = 0
    try:
        conn.setbit(b"k", 0, 1)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.getbit(b"k", 0)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.bitcount(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.bitpos(b"k", 0)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.bitop("NOT", b"dst", [b"k"])
    except redis.ProtocolError:
        errors = errors + 1
    auto builder = conn.bitfield(b"k")
    try:
        builder.get("u8", "0").execute()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        builder.get("u8", "0").execute()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        builder.get("u8", "0").execute()
    except redis.ProtocolError:
        errors = errors + 1
    check(len(builder.execute()) == 0, "failed execute resets builder")
    check(errors == 8 and conn.ping(), "malformed replies consumed without closing connection")

def main():
    auto key = bytes([0, 255, 13, 10])
    list[Step] steps = [
        Step([b"SETBIT", key, b"0", b"1"], b":0\r\n"),
        Step([b"GETBIT", key, b"4294967295"], b":1\r\n"),
        Step([b"BITCOUNT", key], b":8\r\n"),
        Step([b"BITCOUNT", key, b"-1", b"-1"], b":4\r\n"),
        Step([b"BITCOUNT", key, b"-8", b"-1", b"BIT"], b":4\r\n"),
        Step([b"BITPOS", key, b"0"], b":-1\r\n"),
        Step([b"BITPOS", key, b"1", b"2"], b":17\r\n"),
        Step([b"BITPOS", key, b"1", b"0", b"2", b"BYTE"], b":17\r\n"),
        Step([b"BITPOS", key, b"1", b"0", b"2", b"BIT"], b":1\r\n"),
        Step([b"BITOP", b"AND", key, b"a", key, b"a"], b":4\r\n"),
        Step(
            [b"BITFIELD", key, b"OVERFLOW", b"SAT", b"SET", b"i8", b"#1", b"-2", b"GET", b"i8", b"#1",
                b"OVERFLOW", b"FAIL", b"INCRBY", b"u8", b"0", b"-1"],
            b"*3\r\n:0\r\n:-2\r\n$-1\r\n",
        ),
        Step([b"BITFIELD", key, b"OVERFLOW", b"SAT"], b"*0\r\n"),
        Step([b"BITFIELD", key, b"OVERFLOW", b"SAT", b"OVERFLOW", b"WRAP", b"INCRBY", b"u8", b"0", b"-1"], b"*1\r\n:255\r\n"),
        Step([b"BITFIELD", key, b"OVERFLOW", b"SAT", b"INCRBY", b"u8", b"0", b"1"], b"*1\r\n:255\r\n"),
        Step([b"SETBIT", b"k", b"0", b"1"], b":2\r\n"),
        Step([b"GETBIT", b"k", b"0"], b"+OK\r\n"),
        Step([b"BITCOUNT", b"k"], b":-1\r\n"),
        Step([b"BITPOS", b"k", b"0"], b":-2\r\n"),
        Step([b"BITOP", b"NOT", b"dst", b"k"], b":-1\r\n"),
        Step([b"BITFIELD", b"k", b"GET", b"u8", b"0"], b"$-1\r\n"),
        Step([b"BITFIELD", b"k", b"GET", b"u8", b"0"], b"*0\r\n"),
        Step([b"BITFIELD", b"k", b"GET", b"u8", b"0"], b"*1\r\n+OK\r\n"),
        Step([b"BITFIELD", b"k"], b"*0\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        bad_arguments(conn)
        scalar_commands(conn, key)
        field_commands(conn, key)
        bad_replies(conn)
    finally:
        conn.disconnect()
    print("Bitmap command wire tests: PASS")
