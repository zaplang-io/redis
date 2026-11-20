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
                "exact HyperLogLog wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def rejects_bad_replies(redis.Connection conn):
    auto errors = 0
    try:
        conn.pfadd(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfadd(b"k", [b"v"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfadd(b"k", [b"v"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfcount([b"k"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfcount([b"k", b"missing"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfmerge(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.pfmerge(b"k", [b"src"])
    except redis.ProtocolError:
        errors = errors + 1
    check(errors == 7, "reject wrong reply types, ranges and acknowledgements")

def main():
    auto key = bytes([255, 0, 13, 10])
    auto member = bytes([0, 255, 128, 13, 10])
    list[Step] steps = [
        Step([b"PFADD", key], b":1\r\n"),
        Step([b"PFADD", key], b":0\r\n"),
        Step([b"PFADD", key, member, b"", member], b":1\r\n"),
        Step([b"PFCOUNT", key], b":2\r\n"),
        Step([b"PFCOUNT", key, b"", key], b":9223372036854775807\r\n"),
        Step([b"PFMERGE", b""], b"+OK\r\n"),
        Step([b"PFMERGE", key, b"", key, b""], b"+OK\r\n"),
        Step([b"PFADD", b"k"], b":2\r\n"),
        Step([b"PFADD", b"k", b"v"], b":-1\r\n"),
        Step([b"PFADD", b"k", b"v"], b"+OK\r\n"),
        Step([b"PFCOUNT", b"k"], b":-1\r\n"),
        Step([b"PFCOUNT", b"k", b"missing"], b"$-1\r\n"),
        Step([b"PFMERGE", b"k"], b"+PONG\r\n"),
        Step([b"PFMERGE", b"k", b"src"], b":1\r\n"),
        Step([b"PFMERGE", b"k", b"src"], b"-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        auto rejected = False
        try:
            conn.pfcount([])
        except ValueError:
            rejected = True
        check(rejected, "empty PFCOUNT rejected before writing")
        check(conn.pfadd(name=key) == 1, "PFADD creates empty sketch")
        check(conn.pfadd(key, []) == 0, "PFADD with explicit empty elements")
        check(conn.pfadd(name=key, values=[member, b"", member]) == 1, "PFADD preserves binary, empty and repeated elements")
        check(conn.pfcount([key]) == 2, "single PFCOUNT")
        check(conn.pfcount(sources=[key, b"", key]) == 9223372036854775807, "PFCOUNT preserves keys and int64 estimates")
        check(conn.pfmerge(b""), "PFMERGE destination-only")
        check(conn.pfmerge(dest=key, sources=[b"", key, b""]), "PFMERGE preserves binary keys, aliases and order")
        rejects_bad_replies(conn)
        rejected = False
        try:
            conn.pfmerge(b"k", [b"src"])
        except redis.ResponseError:
            rejected = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
        check(rejected and conn.ping(), "response errors preserve connection")
    finally:
        conn.disconnect()
    print("HyperLogLog wire tests: PASS")
