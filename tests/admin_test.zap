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
                "exact admin wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def bulk(bytes data) -> bytes:
    return b"$" + bytes(str(len(data))) + b"\r\n" + data + b"\r\n"

def rejects_options(redis.Connection conn):
    auto errors = 0
    try:
        conn.client_list(client_id=[0])
    except ValueError:
        errors = errors + 1
    try:
        conn.client_kill(b"")
    except ValueError:
        errors = errors + 1
    try:
        conn.client_kill_filter()
    except ValueError:
        errors = errors + 1
    try:
        conn.client_kill_filter(_id=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.client_kill_filter(maxage=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.slowlog_get(num=Optional.Some(-2))
    except ValueError:
        errors = errors + 1
    try:
        conn.memory_usage(b"k", samples=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    check(errors == 7, "invalid admin options are rejected before I/O")

def info(redis.Connection conn, bytes binary):
    auto result = conn.info(section=b"server", sections=[b"stats"])
    check(len(result.sections) == 3 and result.sections[0].name == b"Server" and result.sections[1].name == b"Stats", "ordered sections")
    auto fields = result.sections[0].fields
    check(len(fields) == 4 and fields[0].name == b"redis_version", "ordered server fields")
    match fields[0].value:
        case redis.InfoValue.Text(raw):
            check(raw == b"7.4.12", "version stays text")
        case _:
            raise ValueError("expected INFO text")
    match fields[1].value:
        case redis.InfoValue.Text(raw):
            check(raw == b"a:b:" + binary, "first colon only and binary values")
        case _:
            raise ValueError("expected INFO text")
    match fields[2].value:
        case redis.InfoValue.Integer(value):
            check(value == -3, "signed INFO integer")
        case _:
            raise ValueError("expected INFO integer")
    match fields[3].value:
        case redis.InfoValue.Text(raw):
            check(raw == b"18446744073709551615", "out-of-range integers preserve exact bytes")
        case _:
            raise ValueError("expected INFO large integer as text")
    auto stats = result.sections[1].fields
    match stats[0].value:
        case redis.InfoValue.Decimal(value):
            check(value == 0.125, "typed INFO decimal")
        case _:
            raise ValueError("expected INFO decimal")
    match stats[1].value:
        case redis.InfoValue.Attributes(values):
            check(len(values) == 3 and values[0].key == b"host" and values[0].value == b"::1" and values[2].value == b"", "compound INFO attributes")
        case _:
            raise ValueError("expected INFO attributes")
    check(result.sections[2].name == b"Future Module" and len(result.sections[2].fields) == 1, "unknown sections and keys retained")
    check(len(conn.info().sections) == 0, "empty INFO")

def config_and_flush(redis.Connection conn, bytes binary):
    auto pairs = conn.config_get(pattern=b"max*", patterns=[b"timeout"])
    check(len(pairs) == 2 and pairs[0].key == b"timeout" and pairs[0].value == b"0" and pairs[1].value == binary, "CONFIG GET pairs")
    check(len(conn.config_get()) == 0, "default CONFIG GET pattern")
    check(conn.config_set(name=b"mode", value=b"", additional=[redis.KeyValue(b"other", binary)]), "CONFIG SET multiple pairs")
    check(conn.config_resetstat() and conn.dbsize() == 8, "stats reset and key count")
    check(conn.flushdb() and conn.flushdb(asynchronous=True), "FLUSHDB explicit SYNC and ASYNC")
    check(conn.flushall() and conn.flushall(asynchronous=True), "FLUSHALL explicit SYNC and ASYNC")

def clients(redis.Connection conn):
    check(conn.client_id() == 77 and conn.client_getname().is_none(), "CLIENT ID and missing name")
    check(conn.client_setname(b"worker=1") and conn.client_getname().unwrap() == b"worker=1", "CLIENT name roundtrip")
    auto clients = conn.client_list(_type=b"normal", client_id=[77, 88])
    check(len(clients) == 1 and clients[0].id == 77 and clients[0].fields[0].value == b"worker=1" and clients[0].fields[3].value == b"", "CLIENT field order, empty values, first equals")
    check(len(conn.client_list()) == 0, "empty CLIENT LIST")
    check(conn.client_kill(b"127.0.0.1:1"), "legacy CLIENT KILL")
    check(conn.client_kill_filter(_id=Optional.Some(88), _type=b"normal", addr=b"ip:1", skipme=Optional.Some(False), laddr=b"ip:2", user=b"test", maxage=Optional.Some(0)) == 1, "CLIENT KILL filters")
    check(conn.client_kill_filter(_id=Optional.Some(88), skipme=Optional.Some(True)) == 0, "CLIENT KILL no matches")
    check(conn.client_no_evict(b"ON") and conn.client_no_evict(b"OFF"), "CLIENT NO-EVICT toggles")

def diagnostics(redis.Connection conn, bytes binary):
    auto rows = conn.slowlog_get()
    check(len(rows) == 2 and rows[0].id == 8 and rows[0].timestamp == 10000 and rows[0].duration_us == 999, "typed SLOWLOG counters")
    check(rows[0].args[0] == b"SET" and rows[0].args[1] == binary and rows[0].args[2] == b"", "SLOWLOG arguments preserve bytes and order")
    check(rows[0].client_address.unwrap() == b"127.0.0.1:1" and rows[0].client_name.unwrap() == b"" and rows[0].argument_count.unwrap() == 5, "SLOWLOG empty name")
    check(rows[1].client_address.is_none() and rows[1].client_name.is_none() and rows[1].argument_count.is_none(), "old SLOWLOG rows omit client metadata")
    check(len(conn.slowlog_get(num=Optional.Some(-1))) == 0 and conn.slowlog_reset(), "all entries and reset")
    check(conn.memory_usage(binary, samples=Optional.Some(0)).unwrap() == 1024 and conn.memory_usage(b"absent").is_none(), "MEMORY USAGE zero samples and nil")
    check(conn.command_count() == 100, "COMMAND COUNT")
    auto docs = conn.command_docs(names=[b"get", b"unknown"])
    check(len(docs) == 1 and docs[0].name == b"get" and docs[0].fields[1].name == b"arguments", "named COMMAND DOCS fields")
    match docs[0].fields[1].value:
        case redis.Reply.Array(items):
            check(len(items) == 0, "COMMAND DOCS nested metadata")
        case _:
            raise ValueError("expected metadata array")
    check(len(conn.command_docs()) == 0, "COMMAND DOCS no command filter")

def reject_replies(redis.Connection conn):
    auto errors = 0
    try:
        conn.info()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.config_get()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.dbsize()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.flushdb()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.client_list()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.slowlog_get()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.memory_usage(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.command_docs()
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.config_resetstat()
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"NOPERM":
            errors = errors + 1
    check(errors == 9 and conn.ping(), "malformed replies and permission errors leave the connection usable")

def main():
    auto binary = bytes([0, 255, 13, 128])
    auto report = b"# Server\r\nredis_version:7.4.12\r\npath:a:b:" + binary + b"\nneg:-3\r\nbig:18446744073709551615\r\n\r\n# Stats\r\nfraction:1.25e-1\r\nprimary:host=::1,port=1,name=\r\n# Future Module\r\nnew:unknown\r\n"
    auto slowlog = b"*2\r\n*7\r\n:8\r\n:10000\r\n:999\r\n*3\r\n" + bulk(b"SET") + bulk(binary) + bulk(b"") + bulk(b"127.0.0.1:1") + bulk(b"") + b":5\r\n*4\r\n:7\r\n:9999\r\n:1\r\n*1\r\n" + bulk(b"PING")
    list[Step] steps = [
        Step([b"INFO", b"server", b"stats"], bulk(report)),
        Step([b"INFO"], bulk(b"")),
        Step([b"CONFIG", b"GET", b"max*", b"timeout"], b"*4\r\n" + bulk(b"timeout") + bulk(b"0") + bulk(b"max") + bulk(binary)),
        Step([b"CONFIG", b"GET", b"*"], b"*0\r\n"),
        Step([b"CONFIG", b"SET", b"mode", b"", b"other", binary], b"+OK\r\n"),
        Step([b"CONFIG", b"RESETSTAT"], b"+OK\r\n"),
        Step([b"DBSIZE"], b":8\r\n"),
        Step([b"FLUSHDB", b"SYNC"], b"+OK\r\n"),
        Step([b"FLUSHDB", b"ASYNC"], b"+OK\r\n"),
        Step([b"FLUSHALL", b"SYNC"], b"+OK\r\n"),
        Step([b"FLUSHALL", b"ASYNC"], b"+OK\r\n"),
        Step([b"CLIENT", b"ID"], b":77\r\n"),
        Step([b"CLIENT", b"GETNAME"], b"$-1\r\n"),
        Step([b"CLIENT", b"SETNAME", b"worker=1"], b"+OK\r\n"),
        Step([b"CLIENT", b"GETNAME"], bulk(b"worker=1")),
        Step([b"CLIENT", b"LIST", b"TYPE", b"normal", b"ID", b"77", b"88"], bulk(b"name=worker=1 addr=[::1]:1 id=77 user=\n")),
        Step([b"CLIENT", b"LIST"], bulk(b"")),
        Step([b"CLIENT", b"KILL", b"127.0.0.1:1"], b"+OK\r\n"),
        Step([b"CLIENT", b"KILL", b"ID", b"88", b"TYPE", b"normal", b"ADDR", b"ip:1", b"SKIPME", b"NO", b"LADDR", b"ip:2", b"USER", b"test", b"MAXAGE", b"0"], b":1\r\n"),
        Step([b"CLIENT", b"KILL", b"ID", b"88", b"SKIPME", b"YES"], b":0\r\n"),
        Step([b"CLIENT", b"NO-EVICT", b"ON"], b"+OK\r\n"),
        Step([b"CLIENT", b"NO-EVICT", b"OFF"], b"+OK\r\n"),
        Step([b"SLOWLOG", b"GET"], slowlog),
        Step([b"SLOWLOG", b"GET", b"-1"], b"*0\r\n"),
        Step([b"SLOWLOG", b"RESET"], b"+OK\r\n"),
        Step([b"MEMORY", b"USAGE", binary, b"SAMPLES", b"0"], b":1024\r\n"),
        Step([b"MEMORY", b"USAGE", b"absent"], b"$-1\r\n"),
        Step([b"COMMAND", b"COUNT"], b":100\r\n"),
        Step([b"COMMAND", b"DOCS", b"get", b"unknown"], b"*2\r\n" + bulk(b"get") + b"*4\r\n" + bulk(b"summary") + bulk(b"read a key") + bulk(b"arguments") + b"*0\r\n"),
        Step([b"COMMAND", b"DOCS"], b"*0\r\n"),
        Step([b"INFO"], b"+OK\r\n"),
        Step([b"CONFIG", b"GET", b"*"], b"*1\r\n" + bulk(b"bad")),
        Step([b"DBSIZE"], b":-1\r\n"),
        Step([b"FLUSHDB", b"SYNC"], b"+PONG\r\n"),
        Step([b"CLIENT", b"LIST"], bulk(b"id=nope addr=x\n")),
        Step([b"SLOWLOG", b"GET"], b"*1\r\n*0\r\n"),
        Step([b"MEMORY", b"USAGE", b"k"], b":-1\r\n"),
        Step([b"COMMAND", b"DOCS"], b"*2\r\n" + bulk(b"get") + b"+OK\r\n"),
        Step([b"CONFIG", b"RESETSTAT"], b"-NOPERM access denied\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        rejects_options(conn)
        info(conn, binary)
        config_and_flush(conn, binary)
        clients(conn)
        diagnostics(conn, binary)
        reject_replies(conn)
    finally:
        conn.disconnect()
    print("Server administration wire tests: PASS")
