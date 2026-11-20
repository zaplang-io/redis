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
                "exact Functions wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def bulk(bytes data) -> bytes:
    return b"$" + bytes(str(len(data))) + b"\r\n" + data + b"\r\n"

def reply_array(list[bytes] parts) -> bytes:
    auto result = b"*" + bytes(str(len(parts))) + b"\r\n"
    for part in parts:
        result = result + part
    return result

def success(redis.Connection conn, bytes binary):
    check(conn.function_load(code=b"source") == b"fixture", "load name")
    check(conn.function_load(b"new source", replace=True) == b"fixture", "replace")
    check(conn.function_delete(library=b"old"), "delete status")
    auto libraries = conn.function_list(library=b"f*", withcode=True)
    check(len(libraries) == 1 and libraries[0].name == b"fixture" and libraries[0].engine == b"LUA", "typed library and engine")
    check(libraries[0].code.unwrap() == binary and len(libraries[0].functions) == 2, "withcode preserves bytes")
    auto functions = libraries[0].functions
    check(functions[0].description.is_none() and functions[0].flags[0] == b"no-writes", "nullable description and flags")
    check(functions[1].description.unwrap() == b"" and len(functions[1].flags) == 0, "empty description is present")
    check(len(conn.function_list()) == 0, "empty list")
    check(conn.function_dump() == binary, "binary dump")
    check(conn.function_flush() and conn.function_flush("async"), "flush modes")
    auto stats = conn.function_stats()
    check(stats.running.is_none() and len(stats.engines) == 1, "idle engine stats")
    check(stats.engines[0].engine == b"LUA" and stats.engines[0].libraries_count == 2 and stats.engines[0].functions_count == 3, "engine counters")
    auto busy = conn.function_stats().running.unwrap()
    check(busy.name == b"read" and busy.duration_ms == 123 and busy.command[0] == b"FCALL_RO" and busy.command[3] == binary, "running function command bytes")
    match conn.fcall(function=b"write", keys=[binary, b""], arguments=[b"", binary]):
        case redis.Reply.Integer(number):
            check(number == 2, "FCALL reply")
        case _:
            raise ValueError("expected integer")
    match conn.fcall_ro(b"read", [], [binary]):
        case redis.Reply.NullBulk:
            auto ignored = 0
        case _:
            raise ValueError("expected nil")

def errors(redis.Connection conn):
    auto rejected = 0
    try:
        conn.function_flush("invalid")
    except ValueError:
        rejected = rejected + 1
    try:
        conn.function_load(b"bad")
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"ERR":
            rejected = rejected + 1
    try:
        conn.function_list()
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.function_stats()
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.function_dump()
    except redis.ProtocolError:
        rejected = rejected + 1
    try:
        conn.function_delete(b"fixture")
    except redis.ProtocolError:
        rejected = rejected + 1
    check(rejected == 6 and conn.ping(), "function errors preserve connection")

def main():
    auto binary = bytes([255, 0, 13, 10])
    auto read = reply_array([bulk(b"name"), bulk(b"read"), bulk(b"description"), b"$-1\r\n", bulk(b"flags"), reply_array([bulk(b"no-writes")])])
    auto write = reply_array([bulk(b"name"), bulk(b"write"), bulk(b"description"), bulk(b""), bulk(b"flags"), b"*0\r\n"])
    auto libraries = reply_array([reply_array([bulk(b"engine"), bulk(b"LUA"), bulk(b"library_name"), bulk(b"fixture"), bulk(b"functions"), reply_array([read, write]), bulk(b"library_code"), bulk(binary)])])
    auto stats = reply_array([bulk(b"LUA"), reply_array([bulk(b"libraries_count"), b":2\r\n", bulk(b"functions_count"), b":3\r\n"])])
    auto running = reply_array([bulk(b"name"), bulk(b"read"), bulk(b"duration_ms"), b":123\r\n", bulk(b"command"), reply_array([bulk(b"FCALL_RO"), bulk(b"read"), bulk(b"0"), bulk(binary)])])
    list[Step] steps = [
        Step([b"FUNCTION", b"LOAD", b"source"], bulk(b"fixture")),
        Step([b"FUNCTION", b"LOAD", b"REPLACE", b"new source"], bulk(b"fixture")),
        Step([b"FUNCTION", b"DELETE", b"old"], b"+OK\r\n"),
        Step([b"FUNCTION", b"LIST", b"LIBRARYNAME", b"f*", b"WITHCODE"], libraries),
        Step([b"FUNCTION", b"LIST", b"LIBRARYNAME", b"*"], b"*0\r\n"),
        Step([b"FUNCTION", b"DUMP"], bulk(binary)),
        Step([b"FUNCTION", b"FLUSH", b"SYNC"], b"+OK\r\n"),
        Step([b"FUNCTION", b"FLUSH", b"ASYNC"], b"+OK\r\n"),
        Step([b"FUNCTION", b"STATS"], reply_array([bulk(b"running_script"), b"$-1\r\n", bulk(b"engines"), stats])),
        Step([b"FUNCTION", b"STATS"], reply_array([bulk(b"engines"), stats, bulk(b"running_script"), running])),
        Step([b"FCALL", b"write", b"2", binary, b"", b"", binary], b":2\r\n"),
        Step([b"FCALL_RO", b"read", b"0", binary], b"$-1\r\n"),
        Step([b"FUNCTION", b"LOAD", b"bad"], b"-ERR Bad library data\r\n"),
        Step([b"FUNCTION", b"LIST", b"LIBRARYNAME", b"*"], reply_array([reply_array([bulk(b"library_name"), bulk(b"fixture")])])),
        Step([b"FUNCTION", b"STATS"], reply_array([bulk(b"running_script"), b"$-1\r\n", bulk(b"engines"), reply_array([bulk(b"LUA"), reply_array([bulk(b"libraries_count"), b":-1\r\n", bulk(b"functions_count"), b":3\r\n"])])])),
        Step([b"FUNCTION", b"DUMP"], b"$-1\r\n"),
        Step([b"FUNCTION", b"DELETE", b"fixture"], b"+PONG\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        success(conn, binary)
        errors(conn)
    finally:
        conn.disconnect()
    print("Functions wire tests: PASS")
