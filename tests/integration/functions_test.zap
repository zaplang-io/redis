import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def bulk_values(redis.Reply reply) -> list[bytes]:
    match reply:
        case redis.Reply.Array(items):
            list[bytes] values = []
            for item in items:
                match item:
                    case redis.Reply.Bulk(data):
                        values.append(data)
                    case _:
                        raise ValueError("Expected bulk function result")
            return values
        case _:
            raise ValueError("Expected function result array")

def release_fixture(redis.Connection conn, bytes library):
    if len(conn.function_list(library)) > 0:
        conn.function_delete(library)

def listed(redis.Connection conn, bytes library, bytes source, bytes read, bytes write):
    auto bare = conn.function_list(library)
    auto detailed = conn.function_list(library, withcode=True)
    check(len(bare) == 1 and len(detailed) == 1 and bare[0].name == library and bare[0].engine == b"LUA", "typed function library")
    check(bare[0].code.is_none() and detailed[0].code.unwrap() == source, "WITHCODE includes original source")
    auto found = 0
    for item in detailed[0].functions:
        if item.name == read:
            check(item.description.unwrap() == b"fixture read" and len(item.flags) == 1 and item.flags[0] == b"no-writes", "read function metadata")
            found = found + 1
        if item.name == write:
            check(item.description.is_none() and len(item.flags) == 0, "write function metadata")
            found = found + 1
    check(found == 2 and len(detailed[0].functions) == 2, "both fixture functions listed")
    auto stats = conn.function_stats()
    check(stats.running.is_none(), "idle server")
    auto engine_found = False
    for engine in stats.engines:
        if engine.engine == b"LUA":
            engine_found = engine.libraries_count >= 1 and engine.functions_count >= 2
    check(engine_found, "Lua stats count fixture library and functions")

def functions(redis.Connection conn, integration_fixtures.Fixture f):
    auto prefix = bytes("zap_fn_" + crypto.token_hex(8))
    auto read = prefix + b"_read"
    auto write = prefix + b"_write"
    auto library = prefix + b"_lib"
    auto source = b"#!lua name=" + library + b"\nredis.register_function{function_name='" + read + b"',callback=function(keys,args) return {keys[1] or '', keys[2] or '', args[1], args[2]} end,flags={'no-writes'},description='fixture read'}\nredis.register_function('" + write + b"',function(keys,args) redis.call('SET',keys[1],args[1],'PX',60000); return args[1] end)\n"
    auto key = f.binary_key(bytes([0, 255, 13, 10]))
    auto other = f.key("unused-key")
    auto binary = bytes([255, 0, 128])
    try:
        check(conn.function_load(source) == library, "load fixture library")
        auto duplicate = False
        try:
            conn.function_load(source)
        except redis.ResponseError:
            duplicate = True
        check(duplicate and conn.function_load(source, replace=True) == library, "duplicate fails and REPLACE succeeds")
        listed(conn, library, source, read, write)
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            listed(resp3, library, source, read, write)
            auto values = bulk_values(resp3.fcall_ro(read, [key, other], [binary, b""]))
            check(len(values) == 4 and values[0] == key and values[1] == other and values[2] == binary and values[3] == b"", "binary keys and arguments on FCALL_RO")
        finally:
            resp3.disconnect()
        auto values = bulk_values(conn.fcall(read, [], [binary, b""]))
        check(len(values) == 4 and values[0] == b"" and values[1] == b"" and values[2] == binary, "FCALL derives zero keys")
        auto readonly = False
        try:
            conn.fcall_ro(write, [key], [binary])
        except redis.ResponseError:
            readonly = True
        check(readonly and conn.get(key).is_none(), "read-only mode rejects write without side effects")
        integration_fixtures.bulk(conn.fcall(write, [key], [binary]), binary)
        check(conn.get(key).unwrap() == binary, "FCALL write")
        check(len(conn.function_dump()) > 0, "binary function dump")
        check(conn.function_delete(library) and len(conn.function_list(library)) == 0, "delete fixture")
        auto missing = False
        try:
            conn.fcall(read, [], [b"a", b"b"])
        except redis.ResponseError:
            missing = True
        check(missing and conn.ping(), "missing function preserves connection")
    finally:
        release_fixture(conn, library)

def flush(redis.Connection conn):
    check(len(conn.function_list()) == 0, "flush fixture requires empty library registry")
    auto library = bytes("zap_flush_" + crypto.token_hex(8))
    auto source = b"#!lua name=" + library + b"\nredis.register_function('" + library + b"',function(keys,args) return 1 end)\n"
    try:
        conn.function_load(source)
        check(conn.function_flush() and len(conn.function_list(library)) == 0, "synchronous flush")
        conn.function_load(source)
        check(conn.function_flush("ASYNC") and len(conn.function_list(library)) == 0, "asynchronous flush")
    finally:
        release_fixture(conn, library)

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        functions(conn, f)
        if os.getenv("REDIS_TEST_DISPOSABLE_ADMIN") == "1":
            flush(conn)
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis Functions: PASS")
