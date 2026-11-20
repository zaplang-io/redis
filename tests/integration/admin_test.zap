import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def pair_value(list[redis.KeyValue] pairs, bytes name) -> bytes:
    for pair in pairs:
        if pair.key == name:
            return pair.value
    raise ValueError("Missing key/value field " + str(name))

def info_field(redis.ServerInfo result, bytes section_name, bytes name) -> redis.InfoValue:
    for section in result.sections:
        if section.name == section_name:
            for field in section.fields:
                if field.name == name:
                    return field.value
    raise ValueError("Missing INFO field " + str(name))

def count_commands(redis.Connection conn) -> int64:
    match info_field(conn.info(b"stats"), b"Stats", b"total_commands_processed"):
        case redis.InfoValue.Integer(value):
            return value
        case _:
            raise ValueError("Expected INFO total_commands_processed integer")

def diagnostics(redis.Connection conn, integration_fixtures.Fixture f, bytes key):
    auto report = conn.info(b"server", [b"stats"])
    match info_field(report, b"Server", b"redis_version"):
        case redis.InfoValue.Text(version):
            auto expected = os.getenv("REDIS_TEST_SERVER_VERSION")
            check(len(version) > 0 and (expected == "" or version == bytes(expected)), "typed server version")
        case _:
            raise ValueError("Expected INFO version bytes")
    check(len(conn.info(b"zap_no_such_section").sections) == 0, "unknown INFO section")
    check(count_commands(conn) > 0 and conn.dbsize() >= 0, "INFO counters and DBSIZE")
    auto options = conn.config_get(b"databases", [b"port"])
    check(len(options) == 2 and len(pair_value(options, b"port")) > 0, "multi-pattern CONFIG GET")
    check(len(conn.config_get(b"zap_no_such_config_key")) == 0, "missing CONFIG match")
    check(conn.memory_usage(key).is_none(), "missing MEMORY USAGE")
    f.put(conn, key, bytes([0, 255, 13, 10]))
    check(conn.memory_usage(key, samples=Optional.Some(0)).unwrap() > 0, "MEMORY USAGE binary key")
    check(conn.command_count() > 0, "COMMAND COUNT positive")
    auto docs = conn.command_docs([b"get", b"zap_unknown_command"])
    check(len(docs) == 1 and docs[0].name == b"get", "COMMAND DOCS omits unknown commands")
    auto summary = False
    for field in docs[0].fields:
        if field.name == b"summary":
            match field.value:
                case redis.Reply.Bulk(data):
                    summary = len(data) > 0
                case redis.Reply.Simple(data):
                    summary = len(data) > 0
                case _:
                    raise ValueError("Expected COMMAND DOCS summary")
    check(summary, "COMMAND DOCS named metadata")

def client_metadata(redis.Connection conn):
    auto name = b"zap=admin"
    auto id = conn.client_id()
    auto old = conn.client_getname()
    try:
        check(conn.client_setname(name), "set current connection name")
        check(conn.client_getname().unwrap() == name, "read current connection name")
        auto listed = conn.client_list(client_id=[id])
        check(len(listed) == 1 and listed[0].id == id and pair_value(listed[0].fields, b"name") == name, "CLIENT LIST filtered by ID")
        check(len(conn.client_list(client_id=[9223372036854775807])) == 0, "unknown client list ID")
        check(conn.client_setname(b"") and conn.client_getname().is_none(), "clear connection name")
        auto denied = False
        try:
            conn.client_setname(b"contains space")
        except redis.ResponseError:
            denied = True
        check(denied and conn.client_getname().is_none(), "invalid name leaves current name unchanged")
        check(conn.client_no_evict(b"ON") and conn.client_no_evict(b"OFF"), "toggle eviction protection for this client")
    finally:
        if old.is_some():
            conn.client_setname(old.unwrap())
        else:
            conn.client_setname(b"")

def flush_scopes(redis.Connection admin, integration_fixtures.Fixture f):
    auto other = integration_fixtures.other_database(f)
    auto c = other.open()
    try:
        check(admin.dbsize() == 0 and c.dbsize() == 0, "flush fixture requires two empty databases")
        auto key = f.key("flush-scope")
        f.put(admin, key, b"base")
        other.put(c, key, b"other")
        check(admin.dbsize() == 1 and c.dbsize() == 1, "separate databases contain the fixture key")
        check(admin.flushdb() and admin.dbsize() == 0 and c.get(key).unwrap() == b"other", "SYNC FLUSHDB affects one database")
        f.put(admin, key, b"base")
        check(c.flushdb(asynchronous=True) and c.dbsize() == 0 and admin.get(key).unwrap() == b"base", "ASYNC FLUSHDB affects one database")
        other.put(c, key, b"other")
        check(admin.flushall() and admin.dbsize() == 0 and c.dbsize() == 0, "SYNC FLUSHALL clears both databases")
        f.put(admin, key, b"base")
        other.put(c, key, b"other")
        check(c.flushall(asynchronous=True) and admin.dbsize() == 0 and c.dbsize() == 0, "ASYNC FLUSHALL clears both databases")
    finally:
        c.disconnect()

def slowlog(redis.Connection admin, integration_fixtures.Fixture f):
    auto old = admin.config_get(b"slowlog-log-slower-than", [b"slowlog-max-len"])
    auto threshold = pair_value(old, b"slowlog-log-slower-than")
    auto max_len = pair_value(old, b"slowlog-max-len")
    auto key = f.binary_key(bytes([128, 0, 255]))
    try:
        check(admin.config_set(b"slowlog-log-slower-than", b"0", [redis.KeyValue(b"slowlog-max-len", b"32")]), "two CONFIG SET parameters")
        auto current = admin.config_get(b"slowlog-log-slower-than", [b"slowlog-max-len"])
        check(pair_value(current, b"slowlog-log-slower-than") == b"0" and pair_value(current, b"slowlog-max-len") == b"32", "configuration roundtrip")
        check(admin.client_setname(b"zap-admin-slowlog"), "set slowlog client name")
        # DEBUG is enabled only on this disposable CI service.
        integration_fixtures.simple(admin.execute([b"DEBUG", b"SLEEP", b"0.01"]), b"OK")
        f.put(admin, key, b"fixture")
        auto logs = admin.slowlog_get(num=Optional.Some(-1))
        auto found = False
        for row in logs:
            if len(row.args) > 1 and row.args[1] == key:
                found = True
                check(row.client_name.unwrap() == b"zap-admin-slowlog" and row.timestamp > 0, "SLOWLOG returns client and timing metadata")
                if row.argument_count.is_some():
                    check(row.argument_count.unwrap() == len(row.args), "original argument count for an untruncated command")
        check(found and len(admin.slowlog_get(num=Optional.Some(0))) == 0, "SLOWLOG records the fixture write")
        check(admin.slowlog_reset(), "SLOWLOG RESET succeeds")
        for row in admin.slowlog_get():
            check(len(row.args) < 2 or row.args[1] != key, "reset removes previous fixture row")
        auto before = count_commands(admin)
        check(admin.config_resetstat() and count_commands(admin) < before, "CONFIG RESETSTAT resets counters")
    finally:
        admin.config_set(b"slowlog-log-slower-than", threshold, [redis.KeyValue(b"slowlog-max-len", max_len)])
        admin.client_setname(b"")
    auto restored = admin.config_get(b"slowlog-log-slower-than", [b"slowlog-max-len"])
    check(pair_value(restored, b"slowlog-log-slower-than") == threshold and pair_value(restored, b"slowlog-max-len") == max_len, "configuration restored")

def target_kill(redis.Connection admin, integration_fixtures.Fixture f):
    auto target = f.open()
    auto legacy = f.open()
    try:
        auto id = target.client_id()
        check(admin.client_kill_filter(_id=Optional.Some(id), skipme=Optional.Some(True)) == 1, "filter kills the fixture client")
        check(admin.client_kill_filter(_id=Optional.Some(id)) == 0, "repeat filter matches nobody")
        auto gone = False
        try:
            target.ping()
        except redis.ConnectionError:
            gone = True
        check(gone and admin.ping(), "only the targeted client closed")
        auto clients = admin.client_list(client_id=[legacy.client_id()])
        check(len(clients) == 1 and admin.client_kill(pair_value(clients[0].fields, b"addr")), "legacy kill by fixture address")
        gone = False
        try:
            legacy.ping()
        except redis.ConnectionError:
            gone = True
        check(gone and admin.ping(), "legacy kill closed only the fixture client")
    finally:
        try:
            target.disconnect()
        finally:
            legacy.disconnect()

def acl_denial(redis.Connection admin, integration_fixtures.Fixture f):
    auto username = "zap_admin_" + crypto.token_hex(16)
    auto password = crypto.token_hex(24)
    try:
        integration_fixtures.simple(admin.execute([b"ACL", b"SETUSER", bytes(username), b"reset", b"on", bytes(">" + password), b"+ping", b"+select"]), b"OK")
        auto cfg = redis.options(f.config.host, f.config.port, f.config.database, username, password)
        auto limited = redis.connect_options(cfg)
        try:
            auto denied = 0
            try:
                limited.config_get()
            except redis.ResponseError:
                check(redis.error_prefix(limited.last_error()) == b"NOPERM", "CONFIG ACL denial")
                denied = denied + 1
            try:
                limited.flushall()
            except redis.ResponseError:
                check(redis.error_prefix(limited.last_error()) == b"NOPERM", "FLUSHALL ACL denial")
                denied = denied + 1
            check(denied == 2 and limited.ping(), "ACL denial preserves connection")
        finally:
            limited.disconnect()
    finally:
        admin.execute([b"ACL", b"DELUSER", bytes(username)])

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        if os.getenv("REDIS_TEST_DISPOSABLE_ADMIN") == "1":
            flush_scopes(conn, f)
            slowlog(conn, f)
            target_kill(conn, f)
            acl_denial(conn, f)
        diagnostics(conn, f, f.binary_key(bytes([255, 0])))
        client_metadata(conn)
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            diagnostics(resp3, f, f.binary_key(bytes([255, 1])))
            client_metadata(resp3)
        finally:
            resp3.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis server administration: PASS")
