import redis
import integration_fixtures

# This suite creates an ephemeral ACL user and uses a second database. Run it
# only against an isolated test instance with administrative permissions.
def main():
    integration_fixtures.check(
        os.getenv("REDIS_TEST_ISOLATED") == "1",
        "isolated instance required",
    )
    auto first = integration_fixtures.fixture()
    auto second = integration_fixtures.other_database(first)
    auto admin = first.open()
    auto username = "zap_test_" + crypto.token_hex(16)
    auto password = crypto.token_hex(24)
    auto created = False
    try:
        first.verify_version(admin)
        auto key = first.key("database-isolation")
        auto other_key = second.key("database-isolation")
        integration_fixtures.check(
            key == other_key,
            "same binary key in both databases",
        )
        first.put(admin, key, b"first database")
        auto other = second.open()
        try:
            integration_fixtures.missing(other.execute([b"GET", other_key]))
            second.put(other, other_key, b"second database")
            integration_fixtures.bulk(admin.execute([b"GET", key]), b"first database")
            integration_fixtures.bulk(
                other.execute([b"GET", other_key]),
                b"second database",
            )
            other.reconnect()
            integration_fixtures.bulk(
                other.execute([b"GET", other_key]),
                b"second database",
            )
        finally:
            other.disconnect()

        # Limit the generated user to this run's namespace and explicit commands.
        integration_fixtures.simple(
            admin.execute(
                [
                    b"ACL",
                    b"SETUSER",
                    bytes(username),
                    b"reset",
                    b"on",
                    bytes(">" + password),
                    bytes("~" + first.prefix + "*"),
                    b"+ping",
                    b"+echo",
                    b"+get",
                    b"+set",
                    b"+del",
                    b"+select",
                    b"+auth",
                    b"+client|setname",
                    b"+client|getname",
                    b"+acl|whoami",
                ],
            ),
            b"OK",
        )
        auto cfg = redis.options(
            first.config.host,
            first.config.port,
            first.config.database,
            username,
            password,
            "zap-integration-client",
        )
        auto user = redis.connect_options(cfg)
        try:
            integration_fixtures.bulk(
                user.execute([b"ACL", b"WHOAMI"]),
                bytes(username),
            )
            integration_fixtures.bulk(
                user.execute([b"CLIENT", b"GETNAME"]),
                b"zap-integration-client",
            )
            integration_fixtures.bulk(user.execute([b"GET", key]), b"first database")
            user.reconnect()
            integration_fixtures.bulk(
                user.execute([b"ACL", b"WHOAMI"]),
                bytes(username),
            )
            integration_fixtures.bulk(
                user.execute([b"CLIENT", b"GETNAME"]),
                b"zap-integration-client",
            )
            integration_fixtures.bulk(user.execute([b"GET", key]), b"first database")
        finally:
            user.disconnect()

        auto rejected = False
        try:
            redis.connect(
                first.config.host,
                first.config.port,
                username=username,
                password="incorrect",
            )
        except redis.ResponseError as message:
            rejected = message == "Redis session initialization failed: AUTH"
        integration_fixtures.check(rejected, "real failed ACL authentication")
        integration_fixtures.simple(admin.execute([b"PING"]), b"PONG")
    finally:
        try:
            if created:
                admin.execute([b"ACL", b"DELUSER", bytes(username)])
        finally:
            admin.disconnect()
            try:
                first.cleanup()
            finally:
                second.cleanup()
    auto verifier = first.open()
    try:
        integration_fixtures.missing(
            verifier.execute([b"ACL", b"GETUSER", bytes(username)]),
        )
    finally:
        verifier.disconnect()
    print("Live Redis sessions: PASS")
