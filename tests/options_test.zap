import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def invalid(str address):
    auto rejected = False
    try:
        redis.parse_url(address)
    except ValueError as e:
        check(e.find("SECRET") < 0, "URL diagnostics redact credentials")
        rejected = True
    check(rejected, "invalid URL was rejected")

def main():
    auto defaults = redis.options()
    check(defaults.host == "127.0.0.1", "default host")
    check(defaults.port == 6379, "default port")
    check(defaults.database == 0, "default database")
    check(not defaults.authenticate, "no implicit default authentication")
    check(
        redis.describe_options(defaults) == "redis://127.0.0.1:6379/0",
        "default description",
    )
    auto encrypted = redis.parse_url("rediss://:password@localhost:6380/2")
    check(encrypted.secure, "rediss selects TLS")
    check(
        redis.describe_options(encrypted) == "rediss://***@localhost:6380/2",
        "rediss description",
    )
    check(not defaults.secure, "TCP remains the default")
    auto empty = redis.parse_url("redis://")
    check(empty.host == defaults.host, "URL default host")
    check(empty.port == 6379, "URL default port")
    auto cfg = redis.parse_url(
        "redis://alice%40x:p%3Ass%2F%25%2B@localhost:6380/2?db=3&client_name=worker%2D1",
    )
    check(cfg.host == "localhost", "URL hostname")
    check(cfg.port == 6380, "URL port")
    check(cfg.username == "alice@x", "decoded username")
    check(cfg.password == "p:ss/%+", "decoded password")
    check(cfg.database == 3, "query database overrides path")
    check(cfg.client_name == "worker-1", "decoded client name")
    check(cfg.authenticate, "URL authentication enabled")
    check(
        redis.describe_options(cfg) == "redis://***@localhost:6380/3",
        "redacted description",
    )
    auto ipv6 = redis.parse_url("redis://[::1]:6381/4")
    check(ipv6.host == "::1", "IPv6 brackets removed")
    check(ipv6.port == 6381, "IPv6 port")
    check(redis.describe_options(ipv6) == "redis://[::1]:6381/4", "IPv6 description")
    auto utf8 = redis.parse_url("redis://u%C3%A9:p+q@localhost")
    check(utf8.username == "ué", "UTF-8 percent decoding")
    check(utf8.password == "p+q", "plus remains literal")
    auto password = redis.parse_url("redis://:password@localhost/1")
    check(password.username == "", "password-only authentication")
    check(password.password == "password", "password-only value")
    auto empty_password = redis.parse_url("redis://:@localhost")
    check(empty_password.authenticate, "explicit empty-password authentication")
    check(empty_password.password == "", "empty password preserved")
    check(redis.parse_url("redis:///4").database == 4, "database with default endpoint")
    check(
        redis.parse_url("redis://localhost/%32?%64b=03").database == 3,
        "encoded database and query key",
    )
    list[str] bad = [
        "",
        "http://localhost",
        "redisx://localhost",
        "redis://localhost#fragment",
        "redis://user:SECRET@localhost:0",
        "redis://user:SECRET@localhost:65536",
        "redis://user:SECRET@localhost:-1",
        "redis://user:SECRET@localhost:",
        "redis://user:SECRET@localhost:99999999999999999999999999",
        "redis://localhost/-1",
        "redis://localhost/+1",
        "redis://localhost/2147483648",
        "redis://localhost/1/2",
        "redis://localhost/not-a-db?db=1",
        "redis://localhost?db=1&%64b=2",
        "redis://localhost?db=",
        "redis://localhost?",
        "redis://localhost?unknown=SECRET",
        "redis://localhost?db=1&",
        "redis://localhost?db",
        "redis://user:SECRET%ZZ@localhost",
        "redis://user:SECRET%@localhost",
        "redis://user:SECRET%FF@localhost",
        "redis://user@localhost",
        "redis://user:SECRET@other@localhost",
        "redis://::1",
        "redis://[::1",
        "redis://[localhost]",
        "redis://[::1]extra",
        "redis://[::1]:",
        "redis://bad%2Fhost",
        "redis://bad%3Ahost",
        "redis://bad host",
        "redis://localhost?client_name=bad%20name",
    ]
    for address in bad:
        invalid(address)
    auto rejected = 0
    try:
        redis.options(database=-1)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(database=2147483648)
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(client_name="bad name")
    except ValueError:
        rejected = rejected + 1
    try:
        redis.options(host="user@host")
    except ValueError:
        rejected = rejected + 1
    check(rejected == 4, "invalid direct options")
    print("Connection options and URLs: PASS")
