import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def membership(list[bool] actual, list[bool] expected, str label):
    check(len(actual) == len(expected), label + ": size")
    for i in range(0, len(expected)):
        check(actual[i] == expected[i], label + ": input order")

def multiple_membership_and_cardinality(
    redis.Connection conn,
    integration_fixtures.Fixture f,
):
    auto a = f.binary_key(bytes([0, 13, 10, 255]))
    auto b = f.key("cardinality-b")
    auto missing = f.key("cardinality-missing")
    auto binary = bytes([255, 0, 13, 10])
    conn.sadd(a, [binary, b"", b"shared", b"a"])
    conn.pexpire(a, 60000)
    conn.sadd(b, [binary, b"shared", b"b"])
    conn.pexpire(b, 60000)
    membership(
        conn.smismember(a, [binary, b"absent", b"", binary, b"shared"]),
        [True, False, True, True, True],
        "SMISMEMBER matches binary and empty values and preserves duplicates",
    )
    membership(
        conn.smismember(missing, [b"", binary]),
        [False, False],
        "SMISMEMBER missing key",
    )
    check(conn.sintercard([a, b]) == 2, "SINTERCARD full intersection")
    check(conn.sintercard([a, b], limit=0) == 2, "zero limit is unlimited")
    check(conn.sintercard([a, b], limit=1) == 1, "limit caps result")
    check(conn.sintercard([a, b], limit=10) == 2, "large limit keeps exact count")
    check(conn.sintercard([a, missing]) == 0, "missing input behaves as empty")
    check(conn.sintercard([a, a, b]) == 2, "duplicate input does not duplicate members")
    check(conn.sintercard([a]) == 4, "one input counts the set")
    check(conn.scard(a) == 4 and conn.scard(b) == 3, "queries preserve source contents")

def moves(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("move-a")
    auto b = f.key("move-b")
    auto fresh = f.key("move-fresh")
    auto wrong = f.key("move-wrong")
    auto binary = bytes([128, 0, 255])
    conn.sadd(a, [binary, b"", b"tail"])
    conn.pexpire(a, 60000)
    conn.sadd(b, [binary])
    conn.pexpire(b, 60000)
    auto source_expiry = conn.pexpiretime(a)
    auto dest_expiry = conn.pexpiretime(b)
    check(
        conn.smove(a, a, binary) and conn.scard(a) == 3,
        "SMOVE existing member to same set",
    )
    check(not conn.smove(a, a, b"absent"), "SMOVE missing member to same set")
    check(
        conn.smove(a, b, binary),
        "SMOVE removes from source even if destination contains member",
    )
    check(not conn.smove(a, b, binary), "SMOVE absent member")
    check(conn.scard(a) == 2 and conn.scard(b) == 1, "SMOVE deduplicates target")
    check(
        conn.pexpiretime(a) == source_expiry and conn.pexpiretime(b) == dest_expiry,
        "SMOVE preserves existing expiry",
    )
    check(
        not conn.smove(a, fresh, b"absent") and conn.exists([fresh]) == 0,
        "failed move does not create destination",
    )
    check(
        conn.smove(a, fresh, b"") and conn.sismember(fresh, b""),
        "SMOVE creates target with empty bytes",
    )
    conn.pexpire(fresh, 60000)
    f.put(conn, wrong, b"string")
    auto failed = False
    try:
        conn.smove(a, wrong, b"tail")
    except redis.ResponseError:
        failed = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
    check(failed and conn.ping(), "SMOVE wrong destination leaves connection usable")
    check(
        conn.sismember(a, b"tail") and conn.get(wrong).unwrap() == b"string",
        "failed move preserves both keys",
    )
    check(
        conn.smove(a, b, b"tail") and conn.exists([a]) == 0,
        "moving final member deletes source",
    )
    check(not conn.smove(a, b, b"tail"), "SMOVE missing source")

def expect_set(redis.Connection conn, bytes key, list[bytes] expected, str label):
    check(conn.scard(key) == len(expected), label + ": cardinality")
    for element in expected:
        check(conn.sismember(key, element), label + ": expected member")

def stores(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("store-a")
    auto b = f.key("store-b")
    auto dest = f.binary_key(bytes([1, 0, 255]))
    auto missing = f.key("store-missing")
    auto wrong = f.key("store-wrong")
    auto binary = bytes([0, 255, 128, 10])
    conn.sadd(a, [binary, b"", b"only-a"])
    conn.pexpire(a, 60000)
    conn.sadd(b, [binary, b"only-b"])
    conn.pexpire(b, 60000)
    auto source_expiry = conn.pexpiretime(a)
    f.put(conn, dest, b"previous string")
    check(conn.sdiffstore(dest, [a, b]) == 2, "SDIFFSTORE replaces prior type")
    expect_set(conn, dest, [b"", b"only-a"], "SDIFFSTORE source order")
    check(conn.pttl(dest) == -1, "store replacement clears prior expiry")
    conn.pexpire(dest, 60000)
    check(conn.sinterstore(dest, [a, b, b]) == 1, "SINTERSTORE duplicate keys")
    expect_set(conn, dest, [binary], "SINTERSTORE preserves binary member")
    conn.pexpire(dest, 60000)
    check(
        conn.sunionstore(dest, [a, b, missing]) == 4,
        "SUNIONSTORE ignores missing operand",
    )
    expect_set(
        conn,
        dest,
        [binary, b"", b"only-a", b"only-b"],
        "SUNIONSTORE writes union",
    )
    conn.pexpire(dest, 60000)
    auto expiry = conn.pexpiretime(dest)
    f.put(conn, wrong, b"wrong type")
    auto rejected = 0
    try:
        conn.sdiffstore(dest, [a, wrong])
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            rejected = rejected + 1
    try:
        conn.sinterstore(dest, [a, wrong])
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            rejected = rejected + 1
    try:
        conn.sunionstore(dest, [a, wrong])
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            rejected = rejected + 1
    check(rejected == 3 and conn.ping(), "store errors preserve connection")
    expect_set(
        conn,
        dest,
        [binary, b"", b"only-a", b"only-b"],
        "store errors preserve destination",
    )
    check(conn.pexpiretime(dest) == expiry, "store errors preserve destination expiry")
    check(
        conn.sinterstore(dest, [a, missing]) == 0 and conn.exists([dest]) == 0,
        "empty store deletes destination",
    )
    check(
        conn.pexpiretime(a) == source_expiry,
        "stores preserve non-destination source expiry",
    )
    check(conn.sdiffstore(a, [a, b]) == 2, "SDIFFSTORE can overwrite its first operand")
    conn.pexpire(a, 60000)
    expect_set(conn, a, [b"", b"only-a"], "aliased destination keeps correct result")

def selected(redis.SetSelection result) -> list[bytes]:
    match result:
        case redis.SetSelection.Members(values):
            return values
        case _:
            raise ValueError("expected counted set selection")

def contains(list[bytes] values, bytes member) -> bool:
    for value in values:
        if value == member:
            return True
    return False

def sample_and_pop(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("sample")
    auto single = f.key("sample-singleton")
    auto missing = f.key("sample-missing")
    auto wrong = f.key("sample-wrong")
    auto binary = bytes([255, 0, 128, 13, 10])
    list[bytes] original = [b"", binary, b"third"]
    conn.sadd(key, original)
    conn.pexpire(key, 60000)
    auto expiry = conn.pexpiretime(key)
    conn.sadd(single, [b""])
    conn.pexpire(single, 60000)

    check(
        conn.srandmember(single).unwrap() == b"",
        "SRANDMEMBER returns present empty member",
    )
    check(conn.srandmember(missing, count=None).is_none(), "SRANDMEMBER scalar miss")
    check(
        len(selected(conn.srandmember(missing, Optional.Some(4)))) == 0,
        "SRANDMEMBER counted miss",
    )
    check(
        len(selected(conn.srandmember(missing, Optional.Some(-4)))) == 0,
        "SRANDMEMBER replacement miss",
    )
    check(
        len(selected(conn.srandmember(key, Optional.Some(0)))) == 0,
        "SRANDMEMBER zero count",
    )
    check(
        contains(original, conn.srandmember(key).unwrap()),
        "SRANDMEMBER scalar result present",
    )
    auto distinct = selected(conn.srandmember(key, count=Optional.Some(12)))
    check(len(distinct) == 3, "positive SRANDMEMBER capped at cardinality")
    for member in original:
        check(
            contains(distinct, member),
            "positive SRANDMEMBER contains every distinct member",
        )
    auto repeats = selected(conn.srandmember(single, count=Optional.Some(-4)))
    check(len(repeats) == 4, "negative SRANDMEMBER repeats singleton")
    for member in repeats:
        check(member == b"", "replacement sample preserves empty bytes")
    auto sample = selected(conn.srandmember(key, count=Optional.Some(-8)))
    check(len(sample) == 8, "negative SRANDMEMBER sample length")
    for member in sample:
        check(
            contains(original, member),
            "replacement sample contains only original members",
        )
    expect_set(conn, key, original, "sampling is non-mutating")
    check(conn.pexpiretime(key) == expiry, "sampling preserves expiry")

    check(len(selected(conn.spop(missing, Optional.Some(5)))) == 0, "SPOP counted miss")
    check(conn.spop(missing).is_none(), "SPOP scalar miss")
    check(len(selected(conn.spop(key, Optional.Some(0)))) == 0, "SPOP zero count")
    auto negative = False
    try:
        conn.spop(key, Optional.Some(-1))
    except ValueError:
        negative = True
    check(negative, "SPOP rejects negative count")
    expect_set(conn, key, original, "zero and invalid counts preserve contents")
    check(conn.pexpiretime(key) == expiry, "zero and invalid counts preserve expiry")
    auto popped = selected(conn.spop(key, Optional.Some(2)))
    check(
        len(popped) == 2 and popped[0] != popped[1],
        "counted SPOP selects distinct members",
    )
    for member in popped:
        check(
            contains(original, member) and not conn.sismember(key, member),
            "SPOP removed original member",
        )
    check(
        conn.scard(key) == 1 and conn.pexpiretime(key) == expiry,
        "partial SPOP preserves set expiry",
    )
    auto last = selected(conn.spop(key, Optional.Some(100)))
    check(
        len(last) == 1 and contains(original, last[0]),
        "SPOP bounded by remaining members",
    )
    check(
        not contains(popped, last[0]) and conn.exists([key]) == 0,
        "final SPOP drains key",
    )
    check(
        conn.spop(single, count=None).unwrap() == b"" and conn.exists([single]) == 0,
        "scalar SPOP removes present empty member",
    )

    f.put(conn, wrong, b"string")
    auto rejected = 0
    try:
        conn.srandmember(wrong, Optional.Some(-3))
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            rejected = rejected + 1
    try:
        conn.spop(wrong, Optional.Some(1))
    except redis.ResponseError:
        if redis.error_prefix(conn.last_error()) == b"WRONGTYPE":
            rejected = rejected + 1
    check(
        rejected == 2 and conn.ping(),
        "set selection wrong type preserves connection",
    )
    check(conn.get(wrong).unwrap() == b"string", "failed selection preserves key")

def run(bool resp3):
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    if resp3:
        conn.disconnect()
        conn = redis.connect_resp3_options(f.config)
    try:
        f.verify_version(conn)
        multiple_membership_and_cardinality(conn, f)
        moves(conn, f)
        stores(conn, f)
        sample_and_pop(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()

def main():
    run(False)
    run(True)
    print("Live Redis extended set commands: PASS")
