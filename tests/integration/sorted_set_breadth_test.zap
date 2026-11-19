import redis
import integration_fixtures

def check(bool condition, str label):
    integration_fixtures.check(condition, label)

def members(list[bytes] actual, list[bytes] expected, str label):
    check(len(actual) == len(expected), label + ": member count")
    auto i = 0
    while i < len(actual):
        check(actual[i] == expected[i], label + ": member order and bytes")
        i = i + 1

def plain(redis.SortedSetResult result, list[bytes] expected, str label):
    match result:
        case redis.SortedSetResult.Members(values):
            members(values, expected, label)
        case _:
            raise ValueError("expected sorted-set members: " + label)

def scored(redis.SortedSetResult result, list[redis.ScoredMember] expected, str label):
    match result:
        case redis.SortedSetResult.WithScores(values):
            check(len(values) == len(expected), label + ": scored count")
            auto i = 0
            while i < len(values):
                check(
                    values[i].member == expected[i].member and values[i].score == expected[i].score,
                    label,
                )
                i = i + 1
        case _:
            raise ValueError("expected sorted-set scores: " + label)

def ranges(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([0, 255, 13, 10]))
    auto lex = f.key("lex-range")
    auto dest = f.key("range-output")
    auto absent = f.key("missing-range")
    auto binary = bytes([0, 128, 255, 13, 10])
    conn.execute(
        [b"ZADD", key, b"-inf", binary, b"1", b"one", b"2", b"two", b"+inf", b"last"],
    )
    conn.pexpire(key, 60000)
    conn.execute([b"ZADD", lex, b"0", b"", b"0", binary, b"0", b"a", b"0", b"b"])
    conn.pexpire(lex, 60000)

    plain(
        conn.zrangebyscore(key, b"(1", b"+inf", start=0, num=1),
        [b"two"],
        "score exclusive lower and LIMIT",
    )
    plain(
        conn.zrangebyscore(key, b"-inf", b"+inf", start=0, num=0),
        [],
        "score LIMIT zero",
    )
    plain(
        conn.zrevrangebyscore(key, b"+inf", b"(1"),
        [b"last", b"two"],
        "reverse score boundaries",
    )
    scored(
        conn.zrangebyscore(key, b"-inf", b"2", withscores=True),
        [
            redis.ScoredMember(binary, -math.inf),
            redis.ScoredMember(b"one", 1.0),
            redis.ScoredMember(b"two", 2.0),
        ],
        "WITHSCORES including negative infinity",
    )
    scored(
        conn.zrevrangebyscore(key, b"+inf", b"1", start=1, num=-1, withscores=True),
        [redis.ScoredMember(b"two", 2.0), redis.ScoredMember(b"one", 1.0)],
        "reverse WITHSCORES negative LIMIT count",
    )
    check(
        conn.zrange_withscores(key, 0, 0)[0].member == binary,
        "existing ZRANGE WITHSCORES in both protocols",
    )
    plain(conn.zrangebyscore(absent, b"-inf", b"+inf"), [], "missing score range")
    check(conn.zcount(key, b"-inf", b"(1") == 1, "ZCOUNT includes negative infinity")
    check(
        conn.zcount(key, b"2", b"1") == 0 and conn.zcount(absent, b"-inf", b"+inf") == 0,
        "empty score counts",
    )

    members(
        conn.zrangebylex(lex, b"[" + binary, b"(b"),
        [binary, b"a"],
        "lex binary lower bound",
    )
    members(conn.zrangebylex(lex, b"[", b"["), [b""], "lex empty member")
    members(
        conn.zrevrangebylex(lex, b"+", b"-", start=1, num=2),
        [b"a", binary],
        "reverse lex LIMIT",
    )
    check(conn.zlexcount(lex, b"(" + binary, b"+") == 2, "exclusive binary lex count")
    check(conn.zlexcount(absent, b"-", b"+") == 0, "missing lex count")

    check(
        conn.zrangestore(dest, key, b"(1", b"3", byscore=True) == 1,
        "score range store",
    )
    conn.pexpire(dest, 60000)
    members(conn.zrange(dest, 0, -1), [b"two"], "score store destination")
    check(
        conn.zrangestore(dest, lex, b"+", b"-", bylex=True, desc=True, offset=1, num=2) == 2,
        "reverse lex store LIMIT",
    )
    conn.pexpire(dest, 60000)
    members(
        conn.zrange(dest, 0, -1),
        [binary, b"a"],
        "lex destination has original scores",
    )
    check(
        conn.zrangestore(dest, key, b"0", b"0", desc=True) == 1,
        "descending rank store",
    )
    conn.pexpire(dest, 60000)
    check(
        conn.zscore(dest, b"last").unwrap() == math.inf,
        "rank store preserves infinity",
    )
    check(
        conn.zrangestore(dest, absent, b"0", b"-1") == 0 and conn.exists([dest]) == 0,
        "empty store removes destination",
    )

def combinations(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("combine-a")
    auto b = f.key("combine-b")
    auto dest = f.key("combine-dest")
    auto absent = f.key("combine-missing")
    auto wrong = f.key("combine-wrong")
    auto binary = bytes([255, 0, 128])
    conn.execute([b"ZADD", a, b"1", binary, b"2", b"shared", b"4", b"only-a"])
    conn.pexpire(a, 60000)
    conn.execute([b"ZADD", b, b"3", b"shared", b"5", b"only-b"])
    conn.pexpire(b, 60000)
    plain(
        conn.zunion([a, absent]),
        [binary, b"shared", b"only-a"],
        "ZUNION with missing set",
    )
    plain(conn.zinter([a, absent]), [], "ZINTER with missing set")
    plain(conn.zdiff([a, b]), [binary, b"only-a"], "ZDIFF in score order")
    scored(
        conn.zdiff([b, a], withscores=True),
        [redis.ScoredMember(b"only-b", 5.0)],
        "reverse source order changes ZDIFF",
    )
    scored(
        conn.zinter([a, b], withscores=True, weights=[2.0, 3.0]),
        [redis.ScoredMember(b"shared", 13.0)],
        "weighted ZINTER sum",
    )
    scored(
        conn.zunion([a, b], aggregate="max", withscores=True, weights=[2.0, -1.0]),
        [
            redis.ScoredMember(b"only-b", -5.0),
            redis.ScoredMember(binary, 2.0),
            redis.ScoredMember(b"shared", 4.0),
            redis.ScoredMember(b"only-a", 8.0),
        ],
        "weighted ZUNION max and negative weight",
    )
    check(
        conn.zinterstore(dest, [a, b], aggregate="min", weights=[2.0, 1.0]) == 1,
        "ZINTERSTORE count",
    )
    conn.pexpire(dest, 60000)
    check(conn.zscore(dest, b"shared").unwrap() == 3.0, "ZINTERSTORE weighted MIN")
    check(conn.zunionstore(dest, [a, b]) == 4, "ZUNIONSTORE overwrites destination")
    conn.pexpire(dest, 60000)
    check(conn.zscore(dest, b"shared").unwrap() == 5.0, "ZUNIONSTORE SUM")
    check(conn.zdiffstore(dest, [a, b]) == 2, "ZDIFFSTORE count")
    conn.pexpire(dest, 60000)
    members(conn.zrange(dest, 0, -1), [binary, b"only-a"], "ZDIFFSTORE contents")
    f.put(conn, wrong, b"string")
    auto response = False
    try:
        conn.zunionstore(dest, [a, wrong])
    except redis.ResponseError:
        response = redis.error_prefix(conn.last_error()) == b"WRONGTYPE"
    check(response and conn.ping(), "wrong-type aggregate leaves connection usable")
    members(
        conn.zrange(dest, 0, -1),
        [binary, b"only-a"],
        "failed store preserves destination",
    )
    check(
        conn.zdiffstore(dest, [a, a]) == 0 and conn.exists([dest]) == 0,
        "empty difference clears destination",
    )

    auto version = os.getenv("REDIS_TEST_SERVER_VERSION")
    if version == "8.10.3":
        scored(
            conn.zinter([a, b], aggregate="COUNT", withscores=True, weights=[2.0, 3.0]),
            [redis.ScoredMember(b"shared", 5.0)],
            "Redis 8.8+ COUNT aggregates weighted membership",
        )
    elif version == "7.4.12":
        auto unsupported = False
        try:
            conn.zinter([a, b], aggregate="COUNT")
        except redis.ResponseError:
            unsupported = True
        check(
            unsupported and conn.ping(),
            "older server rejects COUNT and preserves connection",
        )

def pops(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("pop-a")
    auto b = f.key("pop-b")
    auto absent = f.key("pop-absent")
    auto binary = bytes([255, 0, 13, 10])
    conn.execute(
        [b"ZADD", a, b"-inf", binary, b"1", b"one", b"1", b"tie", b"+inf", b"last"],
    )
    conn.pexpire(a, 60000)
    conn.execute([b"ZADD", b, b"2", b"one", b"3", b"three", b"5", b"five"])
    conn.pexpire(b, 60000)
    check(len(conn.zpopmin(absent)) == 0, "ZPOPMIN missing")
    check(len(conn.zpopmax(a, 0)) == 0 and conn.zcard(a) == 4, "ZPOPMAX zero count")
    auto low = conn.zpopmin(a, 2)
    check(
        len(low) == 2 and low[0].member == binary and low[0].score == -math.inf and low[1].member == b"one",
        "ZPOPMIN counted and tie order",
    )
    auto top = conn.zpopmax(a)
    check(
        len(top) == 1 and top[0].member == b"last" and top[0].score == math.inf,
        "ZPOPMAX omitted count",
    )
    auto blocked_min = conn.bzpopmin([absent, a], timeout=0.25).unwrap()
    check(
        blocked_min.key == a and blocked_min.member == b"tie" and blocked_min.score == 1.0,
        "BZPOPMIN skips missing keys",
    )
    auto blocked_max = conn.bzpopmax([b], timeout=0.25).unwrap()
    check(
        blocked_max.key == b and blocked_max.member == b"five" and blocked_max.score == 5.0,
        "BZPOPMAX single result",
    )
    auto batch = conn.zmpop([absent, b], min=True, count=1).unwrap()
    check(
        batch.key == b and len(batch.members) == 1 and batch.members[0].member == b"one",
        "ZMPOP first available key",
    )
    auto blocked = conn.bzmpop(0.25, [b], max=True, count=4).unwrap()
    check(
        blocked.key == b and len(blocked.members) == 1 and blocked.members[0].score == 3.0,
        "BZMPOP returns fewer when exhausted",
    )
    check(conn.zmpop([a, b], min=True).is_none(), "ZMPOP empty returns null")
    check(conn.bzpopmin([absent], timeout=0.01).is_none(), "BZPOPMIN server timeout")
    check(conn.bzpopmax([absent], timeout=0.01).is_none(), "BZPOPMAX server timeout")
    check(
        conn.bzmpop(0.01, [absent], max=True).is_none() and conn.ping(),
        "BZMPOP null keeps connection usable",
    )

def wake_sorted_pop(redis.Options cfg, int64 client_id, bytes key, bytes member):
    auto producer = redis.connect_options(cfg)
    try:
        auto deadline = time.monotonic_ns() + 2000000000
        while time.monotonic_ns() < deadline:
            auto state = producer.execute(
                [b"CLIENT", b"LIST", b"ID", bytes(str(client_id))],
            )
            match state:
                case redis.Reply.Bulk(data):
                    if str(data) =~ m/(?:\A| )flags=[^ ]*b/:
                        producer.zadd(key, {member: 2.5})
                        return
                case _:
                    raise ValueError("expected CLIENT LIST text")
            time.sleep(0.001)
        raise ValueError("sorted-set consumer did not block on Redis")
    finally:
        producer.disconnect()

def wakeup(redis.Connection conn, integration_fixtures.Fixture f):
    auto empty = f.key("wakeup-empty")
    auto ready = f.binary_key(bytes([9, 0, 255]))
    auto member = bytes([0, 255, 13, 10])
    auto client_id = integration_fixtures.integer(conn.execute([b"CLIENT", b"ID"]))
    scope:
        spawn wake_sorted_pop(f.config, client_id, ready, member)
        auto result = conn.bzmpop(2.0, [empty, ready], min=True).unwrap()
        check(
            result.key == ready and len(result.members) == 1,
            "BZMPOP wakes on populated key",
        )
        check(
            result.members[0].member == member and result.members[0].score == 2.5,
            "BZMPOP wakeup preserves binary member and score",
        )

def add_options(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([8, 255, 13, 10]))
    auto binary = bytes([0, 255, 13, 10])
    check(
        conn.zadd(name=key, mapping={binary: 1.0, b"": 0.0}, nx=True).count() == 2,
        "ZADD NX inserts binary and empty members",
    )
    conn.pexpire(key, 60000)
    check(
        conn.zadd(key, {binary: 9.0}, nx=True, ch=True).count() == 0,
        "ZADD NX leaves existing member",
    )
    check(
        conn.zadd(key, {b"absent": 10.0}, xx=True).count() == 0,
        "ZADD XX leaves absent member",
    )
    check(
        conn.zadd(key, {binary: 1.0, b"": 0.0}, ch=True).count() == 0,
        "CH skips equal scores",
    )
    check(
        conn.zadd(key, {binary: 2.0, b"new": 3.0}, ch=True).count() == 2,
        "CH counts changes and additions",
    )
    check(
        conn.zadd(key, {binary: 4.0}, xx=True, gt=True, ch=True).count() == 1,
        "XX GT updates greater score",
    )
    check(
        conn.zadd(key, {binary: 6.0}, xx=True, lt=True, ch=True).count() == 0,
        "LT skips greater score",
    )
    check(
        conn.zadd(key, {binary: 1.0}, xx=True, lt=True, ch=True).count() == 1,
        "XX LT updates lower score",
    )
    check(
        conn.zadd(key, {b"lt-new": 5.0}, lt=True).count() == 1,
        "LT inserts new members",
    )
    check(
        conn.zadd(key, {b"gt-new": 6.0}, gt=True).count() == 1,
        "GT inserts new members",
    )
    match conn.zadd(key, {binary: -1.0}, incr=True):
        case redis.ZAddResult.Score(value):
            check(value == 0.0, "INCR returns zero score as present")
        case _:
            raise ValueError("expected ZADD increment score")
    check(
        conn.zadd(key, {binary: 1.0}, nx=True, incr=True).is_null(),
        "NX INCR existing skips with null",
    )
    check(
        conn.zadd(key, {b"absent": 1.0}, xx=True, incr=True).is_null(),
        "XX INCR absent skips with null",
    )
    check(
        conn.zadd(key, {binary: 0.0}, gt=True, incr=True).is_null(),
        "GT INCR equal skips with null",
    )
    match conn.zadd(key, {b"created": 1.5}, nx=True, incr=True):
        case redis.ZAddResult.Score(value):
            check(value == 1.5, "NX INCR on absent member returns score")
        case _:
            raise ValueError("expected score for created member")
    check(
        conn.zscore(key, binary).unwrap() == 0.0 and conn.zscore(key, b"absent").is_none(),
        "skipped increments preserve state",
    )
    conn.zadd(key, {b"infinite": math.inf})
    auto invalid = False
    try:
        conn.zadd(key, {b"infinite": -math.inf}, incr=True)
    except redis.ResponseError:
        invalid = True
    check(
        invalid and conn.zscore(key, b"infinite").unwrap() == math.inf and conn.ping(),
        "NaN result leaves stored score and connection",
    )

def run(bool resp3):
    auto f = integration_fixtures.fixture()
    f.config.command_timeout_ms = 3000
    auto conn = f.open()
    if resp3:
        conn.disconnect()
        conn = redis.connect_resp3_options(f.config)
    try:
        f.verify_version(conn)
        add_options(conn, f)
        ranges(conn, f)
        combinations(conn, f)
        pops(conn, f)
        wakeup(conn, f)
    finally:
        conn.disconnect()
        f.cleanup()

def main():
    run(False)
    run(True)
    print("Live Redis extended sorted-set commands: PASS")
