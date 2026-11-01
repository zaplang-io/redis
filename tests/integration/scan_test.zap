import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def remember(list[bytes] seen, bytes item):
    for old in seen:
        bytes existing = old
        if existing == item:
            return
    seen.append(item)

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        auto marker = b"bin:" + bytes([255, 0])
        auto first = f.binary_key(marker + b":a")
        auto second = f.binary_key(marker + b":b")
        f.put(conn, first, b"1")
        f.put(conn, second, b"2")
        auto excluded = f.key("excluded")
        f.put(conn, excluded, b"3")
        auto pattern = bytes(f.prefix) + marker + b"*"
        auto cursor = b"0"
        list[bytes] seen = []
        auto pages = 0
        while True:
            auto page = conn.scan(cursor, pattern=pattern, count=1)
            for item in page.items:
                bytes key = item
                check(key == first or key == second, "SCAN binary MATCH")
                remember(seen, key)
            pages = pages + 1
            check(pages < 1000, "SCAN test page budget")
            if page.done():
                break
            cursor = page.cursor
        check(len(seen) == 2, "SCAN complete stable fixture coverage")
        auto hash = f.key("hash")
        auto set_key = f.key("set")
        auto sorted = f.key("sorted")
        auto field = marker + b":field"
        auto other = marker + b":other"
        auto value = bytes([255, 0, 128])
        conn.hset(
            hash,
            [
                redis.HashField(field, value),
                redis.HashField(other, b""),
                redis.HashField(b"skip", b"x"),
            ],
        )
        conn.pexpire(hash, 60000)
        conn.sadd(set_key, [field, other, b"skip"])
        conn.pexpire(set_key, 60000)
        conn.zadd(
            sorted,
            [
                redis.ScoredMember(field, -1.25),
                redis.ScoredMember(other, 1.2345678901234567),
                redis.ScoredMember(b"skip", 0.0),
            ],
        )
        conn.pexpire(sorted, 60000)
        cursor = b"0"
        list[bytes] hash_seen = []
        pages = 0
        while True:
            auto page = conn.hscan(hash, cursor, pattern=marker + b"*", count=1)
            for entry in page.fields:
                check(entry.field == field or entry.field == other, "HSCAN field MATCH")
                if entry.field == field:
                    check(entry.value == value, "HSCAN binary value")
                else:
                    check(entry.value == b"", "HSCAN empty value")
                remember(hash_seen, entry.field)
            pages = pages + 1
            check(pages < 1000, "HSCAN test page budget")
            if page.done():
                break
            cursor = page.cursor
        check(len(hash_seen) == 2, "HSCAN full coverage")
        cursor = b"0"
        list[bytes] set_seen = []
        pages = 0
        while True:
            auto page = conn.sscan(set_key, cursor, pattern=marker + b"*", count=1)
            for item in page.items:
                bytes member = item
                check(member == field or member == other, "SSCAN binary MATCH")
                remember(set_seen, member)
            pages = pages + 1
            check(pages < 1000, "SSCAN test page budget")
            if page.done():
                break
            cursor = page.cursor
        check(len(set_seen) == 2, "SSCAN full coverage")
        cursor = b"0"
        list[bytes] sorted_seen = []
        pages = 0
        while True:
            auto page = conn.zscan(sorted, cursor, pattern=marker + b"*", count=1)
            for entry in page.members:
                check(
                    entry.member == field or entry.member == other,
                    "ZSCAN member MATCH",
                )
                if entry.member == field:
                    check(entry.score == -1.25, "ZSCAN negative score")
                else:
                    check(entry.score == 1.2345678901234567, "ZSCAN precise score")
                remember(sorted_seen, entry.member)
            pages = pages + 1
            check(pages < 1000, "ZSCAN test page budget")
            if page.done():
                break
            cursor = page.cursor
        check(len(sorted_seen) == 2, "ZSCAN full coverage")
        auto missing = f.key("missing")
        auto h = conn.hscan(missing)
        auto s = conn.sscan(missing)
        auto z = conn.zscan(missing)
        check(h.done() and len(h.fields) == 0, "missing HSCAN")
        check(s.done() and len(s.items) == 0, "missing SSCAN")
        check(z.done() and len(z.members) == 0, "missing ZSCAN")
        cursor = b"0"
        pages = 0
        while True:
            auto no_match = conn.hscan(hash, cursor, pattern=b"does-not-match")
            check(len(no_match.fields) == 0, "filtered hash page has no matches")
            pages = pages + 1
            check(pages < 1000, "filtered HSCAN test page budget")
            if no_match.done():
                break
            cursor = no_match.cursor
        auto errors = 0
        try:
            conn.hscan(excluded)
        except redis.ResponseError:
            errors = errors + 1
        try:
            conn.sscan(excluded)
        except redis.ResponseError:
            errors = errors + 1
        try:
            conn.zscan(excluded)
        except redis.ResponseError:
            errors = errors + 1
        check(errors == 3 and conn.ping(), "scan WRONGTYPE preserves connection")
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis scan pages: PASS")
