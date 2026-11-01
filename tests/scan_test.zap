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
        for step in steps:
            auto want = redis.encode_command(step.args)
            auto got = b""
            while len(got) < len(want):
                auto chunk = bytes(net.recv(socket, len(want) - len(got)))
                check(len(chunk) > 0, "command complete")
                got = got + chunk
            check(got == want, "exact helper wire command")
            net.send(socket, step.reply)
    finally:
        net.close_conn(socket)

def invalid_options(redis.Connection conn):
    auto errors = 0
    try:
        conn.scan(b"")
    except ValueError:
        errors = errors + 1
    try:
        conn.scan(b"-1")
    except ValueError:
        errors = errors + 1
    try:
        conn.scan(b"01")
    except ValueError:
        errors = errors + 1
    try:
        conn.scan(bytes([255]))
    except ValueError:
        errors = errors + 1
    try:
        conn.scan(count=-1)
    except ValueError:
        errors = errors + 1
    try:
        conn.hscan(b"k", b"bad")
    except ValueError:
        errors = errors + 1
    try:
        conn.sscan(b"k", count=-1)
    except ValueError:
        errors = errors + 1
    try:
        conn.zscan(b"k", b"+1")
    except ValueError:
        errors = errors + 1
    check(errors == 8, "invalid scan options rejected before I/O")

def main():
    auto huge = b"18446744073709551615"
    auto binary = bytes([255, 0, 128])
    auto pattern = binary + b"*"
    list[Step] steps = [
        Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$20\r\n" + huge + b"\r\n*0\r\n"),
        Step(
            [b"SCAN", huge, b"MATCH", pattern, b"COUNT", b"1"],
            b"*2\r\n$1\r\n1\r\n*2\r\n$3\r\n" + binary + b"\r\n$0\r\n\r\n",
        ),
        Step(
            [b"SCAN", b"1", b"MATCH", b""],
            b"*2\r\n$1\r\n0\r\n*1\r\n$3\r\n" + binary + b"\r\n",
        ),
        Step(
            [b"HSCAN", binary, b"0", b"MATCH", b"*"],
            b"*2\r\n$20\r\n" + huge + b"\r\n*0\r\n",
        ),
        Step(
            [b"HSCAN", binary, huge, b"MATCH", pattern, b"COUNT", b"1"],
            b"*2\r\n$1\r\n0\r\n*2\r\n$3\r\n" + binary + b"\r\n$0\r\n\r\n",
        ),
        Step(
            [b"SSCAN", binary, b"0", b"MATCH", b"*"],
            b"*2\r\n$20\r\n" + huge + b"\r\n*0\r\n",
        ),
        Step(
            [b"SSCAN", binary, huge, b"MATCH", pattern, b"COUNT", b"1"],
            b"*2\r\n$1\r\n0\r\n*1\r\n$3\r\n" + binary + b"\r\n",
        ),
        Step(
            [b"ZSCAN", binary, b"0", b"MATCH", b"*"],
            b"*2\r\n$20\r\n" + huge + b"\r\n*0\r\n",
        ),
        Step(
            [b"ZSCAN", binary, huge, b"MATCH", pattern, b"COUNT", b"1"],
            b"*2\r\n$1\r\n0\r\n*2\r\n$3\r\n" + binary + b"\r\n$5\r\n-1.25\r\n",
        ),
    ]
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"+OK\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*1\r\n$1\r\n0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step([b"SCAN", b"0", b"MATCH", b"*"], b"*3\r\n$1\r\n0\r\n*0\r\n*0\r\n"),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n:0\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$-1\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$0\r\n\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$2\r\n-1\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$2\r\n00\r\n*0\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"SCAN", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n" + bytes([255]) + b"\r\n*0\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n*-1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n:1\r\n"))
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step([b"SCAN", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n*1\r\n$-1\r\n"),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"HSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*1\r\n$1\r\nf\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"HSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*2\r\n$1\r\nf\r\n:1\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"SSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*1\r\n:1\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step([b"SSCAN", b"k", b"0", b"MATCH", b"*"], b"*2\r\n$1\r\n0\r\n" + b"*-1\r\n"),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"ZSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*1\r\n$1\r\nv\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"ZSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*2\r\n$1\r\nv\r\n$3\r\nnan\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    steps.append(
        Step(
            [b"ZSCAN", b"k", b"0", b"MATCH", b"*"],
            b"*2\r\n$1\r\n0\r\n" + b"*2\r\n$-1\r\n$1\r\n1\r\n",
        ),
    )
    steps.append(Step([b"PING"], b"+PONG\r\n"))
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener))
    try:
        invalid_options(conn)
        auto first = conn.scan()
        check(not first.done() and len(first.items) == 0, "empty nonterminal SCAN")
        check(first.cursor == huge, "unsigned cursor retained exactly")
        auto second = conn.scan(first.cursor, pattern=pattern, count=1)
        check(
            not second.done() and len(second.items) == 2,
            "COUNT hint does not truncate page",
        )
        bytes item = second.items[0]
        check(item == binary, "binary SCAN key")
        auto final = conn.scan(second.cursor, pattern=b"")
        check(final.done() and len(final.items) == 1, "terminal page still has data")
        bytes repeated = final.items[0]
        check(repeated == binary, "duplicates preserved across pages")
        auto hash = conn.hscan(binary)
        check(
            not hash.done() and len(hash.fields) == 0 and hash.cursor == huge,
            "empty nonterminal HSCAN",
        )
        auto hash_final = conn.hscan(binary, hash.cursor, pattern=pattern, count=1)
        check(hash_final.done() and len(hash_final.fields) == 1, "HSCAN terminal data")
        check(
            hash_final.fields[0].field == binary and hash_final.fields[0].value == b"",
            "HSCAN binary pair",
        )
        auto members = conn.sscan(binary)
        check(
            not members.done() and len(members.items) == 0 and members.cursor == huge,
            "empty nonterminal SSCAN",
        )
        auto members_final = conn.sscan(
            binary,
            members.cursor,
            pattern=pattern,
            count=1,
        )
        check(
            members_final.done() and len(members_final.items) == 1,
            "SSCAN terminal data",
        )
        bytes member = members_final.items[0]
        check(member == binary, "SSCAN binary member")
        auto sorted = conn.zscan(binary)
        check(
            not sorted.done() and len(sorted.members) == 0 and sorted.cursor == huge,
            "empty nonterminal ZSCAN",
        )
        auto sorted_final = conn.zscan(binary, sorted.cursor, pattern=pattern, count=1)
        check(
            sorted_final.done() and len(sorted_final.members) == 1,
            "ZSCAN terminal data",
        )
        check(
            sorted_final.members[0].member == binary and sorted_final.members[0].score == -1.25,
            "ZSCAN scored pair",
        )
        auto errors = 0
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.scan()
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.hscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.sscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        try:
            conn.zscan(b"k")
        except redis.ProtocolError:
            errors = errors + 1
        check(conn.ping(), "complete malformed typed reply consumed")
        check(errors == 20, "all malformed scan pages rejected")
    finally:
        conn.disconnect()
    print("Scan page wire and validation: PASS")
