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
                "exact stream wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def bulk(bytes text) -> bytes:
    return b"$" + bytes(str(len(text))) + b"\r\n" + text + b"\r\n"

def number(int64 value) -> bytes:
    return b":" + bytes(str(value)) + b"\r\n"

def fields(list[bytes] pairs, list[bytes] extra = []) -> bytes:
    auto wire = b"*" + bytes(str((len(pairs) + len(extra)) * 2)) + b"\r\n"
    for pair in pairs:
        wire = wire + pair
    for pair in extra:
        wire = wire + pair
    return wire

def stream_entry(bytes id, bytes field, bytes value) -> bytes:
    return b"*2\r\n" + bulk(id) + b"*4\r\n" + bulk(field) + bulk(value) + bulk(field) + bulk(b"")

def reject_options(redis.Connection conn):
    auto errors = 0
    auto one = Optional.Some(1)
    auto id = Optional.Some(b"1-0")
    list[redis.StreamField] data = [redis.StreamField(b"f", b"v")]
    try:
        conn.xadd(b"k", data, maxlen=one, minid=id)
    except ValueError:
        errors = errors + 1
    try:
        conn.xadd(b"k", data, limit=one)
    except ValueError:
        errors = errors + 1
    try:
        conn.xadd(b"k", data, b"1-2-*")
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k")
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", maxlen=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", minid=Optional.Some(b"18446744073709551616-0"))
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", minid=Optional.Some(b"$"))
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", minid=id, approximate=False, limit=one)
    except ValueError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", maxlen=one, limit=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", 0, [])
    except ValueError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", 0, [b"1"])
    except ValueError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", -1, [b"1-0"])
    except ValueError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", 0, [b"1-0"], idle=one, time=one)
    except ValueError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", 0, [b"1-0"], retrycount=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.xautoclaim(b"k", b"g", b"c", 0, count=-1, justid=True)
    except ValueError:
        errors = errors + 1
    try:
        conn.xsetid(b"k", b"+")
    except ValueError:
        errors = errors + 1
    try:
        conn.xsetid(b"k", b"1-0", entries_added=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    try:
        conn.xsetid(b"k", b"1-0", max_deleted_id=Optional.Some(b"1"))
    except ValueError:
        errors = errors + 1
    try:
        conn.xinfo_stream(b"k", count=one)
    except ValueError:
        errors = errors + 1
    try:
        conn.xinfo_stream(b"k", full=True, count=Optional.Some(-1))
    except ValueError:
        errors = errors + 1
    check(errors == 20, "invalid stream options are rejected before I/O")

def mutations(redis.Connection conn, bytes key, bytes group, bytes consumer, bytes id):
    list[redis.StreamField] data = [redis.StreamField(consumer, group), redis.StreamField(consumer, b"")]
    check(conn.xadd(key, data, nomkstream=True).is_none(), "NOMKSTREAM nil")
    check(conn.xadd(key, data, b"2-*", maxlen=Optional.Some(0), approximate=False).unwrap() == b"2-0", "explicit exact trim and ms-* ID")
    check(conn.xadd(key, data, id, minid=Optional.Some(b"1-0"), limit=Optional.Some(0)).unwrap() == id, "MINID, LIMIT 0 and unsigned ID")
    check(conn.xtrim(key, minid=Optional.Some(b"3"), approximate=False) == 1, "exact MINID with millisecond-only threshold")
    check(conn.xtrim(key, Optional.Some(2), limit=Optional.Some(5)) == 0, "approximate MAXLEN")
    match conn.xclaim(key, group, consumer, 9, [id, b"1-0"], idle=Optional.Some(0), retrycount=Optional.Some(3), force=True):
        case redis.StreamClaimResult.Entries(values):
            check(len(values) == 1 and values[0].id == id and values[0].fields[0].value == group and values[0].fields[1].field == consumer, "XCLAIM ordered binary fields")
        case _:
            raise ValueError("Expected claimed entries")
    match conn.xclaim(key, group, consumer, 0, [id], time=Optional.Some(123), justid=True):
        case redis.StreamClaimResult.Ids(values):
            check(len(values) == 1 and values[0] == id, "XCLAIM JUSTID")
        case _:
            raise ValueError("Expected claimed IDs")
    auto claim = conn.xautoclaim(key, group, consumer, 0, b"1-0", 2, justid=True)
    check(claim.next_id == b"9-0" and len(claim.entries) == 0 and len(claim.ids) == 1 and claim.ids[0] == id and claim.deleted_ids[0] == b"3-0", "XAUTOCLAIM preserves continuation and deleted IDs with JUSTID")
    auto legacy = conn.xautoclaim(key, group, consumer, 0, justid=True)
    check(legacy.next_id == b"0-0" and len(legacy.ids) == 0 and len(legacy.deleted_ids) == 0, "previous two-element XAUTOCLAIM reply")
    check(conn.xsetid(key, id, entries_added=Optional.Some(5), max_deleted_id=Optional.Some(b"0-0")), "XSETID extended arguments")

def metadata(redis.Connection conn, bytes key, bytes group, bytes consumer, bytes id):
    auto info = conn.xinfo_stream(name=key)
    check(info.length == 1 and info.radix_tree_keys == 1 and info.radix_tree_nodes == 2 and info.entries_added == 5, "XINFO STREAM counters")
    check(info.last_generated_id == id and info.recorded_first_entry_id == id and info.max_deleted_entry_id == b"0-0", "XINFO STREAM IDs")
    match info.details:
        case redis.StreamInfoDetails.Summary(groups, first_entry, last_entry):
            check(groups == 1 and first_entry.unwrap().id == id and last_entry.unwrap().fields[0].value == group, "summary entry boundaries")
        case _:
            raise ValueError("Expected summary")
    auto groups = conn.xinfo_groups(key)
    check(len(groups) == 1 and groups[0].name == group and groups[0].pending == 1 and groups[0].consumers == 1, "XINFO GROUPS counters and binary name")
    check(groups[0].last_delivered_id == id and groups[0].entries_read.is_none() and groups[0].lag.unwrap() == 0, "nullable read position and zero lag")
    auto consumers = conn.xinfo_consumers(key, group)
    check(len(consumers) == 1 and consumers[0].name == consumer and consumers[0].pending == 1 and consumers[0].idle_ms == 3 and consumers[0].inactive_ms.is_none(), "XINFO CONSUMERS never-active sentinel")
    auto full = conn.xinfo_stream(key, full=True, count=Optional.Some(1))
    match full.details:
        case redis.StreamInfoDetails.Full(entries, full_groups):
            check(len(entries) == 1 and entries[0].id == id and len(full_groups) == 1, "FULL typed entry and group")
            auto g = full_groups[0]
            auto c = g.consumers[0]
            check(g.pending_count == 1 and g.pending[0].consumer.unwrap() == consumer and g.pending[0].deliveries == 2, "FULL group pending fields")
            check(c.active_time_ms.unwrap() == 456 and c.seen_time_ms == 678 and c.pending_count == 1 and c.pending[0].consumer.is_none() and c.pending[0].id == id, "FULL consumer times and pending IDs")
        case _:
            raise ValueError("Expected full metadata")

def reject_replies(redis.Connection conn):
    auto errors = 0
    try:
        conn.xadd(b"k", [redis.StreamField(b"f", b"v")])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xtrim(b"k", Optional.Some(1))
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xclaim(b"k", b"g", b"c", 0, [b"1-0"], justid=True)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xautoclaim(b"k", b"g", b"c", 0, justid=True)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xsetid(b"k", b"1-0")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xinfo_stream(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xinfo_groups(b"k")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.xinfo_consumers(b"k", b"g")
    except redis.ProtocolError:
        errors = errors + 1
    check(errors == 8 and conn.ping(), "malformed stream replies leave the connection usable")

def main():
    auto key = bytes([255, 0, 13, 10])
    auto group = bytes([0, 255])
    auto consumer = bytes([128, 0])
    auto id = b"18446744073709551615-0"
    auto entry = stream_entry(id, consumer, group)
    list[bytes] shared = [
        bulk(b"length") + number(1), bulk(b"radix-tree-keys") + number(1),
        bulk(b"radix-tree-nodes") + number(2), bulk(b"last-generated-id") + bulk(id),
        bulk(b"max-deleted-entry-id") + bulk(b"0-0"), bulk(b"entries-added") + number(5),
        bulk(b"recorded-first-entry-id") + bulk(id),
    ]
    auto summary = fields(shared, [
        bulk(b"new-field") + bulk(b"ignored"), bulk(b"last-entry") + entry,
        bulk(b"groups") + number(1), bulk(b"first-entry") + entry,
    ])
    auto groups = b"*1\r\n" + fields([
        bulk(b"lag") + number(0), bulk(b"name") + bulk(group),
        bulk(b"consumers") + number(1), bulk(b"pending") + number(1),
        bulk(b"last-delivered-id") + bulk(id), bulk(b"entries-read") + b"$-1\r\n",
    ])
    auto consumers = b"*1\r\n" + fields([
        bulk(b"name") + bulk(consumer), bulk(b"pending") + number(1),
        bulk(b"idle") + number(3), bulk(b"inactive") + number(-1),
    ])
    auto full_consumer = fields([
        bulk(b"name") + bulk(consumer), bulk(b"seen-time") + number(678),
        bulk(b"active-time") + number(456), bulk(b"pel-count") + number(1),
        bulk(b"pending") + b"*1\r\n*3\r\n" + bulk(id) + number(123) + number(2),
    ])
    auto full_group = fields([
        bulk(b"name") + bulk(group), bulk(b"last-delivered-id") + bulk(id),
        bulk(b"entries-read") + number(1), bulk(b"lag") + number(0), bulk(b"pel-count") + number(1),
        bulk(b"pending") + b"*1\r\n*4\r\n" + bulk(id) + bulk(consumer) + number(123) + number(2),
        bulk(b"consumers") + b"*1\r\n" + full_consumer,
    ])
    auto full = fields(shared, [bulk(b"entries") + b"*1\r\n" + entry, bulk(b"groups") + b"*1\r\n" + full_group])
    list[Step] steps = [
        Step([b"XADD", key, b"NOMKSTREAM", b"*", consumer, group, consumer, b""], b"$-1\r\n"),
        Step([b"XADD", key, b"MAXLEN", b"=", b"0", b"2-*", consumer, group, consumer, b""], bulk(b"2-0")),
        Step([b"XADD", key, b"MINID", b"~", b"1-0", b"LIMIT", b"0", id, consumer, group, consumer, b""], bulk(id)),
        Step([b"XTRIM", key, b"MINID", b"=", b"3"], number(1)),
        Step([b"XTRIM", key, b"MAXLEN", b"~", b"2", b"LIMIT", b"5"], number(0)),
        Step([b"XCLAIM", key, group, consumer, b"9", id, b"1-0", b"IDLE", b"0", b"RETRYCOUNT", b"3", b"FORCE"], b"*1\r\n" + entry),
        Step([b"XCLAIM", key, group, consumer, b"0", id, b"TIME", b"123", b"JUSTID"], b"*1\r\n" + bulk(id)),
        Step([b"XAUTOCLAIM", key, group, consumer, b"0", b"1-0", b"COUNT", b"2", b"JUSTID"], b"*3\r\n" + bulk(b"9-0") + b"*1\r\n" + bulk(id) + b"*1\r\n" + bulk(b"3-0")),
        Step([b"XAUTOCLAIM", key, group, consumer, b"0", b"0-0", b"JUSTID"], b"*2\r\n" + bulk(b"0-0") + b"*0\r\n"),
        Step([b"XSETID", key, id, b"ENTRIESADDED", b"5", b"MAXDELETEDID", b"0-0"], b"+OK\r\n"),
        Step([b"XINFO", b"STREAM", key], summary),
        Step([b"XINFO", b"GROUPS", key], groups),
        Step([b"XINFO", b"CONSUMERS", key, group], consumers),
        Step([b"XINFO", b"STREAM", key, b"FULL", b"COUNT", b"1"], full),
        Step([b"XADD", b"k", b"*", b"f", b"v"], b"$-1\r\n"),
        Step([b"XTRIM", b"k", b"MAXLEN", b"~", b"1"], number(-1)),
        Step([b"XCLAIM", b"k", b"g", b"c", b"0", b"1-0", b"JUSTID"], b"*1\r\n" + bulk(b"wrong")),
        Step([b"XAUTOCLAIM", b"k", b"g", b"c", b"0", b"0-0", b"JUSTID"], b"*3\r\n" + bulk(b"0-0") + b"*0\r\n*1\r\n" + bulk(b"wrong")),
        Step([b"XSETID", b"k", b"1-0"], b"+PONG\r\n"),
        Step([b"XINFO", b"STREAM", b"k"], fields([bulk(b"length") + number(0)])),
        Step([b"XINFO", b"GROUPS", b"k"], b"*1\r\n" + fields([bulk(b"name") + bulk(b"a"), bulk(b"name") + bulk(b"b")])),
        Step([b"XINFO", b"CONSUMERS", b"k", b"g"], b"*1\r\n*1\r\n" + bulk(b"name")),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        reject_options(conn)
        mutations(conn, key, group, consumer, id)
        metadata(conn, key, group, consumer, id)
        reject_replies(conn)
    finally:
        conn.disconnect()
    print("Streams completion wire tests: PASS")
