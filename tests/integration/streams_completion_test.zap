import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def entry_data() -> list[redis.StreamField]:
    auto field = bytes([0, 255, 13, 10])
    return [redis.StreamField(field, b""), redis.StreamField(field, bytes([255, 0, 128]))]

def trim_and_setid(redis.Connection conn, integration_fixtures.Fixture fixture):
    auto key = fixture.binary_key(bytes([255, 0, 13, 10]))
    auto missing = fixture.key("missing")
    auto data = entry_data()
    auto one = Optional.Some(1)
    auto two = Optional.Some(2)
    auto zero = Optional.Some(0)
    check(conn.xadd(missing, data, nomkstream=True).is_none(), "NOMKSTREAM missing returns None")
    check(conn.key_type(missing) == b"none" and conn.xtrim(missing, one) == 0, "missing trim does not create")
    check(conn.xadd(key, data, b"1-*").unwrap() == b"1-0", "server selects sequence for millisecond")
    conn.pexpire(key, 60000)
    check(conn.xadd(key, data, b"1-*", nomkstream=True).unwrap() == b"1-1", "NOMKSTREAM appends to existing key")
    conn.xadd(key, data, b"2-0")
    check(conn.xtrim(key, two, approximate=False) == 1, "exact MAXLEN removes oldest")
    check(conn.xrange(key)[0].id == b"1-1", "MAXLEN kept next entry")
    check(conn.xadd(key, data, b"3-0", minid=Optional.Some(b"2-0"), approximate=False).unwrap() == b"3-0", "XADD exact MINID")
    check(conn.xlen(key) == 2 and conn.xrange(key)[0].id == b"2-0", "MINID retains threshold")
    check(conn.xadd(key, data, b"4-0", maxlen=one, approximate=False).unwrap() == b"4-0", "XADD exact MAXLEN")
    check(conn.xtrim(key, minid=Optional.Some(b"4-0"), approximate=False) == 0 and conn.xlen(key) == 1, "exact MINID equality")
    check(conn.xtrim(key, minid=Optional.Some(b"5"), approximate=False) == 1 and conn.xlen(key) == 0, "exact MINID trims through lower ID")
    check(conn.xadd(key, data, b"5-0", maxlen=zero, limit=zero).unwrap() == b"5-0" and conn.xlen(key) == 0, "approximate XADD MAXLEN 0 removes complete nodes")
    match conn.xinfo_stream(key).details:
        case redis.StreamInfoDetails.Summary(groups, first_entry, last_entry):
            check(groups == 0 and first_entry.is_none() and last_entry.is_none(), "empty stream first and last entries are nil")
        case _:
            raise ValueError("Expected stream summary")
    auto last = b"9223372036854775808-0"
    check(conn.xsetid(key, last, entries_added=Optional.Some(7), max_deleted_id=Optional.Some(b"5-0")), "XSETID on empty existing stream")
    auto info = conn.xinfo_stream(key)
    check(info.last_generated_id == last and info.max_deleted_entry_id == b"5-0" and info.entries_added == 7, "XSETID metadata with uint64 ID")
    check(conn.xadd(key, data, b"9223372036854775808-*").unwrap() == b"9223372036854775808-1", "next generated sequence follows XSETID")
    auto errors = 0
    try:
        conn.xsetid(missing, b"0-0")
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xsetid(key, last)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xsetid(key, b"18446744073709551615-0", entries_added=zero)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xsetid(key, last, max_deleted_id=Optional.Some(b"18446744073709551615-0"))
    except redis.ResponseError:
        errors = errors + 1
    check(errors == 4 and conn.ping(), "server rejects invalid XSETID state")
    check(conn.xinfo_stream(key).last_generated_id == b"9223372036854775808-1", "rejected XSETID preserves last ID")

def approximate_trim(redis.Connection conn, integration_fixtures.Fixture fixture):
    auto key = fixture.key("approximate")
    auto data = entry_data()
    auto i = 1
    while i <= 240:
        conn.xadd(key, data, bytes(str(i) + "-0"))
        if i == 1:
            conn.pexpire(key, 60000)
        i = i + 1
    auto trimmed = conn.xtrim(key, maxlen=Optional.Some(80), limit=Optional.Some(100))
    auto remaining = conn.xlen(key)
    check(trimmed + remaining == 240 and trimmed <= 100 and remaining >= 80, "approximate trim honors LIMIT and preserves complete nodes")
    check(conn.xadd(key, data, b"241-0", minid=Optional.Some(b"130-0"), limit=Optional.Some(0)).unwrap() == b"241-0", "approximate XADD MINID")
    auto entries = conn.xrange(key, count=1)
    check(len(entries) == 1 and conn.xlen(key) >= 112 and conn.xlen(key) <= remaining + 1, "approximate MINID retains all IDs at or above threshold")
    check(conn.xrevrange(key, count=1)[0].id == b"241-0", "approximate trim retains newest entry")

def ids(redis.StreamClaimResult result) -> list[bytes]:
    match result:
        case redis.StreamClaimResult.Ids(values):
            return values
        case _:
            raise ValueError("Expected IDs from XCLAIM JUSTID")

def entries(redis.StreamClaimResult result) -> list[redis.StreamEntry]:
    match result:
        case redis.StreamClaimResult.Entries(values):
            return values
        case _:
            raise ValueError("Expected entries from XCLAIM")

def inspect_claim(redis.Connection conn, bytes key, bytes group, bytes claimant):
    auto info = conn.xinfo_stream(key, True, count=Optional.Some(0))
    match info.details:
        case redis.StreamInfoDetails.Full(stream_entries, groups):
            check(len(stream_entries) == 4 and len(groups) == 1 and groups[0].name == group, "FULL count zero returns all stream entries")
            auto g = groups[0]
            check(g.pending_count == 4 and len(g.pending) == 4, "FORCE adds previously undelivered entry to PEL")
            auto pending = g.pending[1]
            check(pending.id == b"2-0" and pending.consumer.unwrap() == claimant, "claim transfers ownership")
            check(pending.deliveries == 7 and pending.delivery_time_ms == 1, "TIME sets milliseconds and JUSTID preserves RETRYCOUNT")
        case _:
            raise ValueError("Expected FULL stream info")

def metadata(redis.Connection conn, bytes key, bytes group, bytes claimant):
    auto info = conn.xinfo_stream(key)
    check(info.length == 3 and info.entries_added == 4 and info.last_generated_id == b"4-0" and info.max_deleted_entry_id == b"1-0", "summary after deletion")
    match info.details:
        case redis.StreamInfoDetails.Summary(groups, first_entry, last_entry):
            check(groups == 1 and first_entry.unwrap().id == b"2-0" and last_entry.unwrap().id == b"4-0", "summary first and last after deletion")
        case _:
            raise ValueError("Expected stream summary")
    auto groups = conn.xinfo_groups(key)
    check(len(groups) == 1 and groups[0].name == group and groups[0].pending == 2 and groups[0].last_delivered_id == b"3-0", "group pending and delivery metadata")
    auto consumers = conn.xinfo_consumers(key, group)
    auto found = False
    for consumer in consumers:
        if consumer.name == claimant:
            found = consumer.inactive_ms.is_some()
    check(found, "successful consumer exposes inactive time")
    auto full = conn.xinfo_stream(key, full=True, count=Optional.Some(1))
    match full.details:
        case redis.StreamInfoDetails.Full(stream_entries, full_groups):
            check(len(stream_entries) == 1 and len(full_groups) == 1, "FULL COUNT caps entries")
            auto g = full_groups[0]
            check(g.pending_count == 2 and len(g.pending) == 1, "FULL COUNT caps pending samples but preserves the total")
            check(g.pending[0].id == b"2-0" and g.pending[0].deliveries == 7, "XAUTOCLAIM JUSTID preserves retry count")
        case _:
            raise ValueError("Expected FULL stream details")

def claim(redis.Connection conn, integration_fixtures.Fixture fixture):
    auto key = fixture.key("groups")
    auto group = bytes([255, 0, 13])
    auto consumer = bytes([128, 0])
    auto claimant = bytes([255, 1])
    auto next_consumer = bytes([255, 2])
    auto data = entry_data()
    conn.xadd(key, data, b"1-0")
    conn.pexpire(key, 60000)
    conn.xadd(key, data, b"2-0")
    conn.xadd(key, data, b"3-0")
    conn.xadd(key, data, b"4-0")
    check(len(conn.xinfo_groups(key)) == 0, "stream without groups")
    conn.xgroup_create(key, group, b"0-0")
    check(len(conn.xinfo_consumers(key, group)) == 0, "group without consumers")
    auto batch = conn.xreadgroup(group, consumer, [redis.StreamCursor(key, b">")], count=3)
    check(len(batch[0].entries) == 3, "three pending entries")
    check(len(entries(conn.xclaim(key, group, claimant, 3600000, [b"2-0"]))) == 0, "minimum idle skips fresh delivery")
    check(len(entries(conn.xclaim(key, group, claimant, 0, [b"4-0"]))) == 0, "no claim for nonpending ID without FORCE")
    auto claimed = ids(conn.xclaim(key, group, claimant, 0, [b"2-0", b"4-0"], idle=Optional.Some(10000), retrycount=Optional.Some(7), force=True, justid=True))
    check(len(claimed) == 2 and claimed[0] == b"2-0" and claimed[1] == b"4-0", "JUSTID transfer and forced claim")
    check(len(ids(conn.xclaim(key, group, claimant, 0, [b"2-0"], time=Optional.Some(1), justid=True))) == 1, "TIME claim")
    inspect_claim(conn, key, group, claimant)
    check(conn.xdel(key, [b"1-0", b"1-0", b"9-0"]) == 1, "XDEL duplicates and absent IDs")
    auto scan = conn.xautoclaim(key, group, next_consumer, 0, count=2, justid=True)
    check(len(scan.deleted_ids) == 1 and scan.deleted_ids[0] == b"1-0" and len(scan.ids) == 1 and scan.ids[0] == b"2-0" and scan.next_id != b"0-0", "JUSTID scan preserves deleted IDs and a continuation")
    auto rest = conn.xautoclaim(key, group, claimant, 0, scan.next_id)
    check(rest.next_id == b"0-0" and len(rest.entries) == 2 and len(rest.ids) == 0, "continuation claims remaining entries")
    check(rest.entries[0].id == b"3-0" and rest.entries[0].fields[0].field == data[0].field and len(rest.entries[0].fields) == 2, "claimed entries preserve ordered binary fields")
    check(conn.xack(key, group, [b"3-0"]) == 1, "acknowledge an entry")
    metadata(conn, key, group, claimant)
    auto resp3 = redis.connect_resp3_options(fixture.config)
    try:
        metadata(resp3, key, group, claimant)
    finally:
        resp3.disconnect()

def wrong_type(redis.Connection conn, integration_fixtures.Fixture fixture):
    auto key = fixture.key("wrong-type")
    auto missing = fixture.key("absent-info")
    fixture.put(conn, key, b"string")
    auto errors = 0
    try:
        conn.xtrim(key, Optional.Some(1))
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "trim WRONGTYPE")
        errors = errors + 1
    try:
        conn.xadd(key, entry_data(), nomkstream=True)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xinfo_stream(missing)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xinfo_groups(key)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.xclaim(key, b"g", b"c", 0, [b"1-0"])
    except redis.ResponseError:
        errors = errors + 1
    check(errors == 5 and conn.get(key).unwrap() == b"string" and conn.ping(), "stream server errors preserve connection and key")

def main():
    auto fixture = integration_fixtures.fixture()
    auto conn = fixture.open()
    try:
        fixture.verify_version(conn)
        trim_and_setid(conn, fixture)
        approximate_trim(conn, fixture)
        claim(conn, fixture)
        wrong_type(conn, fixture)
    finally:
        conn.disconnect()
        fixture.cleanup()
    print("Live Redis streams completion: PASS")
