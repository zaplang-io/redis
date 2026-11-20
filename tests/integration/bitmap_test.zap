import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def basic(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.binary_key(bytes([0, 255, 13, 10]))
    check(conn.getbit(key, 0) == 0 and conn.bitcount(key) == 0, "missing bitmap reads")
    check(conn.bitpos(key, 1) == -1 and conn.bitpos(key, 0) == 0, "missing bitmap positions")
    check(conn.setbit(key, 0, 1) == 0 and conn.setbit(key, 0, 1) == 1, "SETBIT old value")
    conn.pexpire(key, 60000)
    check(conn.setbit(key, 9, 1) == 0, "SETBIT extends with zeros")
    check(conn.get(key).unwrap() == bytes([128, 64]), "offset zero is the MSB")
    check(conn.getbit(key, 9) == 1 and conn.getbit(key, 10) == 0 and conn.getbit(key, 128) == 0, "read present and zero-filled bits")
    check(conn.setbit(key, 0, 0) == 1, "clear a bit")
    check(conn.bitcount(key) == 1, "count whole bitmap")
    check(conn.bitcount(key, Optional.Some(0), Optional.Some(0), mode="BYTE") == 0, "first byte excludes second")
    check(conn.bitcount(key, Optional.Some(-1), Optional.Some(-1)) == 1, "negative byte indexes")
    check(conn.bitcount(key, Optional.Some(9), Optional.Some(9), mode="BIT") == 1, "inclusive bit endpoint")
    check(conn.bitcount(key, Optional.Some(-7), Optional.Some(-7), mode="BIT") == 1, "negative bit index")
    check(conn.bitcount(key, Optional.Some(9), Optional.Some(8), mode="BIT") == 0, "empty reversed bit range")
    check(conn.bitpos(key, 1) == 9, "first set position is absolute")
    check(conn.bitpos(key, 1, start=Optional.Some(1)) == 9, "start-only byte range")
    check(conn.bitpos(key, 1, Optional.Some(0), Optional.Some(0), mode="BYTE") == -1, "no hit in byte range")
    check(conn.bitpos(key, 1, Optional.Some(-7), Optional.Some(-7), mode="BIT") == 9, "negative bit endpoints preserve absolute position")
    auto full = f.key("full")
    f.put(conn, full, bytes([255]))
    check(conn.bitpos(full, 0) == 8, "open zero search includes zero padding")
    check(conn.bitpos(full, 0, Optional.Some(0), Optional.Some(0)) == -1, "bounded zero search excludes padding")

def bitops(redis.Connection conn, integration_fixtures.Fixture f):
    auto a = f.key("a")
    auto b = f.binary_key(bytes([128, 13]))
    auto dest = f.key("dst")
    auto missing = f.key("missing")
    f.put(conn, a, bytes([240, 170]))
    f.put(conn, b, bytes([204]))
    check(conn.bitop("and", dest, [a, b]) == 2, "AND uses longest input length")
    conn.pexpire(dest, 60000)
    check(conn.get(dest).unwrap() == bytes([192, 0]), "AND zero-pads short source")
    check(conn.bitop("OR", dest, [a, b, missing]) == 2, "OR missing input")
    conn.pexpire(dest, 60000)
    check(conn.get(dest).unwrap() == bytes([252, 170]), "OR output")
    check(conn.bitop("XOR", dest, [a, b, b]) == 2, "XOR duplicate sources")
    conn.pexpire(dest, 60000)
    check(conn.get(dest).unwrap() == bytes([240, 170]), "XOR duplicate cancellation")
    check(conn.bitop("not", b, [b]) == 1, "NOT with aliased destination")
    conn.pexpire(b, 60000)
    check(conn.get(b).unwrap() == bytes([51]), "NOT output")
    check(conn.bitop("OR", dest, [missing]) == 0 and conn.get(dest).is_none(), "all missing deletes destination")

def fields(redis.Connection conn, bytes key):
    auto builder = conn.bitfield(key)
    auto read = builder.get("u8", "0").execute()
    check(len(read) == 1 and read[0].unwrap() == 0, "GET on missing field")
    check(conn.get(key).is_none(), "GET does not create a key")
    builder.set_value("u4", "#1", 15).get("u4", "4")
    builder.incrby("u4", "#1", 2)
    builder.overflow("SAT").incrby("u4", "#1", 30)
    builder.incrby("u4", "#1", 1, overflow="FAIL")
    builder.get("u4", "#1")
    builder.set_value("u4", "#1", 16)
    builder.overflow("WRAP").set_value("i8", "#1", -5).get("i8", "8")
    auto values = builder.execute()
    conn.pexpire(key, 60000)
    check(len(values) == 9, "ordered reply per GET, SET, INCRBY")
    check(values[0].unwrap() == 0 and values[1].unwrap() == 15, "SET returns old value and GET reads updated field")
    check(values[2].unwrap() == 1 and values[3].unwrap() == 15, "wrap and saturate increments")
    check(values[4].is_none() and values[5].unwrap() == 15 and values[6].is_none(), "FAIL preserves field on INCRBY and SET")
    check(values[7].unwrap() == 0 and values[8].unwrap() == -5, "signed field round trip")
    check(conn.get(key).unwrap() == bytes([15, 251]), "packed byte representation")
    check(len(builder.execute()) == 0, "execution clears operations")
    builder.incrby("u4", "#1", 1)
    check(builder.execute()[0].unwrap() == 0, "execution restores default wrap")
    auto default_fail = conn.bitfield(key, default_overflow="FAIL")
    default_fail.incrby("u4", "#1", -1)
    check(default_fail.execute()[0].is_none(), "custom default overflow")
    default_fail.overflow("WRAP").incrby("u4", "#1", -1)
    default_fail.reset()
    default_fail.incrby("u4", "#1", -1)
    check(default_fail.execute()[0].is_none(), "manual reset restores default without applying discarded work")

def server_errors(redis.Connection conn, integration_fixtures.Fixture f):
    auto wrong = f.key("wrong")
    auto dest = f.key("wrong-dest")
    auto key = f.key("invalid-field")
    conn.sadd(wrong, [b"x"])
    conn.pexpire(wrong, 60000)
    auto errors = 0
    try:
        conn.setbit(wrong, 0, 1)
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "SETBIT WRONGTYPE")
        errors = errors + 1
    try:
        conn.getbit(wrong, 0)
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "GETBIT WRONGTYPE")
        errors = errors + 1
    try:
        conn.bitcount(wrong)
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "BITCOUNT WRONGTYPE")
        errors = errors + 1
    try:
        conn.bitpos(wrong, 1)
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "BITPOS WRONGTYPE")
        errors = errors + 1
    try:
        conn.bitop("AND", dest, [wrong])
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "BITOP WRONGTYPE")
        errors = errors + 1
    try:
        conn.bitfield(wrong).get("u8", "0").execute()
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.bitop("invalid", dest, [key])
    except redis.ResponseError:
        errors = errors + 1
    auto builder = conn.bitfield(key)
    try:
        builder.set_value("u8", "0", 1).get("u64", "0").execute()
    except redis.ResponseError:
        errors = errors + 1
    check(conn.get(key).is_none(), "invalid format rejects batch before applying writes")
    try:
        builder.get("u8", "#-1").execute()
    except redis.ResponseError:
        errors = errors + 1
    check(builder.get("u8", "0").execute()[0].unwrap() == 0, "server error clears queue")
    check(errors == 9 and conn.ping(), "bitmap server errors preserve connection")

def main():
    auto f = integration_fixtures.fixture()
    auto key2 = f.key("field-resp2")
    auto key3 = f.key("field-resp3")
    auto conn = f.open()
    try:
        f.verify_version(conn)
        basic(conn, f)
        bitops(conn, f)
        fields(conn, key2)
        server_errors(conn, f)
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            fields(resp3, key3)
        finally:
            resp3.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis bitmap commands: PASS")
