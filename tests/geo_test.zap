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
                "exact geo wire command at step " + str(number) + " (" + str(step.args[0]) + ")",
            )
            net.send(socket, step.reply)
            number = number + 1
    finally:
        net.close_conn(socket)

def reject_options(redis.Connection conn):
    auto errors = 0
    try:
        conn.geoadd(b"k", [])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, 0.0, b"m")], nx=True, xx=True)
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, 0.0, b"ok"), redis.GeoEntry(-180.01, 0.0, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(180.01, 0.0, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, 85.05112879, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, -85.05112879, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(math.nan, 0.0, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, math.inf, b"bad")])
    except ValueError:
        errors = errors + 1
    try:
        conn.geodist(b"k", b"a", b"b", unit="yd")
    except ValueError:
        errors = errors + 1
    auto member = Optional.Some(b"a")
    auto radius = Optional.Some(1.0)
    try:
        conn.geosearch(b"k", radius=radius)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", longitude=Optional.Some(0.0), radius=radius)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, latitude=Optional.Some(0.0), radius=radius)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, width=radius)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, width=radius, height=radius)
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=Optional.Some(-1.0))
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, width=radius, height=Optional.Some(math.inf))
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, unit="yd")
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, sort="sideways")
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, count=Optional.Some(0))
    except ValueError:
        errors = errors + 1
    try:
        conn.geosearchstore(b"d", b"k", member=member, radius=radius, any=True)
    except ValueError:
        errors = errors + 1
    check(errors == 21, "invalid geo coordinates and option combinations reject before I/O")

def lookups(redis.Connection conn, bytes key, bytes member):
    check(conn.geoadd(name=key, values=[redis.GeoEntry(-180.0, 4.5, member), redis.GeoEntry(180.0, -4.5, b"")], nx=True, ch=True) == 2, "GEOADD order and binary tokens")
    check(conn.geodist(name=key, place1=member, place2=b"", unit="km").unwrap() == 1.25, "GEODIST parses numeric bulk")
    check(conn.geodist(key, member, b"absent").is_none(), "GEODIST missing")
    auto positions = conn.geopos(key, [member, b"absent", member])
    auto position = positions[0].unwrap()
    auto repeated = positions[2].unwrap()
    check(len(positions) == 3 and positions[1].is_none(), "GEOPOS preserves missing member slot")
    check(position.longitude == -12.5 and position.latitude == 4.25 and repeated.longitude == -12.5, "GEOPOS coordinate order and repeated members")
    auto hashes = conn.geohash(key, [member, b"absent", b""])
    check(len(hashes) == 3 and hashes[0].unwrap() == b"sqc8b49rny0" and hashes[1].is_none() and hashes[2].unwrap() == b"00000000000", "GEOHASH preserves missing and binary member slots")
    check(len(conn.geopos(key, [])) == 0 and len(conn.geohash(key, [])) == 0, "empty lookup arrays")

def searches(redis.Connection conn, bytes key, bytes member):
    match conn.geosearch(key, member=Optional.Some(b""), radius=Optional.Some(0.0), sort="asc"):
        case redis.GeoSearchResult.Members(values):
            check(len(values) == 2 and values[0] == member and values[1] == b"", "bare member results")
        case _:
            raise ValueError("Expected members")
    match conn.geosearch(
        key, longitude=Optional.Some(-12.5), latitude=Optional.Some(4.25),
        unit="ft", width=Optional.Some(2.0), height=Optional.Some(3.0),
        sort="DESC", count=Optional.Some(1), any=True, withcoord=True, withdist=True, withhash=True,
    ):
        case redis.GeoSearchResult.Hits(values):
            check(len(values) == 1, "one metadata row")
            auto hit = values[0]
            auto position = hit.position.unwrap()
            check(hit.member == member and hit.distance.unwrap() == 1.25 and hit.hash.unwrap() == 3479099956230698, "GEOSEARCH metadata order and 52-bit hash")
            check(position.longitude == -12.5 and position.latitude == 4.25, "GEOSEARCH coordinates")
        case _:
            raise ValueError("Expected hits")
    match conn.geosearch(key, member=Optional.Some(member), radius=Optional.Some(2.0), unit="mi", withhash=True):
        case redis.GeoSearchResult.Hits(values):
            auto hit = values[0]
            check(hit.hash.unwrap() == 42 and hit.distance.is_none() and hit.position.is_none(), "unrequested metadata stays absent")
        case _:
            raise ValueError("Expected hash result")
    check(conn.geosearchstore(dest=member, name=key, member=Optional.Some(b""), unit="km", radius=Optional.Some(1.0), count=Optional.Some(2), any=True, storedist=True) == 2, "GEOSEARCHSTORE order and STOREDIST")

def reject_replies(redis.Connection conn):
    auto errors = 0
    try:
        conn.geoadd(b"k", [redis.GeoEntry(0.0, 0.0, b"m")])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geodist(b"k", b"a", b"b")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geodist(b"k", b"a", b"b")
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geopos(b"k", [b"a", b"b"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geopos(b"k", [b"a"])
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geohash(b"k", [b"a"])
    except redis.ProtocolError:
        errors = errors + 1
    auto member = Optional.Some(b"a")
    auto radius = Optional.Some(1.0)
    try:
        conn.geosearch(b"k", member=member, radius=radius)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, withcoord=True)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geosearch(b"k", member=member, radius=radius, withhash=True)
    except redis.ProtocolError:
        errors = errors + 1
    try:
        conn.geosearchstore(b"d", b"k", member=member, radius=radius)
    except redis.ProtocolError:
        errors = errors + 1
    check(errors == 10 and conn.ping(), "invalid geo replies are consumed and connection remains usable")

def main():
    auto key = bytes([0, 255, 13, 10])
    auto member = bytes([255, 0, 128])
    list[Step] steps = [
        Step([b"GEOADD", key, b"NX", b"CH", b"-180", b"4.5", member, b"180", b"-4.5", b""], b":2\r\n"),
        Step([b"GEODIST", key, member, b"", b"km"], b"$4\r\n1.25\r\n"),
        Step([b"GEODIST", key, member, b"absent"], b"$-1\r\n"),
        Step([b"GEOPOS", key, member, b"absent", member], b"*3\r\n*2\r\n$5\r\n-12.5\r\n$4\r\n4.25\r\n*-1\r\n*2\r\n$5\r\n-12.5\r\n$4\r\n4.25\r\n"),
        Step([b"GEOHASH", key, member, b"absent", b""], b"*3\r\n$11\r\nsqc8b49rny0\r\n$-1\r\n$11\r\n00000000000\r\n"),
        Step([b"GEOPOS", key], b"*0\r\n"),
        Step([b"GEOHASH", key], b"*0\r\n"),
        Step([b"GEOSEARCH", key, b"FROMMEMBER", b"", b"BYRADIUS", b"0", b"m", b"ASC"], b"*2\r\n$3\r\n" + member + b"\r\n$0\r\n\r\n"),
        Step(
            [b"GEOSEARCH", key, b"FROMLONLAT", b"-12.5", b"4.25", b"BYBOX", b"2", b"3", b"ft", b"DESC", b"COUNT", b"1", b"ANY", b"WITHCOORD", b"WITHDIST", b"WITHHASH"],
            b"*1\r\n*4\r\n$3\r\n" + member + b"\r\n$4\r\n1.25\r\n:3479099956230698\r\n*2\r\n$5\r\n-12.5\r\n$4\r\n4.25\r\n",
        ),
        Step([b"GEOSEARCH", key, b"FROMMEMBER", member, b"BYRADIUS", b"2", b"mi", b"WITHHASH"], b"*1\r\n*2\r\n$3\r\n" + member + b"\r\n:42\r\n"),
        Step([b"GEOSEARCHSTORE", member, key, b"FROMMEMBER", b"", b"BYRADIUS", b"1", b"km", b"COUNT", b"2", b"ANY", b"STOREDIST"], b":2\r\n"),
        Step([b"GEOADD", b"k", b"0", b"0", b"m"], b":2\r\n"),
        Step([b"GEODIST", b"k", b"a", b"b"], b"$2\r\n-1\r\n"),
        Step([b"GEODIST", b"k", b"a", b"b"], b"$3\r\ninf\r\n"),
        Step([b"GEOPOS", b"k", b"a", b"b"], b"*1\r\n$-1\r\n"),
        Step([b"GEOPOS", b"k", b"a"], b"*1\r\n*1\r\n$1\r\n0\r\n"),
        Step([b"GEOHASH", b"k", b"a"], b"*1\r\n:5\r\n"),
        Step([b"GEOSEARCH", b"k", b"FROMMEMBER", b"a", b"BYRADIUS", b"1", b"m"], b"*1\r\n$-1\r\n"),
        Step([b"GEOSEARCH", b"k", b"FROMMEMBER", b"a", b"BYRADIUS", b"1", b"m", b"WITHCOORD"], b"*1\r\n*1\r\n$1\r\na\r\n"),
        Step([b"GEOSEARCH", b"k", b"FROMMEMBER", b"a", b"BYRADIUS", b"1", b"m", b"WITHHASH"], b"*1\r\n*2\r\n$1\r\na\r\n:-1\r\n"),
        Step([b"GEOSEARCHSTORE", b"d", b"k", b"FROMMEMBER", b"a", b"BYRADIUS", b"1", b"m"], b":-1\r\n"),
        Step([b"PING"], b"+PONG\r\n"),
    ]
    auto listener = net.listen("127.0.0.1", 0)
    spawn server(listener, steps)
    auto conn = redis.connect(port=net.port(listener), command_timeout_ms=2000)
    try:
        reject_options(conn)
        lookups(conn, key, member)
        searches(conn, key, member)
        reject_replies(conn)
    finally:
        conn.disconnect()
    print("Geospatial wire tests: PASS")
