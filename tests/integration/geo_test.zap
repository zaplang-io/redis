import redis
import integration_fixtures

def check(bool ok, str label):
    integration_fixtures.check(ok, label)

def close_to(float64 actual, float64 expected, float64 tolerance, str label):
    check(actual >= expected - tolerance and actual <= expected + tolerance, label + ": " + str(actual))

def member_names(redis.GeoSearchResult result) -> list[bytes]:
    match result:
        case redis.GeoSearchResult.Members(values):
            return values
        case _:
            raise ValueError("Expected GEOSEARCH member-only results")

def hit_rows(redis.GeoSearchResult result) -> list[redis.GeoHit]:
    match result:
        case redis.GeoSearchResult.Hits(values):
            return values
        case _:
            raise ValueError("Expected GEOSEARCH metadata results")

def lookups(redis.Connection conn, integration_fixtures.Fixture f, str protocol):
    auto key = f.binary_key(bytes(protocol) + bytes([255, 0, 13, 10]))
    auto missing = f.key(protocol + "-missing")
    auto binary = bytes([255, 0, 13, 10])
    check(conn.geodist(missing, binary, b"").is_none(), "GEODIST missing key")
    check(conn.geopos(missing, [binary])[0].is_none(), "GEOPOS missing key")
    check(conn.geohash(missing, [binary])[0].is_none(), "GEOHASH missing key")
    check(conn.geoadd(key, [redis.GeoEntry(0.0, 0.0, b"")], xx=True) == 0, "XX cannot create")
    check(conn.key_type(key) == b"none", "XX leaves missing key absent")
    check(conn.geoadd(key, [redis.GeoEntry(0.0, 0.0, b""), redis.GeoEntry(0.01, 0.0, binary)], nx=True) == 2, "insert binary and empty members")
    conn.pexpire(key, 60000)
    check(conn.geoadd(key, [redis.GeoEntry(1.0, 1.0, binary)], nx=True, ch=True) == 0, "NX does not update existing member")
    check(conn.geoadd(key, [redis.GeoEntry(0.02, 0.0, binary)], xx=True, ch=True) == 1, "XX CH counts an update")
    check(conn.geoadd(key, [redis.GeoEntry(0.02, 0.0, binary)], ch=True) == 0, "CH ignores a no-op")
    check(conn.geoadd(key, [redis.GeoEntry(0.01, 0.0, binary)]) == 0, "without CH reports only additions")
    auto positions = conn.geopos(key, [binary, b"absent", b"", binary])
    check(len(positions) == 4 and positions[1].is_none(), "position keeps absent slot")
    auto point = positions[0].unwrap()
    close_to(point.longitude, 0.01, 0.00001, "longitude is first")
    close_to(point.latitude, 0.0, 0.00001, "latitude is second")
    check(positions[3].unwrap().longitude == point.longitude, "position repeats requested members")
    auto hashes = conn.geohash(key, [b"absent", b"", binary, binary])
    check(len(hashes) == 4 and hashes[0].is_none(), "hash keeps absent slot")
    check(len(hashes[1].unwrap()) == 11 and len(hashes[2].unwrap()) == 11, "standard geohash length")
    check(hashes[1].unwrap() != hashes[2].unwrap() and hashes[3].unwrap() == hashes[2].unwrap(), "hash order and duplicates")
    check(conn.geodist(key, b"", b"").unwrap() == 0.0, "distance to self is zero")
    check(conn.geodist(key, b"absent", binary).is_none(), "absent member distance is nil")
    auto meters = conn.geodist(key, b"", binary).unwrap()
    close_to(meters, 1112.0, 2.0, "distance in default meters")
    close_to(conn.geodist(key, binary, b"", unit="m").unwrap(), meters, 0.001, "explicit meters and reverse symmetry")
    close_to(conn.geodist(key, b"", binary, unit="km").unwrap() * 1000.0, meters, 0.1, "kilometers")
    close_to(conn.geodist(key, b"", binary, unit="mi").unwrap() * 1609.34, meters, 0.2, "miles")
    close_to(conn.geodist(key, b"", binary, unit="ft").unwrap() * 0.3048, meters, 0.1, "feet")
    auto hits = hit_rows(conn.geosearch(key, member=Optional.Some(b""), radius=Optional.Some(2.0), unit="km", sort="ASC", withdist=True, withhash=True, withcoord=True))
    check(len(hits) == 2 and hits[0].member == b"" and hits[1].member == binary, "FROMMEMBER supports empty and binary members")
    check(hits[0].distance.unwrap() == 0.0 and hits[1].hash.unwrap() > 0, "zero distance and integer raw hash")
    close_to(hits[1].distance.unwrap() * 1000.0, meters, 0.1, "metadata distance uses search unit")
    check(hits[1].position.unwrap().longitude == point.longitude, "metadata coordinate matches GEOPOS")

def search_and_store(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("search")
    auto destination = f.binary_key(bytes([128, 0, 255]))
    auto distances = f.key("distance-store")
    auto missing = f.key("search-missing")
    auto center = Optional.Some(b"center")
    auto zero = Optional.Some(0.0)
    auto four = Optional.Some(4.0)
    check(conn.geoadd(key, [
        redis.GeoEntry(0.0, 0.0, b"center"),
        redis.GeoEntry(0.01, 0.0, b"east"),
        redis.GeoEntry(-0.025, 0.0, b"west"),
        redis.GeoEntry(0.0, 0.02, b"north"),
    ]) == 4, "seed search points")
    conn.pexpire(key, 60000)
    auto ascending = member_names(conn.geosearch(key, member=center, unit="km", radius=four, sort="ASC"))
    check(len(ascending) == 4 and ascending[0] == b"center" and ascending[1] == b"east" and ascending[2] == b"north" and ascending[3] == b"west", "radius and ASC")
    auto descending = member_names(conn.geosearch(key, longitude=zero, latitude=zero, unit="km", width=Optional.Some(8.0), height=Optional.Some(1.0), sort="DESC"))
    check(len(descending) == 3 and descending[0] == b"west" and descending[1] == b"east" and descending[2] == b"center", "box excludes north and sorts descending")
    auto closest = member_names(conn.geosearch(key, member=center, unit="mi", radius=Optional.Some(3.0), sort="ASC", count=Optional.Some(2)))
    check(len(closest) == 2 and closest[0] == b"center" and closest[1] == b"east", "COUNT without ANY chooses closest")
    auto any = member_names(conn.geosearch(key, member=center, unit="ft", radius=Optional.Some(14000.0), count=Optional.Some(1), any=True))
    check(len(any) == 1, "COUNT ANY returns just one match")
    auto hits = hit_rows(conn.geosearch(key, member=center, radius=Optional.Some(1.0), withcoord=True))
    check(len(hits) == 1 and hits[0].member == b"center" and hits[0].position.is_some() and hits[0].distance.is_none() and hits[0].hash.is_none(), "coordinate-only metadata")
    check(len(member_names(conn.geosearch(missing, longitude=zero, latitude=zero, radius=four))) == 0, "missing index without metadata")
    check(len(hit_rows(conn.geosearch(missing, longitude=zero, latitude=zero, radius=four, withdist=True))) == 0, "missing index with metadata")

    f.put(conn, destination, b"overwritten")
    check(conn.geosearchstore(destination, key, member=center, unit="km", radius=four, sort="ASC", count=Optional.Some(2)) == 2, "store restricted match set")
    conn.pexpire(destination, 60000)
    check(conn.zscore(destination, b"east").unwrap() == conn.zscore(key, b"east").unwrap(), "default store keeps original geo scores")
    check(conn.geopos(destination, [b"east"])[0].is_some() and conn.zscore(destination, b"north").is_none(), "stored index supports geo lookup")
    check(conn.geosearchstore(distances, key, longitude=zero, latitude=zero, unit="km", width=Optional.Some(8.0), height=Optional.Some(1.0), storedist=True) == 3, "box distance store")
    conn.pexpire(distances, 60000)
    close_to(conn.zscore(distances, b"east").unwrap(), 1.112, 0.002, "STOREDIST scores use km")
    check(conn.zcard(key) == 4, "searches and stores preserve source")
    check(conn.geosearchstore(destination, missing, longitude=zero, latitude=zero, radius=four) == 0, "missing source yields zero")
    check(conn.key_type(destination) == b"none", "empty result removes earlier destination")

def validation(redis.Connection conn, integration_fixtures.Fixture f):
    auto key = f.key("validation")
    auto wrong = f.key("wrong-type")
    auto target = f.key("target")
    check(conn.geoadd(key, [
        redis.GeoEntry(-180.0, -85.05112878, b"min"),
        redis.GeoEntry(180.0, 85.05112878, b"max"),
    ]) == 2, "inclusive geographic boundaries")
    conn.pexpire(key, 60000)
    f.put(conn, wrong, b"wrong type")
    f.put(conn, target, b"keep")
    auto errors = 0
    try:
        conn.geoadd(wrong, [redis.GeoEntry(0.0, 0.0, b"m")])
    except redis.ResponseError:
        check(redis.error_prefix(conn.last_error()) == b"WRONGTYPE", "geo add wrong type")
        errors = errors + 1
    try:
        conn.geodist(wrong, b"a", b"b")
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.geopos(wrong, [b"a"])
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.geohash(wrong, [b"a"])
    except redis.ResponseError:
        errors = errors + 1
    auto radius = Optional.Some(1.0)
    auto zero = Optional.Some(0.0)
    try:
        conn.geosearch(wrong, longitude=zero, latitude=zero, radius=radius)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.geosearchstore(target, wrong, longitude=zero, latitude=zero, radius=radius)
    except redis.ResponseError:
        errors = errors + 1
    try:
        conn.geosearch(key, member=Optional.Some(b"missing"), radius=radius)
    except redis.ResponseError:
        errors = errors + 1
    check(errors == 7 and conn.ping(), "geo server errors preserve connection")
    check(conn.get(target).unwrap() == b"keep", "failed store preserves destination")

def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    try:
        f.verify_version(conn)
        lookups(conn, f, "resp2")
        search_and_store(conn, f)
        validation(conn, f)
        auto resp3 = redis.connect_resp3_options(f.config)
        try:
            lookups(resp3, f, "resp3")
        finally:
            resp3.disconnect()
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis geospatial commands: PASS")
