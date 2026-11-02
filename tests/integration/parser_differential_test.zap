import redis
import integration_fixtures

def wire(redis.Reply reply) -> bytes:
    match reply:
        case redis.Reply.Simple(data):
            return b"+" + data + b"\r\n"
        case redis.Reply.Error(data):
            return b"-" + data + b"\r\n"
        case redis.Reply.Integer(value):
            return bytes(":" + str(value) + "\r\n")
        case redis.Reply.Bulk(data):
            return bytes("$" + str(len(data)) + "\r\n") + data + b"\r\n"
        case redis.Reply.NullBulk:
            return b"$-1\r\n"
        case redis.Reply.NullArray:
            return b"*-1\r\n"
        case redis.Reply.Array(items):
            auto out = bytes("*" + str(len(items)) + "\r\n")
            for item in items:
                out = out + wire(item)
            return out


def main():
    auto f = integration_fixtures.fixture()
    auto conn = f.open()
    list[bytes] keys = []
    auto source = b"return {tonumber(ARGV[1]), ARGV[2], false, {ARGV[2]}}"
    auto seed = 322
    try:
        f.verify_version(conn)
        for i in range(0, 64):
            seed = (seed * 251 + 13849) % 65521
            auto n = seed - 32760
            auto binary = bytes([0, 255, 13, 10, seed % 256])
            auto expected = b"*4\r\n" + bytes(":" + str(n) + "\r\n") + b"$5\r\n" + binary + b"\r\n$-1\r\n*1\r\n$5\r\n" + binary + b"\r\n"
            auto decoded = redis.decode(expected)
            integration_fixtures.check(
                decoded.status == "ok",
                "generated differential frame",
            )
            auto actual = conn.eval(source, keys, [bytes(str(n)), binary])
            integration_fixtures.check(
                wire(actual) == wire(decoded.value),
                "Redis RESP2 differential seed 322",
            )
    finally:
        conn.disconnect()
        f.cleanup()
    print("Live Redis RESP2 differential seed 322: PASS")
