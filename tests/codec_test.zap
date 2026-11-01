import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

# A readable tree signature for assertions (bulk data itself stays binary).
def signature(redis.Reply value) -> str:
    match value:
        case redis.Reply.Simple(data):
            return "simple:" + hex.encode(data)
        case redis.Reply.Error(data):
            return "error:" + hex.encode(data)
        case redis.Reply.Integer(value):
            return "int:" + str(value)
        case redis.Reply.Bulk(data):
            return "bulk:" + hex.encode(data)
        case redis.Reply.NullBulk:
            return "null-bulk"
        case redis.Reply.NullArray:
            return "null-array"
        case redis.Reply.Array(items):
            auto out = "["
            auto i = 0
            while i < len(items):
                out = out + signature(items[i]) + ";"
                i = i + 1
            return out + "]"

def valid(bytes wire, str want):
    auto result = redis.decode(wire)
    check(result.status == "ok", "valid status: " + want)
    check(result.consumed == len(wire), "exact consumption: " + want)
    check(signature(result.value) == want, "value: " + want)
    # Every proper prefix is incomplete, including CRLF and payload splits.
    auto i = 0
    while i < len(wire):
        auto prefix = redis.decode(wire[0:i])
        check(prefix.status == "incomplete", "fragment " + str(i) + " of " + want)
        check(prefix.consumed == 0, "fragment consumed no input")
        i = i + 1
    auto exact = redis.decode(wire, max_frame=len(wire))
    check(exact.status == "ok", "exact byte budget")

def invalid(bytes wire):
    auto result = redis.decode(wire)
    check(result.status == "invalid", "invalid frame: " + hex.encode(wire))
    check(result.consumed == 0, "invalid consumed no input")
    check(len(result.message) > 0, "invalid has diagnostic")

def main():
    check(redis.encode_command([b"PING"]) == b"*1\r\n$4\r\nPING\r\n", "PING encoding")
    check(
        redis.encode_command([b"SET", b"key", b""]) == b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
        "empty argument",
    )
    auto binary = bytes([0, 255, 13, 10, 128])
    check(
        redis.encode_command([b"ECHO", binary]) == b"*2\r\n$4\r\nECHO\r\n$5\r\n" + binary + b"\r\n",
        "binary argument",
    )
    check(
        redis.encode_command([bytes("café")]) == b"*1\r\n$5\r\ncafé\r\n",
        "UTF-8 byte length",
    )
    auto rejected = False
    try:
        list[bytes] empty = []
        redis.encode_command(empty)
    except ValueError:
        rejected = True
    check(rejected, "empty command rejected")

    valid(b"+OK\r\n", "simple:4f4b")
    valid(b"+\r\n", "simple:")
    valid(b"-ERR bad\r\n", "error:45525220626164")
    valid(b":0\r\n", "int:0")
    valid(b":+42\r\n", "int:42")
    valid(b":-42\r\n", "int:-42")
    valid(b":9223372036854775807\r\n", "int:9223372036854775807")
    valid(b":-9223372036854775808\r\n", "int:-9223372036854775808")
    valid(b"$0\r\n\r\n", "bulk:")
    valid(b"$5\r\nhello\r\n", "bulk:68656c6c6f")
    valid(b"$5\r\n" + binary + b"\r\n", "bulk:00ff0d0a80")
    valid(b"$5\r\ncafé\r\n", "bulk:636166c3a9")
    valid(b"$-1\r\n", "null-bulk")
    valid(b"*-1\r\n", "null-array")
    valid(b"*0\r\n", "[]")
    valid(b"*3\r\n$1\r\nx\r\n$-1\r\n:2\r\n", "[bulk:78;null-bulk;int:2;]")
    valid(
        b"*2\r\n*2\r\n+OK\r\n-ERR\r\n*-1\r\n",
        "[[simple:4f4b;error:455252;];null-array;]",
    )

    invalid(b"?\r\n")
    invalid(b"_\r\n")
    invalid(b"+bad\n")
    invalid(b"+bad\rX")
    invalid(b":\r\n")
    invalid(b":+\r\n")
    invalid(b":-\r\n")
    invalid(b": 1\r\n")
    invalid(b":1a\r\n")
    invalid(b":9223372036854775808\r\n")
    invalid(b":-9223372036854775809\r\n")
    invalid(b"$\r\n")
    invalid(b"$-2\r\n")
    invalid(b"$-0\r\n")
    invalid(b"$-01\r\n")
    invalid(b"$+1\r\n")
    invalid(b"$1\r\naXY")
    invalid(b"$9223372036854775807\r\n")
    invalid(b"*-2\r\n")
    invalid(b"*+1\r\n")
    invalid(b"*1\r\n?\r\n")
    invalid(b"*9223372036854775807\r\n")

    check(redis.decode(b"+OK\r\n", max_frame=4).status == "invalid", "line byte limit")
    check(
        redis.decode(b"$4\r\n", max_frame=9).status == "invalid",
        "declared bulk limit",
    )
    check(
        redis.decode(b"*2\r\n", max_frame=9).status == "invalid",
        "declared array limit",
    )
    check(
        redis.decode(b"+OK\r\n", max_frame=2).status == "invalid",
        "invalid byte limit",
    )
    check(
        redis.decode(b"+OK\r\n", max_depth=-1).status == "invalid",
        "invalid depth limit",
    )
    check(redis.decode(b"+OK\r\n", max_depth=0).status == "ok", "scalar depth zero")
    check(redis.decode(b"*0\r\n", max_depth=0).status == "invalid", "array depth zero")
    check(
        redis.decode(b"*1\r\n*0\r\n", max_depth=1).status == "invalid",
        "nested depth limit",
    )
    check(
        redis.decode(b"*1\r\n*0\r\n", max_depth=2).status == "ok",
        "exact depth limit",
    )
    check(redis.decode(b"*-1\r\n", max_depth=0).status == "ok", "null array depth zero")

    auto joined = b"+OK\r\n:42\r\n$-1\r\n"
    auto first = redis.decode(joined, max_frame=5)
    check(first.status == "ok", "coalesced buffer exceeds individual frame limit")
    check(first.consumed == 5, "first coalesced frame")
    auto rest = joined[first.consumed:len(joined)]
    auto second = redis.decode(rest)
    check(signature(second.value) == "int:42", "second coalesced frame")
    auto third = redis.decode(rest[second.consumed:len(rest)])
    check(signature(third.value) == "null-bulk", "third coalesced frame")
    auto trailing = redis.decode(b"+OK\r\n?")
    check(trailing.status == "ok", "trailing malformed frame untouched")
    check(trailing.consumed == 5, "trailing input preserved")
    print("RESP2 codec: PASS")
