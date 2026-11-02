import redis

def check(bool ok, str label):
    if not ok:
        raise ValueError("FAIL: " + label)

def items_sig(list[redis.Resp3] items) -> str:
    auto out = ""
    for item in items:
        out = out + signature(item) + ";"
    return out

def pairs_sig(list[redis.Resp3Pair] pairs) -> str:
    auto out = ""
    for pair in pairs:
        out = out + signature(pair.key) + "=" + signature(pair.value) + ";"
    return out

def signature(redis.Resp3 value) -> str:
    match value:
        case redis.Resp3.Simple(data):
            return "simple:" + hex.encode(data)
        case redis.Resp3.Error(data):
            return "error:" + hex.encode(data)
        case redis.Resp3.Integer(n):
            return "int:" + str(n)
        case redis.Resp3.Bulk(data):
            return "bulk:" + hex.encode(data)
        case redis.Resp3.Array(items):
            return "array[" + items_sig(items) + "]"
        case redis.Resp3.NullBulk:
            return "null-bulk"
        case redis.Resp3.NullArray:
            return "null-array"
        case redis.Resp3.Null:
            return "null"
        case redis.Resp3.Boolean(flag):
            return "bool:" + str(flag)
        case redis.Resp3.Double(n):
            if math.isnan(n):
                return "double:nan"
            return "double:" + format(n, ".17g")
        case redis.Resp3.BigNumber(decimal):
            return "big:" + str(decimal)
        case redis.Resp3.BlobError(data):
            return "blob-error:" + hex.encode(data)
        case redis.Resp3.Verbatim(kind, data):
            return "verbatim:" + hex.encode(kind) + ":" + hex.encode(data)
        case redis.Resp3.Map(pairs):
            return "map[" + pairs_sig(pairs) + "]"
        case redis.Resp3.Set(items):
            return "set[" + items_sig(items) + "]"
        case redis.Resp3.Attributed(attributes, child):
            return "attr[" + pairs_sig(attributes) + "]" + signature(child)
        case redis.Resp3.Push(items):
            return "push[" + items_sig(items) + "]"

def valid(bytes frame, str expected):
    auto result = redis.decode3(frame)
    check(result.status == "ok", "valid RESP3 status: " + expected)
    check(result.consumed == len(frame), "exact RESP3 consumption")
    check(signature(result.value) == expected, "RESP3 value: " + expected)
    auto i = 0
    while i < len(frame):
        auto prefix = redis.decode3(frame[0:i])
        check(
            prefix.status == "incomplete" and prefix.consumed == 0,
            "RESP3 proper prefix",
        )
        i = i + 1
    check(
        redis.decode3(frame, max_frame=len(frame)).status == "ok",
        "RESP3 exact frame limit",
    )
    auto extra = redis.decode3(frame + b"+NEXT\r\n", max_frame=len(frame))
    check(
        extra.status == "ok" and extra.consumed == len(frame),
        "coalesced RESP3 per-frame limit",
    )

def invalid(bytes frame):
    auto result = redis.decode3(frame)
    check(result.status == "invalid" and result.consumed == 0, "invalid RESP3 frame")
    check(len(result.message) > 0, "RESP3 diagnostic")

def depth_worker(bytes frame, channel[bool] done):
    done <- redis.decode3(frame, max_depth=64).status == "ok"

def main():
    valid(b"+OK\r\n", "simple:4f4b")
    valid(b"-ERR broken\r\n", "error:4552522062726f6b656e")
    valid(b":-9223372036854775808\r\n", "int:-9223372036854775808")
    valid(b"$-1\r\n", "null-bulk")
    valid(b"*-1\r\n", "null-array")
    valid(b"_\r\n", "null")
    valid(b"#t\r\n", "bool:True")
    valid(b"#f\r\n", "bool:False")
    valid(b",1.25e2\r\n", "double:125")
    valid(b",-0\r\n", "double:-0")
    valid(b",inf\r\n", "double:inf")
    valid(b",-inf\r\n", "double:-inf")
    valid(b",nan\r\n", "double:nan")
    valid(b",5e-324\r\n", "double:4.9406564584124654e-324")
    valid(
        b"(3492890328409238509324850943850943825024385\r\n",
        "big:3492890328409238509324850943850943825024385",
    )
    valid(b"(-000123\r\n", "big:-000123")
    auto binary = bytes([0, 255, 13, 10])
    valid(b"$4\r\n" + binary + b"\r\n", "bulk:00ff0d0a")
    valid(b"!4\r\n" + binary + b"\r\n", "blob-error:00ff0d0a")
    valid(b"=8\r\ntxt:" + binary + b"\r\n", "verbatim:747874:00ff0d0a")
    valid(
        b"%2\r\n+first\r\n:1\r\n+second\r\n#f\r\n",
        "map[simple:6669727374=int:1;simple:7365636f6e64=bool:False;]",
    )
    valid(b"%2\r\n+k\r\n:1\r\n+k\r\n:2\r\n", "map[simple:6b=int:1;simple:6b=int:2;]")
    valid(b"~2\r\n+a\r\n+b\r\n", "set[simple:61;simple:62;]")
    valid(
        b">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n",
        "push[simple:696e76616c6964617465;array[bulk:6b;];]",
    )
    valid(b"|1\r\n+ttl\r\n:5\r\n$1\r\nx\r\n", "attr[simple:74746c=int:5;]bulk:78")
    valid(b"|0\r\n|0\r\n_\r\n", "attr[]attr[]null")
    valid(b"*2\r\n|0\r\n#t\r\n%0\r\n", "array[attr[]bool:True;map[];]")
    valid(b"%0\r\n", "map[]")
    valid(b"~0\r\n", "set[]")
    valid(b">0\r\n", "push[]")
    for frame in [b"_x\r\n", b"#T\r\n", b"#1\r\n", b",NaN\r\n", b",1x\r\n", b", 1\r\n", b",0x1p2\r\n", b",1e9999\r\n", b",1e-9999\r\n", b"(1.2\r\n", b"(\r\n", b"!+1\r\nx\r\n", b"!-1\r\n", b"=3\r\ntxt\r\n", b"=4\r\ntxt?\r\n", b"%-1\r\n", b"%9223372036854775807\r\n", b"$?\r\n", b"*?\r\n", b"%?\r\n", b";0\r\n", b".\r\n", b"#t\nx", b":9223372036854775808\r\n"]:
        invalid(frame)
    invalid(b",1" + bytes([0]) + b"\r\n")
    invalid(b"," + bytes([255]) + b"\r\n")
    check(
        redis.decode3(b"|0\r\n").status == "incomplete",
        "attributes require next value",
    )
    check(
        redis.decode3(b"|0\r\n", max_frame=4).status == "invalid",
        "attribute value shares frame budget",
    )
    check(
        redis.decode3(b"%1\r\n+k\r\n").status == "incomplete",
        "map needs matching value",
    )
    check(redis.decode3(b"~0\r\n", max_depth=0).status == "invalid", "RESP3 depth zero")
    auto deep = b"_\r\n"
    for i in range(0, 64):
        deep = b"*1\r\n" + deep
    check(redis.decode3(deep, max_depth=64).status == "ok", "RESP3 max depth")
    check(
        redis.decode3(deep, max_depth=63).status == "invalid",
        "RESP3 depth enforcement",
    )
    auto done = channel[bool]()
    spawn depth_worker(deep, done)
    check(<-done, "RESP3 maximum depth on coroutine stack")
    check(redis.decode(b"_\r\n").status == "invalid", "RESP2 mode stays explicit")
    print("Bounded RESP3 decoding: PASS")
