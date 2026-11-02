import redis

# Diagnostic baseline, not a timing threshold. Compare the original growing
# concatenation encoder with the library's balanced encoder on identical input.
def legacy(list[bytes] args) -> bytes:
    auto out = bytes("*" + str(len(args)) + "\r\n")
    for argument in args:
        bytes value = argument
        out = out + bytes("$" + str(len(value)) + "\r\n") + value + b"\r\n"
    return out

def main():
    auto payload = b"0123456789abcdef0123456789abcdef"
    for n in [512, 1024, 2048]:
        list[bytes] args = []
        for i in range(0, n):
            args.append(payload)
        auto start = time.monotonic_ns()
        auto old = legacy(args)
        auto old_ns = time.monotonic_ns() - start
        start = time.monotonic_ns()
        auto encoded = redis.encode_command(args)
        auto new_ns = time.monotonic_ns() - start
        if old != encoded:
            raise ValueError("benchmark encoders disagree")
        # This estimates copied string bytes from the source expressions, not
        # peak RSS or collector overhead. Legacy has three growing additions.
        auto legacy_copies = 0
        auto length = len(bytes("*" + str(n) + "\r\n"))
        for i in range(0, n):
            legacy_copies = legacy_copies + (length + 5) + (length + 37) + (length + 39)
            length = length + 39
        auto levels = 0
        auto fragments = 3 * n + 1
        while fragments > 1:
            fragments = (fragments + 1) // 2
            levels = levels + 1
        print(
            "encode args=" + str(n) + " bytes=" + str(len(encoded)) + " legacy_ns=" + str(old_ns) + " balanced_ns=" + str(new_ns),
        )
        print(
            "copy_model legacy_bytes=" + str(legacy_copies) + " balanced_upper_bytes=" + str(len(encoded) * levels),
        )
    for n in [512, 1024, 2048]:
        auto frame = bytes("*" + str(n) + "\r\n")
        for i in range(0, n):
            frame = frame + b":1\r\n"
        auto start = time.monotonic_ns()
        auto end = 0
        auto calls = 0
        while end < len(frame):
            redis.decode(frame[0:end])
            calls = calls + 1
            end = end + 128
        auto result = redis.decode(frame)
        if result.status != "ok":
            raise ValueError("benchmark frame rejected")
        print(
            "incremental wide_items=" + str(n) + " calls=" + str(calls + 1) + " elapsed_ns=" + str(time.monotonic_ns() - start),
        )
