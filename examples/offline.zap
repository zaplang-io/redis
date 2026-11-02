import redis

def main():
    auto encoded = redis.encode_command([b"ECHO", bytes([0, 255, 13, 10])])
    if len(encoded) == 0:
        raise ValueError("empty encoded command")
    auto reply = redis.decode(b"$3\r\nzap\r\n")
    if reply.status != "ok":
        raise ValueError("codec import failed")
    auto cfg = redis.parse_url("redis://127.0.0.1:6379/0?command_timeout_ms=1000")
    if cfg.command_timeout_ms != 1000:
        raise ValueError("URL options import failed")
    auto pool = redis.pool(cfg)
    pool.shutdown()
    print("Redis package import: PASS")
