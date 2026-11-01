import redis

def check(bool condition, str label):
    if not condition:
        raise ValueError("FAIL: " + label)

def reject_server(bytes listen, channel[bool] done):
    # tls.accept skips failed handshakes internally. Only listener shutdown
    # raises; a returned connection means verification unexpectedly succeeded.
    auto none_accepted = True
    try:
        while True:
            auto socket = tls.accept(listen)
            none_accepted = False
            tls.close_conn(socket)
    except Exception:
        done <- none_accepted

def rejected(str host, int64 port) -> bool:
    try:
        auto conn = redis.connect(host=host, port=port, secure=True)
        conn.disconnect()
        return False
    except redis.ConnectionError as message:
        return message.find("Redis TLS connect failed") >= 0 and message.find("certificate verify failed") >= 0

def verify_rejection(str host, str certificate, str key):
    auto certs = os.getenv("REDIS_TLS_TEST_CERTS")
    check(len(certs) > 0, "TLS fixture directory required")
    auto listen = tls.listen(
        "127.0.0.1",
        0,
        certs + "/" + certificate,
        certs + "/" + key,
    )
    auto port = tls.port(listen)
    auto done = channel[bool]()
    spawn reject_server(listen, done)
    auto all_rejected = True
    try:
        all_rejected = rejected(host, port)
        # Prime lazy runtime/OpenSSL setup before checking per-attempt fd leaks.
        auto before = 0
        if os.path_exists("/proc/self/fd"):
            before = len(os.listdir("/proc/self/fd"))
        auto i = 0
        while i < 8:
            auto client_rejected = rejected(host, port)
            all_rejected = all_rejected and client_rejected
            i = i + 1
        if os.path_exists("/proc/self/fd"):
            auto after = len(os.listdir("/proc/self/fd"))
            check(after <= before + 1, "failed handshakes do not leak descriptors")
    finally:
        tls.close_listener(listen)
    check(<-done, "server accepted no rejected certificate session")
    check(all_rejected, "every handshake rejected by verification")

def main():
    # Correct hostname, but a chain absent from the configured trust store.
    verify_rejection("localhost", "untrusted.pem", "untrusted.key")
    # Trusted chain, but the certificate has DNS:localhost and no IP SAN.
    verify_rejection("127.0.0.1", "server.pem", "server.key")
    print("TLS trust and hostname verification: PASS")
