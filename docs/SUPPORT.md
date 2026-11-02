# v0.1 support matrix

Status: **unreleased candidate**, manifest version 0.1.0. This repository prepares
the first tag; the release preflight does not create a release or publish remotely.

| Area | v0.1 contract |
| --- | --- |
| Zap runtime | Tested with core commit `2b6a607b5f11eb1e52016450c41403c0a2b77997`, including core #575/#578/#583; use the exact CI image below. The runtime's generic version string alone does not identify these fixes. |
| Toolchain image | `ghcr.io/zaplang-io/zap@sha256:593d82a7938f1fc7f4fb1ead916309089ee17537a1c83ec797bb97a55a82faec` |
| Client CI platform | Linux x86_64. Other architectures/OSes are not certified by this library's matrix. |
| Redis servers | CI tests Redis 7.4.12 and 8.10.3. Earlier/later versions are not promised by this matrix. Individual commands can have lower version floors. |
| Transport | Standalone TCP and verified TLS/rediss, OpenSSL trust paths. No per-connection CA/mTLS API; see core #560. |
| Protocol | RESP2 command/reply transport. RESP3 decoder is an isolated API, not negotiation or push support. |
| Data | Keys, arguments, values and server-error bytes remain `bytes`; callers choose text decoding. |
| Ownership | One active owner per Connection/Lease; transactions retain ownership through caller code. Concurrent use rejects with BusyError. |
| Timeouts | Connect budget includes numeric TCP/TLS/setup; command budget spans write/reply or whole pipeline. Synchronous hostname DNS remains unbounded unless caller supplies a resolved address. |
| Blocking lists | BLPOP, BRPOP, BLMOVE on standalone connections or dedicated pool leases. Redis timeout seconds and client budget milliseconds are independent. |
| Pool reuse | RESET plus original setup under a finite reset budget. Requires Redis 6.2+ and suitable ACLs for reuse; denied/failed reset discards. |
| Recovery | Cancellation/incomplete exchanges discard sockets. Explicit reconnect only; ambiguous commands/transactions are never replayed. |
| Parser limits | Default 16 MiB per reply, depth 64; configurable read chunk. Pipeline request-byte/command limits are separate. |
| Installation | Tested external consumers use path dependencies and a disposable file:// static registry, without copying the module beside main. |

The image pins a working toolchain rather than promising compatibility with every
Zap compiler revision. Public APIs may evolve before a stable release; consult
the changelog when upgrading and commit application `zap.lock` files.

The implemented subset includes key/string/counter/expiry, hashes, non-blocking
and blocking lists, sets, sorted sets, cursor pages, scripts, pipelines,
transactions, pools, dedicated Pub/Sub, and basic Streams. Raw commands provide an escape hatch with the same
ownership/deadline rules; that is not full redis-py or Redis-module parity.

Later optional work remains separate: Streams consumer groups (#22), RESP3
negotiation/push dispatch (#26), Sentinel (#27), Cluster routing/redirections
(#28/#29), and per-connection trust/mTLS (core #560). These are not v0.1 blockers.
Blocking lists (#15), Pub/Sub (#20), and basic Streams (#21) are now included
in the unreleased candidate. Pub/Sub is at-most-once with no transparent
resubscription; Streams has no automatic cursor or consumer acknowledgement policy.
