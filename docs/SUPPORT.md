# v0.1 support matrix

Status: **unreleased candidate**, manifest version 0.1.0. This repository prepares
the first tag; the release preflight does not create a release or publish remotely.

| Area | v0.1 contract |
| --- | --- |
| Zap runtime | CI uses the latest published Zap toolchain with LLVM code generation. ZADD uses the float dictionary values introduced by core D-0137. |
| Toolchain image | `ghcr.io/zaplang-io/zap:latest` |
| Client CI platform | Linux x86_64. Other architectures/OSes are not certified by this library's matrix. |
| Redis servers | CI tests Redis 7.4.12 and 8.10.3. Earlier/later versions are not promised by this matrix. Individual commands can have lower version floors. |
| Transport | Standalone TCP and verified TLS/rediss, OpenSSL trust paths. No per-connection CA/mTLS API; see core #560. |
| Protocol | RESP2 by default; explicit RESP3 HELLO negotiation with bounded push dispatch. |
| Data | Keys, arguments, values and server-error bytes remain `bytes`; callers choose text decoding. |
| Ownership | One active owner per Connection/Lease; transactions retain ownership through caller code. Concurrent use rejects with BusyError. |
| Timeouts | Connect budget includes numeric TCP/TLS/setup; command budget spans write/reply or whole pipeline. Synchronous hostname DNS remains unbounded unless caller supplies a resolved address. |
| Blocking pops and moves | BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP, BZPOPMIN, BZPOPMAX, and BZMPOP on standalone connections or dedicated pool leases. Redis timeout seconds and client budget milliseconds are independent. |
| Pool reuse | RESET plus original setup under a finite reset budget. Requires Redis 6.2+ and suitable ACLs for reuse; denied/failed reset discards. |
| Recovery | Cancellation/incomplete exchanges discard sockets. Explicit reconnect only; ambiguous commands/transactions are never replayed. |
| Parser limits | Default 16 MiB per reply, depth 64; configurable read chunk. Pipeline request-byte/command limits are separate. |
| Installation | Tested external consumers use path dependencies and a disposable file:// static registry, without copying the module beside main. |

CI validates the published Zap image when a workflow runs. Public APIs may
evolve before a stable release. Consult the changelog when upgrading and
commit application `zap.lock` files.

The implemented subset includes typed SET options and binary string commands,
counters and expiry, generic key copy,
rename, DUMP/RESTORE, and metadata, hashes, non-blocking
and blocking lists, sets, sorted sets, cursor pages, scripts, pipelines,
transactions, pools, dedicated Pub/Sub, Streams consumer groups, explicit
RESP3 connections, Sentinel primary discovery and Redis Cluster slot routing
with MOVED/ASK handling and a documented same-slot multi-key subset. Raw
commands provide an escape hatch with the same
ownership/deadline rules; that is not full redis-py or Redis-module parity.

Later optional work remains separate: per-connection trust/mTLS (core #560).
This is not a v0.1 blocker.
Blocking lists (#15), Pub/Sub (#20), and basic Streams (#21) are now included
in the unreleased candidate. Pub/Sub is at-most-once with no transparent
resubscription; Streams has no automatic cursor or consumer acknowledgement policy.
