# Changelog

## 0.1.0 — unreleased

First standalone client candidate. No v0.1.0 tag or hosted-registry publication
is created by this change.

- Binary-safe RESP2 codec, resumable receive framing, bounded parser limits,
  raw commands, typed errors, explicit reconnect, and no automatic replay.
- Connection options/URLs, AUTH, database/client name, verified TLS/rediss,
  separate connect/session and command budgets, and structured cancellation.
- Typed key/string/counter/expiry, hash, list, blocking list, set, sorted-set,
  scan-page and scripting helpers; bounded pipelines and exclusive transactions.
- Bounded guarded pools with finite reset/reinitialization and dedicated leases.
- Dedicated pull-based Pub/Sub sessions and PUBLISH, with typed binary events,
  explicit acknowledgements and at-most-once/no-replay semantics.
- Basic Streams with unsigned ID tokens, ordered/repeated binary fields, ranges,
  deletion and nonblocking/dedicated-blocking XREAD.
- Decoder-only RESP3 values; no RESP3 transport negotiation or push dispatch.
- Executable consumer examples, path/static-registry installation tests, pinned
  runtime/server CI, and version/tag release preflight.
