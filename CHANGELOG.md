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
- Typed key rename, copy, discovery, touch, binary DUMP/RESTORE, absolute
  expiry, and OBJECT metadata (Redis #56).
- Typed GETDEL, GETEX, string ranges, APPEND, and STRLEN. SET supports EXAT,
  PXAT, KEEPTTL, and GET, and returns a tagged `SetResult` (#57).
- Sorted-set score and lex ranges, stores, set operations, and cancellable
  blocking pops. ZADD takes a member-to-score mapping with redis-py flags and
  returns a tagged result (#58).
- Typed LPOS, LINSERT, LREM, conditional pushes, and list moves and
  multi-pops, including cancellation-safe BRPOPLPUSH and BLMPOP (#59).
- Typed set membership, intersection cardinality, moves, and store operations.
  SPOP and SRANDMEMBER support counted and uncounted selection (#60).
- Bounded guarded pools with finite reset/reinitialization and dedicated leases.
- Dedicated pull-based Pub/Sub sessions and PUBLISH, with typed binary events,
  explicit acknowledgements and at-most-once/no-replay semantics.
- Basic Streams with unsigned ID tokens, ordered/repeated binary fields, ranges,
  deletion and nonblocking/dedicated-blocking XREAD.
- Stream consumer groups with pending summaries, acknowledgements, and stale
  pending-entry claims.
- Opt-in RESP3 HELLO negotiation, rich replies/attributes, bounded push delivery,
  and RESP3 Pub/Sub dispatch; RESP2 remains the default.
- Sentinel primary discovery with separate Sentinel/data credentials and explicit
  reconnect, plus Redis Cluster slot discovery and cached single-key routing.
- Cluster MOVED/ASK redirects with bounded retries and same-slot validation for
  a supported multi-key command subset.
- Executable consumer examples, path/static-registry installation tests, the latest
  Zap image and pinned Redis server versions in CI, and version/tag release preflight.
