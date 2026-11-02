# RESP2 hardening workload

Reproduce with the pinned Zap container used by CI:

```sh
sh tools/run-tests.sh
sh tools/run-integration-tests.sh --isolated
```

The offline runner stages and executes `tools/bench/resp2.zap` after golden
assertions. It compares the former growing-concatenation command encoder with
the balanced encoder at 512/1024/2048 arguments, each 32 bytes. Both outputs must
match exactly. Timing uses monotonic nanoseconds, and has no pass/fail threshold.
The reported copy model estimates string payload bytes copied by concatenation;
it is not peak RSS or measured collector overhead. Balanced joining allocates
O(argument count) fragment references and copies at most O(total bytes × log
fragment count), instead of the prior quadratic growth in many-argument cases.

A second workload repeatedly decodes prefixes of 512/1024/2048-element arrays
in 128-byte increments. `decode` remains stateless: repeated prefixes reparse
and reallocate prior array elements. Connection buffer concatenation also
copies accumulated data on fragmented reads. These costs are bounded by the
configured per-frame byte/depth limits, but are not claimed to be linear-time
streaming. A resumable parser and chunked buffer need a separate concrete
follow-up after the measured baseline is recorded.

Native baseline: pending first CI run for this combined change. Results will
be recorded here before the PR is marked ready for review.

The fixed-seed corpus uses recurrence `(seed * 251 + 13849) % 65521`, seed 322.
It covers 256 generated valid reply trees, all their proper prefixes and
coalesced next replies, unknown-tag mutations, 512 arbitrary byte strings,
int64 overflow/invalid lengths/CRLF cases, a 256-level array, 2048-element wide
arrays, and a 64 KiB binary bulk string fragmented every 257 bytes. Live Redis
uses 64 generated nested Lua replies as a differential oracle. Tests assert
structural equality, diagnostics, consumption bounds, and resource limits.
