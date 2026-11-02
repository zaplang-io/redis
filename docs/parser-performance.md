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
streaming. A resumable parser and chunked buffer are tracked separately in
[issue #45](https://github.com/zaplang-io/redis/issues/45).

Native baseline from [CI run #52](https://github.com/zaplang-io/redis/actions/runs/41381721628/job/125073765164),
Ubuntu runner, pinned Zap image `sha-d8885f5`, 2026-11-02. These are single-run
observations, affected by JIT-free native execution, allocator/collector setup,
and runner load; they are not portable performance guarantees.

| Arguments | Encoded bytes | Former encoder (ms) | Balanced encoder (ms) | Former copy model (bytes) | Balanced upper bound (bytes) |
| --- | --- | --- | --- | --- | --- |
| 512 | 19,974 | 5.596 | 0.178 | 15,356,160 | 219,714 |
| 1,024 | 39,943 | 6.786 | 0.319 | 61,386,240 | 479,316 |
| 2,048 | 79,879 | 14.829 | 0.634 | 245,455,872 | 1,038,427 |

The modeled concatenation allocation/copy volume drops sharply; the fragment
list and small header allocations are not included in this model. No peak-RSS
claim is made.

| Wide array items | Prefix decode calls | Elapsed (ms) |
| --- | --- | --- |
| 512 | 18 | 1.393 |
| 1,024 | 34 | 4.905 |
| 2,048 | 66 | 18.557 |

Doubling this incremental workload takes roughly 3.5–3.8 times as long. This
confirms repeated-prefix work remains approximately quadratic; #45 specifies
a persistent-framing/chunked-buffer implementation and scaling checks rather
than hiding that limitation behind the frame-size bound.

The fixed-seed corpus uses recurrence `(seed * 251 + 13849) % 65521`, seed 322.
It covers 256 generated valid reply trees, all their proper prefixes and
coalesced next replies, unknown-tag mutations, 512 arbitrary byte strings,
int64 overflow/invalid lengths/CRLF cases, a 64-level array on both native and coroutine stacks, 2048-element wide
arrays, and a 64 KiB binary bulk string fragmented every 257 bytes. Live Redis
uses 64 generated nested Lua replies as a differential oracle. Tests assert
structural equality, diagnostics, consumption bounds, and resource limits.

Depth 256 passed on the native main stack but failed on the pinned runtime's
128 KiB default coroutine stack in CI run #54. The supported maximum is therefore
64, matching the existing default, with a regression at that limit on both
stacks. Larger configured depths are rejected before recursive parsing.
