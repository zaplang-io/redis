# Redis package decisions

## SET options and result

Issue [#57](https://github.com/zaplang-io/redis/issues/57) uses one setter
for SET with and without GET, following [redis-py's SET argument
order](https://redis.readthedocs.io/en/stable/commands.html#redis.commands.core.BasicKeyCommands.set)
for the options shared by the supported Redis versions.

Zap reserves `set`, so the package retains the name `set_value`. It uses
`int64` for expiry options and zero to omit them, consistent with the
existing EX/PX convention. Values remain bytes. Zap needs one declared
return type, so `SetResult.Written`, `.Previous(bytes)`, and `.Null`
represent the redis-py results `True`, previous bytes, and `None`.
A null result has the same option-dependent meaning as the Redis reply.

This changes the unreleased boolean API. Callers can use `.is_written()`
for status-only SET calls, switch the `key` keyword to `name`, and name
any previously positional SET options to account for the new order.

## Sorted-set mapping and result types

Issue [#58](https://github.com/zaplang-io/redis/issues/58) changes ZADD
to take a `dict[bytes, float64]` member-to-score mapping with the same
option names and order as [redis-py](https://redis.readthedocs.io/en/stable/commands.html#redis.commands.core.SortedSetCommands.zadd).
`ZAddResult` represents the count, incremented score, or skipped increment.
`ScoredMember` remains the representation of member/score replies.

`SortedSetResult` represents the two WITHSCORES list shapes. Sort boundaries
use byte tokens so a single argument type covers numeric, exclusive, infinite,
and binary lex bounds. Union and intersection take an ordered key list and
optional parallel weights. Multi-pop derives the count of input keys from
the list. Scored results always use float64.

## List option-dependent results and key counts

Issue [#59](https://github.com/zaplang-io/redis/issues/59) follows
[redis-py's list command names and options](https://redis.readthedocs.io/en/stable/commands.html).
`ListPosition` distinguishes the scalar, missing, and counted LPOS replies.
The LPOS count uses -1 to omit COUNT, preserving zero as the server's
all-matches value. LMPOP and BLMPOP derive the key count from a typed list
and return `ListPopBatch` with its key and values. The conditional pushes
take a list of values, matching the existing typed LPUSH and RPUSH APIs.
