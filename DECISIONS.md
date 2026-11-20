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

## Set sampling and counted pops

Issue [#60](https://github.com/zaplang-io/redis/issues/60) follows
[redis-py's single entry point per command](https://redis.readthedocs.io/en/stable/commands.html).
SPOP and SRANDMEMBER use `Optional[int64]` to distinguish an omitted count
from zero and negative values. `SetSelection` distinguishes a scalar member,
missing result, and counted list. Scalar SPOP checks and unwrapping remain
available on the tagged result.

SMISMEMBER returns boolean membership flags in request order, following the
existing SISMEMBER type. SINTERCARD derives the Redis key count from the
typed key list, as the package does for other counted-key commands.

## Bitmap options and BITFIELD builder

Issue [#61](https://github.com/zaplang-io/redis/issues/61) follows
[redis-py's bitmap command names and options](https://redis.readthedocs.io/en/stable/commands.html).
BITCOUNT and BITPOS represent omitted endpoints with `Optional[int64]`,
preserving zero and negative indexes. BITOP takes a typed source-key list.
BITFIELD uses a text offset token for both absolute offsets and `#` field
indexes. Zap reserves `set`, so the builder uses `set_value` for SET.
The builder is bound to a connection and key, queues ordered operations,
and resets after execution. Its result uses `Optional[int64]` for each
slot so a FAIL overflow remains distinct from zero.

## HyperLogLog argument lists

Issue [#62](https://github.com/zaplang-io/redis/issues/62) follows
[redis-py's HyperLogLog method names and argument order](https://redis.readthedocs.io/en/stable/commands.html).
Variable-length elements and source keys use `list[bytes]` in Zap. PFADD
and PFMERGE default to an empty list, preserving their key-only forms.
PFCOUNT requires a nonempty list. PFADD returns the integer register
update flag, PFCOUNT the integer estimate, and PFMERGE its success status.

## Geospatial inputs and results

Issue [#63](https://github.com/zaplang-io/redis/issues/63) follows
[redis-py's geospatial methods and option names](https://redis.readthedocs.io/en/stable/commands.html).
GEOADD uses a list of `GeoEntry` records to type each longitude, latitude,
and binary member. GEOPOS and GEOHASH keep a nullable slot for each member
in a typed input list. The search origin, shape, and count options use
`Optional` so that zero and empty member bytes remain valid inputs.

`GeoSearchResult.Members` carries the plain member list. When a WITH flag is
requested, `GeoSearchResult.Hits` carries typed records with optional
distance, raw hash, and position fields.

## Stream trimming, claim results, and metadata

Issue [#64](https://github.com/zaplang-io/redis/issues/64) follows
[redis-py's Stream option names](https://redis.readthedocs.io/en/stable/commands.html).
XADD retains the ordered field pair list to preserve repeated field names
and returns `Optional[bytes]` for NOMKSTREAM. Callers that permit stream
creation can unwrap the result. Trim thresholds and limits use Optional
so omitted values stay distinct from zero.

XCLAIM uses a tagged value for entries or JUSTID results. XAUTOCLAIM
retains the continuation and deleted IDs in both modes and exposes JUSTID
values in its new `ids` field. XINFO metadata uses typed records and
ignores unknown fields. Stream IDs and group and consumer names remain bytes.

## Server administration results

Issue [#65](https://github.com/zaplang-io/redis/issues/65) follows
[redis-py's administration method names and option shapes](https://redis.readthedocs.io/en/stable/commands.html).
INFO keeps ordered sections and tags numeric, text, and compound values.
CONFIG and CLIENT metadata use ordered byte pairs. COMMAND DOCS exposes
named fields with their Redis reply types. The `asynchronous` option on
FLUSHDB and FLUSHALL selects ASYNC when true and explicit SYNC when false.
DEBUG SLEEP is used only in the disposable integration fixture.

## Functions and shard session routing

Issue [#66](https://github.com/zaplang-io/redis/issues/66) follows
[redis-py's Functions and shard command names](https://redis.readthedocs.io/en/stable/commands.html).
FCALL and FCALL_RO derive the key count from the binary key list, as EVAL
does. Function listing and stats expose typed records with nullable metadata.

A Cluster shard subscription uses the existing slot and redirect logic
and owns a separate socket. The first acknowledgement stays visible to
callers of `next_event()`. Ordinary and shard counts are tracked separately
because Redis reports them separately. Cluster command connection ownership
stays independent of the returned session.
