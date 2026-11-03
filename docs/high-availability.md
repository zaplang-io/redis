# Sentinel and Redis Cluster

These APIs opt into Redis's high-availability discovery mechanisms. They do not
retry commands whose outcome may be ambiguous.

## Sentinel primary discovery

Build Sentinel seed options separately from data-node options so the two
connections can use different credentials and TLS settings:

```zap
list[redis.Options] sentinels = [
    redis.options("sentinel-a.example", 26379, password="sentinel-secret"),
    redis.options("sentinel-b.example", 26379, password="sentinel-secret"),
]
auto data = redis.options("127.0.0.1", 6379, password="redis-secret")
auto client = redis.sentinel_connect(sentinels, b"orders", data)
try:
    print(str(client.primary().host) + ":" + str(client.primary().port))
    client.execute([b"GET", b"orders:42"])
finally:
    client.disconnect()
```

`sentinel_connect` queries `SENTINEL GET-MASTER-ADDR-BY-NAME` in seed order and
opens a data connection to the discovered primary. It tries later seeds when a
seed is unreachable or cannot provide a valid primary. Every Sentinel seed must
set positive connect and command budgets, and the seed list is finite, so
discovery attempts have a bound. Data options must set a positive connect
budget so replacing the primary connection is bounded too. Sentinel and
data-node options carry independent authentication, database, TLS and timeout
settings.

`primary()` reports the last discovered address. After a connection failure or
Sentinel failover, call `reconnect()` to discover the primary again and replace
the data connection. The failed command is never replayed: it may have already
run on the old primary. Decide whether it is safe to issue a new command after
the explicit reconnect. Connections use one active owner at a time, so callers
must serialize access to a `SentinelClient`.

## Cluster slot routing

```zap
list[redis.Options] seeds = [
    redis.options("127.0.0.1", 7000),
    redis.options("127.0.0.1", 7001),
]
auto cluster = redis.cluster_connect(seeds)
try:
    cluster.execute_for(b"account:{42}", [b"GET", b"account:{42}"])
finally:
    cluster.disconnect()
```

`cluster_connect` reads `CLUSTER SLOTS`, requires a complete, non-overlapping
mapping of all 16,384 slots, and supports database zero only. Each seed must set
positive connect and command budgets. `cluster_slot` implements Redis
CRC16/XMODEM and hash-tag selection. `execute_for(key, args)` routes this
single-key command subset using `key`, and requires `args[1]` to be the same
binary key:

- Strings and key state: `APPEND`, `DECR`, `DECRBY`, `EXPIRE`, `EXPIREAT`,
  `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`,
  `INCRBYFLOAT`, `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PTTL`, `RESTORE`, `SET`,
  `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`, `SUBSTR`, `TTL`, `TYPE`.
- Hashes: `HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`,
  `HKEYS`, `HLEN`, `HMGET`, `HSET`, `HSETNX`, `HSTRLEN`, `HVALS`.
- Lists: `LINDEX`, `LINSERT`, `LLEN`, `LPOP`, `LPOS`, `LPUSH`, `LPUSHX`,
  `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPUSH`, `RPUSHX`.
- Sets: `SADD`, `SCARD`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SPOP`,
  `SRANDMEMBER`, `SREM`.
- Streams: `XADD`, `XDEL`, `XLEN`, `XPENDING`, `XRANGE`, `XREVRANGE`.
- Sorted sets: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZLEXCOUNT`, `ZMSCORE`,
  `ZPOPMAX`, `ZPOPMIN`, `ZRANGE`, `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANK`,
  `ZREM`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`, `ZREVRANK`,
  `ZSCAN`, `ZSCORE`.

`execute_multi(args)` supports `MGET`, `DEL`, `UNLINK`, `EXISTS`, `MSET`,
`RENAME`, `RENAMENX`, `RPOPLPUSH`, `SMOVE`, `SUNION`, `SINTER`, `SDIFF`, and
`BITOP`. It extracts each key for those command forms and rejects a cross-slot
request locally before opening a socket or sending a command. Unknown command
forms, pipelines, transactions, Pub/Sub and blocking/stateful commands are not
supported by the Cluster client.

The client caches a connection for each primary and reuses it. A MOVED response
refreshes the slot map and routes that slot to the advertised primary. An ASK
response sends `ASKING` and the original command on the temporary node without
changing the slot map. Redirects are limited to five hops; malformed redirects,
slot mismatches and loops raise `ProtocolError`. `refresh()` can be called after
an operator-directed topology change; it retains connections for primaries
still in the map and closes cached connections for removed primaries.

A transport failure closes the affected connection and does not replay the
command. A server may have executed it before the connection failed. Discovered
addresses and redirect endpoints inherit the configured verified TLS mode.

The clients inherit the supplied options for authentication, TLS and timeouts.
Discovered addresses are used as returned by Sentinel or `CLUSTER SLOTS`. For a
bounded connect timeout, discovered and redirected addresses must be numeric;
there is no endpoint-specific resolved-IP override. Hostname DNS resolution
remains synchronous and unbounded.
