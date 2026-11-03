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
positive connect and command budgets. `cluster_slot`
implements Redis CRC16/XMODEM and hash-tag selection. `execute_for(key, args)`
routes supported single-key commands using `key`; `args[1]` must be the same
binary key. It caches a connection for each primary and reuses it for later
commands. Unsupported commands, multi-key operations and transactions are
rejected locally.

Call `refresh()` after an operator-directed topology change. It rediscovers
slots, retains connections for primaries still in the map, and closes cached
connections for removed primaries. This version does not process MOVED or ASK
responses automatically, follow redirects, or validate same-slot multi-key
commands; those behaviors are tracked separately in #29. A transport failure
closes the affected connection and does not replay the command. The next
operation reconnects to the cached primary; call `refresh()` when the topology
may have changed.

The clients inherit the supplied options for authentication, TLS and timeouts.
Discovered addresses are used as returned by Sentinel or `CLUSTER SLOTS`. For a
bounded connect timeout, the discovered address must be numeric unless the
library can be given a matching resolved IP through a future endpoint-specific
configuration API. Hostname DNS resolution remains synchronous and unbounded.
