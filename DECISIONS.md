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
