# 013 — Completion Cache Design

## Background

`clutch--tables-in-buffer` and `clutch--tables-in-query` are called on every
completion-at-point invocation (i.e., every keystroke during typing). Both
iterate over the schema hash-table and scan the buffer, making them the dominant
cost of interactive SQL completion.

## Decision

Cache the results buffer-locally, keyed by:

- `clutch--tables-in-buffer-cache`: `(schema-object-identity, buffer-chars-modified-tick)`
- `clutch--tables-in-query-cache`: `(schema-object-identity, buffer-chars-modified-tick, beg, end)`

Schema identity is compared with `eq` (object identity), not `equal` (structural
equality). `beg` and `end` are the statement boundaries (semicolons / blank lines)
surrounding point at the time of the call.

## Why `eq` for Schema Identity Is Safe

Schema is a hash-table stored in `clutch--schema-cache`. `clutch--refresh-schema-cache`
always creates a **new** hash-table and replaces the old entry via `puthash` — it
does not mutate the existing object in place. So after a schema refresh, the new
hash-table has a different object identity, and `eq` fails, correctly invalidating
the cache.

`clutch--ensure-columns` does mutate the schema hash-table in place (it adds
column details to an existing table entry), but both caches only track table names,
not column details. An in-place column update therefore does not stale the cache.

If `clutch--refresh-schema-cache` were ever changed to mutate in place rather than
replace, these caches would silently serve stale table names until the buffer was
edited. That would be a bug. The `eq` assumption must be preserved.

## Why `buffer-chars-modified-tick`

`buffer-chars-modified-tick` increments on every character change and is cheaper
than comparing buffer contents. It correctly invalidates the cache on any edit,
which is the right granularity — table names in the buffer can change with any
edit.

## Why `beg` / `end` in the Query Cache

`clutch--tables-in-query` restricts the regex search to the SQL statement
surrounding point (bounded by semicolons or blank lines). The same buffer tick
but a different point (in a different statement) should yield a different result.
Storing `beg` and `end` handles this: when point moves to a different statement,
`beg`/`end` change, the cache misses, and the correct tables are returned.
