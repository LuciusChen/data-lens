## 043. Schema Browser Async Refresh Consistency

### Problem

After lazy JDBC schema refresh moved to background backfill, two callers still
assumed the old synchronous model:

- the schema browser refreshed its view immediately and kept showing stale table
  lists until the user reopened it
- async JDBC schema refresh timeout still used the global
  `clutch-jdbc-rpc-timeout-seconds`, even when the connection had an explicit
  `:rpc-timeout`

This created another class of "looks slow" bugs that were actually state-model
bugs:

- schema browser looked stuck on old data
- one JDBC connection could time out differently depending on whether the schema
  refresh path was sync or async

### Root Cause

The earlier lazy refresh work fixed completion and console status first, but the
schema browser and async RPC timeout path still carried synchronous assumptions.

### Fix

- refresh schema browser buffers when schema status/cache updates arrive
- treat `clutch-schema-refresh` as "refresh started" for lazy backends, then
  rerender from the latest cache snapshot
- make async JDBC schema refresh use connection-local `:rpc-timeout`

### Result

Lazy JDBC backends now use one consistent model:

- completion stays fast via direct search
- full schema snapshot backfills in the background
- schema browser updates when the backfill lands
- manual refresh follows the same async timeout semantics as the connection

