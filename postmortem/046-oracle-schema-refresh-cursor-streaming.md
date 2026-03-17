# 046 — Oracle schema refresh: cursor-based streaming for large schemas

## Background

Connecting to an Oracle production database with 23,559 tables, `clutch`'s
schema refresh (`get-tables` RPC) consistently timed out and never succeeded,
even though ordinary queries worked (albeit sometimes slowly).

## Root Cause

The previous implementation of `getTables()` in the Java agent had two
compounding problems:

1. **Oracle JDBC default fetch size is 10 rows.**  Fetching 23,559 rows required
   ~2,356 sequential network round-trips to the database server before the agent
   had anything to return.  The entire collection had to complete within the
   `setQueryTimeout(15)` window, which it could not.

2. **Single-payload response.**  Even if the query had completed in time, the
   entire table list would arrive as one JSON response that Emacs had to receive
   within `clutch-jdbc-rpc-timeout-seconds` (30 s).  Two separate timeouts had to
   be satisfied sequentially by a single operation.

A stopgap of raising `setQueryTimeout` to 120 s and adding a dedicated
`clutch-jdbc-schema-rpc-timeout-seconds = 120` was considered and initially
applied, but it only masked the symptom without fixing the architecture.

## Decision

Stream the Oracle `get-tables` result via the existing cursor/fetch protocol
instead of collecting all rows before responding.

**Java agent (`Dispatcher.java`):**
- `oracleTablesCursor()`: executes the `user_tables ∪ user_views` query, sets
  `rs.setFetchSize(1000)` on the ResultSet (reducing Oracle round-trips from
  ~2,356 to ~24 for 23,559 tables), registers the open ResultSet as a cursor
  with `CursorManager`, and returns the first batch + cursor-id.  Subsequent
  batches are retrieved via the normal `"fetch"` op.
- `jdbcTablesOneBatch()`: non-Oracle JDBC path is semantically unchanged (still
  materializes via `DatabaseMetaData.getTables()`) but returns the same
  cursor-format response (`cursor-id: null, done: true`) so the Emacs caller
  has a single code path for all backends.
- SQL column aliases normalized to `name`/`type`/`schema` so Emacs can use
  `(car row)` without column-name lookup.

**Emacs (`clutch-db-jdbc.el`):**
- `clutch-db-list-tables` and `clutch-db-refresh-schema-async` now read the
  first batch from the `get-tables` response, then call `clutch-jdbc--fetch-all`
  for any remaining batches.  The reuse of the existing helper keeps the change
  minimal.
- `clutch-jdbc-schema-rpc-timeout-seconds` was removed; each individual `fetch`
  call is bounded by the standard 30 s `rpc-timeout`, which is more than enough
  for a single 1,000-row batch.

## Alternatives Considered

**Multithreading inside the agent:** rejected.  The bottleneck is sequential
network I/O on a single JDBC connection, not CPU.  Parallelising with multiple
threads would not increase throughput and would introduce synchronisation
complexity forbidden by the project's design principles.

**Just raising both timeouts (stopgap):** applied briefly, then reverted.  It
pushed the problem forward rather than solving it, and a 120 s RPC timeout
degrades the error-detection experience for all other operations.

**Full incremental schema cache update (show N tables, then 2N, …):** a valid
UX improvement but orthogonal to the correctness fix.  Deferred; would require
changes to `clutch--install-schema-cache` to support partial/incremental updates.

## Impact on Other Databases

Only the `get-tables` protocol changed.  Non-Oracle JDBC backends (SQL Server,
DB2, Snowflake, Redshift) now receive a `done: true` cursor-format response in
the first batch — identical in effect to the old `tables` list, but with
consistent field names.  Native MySQL, PostgreSQL, and SQLite backends are
completely unaffected.
