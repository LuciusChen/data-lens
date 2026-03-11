# 012 — Oracle 11g Connection Hang

## Background

Connecting to Oracle 11g via the JDBC backend caused Emacs to hang. The symptom
was a timeout error on request 3 (`clutch-jdbc-agent: timeout waiting for
response to request 3`). The hang occurred immediately after selecting a JDBC
connection in the console picker.

## Root Causes

Three independent issues combined to cause the hang.

### 1. `DatabaseMetaData.getTables()` hangs on Oracle 11g

`getTables()` internally joins against `ALL_TAB_COMMENTS`, which on certain
Oracle 11g server configurations takes indefinitely long or never returns. This
blocked the schema refresh that `clutch-connect` triggers synchronously right
after connecting.

**Fix**: In `Dispatcher.java`, detect Oracle by product name and use direct SQL
against `USER_TABLES` / `USER_VIEWS` (for the current user) or `ALL_TABLES` /
`ALL_VIEWS` (for other schemas) instead of `DatabaseMetaData.getTables()`. These
views do not involve `ALL_TAB_COMMENTS`.

### 2. No timeout on `execute()` for Oracle

For certain invalid or slow queries, `stmt.execute()` would also hang without
ever returning an error. There was no safety net.

**Fix**: Added `query-timeout-seconds` parameter to the `execute` RPC call,
passed via `stmt.setQueryTimeout()`. The Elisp side derives this from
`clutch-jdbc-rpc-timeout`.

### 3. Missing `clutch-db-eager-schema-refresh-p` for Oracle

`clutch-connect` calls `clutch--refresh-schema-cache` synchronously right after
opening the connection. For Oracle, this triggers `get-tables` (which was root
cause #1). Even after fixing the Java side, the Elisp side would still make the
blocking call.

**Fix**: Added `cl-defgeneric clutch-db-eager-schema-refresh-p` in `clutch-db.el`
(default: `t`). The JDBC backend (`clutch-db-jdbc.el`) returns `nil` for Oracle,
so schema is not fetched synchronously on connect. Schema loads lazily on first
use instead.

## Additional Bug: JSON `false` Handling

`(plist-get response :ok)` returns the keyword `:false` when the JSON value is
`false`. In Elisp, `:false` is truthy, so error responses were being treated as
successes.

**Fix**: Changed all ok-checks to `(eq t (plist-get response :ok))`.

## Wrong Approaches Tried

- **`defaultLobPrefetchSize=0`**: Added as a connection property; no effect on
  the hang.
- **`PreparedStatement` for SELECT**: Routing SELECT/WITH queries through
  `prepareStatement()` instead of `createStatement()`; no effect. The hang was
  in `getTables()`, not query parsing.
- **Oracle-specific `getTables()` in Java only**: Added the fix to the Java side
  but forgot the Elisp-side `clutch-db-eager-schema-refresh-p`. The hang persisted
  because Emacs still called `get-tables` synchronously. Also, the old jar was
  still running — agent process was not restarted after rebuild.

## Lessons

- When a fix has both a Java side and an Elisp side, verify both are deployed
  before concluding it didn't work. Restart the agent process after rebuilding the jar.
- `DatabaseMetaData` methods on Oracle 11g are unreliable. Prefer direct SQL
  against `USER_*` / `ALL_*` views for any metadata query.
- Synchronous schema refresh on connect is a footgun for slow databases. The
  eager/lazy split via `clutch-db-eager-schema-refresh-p` is the right model.
