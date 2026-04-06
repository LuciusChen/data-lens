# JDBC Agent Protocol

This document describes the wire protocol and runtime model used by
`clutch-db-jdbc.el` and `clutch-jdbc-agent`.

It is intentionally narrower than the user-facing JDBC section in `README.org`.
`README.org` explains how to install and use the JDBC backend; this file
documents how the Elisp side and JVM sidecar talk to each other.

## Runtime model

`clutch` launches `clutch-jdbc-agent` as a local JVM sidecar process and
communicates with it over stdin/stdout using one JSON object per line.

Each logical JDBC connection in clutch maps to one logical session in the
agent.  That session owns two JDBC connections:

- `primary`: foreground SQL execution, DML, DDL, transactions
- `metadata`: schema and object introspection

This split exists to keep metadata traffic from contending with foreground SQL,
especially on Oracle.

Runtime schema switching updates both sessions together so one clutch
connection still presents one effective schema/database context.

The stdin reader is intentionally not blocked by one long-running request.
Each decoded request is submitted to a request pool.  The dispatcher then
serializes most operations per `conn-id`, so one JDBC connection still sees one
foreground operation at a time.

`cancel` is the exception: it bypasses the per-connection lock, looks up the
currently running `Statement` for the target `conn-id`, and calls
`Statement.cancel()` from another request thread.  This is what makes
recoverable `C-g` interruption possible on the Elisp side.

## Transport

- Request transport: stdin
- Response transport: stdout
- Format: one complete JSON object per line
- Process stderr: reserved for startup failures and JVM/driver diagnostics

The agent emits an initial ready response on startup before normal RPC traffic
begins.

## Message shape

Requests:

```json
{"id":1,"op":"connect","params":{"url":"jdbc:oracle:thin:@//db:1521/FREEPDB1","user":"system","password":"secret"}}
```

Success responses:

```json
{"id":1,"ok":true,"result":{"conn-id":7}}
```

Error responses:

```json
{"id":1,"ok":false,"error":"connect failed","diag":{"category":"connect","op":"connect","request-id":1,"context":{"redacted-url":"jdbc:oracle:thin:@//db:1521/FREEPDB1"}}}
```

Error responses with opt-in debug payload:

```json
{"id":1,"ok":false,"error":"connect failed","diag":{"category":"connect","op":"connect","request-id":1},"debug":{"thread":"pool-1-thread-2","request-context":{"redacted-url":"jdbc:oracle:thin:@//db:1521/FREEPDB1?password=<redacted>","user":"system","property-keys":["oracle.net.wallet_location"]},"stack-trace":"java.sql.SQLException: connect failed\n..."}}
```

Rules:

- `id` is client-generated and matched exactly in the response
- `op` is a string RPC name
- `params` is an object whose fields depend on the operation
- `params.debug=true` opts into an additional redacted `debug` payload on
  failures; normal requests should leave it unset
- `ok=true` carries a `result` object
- `ok=false` carries an `error` string
- `ok=false` may also carry a `diag` object with structured troubleshooting data
- `ok=false` may also carry a `debug` object with redacted verbose debugging
  data when the request explicitly asked for it
- `diag.context` may include `generated-sql` when the failing operation ran
  hidden/internal SQL rather than user-authored SQL

## Core operations

Connection lifecycle:

- `connect`
- `disconnect`
- `commit`
- `rollback`
- `set-auto-commit`
- `set-current-schema`

Execution and cursor flow:

- `cancel`
- `execute`
- `fetch`

Schema and object metadata:

- `get-schemas`
- `get-tables`
- `search-tables`
- `get-columns`
- `search-columns`
- `get-primary-keys`
- `get-foreign-keys`
- `get-indexes`
- `get-index-columns`
- `get-sequences`
- `get-procedures`
- `get-functions`
- `get-procedure-params`
- `get-function-params`
- `get-triggers`
- `get-object-source`
- `get-object-ddl`
- `get-referencing-objects`

Not every backend implements every metadata operation.  On the Elisp side,
unsupported capabilities are represented by normal feature fallbacks rather
than a separate protocol version.

## Connection semantics

`connect` accepts:

- `url`
- `user`
- `password`
- `props`
- `connect-timeout-seconds`
- `network-timeout-seconds`
- `auto-commit`

`auto-commit=false` is how clutch requests manual-commit mode for the primary
session.  The metadata session stays read-only/autocommit-oriented.

The connect response returns:

- `conn-id`

That `conn-id` is then used in all subsequent operations.

`cancel` accepts:

- `conn-id`

Its success response returns:

- `conn-id`
- `request-id` when a running statement was found
- `cancelled` (`true` when a statement was cancelled, `false` when nothing was running)

The cancelled `execute`/`fetch` request may still produce a late response after
the client has already committed to the interrupt path.  The Elisp side tracks
request ids explicitly so those late responses can be dropped instead of
polluting the next request.

The current Elisp client closes cursor state implicitly by fetching until the
agent replies with `done=true`; it does not issue a separate `close-cursor`
RPC.

## Error semantics

There are three distinct failure classes:

1. A normal request-level database error.
   - Examples: SQL syntax error, object-not-found, cancelled statement.
   - The agent stays up and Elisp surfaces the database error normally.
   - The response may also include a structured `diag` object with fields such
     as category, request id, connection id, exception class, SQLState, cause
     chain, and redacted request context.

2. The agent is running but a request times out or the underlying JDBC session
   wedges.
   - Elisp reports this as a connection-loss error and clears the local agent
     state.

3. The agent exits before replying.
   - Elisp reads agent stderr and reports the startup failure directly.
   - If stderr contains `UnsupportedClassVersionError`, clutch reports that the
     configured Java runtime is too old for the current jar.

This distinction matters because a JVM startup failure should not look like a
normal database disconnect.

The short `error` string and the optional `diag` payload serve different roles:

- `error`: concise default summary suitable for normal user-facing display
- `diag`: richer request diagnostics for troubleshooting UI / logs / tests
- `debug`: opt-in verbose debugging payload; may include redacted request
  context and a redacted stack trace, but is omitted unless the client sent
  `params.debug=true`

stderr remains for process/runtime logging and should not be treated as the
primary source of request-level diagnostics.

## Java requirement

The current `clutch-jdbc-agent` jar requires Java 17+.

If `clutch-jdbc-agent-java-executable` or `JAVA_HOME` points to an older Java,
the agent may fail before sending its ready response.  clutch now surfaces that
as a startup/runtime mismatch instead of a generic connection-loss timeout.

## Relationship to README

Use `README.org` for:

- installation
- driver setup
- connection examples
- user-visible workflow

Use this file for:

- protocol shape
- agent/session model
- RPC inventory
- startup/error behavior
