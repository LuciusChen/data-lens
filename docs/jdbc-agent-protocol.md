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
{"id":1,"ok":false,"error":"connect: 'url' is required"}
```

Rules:

- `id` is client-generated and matched exactly in the response
- `op` is a string RPC name
- `params` is an object whose fields depend on the operation
- `ok=true` carries a `result` object
- `ok=false` carries an `error` string

## Core operations

Connection lifecycle:

- `connect`
- `disconnect`
- `commit`
- `rollback`
- `set-auto-commit`
- `set-current-schema`

Execution and cursor flow:

- `execute`
- `fetch`
- `close-cursor`

Schema and object metadata:

- `get-tables`
- `search-tables`
- `get-columns`
- `get-primary-keys`
- `get-indexes`
- `get-index-columns`
- `get-sequences`
- `get-procedures`
- `get-functions`
- `get-procedure-params`
- `get-function-params`
- `get-triggers`
- `get-users`
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

## Error semantics

There are two distinct failure classes:

1. The agent is running but a request times out or the underlying JDBC session
   wedges.
   - Elisp reports this as a connection-loss error and clears the local agent
     state.

2. The agent exits before replying.
   - Elisp reads agent stderr and reports the startup failure directly.
   - If stderr contains `UnsupportedClassVersionError`, clutch reports that the
     configured Java runtime is too old for the current jar.

This distinction matters because a JVM startup failure should not look like a
normal database disconnect.

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
