# 057 — Isolate JDBC Metadata Traffic from Foreground SQL

## Background

Oracle JDBC connections became noticeably laggy after connect, and some query
runs ended with clutch showing `Not connected` immediately afterwards.

The visible symptoms were:

- `C-c C-j` and other object/schema actions sometimes felt slow again
- ordinary foreground queries became slower than before
- after a query finished, the console could flip into a disconnected state

## Root cause

The problem was not the header-line or mode-line UI.  It was the JDBC session
model.

Before this change, one logical clutch JDBC connection mapped to exactly one
JDBC `Connection` inside `clutch-jdbc-agent`.  That single session handled both:

- foreground SQL execution
- schema refresh
- object warmup
- column and object metadata introspection

On Oracle, that meant background metadata traffic could contend directly with
foreground queries on the same server session.

## Decision

Keep the Elisp-side workflow unchanged and fix the problem in the JDBC agent.

`clutch-jdbc-agent` now maintains two JDBC sessions per logical clutch
connection:

- `primary`: foreground SQL execution, transactions, DDL, commit/rollback
- `metadata`: schemas, tables, columns, indexes, routines, triggers, and other
  introspection RPCs

Runtime schema switching updates both sessions together so the logical clutch
connection still presents one effective schema.

## Rejected alternatives

### Disable warmup or async refresh in Elisp

Rejected because it only hides the symptom by reducing concurrency.  It does
not fix the underlying shared-session contention, and it would degrade the
object picker experience again.

### Pause warmup while commands are active

Rejected as another timing workaround.  It adds more lifecycle state to
`clutch.el` while leaving the real resource-sharing problem intact.

## Result

The correct boundary is now clearer:

- `clutch.el` decides *when* metadata is requested
- `clutch-jdbc-agent` decides *how* JDBC sessions are isolated so metadata work
  does not destabilize foreground SQL

The follow-through fix keeps the same boundary:

- JDBC object warmup now uses async metadata RPCs instead of synchronous
  `clutch-db-list-objects` calls on the Emacs main thread
- background object discovery no longer needs to freeze the UI just because it
  happens after connect
- Oracle/JDBC live verification also depends on running the agent with a Java
  runtime new enough for the current `clutch-jdbc-agent` jar; otherwise startup
  failures can masquerade as generic connection-loss errors
