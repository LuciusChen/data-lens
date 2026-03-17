# 048 — JDBC transaction control moves into the UI contract

## Background

JDBC backends originally inherited the driver default of `autoCommit=true`.
That was convenient, but unsafe for Oracle query-console use: every
`INSERT`/`UPDATE`/`DELETE` became permanent immediately.

The practical requirement changed once Oracle became a first-class workflow.
For that audience, manual commit is the safer default.

## Decision

Add three backend generics:

- `clutch-db-manual-commit-p`
- `clutch-db-commit`
- `clutch-db-rollback`

JDBC implements them by calling the agent protocol.  Oracle JDBC connections
default to manual-commit mode; other JDBC backends remain auto-commit unless
`:manual-commit t` is set explicitly.  The Oracle default is itself configurable
through `clutch-jdbc-oracle-manual-commit`, and `:manual-commit` overrides the
global default when present in a connection plist.

`clutch.el` now tracks uncommitted DML in a weak hash keyed by the live
connection object and exposes:

- mode-line indicator `"[TX*]"`
- `clutch-commit`
- `clutch-rollback`
- disconnect / reconnect replacement guards

## Why The Dirty State Lives In `clutch.el`

The agent should not know about UI notions like mode-line badges, prompts, or
"safe to disconnect" checks.  It only exposes JDBC primitives.

The UI already centralizes query execution flows.  Recording dirty state there
keeps the protocol thin while covering every DML entry path:

- query console execution
- multi-statement execution
- result-buffer staged INSERT/UPDATE/DELETE
- REPL execution

## Important Correction

Dirty state is **not** keyed by `clutch--connection-key`.

That string identifies a profile (`user@host:port/db`), not a specific session.
Reusing it would leak `[TX*]` across reconnects.  The correct identity is the
live connection object itself.

## Oracle DDL

Oracle DDL auto-commits even when auto-commit is disabled.  The UI therefore
clears `[TX*]` after successful Oracle schema-affecting DDL to stay aligned
with the server's actual transaction state.

## Release Note

This depends on a matching agent protocol release.  Update the bundled agent
version/checksum in `clutch-db-jdbc.el` only after the published jar exists.
