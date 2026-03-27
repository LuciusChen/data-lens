# 064 — ClickHouse JDBC Catalog Scoping and Database Switching

## Context

ClickHouse support was added through the JDBC path, and the first pass looked
healthy on simple connection tests: connect worked, ordinary queries worked,
and known table names could still be described.

The broken behavior showed up in metadata-driven workflows:

- object picker / describe showed duplicate table names from multiple databases
- table search and schema warmup were polluted by `INFORMATION_SCHEMA` and
  other databases
- `C-c C-l` still followed the generic "switch schema" mental model, which does
  not match ClickHouse

## Root cause

The bug was not in the picker UI or schema cache timing.  It was in the JDBC
metadata model.

ClickHouse maps its current database to JDBC `catalog`, not `schema`.  The
published `clutch-jdbc-agent` protocol only threaded `schema` through metadata
RPCs such as:

- `get-tables`
- `search-tables`
- `get-columns`
- related PK/FK/index/procedure metadata calls

That made ClickHouse metadata requests ambiguous:

- `schema=nil` returned objects from every visible database
- `schema="default"` returned nothing useful because ClickHouse does not expose
  business databases as JDBC schemas

## Decision

Fix the protocol and agent first, then keep Elisp thin.

### JDBC agent

`clutch-jdbc-agent` now accepts optional `catalog` for generic metadata RPCs
and passes both `catalog` and `schema` to `DatabaseMetaData`.

ClickHouse needed one more detail: its JDBC driver may return the database name
in `*_SCHEM` while leaving `*_CAT` blank even when the request was filtered by
catalog.  The agent therefore filters metadata rows by:

1. `*_CAT` when present
2. `*_SCHEM` as a fallback when `*_CAT` is blank

This keeps the fix in the metadata layer instead of compensating in Emacs.

### Elisp side

`clutch-db-jdbc.el` now sends ClickHouse `:database` as metadata `catalog`.
The temporary ClickHouse-specific SQL fallback (`system.tables`,
`system.columns`, ad-hoc `SHOW CREATE`) was removed once the agent fix was in
place.

## Why not keep the Elisp workaround

The SQL fallback proved the symptom, but it was the wrong ownership boundary:

- it duplicated metadata behavior only for one backend
- it left the JDBC protocol incorrect
- it would have drifted from PK/FK/index/object metadata over time

Per the project rule, this belonged in the JDBC agent.

## Switching databases

ClickHouse does not fit the existing runtime schema-switch model.

The generic `C-c C-l` command already meant "switch current namespace" for
Oracle and MySQL, but ClickHouse has databases rather than business schemas.
Official ClickHouse docs also note that `USE db` is session-based and cannot be
used over HTTP because there is no session concept there.  The JDBC URL used by
clutch defaults to the HTTP port (`8123`), so a session-local `USE` model is
the wrong foundation.

The chosen behavior is:

- keep one command: `C-c C-l`
- on ClickHouse, list databases via `SHOW DATABASES`
- reconnect with the selected `:database`
- clear metadata caches and refresh the header line

This keeps the user-facing entry point converged while respecting the actual
backend model.

## Header-line follow-through

The first implementation switched databases correctly but still rendered only
"current schema" in the query-console header line.  That made ClickHouse look
stale even after reconnect.

The fix was to treat the header-line namespace segment as
"current schema/database":

- ClickHouse shows `clutch-db-database`
- other backends continue to show `clutch-db-current-schema`

## Lessons

- Metadata scoping bugs should be diagnosed with live object-discovery tests,
  not only `SELECT 1`.
- JDBC `schema` is not a universal "current namespace" abstraction.
- A temporary Elisp fallback is acceptable as a diagnosis aid, but the final
  fix still needs to move to the agent when the protocol layer is wrong.
- If a command becomes namespace-generic, user-facing docs and header-line copy
  must stop saying only "schema".
