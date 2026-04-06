# 074 — Keep Native Schema Refresh Off the Connect Path

## Background

After the package split, MySQL and PostgreSQL no longer lived inside
`clutch.el`, but their connect path still behaved like the old in-tree native
drivers: connect, reconnect, and schema switch could block the Emacs main
thread while the initial table snapshot loaded.

That was increasingly inconsistent with the rest of clutch:

- JDBC had already moved schema/object metadata off the foreground path
- the console status model already had `schema...` / `schema~` / `schema!`
- object warmup and async schema tickets already existed in the cache layer

The remaining problem was native backends still treating schema priming as part
of "successful connect" instead of as background cache hydration.

## Decision

Keep the existing async metadata API surface in `clutch-schema.el` /
`clutch-object.el`, and make native backends implement it behind the scenes.

The shape of the fix is:

- MySQL and PostgreSQL open an isolated metadata context for background schema
  refresh work
- worker threads stay a backend-internal mechanism, not a new UI-layer
  abstraction
- the schema cache lifecycle continues to use the existing ticket-based stale
  response guard
- SQLite remains synchronous for now because its in-process metadata path is
  not the bottleneck this change is trying to solve

## Rejected Alternatives

### Keep native refresh synchronous and just "hide" it in the UI

Rejected because the problem was not cosmetic.  The connect path itself was
doing too much work on the main thread.

### Make all metadata commands async

Rejected because explicit detail commands have different UX expectations from
passive cache hydration.  If the user asks to describe an object right now,
blocking for that answer is reasonable.  The async requirement applies to
connect-time and background metadata work, not every command that touches
metadata.

### Add a new top-level worker strategy abstraction

Rejected because clutch already had the right async API shape:

- `clutch-db-refresh-schema-async`
- `clutch-db-column-details-async`
- `clutch-db-list-objects-async`

Adding a second "strategy" layer in front of those would mostly duplicate
dispatch semantics without improving ownership.

## Result

The boundary is clearer now:

- `clutch-schema.el` decides when cache hydration should happen
- each backend decides how metadata isolation works
- JDBC keeps using its metadata session
- native MySQL/PostgreSQL use backend-internal workers plus isolated metadata
  connections

This keeps connect and reconnect responsive without forcing the rest of the UI
into an async state machine.
