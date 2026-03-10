# 001 — Staged Commit Workflow for Insert and Delete

## Background

Edit-cell had a two-step workflow from the start: stage the change
visually, then `C-c C-c` to commit. Delete and insert did not:

- **Delete**: `d` → `yes-or-no-p` confirmation → immediate execute
- **Insert**: fill buffer → `C-c C-c` → `yes-or-no-p` → immediate execute

Three different mental models for three mutation types. Users had to
remember which commands staged and which executed immediately.

## Decision

Unify all three under a single staged-commit model:

- `d` marks rows red (strikethrough) in `clutch--pending-deletes`
- `i` opens fill buffer → `C-c C-c` returns a green ghost row to the
  result buffer via `clutch--pending-inserts`
- `C-c C-c` in the result buffer commits everything at once: shows all
  pending SQL in a single confirmation, then executes INSERT → UPDATE →
  DELETE in that order
- `C-c C-k` discards the staged change at point

## Why

**One mental model.** `C-c C-c` always means "commit what's staged".
Users never need to ask "does this execute immediately or stage?".

**Review before commit.** Staging makes all pending changes visible at
once. You can stage 5 deletions, inspect them visually, then decide.

**Mixed commits.** Edits + inserts + deletes can be committed together
in one atomic interaction — matching how professional DB tools work
(DataGrip's "Submit Changes", pgAdmin's edit grid).

## Commit Order: INSERT → UPDATE → DELETE

This order is the safest:

- INSERT first: new rows exist before any UPDATE might reference them
  (e.g., updating a FK column to point to a just-inserted row)
- UPDATE second: modifies existing rows while all rows still exist
- DELETE last: ensures UPDATE targets are not prematurely removed; also
  avoids violating FK constraints on rows you haven't inserted yet

## State Representation

- `clutch--pending-deletes`: list of ridx integers (indices into
  `clutch--result-rows`). Simple; correlates directly with the render loop.
- `clutch--pending-inserts`: list of field alists `((col . val) ...)`,
  appended in chronological order via `append`. Preserves user input
  structure; maps directly to `clutch-result-insert--build-sql`.

Ghost insert rows use synthetic ridx = `(length real-rows) + iidx` so
`clutch-row-idx` text properties remain consistent for navigation and
`clutch-result-discard-pending-at-point` detection.

## Alternatives Considered

**Keep delete immediate with yes/no**: inconsistent with edit-cell; users
can't stage multiple deletes to review together.

**Separate "pending mutations" buffer showing SQL**: over-engineering.
Adds a third buffer type and navigation complexity for no real gain; the
confirmation dialog already shows the SQL.

**Single `clutch--pending-mutations` list mixing all types**: harder to
build type-specific SQL statements, harder to render differently per type
(red vs green), harder to reason about commit ordering.

## Known Limitation: Client-Side Filter Interaction

`clutch--pending-deletes` stores ridx values from the render loop. When
`clutch--filtered-rows` is active, these indices are positions in the
*filtered* list, not in `clutch--result-rows`. Committing while a filter
is active will target wrong rows.

This is a pre-existing issue shared with edit-cell (same ridx semantics).
**Do not stage mutations while a client-side filter is active.** A proper
fix requires storing primary-key values instead of render-loop indices —
deferred until the edit-cell system is also updated.
