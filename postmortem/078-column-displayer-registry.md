#+title: Column Displayer Registry
#+date: 2026-04-06

* Context

Result buffers already had one rendering pipeline:

- raw value
- =clutch--format-value=
- truncation / padding
- cell face application

That kept the table renderer simple, but it also meant every column in every
table had to look the same.  Users had no narrow hook for common cases such as:

- BYTEA / blob thumbnails
- JSON summary labels
- clickable URL cells
- enum or status-code badges

* Decision

Add a per-table/per-column displayer registry in =clutch-ui.el=.

- Lookup keys are the detected source table name and column name
- Matching is case-insensitive
- The registered function receives the raw cell value
- Returning nil falls back to the default renderer
- Renderer failures are isolated at the UI boundary and fall back to the
  default display

The hook is intentionally narrow: it changes only the visible cell text in the
result table.

* Why Here

This is a UI concern, not a backend concern.

- Backends should keep returning raw values
- The value viewer should keep showing the raw value
- Edit / insert flows should keep using the raw value
- Cell face, padding, and borders should keep following the existing render
  rules

Putting the hook in =clutch--cell-display-content= keeps the behavior local to
the one place where raw values become table text.

* What We Did Not Add

We did not add:

- backend-specific display hooks
- value mutation hooks
- viewer-specific rendering hooks
- edit-buffer rendering hooks

That would blur the boundary between stored data and displayed text.  The
registry is deliberately only a result-table presentation override.
