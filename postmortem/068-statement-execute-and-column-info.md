# 068 — Statement-Scoped Execute and Inline Column Info

## Context

Two friction points remained in the main `clutch` workflows:

- In SQL buffers, `clutch-execute` still treated blank lines as statement
  boundaries when no region was active.  That made it awkward to keep one
  logical statement visually spaced across multiple paragraphs.
- In result buffers, column metadata already existed in the schema layer, but
  there was no direct way to inspect it from the grid itself.

Both are user-visible workflow changes, so the decision needs to be explicit.

## Decision

- Add `clutch-execute-statement-at-point`.
  It uses semicolons as the only statement delimiter and ignores blank lines.
- Teach `clutch-execute-dwim` to prefer that semicolon-delimited statement
  behavior whenever the current SQL buffer contains top-level semicolons.
  The direct `C-c ;` binding was later removed once the DWIM path absorbed the
  same safe default.
- Add `clutch-result-column-info` on `?` in result buffers.
  It shows column type/default/nullable/comment info at point.

## Why DWIM Now Prefers Statement Semantics in SQL Buffers

`C-c C-c` remains the DWIM entry point:

- region if active
- otherwise semicolon-delimited statement-at-point when the buffer is clearly
  using semicolon-delimited SQL
- otherwise query-at-point using the lightweight blank-line-aware boundary rules

That keeps the default execution key aligned with real SQL editing.  When a
buffer already contains top-level semicolons, the more precise statement
boundary is the safer interpretation.  The blank-line query-at-point fallback
still matters for quick fragments and scratch-style buffers with no semicolons.

The explicit `clutch-execute-statement-at-point` command remains available for
`M-x` and transient workflows, but it no longer needs its own dedicated key in
`clutch-mode`.

This preserves both workflows without forcing users to remember a second
execute key:

- `C-c C-c`: broad DWIM, but semicolon-aware when the buffer needs it
- `M-x clutch-execute-statement-at-point` / transient `X`: exact statement execution

## Why Blank Lines Must Not Split Statements Here

Blank lines are a formatting tool, not SQL syntax.  In real query editing,
users often insert visual spacing inside one statement:

- long `SELECT` lists
- CTE-heavy queries
- grouped predicates

Treating blank lines as hard boundaries makes formatting change semantics.
That is the wrong abstraction boundary.  The explicit statement command should
follow SQL delimiters, not editor layout.

## Why Column Info Belongs in the Result Buffer

Once a user is in the result grid, the active question is usually about the
visible data:

- what type is this column really
- is it nullable
- does it have a default or comment

Requiring a separate object-describe workflow for that is too indirect.
The result buffer already knows the active query and visible columns, so it is
the right place to surface column metadata inline.

The chosen shape is intentionally lightweight:

- `?` for an explicit message at point
- fetch details lazily when missing

This keeps the workflow discoverable without turning result rendering into a
heavy describe UI.

## Alternatives Rejected

### Keep `C-c C-c` on blank-line query parsing even in semicolon-delimited buffers

Rejected because it makes the primary execute key unsafe for valid SQL strings
or formatted statements that contain blank lines.

### Use paragraph / blank-line heuristics for the new command

Rejected because those heuristics encode formatting conventions rather than SQL
structure.

### Expose column info only through object describe

Rejected because it forces users to leave the result-grid context for a small,
point-local question.

## Consequences

- SQL buffers now use semicolon-scoped execution by default when the buffer is
  clearly semicolon-delimited.
- The explicit semicolon-scoped command remains available for command-driven
  workflows, but no longer needs a dedicated `C-c ;` binding.
- Result buffers expose column metadata directly at point.
- Existing blank-line query-at-point behavior remains available as the fallback
  when no top-level semicolons are present.
