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

- Add `clutch-execute-statement-at-point` on `C-c ;`.
  It uses semicolons as the only statement delimiter and ignores blank lines.
- Add `clutch-result-column-info` on `?` in result buffers.
  It shows column type/default/nullable/comment info at point.

## Why `C-c ;` Exists Beside `C-c C-c`

`C-c C-c` remains the DWIM entry point:

- region if active
- otherwise query-at-point using the existing lightweight boundary rules

That command is still useful for quick fragments and legacy behavior.  The new
`C-c ;` is not a replacement; it is the explicit "run the semicolon-delimited
statement I am inside" command.

Separating the commands keeps both workflows legible:

- `C-c C-c`: broad DWIM
- `C-c ;`: exact statement execution

The direct key is `;` rather than `RET` because Emacs normalizes `RET` and
`C-m` to the same event.  `C-c RET` therefore collides with `C-c C-m`, which is
already used for transaction commit.

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

### Replace `C-c C-c` semantics entirely

Rejected because it would silently change an established execute path instead of
adding a more explicit one.

### Use paragraph / blank-line heuristics for the new command

Rejected because those heuristics encode formatting conventions rather than SQL
structure.

### Expose column info only through object describe

Rejected because it forces users to leave the result-grid context for a small,
point-local question.

## Consequences

- SQL buffers now have an explicit semicolon-scoped execute command.
- Result buffers expose column metadata directly at point.
- Existing DWIM execute behavior remains available for users who prefer it.
