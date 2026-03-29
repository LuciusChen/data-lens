# 003 — Embark Integration for Table Operations

## Background

Table operations were scattered across the UI with overlapping entry points:

- `C-c C-j` → completing-read → browse (INSERT SELECT * into console)
- `C-c C-d` → describe at point OR completing-read if not on a symbol
- Schema buffer: `RET` describe, `v` browse — mode-specific bindings
- Transient menu: `t` list, `D` describe, `j` browse

When a user invoked `C-c C-j`, selected a table in completing-read, then
wanted to describe it instead, they had to cancel and re-invoke `C-c C-d`.
No way to switch action mid-flight.

The harder problem was that these entry points each carried slightly different
object-resolution rules.  Users had to predict which command would act at
point, which one would prompt, and which metadata was already available.

## Decision

Define a `clutch-table` Embark target type with three recognition contexts:

1. **completing-read minibuffer**: annotate table collections with
   `(category . clutch-table)` via `clutch--read-table-name`. Embark's
   built-in minibuffer target finder picks this up automatically.
2. **Schema buffer**: table name via `clutch-schema-table` text property
   (present on table header lines and column detail lines).
3. **SQL buffer**: symbol at point, validated against schema cache via
   `gethash`.

Action map (`clutch-embark-table-actions`, inheriting `embark-general-map`):
- `b` browse-table
- `d` describe-table
- `w` copy table name to kill ring

`C-c C-d` remains a describe-focused wrapper over the same object resolution
pipeline.  It resolves the object at point when possible and otherwise prompts
with the shared picker.

## Why `category` Metadata for completing-read

This is the idiomatic Embark mechanism. The completing-read framework
passes `category` metadata to Embark's `embark-target-completion-at-point`
automatically — no custom minibuffer target finder needed.

The alternative (a target finder checking `this-command` or
`minibuffer-history-variable`) is fragile: it breaks if commands are
renamed, aliased, or called indirectly. Category metadata is stable and
command-name-agnostic.

`clutch--read-table-name` centralizes the annotation. All table
completing-reads go through it; none need to add category metadata
individually.

## Why Keep Shared Resolution Logic

`C-c C-j`, `C-c C-d`, and `C-c C-o` now all sit on the same object model and
resolution path.  The difference is the action they run after resolution:

- `C-c C-j` prefers browse
- `C-c C-d` prefers describe
- `C-c C-o` prefers the action picker

Keeping the fallback prompt for `C-c C-d` is intentional.  It preserves the
direct "describe this object" workflow while still benefiting from the same
flat picker, object warming, and Embark integration as the other entry points.
The simplification is in the shared resolver and action model, not in forcing
every command into an at-point-only contract.

## Optional Dependency Design

`clutch--read-table-name` adds `(category . clutch-table)` metadata
unconditionally. This is harmless without Embark — completing-read
ignores unknown metadata — and avoids conditional code paths elsewhere.

The Embark-specific pieces (`defvar-keymap`, `add-to-list` calls) are
wrapped in `(with-eval-after-load 'embark)`. clutch loads and functions
fully without Embark installed.

`embark-general-map` is set as `:parent` of `clutch-embark-table-actions`
so general Embark actions (insert, isearch, describe symbol, etc.) remain
available on `clutch-table` targets. Without the parent, the action map
would shadow general actions rather than extend them.

## Limitations

**Schema cache dependency**: the SQL buffer target finder validates the
symbol against `clutch--schema-for-connection`. If the cache is not yet
populated (first connection, no schema refresh), valid table names at
point will not be recognized. The user can trigger `clutch-list-tables`
to populate the cache.

**Column lines in schema buffer**: the target finder activates on any
line with `clutch-schema-table` text property, including column detail
lines. This is intentional — describing the parent table from a column
line is a natural action.
