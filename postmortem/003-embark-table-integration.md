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

`C-c C-d`'s fallback to completing-read when not on a symbol silently
duplicated `C-c C-j`'s completing-read, creating two paths to the same
UX with no distinction the user could reason about.

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

`C-c C-d` made strict: errors if not on a symbol (no fallback).

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

## Why Remove `C-c C-d` Fallback

Before: `C-c C-d` not on symbol → completing-read for describe.
After: `C-c C-d` not on symbol → `user-error "No table name at point"`.

The fallback created two identical workflows to the same outcome
(completing-read → describe). With Embark, the unified path is:
`C-c C-j` (completing-read) → Embark `d` (describe). A second
completing-read entry point adds no value and confuses the mental model
of what `C-c C-d` is for.

`C-c C-d` now has a single, precise semantics: act on the table whose
name the cursor is already on. This makes it a genuine complement to
`C-c C-j`, not a redundant alias.

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
