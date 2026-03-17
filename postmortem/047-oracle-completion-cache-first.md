# 047 — Oracle table completion: cache-first + cursor-drain helper

## Background

After 046 introduced cursor-based streaming for Oracle `get-tables`, two
follow-up cleanup opportunities were identified:

1. The "drain a cursor result into a flat row list" pattern was duplicated
   verbatim in `clutch-db-list-tables` and the async callback inside
   `clutch-db-refresh-schema-async`.

2. `clutch-db-complete-tables` for Oracle fired a `search-tables` RPC on every
   completion keystroke.  Now that `clutch--schema-cache` is populated by
   `clutch-db-refresh-schema-async`, the full table list is already available
   locally after the first schema refresh.

## Decisions

### Extract `clutch-jdbc--collect-table-rows`

A private helper drains a cursor-format `get-tables` result into a flat row
list.  If `:done` is `t`, it returns `:rows` directly.  Otherwise it calls
`clutch-jdbc--fetch-all` to page through the rest of the cursor.  Both
`clutch-db-list-tables` and the async callback now delegate to this helper.

### Cache-first Oracle completion

`clutch-db-complete-tables` now checks `clutch--schema-cache` keyed by
`(clutch--connection-key conn)` before firing any RPC:

- **Cache hit**: filter `(hash-table-keys schema)` locally with `string-prefix-p`
  (case-folded to uppercase to match Oracle convention).  No network round-trip.
- **Cache miss** (schema not yet loaded): fall back to the existing
  `search-tables` RPC so that completion works on a fresh connection before the
  first `C-c C-s`.

### Forward-declaration pattern for `clutch.el` symbols

`clutch-db-jdbc.el` is the JDBC backend and should not `(require 'clutch)`.
The CLAUDE.md architecture rule is "one-directional dependency flow".
`clutch--connection-key` and `clutch--schema-cache` live in `clutch.el`.

At call time, `clutch.el` is always loaded first (it is the UI layer that
dispatches completion), so both symbols are available.  The chosen pattern:

```elisp
(declare-function clutch--connection-key "clutch" (conn))
(defvar clutch--schema-cache)
```

`declare-function` silences the byte-compiler warning.  The bare `defvar`
marks `clutch--schema-cache` as a special (dynamic) variable in this file so
the bytecode accesses the global binding rather than an unrelated lexical slot.

### Test isolation for `clutch--schema-cache`

The test batch does not load `clutch.el`.  Without a `defvar` declaration in
`clutch-db-test.el`, a `let` binding of `clutch--schema-cache` in lexical-
binding mode would create a *lexical* binding invisible to the bytecode.

Fix: add `(defvar clutch--schema-cache (make-hash-table :test 'equal))` at the
top of the test file so `let` creates a *dynamic* binding.  Tests that need to
isolate the cache shadow it with a fresh hash table, and `clutch--connection-key`
is stubbed via `cl-letf`.

## Alternatives considered

**Move cache check to `clutch.el`** — skip the `clutch-db-complete-tables` call
when `schema` is already populated.  This avoids any cross-layer reference but
would require the new unit tests to cover the capf function in `clutch.el`
rather than the JDBC method in isolation.  The plan specified testing through
`clutch-db-complete-tables` directly, so the approach above was kept.

## Known limitations

`clutch-db-complete-columns` is not changed.  Column preloading is a separate
concern (async column detail fetch is already in place for Oracle).
