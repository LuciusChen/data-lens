# 063 — Unified Database Error Humanization

## Context

Database errors were passed through raw to users: ClickHouse queryId/version
noise, Oracle error codes, Java stack traces, and redundant "Statement
failed:"/"UPDATE failed:"/"DELETE failed:" prefixes from staged mutation paths.

## Decision

Add a single `clutch--humanize-db-error` function with a pattern→hint lookup
table (`clutch--db-error-hints`), applied at all seven user-facing error paths:

1. SQL buffer overlay banner (`clutch--format-error-banner`)
2. Staged INSERT commit (`clutch-result-commit`)
3. Staged UPDATE commit (`clutch-result--confirm-and-run-updates`)
4. Staged DELETE commit (`clutch-result--confirm-and-run-deletes`)
5. Pagination query error
6. REPL error output
7. Connection failure (`clutch--build-conn`)

## Why centralized

Each path had its own ad-hoc formatting (prefix strings, truncation). A single
entry point ensures consistent noise stripping and hint lookup. The raw message
is still available in overlay `help-echo` for debugging.

## Noise stripping

- ClickHouse `(queryId=...)` and `(version N.N.N (official build))` suffixes
- Java stack traces (from first `at ` frame onward)
- Redundant `Database error:` prefix from error propagation layers
- Whitespace normalization

The version regex handles nested parens: `(version 26.2.5.45 (official build))`.

## Hint table design

The `clutch--db-error-hints` alist maps regex patterns to short English hints.
Hints are appended as `[hint]` suffix. Unknown errors pass through cleaned but
unhinted. The table covers ClickHouse, Oracle, MySQL, PostgreSQL, and generic
connection errors. It can be extended incrementally.

## Lessons

- All hints must be in English — user explicitly requested no Chinese text.
- ClickHouse delete error can be singular ("delete is") or plural ("deletes
  are"); regex must handle both: `Lightweight deletes? \(?:is\|are\) not supported`.
- Cross-file private function calls (`clutch-edit.el` → `clutch.el`) need
  `declare-function` for clean byte-compilation.
- Stale `.elc` files can mask regex fixes — always `rm -f *.elc` before
  recompiling after regex changes.
