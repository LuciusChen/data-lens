# 009 — SQL Error Position Overlay

## Background

When a query fails, the DB error message is shown via `user-error`. The user must read the message, mentally find the offending token in the SQL buffer, and fix it. For longer queries this is tedious.

PostgreSQL's wire protocol includes a character offset (`?P` field) in error responses. It was already parsed by `pg--parse-error-fields` but discarded by `pg--error-fields-message`.

## Decision

Expose PG's character position as a red wave-underline overlay in the SQL source buffer, placed at the exact character where PG reported the error.

### Implementation approach

1. `pg--error-fields-message` appends ` (position N)` to the formatted message when `?P` is present. This keeps the protocol boundary clean — the position travels as part of the string that already crosses the `clutch-db-error` signal boundary.

2. `clutch--parse-error-position` extracts the number with a regex. Pure function, no side effects.

3. `clutch--executing-sql-start` is a dynamic variable bound in `clutch--execute-and-mark` to `beg` (the buffer position where the SQL text starts). This gives the error handler the offset needed to translate PG's 1-based character count to a buffer position.

4. The overlay is stored in `clutch--error-position-overlay` (buffer-local on the SQL buffer). It is cleared at the top of each `clutch--execute` call. No timer — the overlay persists until the user re-runs a query, which is the natural moment to dismiss it.

## Alternatives Considered

**Pass `beg` as argument through `clutch--execute`**: Would require touching the signature of `clutch--execute` and several callers. The dynamic variable approach is consistent with `clutch--source-window` which already uses the same pattern.

**Timed auto-dismiss**: Overlay disappears after N seconds. Rejected: the user may be reading a long error message or switching windows. Clearing on next execution is the least surprising behavior.

**MySQL support**: MySQL's wire-protocol error does not include a character offset. The `near 'TOKEN'` phrase in the message could be used to search the SQL buffer, but the token may appear multiple times, making placement unreliable. Deferred.

**Face colors inline vs. defface**: Using a named face allows users to customize via `M-x customize-face`. Required by CLAUDE.md for all user-visible appearance.

## Known Limitations

- PG only. MySQL error position is unavailable in the standard protocol error message.
- The overlay covers exactly one character. If the error position points between tokens (e.g., end of line), the highlighted character may not be the visually intuitive one. PG's own `psql` has the same limitation.
- If the SQL buffer is killed between execution and error display, `clutch--mark-error-position` silently does nothing (guarded by `buffer-live-p`).
