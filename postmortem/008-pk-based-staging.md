# 008 — PK-Based Staging for Pending Mutations

## Background

`clutch--pending-deletes` originally stored ridx integers — the loop counter from the render pass in `clutch--insert-data-rows`. `clutch--pending-edits` used `(ridx . cidx)` pairs as alist keys. At commit time, both SQL builders did `(nth ridx clutch--result-rows)` to recover the row.

## The Bug

When `clutch--filtered-rows` is active (user pressed `f` to filter), `clutch--insert-data-rows` iterates over the filtered list. The ridx counter starts at 0 for the first *filtered* row. But `clutch--result-rows` is the full unfiltered list. So `(nth ridx clutch--result-rows)` looked up the wrong row — silently generating DELETE/UPDATE statements targeting unrelated records.

Example: filter shows rows [5, 12, 23] from a 30-row result. Pressing `d` on the first visible row stores ridx=0. Commit does `(nth 0 clutch--result-rows)` → row with id=1, not id=5.

## Decision

Store PK values at staging time, not ridx.

- `clutch--pending-deletes`: list of pk-value vectors (one vector per staged row).
- `clutch--pending-edits`: alist of `((pk-vec . cidx) . new-value)`.

At commit time, the WHERE clause is built directly from the stored pk-vec. No row lookup needed. The vectors are compared with `equal`, so `cl-find`, `cl-remove`, and hash-table lookups use `:test #'equal`.

### Cache for render loop

The render loop calls `clutch--row-pending-delete-p` per row, which needs pk-indices. Calling `clutch-result--detect-primary-key` (a DB query) inside the render loop is unacceptable. Fix: cache pk-indices as `clutch--cached-pk-indices` at the same point FK info is loaded, after each query execution.

## Alternatives Considered

**Keep ridx, fix lookup**: At commit time, look up from `(or clutch--filtered-rows clutch--result-rows)` instead of `clutch--result-rows`. Rejected: the filter might have changed between staging and commit. If the user stages a delete, then clears the filter, ridx no longer points to the right row.

**Use row vector equality**: Store the full row vector, compare with `equal` at commit time. Rejected: rows may not be unique if there are duplicate records. PK is the correct identity criterion.

**Silent ridx fallback when no PK**: Rejected. Silent fallback would allow destructive SQL on wrong rows. `user-error` at staging time is far safer — it forces the user to acknowledge the limitation.

## Known Limitations

Tables without a detectable primary key cannot use delete or edit staging. `clutch-result--detect-primary-key` returns nil for such tables and the staging functions call `user-error`. This is deliberate.
