# 020 — DESCRIBE and SHOW Results Routed to DML Display Path

## Background

`data-lens--execute` classified queries as either SELECT (returns a tabular
result) or DML (INSERT / UPDATE / DELETE / DDL, returns an affected-row count).
The classification used `data-lens--select-query-p`, which recognized `SELECT`
and `WITH` as the start keywords.

`DESCRIBE`, `SHOW`, and `EXPLAIN` queries return tabular results from the
server — column definitions and rows, exactly like a SELECT — but do not start
with `SELECT`.

## Root Cause

The execution router sent any query that failed `data-lens--select-query-p` to
`data-lens--execute-dml`, which displayed the result using
`data-lens--display-dml-result`. That function only extracted `affected-rows`
from the result struct and showed "Affected rows: N". It ignored the `columns`
and `rows` fields entirely.

`DESCRIBE users` sent to this path displayed "Affected rows: 0" — showing
nothing useful.

The routing assumption was wrong: *non-SELECT query* ≠ *DML query*. The
protocol layer correctly returned a `mysql-result` with populated `columns` and
`rows` for `DESCRIBE` and `SHOW`. The UI layer discarded that data because it
never checked whether a non-SELECT result had columns.

## Fix

In `data-lens--display-result` (the DML display entry point), check whether the
result struct carries column definitions before choosing a display path:

```elisp
(if col-names
    ;; Tabular result (DESCRIBE, SHOW, EXPLAIN, etc.)
    (progn ... (data-lens--display-select-result col-names rows columns))
  ;; DML result
  (data-lens--display-dml-result result sql elapsed))
```

Non-paginated tabular display is used: no base query, no page navigation. This
matches the semantics — `DESCRIBE` / `SHOW` are metadata snapshots, not paged
data sets.

As a companion fix, `data-lens--execute-page` now guards against being called
when `data-lens--base-query` is nil, issuing `user-error "Pagination not
available for this query"` instead of silently re-running the last paged query.

## Lesson

Route query results on the *actual result shape* — does the result have column
definitions? — rather than on the inferred type of the input SQL. The server is
the authority on what shape a query produces. Classifying input syntax and
assuming the corresponding output shape breaks for any command that doesn't fit
the SELECT / DML binary.
