# 019 — BLOB Placeholder Shown for Short Strings in BLOB-Type Columns

## Background

MySQL's wire protocol includes column type metadata. Columns of type `BLOB`,
`TEXT`, `MEDIUMBLOB`, etc. are flagged by `data-lens--long-field-type-p`. The
renderer showed a `<BLOB>`, `<TEXT>`, or `<JSON>` placeholder for these columns
instead of their content, to avoid rendering potentially megabyte-sized binary
values inline.

The `DESCRIBE <table>` command returns a result set that always includes a
`Type` column and a `Default` column. MySQL defines both as `BLOB`-typed in the
protocol metadata, even though in practice they always contain short
human-readable strings like `"varchar(255)"` or `"NULL"`.

## Root Cause

The placeholder logic checked only the column's type flag:

```elisp
(if (data-lens--long-field-type-p col-def)
    (data-lens--long-field-placeholder col-def)
  (format-value display-val))
```

`data-lens--long-field-type-p` returns non-nil for any `BLOB` / `TEXT` / `JSON`
column regardless of the actual value size or type. When `DESCRIBE users` was
executed, every cell in the `Type` and `Default` columns showed `<BLOB>`
instead of the actual type string — making `DESCRIBE` output unreadable.

The same issue affected any user-defined table where a `TEXT` column happened to
contain short values.

## Fix

Add two additional conditions before showing the placeholder:

1. The formatted string length exceeds the column's display width — the value is
   actually long enough to warrant truncation.
2. The Lisp value is not a string — it is actual binary data.

```elisp
(if (and (not edited)
         (data-lens--long-field-type-p col-def)
         (> (length s) w)
         (not (stringp display-val)))
    (data-lens--long-field-placeholder col-def)
  s)
```

Condition 2 (`not (stringp display-val)`) is the critical gate: the pg.el and
mysql.el backends return binary BLOB data as byte vectors, not Lisp strings.
Short text fields (including `DESCRIBE`'s `Type` column) always arrive as proper
Lisp strings. This cleanly distinguishes "binary BLOB data" from "string stored
in a BLOB-typed column".

## Lesson

Column type metadata describes the database's declared storage class, not the
actual value shape. A column typed `BLOB` may contain a string; a `VARCHAR`
column may contain binary data after encoding. Any heuristic that maps type
metadata to display behavior must also examine the runtime value to avoid false
positives. Guard on value type (Lisp representation) in addition to schema type.
