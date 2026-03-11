# 016 — Completion Freeze from Schema-Scale Column Loading

## Background

`data-lens-completion-at-point` offered both table names and column names as
candidates. Column names were loaded lazily per table via `SHOW COLUMNS FROM
<table>` (MySQL) or the `information_schema` equivalent (PostgreSQL), and cached
in `data-lens--schema-cache`.

On databases with large schemas, the first invocation of completion triggered a
`SHOW COLUMNS` query for every table in the schema before returning any
candidates.

## Root Cause

The candidate-building loop iterated `(hash-table-keys schema)` — all known
tables — and called `data-lens--ensure-columns` for each. On a database with
410 tables, this issued 410 sequential blocking `SHOW COLUMNS` queries on first
completion. Each query takes 5–20ms on a local connection, producing a 2–8
second freeze on the first keystroke that triggered completion.

The schema hash table was populated by `SHOW TABLES` / `information_schema.tables`
at connect time (fast, one query). Column loading was supposed to be lazy, but
"lazy per table" across *all* tables was not meaningfully different from eager
when completion fired in a context where all tables were candidates.

## Fix

Extract `data-lens--tables-in-buffer`: scan the current buffer text once with
`string-match-p (regexp-quote tbl)` for each table name, returning only those
that actually appear in the buffer. The completion candidate loop iterates this
smaller set instead of the full schema.

In typical use, a SQL buffer references 1–5 tables. Column loading drops from
410 queries to 1–5 queries, making completion near-instant even with large
schemas.

## Trade-off

The buffer scan is O(tables × buffer-length). For enormous schemas in
combination with large SQL files this could become slow. In practice, schemas
with 1000+ tables are rare and SQL buffers are rarely more than a few KB. This
is an acceptable trade-off against the previous guaranteed freeze.

A more precise approach would parse the SQL FROM/JOIN clause for table names.
That was not implemented because it would require a partial SQL parser and the
buffer-scan approximation covers the common case well. Over-completion (loading
columns because a table name appears in a comment) is harmless; under-completion
(missing a dynamically constructed table name) is acceptable.

## Interaction with the Re-entrancy Guard

Postmortem 015 handles the case where completion fires *during* an in-flight
query: it skips column loading entirely and returns only table names. This fix
handles the case where completion fires while the connection is idle but the
schema is large. The two defenses are independent and complementary.

## Lesson

"Lazy loading" is only lazy if the trigger is scoped. Loading on first
completion invocation is only lazy relative to connection time — it is eager
relative to the user's first keystroke. Scoping lazy loading to the observable
context (tables actually present in the current buffer) is necessary for it to
remain imperceptible.
