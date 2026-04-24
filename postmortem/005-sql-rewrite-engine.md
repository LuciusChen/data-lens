# 005 — SQL Rewrite Engine: Pagination, COUNT, and WHERE Filter

## Background

clutch needs to rewrite user SQL for three purposes:
1. **Pagination**: wrap with LIMIT/OFFSET
2. **COUNT**: rewrite to `SELECT COUNT(*)` for total-row counting
3. **WHERE filter**: inject a filter clause from the client-side filter UI

Each rewrite had multiple failed iterations before arriving at the
current approach. The guardrails in AGENTS.md exist because of these
failures.

## Pagination: Top-Level LIMIT Detection

**Failed approach**: `string-match-p "\\bLIMIT\\b"` on raw SQL.
False positives on queries like `SELECT * FROM (SELECT ... LIMIT 5) t`.
The inner LIMIT triggered the check, causing clutch to skip its own
LIMIT/OFFSET injection and show all rows.

**Current approach** (`ad2b994`): `clutch-db-sql-has-top-level-limit-p`
uses `clutch--sql-find-top-level-clause` to detect LIMIT only at the
top level of the query (parenthesis-depth 0). Subquery LIMITs are
correctly ignored.

## COUNT Rewrite: Avoiding Duplicate Column Name Errors

**Failed approach 1**: Wrap in a subquery:
`SELECT COUNT(*) FROM (...user SQL...) AS _cnt`

MySQL error [1060] "Duplicate column name" fires when the inner SELECT
has duplicate column names (e.g., `SELECT t1.id, t2.id FROM t1 JOIN t2`
— both columns are named `id`). The derived table inherits the ambiguous
names. (`94b32db`, `3ea922b`)

**Failed approach 2**: Replace the SELECT list, keeping FROM onwards.
Worked for simple queries but broke on `GROUP BY`/`HAVING` (counting
groups, not rows) and still failed for JOIN queries with duplicate names.

**Current approach** (`3ea922b`): Direct SELECT-list rewriting using
`clutch--sql-find-top-level-clause` to locate the FROM position, then
replacing `SELECT ... FROM` with `SELECT COUNT(*) FROM`. Falls back to
`clutch--sql-rewrite-fallback` (subquery wrapping) only when the
clause-level rewrite cannot determine the structure.

## WHERE Filter: Derived Table Wrapping

**Failed approach**: Inject `WHERE`/`AND` before `ORDER BY`/`GROUP BY`/
`HAVING` tail clauses. Brittle: broke on CTEs (`WITH ... AS (...)`),
UNIONs, subqueries in the FROM clause, and nested SQL where the tail
clause detection gave false matches. (`eb0970b`)

**Current approach**: Wrap the entire user query in a derived table:
```sql
SELECT * FROM (...user SQL...) AS _clutch_filter WHERE <filter>
```

This is semantically safe regardless of the inner query structure. CTEs,
UNIONs, window functions, nested subqueries — all handled correctly
because the outer WHERE only sees the derived table's columns. The alias
`_clutch_filter` is stable and unlikely to collide.

## `clutch--sql-rewrite`: Unified Entry Point

`clutch--sql-rewrite` is the single function for all rewrites. It calls
`clutch--sql-find-top-level-clause` for clause-aware detection, then
applies the appropriate transformation with `condition-case` fallback to
`clutch--sql-rewrite-fallback` for queries that cannot be parsed at the
clause level.

`clutch--sql-find-top-level-clause` tracks parenthesis depth and string
literal state to find clauses only at the top level of the query.

## AGENTS.md Guardrails (Why They Exist)

These rules were written after the above failures:

- **Do not rewrite SQL by brittle raw string insertion of `WHERE` /
  `ORDER BY` / `LIMIT`.**  String insertion that doesn't track nesting
  will break on CTEs, UNIONs, subqueries.

- **Prefer top-level clause-aware transformations with safe fallback.**
  `clutch--sql-find-top-level-clause` is the correct primitive. Use it.

- **For complex queries, prioritize semantic correctness over aggressive
  rewriting.**  If unsure, use the derived-table wrapper — it's always
  semantically safe even if it produces less elegant SQL.

## Roadmap: AST-Level Rewriting

The current approach is clause-level heuristics with fallback. A proper
AST-based SQL parser would handle all edge cases without heuristics.

This is explicitly on the roadmap (`eb0970b`) but not implemented because
the incremental complexity of a full SQL AST parser is not justified by
the current bug surface. The derived-table fallback handles all known
failure cases.

**Do not force full AST complexity into small fixes.** When a fix requires
understanding the full query tree, add a case to the fallback instead and
note it as an AST-roadmap item.

## Risky DML Guardrails

Two tiers of protection for destructive queries:

1. **Standard** (`DELETE`, `DROP`, `TRUNCATE`, `ALTER`): `yes-or-no-p`
   confirmation.

2. **Risky DML** (`UPDATE` or `DELETE` without a top-level `WHERE`):
   requires typing `YES` in a `read-string` prompt. A plain `yes-or-no-p`
   is too easy to dismiss accidentally for table-wide mutations.

`clutch--risky-dml-p` uses `clutch--sql-find-top-level-clause` to check
for the absence of a WHERE clause — not a simple regex, to avoid false
negatives from WHERE inside subqueries.
