# 017 — Subquery Wrapping Fails on Duplicate Column Names

## Background

Pagination and total-count queries both required transforming a user-supplied
SELECT into a new SQL statement. The initial approach wrapped the original query
in a derived table (subquery) and applied the transformation at the outer level:

- **Pagination**: `SELECT * FROM (<user-sql>) AS _dl_t LIMIT N OFFSET M`
- **Count**: `SELECT COUNT(*) FROM (<user-sql>) AS _dl_cnt`

This appeared to work correctly for typical single-table queries.

## Root Cause

Both MySQL and PostgreSQL require that all columns in a derived table (subquery
used as a table reference) have unique names. A query like:

```sql
SELECT t1.channel, t2.channel FROM events t1 JOIN events t2 ON ...
```

has two columns both named `channel`. Wrapping this in a subquery produces:

```
[1060] Duplicate column name 'channel'   (MySQL)
ERROR: column "channel" specified more than once   (PostgreSQL)
```

The error surfaces as a complete query failure. Users who write JOIN queries
selecting the same column name from multiple tables cannot paginate or count
their results. This was not caught during initial development because test
queries used single-table SELECTs with unique column names.

## Fix — Pagination (commits `94b32db`, `b56bbe0`)

Remove the subquery wrapper entirely. Append `LIMIT N OFFSET M` directly to the
trimmed SQL string:

```
<user-sql> [ORDER BY ...] LIMIT N OFFSET M
```

This is semantically correct for SQL dialects that support inline `LIMIT` /
`OFFSET` (MySQL, PostgreSQL, SQLite). The derived table was never necessary for
pagination — it was cargo-culted from a pattern used in ORMs that need to apply
window functions to arbitrary queries. Applied first to MySQL
(`clutch-db-mysql.el`), then identically to PostgreSQL (`clutch-db-pg.el`).

## Fix — Count Query (commit `3ea922b`)

The `COUNT(*)` transform cannot simply append — it needs to replace the SELECT
column list with `COUNT(*)`. Direct subquery removal requires SQL structure
awareness.

A depth-aware top-level clause finder (`clutch--sql-find-top-level-clause`)
scans the SQL at parenthesis depth 0 to locate `FROM`, `GROUP BY`, `HAVING`,
`ORDER BY`, and `LIMIT` positions without being confused by subqueries or CTEs
that contain the same keywords.

For simple SELECT statements (no `GROUP BY`, no `HAVING`): rewrite as
`SELECT COUNT(*) FROM ... [WHERE ...]`, stripping trailing `ORDER BY` and
`LIMIT` clauses which are irrelevant to counting.

For aggregated queries (`GROUP BY` / `HAVING`): the count of rows in the result
set cannot be obtained by rewriting — the derived table wrapping is semantically
required. Fall back to `SELECT COUNT(*) FROM (...) AS _dl_cnt`. These queries
are unlikely to have duplicate column names at the outer level since `GROUP BY`
typically produces a single canonical column per group key.

## Why Three Commits

The pagination fix came first (simpler case: no rewriting needed, just remove
the wrapper). The COUNT fix required more analysis because replacing the SELECT
column list demands clause-level SQL awareness. The PostgreSQL pagination fix was
a separate commit because the pg backend's identical subquery pattern was
identified after the MySQL fix was merged.

## Lesson

Wrapping user SQL in a derived table is not a neutral operation. It imposes
constraints the user's query did not have: unique column names, no bare `ORDER
BY` at the outer level in some dialects, and restricted use of windowing
constructs. The correct mental model is: derive the minimum transformation that
achieves the goal. For pagination, direct append achieves the goal with no new
constraints. For COUNT, clause-level rewriting achieves the goal for the common
case while a subquery fallback handles the remainder.
