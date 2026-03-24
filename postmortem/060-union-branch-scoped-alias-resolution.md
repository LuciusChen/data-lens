# 060 — UNION-Branch-Scoped Alias Resolution

## Decision

SQL completion alias resolution is scoped to the UNION branch
containing the cursor, not the entire statement.

## Why

A query like:

```sql
SELECT t.id FROM users t
UNION ALL
SELECT t.title FROM posts t
```

has the same alias `t` in both branches, mapping to different tables.
The old code extracted all aliases from the full statement and returned
the first match, so `t` always resolved to `users` regardless of cursor
position.  Completion in the second branch would offer `users` columns
instead of `posts` columns.

The real-world trigger was a query with UNION ALL inside a subquery,
where the alias mapped to `ffp_order_plan` in one branch and
`ffp_order_payoil` in the other.

## Approach

1. **`clutch--innermost-paren-range`** finds the tightest parenthesized
   block enclosing the cursor position.  This handles UNION ALL inside
   subqueries — the scope narrows to the subquery before splitting.

2. **`clutch--union-branch-range`** splits the scoped text by depth-0
   UNION / UNION ALL keywords and returns the segment containing the
   cursor.

3. **`clutch--extract-aliases-in-range`** re-extracts alias-to-table
   mappings from the narrowed range.

4. **`clutch--table-aliases-in-current-statement`** combines the above:
   if the range spans the whole statement (no UNION), it returns the
   cached full-statement aliases; otherwise it returns branch-scoped
   aliases.

## Alternatives Rejected

### Precompute per-branch aliases in the cache

Rejected because UNION-branch boundaries depend on cursor position
relative to parenthesized scopes, which change as the user types.
Caching per-branch results would require invalidation on every edit,
adding complexity for little gain since the re-extraction is fast
(single regex pass over a small substring).

## Consequences

- Alias completion in UNION queries now resolves to the correct table
  per branch.
- Non-UNION queries are unaffected (fast path returns cached aliases).
- The approach handles nested subqueries with UNION by first narrowing
  the scope to the innermost parenthesized block.

## Lesson Learned

The initial implementation only split by depth-0 UNION in the full
statement text, which failed when UNION ALL was inside a subquery
(the parentheses meant the UNION keywords were never at depth 0 of the
full statement).  The fix was to first narrow the scope to the innermost
enclosing parenthesized block, then split within that scope.  A `let`
vs `let*` bug in the first version of `clutch--innermost-paren-range`
was caught by tests.
