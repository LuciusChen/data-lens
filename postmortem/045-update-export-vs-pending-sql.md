# 045. UPDATE Export and Pending SQL Are Different Workflows

Issue #3 started as a request for SQL export, but the discussion clarified two
different user needs that should not be collapsed into one command family.

## The Split

There are two distinct things a user may want from a result buffer:

1. turn the current result set into SQL text
2. export the exact staged mutations that would run on commit

Those are related, but they are not the same workflow.

### Result Export

`INSERT` and `UPDATE` in copy/export menus belong to a "data extractor" model:

- take the current result rows
- represent them as SQL text
- copy or save that representation

This is why `UPDATE` belongs next to `TSV`, `CSV`, and `INSERT` in the result
copy/export UI.  It answers the same question: "how should this result set be
serialized?"

### Pending SQL

Pending SQL is different:

- it is not a result-set serialization
- it is the exact staged mutation batch
- it should match preview/commit semantics

If a user has staged two cell edits and one delete, pending SQL should export
exactly those three statements and nothing else.  It should not re-derive SQL
from the visible result rows.

That makes pending SQL part of the mutation workflow, not part of the copy
extractor family.

## Why Pending SQL Does Not Belong In `c`

The `c` transient is result-oriented:

- copy current selection/result as TSV
- copy current selection/result as CSV
- copy current selection/result as INSERT
- copy current selection/result as UPDATE

Putting pending SQL in the same menu would mix two mental models:

- "copy this result as a format"
- "copy the staged changes I have accumulated"

Those should stay separate.  Pending SQL therefore lives in the result
transient as its own action group, near preview/commit behavior.

## Why UPDATE Export Needs Extra Guardrails

`INSERT` export can serialize arbitrary result values into rows more freely.
`UPDATE` export cannot.

To generate defensible `UPDATE` statements, clutch needs all of these:

- a detectable source table
- a stable primary key
- selected columns that map to real source columns

If the result includes aliases or computed columns such as:

```sql
SELECT id, name, now() AS ts FROM users
```

then exporting that as `UPDATE` would be misleading unless clutch silently
dropped `ts`, which would make the command hard to reason about.

The chosen rule is simpler and safer:

- reject `UPDATE` export when selected columns are not real source columns
- explain the failure clearly

This keeps `UPDATE` export strict and predictable.

## Outcome

- result copy/export now includes `UPDATE`
- pending SQL is exported separately as the exact staged batch
- `UPDATE` export is guarded so it only operates on real source columns
- the two workflows stay aligned with their real semantics instead of being
  merged into one vague "SQL export" bucket
