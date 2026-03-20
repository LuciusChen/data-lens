# 056 — Prioritize Runtime Schema Switching over Server Objects

## Background

After the object-centric schema workflow settled, the next candidate feature was
`server objects`: users, roles, tablespaces, collations, modules, and similar
admin-level metadata.

That looked attractive in the abstract, but the concrete requirements turned
out to be weak and backend-specific:

- Oracle, MySQL, and SQLite expose different categories
- the default action is not consistent across those categories
- the actual day-to-day workflow was still centered on schema objects
- a real missing workflow was switching the active business schema inside one
  connection, for example from `zj_test` to `cjh_test`

In other words, the pressure was not "show me more admin objects", but "let one
connection move between the schemas I actually work in".

## Decision

Do not continue expanding `server objects` right now.

Instead:

- keep the server-object experiment out of the branch
- add runtime schema switching for a single active connection
- keep `C-c C-j` / `C-c C-d` / `C-c C-o` focused on schema objects

The first schema-switching implementation supports:

- Oracle/JDBC: switch current business schema within one connection
- MySQL: switch current database via `USE ...`

## Why

Schema switching has a clear, high-frequency workflow:

- change the active schema
- refresh metadata
- keep using the same object picker and SQL console

Server objects did not have the same clarity.  The categories differ by backend,
the actions differ by object kind, and the feature risked becoming a large,
low-frequency surface added mainly for parity theater.

This also fits the project rule in `AGENTS.md`:

- converge UX around a small number of clear entry points
- avoid building a second object universe without a proven workflow
- prefer the smallest concrete feature that unlocks real work

## Alternatives considered

### Expand server objects first

Rejected because the categories were already diverging (`users`, `roles`,
`tablespaces`, `collations`, `virtual views`, `modules`, `routines`) before a
clear cross-backend workflow existed.

### Implement DataGrip-style multi-schema workspace immediately

Rejected as too large for the first step.  One connection with one effective
schema is enough to unlock the practical use case without introducing a second
selection model or multi-schema cache complexity.

## Accepted limitations

- This is not a multi-schema explorer.  One connection still has one active
  schema/database at a time.
- PostgreSQL and other backends are not part of the first runtime switching
  implementation.
- The object cache key should eventually widen to
  `(connection, effective-schema)` once switching becomes a more heavily used
  workflow.
