# 031 — PostgreSQL Query Timeout Without Breaking Transaction Recovery

## Background

Native PostgreSQL support already had connect timeout and read-idle timeout,
but not a true database-side statement timeout.

Adding `query-timeout` to `pg.el` looked straightforward: issue
`SET statement_timeout = ...` before each query and restore the default after.

That worked for simple queries, but transaction error handling exposed a more
important constraint.

## Problem

In PostgreSQL, once a transaction enters the failed state, ordinary commands are
rejected until `ROLLBACK`.

The first implementation tried to apply timeout uniformly to every query,
including:

- `BEGIN`
- `COMMIT`
- `ROLLBACK`

That created a bad interaction:

1. a statement inside a transaction failed
2. the transaction entered aborted state
3. clutch tried to run `ROLLBACK`
4. but first it attempted `SET statement_timeout = ...`
5. PostgreSQL rejected that `SET` with `25P02`
6. the actual `ROLLBACK` never ran

So the connection remained stuck in aborted transaction state.

## Decision

Treat transaction control statements as a special class that must bypass the
timeout wrapper:

- `BEGIN`
- `START TRANSACTION`
- `COMMIT`
- `END`
- `ROLLBACK`
- `ABORT`
- `SAVEPOINT`
- `RELEASE`

Regular statements still receive the timeout wrapper. Transaction control
statements do not.

## Why This Fix

The real invariant is not "every round-trip must be wrapped in
statement_timeout". The invariant is:

"Query timeout should work, but transaction recovery must always remain
possible."

Once that is clear, `ROLLBACK` must win over timeout instrumentation.

## Additional Follow-up

The timeout unification layer in `clutch.el` also needed to pass
`:query-timeout` to the native PostgreSQL backend. Without that, the new
feature only worked for direct `pg-connect` users and not for normal clutch
connection flows.

## Validation

The corrected behavior was verified in two live scenarios:

1. `SELECT pg_sleep(2)` fails under a `1s` query timeout, and the same
   connection remains usable afterward
2. after a statement error inside `BEGIN`, an explicit `ROLLBACK` succeeds and
   clears the aborted transaction state

## Known Limitations

- This is still implemented by setting `statement_timeout` around statements,
  not by a lower-level protocol cancel request.
- Native MySQL-family backends remain a separate case: older MySQL versions do
  not provide a clean PostgreSQL-like general statement timeout.
