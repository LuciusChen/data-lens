# 042. Lazy Schema Refresh Needed Failure Exit and One Consistent Manual Path

Oracle/JDBC had already moved to a better metadata model:

- interactive completion used Oracle-specific prefix lookups
- full schema snapshots were backfilled in the background
- connect no longer blocked on `get-tables`

That solved the original "completion is empty or connect hangs" problem, but it
still left two inconsistencies.

## Problem 1: Async Refresh Had No Failure Exit

The first background refresh was asynchronous, but the state model still acted
like "refreshing" would always eventually converge to "ready".

If the JDBC agent stalled, dropped a response, or a metadata call took too
long, the console could sit at `schema...` indefinitely.  That looked like a
performance issue, but the real problem was a missing state transition:

- request started
- no response arrived
- nothing converted that into a visible failure

Asynchronous work still needs a timeout path and cleanup, otherwise "non-
blocking" just becomes "silently stuck".

## Problem 2: Manual Refresh Still Used the Old Synchronous Model

After lazy backfill existed, `C-c C-s` still ran the old synchronous refresh
path.  So the same backend had two different schema-refresh semantics:

- automatic refresh after connect or DDL: asynchronous
- manual refresh: synchronous

For Oracle/JDBC this was the wrong split.  It meant users could still be pushed
back into the blocking `get-tables` path even though the design had already
accepted that full schema enumeration should not sit on the interactive path.

Manual refresh should not secretly reintroduce the exact synchronous behavior
that lazy refresh was meant to remove.

## What Changed

- asynchronous JDBC schema refresh now has a timeout/failure exit
- timed-out async requests are removed from the callback registry
- lazy-backend `C-c C-s` now starts the same background refresh model instead of
  forcing a synchronous schema load
- schema guidance text was tightened so it no longer implies that Oracle/JDBC
  completion itself depends on the full schema cache

## Why The Wording Changed Too

Once Oracle/JDBC completion was decoupled from full schema refresh, the old
messages became misleading:

- `schema~` did **not** mean completion was unavailable
- `schema...` did **not** mean typing had to wait

What actually lagged behind was:

- cached table-name snapshots
- schema browser state
- table count in the console name

So the guidance now talks about cache-backed schema state, not identifier
completion in general.

## Outcome

- lazy backends no longer get stuck forever in `schema...` just because an async
  response never arrived
- manual schema refresh no longer regresses Oracle/JDBC into the old blocking
  model
- status text matches the real split between fast completion and slower cache
  truth
