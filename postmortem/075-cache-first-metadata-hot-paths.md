# 075 — Keep Metadata Hot Paths Cache-First

## Background

Moving native MySQL/PostgreSQL schema refresh off the connect path fixed the
largest UI stall, but it did not finish the job.

Two hot paths still had the old behavior:

- CAPF could synchronously fetch column names while the user was typing
- Eldoc could synchronously fetch column details or table comments while point
  moved through SQL

Those paths are qualitatively different from explicit metadata commands.
Blocking for `describe table`, `show definition`, or result-buffer `?` is a
reasonable contract because the user explicitly asked for detail.  Blocking in
completion or Eldoc is not.

## Decision

Keep the hot paths cache-first.

The rule is:

- completion / eldoc only read cache on the foreground path
- cache misses queue background metadata preheat through the existing async
  callback surface
- explicit detail commands remain synchronous

This keeps the clutch UI model simple:

- no async state machine for CAPF or Eldoc
- no "Loading..." minibuffer churn on every point move
- no second workflow vocabulary for "sometimes async, sometimes sync" metadata
  commands

Result buffers follow the same rule.  Initial render uses cached column detail
when available, then background preheat fills the cache for later `?` and
related metadata display.

## Rejected Alternatives

### Keep synchronous metadata loads but just narrow the query

Rejected because even "small" metadata round-trips are still foreground I/O.
The bug was not only query size; it was that hot paths were allowed to block
at all.

### Make every metadata command async

Rejected because it would force describe / DDL / explicit detail workflows into
state machines that provide no user benefit.  Those commands already have a
clear, acceptable synchronous contract.

### Disable metadata features when background loading is unavailable

Rejected because that would make clutch worse on builds without thread support.
The right fallback is the older synchronous behavior, not feature loss.

## Result

Metadata behavior is now split by UX intent instead of by backend accident:

- passive surfaces preheat in the background
- explicit detail commands stay synchronous
- native backends fall back to synchronous metadata loading if worker threads
  are unavailable

That keeps typing and cursor motion responsive without turning the rest of the
package into an async-only UI architecture.
