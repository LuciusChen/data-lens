# 041. Oracle/JDBC Needed Background Schema Backfill, Not Just Fast Completion

Oracle/JDBC completion had already been split away from full schema refresh:

- table completion used direct `search-tables`
- column completion used direct `search-columns`
- connect no longer blocked on a full `get-tables`

That fixed the worst typing stalls, but it left one obvious semantic gap:

- completion worked immediately
- the console still stayed at `schema~`
- the eventual table count (`schema Nt`) never arrived unless the user ran an
  explicit refresh

So the UI was still reporting "schema cache stale" even after the user could
already complete Oracle identifiers successfully.

## What Changed

- JDBC gained an asynchronous schema-refresh path for `get-tables`
- `clutch` now uses it for lazy backends instead of only setting `stale`
- Oracle/JDBC connect and reconnect now start a background full table refresh
- schema-affecting DDL on Oracle/JDBC also schedules the same background refresh
- completion remains on the existing Oracle prefix fast path and does not wait
  for the background snapshot

## Why This Shape

There are really two different metadata products:

1. interactive identifier completion while the user is typing
2. a complete schema snapshot for cache-backed browser/status features

Oracle makes them very different cost profiles.  Prefix completion can be fast
enough to serve synchronously; full table enumeration is slower and should not
be on the typing path.

So the right model is:

- fast path for interactive completion
- background path for full cache truth

not one shared synchronous metadata step.

## Guardrails

Background refreshes are ticketed.  If a connection is replaced or a newer
refresh starts, older callbacks are ignored and cannot overwrite the newer
schema state.

## Outcome

- Oracle/JDBC connect stays non-blocking
- Oracle/JDBC completion stays responsive
- `schema~` can automatically converge to `schema Nt`
- cache-backed schema actions still reflect a real snapshot instead of guessing
