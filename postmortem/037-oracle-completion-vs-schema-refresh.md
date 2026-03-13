# 037 — Oracle Completion Had Different Performance Needs Than Schema Refresh

## Background

Oracle/JDBC had already been moved to lazy schema refresh so connect would not
block on slow metadata enumeration.  That was correct for connect latency, but
it exposed a second problem:

- completion still depended on schema cache in the general path
- column completion still used synchronous metadata loading

That meant two bad outcomes were possible:

1. no completion at all when Oracle stayed in `schema~`
2. visible completion stalls when metadata calls ran synchronously in the hot
   path

## Root Cause

Two different jobs had been treated as if they were the same thing:

- schema refresh for cache-backed browsing
- interactive identifier completion while the user is typing

For Oracle they have very different cost profiles.  A full schema refresh can
be expensive enough that it must stay off the connect path.  Completion, on the
other hand, needs narrow prefix-based answers with predictable latency.

## Decision

Split the two responsibilities:

- schema refresh remains cache-oriented and explicit
- Oracle/JDBC completion uses direct prefix lookups instead of requiring a full
  schema cache

The practical model is now:

- table completion can query matching Oracle tables/views directly
- column completion can query matching Oracle columns for the tables already
  referenced in the current statement
- eldoc no longer synchronously forces Oracle column metadata loads during point
  movement

## Why This Touches Both Repos

The fix is cross-layer:

- `clutch-jdbc-agent` needs Oracle-specific fast metadata queries for prefix
  search and column listing
- `clutch` needs to stop assuming every backend should synchronously hydrate
  columns during completion

Fixing only one side would leave either the wrong orchestration or the wrong
query path in place.

## Consequences

- Oracle connect remains non-blocking.
- Oracle completion no longer depends on a successful full schema refresh.
- Cache-backed schema actions still honestly surface `schema~` / `schema!`
  because that state now refers to schema-cache freshness, not whether
  completion is fundamentally available.

## Known Limitations

- Schema browser and other cache-backed workflows still use the schema cache and
  may still require an explicit refresh on slow Oracle environments.
- Prefix completion is intentionally conservative; it trades exhaustive
  preloading for lower interactive latency.
