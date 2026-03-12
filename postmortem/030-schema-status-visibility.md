# 030 — Make Schema Refresh State Explicit

## Background

Clutch already had schema caching and manual refresh, but the UI still left too
much to inference:

- eager backends could spend noticeable time refreshing schema after connect
- lazy backends could connect successfully while completions were still using no
  schema or stale schema
- result buffers showed query state, but not whether the attached schema view
  was fresh

This was especially easy to misread in slow metadata environments: the user saw
"connected" or a result grid, but not whether schema state was still loading,
stale, or failed.

## Decision

Add an explicit schema-status model with four visible states:

- `refreshing`
- `ready`
- `stale`
- `failed`

And surface it in two places:

1. the console mode-line, in compact form for non-ready states
2. the query console buffer name, which remains visible even under external
   mode-line frameworks such as doom-modeline

## Why a Small State Model

The problem was not the absence of more metadata; it was the absence of a
clear answer to a simple question:

"Is the schema behind completion / introspection fresh right now?"

Four states are enough to answer that without introducing a heavier lifecycle
framework:

- `refreshing` means clutch is actively trying to load table names
- `ready` means the current cache was refreshed successfully
- `stale` means the connection is usable, but schema should not be assumed fresh
- `failed` means a refresh attempt happened and did not succeed

## Why Distinguish Eager and Lazy Backends in UI

Clutch already had `clutch-db-eager-schema-refresh-p`, originally introduced so
Oracle JDBC would not block connect on slow metadata enumeration.

That backend distinction was correct, but the UI still treated "connected" as
if schema behavior were uniform. The better model is:

- eager backends: connect implies `refreshing -> ready/failed`
- lazy backends: connect starts at `stale`

That matches backend reality instead of hiding it.

## Why Put Status Into Existing Surfaces

Result buffers already have a status line, but schema freshness is not really a
property of a specific result set. It is a property of the connection metadata
cache.

The console mode-line already carries connection identity, but some user setups
replace or compress it heavily. Appending a schema marker to the console buffer
name gives a second visibility surface that is independent of modeline theme
choices. That marker now remains visible for the steady-state happy path too,
using a compact ready form such as `schema 42t`, so fast eager refreshes do not
flash and disappear before the user can notice them.

This keeps the state visible without mislabeling it as result state.

## Additional Rule

Successful schema-affecting statements now mark schema state stale:

- `CREATE`
- `ALTER`
- `DROP`
- `TRUNCATE`
- `RENAME`

That does not auto-refresh schema immediately; it simply stops implying that
the old cache is still current after DDL.

## Known Limitations

- The state model currently tracks table-name refresh freshness, not every lazy
  column-detail cache individually.
- A stale state tells the truth ("do not trust this cache as current"), but it
  does not yet provide one-click background refresh.
- PostgreSQL native `query-timeout` remains a separate in-progress effort and
  is not part of this change.
