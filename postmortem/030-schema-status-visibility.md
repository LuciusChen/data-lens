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

## Follow-Through: Turn State Into a Recovery Hint

The original change made schema freshness visible, but still left too much of
the recovery step implicit. Seeing `schema~` or `schema!` was truthful, but the
UI still expected the user to remember which command repaired that state.

The follow-through decision is intentionally small:

- keep buffer-name markers compact
- use the console mode-line to append the direct recovery hint
- converge console refresh and schema-browser refresh on the same
  connection-level refresh action

This keeps the state model simple while making the stale/failed path more
operational:

- `schema~` now points directly to refresh
- `schema!` now points directly to retry
- schema browser refresh reuses the same connection refresh semantics instead of
  behaving like a parallel workflow

The next follow-through keeps the same philosophy for cache-backed actions:

- do not auto-refresh schema just because a prompt wants table names
- do not silently pretend cached metadata is current
- do show a direct recovery hint before cache-backed table prompts and similar
  schema-dependent actions

That keeps performance predictable while still telling the truth about cache
freshness.

The same state should remain visible after the user leaves the console and
enters the schema browser.  The schema browser now renders a compact status
line near the top of the buffer for `stale`, `failed`, and `refreshing`
states, using the same recovery vocabulary (`g` / `C-c C-s`) instead of
inventing a different browser-only language.

## Known Limitations

- The state model currently tracks table-name refresh freshness, not every lazy
  column-detail cache individually.
- The model still does not auto-refresh schema in the background; this remains
  deliberate because synchronous or overly eager refreshes can regress slow
  backends.
- Completion and other cache-backed helpers now warn more clearly when schema is
  stale or failed, but they still do not auto-heal the cache on their own.
- The schema browser status line is still a summary, not a richer background
  job UI; it does not show refresh progress beyond the current coarse state.
- PostgreSQL native `query-timeout` remains a separate in-progress effort and
  is not part of this change.
