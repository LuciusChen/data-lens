# 084 — Native Metadata Latency Model

## Context

Native MySQL/PostgreSQL metadata used to sit directly on interactive paths:

- connect/reconnect blocked on initial schema snapshots
- CAPF and Eldoc could synchronously fetch metadata during typing
- explicit schema refresh and passive cache hydration were not clearly split

Those are different workflows and need different latency contracts.

## Final Decision

`clutch` now uses one metadata latency model:

- passive metadata hydration stays off the immediate command path
- native CAPF and Eldoc stay cache-first
- explicit `C-c C-s` schema refresh stays foreground
- native backends do not use Emacs Lisp worker threads

Native MySQL/PostgreSQL still expose async metadata entry points, but those
paths now defer work with idle-time callbacks on the main thread rather than
real worker threads. JDBC remains async through the external agent, and SQLite
keeps its synchronous in-process path.

## Why This Split

Background schema priming exists to keep connect usable sooner. Completion and
Eldoc must never block on metadata round-trips because they sit on typing and
point-motion hot paths. Explicit repair commands are different: when the user
presses `C-c C-s`, the useful contract is "refresh now", not "queued in the
background".

Result buffers follow the same rule. Initial render uses cached column detail
when available, then deferred metadata work fills the cache for later `?` and
related detail display.

## Why Worker Threads Were Abandoned

The first native implementation used one Emacs worker thread per connection.
That reduced foreground blocking but proved unsafe: unrelated package timers
could run on the worker thread, which caused real crashes on macOS.

The durable lesson is not "hide the worker better"; it is that native metadata
isolation in Emacs Lisp threads is the wrong boundary for `clutch`.

## Result

- connect/reconnect can return before passive metadata hydration finishes
- CAPF and Eldoc stay responsive and cache-first
- explicit refresh remains the trusted recovery path
- the package keeps deferred metadata behavior without owning unsafe worker
  threads
