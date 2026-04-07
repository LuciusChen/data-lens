# 083 — Postmortem Consolidation Plan

## Why this audit exists

The `postmortem/` directory is valuable because it records *why* `clutch`
arrived at its current behavior, not just what the code does today.

That also means stale reports are dangerous in two different ways:

- a report can describe an implementation detail that no longer exists
- several reports can describe one design arc in fragments, forcing future
  readers to reconstruct the final lesson from multiple files

This audit does **not** treat every old implementation as disposable history.
If a report still captures a load-bearing lesson, it should stay even when the
code later moved or was renamed.

## Safe delete now

### Delete `076-persistent-worker-metadata-contexts.md`

Reason: it is fully superseded by
`082-abandon-native-metadata-worker-threads.md`.

`076` only explains how to make native worker-thread metadata contexts cheaper
and more persistent. `082` later records that the entire native worker-thread
approach was abandoned as unsafe on Emacs because unrelated timers can run on
the worker thread. Once that decision landed, the session-lifetime tuning from
`076` stopped being load-bearing.

The important surviving lesson is no longer "reuse the worker metadata
context"; it is "do not create native metadata worker threads in the first
place". `082` already captures that final lesson.

## Merge candidates

### Cluster A — Native metadata latency model

Files:

- `074-native-schema-refresh-off-connect-path.md`
- `075-cache-first-metadata-hot-paths.md`
- `080-explicit-schema-refresh-stays-foreground.md`
- `082-abandon-native-metadata-worker-threads.md`

Why merge:

- all four files describe one design arc: move passive metadata off the hot
  path, keep editing-time metadata cache-first, keep explicit repair
  foreground, and eventually replace worker threads with idle main-thread
  scheduling
- `074` and `075` still contain useful product decisions, but they mention
  worker-thread details that are no longer true after `082`
- `080` is still current, but it reads as a follow-up patch unless the reader
  already knows the surrounding metadata model

Proposed merged file:

- `native-metadata-latency-model.md`

Proposed delete after merge:

- delete `074`, `075`, `080`, `082`

Draft merged content:

> # Native Metadata Latency Model
>
> ## Context
>
> Native MySQL/PostgreSQL metadata used to sit directly on interactive paths:
>
> - connect/reconnect blocked on initial schema snapshots
> - CAPF and Eldoc could synchronously fetch metadata during typing
> - explicit schema refresh and passive cache hydration were not clearly split
>
> Those are different workflows and need different latency contracts.
>
> ## Final decision
>
> `clutch` now uses one metadata latency model:
>
> - passive metadata hydration stays off the immediate command path
> - CAPF and Eldoc stay cache-first
> - explicit `C-c C-s` schema refresh stays foreground
> - native backends do **not** use Emacs Lisp worker threads
>
> Native MySQL/PostgreSQL still expose async metadata entry points, but those
> paths now defer work with idle-time callbacks on the main thread rather than
> real worker threads. JDBC remains async through the external agent.
>
> ## Why this split
>
> Background schema priming exists to keep connect usable sooner.
> Completion and Eldoc must never block on metadata round-trips because they sit
> on typing and point-motion hot paths.
> Explicit repair commands are different: when the user presses `C-c C-s`, the
> useful contract is "refresh now", not "queued in the background".
>
> ## Why worker threads were abandoned
>
> The first native implementation used one Emacs worker thread per connection.
> That reduced foreground blocking but proved unsafe: unrelated package timers
> could run on the worker thread, which caused real crashes on macOS.
>
> The durable lesson is not "hide the worker better"; it is that native
> metadata isolation in Emacs Lisp threads is the wrong boundary for `clutch`.
>
> ## Result
>
> - connect/reconnect can return before passive metadata hydration finishes
> - CAPF / Eldoc stay responsive and cache-first
> - explicit refresh remains the trusted recovery path
> - the package keeps deferred metadata behavior without owning unsafe worker
>   threads

### Cluster B — Oracle/JDBC completion versus schema cache

Files:

- `037-oracle-completion-vs-schema-refresh.md`
- `041-oracle-schema-refresh-backfill.md`
- `042-lazy-schema-refresh-failure-exit.md`

Why merge:

- all three describe the same split between fast Oracle completion and slower
  full schema snapshots
- `037` establishes the model
- `041` adds background backfill so `schema~` can converge to a real snapshot
- `042` adds failure-exit semantics, but its manual-refresh conclusion was later
  reversed package-wide by `080`

Proposed merged file:

- `oracle-jdbc-completion-and-schema-cache.md`

Proposed delete after merge:

- delete `037`, `041`, `042`

Draft merged content:

> # Oracle/JDBC Completion and Schema Cache Are Different Products
>
> ## Context
>
> Oracle made an existing design mistake visible: interactive identifier
> completion and full schema snapshots were treated as if they had the same
> latency budget.
>
> They do not.
>
> Oracle table/column prefix lookup can be narrow and fast enough for typing.
> Full table enumeration is slower and belongs in cache-hydration paths instead.
>
> ## Final decision
>
> Keep the split explicit:
>
> - completion uses direct Oracle/JDBC prefix lookups
> - full schema snapshots are backfilled separately for cache-backed browsing
> - async schema refreshes need a visible failure/timeout exit instead of
>   assuming they always converge
>
> ## Important note
>
> One part of the earlier design did not survive unchanged: manual refresh no
> longer follows the old Oracle-specific "always background" rule. The package
> later converged on one user-visible contract: explicit `C-c C-s` refresh is a
> foreground recovery action across backends.
>
> The lesson that remains load-bearing is the split between fast completion and
> slower cache truth, plus the need for async refresh failure states.
>
> ## Result
>
> - Oracle connect does not depend on a full schema snapshot
> - completion does not wait for the schema cache
> - schema status still honestly reflects cache freshness
> - async refreshes can fail visibly instead of hanging forever in
>   `schema...`

### Cluster C — Oracle large-schema enumeration

Files:

- `046-oracle-schema-refresh-cursor-streaming.md`
- `047-oracle-completion-cache-first.md`

Why merge:

- `047` is a tight follow-up to `046`
- both describe the same "large Oracle schema" stabilization arc:
  cursor-format table enumeration plus local cache-first completion once the
  snapshot exists
- keeping them separate makes the reader reconstruct one implementation pass
  from two files

Proposed merged file:

- `oracle-large-schema-enumeration.md`

Proposed delete after merge:

- delete `046`, `047`

Draft merged content:

> # Oracle Large-Schema Enumeration
>
> ## Context
>
> Oracle deployments with very large table counts exposed two different scaling
> costs:
>
> - `get-tables` could not complete within one giant synchronous metadata
>   payload
> - table completion should not keep re-querying the agent once a schema
>   snapshot already exists locally
>
> ## Final decision
>
> Use cursor-based streaming for large Oracle table enumeration, then make table
> completion cache-first against the completed snapshot.
>
> The agent streams table batches through the existing cursor/fetch protocol.
> The Elisp side drains those batches into the schema cache.
> Once the cache exists, Oracle table completion filters locally instead of
> issuing a `search-tables` RPC on every keystroke.
>
> ## Result
>
> - large Oracle schema refreshes no longer depend on one monolithic payload
> - completion benefits from the cached snapshot after the first successful
>   refresh
> - the JDBC/Elisp contract stays one-directional: the backend exposes data, the
>   UI decides how to use cached versus remote metadata

## Keep as standalone records

Do **not** merge or delete these just because they mention old code names such
as `data-lens` or because later implementation details changed:

- `005-sql-rewrite-engine.md`
- `015-reentrant-completion-corruption.md`
- `016-completion-schema-scale.md`
- `021-pixel-scroll-vscroll-partial-row.md`
- `022-cursor-scroll-preservation-on-refresh.md`

They still explain project guardrails that remain load-bearing today:

- SQL rewriting must stay clause-aware
- completion/network I/O is vulnerable to Emacs event-loop re-entrancy
- "lazy" metadata can still freeze the first completion if the scope is wrong
- result rendering must preserve visual correctness during scroll/refresh

Those are architectural lessons, not merely implementation notes.
