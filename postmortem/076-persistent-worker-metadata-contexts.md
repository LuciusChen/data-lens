# 076 — Reuse Native Worker Metadata Contexts

## Background

The first native async metadata slice fixed the biggest UI problem: MySQL and
PostgreSQL no longer blocked connect and hot metadata paths on the Emacs main
thread.

But the first implementation still paid a large cost on every background task:

- open metadata connection
- run one query
- close metadata connection

That was acceptable as a proof of direction, but it left an obvious mismatch
with the worker model itself.  The worker already existed per connection; only
the metadata session was still being treated as disposable.

For native backends, especially with TLS enabled, that meant the async path
could remain noticeably more expensive than it needed to be.

## Decision

Keep one reusable metadata context per native worker.

The boundary stays the same:

- `clutch-schema.el` and `clutch-object.el` decide when passive metadata work
  should run
- backend async methods decide how to reach metadata
- the worker remains a backend-internal implementation detail

What changes is session lifetime:

- MySQL / PostgreSQL open the metadata context lazily on the first worker task
- later tasks reuse that same context
- worker shutdown closes the context
- task errors reset the context so the next task can reopen cleanly

## Why Not Keep Per-Task Connect / Disconnect

Rejected because it solved the wrong problem.

Per-task connect / disconnect avoided foreground blocking, but it still made
passive metadata refresh much more expensive than the worker architecture
needed to be.  Once the package already had one worker per connection, keeping
metadata session lifetime at "one query" no longer matched the execution model.

## Why Diagnostics Stay Above the Worker

Rejected the alternative of teaching the worker about schema refresh,
column-detail preheat, object warmup, and similar UI-level operation names.

Those concepts belong to the cache lifecycle, not to the thread primitive.

The worker should know:

- how to queue work
- how to reuse or reset the metadata context
- how to deliver success or failure back to the main thread

`clutch-schema.el` / `clutch-object.el` should know:

- whether an async result is still current
- whether a callback became stale
- how to describe the operation in the debug trace

That keeps "threading" from leaking upward into clutch's user-facing metadata
model.

## Result

The native async path now matches the architecture more honestly:

- one worker per live native connection
- one reusable metadata context per worker
- stale-drop / success / error events recorded at the cache boundary

This keeps passive metadata work cheap enough to stay in the background without
turning the worker into a second UI abstraction layer.
