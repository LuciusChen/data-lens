# 082 — Abandon Native Metadata Worker Threads

## Background

Clutch moved native MySQL/PostgreSQL schema refresh and passive metadata
preheat off the foreground path by introducing one Emacs Lisp worker thread per
connection. The intended boundary looked reasonable:

- keep the existing async metadata API in `clutch-schema.el` and
  `clutch-object.el`
- let native backends hide the thread and metadata-session details
- preserve `schema...` / `schema!` status reporting without forcing the UI into
  an async state machine

That design reduced foreground blocking, but it assumed Emacs worker threads
could safely isolate passive database I/O from the rest of the session.

## What Failed

They cannot.

On macOS, a live crash showed unrelated timer code from another package
(`sis__auto_refresh_timer_function`) executing on the `clutch-metadata` worker
thread. The resulting call into `mac_input_source_properties` aborted Emacs,
because that code must run on the main thread.

The key point is that this was not a MySQL-specific SQL or authentication bug.
The root problem was Emacs thread semantics:

- a worker thread that waits for process output or idles in the event loop can
  run ordinary timer callbacks
- those callbacks are not scoped to clutch
- once clutch creates a Lisp worker thread, it becomes possible for unrelated
  package code to run there

That makes "background thread for native metadata" the wrong abstraction
boundary for clutch.

## Decision

Remove native metadata worker threads entirely.

Native MySQL/PostgreSQL keep the existing async metadata API surface, but the
implementation changes:

- `clutch-db-refresh-schema-async`
- `clutch-db-list-columns-async`
- `clutch-db-column-details-async`
- `clutch-db-table-comment-async`
- `clutch-db-list-objects-async`

These now schedule work back onto the main thread with idle timers instead of
running on a separate Emacs thread.

JDBC is unchanged because its async metadata path lives in the external agent,
not in an Emacs Lisp worker thread.

## Why Not Keep the Worker Behind More Guards

Rejected because the crash source was not clutch's own callback delivery.

Even if clutch stopped using `run-at-time` inside the worker, the worker still
had to wait for network process output. That waiting point is enough for Emacs
to run unrelated timers in the worker thread. The unsafe part was the existence
of the Lisp worker itself, not just one helper used to hand results back.

## Why Idle-Time Main-Thread Scheduling Is Acceptable

It preserves the user-facing intent of the async API:

- connect/reconnect can still return before passive metadata hydration finishes
- schema/object warmup stays off the immediate command path
- cache tickets and stale-drop logic remain valid

What changes is the isolation model:

- no real parallelism for native metadata
- no separate metadata connection/session lifetime
- no risk of arbitrary package timers running in a clutch-owned worker thread

This trades some responsiveness on slow native metadata queries for session
stability, which is the right trade once the crash mode is understood.
