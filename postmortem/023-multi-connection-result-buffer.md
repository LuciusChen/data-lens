# 023 — Multi-Connection Result Buffer Collisions and Spinner UX

## Background

Users running clutch with multiple simultaneous connections (e.g., production
PostgreSQL and a local SQLite) shared a single result buffer named
`*clutch-result: <database>*`. Two connections to the same database name would
write to the same buffer, causing results from one connection to be overwritten
by the other. Additionally, during a slow query, the UI gave no indication that
work was in progress — the console appeared to hang.

---

## Issue 1 — Result Buffer Name Collision

### Root Cause

`clutch--result-buffer-name` used `(clutch-db-database conn)` — the database
name alone — as the buffer identifier. Two connections with different hosts but
the same database name (common: `db` for both staging and production) produced
the same buffer name. A query on the second connection silently overwrote the
result of the first.

### Fix

Use the full connection key (`user@host:port/database`) as the buffer name
component. Each unique host + port + user + database combination gets its own
result buffer. Users can query production and staging in parallel without result
contamination.

---

## Issue 2 — Result Window Opens in Wrong Console

### Root Cause

`display-buffer-at-bottom` placed the result buffer at the absolute bottom of
the Emacs frame, regardless of which console window issued the query. With two
consoles open side by side, a query in the right console opened its result
below the left console.

### Fix

Capture `(selected-window)` at `clutch--execute` call time and bind it to the
dynamic variable `clutch--source-window`. Both `clutch--execute-select` and
`clutch--display-result` use this window as the reference for
`display-buffer-in-direction` with `direction: below`, so results always open
below the console that issued the query.

---

## Issue 3 — No Feedback During Execution

### Root Cause

`clutch--execute` calls `clutch-db-query` which blocks on
`accept-process-output`. For slow queries (seconds to minutes), there was no
indication that a query was in progress. Users could not distinguish "query
running" from "Emacs hung".

### Fix

Add `clutch--executing-p` (`defvar-local`), set to `t` around the query
execution body with `unwind-protect`. `clutch--update-mode-line` shows
`Backend[key …]` (with trailing `…`) while executing. `(redisplay t)` is called
before the blocking call to force the mode-line update to paint before execution
begins.

`unwind-protect` ensures the spinner clears even if the query signals an error —
without it, any query error would leave the `…` permanently in the mode-line.

---

## Lesson

Buffer names that encode partial connection identity cause silent collision bugs
in multi-connection workflows. Always use the full unique connection key wherever
a per-connection buffer is named.

Dynamic binding (`let ((clutch--source-window (selected-window)))`) is the
correct pattern for threading contextual state through a call chain that spans
display and execution logic without adding an extra parameter to every
intermediate function.

Visual feedback for blocking I/O must be forced with `(redisplay t)` before the
blocking call. Without it, Emacs queues the mode-line change and the user sees
nothing until after the call returns.
