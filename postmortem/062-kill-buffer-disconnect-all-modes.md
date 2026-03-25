# 062 — Connection Lifecycle: Owner Model

## Context

GitHub issue #7: killing a clutch buffer left the underlying process
alive, leaking connections.

## Failed Approach: Last-Holder Disconnects

The first attempt hooked `clutch--disconnect-on-kill` into every mode
that holds `clutch-connection`, with `clutch--connection-shared-p` to
skip disconnect when other buffers still referenced the same connection.

This turned "process leak" into "connection splitting": closing the
console while result buffers remained kept the old connection alive;
reopening the console created a new connection, so two sessions coexisted
under one logical name.

## Final Design: Owner Model

Only **owner buffers** disconnect.  Owners are:

- `clutch-mode` buffers (excluding `clutch-indirect-mode`)
- `clutch-repl-mode` buffers

Derived buffers (`clutch-result-mode`, `clutch-record-mode`,
`clutch-describe-mode`) never disconnect.  When an owner disconnects
(kill, explicit disconnect, or reconnect), it:

1. Marks DML result buffers as connection-closed
2. Invalidates all derived buffers (`clutch-connection` → nil,
   `force-mode-line-update`)
3. Clears transaction dirty state
4. Disconnects the underlying connection

Derived buffers retain cached data for viewing.  Operations that need
the connection (`clutch--ensure-connection`) report:
"Connection closed.  Reconnect from the SQL buffer or REPL"

## Guard Mechanism

`clutch--disconnect-on-kill` uses
`(not (bound-and-true-p clutch-indirect-mode))` to distinguish owners
from indirect SQL editing buffers.  Earlier iterations used
`clutch--console-name` as the guard, which missed plain `.sql` files
that connect via `clutch-connect` without a console name.

## Invalidation Points

Three code paths invalidate derived buffers:

- `clutch--disconnect-on-kill` — owner buffer killed
- `clutch-disconnect` — explicit interactive disconnect
- `clutch-connect` — reconnect replaces old connection

## Collateral Fix: `clutch-db-live-p` Default Method

Added a default `nil` body to `clutch-db-live-p` so that non-connection
objects (test stubs using symbols) do not signal
`cl-no-applicable-method`.

## Lessons

- Connection lifecycle belongs to the buffer that established the
  connection, not to every buffer that references it.
- "Last holder disconnects" sounds correct but creates connection
  splitting on reconnect.
- Guard conditions must cover all owner types (console, plain SQL file,
  REPL), not just the most common one.
