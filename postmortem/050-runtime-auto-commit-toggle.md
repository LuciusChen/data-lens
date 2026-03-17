# 050 — Runtime Auto-Commit Toggle (`clutch-toggle-auto-commit`)

## Background

After implementing manual-commit mode as the Oracle default (postmortem 048), users
had a gap: once connected, there was no way to switch between auto-commit and
manual-commit without disconnecting. A DataGrip user expects to be able to toggle
this live, for example to run a one-off destructive script in auto-commit mode on an
Oracle session that defaults to manual.

## Decision

Add `clutch-toggle-auto-commit` (`C-c C-a`) that calls a new `set-auto-commit` agent
op at runtime.  The command:

1. Checks whether the connection is currently in manual-commit mode.
2. If switching manual→auto and the connection is dirty, prompts for confirmation
   (the JDBC driver will commit the pending transaction implicitly).
3. Calls `clutch-db-set-auto-commit` which fires the RPC and updates `:manual-commit`
   in the stored conn params.
4. Clears the tx-dirty flag if switching to auto-commit.
5. Updates the header-line via `clutch--update-mode-line`.

The generic `clutch-db-set-auto-commit` has a `user-error` default so non-JDBC
backends give a clear message rather than a mysterious "No applicable method" error.

## Why `:manual-commit` Is Updated in Params

`clutch-db-manual-commit-p` derives its answer from `clutch-jdbc--manual-commit-mode`,
which reads `:manual-commit` from the stored params plist. After a runtime toggle,
the live JDBC connection's auto-commit state has changed, but the stored params haven't.
Updating `:manual-commit` in place ensures all subsequent reads (header-line, guard
in `clutch-commit`/`clutch-rollback`, reconnect) reflect the new mode.

An alternative was to query `Connection.getAutoCommit()` via a new agent op on each
call to `clutch-db-manual-commit-p`. This was rejected: it adds a synchronous round-trip
to every header-line redraw and schema-refresh check. Storing the state locally is
faster and already consistent with how connect sets the initial value.

## Implicit Commit on Manual→Auto Transition

The JDBC specification (java.sql.Connection §14.1) states: "If the `setAutoCommit`
method is called during a transaction and the auto-commit mode is changed, the
transaction is committed." This means switching manual→auto has a side-effect.

The UI handles this by:
- Prompting the user when the tx-dirty flag is set.
- Clearing the tx-dirty flag after the RPC returns (the implicit commit has happened).

The header-line then shows `Tx: Manual` → (nothing) after the transition, which
accurately reflects the new state.

## The `clutch-jdbc--json-false` Encoding Issue

The first agent implementation used `(boolean) req.params.getOrDefault(...)` which
fails when the Elisp JSON false sentinel arrives as a string. See agent postmortem
004 for the full explanation. The fix is to use `Boolean.TRUE.equals(value)` which
treats any non-`Boolean.TRUE` object as false.

**Lesson carried forward:** any new boolean parameter in the agent protocol must use
`Boolean.TRUE.equals(value)`, not a cast or `Boolean.parseBoolean`.

## Keybinding

`C-c C-a` was chosen for "auto-commit" toggle.  It sits alongside:
- `C-c C-m` — commit
- `C-c C-u` — rollback

This keeps the three transaction-related bindings under `C-c C-*`.

## Testing

- 3 Elisp unit tests in `clutch-test.el` covering manual→auto, auto→manual, and
  the dirty-abort path.
- 2 JDBC unit tests in `clutch-db-test.el` verifying RPC firing and params update.
- 1 Oracle live test confirming the round-trip works against a real Oracle instance.
