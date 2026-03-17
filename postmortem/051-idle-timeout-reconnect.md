# 051 — Idle-Timeout Reconnect (`clutch-jdbc-agent` v0.1.9)

## Background

Users reported two symptoms when a JDBC connection was left idle long enough
for a firewall or the Oracle server's `IDLE_TIME` profile to silently drop it:

1. The next SQL execution would freeze for the full RPC timeout (30 s by default),
   then surface `"timeout waiting for response to request N"`.
2. Executing again would produce `"Closed Connection"` (from the JDBC driver) or,
   after the fix, `"Unknown connection id: 1"`.

## Root Cause — Two Separate Bugs

### Bug A: No liveness check before execute (agent)

`Dispatcher.execute()` called `conn.createStatement()` directly on a dead JDBC
`Connection`.  The Oracle JDBC driver blocks on network I/O waiting for a server
that no longer has that session, causing the 30-second hang.

**Fix:** call `conn.isValid(3)` before touching the statement.  `isValid` is
specified by JDBC 4.0 to return `false` (or throw and return false) within the
given timeout when the connection is not live.  The agent immediately returns:

```
{"ok":false,"error":"Connection lost: the server closed the connection
 (idle timeout). Please reconnect."}
```

This turns a 30-second hang into a ≤ 3-second fast failure.  Note that the
threading fix in v0.1.8 (postmortem 005) already handles the lock-wait deadlock
independently; `isValid` is a complementary belt-and-suspenders check that gives
a cleaner, faster error message for the dead-connection case specifically.

### Bug B: Race between `delete-process` and `process-live-p` (Elisp)

The Elisp timeout handler did:

```elisp
(delete-process clutch-jdbc--agent-process)
(setq clutch-jdbc--agent-process nil ...)
(signal 'clutch-db-error ...)
```

`delete-process` sends `SIGTERM`.  The JVM enters its shutdown sequence but does
not die instantly.  During the shutdown window — sometimes several hundred
milliseconds — `(process-live-p old-process)` still returns `t`.

`clutch-db-live-p` for JDBC connections checked only:

```elisp
(process-live-p (clutch-jdbc-conn-process conn))
```

Because the `conn` struct's `:process` field holds the **old** process object
(set at connect time), and `clutch-jdbc--agent-process` had already been set to
`nil`, the following race occurred:

1. Timeout fires → `clutch-jdbc--agent-process = nil` → error shown to user.
2. User executes again → `clutch--ensure-connection` calls `clutch-db-live-p`.
3. Old process still returning `t` from `process-live-p` → connection considered
   **live** → reconnect skipped.
4. `clutch-jdbc--ensure-agent` sees `clutch-jdbc--agent-process = nil` → starts
   a **fresh agent** with no connections.
5. Execute fires with `conn-id=1` against a fresh agent → `"Unknown connection id: 1"`.

This manifested as: JVM startup delay (~3 s "hang") followed by the unexpected
error, making it look like the first execute was still broken.

**Fix:** `clutch-db-live-p` now checks three conditions:

```elisp
(and (clutch-jdbc-conn-p conn)
     clutch-jdbc--agent-process                         ; agent variable must be set
     (eq (clutch-jdbc-conn-process conn)                ; conn must belong to
         clutch-jdbc--agent-process)                    ;   the current agent
     (process-live-p (clutch-jdbc-conn-process conn)))
```

The `eq` identity check closes the window: any conn whose agent has been
replaced (variable set to nil or to a new process) is immediately considered
dead, triggering `clutch--try-reconnect` on the next execute.

## Why This Was Not Caught by Tests

The unit tests mock `clutch-jdbc--rpc` entirely and never exercise the
`clutch-jdbc--recv-response` timeout path or real process lifecycle.  Neither
the agent tests nor the Elisp unit tests simulated `delete-process` followed
by a brief survival window.

**Lessons:**

- Any change to the agent-kill / reconnect path requires unit tests for the
  exact state transitions: timeout fires → kill → live-p returns nil → reconnect.
- `process-live-p` is not a synchronous "is the process dead" predicate; it
  reflects kernel state which lags behind `delete-process`.
- Commit only after writing tests that would have caught the bug, not after.

## Tests Added

**`clutch-db-test.el`** (8 new tests):

| Test | What it covers |
|------|----------------|
| `clutch-db-test-jdbc-live-p-matching-live-process` | Happy path: live conn → t |
| `clutch-db-test-jdbc-live-p-dead-process` | Dead process → nil |
| `clutch-db-test-jdbc-live-p-nil-agent-process` | Agent nil → nil (the core fix) |
| `clutch-db-test-jdbc-live-p-mismatched-process` | Old conn vs new agent → nil |
| `clutch-db-test-jdbc-recv-response-returns-matching` | In-queue response → no kill |
| `clutch-db-test-jdbc-recv-response-timeout-kills-agent` | Timeout → kill + reset + error |
| `clutch-db-test-jdbc-recv-response-timeout-agent-already-dead` | Dead agent → no crash |
| `clutch-db-test-jdbc-recv-response-timeout-error-contains-connection-lost` | Error text |

**`DispatcherTest.java`** (1 new test, in agent v0.1.9):

| Test | What it covers |
|------|----------------|
| `executeReturnsErrorWhenConnectionIsInvalid` | `isValid=false` → error response, not hang |
