# 066 — Converge Troubleshooting to a Single Debug Buffer

## Context

`clutch` currently exposes troubleshooting through three overlapping concepts:

- short inline/minibuffer errors
- `M-x clutch-show-error-details`
- `M-x clutch-debug-mode`, which enriches the same details buffer
- JDBC's raw `*clutch-jdbc-agent-stderr*` buffer

This design was an improvement over string-only diagnostics, but it still feels
foreign relative to established Emacs workflows.

Two concrete problems remain:

1. **The main troubleshooting entry point is ambiguous.**
   Users must learn whether to inspect the inline message, the details buffer,
   or the raw stderr buffer.

2. **The details view mixes two different jobs.**
   It tries to serve both as:
   - a user-facing "what failed" surface
   - a maintainer/debugging transcript

That makes it heavier than a normal error view and weaker than a real log view.

## Why the Current Model Feels Wrong

The existing model does not match the interaction style of strong Emacs-native
tools:

- Flymake separates *problem navigation/listing* from low-level transport
  debugging.
- Eglot provides explicit events/stderr buffers for protocol/runtime debugging.
- Magit uses a process buffer for command/runtime output instead of folding that
  log into each user-facing failure.

`clutch` instead routes user diagnostics, backend diagnostics, and runtime log
snippets into one "details" surface.  That convergence is too shallow:

- still too many entry points for users
- still not a true debug log buffer for maintainers
- still easy for one path (query, connect, object metadata) to miss the
  expected storage hook and leave "no details available"

## Decision

Adopt a stricter two-layer UX:

### 1. Normal operation

Normal users should only encounter:

- short minibuffer / overlay error messages
- no dedicated troubleshooting buffer by default

These messages stay concise and actionable.

### 2. Explicit debug mode

When `clutch-debug-mode` is enabled, `clutch` gets exactly one official
troubleshooting surface:

- a dedicated debug buffer

This buffer becomes the sole user-facing place to inspect:

- structured failure details
- generated/internal SQL
- debug event trace
- JDBC backend debug payload
- JDBC agent stderr tail

No second troubleshooting UI should coexist with it.

## Command Model

The new workflow is:

1. `M-x clutch-debug-mode`
2. reproduce the problem
3. inspect `*clutch-debug*`

That is the full supported debug workflow.

The previous `clutch-show-error-details` command should be deleted, not aliased.
This is intentional:

- no compatibility shim
- no fallback command
- no dual-maintained UI

`*clutch-jdbc-agent-stderr*` remains an implementation detail, not a documented
primary user entry point.

After the migration, the intended command set is:

- normal commands keep surfacing short user-facing errors inline
- `clutch-debug-mode` toggles extra capture
- `*clutch-debug*` becomes the only supported troubleshooting surface

No second "details" command should remain.

## Buffer Semantics

The debug buffer must be scoped, not global-by-accident.

The intended resolution order is strict:

1. current buffer's captured failures/events
2. current live connection's captured failures/events

If neither exists, the command should error clearly.

There should be no fallback to:

- "last error anywhere"
- another buffer's state
- raw stderr alone

## Data Model

The implementation should separate two records:

### Problem record

A user-visible failure snapshot:

- summary
- backend
- raw backend message
- operation
- SQL or generated SQL when relevant
- structured diagnostics payload when available

The intended representation is a plain plist, not a new abstraction-heavy layer.
At minimum it should support:

- `:summary`
- `:backend`
- `:op`
- `:raw-message`
- `:diag`
- `:sql`
- `:generated-sql`
- `:buffer`
- `:connection`
- `:time`

Storage should be strict and explicit:

- one buffer-local current problem record
- one connection-scoped current problem record for live connection contexts

There should be no global "last problem anywhere" registry.

### Debug event record

A time-ordered troubleshooting event:

- timestamp
- buffer / connection scope
- backend
- op / phase
- request id / conn id when available
- SQL preview / generated SQL preview
- redacted context
- elapsed time when relevant
- stderr excerpt or backend debug payload reference when relevant

This record should also stay a plain plist.
At minimum:

- `:time`
- `:buffer`
- `:connection`
- `:backend`
- `:op`
- `:phase`
- `:summary`
- `:sql-preview`
- `:context`
- `:elapsed`
- `:request-id`
- `:conn-id`

The debug buffer may render both record types together, but they must not be
stored as one ad-hoc plist blob.

## Ownership and State

The redesign should reduce mixed responsibilities, not create more glue.

### State that stays

- buffer-local current problem record
- buffer-local recent debug events
- connection-scoped recent debug events
- JDBC connection-scoped diagnostics cache, if needed at the backend boundary

### State that should disappear from the UI layer

- the dedicated "error details buffer fetcher" model
- ad-hoc details-buffer-local copy state
- the idea that a troubleshooting buffer must be regenerated via a thunk tied to
  another source buffer

The debug buffer should instead render from shared current problem/debug
registries directly.

## Source Integration Rules

Every workflow that can fail must write into the same debug pipeline:

- connect
- query execute / fetch / cancel
- object describe / browse / definition
- metadata / schema refresh

Do not create workflow-specific troubleshooting UIs.
Do not let object/metadata errors bypass the shared recording path.

## Non-goals

This design explicitly does not aim to:

- make `clutch-show-error-details` coexist with the new debug buffer
- preserve old command names through aliases
- add more fallback behavior to "help" missing state
- expose raw stack traces in normal user-facing error messages
