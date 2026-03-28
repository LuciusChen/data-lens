# 065 — Error Diagnostics Model and Troubleshooting Workflow

## Context

`clutch` currently has one strong capability and one clear gap:

- It already humanizes database errors well at the user-facing layer via
  `clutch--humanize-db-error`.
- It does not yet have a first-class diagnostics model for troubleshooting.

Today, most failures eventually collapse into a single message string:

- native backends signal `clutch-db-error` with a string
- JDBC agent responses carry only `{"ok":false,"error":"..."}`
- Elisp may further humanize, trim, or wrap that string before surfacing it

This keeps the default UX compact, but it also flattens the information needed
to debug real failures:

- connection/auth/TLS/driver/classpath failures
- lazy-connect drivers that fail on first `execute`
- metadata/object-browser generated SQL
- JDBC-side request timeouts vs true session death
- internal generated SQL that the user did not type directly

The current JDBC stderr buffer (`*clutch-jdbc-agent-stderr*`) is useful but too
implicit to serve as the primary troubleshooting workflow.

## Problem

The project currently mixes three distinct concerns into one string channel:

1. user-facing summary
2. machine-meaningful diagnostics
3. implementation logs

That makes the system awkward in both directions:

- If we optimize for the user message, diagnostics are lost.
- If we stuff more detail into the message, the default UX gets noisy and brittle.

The ClickHouse and JDBC cases exposed the real missing pieces:

- users need a stable place to inspect the last detailed failure
- users need a stable place to inspect generated/internal SQL
- driver/runtime details need structure, not more prose concatenation

## Decision

Adopt a three-layer error model.

### 1. User summary

The default surfaced error remains short and actionable.

- minibuffer / `user-error`
- inline error banner
- REPL/query-console visible message

This layer should continue to use `clutch--humanize-db-error` and related UI
cleanup rules.

### 2. Structured diagnostics

Introduce a separate diagnostics payload for request-level failures.

This payload is not the default user message.  It is a structured record used by
the troubleshooting UI and by tests.

The model should cover at least:

- error category
- operation name
- request id
- connection id when available
- backend / driver identity
- exception class
- SQLState when available
- vendor error code when available
- cause chain
- whether the failure happened during connect / execute / fetch / cancel /
  metadata
- raw database error text before UI humanization
- generated SQL when the failing operation was not user-authored SQL

Protocol shape is intentionally deferred here.  The important constraint is
semantic separation: summary is for humans by default; diagnostics are for
inspection.

### 3. Runtime logs

Keep runtime logs separate from request diagnostics.

- JDBC agent stderr remains the right place for lifecycle/runtime logging:
  startup, driver loading, process exit, overload, unexpected boundary
  exceptions.
- It should not be the only place where a user can retrieve request-level debug
  information.

## Generated SQL Visibility

`clutch` must make internal/generated SQL inspectable.

This includes SQL produced by:

- object browse / jump / describe flows
- metadata lookups
- schema refreshes
- dialect-specific rewritten statements where relevant

This is a separate requirement from richer error text.  A user must be able to
see what `clutch` actually sent, even when the query was not typed by hand.

The design target is similar to `psql -E` / `ECHO_HIDDEN`: generated SQL should
be inspectable without requiring ad-hoc instrumentation.

## Troubleshooting Workflow

Add one explicit troubleshooting path instead of relying on hidden buffers and
tribal knowledge.

The intended workflow is:

1. User sees a short failure message.
2. User invokes `clutch-show-error-details`.
3. The details view shows:
   - concise summary
   - raw backend/driver error
   - structured diagnostics fields
   - generated SQL when relevant
   - related JDBC stderr tail when relevant

This should become the documented workflow for bug reports and self-diagnosis.

## Error Categories

Diagnostics should classify failures into stable buckets.

Initial categories:

- `connect`
- `auth`
- `tls`
- `driver-load`
- `agent-startup`
- `connection-lost`
- `timeout`
- `query`
- `fetch`
- `cancel`
- `metadata`
- `protocol`
- `internal`

The categories do not need perfect vendor-specific precision on day one.  They
do need enough stability that the UI and docs can guide users to the right next
step.

## Redaction Rules

Diagnostics must be safe to show and safe to copy into issue reports.

Never expose:

- passwords
- pass-entry resolved secrets
- auth tokens
- cookie values
- Authorization headers
- secret-bearing JDBC URL query parameters

Allowed:

- host / port / database name when already user-visible
- property names
- redacted URL forms
- driver class / backend identity
- SQLState / vendor code / exception class

Redaction is part of the contract, not a later polish pass.

## Boundaries

### Elisp

- Humanization stays in the UI layer.
- The details view belongs in `clutch`, not in backend libraries.
- The generic backend interface should expose diagnostics without forcing every
  backend to invent a separate UI.

### JDBC agent

- Request-level diagnostics should be captured at the process boundary, not via
  scattered try/catch blocks inside handler logic.
- The protocol should not regress to stack traces in the normal `error` field.
- Unexpected process/runtime failures should still be logged to stderr.

## Non-goals

This design does not aim to:

- expose full Java stack traces in the minibuffer
- turn the agent into a general logging framework
- add multi-level configurable logging before the basic diagnostics path exists
- rewrite all existing native backend errors into a new taxonomy in one step

## Phased Plan

### Phase 1 — Freeze the model

- add this design note
- align protocol/docs terminology around summary vs diagnostics vs logs
- define the initial diagnostics schema and redaction policy

### Phase 2 — JDBC protocol foundation

- extend JDBC request failures to carry structured diagnostics in addition to
  the short error summary
- cover both `connect` and lazy-failure `execute` / `fetch` paths
- keep stderr as correlated runtime logging, not the sole debug channel

### Phase 3 — Troubleshooting UI in clutch

- add a dedicated `clutch-show-error-details` entry point
- surface generated/internal SQL in the same workflow
- make JDBC stderr access explicit from the UI instead of buffer-name knowledge

This phase is now implemented for JDBC diagnostics: `clutch-show-error-details`
is a strict current-buffer troubleshooting entry point.  Connected buffers show
details for their current connection; unconnected buffers show the most recent
structured JDBC failure captured for that buffer.  Early startup/build
`user-error`s that never produced diagnostics are still reported only in the
original command failure path.  The same view shows the structured payload, agent
stderr tail, and generated/internal SQL when the failing path used hidden SQL.
The details buffer is also the copy surface: `w` copies the raw backend
message, and `W` copies the full rendered report.

The next step is also now implemented: `clutch-debug-mode` is the single opt-in
deep-debug switch.  It does not add a second troubleshooting UI.  Instead, it
extends the same `clutch-show-error-details` workflow with two extra layers:

- a bounded redacted recent-event trace for the current buffer / connection
  context
- an opt-in JDBC backend `debug` payload for failures

This keeps the default UX short while giving maintainers and users a reproducible
"turn it on, reproduce, inspect the same buffer" workflow.

### Phase 4 — Documentation

- add a Troubleshooting section to `README.org`
- document what to inspect for connect failures, lazy query failures, interrupt
  failures, and generated-SQL mismatches
- document what gets redacted and what users can safely share

### Phase 5 — Native backend follow-up

- adopt the same details workflow for native PostgreSQL / MySQL / SQLite where
  feasible
- do not block the JDBC redesign on full native parity

## Testing Discipline

Tests for this work must lock contracts, not prose.

Good tests:

- structured diagnostics preserve category / SQLState / vendor code / cause
- redaction removes secrets while keeping useful identifiers
- generated SQL is captured for hidden/internal query flows
- details view can retrieve the last diagnostics payload after a failure

Bad tests:

- asserting one exact vendor error sentence
- asserting a whole formatted details buffer string when only one field matters
- tests that stay green if the implementation returns a hard-coded string

## Why this is the right layer

This design keeps the current UX strengths:

- short default errors
- friendly hints
- backend-specific recovery behavior

while fixing the real missing capability:

- reliable, inspectable diagnostics for users and maintainers

The alternative — continuing to append more debugging prose to `error` strings —
would make both UX and debugging worse over time.
