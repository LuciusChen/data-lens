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

The user-facing UI workflow for this model is specified in
[066](066-single-debug-buffer-workflow.md).

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
2. User enables `clutch-debug-mode` and reproduces the failure.
3. The dedicated `*clutch-debug*` buffer shows:
   - concise summary
   - raw backend/driver error
   - structured diagnostics fields
   - generated SQL when relevant
   - related JDBC stderr tail when relevant

This section describes the diagnostics model, but the user-facing UI workflow is
now superseded by [066](066-single-debug-buffer-workflow.md).

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
