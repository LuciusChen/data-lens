# 053 — Org Babel Saved Connections Must Resolve Backend Before Connect

## Background

`ob-clutch` supports two ways to execute a generic `#+begin_src clutch` block:

- inline connection parameters such as `:backend`, `:host`, and `:database`
- a saved `:connection` entry from `clutch-connection-alist`

Issue `#6` reported that a generic `clutch` block still failed unless
`:backend` was written explicitly, even when `:connection` was present.

The first visible error was:

- `Missing :backend for clutch block`

After removing that eager check, a second real failure remained:

- `clutch-db-connect: Unknown backend: nil`

## What happened

There were two separate defects.

First, `org-babel-execute:clutch` required `:backend` too early.  It rejected
generic blocks before `ob-clutch--resolve-connection` had a chance to look up
the saved connection entry.

Second, the saved-connection path in `ob-clutch--resolve-connection` did not
follow clutch's normal default-backend rule.  If a saved connection entry
omitted `:backend`, `ob-clutch` passed `nil` through to `clutch-db-connect`
instead of defaulting to `mysql`.

Together these produced a layered failure mode:

- blocks with `:connection` could fail before connection resolution
- after the first fix, some saved connections still reached connect with a
  `nil` backend

## Decision

Move backend validation into `ob-clutch--resolve-connection`, where the code can
see both sources of truth:

- a saved `:connection` entry
- inline `:backend`

The resolution rules are now:

- if `:connection` is present, load the saved entry and use its backend
- if the saved entry omits `:backend`, default to `mysql`
- if `:connection` is absent, require an inline `:backend`
- only error when both routes fail

## Why the original tests missed it

The earlier coverage was too shallow in two ways.

First, it only asserted that the top-level generic executor no longer raised
`Missing :backend`.  That proved the eager guard had moved, but not that a real
connection could be resolved and used.

Second, it did not cover saved connection entries that omit `:backend`.  That
meant the `nil` backend path was never exercised, even though clutch itself
allows saved connections to inherit the historical mysql default.

In other words, the tests verified *control flow* but not *resolved execution
state*.

## Test changes

Regression coverage now checks the full path:

- generic `clutch` blocks accept `:connection` without inline `:backend`
- saved connection entries without `:backend` default to `mysql`
- `clutch-db-connect` is called with the resolved backend and params
- `clutch-db-query` is actually invoked after connect
- blocks with neither `:connection` nor `:backend` still raise a clear
  `user-error`

## Rationale

`ob-clutch` should behave like the rest of clutch: a saved connection is the
authoritative source of connection metadata, and backend selection should be
resolved once in a single place.

The testing lesson is equally important: when a bug is about parameter
resolution, unit tests should assert the final call arguments to the execution
boundary, not only the absence of an earlier error.
