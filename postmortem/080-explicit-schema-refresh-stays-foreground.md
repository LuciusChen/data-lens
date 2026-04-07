# 080 — Keep Explicit Schema Refresh in the Foreground

## Background

Moving MySQL/PostgreSQL schema priming off the connect path made the first
buffer usable much sooner, but it also changed the semantics of the explicit
refresh command.

`clutch-refresh-schema` (`C-c C-s`) had been routed through the same lazy
background path as connect-time priming.  That kept the implementation small,
but it weakened the recovery story:

- a failed background refresh left the console in `schema!`
- pressing `C-c C-s` did not force a definitive refresh
- users had no direct "do it now" escape hatch for native metadata failures

That is the wrong UX split.  Passive cache hydration and explicit repair are
different workflows, even if they touch the same cache.

## Decision

Keep connect/reconnect priming lazy, but make explicit manual refresh
foreground.

The rule is now:

- automatic priming after connect/reconnect still prefers the background path
- explicit `C-c C-s` refresh bypasses background dispatch and runs a
  synchronous refresh immediately

## Why Not Keep One Path For Everything

Rejected because the user intent is different.

Background priming is about startup responsiveness.  Explicit refresh is about
recoverability and trust.  When a user asks to refresh metadata right now,
returning "started in background" after a visible `schema!` failure is not a
useful answer.

## Result

The contract is clearer:

- background refresh keeps connect responsive
- `C-c C-s` is the direct recovery path when metadata state is wrong
- the package does not need to guess when a failed async refresh should be
  retried automatically
