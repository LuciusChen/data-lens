# 002 — Pending Changes Guard Placement

## Background

After introducing staged mutations, silently discarding them on
re-execution would be a silent data-loss risk. The question was where
to place the guard — at each individual entry point, or at a shared
bottleneck.

clutch has multiple execution entry points:
- `C-c C-c` in clutch-mode (SQL buffer) → `clutch-execute-dwim`
- `g` in result buffer → `clutch-result-rerun`
- `clutch-execute`, `clutch-execute-region`, `clutch-execute-buffer`
- Pagination: `N`/`P` → `clutch--execute-page` (separate path)

## Decision

Two guards, two locations:

1. **`clutch--execute`** — single guard covering all query re-execution
   paths. Checks the result buffer (looked up by name from current
   connection) for any pending edits, deletes, or inserts.

2. **`clutch--execute-page`** — inline guard at the top of the function,
   checking buffer-local pending state directly (already inside the
   result buffer). Message says "change page" not "re-run query" to be
   accurate.

## Why `clutch--execute` as the Central Guard

All user-initiated query executions (from SQL buffer or result buffer)
converge at `clutch--execute`. A guard here catches all current and
future entry points automatically.

The alternative — guarding at each call site — was tried first
(guard only in `clutch-result-rerun`). It immediately proved insufficient:
the SQL buffer's `C-c C-c` path bypassed it entirely. Centralizing
eliminates this class of omission.

## Why `clutch--execute-page` Needs Its Own Guard

Pagination is a structurally separate execution path. It calls
`clutch--update-page-state` directly, which resets pending state, and
never passes through `clutch--execute`. It cannot be consolidated into
`clutch--execute` without restructuring the entire pagination system.

The inline guard in `clutch--execute-page` is intentionally direct
(checking buffer-locals, not going through `clutch--check-pending-changes`)
because the context is different: we are already inside the result buffer,
and the user message should reflect the actual action ("change page").

## Why Not Guard in `clutch--update-page-state`

`clutch--update-page-state` is a pure state-update function. Adding a
`yes-or-no-p` prompt there would violate single responsibility and make
it impossible to call it without side-effecting the minibuffer (e.g.,
from tests or programmatic callers).

## Double-Prompt Avoidance

`clutch-result-commit` clears all pending state before calling
`clutch--execute` to refresh the result. By the time `clutch--execute`
runs, pending state is nil, so the guard is a no-op. No double prompt.
