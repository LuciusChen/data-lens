# 072 — Result Refresh Keeps Header Ownership

## Why this changed

Result buffers already use the header line as the rendered table header.  During
`g` re-run and other SQL-backed refresh paths, the buffer briefly showed the
query-console connection header instead, then switched back to the table header
after the result finished loading.

That flicker was not a rendering delay problem.  It was an ownership problem:
`clutch--update-mode-line` updated both the mode line and the header line even
when the current buffer was a result buffer.

## Design decisions

### 1. Keep connection headers scoped to consoles and REPLs

The query console and REPL own the connection-status header line.  Result and
record views do not.

The fix therefore belongs in `clutch--update-mode-line`, not in the result
renderer.  Busy-state code should not overwrite a header line that belongs to a
different workflow.

### 2. Reuse the elapsed-time slot for refresh activity

Result buffers already own a footer-mode-line path for row counts, ordering,
filters, timing, and cursor position.  That is the right place for a refresh
spinner.

This keeps one stable visual model:

- header line: rendered table header
- footer/mode line: transient execution state and result status

The spinner should not be appended as a new footer segment.  It should occupy
the existing elapsed-time slot while execution is in progress, then give that
slot back to the final elapsed time once the query completes.

### 3. Do not add a second loading banner

The existing spinner model was already enough.  Adding a separate loading
banner or temporary header copy would have introduced more duplicated status UI
instead of clarifying ownership.

## Consequences

- result-buffer SQL refreshes keep the column header visible
- busy state is still explicit via the footer spinner
- query consoles and REPLs continue to show the connection header they own
