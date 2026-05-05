# 079 — Incremental Result Redraw Strategy

## Context

`clutch` result buffers currently rebuild the table body with
`clutch--render-result` and `clutch--refresh-display`, both of which erase the
buffer and insert every row again.

That model is correct but expensive in three ways:

- small UI actions still pay full-body redraw cost
- point / window-start preservation logic becomes central and fragile
- async metadata enrichment has no cheap way to refresh only the part that changed

This is the same general display class as `ghostel`: keep one real Emacs
buffer with searchable text and data-bearing text properties, but make redraw
work narrower and more explicit.

Two existing clutch decisions constrain the design:

- `059-single-page-result-layout.md` already chose one searchable buffer over
  virtual column paging
- `022-cursor-scroll-preservation-on-refresh.md` already showed that full
  redraw is a correctness hotspot, not only a performance cost

## Proposed Decision

Keep the current full render path as the authoritative fallback, but add a
small incremental redraw layer in `clutch-ui.el`.

The first slice should support four refresh scopes:

- full body redraw
- one or more specific data rows
- header-line rebuild
- mode-line/footer rebuild

Do not add a native module, hidden off-screen columns, or a second display
model.  The result buffer should remain a normal Emacs buffer whose visible
text matches what search, region operations, and export logic expect.

## Why This Level

This should stay a UI-layer change.

- The data model is already cached in buffer-local state
- Rendering already happens from cached data, not by reparsing displayed text
- The expensive part is redraw scope, not protocol decoding

This should not become a speculative rendering framework.

- No generic diff engine
- No per-cell marker graph
- No timer-driven frame loop
- No row cache until measurement shows row string construction is the bottleneck

The minimal useful change is narrower mutation of the existing buffer.

## Design

### Invariants

These rules should remain true:

- The result buffer is the canonical searchable text surface
- Cell semantics stay on text properties such as `clutch-row-idx`,
  `clutch-col-idx`, and `clutch-full-value`
- Overlays remain ephemeral visuals only, such as row highlight and SQL error
  markers
- Full redraw remains available and is used whenever shape-level assumptions
  change

### New Redraw Helpers

Add narrow helpers in `clutch-ui.el` instead of widening `clutch--refresh-display`:

- `clutch--refresh-header-line`
- `clutch--refresh-footer-line`
- `clutch--render-row-line`
- `clutch--replace-row-at-index`
- `clutch--reindex-row-start-positions-from`
- `clutch--refresh-rows`

The important constraint is that row-local redraw should reuse the existing row
renderer, not invent a second rendering path.  `clutch--render-row`,
`clutch--render-cell`, and the current render-state helpers should stay the one
source of truth for row text and properties.

### Row Replacement Model

The first implementation should be simple:

1. Render the target row string with current widths and render state.
2. Replace exactly one buffer line.
3. Recompute `clutch--row-start-positions` from the changed row forward.
4. Restore point in logical row/column terms.

Do not switch to marker-per-row storage in the first slice.  A vector of line
starts already exists and is easy to reason about.  Reindexing from the changed
row is simpler than introducing a permanent marker graph.

### Dirty Scopes

Use explicit invalidation scopes, not inferred redraw heuristics.

Full redraw still handles:

- new query results
- page changes
- filter changes
- `ORDER BY` changes
- column width changes
- window width changes
- result shape changes such as different columns or different pending-insert row count

Partial row redraw should handle:

- mark / unmark one row
- stage / unstage delete for one row
- stage / unstage edit for one row
- editing values inside a pending insert ghost row
- commit / discard paths that only clear staged state on already-rendered rows

Header-only redraw should handle:

- active column highlight changes
- sort indicator changes
- metadata-driven header help changes, if added later

Footer-only redraw should handle:

- cursor segment changes
- pending edit/delete/insert counts
- aggregate summary changes
- query timing / spinner state
- connection-status badge changes

### Scheduling

Do not copy `ghostel`'s timer-based redraw loop directly.  `clutch` is not a
streaming terminal.

The initial implementation should stay synchronous per command and per async
callback.  If resize storms or async metadata callbacks later show visible
churn, add a very small coalescing layer such as `clutch--request-refresh`
around the new helpers.

That gives clutch the useful part of the `ghostel` lesson:

- batch bursts when they are real
- do not build a frame scheduler before measurement

## Expected File Ownership

### `clutch-ui.el`

Own the incremental redraw machinery:

- row replacement
- row-start reindexing
- split header/footer/body refresh entry points
- fallback from partial redraw to full redraw when assumptions do not hold

### `clutch.el`

Own command-level invalidation choices:

- commands that currently call `clutch--refresh-display` after row-local state
  changes should switch to targeted row refresh
- navigation-only commands should continue to avoid body redraw
- header highlight logic can keep rebuilding the header line independently

### `clutch-schema.el`

Own async metadata-triggered refresh calls:

- result metadata enrichment should request the narrowest possible refresh
- if metadata only affects info lookups and not visible text, do not redraw the
  body at all

## Rejected Alternatives

### Native module rendering

Rejected.  `clutch` is not bottlenecked by VT parsing or keystroke echo.
Bringing in a native module would raise complexity at the wrong layer.

### Hide off-screen columns instead of rendering one real table

Rejected by the existing single-page layout decision.  Search and motion must
continue to operate on real visible buffer text.

### Whole-buffer diffing after every command

Rejected as unnecessary complexity.  The command layer already knows whether a
change is row-local, header-local, footer-local, or global.

### Marker-per-row anchors in the first slice

Rejected for now.  They add bookkeeping pressure to every render path.  The
existing vector plus incremental reindexing is the simpler starting point.

## Implementation Plan

### Phase 1

Split refresh entry points without changing behavior:

- extract header-only and footer-only refresh helpers
- keep body refresh full
- make current callers state their intended scope

This reduces ambiguity before any partial body redraw is added.

### Phase 2

Add row-local replacement for the easiest staged-state commands:

- mark / unmark
- pending delete toggle
- pending edit clear/apply for one row

This proves whether line replacement plus row-start reindexing is stable.

### Phase 3

Extend row-local redraw to pending insert ghost rows and commit/discard flows.

At this point the common mutation workflows should avoid full-body redraw in
steady state.

### Phase 4

Add measurement before any broader optimization:

- benchmark full redraw vs row-local redraw on wide tables
- measure resize churn
- measure async metadata callback impact

Only after that should clutch consider row-string caching or debounced refresh.

## Test Expectations

Add focused tests for:

- partial row redraw preserves point row/column
- partial row redraw preserves window-start row when possible
- only the targeted row text changes for row-local commands
- full redraw still occurs for width/page/filter/order changes
- header-only and footer-only refresh do not rebuild the table body

The benchmark suite should also gain a UI-focused render benchmark.  Current
benchmarks measure backend query cost, not result-buffer redraw cost.

## Review Focus For Claude

Please review these specific design choices:

1. Is "replace one line, then reindex row starts from that row forward" the
   right first implementation, or is there a stronger reason to introduce row
   markers immediately?
2. Is it correct to keep window-width changes on the full-redraw path in the
   first slice?
3. Which current commands in `clutch.el` are the best initial adopters for
   row-local redraw, and are any apparently row-local commands actually shape
   changes in disguise?
4. Should metadata enrichment trigger any body redraw at all, or should it stay
   cache-only unless the rendered text actually depends on metadata?

The main goal is not maximum cleverness.  The goal is to reduce redraw scope
without weakening the one-buffer, search-friendly result model.
