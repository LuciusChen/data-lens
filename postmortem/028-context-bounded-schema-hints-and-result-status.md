# Context-Bounded Schema Hints and Unified Result Status

## Background

Two small but persistent UX problems remained after the earlier completion and
result-buffer fixes:

- moving around a console with a live connection could still trigger noticeable
  synchronous schema work through eldoc or identifier completion
- the result buffer already showed query state, but it was split between the
  tab-line footer and a separate in-buffer "pending changes" banner

Neither issue was a correctness bug. Both were about reducing friction in
normal interactive use.

## Decision

1. Keep synchronous column lookup bounded to the current statement and only
   when the visible table set is small.
2. Move pending-change summary into the existing tab-line status model instead
   of maintaining a second banner inside the result body.

## Why Bound Schema Hints

`clutch-completion-at-point` and `clutch--eldoc-schema-string` are invoked on
high-frequency editor paths. Even after the earlier caching work, they could
still synchronously call `clutch--ensure-columns` for several tables.

That is acceptable only when the context is narrow enough that the work stays
imperceptible. The practical rule chosen here is:

- current statement only
- no fallback to whole-buffer table guesses for eldoc
- no synchronous column loading for very short prefixes
- no synchronous column loading when too many tables are involved

This keeps the common case fast without removing the feature entirely.

## Why Not Async Completion/Eldoc

Asynchronous enrichment would be more ambitious, but it would also add state
coordination, redraw complexity, and more interaction with the existing busy /
re-entrancy guards.

The current issue was not "schema hints are impossible synchronously"; it was
"schema hints are still too eager in some interactive paths". Tightening the
scope is the smaller and more reliable fix.

## Why Move Pending State into the Footer

The result buffer already had a fixed status surface in the tab-line:

- rows
- page
- column page
- elapsed time
- filters
- aggregate summary

Pending edits/deletes/inserts were shown separately as an inserted line at the
top of the buffer body. That split the current-state model in two places and
made the buffer feel less coherent.

Moving pending state into the same footer model has two advantages:

- the summary remains visible while navigating the table
- "what state is this result in?" can be answered from one place

The same pass also adds active sort state there, since ORDER BY is part of the
same operational context as filters and pagination.

## Alternatives Considered

### Keep the pending banner and also add footer summary

Rejected. That would improve discoverability at the cost of duplicate state
presentation, which is exactly the inconsistency the change was trying to
reduce.

### Use the mode-line instead of the tab-line

Rejected. The mode-line already carries connection/execution state and is
shared with the rest of Emacs. Result-specific page/filter/sort/pending status
belongs closer to the result view itself.

### Keep eldoc using whole-buffer table guesses

Rejected. That was the remaining source of "cursor move causes work unrelated
to the current statement". Completion can still use broader fallbacks for table
names; eldoc should stay narrower.

## Known Limitations

- Completion still returns all table names quickly even when column loading is
  skipped; this is intentional and may still be noisy on extremely large
  schemas.
- The tab-line status bar is richer now, but it still does not represent every
  possible connection state (for example, schema refresh staleness remains a
  separate future improvement).
