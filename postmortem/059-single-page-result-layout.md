# 059 — Single-Page Result Layout Replaces Column Paging

## Decision

Result buffers render every visible result column into one buffer body.
Horizontal overflow is handled by Emacs window hscroll, not by swapping
column pages in and out of the buffer.

## Why

Column paging made wide tables impossible to search as normal text.
Columns on non-current pages were not rendered, so `isearch` could not
find them and `TAB` navigation stopped at the last column of the current
page. Those are not incidental bugs; they follow directly from hiding
columns by not inserting them into the buffer at all.

The core result-buffer workflow is text navigation over rendered cells.
That workflow must keep one consistent search and motion model. A result
buffer that sometimes behaves like a real buffer and sometimes like a
paged viewport creates the wrong abstraction boundary.

## Alternatives Rejected

### Keep column paging and patch `isearch`

Rejected because it would require one of two bad options:

1. Re-rendering pages opportunistically during search, which makes search
   state depend on view state.
2. Inserting off-page columns into the buffer and hiding them with display
   tricks, which complicates point motion, region handling, overlays, and
   header alignment for little value.

Both approaches preserve the old abstraction while fighting its natural
behavior instead of simplifying it.

## Consequences

- `[` and `]` become plain horizontal scrolling affordances for wide rows.
- `C` continues to jump to a column, but no longer changes result pages.
- Pin/unpin commands and column-page indicators are removed.
- Header/footer rendering no longer needs left/right page-edge markers.

## Accepted Trade-Off

Very wide tables can require more horizontal scrolling than before. This
is acceptable because the buffer stays searchable, navigable, and
internally coherent.
