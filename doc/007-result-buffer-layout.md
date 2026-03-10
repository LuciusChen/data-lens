# 007 — Result Buffer Layout: Header, Footer, and Scroll Zones

## Final Layout

Three fixed zones above the scrollable data body:

- **`tab-line-format`** (topmost, always visible): status footer —
  Σ rows, current page / total, ⏱ elapsed time, column-page indicator
- **`header-line-format`** (below tab-line, always visible): column
  names aligned to data cells, no `│` borders
- **Buffer body**: self-contained data table with Unicode box-drawing
  borders (`┌┬┐ │ └┴┘`) starting at the top of the scrollable area

Both `tab-line` and `header-line` are sticky — they remain visible
while the data body scrolls. This ensures column names and pagination
status are always visible regardless of scroll position.

## Iteration History

Multiple layout arrangements were tried before the current design:

1. **Early**: status in mode-line, header in buffer body as first line.
   Problem: mode-line is too small and shared with Emacs state; header
   scrolled away with data.

2. **`540a451`**: Status moved to `tab-line`; elapsed shown as ms when
   < 1s. Header still in buffer body.

3. **`1945e58`**: Top border separator (`┌──┬──┐`) moved to
   `tab-line-format`; header row moved to `header-line-format`. This
   gave a proper visual top border above the header while keeping the
   active-column highlight working via `header-line-format` text
   properties.

4. **`0821571`** (current): Borderless header (no `│` separators in the
   `header-line-format`). Footer in `tab-line`. Data table fully
   self-contained in the body with all borders. Cleaner visual
   separation between the fixed zones and the scrollable content.

## Row Numbers: `line-prefix` Property

Row numbers are rendered via the `line-prefix` text property, not as
buffer text (`741ad6f`). This makes them purely visual: they cannot be
selected, copied, or interfere with buffer content. Killed text from
the buffer does not include row numbers.

## vscroll Reset Hook: Tried and Removed

A `window-scroll-functions` hook (`data-lens--reset-vscroll`) was added
to snap to whole lines and prevent `pixel-scroll-precision-mode` from
leaving partial rows visible below the fixed header (`f16392a`).

It was reverted entirely (`1de33b7`) because it **broke pixel
scrolling**. The hook and `pixel-scroll-precision-mode` cannot coexist:
the hook's forced scroll-to-whole-line reset fired on every pixel scroll
event, making the buffer feel laggy and jumpy.

**Do not re-add a `window-scroll-functions` hook for vscroll reset.**
The partial-row-under-header artifact is an accepted trade-off for
users of `pixel-scroll-precision-mode`. The alternative (requiring
non-pixel scrolling) is a worse trade-off.

## Scroll Position Preservation Across Re-render

After any re-render (`clutch--refresh-display`), the buffer is erased
and rebuilt from scratch. The cursor row is restored via
`clutch--goto-cell`, but Emacs's default behavior auto-scrolls to put
the restored row at the bottom of the window.

Fix (`265e229`): before rendering, save the cursor's line offset from
`window-start` as `win-line = count-lines(window-start, point)`. After
`clutch--goto-cell`, call `(recenter win-line)` to restore the visual
position. This preserves both the cursor row and the window's scroll
position.

Additionally, `clutch-row-idx` text property is only set on cell value
text, not on the gutter (`│`, mark char, row number). Before this was
fixed (`a2ebebd`), pressing `d` or any command that refreshed the
display while the cursor was on the gutter would lose the row index
and jump to `point-min`. Fix: use `clutch-result--row-idx-at-line` as
a fallback in both `clutch--render-result` and `clutch--refresh-display`
to scan the full line for a `clutch-row-idx` property.

## Column Paging

Wide result sets that exceed the window width are split into column
pages. `◂` and `▸` edge indicators (using `clutch-col-page-face`) show
when there are columns to the left or right. `[` and `]` navigate pages.

Pinned columns (`C-c p`) always appear on every page. The column page
computation (`clutch--compute-column-pages`) accounts for pinned columns
and the fixed gutter width (border + mark char + row number + padding).
