# 022 — Cursor and Scroll Position Lost on Result Buffer Refresh

## Background

Several user actions — staging a delete with `d`, pinning a column, toggling
sort — refresh the result buffer by calling `clutch--render-result` or
`clutch--refresh-display`. These functions erase the buffer and re-insert all
rows from scratch. The intended behavior is that the cursor returns to the same
cell it was on before the refresh.

Two independent bugs prevented this from working correctly.

## Bug 1: Point in Gutter Loses Row (commit `a2ebebd`)

### Root Cause

Cell values carry a `clutch-row-idx` text property. The cursor position save
logic read this property with `(get-text-property (point) 'clutch-row-idx)`.
However, `clutch-row-idx` is only set on cell value text — not on adjacent
characters: the border `│`, the mark glyph, or the row-number gutter all lack
this property.

When a user pressed `d` with the cursor on the row number (a common position
when navigating by row), `save-ridx` was `nil`. After re-render, point fell back
to `(point-min)` — the top of the buffer.

### Fix

Use `clutch-result--row-idx-at-line` as a fallback. This function scans the
entire current line for any `clutch-row-idx` property, finding the row index
regardless of which character within the line the cursor is on. Applied in both
`clutch--render-result` and `clutch--refresh-display`.

## Bug 2: Window Auto-Scrolls After goto-cell (commit `265e229`)

### Root Cause

After `clutch--goto-cell` moved point to the saved cell, Emacs's standard
scrolling logic scrolled the window to display that point. The default behavior
when `goto-char` moves point outside the visible window is to center or
bottom-align the point. For a row near the bottom of the previously visible
area, Emacs placed it at the bottom of the window — the visual appearance shifted
even though the absolute row index was correct.

### Fix

Save the cursor's line offset from `window-start` before the re-render:
`(win-line = (count-lines (window-start win) (point)))`. After
`clutch--goto-cell`, call `(recenter win-line)` with the saved offset. This
restores the exact visual position of the row within the window.

## Why One Postmortem for Two Commits

The two bugs have the same observable symptom (cursor jumps on refresh) and
were discovered in the same debugging session. Bug 1 (gutter property gap) was
fixed first. After that fix, the row was preserved but the window scroll was
wrong — making the fix appear incomplete. Bug 2 was identified immediately after
and fixed in the following commit. They form a single causal chain.

## Lesson

Text-property-based position save/restore must account for non-property-bearing
characters that are visually part of the same logical row (borders, gutters,
indicators). `get-text-property` at `(point)` is insufficient; a full-line scan
via `clutch-result--row-idx-at-line` is required. Additionally, restoring the
logical position (row index) alone is insufficient when the window can scroll
independently — the visual offset within the window must be saved and restored
separately.
