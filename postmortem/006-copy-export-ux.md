# 006 — Copy/Export UX Convergence

## Background

The copy/export surface went through extensive iteration before settling
on the current design. At peak proliferation, the result buffer had five
overlapping copy commands with inconsistent region semantics:

- `Y` — copy current cell value
- `C` — copy with format picker (TSV/CSV/INSERT)
- `w` — copy rows as INSERT (C-u for column picker)
- `y` — copy rows as CSV (C-u for column picker)
- `e` — export to file

## Final Design

**`c`** — single entry point for all copy actions, context-driven:
- No region active: copies current cell value
- Regular region: copies selected cells as TSV
- Rectangular region (`C-x SPC`): copies the rectangle as TSV
- `C-u c`: enters visual refine mode (select rows/columns to include)

**`e`** — export only (all rows to file or clipboard, with encoding choice)

**`C`** — freed up for jump-to-column (no longer a copy command)

No format picker on plain `c`. No standalone `w`/`y`/`Y` keybindings.
All copy behaviors are accessible via `c` and the dispatch menu.

## Iteration History

1. **`a6337e3`**: First unification — `C` as single entry point with
   format picker. `C` was chosen to avoid conflicting with existing keys.

2. **`c9cc9dd`**: `c` replaces `C` as copy entry point; `C` freed for
   jump-to-column. Removed standalone `w`, `y`, `Y` keybindings.
   Region semantics: no region = cell, region = rows. Rectangular region
   (`C-x SPC`) added for block selection.

3. **`456f05c`**: Removed minibuffer column fallback for `C-u` copy.
   Previously `C-u c` with no region prompted for columns in the
   minibuffer. Removed as redundant — visual refine mode covers this
   more clearly.

4. **`c3922dd`**: Simplified `c` to pure region-semantic behavior.
   Removed the format-picker from plain `c` (TSV is the default,
   INSERT is via dispatch). `e` becomes the sole export path.

5. **`2a85187`**: Visual refine minor mode for `C-u c` and `C-u A`
   (aggregate). Replaced the prior minibuffer exclusion prompt
   (checkboxes in a `completing-read`) with an in-buffer visual
   overlay mode where users toggle rows/columns by pressing `n`/`p`/
   `TAB` then `RET` to confirm. This was a significant UX improvement
   — the prior checkboxes approach was described as awkward and hard
   to use.

## Record View: Copy Removed Entirely

`c` (copy field) and `C-c '` (edit field) were removed from
`clutch-record-mode` (`4250232`, `ce6d3dc`). The record view is now
read-only navigation only. All editing and copying are done from the
result buffer.

Rationale: the record view is a detail view, not an editing surface.
Having copy/edit in both the result buffer and record view created two
parallel paths to the same actions with inconsistent behavior (record
view's copy had different region semantics). Removing them from the
record view forces one canonical path.

## Principle

The AGENTS.md rule "Converge UX, avoid mode branches" was explicitly
added (`26a8d20`) as a result of this iteration. Before adding a new
copy variant or format, check whether it can be expressed as a context
of the existing `c` command. Do not reintroduce standalone copy
keybindings for specific formats.
