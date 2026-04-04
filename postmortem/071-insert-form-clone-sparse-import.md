# 071 — Insert Form Clone, Sparse Layout, and Delimited Import

## Why this changed

The existing insert buffer had good field-level editing, but it still made the
common workflows too slow:

- inserting a row similar to the current one still started from an empty form
- wide tables opened with every column expanded, even when most fields were
  generated or defaulted
- pasting spreadsheet-style data still required manual field-by-field transfer

These are workflow problems, not rendering problems.  The right fix is to make
the insert form faster to populate, not to replace it with an unrelated buffer
model.

## Design decisions

### 1. Keep the insert buffer as a standalone form

This does **not** switch insert/record workflows to Emacs indirect buffers.
The insert form owns validation state, field markers, hidden-field values, and
staged SQL semantics.  Sharing underlying text with another buffer would not
help with those concerns.

### 2. Add explicit row cloning

`I` in result/record buffers clones the current row into a prefilled insert
form, but intentionally leaves primary-key fields blank.

This is separate from plain `i` on purpose:

- `i` stays the blank "new row" entry point
- `I` is the fast duplication workflow
- cloned forms avoid copying primary-key values into a new INSERT

Keeping both commands explicit avoids making blank inserts and cloned inserts
fight over one key or one prefix convention.

### 3. Make sparse layout the default

The insert buffer now opens in a sparse layout:

- required columns
- columns without defaults
- columns that already have a value

Generated/defaulted columns are still available via `C-c C-a`.

The important part is that toggling layout never drops edits.  The buffer now
keeps a canonical all-fields state internally, and rendering is just a view of
that state.

### 4. Import by header when possible, otherwise by visible fields

`C-c C-y` supports two mapping modes:

- if the first imported row is a header, map by column name
- otherwise map positionally using the fields currently visible in the form

This makes sparse layout and import cooperate instead of competing:

- sparse mode is the fast path for "just fill the important columns"
- all-column mode is available when positional import needs the full table

### 5. Single-row import and multi-row import should behave differently

Single-row import prefills the current form.

Multi-row import stages pending inserts immediately.

Trying to force both through the exact same interaction would make one of them
awkward:

- prefilling is the right behavior when the user still wants to inspect/edit one row
- immediate staging is the right behavior when the user is pasting a batch

## Consequences

- insert buffers are faster on wide tables
- duplication from result/record buffers becomes a first-class workflow
- spreadsheet-style paste becomes practical without adding a second bulk-insert UI
- insert state is now more explicit internally, which also makes future field
  filtering/layout changes safer
