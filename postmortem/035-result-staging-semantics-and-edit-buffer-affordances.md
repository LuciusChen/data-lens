# 035 — Result Staging Semantics and Edit-Buffer Affordances

## Prior Art: Insert Buffer Evolution

The insert buffer evolved through two earlier design passes:

- **Form model** (033): TAB/S-TAB navigate fields, M-TAB completes, C-c . fills
  temporal fields. Staged insert repair via C-c ' on ghost rows. Local
  validation for enum/bool/JSON/numeric/date fields.
- **Live validation and JSON editor** (034): Aligned field labels, active-field
  highlight, inline live validation with idle delay for JSON, RET as
  accept-and-move, C-c ' JSON child editor on demand.

These decisions are now subsumed by the unified staged-mutation model below.

## Background

The result buffer had already grown into a staged-mutation workflow, but several
parts were still speaking different dialects:

- the left marker column used different visual languages for insert vs delete
- the footer described pending work in full English phrases while other footer
  blocks used compact status tokens
- transient labels, mode help, README, and actual command behavior had drifted
- pending edits were already keyed by primary key, but some render paths still
  behaved as if edits were keyed by `(ridx . cidx)`

At the same time, the insert buffer had become a capable form UI, while
single-cell edit still behaved like a plain scratch buffer with no type-aware
help.

These were not separate problems.  They were both symptoms of the same gap:
the staged workflow had a stronger model than the UI surfaces around it.

## Problems Observed

### 1. Staged changes were not rendered consistently

Delete rows showed `D`, while pending inserts still used `+` / `+1`.  The
footer used long phrases such as `1 edit, 1 deletion, 1 insertion`, even though
the rest of the result UI already preferred compact tokens.

That made the staged model feel less deliberate than it actually was.

### 2. Command names had drifted from actual behavior

Examples:

- `Discard pending` only discarded delete/insert, not edit
- `Preview SQL` in result mode mixed three meanings:
  - preview current query
  - preview pending UPDATE
  - preview selected DELETE
- `Commit edits as UPDATE` was stale once staged INSERT / DELETE / UPDATE were
  committed together

This made the workflow harder to learn because similar surfaces implied
different mental models.

### 3. Pending edits were stored correctly but not rendered correctly

Staged edits already used PK-based keys, but the result/record rendering path
still looked up edits as if they were keyed by row index.  That produced the
worst kind of UI bug: the staged state existed, but the visible value did not
change, the cell was not highlighted, and the row did not show an edit marker.

### 4. Edit buffers lagged behind insert buffers

Insert buffers already knew about enum/json/temporal fields, completion, JSON
child editing, and the temporal `C-c .` helper.  Single-cell edit did not.

The wrong fix would have been "copy all insert metadata into edit".  Some
metadata is only safe in insert semantics:

- `default=...` is meaningful when omitting a column from INSERT
- `generated` often means "do not edit", not "show a passive tag"

So edit needed affordances, but not blind symmetry.

## Decision

Treat the result buffer as one coherent staged-change workflow, and treat the
edit buffer as a single-field variant of the insert form where that mapping is
semantically valid.

## Result Buffer: One Vocabulary

The result buffer now uses one compact mutation vocabulary everywhere:

- left marker column:
  - `E` for staged edit
  - `D` for staged delete
  - `I` for staged insert
- footer summary:
  - `E-<n> D-<n> I-<n>`
- footer actions:
  - `commit:C-c C-c`
  - `discard:C-c C-k`

`nf-cod-diff_modified` remains as a dim group icon for the whole pending block,
not as the icon for "edit" specifically.

This keeps the main signal in the counts, while preserving footer structure
consistency with other icon-led blocks.

## Commands: One Mental Model

The staged workflow terminology is now aligned around:

- `Edit / re-edit`
- `Discard pending change at point`
- `Preview execution`
- `Commit pending changes`

In result mode, `Preview execution` now follows one rule:

- if any staged `I/E/D` changes exist, preview the exact commit batch in commit
  order: `INSERT -> UPDATE -> DELETE`
- otherwise preview the effective query

This removed the old hybrid behavior where preview secretly changed meaning
based on region state or on whether only edits happened to exist.

## PK-Keyed State Must Also Be PK-Keyed in Render Paths

The render path now respects the same PK-keyed identity that staging already
used.

That means staged edits now immediately affect:

- displayed cell value
- modified face on the cell
- `E` row marker in the result table
- record-buffer rendering of the same row

The important lesson is simple: once staging identity moved from ridx to PK,
every display lookup had to move with it.  Partial migration created invisible
state.

## Edit Buffer: Affordances, Not Full Form Parity

Single-cell edit now borrows the pieces from insert that map cleanly to UPDATE
semantics:

- enum/bool-like completion via `M-TAB` / `C-M-i`
- JSON child editing via `C-c '`
- temporal `C-c .`
- metadata tags such as `[enum]`, `[json]`, `[datetime]` in the header line
- the same local field validation rules used by insert staging
- the same compact error-token model used by insert live validation

This gives edit buffers the same useful guidance as insert buffers without
pretending they are the same workflow.

Validation now happens before the edit buffer closes.  That matters because a
failed local check should keep the user in the editing context, not throw the
buffer away and force them to re-open it just to fix one value.

Both buffers now follow the same split:

- numeric / enum / bool / temporal validation can update immediately
- JSON validation waits for a short idle pause
- the visible UI shows a short token such as `invalid numeric` instead of
  rendering the full long-form error inline

The long-form error still matters for correctness, but it does not belong in
the primary form layout.

The JSON child editor is now also implemented as a shared sub-editor path
instead of two separate open-buffer flows.  Insert and edit still write back to
different parent contexts, but the JSON editor lifecycle itself is now one
helper, not two near-identical copies.

Once that shared helper existed, the next UX step became straightforward:
editing a JSON cell now enters the JSON sub-editor directly.  The plain
single-field edit buffer still exists as the parent context for staging and for
cancel/fallback behavior, but it is no longer the first stop for the common
JSON-editing path.

### Why not bring `default` and `generated` over too?

Because the semantics are different.

`default=...` in insert means "the database may supply this if the column is
omitted."  In edit mode, the equivalent operation would be a deliberate
`SET col = DEFAULT`, which clutch does not currently expose as a general edit
action.

`generated` is even stronger: it usually wants a read-only or blocked-edit
policy, not a passive hint.

So the edit buffer deliberately includes only metadata that helps the user pick
or shape a value, not metadata that implies a write operation clutch does not
yet model.

## Temporal Helper Rule

`C-c .` is now defined the same way in both insert and edit:

- set the current field to "now"
- if the field already has a value, replace it
- if it is empty, fill it

The concrete output still depends on column type:

- `date` -> current date
- `time` -> current time
- `datetime` -> current date-time

This is a better rule than "fill if empty", because it gives one predictable
mental model across both workflows.

## Follow-up

The next semantic boundary is not enum/json/temporal anymore.  It is
`default/generated` in update workflows.

If clutch wants to expose those in edit mode, it should do so explicitly with
real write semantics, not by copying insert-buffer tags into a different
operation and hoping the user infers the right meaning.
