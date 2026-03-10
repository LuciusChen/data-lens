# 004 — Edit-Cell Two-Step Flow (Do Not Auto-Commit)

## Background

Cell editing has always been two-step: `C-c '` opens an edit buffer,
`C-c C-c` in that buffer *stages* the edit, then `C-c C-c` in the
result buffer *commits* all staged edits via UPDATE.

## The Attempted Change and Revert

Commit `badf034` ("Make C-c C-c in edit buffer commit directly") changed
`C-c C-c` in the edit buffer to immediately execute the UPDATE — bypassing
staging entirely. The rationale was "one step is simpler than two."

Commit `93029b3` ("Revert to two-step edit flow: stage then commit")
reverted this immediately. The commit message: "too aggressive."

## Why Two Steps Are Correct

**`C-c C-c` always means "commit what is staged", never "execute
immediately".** This is the invariant across the entire system (edit
cells, insert rows, delete rows, all use the same model). An edit buffer
that auto-commits on `C-c C-c` breaks the invariant and surprises users
who expect to be able to stage multiple cells before committing.

**Review before commit.** The staging step lets users see all pending
edits (shown in the pending-changes header) before deciding to commit.
Auto-committing removes this safety window, especially dangerous for
edits to production data.

**Mixed commits.** The two-step model allows staging edits across
multiple cells, then committing them together with deletes or inserts in
a single SQL transaction (INSERT → UPDATE → DELETE order).

## What Was Retained from the Reverted Commit

The table-detection regex improvements introduced in `badf034` were
kept: `clutch-result--table-from-sql` handles backtick-quoted,
double-quoted, and CJK-identifier table names. The `clutch-result--edit-
result-buffer` variable (tracks the originating result buffer from the
edit buffer) was also kept — it enables correct return navigation when
the edit was invoked from a record buffer rather than directly from the
result buffer.

## Invariant to Preserve

The header line of the edit buffer reads:
`Editing row N, column "X"  |  C-c C-c: stage  C-c C-k: cancel`

It says **stage**, not **commit** or **execute**. This wording is
intentional. Do not change it to "commit" or make `C-c C-c` execute
directly, regardless of how "simpler" it might seem.
