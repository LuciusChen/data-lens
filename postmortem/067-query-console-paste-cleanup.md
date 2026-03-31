# 067 — Query Console Paste Cleanup Stays Region-Scoped

## Context

`clutch-query-console` is a long-lived scratch/work buffer, not a disposable
command prompt.  Users paste SQL from many sources into it:

- editor buffers with tabs or CRLF line endings
- email or chat snippets with trailing whitespace
- database tools that emit uneven indentation

That creates a practical paper-cut.  The pasted SQL is often noisier than the
SQL the user actually wants to run, but the surrounding console buffer may also
contain deliberate formatting, comments, and half-finished statements that
should not be rewritten.

## Decision

After `yank`, `yank-pop`, or `clipboard-yank` in a query console, `clutch`
cleans whitespace in the pasted region only.

This behavior is enabled by default and controlled by
`clutch-console-yank-cleanup`.

The cleanup scope is intentionally narrow:

- trim pasted trailing whitespace
- normalize pasted indentation/line endings as `whitespace-cleanup-region` does
- do not touch text outside the just-inserted region

## Why This Scope Is Correct

The query console's main job is to preserve user intent across an evolving SQL
workspace.  A whole-buffer cleanup model violates that.

If cleanup rewrites the entire buffer on each paste, then one accidental paste
can mutate:

- previously reviewed SQL
- commented-out variants kept for comparison
- deliberately aligned formatting in another statement

That is the wrong ownership boundary.  The paste action only owns the newly
inserted text, so cleanup should be limited to that same region.

Region-scoped cleanup also keeps the behavior legible:

- paste something messy
- the pasted snippet becomes runnable immediately
- nothing else moves

This is easy to trust and easy to explain.

## Why Default-On

The common case is that pasted SQL should be executable and visually clean
without an extra command.  Making cleanup opt-in would keep the current friction
in the dominant workflow while helping only users who already know about the
setting.

Default-on is justified here because the behavior is:

- local to query consoles
- local to yank commands
- local to the pasted region
- reversible by normal undo

Those boundaries make the default practical instead of surprising.

## Alternatives Rejected

### Manual cleanup command only

Rejected because it adds a second step to the normal paste-run-edit loop while
leaving the most common annoyance in place.

### Whole-buffer cleanup after paste

Rejected because it rewrites unrelated SQL and turns a local paste action into a
global formatting action.

### Save-time cleanup

Rejected because it delays the effect until persistence and still rewrites text
the user did not just touch.  It also makes the buffer state differ from what
the user saw while editing.

## Consequences

- Query console pastes become cleaner by default.
- Existing SQL already in the console is preserved.
- Users who prefer raw paste behavior can disable it with
  `clutch-console-yank-cleanup`.
