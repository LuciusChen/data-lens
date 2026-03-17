# 049 — Session mistakes: March 2026

## Background

This record documents three distinct mistakes made during the March 2026 session
that implemented Oracle JDBC manual-commit mode, fixed the JSON Unicode viewer
bug, and added Oracle live tests.  Each mistake is described with root cause and
a rule to prevent recurrence.

---

## Mistake 1 — `replace_all` missed occurrences with different indentation

### What happened

`:service` needed to be renamed to `:database` in two test connections inside
`clutch-db-test.el`.  A `replace_all` edit was used with a string that included
the surrounding whitespace.  Two occurrences had 23-space leading indent; the
pattern used 24-space indent.  Those two were silently skipped.  The tests
continued to fail with ORA-12514 until the next session caught the mismatch.

### Root cause

`replace_all` matches literally, including whitespace.  When the replacement
pattern contains indentation-sensitive context, any variation in surrounding
whitespace causes a silent non-match rather than an error.

### Rule

**Before using `replace_all` for a rename, grep for ALL occurrences first and
confirm the count matches what `replace_all` will change.**  If counts differ,
use targeted per-occurrence edits instead of a bulk replace.

---

## Mistake 2 — Commit message described changes that were in a prior commit

### What happened

Commit `d1c7b0f` ("Add Oracle JDBC live tests and generics for commit/rollback")
stated in its bullet points: "Move clutch-db-commit/rollback/manual-commit-p
generics from plan annotations into clutch-db.el".  Those generics were
actually added in the earlier commit `ff91b4b`.  The message was inaccurate.

Additionally, that commit introduced a spurious formatting change to
`clutch-db.el` (moving a `)` to its own line) that had no semantic value and
should not have been staged.

### Root cause

The commit was assembled without running `git diff HEAD` to read every changed
line.  The CLAUDE.md pre-commit checklist requires reading the full diff first,
but that step was skipped.

### Rule

**Always run `git diff HEAD` (or `git diff --cached`) and read every line before
writing the commit message.**  If a file appears in the diff unexpectedly (e.g.
`clutch-db.el` for a test-only commit), investigate and unstage it.

---

## Mistake 3 — Closed a GitHub issue before push and verification were complete

### What happened

A `gh issue close --comment` call was made immediately after writing the fix,
before the branch was pushed and before live testing confirmed the fix worked.
The comment text also had characters dropped due to backtick expansion in the
shell heredoc.  The user had to point this out; the issue was reopened.

### Root cause

Closing an issue is a visible, external action.  It was treated as a local
action (like staging a file) when it is not.  The backtick problem was caused
by not quoting shell special characters in the heredoc body.

### Rule

**Do not close or comment on issues until: (a) the fix is pushed, (b) tests
pass, and (c) the user has reviewed the result.**  For issue comments containing
code or backtick characters, use `gh api --method PATCH` with a HEREDOC, or
escape all shell-special characters explicitly.
