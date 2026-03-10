# 011 — Package Quality Fixes

## Background

A comprehensive code review surfaced 27 potential issues. After reading exact code,
8 were real and actionable; the remaining 19 were false positives or documented
deferred decisions (see plan file for full exclusion rationale).

---

## Fix 1 — `pg-oid-bool` Type-Category: `numeric` → `text`

### Problem
`clutch-db-pg--type-category-alist` mapped `pg-oid-bool` to `'numeric`, causing
boolean columns to be right-aligned as numbers in the result table.

### Decision
Changed to `'text`. Boolean values (`t`, `f`) are strings and should left-align.

---

## Fix 2 — Pending State Cleared on Reconnect

### Problem
`clutch--try-reconnect` silently rebuilt the connection without clearing or warning
about pending inserts/edits/deletes in result buffers. A user could reconnect to a
different host and accidentally commit stale mutations from a prior session.

### Decision
Added `clutch--result-buffers-with-pending` helper that finds all live result buffers
with uncommitted state. On successful reconnect, pending state is cleared and the
user receives a message indicating how many result buffers were affected.

### Why clear rather than warn
Stale pending state after a reconnect is always wrong — the row IDs or values may
refer to data on a different host. Silent discard with a message is safer than
leaving corrupted state in place.

---

## Fix 3 — Shared `clutch-db-format-temporal` Helper

### Problem
`clutch--format-value` and `ob-clutch--format-value` contained identical date/time
plist formatting logic (datetime, date-only, time-only, negative time). Any bug fix
or format change had to be applied in two places.

### Decision
Extracted `clutch-db-format-temporal` to `clutch-db.el` (the pure, no-UI layer).
Both format-value functions now delegate the plist case to this shared helper.

### Preserved difference
`clutch--format-value` returns `(number-to-string val)` for numbers.
`ob-clutch--format-value` returns the raw number (for Org table column alignment).
This intentional difference is retained.

---

## Fix 4 — `clutch-refresh-schema` Interactive Command

### Problem
Schema caches never expired. DDL executed outside clutch (CREATE TABLE, ALTER TABLE,
DROP TABLE) left stale completion candidates with no user-accessible refresh path.

### Decision
Added `clutch-refresh-schema` (autoloaded, `C-c C-s`) that calls
`clutch--refresh-schema-cache` and confirms with a message. Also added to the
`clutch-dispatch` transient under "Schema".

### Key choice: `C-c C-s`
`C-c C-r` was already bound to `clutch-execute-region`. Used `C-c C-s` (s = schema).

---

## Fix 5 — ob-clutch Connection Cleanup on Emacs Exit

### Problem
`ob-clutch--connection-cache` held live DB connections indefinitely. On Emacs exit,
these were dropped without calling `clutch-db-disconnect`, skipping proper teardown.

### Decision
Added `ob-clutch--disconnect-all` registered on `kill-emacs-hook`. Uses
`condition-case` around each disconnect so one failing connection does not prevent
the others from being cleaned up.

---

## Fix 6 — Copyright Header

### Problem
`clutch.el` line 18 read `along with mysql.el.` (copy-paste leftover).

### Decision
Corrected to `along with clutch.`

---

## Fix 7 — Docstrings (Already Present)

Inspection revealed all four predicate functions (`clutch--numeric-type-p`,
`clutch--long-field-type-p`, `clutch--json-like-string-p`, `clutch--xml-like-string-p`)
already had docstrings. No change needed.

---

## Fix 8 — README Org-Babel Integration Section

### Problem
`ob-clutch.el` was fully implemented but undocumented in README.org.

### Decision
Added an "Org-Babel Integration" section covering activation, header arguments
(`:backend`, `:host`, `:port`, `:user`, `:database`, `:pass-entry`), example source
blocks for each backend, and the connection caching + cleanup behavior.

---

## Bonus Fixes (Found During Testing)

### Extra `)` in `pg--parse-value` (`pg.el:736`)
A stray closing paren caused a load-time read error. Fixed during this session.

### `clutch-test-execute-*` tests missing mocks
Two existing tests (`clutch-test-execute-quit-disconnects-and-clears-connection`,
`clutch-test-execute-runs-risky-dml-confirmation`) were silently passing against a
stale `.elc`. Fresh recompilation revealed they lacked mocks for
`clutch--check-pending-changes` and `clutch--clear-error-position-overlay`, which
call `clutch-db-user` on mock symbols. Added the missing `cl-letf` stubs.

### PBKDF2 c=1 test vectors were incorrect
`clutch-test-pbkdf2-sha256-c1-dklen32` and `clutch-test-pbkdf2-sha256-multi-block`
had wrong expected hex strings. Verified correct values with Python
`hashlib.pbkdf2_hmac` and updated the tests. The c=4096 test was always correct
because the inner loop ran enough iterations to produce the right XOR result even
if the c=1 path had an issue.
