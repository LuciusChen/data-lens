# Row Identity Editing Beyond Primary Keys

## Problem

Result editing and deletion were keyed exclusively by detected primary-key
columns.  Tables without primary keys were rejected even when the backend had a
safe row identity available, such as a not-null unique key, PostgreSQL `ctid`,
SQLite `rowid`, or Oracle `ROWID`.

That made the UI limitation stricter than the database limitation.  It also
kept the decision in the edit layer, so other result workflows such as export
UPDATE, record view, clone-to-insert, and pending-change rendering all inherited
the same primary-key-only assumption.

## Fix

The backend contract now exposes ordered row identity candidates:

- `primary-key` and `unique-key` candidates identify rows by source columns.
- `row-locator` candidates provide hidden SELECT expressions and a backend
  WHERE predicate.

The query path augments simple single-table SELECTs with hidden identity columns
when needed, marks those columns as hidden metadata, and keeps them out of UI
rendering, copy/export, filtering, record view, and insert cloning.  Staged edits
and deletes are now keyed by row identity vectors instead of row-number or
primary-key vectors.

Primary-key source columns remain tracked separately from hidden locator columns
so existing behaviors that avoid copying/updating primary keys still target the
visible source columns, not the hidden identity payload.

## Backend Policy

Backends return candidates in stability order:

- primary key
- not-null unique indexes
- backend row locator when available

PostgreSQL uses `ctid` only for ordinary heap tables.  SQLite uses `rowid` only
for tables that are not declared `WITHOUT ROWID`.  JDBC exposes Oracle `ROWID`
through the generic row-locator contract.  MySQL has no stable physical row
locator in this implementation, so it stops at primary and not-null unique keys.

## Guardrails

Hidden identity injection is limited to simple single-table SELECTs.  The code
does not inject into joins, DISTINCT/GROUP/HAVING queries, set operations, or
other derived results where a table-local row identity would be ambiguous.

UPDATE and DELETE commits verify that the backend-reported affected row count is
exactly one when the backend provides that count.  This catches stale or unsafe
locator matches without requiring every backend to report row counts.
