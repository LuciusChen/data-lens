# 029 — MySQL 5.6 and MariaDB Compatibility Boundary

## Background

The README had drifted into a conservative but increasingly inaccurate summary:

- it documented native MySQL support as `MySQL 5.7+ / 8.x`
- it did not mention MariaDB at all

That wording was no longer grounded in current evidence. The native backend had
already been exercised locally against MySQL 5.6, but the project had not yet
written down what was actually verified versus what remained version-specific.

## Validation Performed

Live regression runs were executed against:

- MySQL 5.6
- MySQL 8.0
- MariaDB 10.11

Using the existing native `mysql` backend, the following paths were verified on
MySQL 5.6 and MariaDB 10.11:

- connect / disconnect
- basic queries and errors
- DML
- schema introspection
- prepared statements
- clutch UI live workflows

In practice, the native backend passed the same live MySQL and clutch test
surfaces on both servers.

## What This Means

The project does not need a separate MariaDB protocol implementation just to
claim basic compatibility.

The current native backend already speaks the MySQL wire protocol and uses
schema SQL that remains compatible across the tested MySQL-family servers.
So the correct statement is:

- MySQL 5.6+ core functionality is supported
- MariaDB is compatible through the same `mysql` backend
- MariaDB is not yet a separately optimized or independently feature-gated
  backend

## Important Differences Observed

### 1. MySQL 5.6 has no native JSON column type

Creating a table with `doc JSON` failed on MySQL 5.6 with a syntax error.

That is a server capability difference, not a clutch compatibility bug. The
native backend still works; the server simply does not provide that type.

### 2. MariaDB `JSON` is not the same thing as MySQL native `JSON`

On MariaDB 10.11, `SHOW CREATE TABLE` revealed that `JSON` is represented as:

- `LONGTEXT`
- with `CHECK (json_valid(...))`

That matters for documentation and expectations. "Supports JSON" in the
MySQL-family world does not mean the same storage or metadata semantics across
products.

### 3. Authentication capability still depends on server family/version

The tested MySQL 5.6 and MariaDB 10.11 instances both used
`mysql_native_password`.

That is enough to confirm baseline compatibility, but it does not prove that
all MariaDB-specific authentication plugins or every TLS/auth combination have
been validated in the native path.

## Decision

Update the README to reflect the tested compatibility boundary more accurately:

1. change native MySQL requirements from `MySQL 5.7+ / 8.x` to `MySQL 5.6+ / 8.x`
2. explicitly mention MariaDB 10.11 as live-validated via the same `mysql`
   backend
3. document the important server-side feature differences instead of implying
   complete interchangeability

## Why Not Add a Separate `mariadb` Backend Now

That would create a second protocol/backend surface without evidence that the
current shared implementation is insufficient.

The validation showed the opposite: for the tested paths, MariaDB already works
well through the existing backend.

The right trade-off is:

- keep one native MySQL-family backend for now
- add targeted conditionals later only if a real MariaDB divergence appears

## Known Limitations

- TLS live tests were not part of this compatibility pass.
- MariaDB compatibility has been verified on 10.11, not across a broad version
  matrix yet.
- Native MySQL-family `query-timeout` remains constrained by server-version
  capabilities; MySQL 5.6 in particular does not provide a clean PostgreSQL-like
  general statement timeout.

## Lessons

- Compatibility documentation should follow live validation, not habit.
- "MySQL-family compatible" is more accurate than assuming either total
  interchangeability or total separation.
- Server feature differences like JSON semantics should be documented as
  product/version boundaries, not mistaken for client bugs.
