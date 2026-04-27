#+title: Prefixed MySQL Protocol Layer
#+date: 2026-04-27

* Context

The package split originally moved MySQL and PostgreSQL protocol code into
external packages.  PostgreSQL has a mature upstream package in =pg-el=, so the
=clutch= PostgreSQL adapter can depend on =pg= and send upstream patches when it
needs protocol-level state.

The MySQL protocol layer is different.  It is young, mostly used by =clutch=,
and does not yet have a long compatibility and maintenance history across new
MySQL-family releases.

* Decision

Keep the MySQL protocol implementation inside =clutch= for now, but maintain it
with standalone-library boundaries:

- =clutch-mysql.el= owns the wire protocol and uses =clutch-mysql-*= public
  symbols plus =clutch-mysql--*= private symbols
- =clutch-db-mysql.el= only adapts =clutch-db-*= generics to that protocol API
- =docs/mysql-protocol.md= documents API, compatibility scope, and known gaps
- =test/clutch-mysql-test.el= carries focused protocol-layer tests
- =CHANGELOG.md= records protocol-layer changes alongside user-visible clutch
  changes

* Why

This keeps MELPA packaging simple: users install one =clutch= package for MySQL.
It also avoids asking MELPA to index a new general-purpose MySQL wire protocol
package before the compatibility matrix and maintenance pattern are convincing.

The prefixed boundary still preserves the future split path.  If the MySQL
protocol layer later graduates to a separate package, the main mechanical step
is renaming =clutch-mysql-*= to =mysql-*= and moving the focused docs/tests with
it.

* Tradeoff

MySQL and PostgreSQL are no longer symmetrical at the package-manager layer:
MySQL is bundled while PostgreSQL uses upstream =pg=.  The symmetry that matters
for =clutch= remains at the adapter boundary: both backends implement the same
=clutch-db-*= generic API.
