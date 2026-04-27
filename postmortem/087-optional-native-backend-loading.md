#+title: Optional Native Backend Loading
#+date: 2026-04-09

Update 2026-04-27: this remains true for PostgreSQL's upstream =pg= package.
MySQL is now bundled as the prefixed =clutch-mysql.el= protocol layer; see
=postmortem/089-prefixed-mysql-protocol-layer.md=.

* Context

After splitting =mysql= and =pg= into external protocol packages, =clutch=
still kept them as hard =Package-Requires= dependencies and still treated a
missing =:backend= as implicit =mysql=.

That created the wrong package boundary:

- MySQL-only users were forced to install =pg=
- PostgreSQL-only users were forced to install =mysql=
- connection entries without =:backend= were still accepted, so the runtime
  silently guessed instead of making the dependency explicit

The runtime already loaded backend adapters at connect time.  The remaining
problem was the install-time contract and the implicit mysql default.

* Decision

Keep =clutch= as one package, but make native backend packages optional at the
package-manager layer.

- =clutch= no longer declares =mysql= and =pg= as top-level package dependencies
- every connection entry must specify =:backend=
- =clutch-db-connect= keeps loading backend adapters on demand
- JDBC driver backends also re-check their registration on demand before
  reporting "unknown backend"

* Why We Did Not Keep the Implicit MySQL Default

The mysql default only looked convenient while mysql was a hard dependency.
Once native backends became optional, the default became misleading.

- a missing =:backend= is configuration ambiguity, not a good defaulting case
- guessing mysql would hide the real problem until connect time
- explicit =:backend= keeps the package boundary, runtime behavior, and README
  all aligned

Fail fast is the cleaner model here.

* Why We Did Not Split Into clutch-mysql / clutch-pg

That would have solved the package-manager problem too, but at a much higher
cost:

- more MELPA recipes
- more versioning and release coordination
- more glue around one UI package that already has a clean runtime dispatcher

The existing connect-time backend dispatch was already the right abstraction.
The simplest fix was to remove the hard install-time dependency, not add more
packages.

* Tradeoff

Users installing from MELPA now need to install the native backend package they
actually use.

That is a small cost, but it matches reality:

- =clutch= is the UI and workflow package
- =mysql= and =pg= are protocol packages
- JDBC is activated when a JDBC backend is chosen

That boundary is clearer than pretending all native backends are always present.
