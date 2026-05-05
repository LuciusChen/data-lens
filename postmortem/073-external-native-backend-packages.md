#+TITLE: Postmortem 073: External Native Backend Packages

* Why this split happened

Clutch started with three responsibilities in one repository:

- protocol implementations (=mysql.el= and =pg.el=)
- the interactive database client (=clutch=)
- the Org-Babel bridge (=ob-clutch=)

That layout worked while everything moved together, but it became a bad fit for
MELPA packaging:

- protocol libraries want their own public namespaces and release cadence
- the interactive client should depend on stable protocol packages instead of
  owning them inline
- Org-Babel integration is optional, and should not be bundled into the core UI
  package

Keeping all three concerns in one package blurred ownership and made MELPA
linting complain for the right reason: they are separate products.

* Why clutch now depends on external packages

The native MySQL and PostgreSQL adapters stay in clutch because they express
clutch's generic backend contract:

- =clutch-db-mysql.el= adapts clutch generics to =mysql=
- =clutch-db-pg.el= adapts clutch generics to upstream =pg=

This keeps the UI and workflow code stable while letting the protocol clients
evolve in their own repositories.  The adapters are the right place to translate
between clutch's generic operations and each protocol package's API.

* Why ob-clutch is a separate optional package

Org-Babel support is useful, but it is not part of clutch's primary interactive
workflow.  Bundling it into the main repo made the package boundary fuzzy and
forced clutch's own tests and docs to carry optional Org-specific surface area.

Moving =ob-clutch= out makes the dependency direction explicit:

- clutch is the core interactive client
- ob-clutch is an optional bridge on top of clutch

That is simpler for users and cleaner for package review.

* Tradeoff

The main tradeoff is developer setup: local byte-compilation and tests now need
the external =mysql= and =pg= packages on the load-path (or installed
normally).  That cost is acceptable because it matches the real package boundary
instead of hiding it inside one repo.
