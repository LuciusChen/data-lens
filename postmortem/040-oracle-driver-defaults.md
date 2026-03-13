# 040. Oracle JDBC Default Driver Line

## Context

`clutch-jdbc-install-driver 'oracle` originally installed `ojdbc11.jar`.
That was reasonable for newer Oracle deployments, but it did not match the
compatibility reality we had already documented elsewhere:

- Oracle 11g/12c behave better with `ojdbc8`
- users often keep only one Oracle JDBC jar locally
- loading both `ojdbc8.jar` and `ojdbc11.jar` at once causes driver ambiguity

This left the docs and the default install path pulling in opposite directions.

## Decision

- `clutch-jdbc-install-driver 'oracle` now installs `ojdbc8.jar`
- `clutch-jdbc-install-driver 'oracle-11` is the explicit opt-in for
  `ojdbc11.jar`
- installing one Oracle driver line automatically removes the conflicting
  Oracle jar
- `orai18n.jar` remains an automatic companion for both Oracle driver lines

## Why

`ojdbc8` is the safer default across Oracle 11g/12c/19c, while `ojdbc11`
is mainly useful as an explicit newer-line choice for users who know they do
not need 11g compatibility.

The important part is not just which jar we download, but that the active
driver set stays unambiguous.  The shared JDBC agent scans every `*.jar` in
`drivers/`; silently leaving both Oracle jars active makes runtime behavior
harder to reason about.

## Consequences

- Fresh Oracle installs now favor compatibility first
- Existing users with an old `ojdbc11.jar` can normalize their setup by simply
  rerunning `clutch-jdbc-install-driver 'oracle`
- Users who want the newer Oracle line still have an explicit path:
  `clutch-jdbc-install-driver 'oracle-11`
