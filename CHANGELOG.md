# Changelog

## [Unreleased]

- Bundle the MySQL wire protocol layer as `clutch-mysql.el` with
  `clutch-mysql-*` public symbols and `clutch-mysql--*` internal symbols.
- Keep PostgreSQL on upstream `pg-el`, while MySQL is maintained inside
  `clutch` until the protocol layer has enough maintenance history to split.
- Add focused MySQL protocol API and compatibility documentation.
- Carry forward the `feature/package-split` work on optional backend loading,
  transaction toggles, metadata warmup, SSH tunnels, UI polish, and test
  consolidation.
