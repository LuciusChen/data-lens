# 058 — Tree Browser Historical Context

## Purpose

This note exists to keep older schema-browser postmortems readable after the
project moved to an object-centric workflow.

The following records are still useful, but they describe the removed
tree/schema-browser path rather than the current primary interaction model:

- `030-schema-status-visibility.md`
- `043-schema-browser-async-refresh-consistency.md`
- `044-schema-browser-avoids-sync-metadata.md`

## How to read them now

- Read them for background on schema cache state, async refresh behavior, and
  lazy backend performance constraints.
- Do not treat them as guidance for current UI entry points or buffer layout.
- When a current workflow question conflicts with those documents, prefer:
  - `054-object-centric-workflow-without-provider-layer.md`
  - `056-schema-switching-over-server-objects.md`
  - `057-jdbc-metadata-isolation-for-oracle-stability.md`

## Why keep the older records

The old browser is gone, but the backend realities it exposed are still real:

- schema freshness is still a first-class concern
- async metadata behavior still shapes UX decisions
- Oracle/JDBC performance constraints still matter

The older records are therefore historical context, not current workflow
specification.
