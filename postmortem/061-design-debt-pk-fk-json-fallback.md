# 061 — Design Debt: Late-Stage PK/FK Loading and JSON Fallback

## Items

### PK/FK late-stage detection (clutch-edit.el)

`clutch-result--detect-primary-key` and `clutch--load-fk-info` query the
database after result display to populate edit metadata.  Both wrap calls
in `condition-case nil`, silently returning nil on failure.

This is a design boundary issue, not a bug.  The edit layer compensates
for information not carried in `clutch-db-result`.  A cleaner design
would enrich result metadata with PK/FK info during result assembly,
so the edit layer reads pre-computed data rather than re-querying.

Not urgent: the current code works correctly when metadata is available
and degrades gracefully when it is not.

### JSON serialization fallback

Resolved in April 2026.  Core parameter rendering and result formatting now
signal `clutch-db-error` when JSON serialization fails instead of falling back
to Lisp printed forms.  JSON edit/view helpers still normalize raw JSON text,
but they no longer hide serialization failures for parsed object/array values.

## Status

PK/FK late-stage detection remains design debt.  The JSON fallback item is
closed.
