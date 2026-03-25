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

### JSON serialization fallback (clutch.el)

Two sites fall back when `json-serialize` fails:
- Line ~1307: `(condition-case nil (json-serialize val) (error (format "%S" val)))`
- Line ~4231: `(condition-case nil (json-serialize (json-parse-string val)) (error (json-serialize val)))`

These display non-standard JSON values rather than raising errors.
From a UI resilience perspective this is intentional — a result buffer
should not crash because one cell has unexpected content.  But it masks
upstream data quality issues.

Lower priority: only worth addressing when the result ingestion layer
enforces normalized JSON values.

## Status

Recorded as design debt.  Neither item is a current defect or blocker.
