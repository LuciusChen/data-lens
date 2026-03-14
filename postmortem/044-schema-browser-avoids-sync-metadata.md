## 044. Schema Browser Avoids Sync Metadata On Lazy Backends

### Problem

Lazy JDBC backends had already moved console completion and schema refresh to
background models, but the schema browser still used synchronous metadata in two
places:

- opening the schema browser called `clutch-db-list-tables` directly
- expanding a table called `clutch-db-column-details` directly

That produced another class of "performance" bugs that were really UI-path
bugs: just opening the browser or expanding a table could block on Oracle
metadata.

### Fix

- schema browser now opens from the current schema cache when available
- lazy backends open immediately and start background table refresh instead of
  synchronously loading table names
- expanded tables on lazy backends now use cached column details or show a
  loading placeholder while async column metadata backfills

### Result

Lazy backend schema behavior is now consistent across console and browser:

- open immediately
- backfill metadata in the background
- rerender when the cache lands

