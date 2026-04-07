# clutch — Product Requirements Document

## 1. Project Overview

**clutch** is an interactive Emacs database client designed to provide an intuitive visual interface for browsing, querying, and mutating SQL databases directly within Emacs. It eliminates the need for external GUI tools or CLI clients by providing a rich, single-page result browser, object-centric schema workflow, and interactive REPL.

### Problem Statement

Emacs users lack a seamless, integrated database client that operates within their primary editor. Existing solutions require:
- External database GUIs (heavyweight, context-switching)
- Command-line clients (difficult data inspection)
- SQL-specific IDEs (separate tool ecosystem)

### Solution

clutch integrates directly into Emacs, offering:
- Native MySQL/PostgreSQL backends via external pure Elisp protocol packages
- Interactive SQL editing with completion
- Unified transient-based mutation workflow (edit/delete/insert with staged preview/commit)
- Schema caching and intelligent completion
- Optional Org-Babel integration via the separate `ob-clutch` package

### Target Users

- Data engineers and analysts working in Emacs
- Backend developers debugging databases directly from their editor
- Users who prefer text-driven, keyboard-centric workflows
- Researchers and analysts working with SQL data in Org-mode

---

## 2. Architecture

clutch follows a **layered, interface-based architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│  UI Layer (clutch.el)                                   │
│  - Interactive modes and transient menus                │
│  - Result display, editing buffers, object workflow     │
│  - Query execution, column paging, mutation workflow    │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────v──────────────────────────────────┐
│  Generic Interface (clutch-db.el)                       │
│  - cl-defgeneric methods (dispatch on conn type)        │
│  - Unified schema, query, lifecycle API                 │
│  - Result struct: clutch-db-result                      │
└──────────────────────┬──────────────────────────────────┘
         ┌─────────────┼─────────────┬──────────────────┐
         │             │             │                  │
         v             v             v                  v
    ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────────┐
    │  MySQL  │  │   PG    │  │  SQLite  │  │ JDBC Agent   │
    │ Backend │  │ Backend │  │ Backend  │  │ (JVM sidecar)│
    │clutch-  │  │clutch-  │  │clutch-   │  │clutch-db-    │
    │db-      │  │db-      │  │db-       │  │jdbc.el       │
    │mysql.el │  │pg.el    │  │sqlite.el │  │              │
    └─────────┘  └─────────┘  └──────────┘  └──────────────┘
         │             │             │                  │
         v             v             v                  v
    ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌──────────────┐
    │mysql-wire│ │ upstream│  │ Emacs    │  │ Java 17+ JVM │
    │(external │ │ pg-el   │  │ 29.1+    │  │ process +    │
    │ pure     │ │ package)│  │ -in      │  │ JDBC drivers │
    │ Elisp)   │ │         │  │ sqlite-* │  │              │
    │          │ │         │  │ functions│  │              │
    └─────────┘  └─────────┘  └──────────┘  └──────────────┘
```

### File Responsibilities

| File | Lines | Purpose |
|------|-------|---------|
| `clutch.el` | ~7800 | Main UI: modes, transient menus, result display, mutation workflow, object-centric schema workflow, schema caching |
| `clutch-db.el` | ~300 | Generic interface: `cl-defgeneric` definitions, result struct, shared helpers |
| `clutch-db-mysql.el` | ~320 | MySQL backend adapter, type-category mapping |
| `clutch-db-pg.el` | ~350 | PostgreSQL backend adapter, OID-to-type mapping |
| `clutch-db-sqlite.el` | ~330 | SQLite backend adapter (Emacs 29.1+ `sqlite-*` functions) |
| `clutch-db-jdbc.el` | ~980 | JDBC backend: JVM sidecar management, JSON protocol, async schema, runtime schema switching |
| External dependency: `mysql-wire` | n/a | Pure Elisp MySQL wire protocol client (separate package) |
| External dependency: `pg` | n/a | PostgreSQL client from upstream `pg-el` (separate package) |
| Optional package: `ob-clutch` | n/a | Org-Babel integration bridge (separate package) |

For JDBC-backed databases, one logical clutch connection now maps to two JDBC
sessions inside the sidecar:
- a primary session for foreground SQL, transactions, and DDL
- a metadata session for schema/object introspection

This keeps Oracle-style metadata refresh and object warming from contending with
user queries on the same JDBC session.

---

## 3. Supported Backends

### Native Backends (Pure Elisp)

| Backend | Emacs Version | Implementation | Notes |
|---------|---------------|----------------|-------|
| **MySQL** | 28.1+ | `mysql-wire` | External pure Elisp protocol package; supports MySQL 5.6+, 8.0+, MariaDB 10.11+ |
| **PostgreSQL** | 28.1+ | `pg` | External `pg-el` package; supports PG 12+ |
| **SQLite** | 29.1+ | Emacs built-in `sqlite-*` | Synchronous queries only |

### JDBC Backends (via JVM Sidecar)

| Backend | Driver | Version | Source |
|---------|--------|---------|--------|
| **Oracle** | `ojdbc8` | 19.21.0.0 | Maven Central (auto-download) |
| **Oracle i18n** | `orai18n` | 21.13.0.0 | Maven Central (optional, for non-ASCII) |
| **SQL Server** | `mssql-jdbc` | 13.4.0.jre11 | Maven Central (auto-download) |
| **Snowflake** | `snowflake-jdbc` | 3.14.4 | Maven Central (auto-download) |
| **Amazon Redshift** | `redshift-jdbc42` | 2.1.0.30 | Maven Central (auto-download) |
| **DB2** | `db2jcc4` | — | Manual installation from IBM |
| **Generic JDBC** | any | — | Drop jar into `clutch-jdbc-agent-dir/drivers/` |

---

## 4. Version Requirements

| Component | Minimum Version | Notes |
|-----------|-----------------|-------|
| Emacs | 28.1 | MySQL, PostgreSQL native backends |
| Emacs | 29.1 | SQLite (built-in `sqlite-*` functions) |
| Java | 17 | JDBC agent (`clutch-jdbc-agent.jar`) |
| MySQL | 5.6 | Wire protocol baseline |
| PostgreSQL | 12 | Information schema queries |
| MariaDB | 10.11 | Compatible with MySQL wire protocol |

---

## 5. Modes and Buffers

### clutch-mode (SQL Editor)

**Derived from**: `sql-mode`
**Buffer name pattern**: `*clutch: NAME*`

SQL query editing and execution mode. The primary entry point for interacting with a database.

**Buffer-local state**:

| Variable | Purpose |
|----------|---------|
| `clutch-connection` | Current live database connection |
| `clutch--connection-params` | Stored params for auto-reconnect |
| `clutch--executing-p` | Query execution in progress flag |
| `clutch--executing-sql-start/end` | Region markers for current query |
| `clutch--last-query` | Last executed SQL string |
| `clutch--console-name` | Name for persisting console to disk |
| `clutch--tables-in-buffer-cache` | Cached `(tick . tables)` for completion |
| `clutch--tables-in-query-cache` | Cached `(tick beg end . tables)` |

Shared schema metadata consulted by `clutch-mode` lives in global caches keyed
by `clutch--connection-key`: `clutch--schema-cache`,
`clutch--column-details-cache`, and `clutch--schema-status-cache`.

**Keybindings**:

| Key | Command | Description |
|-----|---------|-------------|
| `C-c C-e` | `clutch-connect` | Connect; query consoles reconnect their own saved connection |
| `C-c C-c` | `clutch-execute-dwim` | Execute the region when active, otherwise the query at point |
| `C-c ;` | `clutch-execute-statement-at-point` | Execute statement (`;`-only delimiter; blank lines preserved) |
| `C-c C-r` | `clutch-execute-region` | Execute the active region |
| `C-c C-b` | `clutch-execute-buffer` | Execute the whole buffer |
| `C-c C-m` | `clutch-commit` | Commit transaction (manual-commit connections only) |
| `C-c C-u` | `clutch-rollback` | Roll back transaction (manual-commit connections only) |
| `C-c C-a` | `clutch-toggle-auto-commit` | Toggle auto-commit for the current connection |
| `C-c C-j` | `clutch-jump` | Resolve an object and run its default action |
| `C-c C-d` | `clutch-describe-dwim` | Describe the object at point, or prompt |
| `C-c C-o` | `clutch-act-dwim` | Show object actions for the current object, or prompt |
| `C-c C-l` | `clutch-switch-schema` | Switch the current schema/database |
| `C-c C-p` | `clutch-preview-execution-sql` | Preview the current execution payload |
| `C-c C-s` | `clutch-refresh-schema` | Refresh schema cache |
| `C-c ?` | Transient dispatch | Main command menu |

`clutch-mode` also installs a buffer-local xref backend and CAPF pipeline, so
standard Emacs bindings such as `M-.` and `TAB` keep working through inherited
xref / completion machinery rather than explicit `define-key` entries.

---

### clutch-result-mode (Query Results Table)

**Derived from**: `special-mode` (read-only)
**Buffer name pattern**: `*clutch-result: USER@HOST:PORT/DB*`

Interactive result browsing with column paging, sorting, filtering, mutations.
The result buffer owns the table header line; SQL-backed refreshes keep that
header visible and reuse the elapsed-time footer slot as a spinner while the
query is still running.

**Buffer-local state**:

| Variable | Purpose |
|----------|---------|
| `clutch--result-columns` | Column metadata plists (`(:name N :type T ...)`) |
| `clutch--result-rows` | Row data (list of vectors) |
| `clutch--result-column-defs` | Column definitions from query |
| `clutch--column-widths` | Display width vector |
| `clutch--row-start-positions` | Vector of row buffer positions (O(1) goto) |
| `clutch--pending-edits` | Staged cell modifications (PK → col → value) |
| `clutch--pending-deletes` | Staged row deletions (list of PK vectors) |
| `clutch--pending-inserts` | Staged new rows |
| `clutch--marked-rows` | Dired-style marked row index set |
| `clutch--cached-pk-indices` | Primary key column positions |
| `clutch--fk-info` | Foreign key metadata for result table |
| `clutch--page-current` | Current row page (0-based) |
| `clutch--page-total-rows` | Total row count (from COUNT(*)) |
| `clutch--sort-column` | Current sort column name |
| `clutch--sort-descending` | Sort direction flag |
| `clutch--order-by` | `(COL . "ASC"\|"DESC")` |
| `clutch--where-filter` | Active SQL WHERE clause |
| `clutch--filter-pattern` | Client-side row filter regex |
| `clutch--filtered-rows` | Filtered row subset or nil |
| `clutch--last-query` | Last executed query SQL string |
| `clutch--base-query` | Base query used for SQL-backed refinement/filtering |
| `clutch--query-elapsed` | Elapsed execution time for the current result |
| `clutch--result-source-table` | Detected source table name (for mutations / metadata enrichment) |
| `clutch-connection` | Connection from parent clutch-mode buffer |

**Keybindings**:

| Key | Command | Description |
|-----|---------|-------------|
| `RET` | `clutch-result-open-record` | Open record view |
| `TAB` / `S-TAB` | `clutch-result-next-cell` / `clutch-result-prev-cell` | Move between cells |
| `n` / `p` | `clutch-result-down-cell` / `clutch-result-up-cell` | Move to next/previous row in same column |
| `N` / `P` | `clutch-result-next-page` / `clutch-result-prev-page` | Next / previous SQL page |
| `M-<` / `M->` | `clutch-result-first-page` / `clutch-result-last-page` | First / last SQL page |
| `]` / `[` | `clutch-result-scroll-right` / `clutch-result-scroll-left` | Page right / left (snap to column border) |
| `=` / `-` | `clutch-result-widen-column` / `clutch-result-narrow-column` | Adjust column width |
| `C` | `clutch-result-goto-column` | Jump to a column by name |
| `?` | `clutch-result-column-info` | Show column type info at point |
| `s` / `S` | `clutch-result-sort-by-column` / `clutch-result-sort-by-column-desc` | Sort by current column |
| `W` | `clutch-result-apply-filter` | Apply SQL WHERE filter (column completion with auto-equal) |
| `/` | `clutch-result-filter` | Client-side fuzzy filter |
| `C-c '` | `clutch-result-edit-cell` | Edit / re-edit current cell |
| `i` | `clutch-result-insert-row` | Open insert buffer |
| `I` | `clutch-clone-row-to-insert` | Clone current row into a prefilled insert buffer without PK values |
| `d` | `clutch-result-delete-rows` | Stage row(s) for deletion |
| `C-c C-c` | `clutch-result-commit` | Commit staged INSERT/UPDATE/DELETE changes |
| `C-c C-k` | `clutch-result-discard-pending-at-point` | Discard pending change at point |
| `C-c C-p` | `clutch-preview-execution-sql` | Preview pending batch or effective query |
| `c` | `clutch-result-copy-dispatch` | Copy transient (TSV / CSV / INSERT / UPDATE) |
| `e` | `clutch-result-export` | Export all rows (CSV / INSERT / UPDATE copy/file) |
| `v` | `clutch-result-view-value` | View current cell value |
| `|` | `clutch-result-shell-command-on-cell` | Pipe the current cell through a shell command |
| `g` | `clutch-result-rerun` | Re-execute original query |
| `#` | `clutch-result-count-total` | Count total rows |
| `A` | `clutch-result-aggregate` | Aggregate numeric values |
| `f` | `clutch-result-fullscreen-toggle` | Toggle fullscreen |
| `C-c ?` | `clutch-result-dispatch` | Result-buffer transient menu |
| `q` | `quit-window` | Close result buffer |

**Pending SQL workflow**:
- Result transient includes a dedicated *Pending* group:
  - `y` → `clutch-result-copy-pending-sql`
  - `Y` → `clutch-result-save-pending-sql`
- This exports the exact staged SQL batch that `C-c C-c` would execute, rather than re-exporting the full result set.

---

### clutch-record-mode (Single-Row Detail View)

**Derived from**: `special-mode`
**Buffer name pattern**: `*clutch-record*`

Full-width inspection of a single row; each field occupies one or more lines.

**Buffer-local state**:

| Variable | Purpose |
|----------|---------|
| `clutch-record--result-buffer` | Reference to parent result buffer |
| `clutch-record--row-idx` | Current row index |
| `clutch-record--expanded-fields` | Set of column indices with expanded values |

**Keybindings**:

| Key | Command | Description |
|-----|---------|-------------|
| `RET` | `clutch-record-toggle-expand` | Expand/collapse long field, follow FK, or echo short value |
| `p` | `clutch-record-prev-row` | Previous row |
| `n` | `clutch-record-next-row` | Next row |
| `I` | `clutch-clone-row-to-insert` | Clone current record into a prefilled insert buffer without PK values |
| `v` | `clutch-record-view-value` | View current field value |
| `g` | `clutch-record-refresh` | Re-render the current record |
| `C-c ?` | `clutch-record-dispatch` | Record-buffer transient menu |
| `q` | `quit-window` | Close record buffer |

---

### clutch-result-insert-mode (New Row Creation Form)

**Purpose**: Create and validate a new row with per-field editors.

Each field is displayed with:
- A read-only colored prefix (field name + metadata tags)
- An editable value region

**Keybindings**:

| Key | Command | Description |
|-----|---------|-------------|
| `RET` | `clutch-result-insert-submit-field` | Accept current field and move to next |
| `TAB` | `clutch-result-insert-next-field` | Move to next field |
| `S-TAB` | `clutch-result-insert-prev-field` | Move to previous field |
| `M-TAB` / `C-M-i` | `clutch-result-insert-complete-field` | Complete enum/bool-like values |
| `C-c '` | `clutch-result-insert-edit-json-field` | Open JSON sub-editor for current field |
| `C-c .` | `clutch-result-insert-fill-current-time` | Fill current temporal field with now |
| `C-c C-a` | `clutch-result-insert-toggle-field-layout` | Toggle sparse vs all-column layout |
| `C-c C-y` | `clutch-result-insert-import-delimited` | Import TSV / CSV into the form |
| `C-c C-c` | `clutch-result-insert-commit` | Validate and stage row(s) |
| `C-c C-k` | `clutch-result-insert-cancel` | Cancel and close buffer |

---

### clutch-repl-mode (Interactive REPL)

**Derived from**: `comint-mode`
**Buffer name pattern**: `*clutch REPL*`

Line-by-line SQL evaluation with history and inline results.

**Features**:
- SQL history via comint ring
- Completion from cached schema (tables, columns)
- Result display inline or in companion result buffer

**Keybindings**:

| Key | Command | Description |
|-----|---------|-------------|
| `C-c C-e` | `clutch-connect` | Connect to a database |
| `C-c C-m` | `clutch-commit` | Commit transaction |
| `C-c C-u` | `clutch-rollback` | Roll back transaction |
| `C-c C-a` | `clutch-toggle-auto-commit` | Toggle auto-commit |
| `C-c C-j` | `clutch-jump` | Resolve an object and run its default action |
| `C-c C-d` | `clutch-describe-dwim` | Describe the object at point, or prompt |
| `C-c C-o` | `clutch-act-dwim` | Show object actions |
| `C-c C-l` | `clutch-switch-schema` | Switch the current schema/database |

---

## 6. All Interactive Commands

Section 5 documents per-mode navigation and editing keys.  This section focuses
on public `M-x` entry points and named commands that users may call directly.

### Connection

| Command | Description |
|---------|-------------|
| `clutch-connect` | Connect using profile from `clutch-connection-alist` or inline params; query consoles reconnect their associated saved connection |
| `clutch-disconnect` | Close database connection |
| `clutch-commit` | Commit the current transaction |
| `clutch-rollback` | Roll back the current transaction |
| `clutch-toggle-auto-commit` | Toggle auto-commit / manual-commit mode |
| `clutch-switch-console` | Switch to a named query console buffer |
| `clutch-query-console` | Open or switch to a named query console |
| `clutch-refresh-schema` | Manually refresh schema cache for current connection |
| `clutch-switch-schema` | Switch the current schema/database on the active connection |
| `clutch-switch-database` | ClickHouse-only reconnect-based database switch |
| `clutch-debug-mode` | Toggle the dedicated `*clutch-debug*` capture buffer |

### Execution

| Command | Description |
|---------|-------------|
| `clutch-preview-execution-sql` | Preview the effective execution payload for the current workflow |
| `clutch-execute-query-at-point` | Execute the SQL query at point |
| `clutch-execute-statement-at-point` | Execute statement using `;` as only delimiter (blank lines preserved) |
| `clutch-execute-dwim` | Execute the active region, or the query at point |
| `clutch-execute-region` | Execute the active region |
| `clutch-execute-buffer` | Execute the whole buffer |
| `clutch-execute` | Execute SQL from any buffer using the current/live clutch connection |
| `clutch-edit-indirect` | Open the current region/string/line in an indirect SQL edit buffer |
| `clutch-indirect-execute` | Execute SQL from the indirect edit buffer and close it |
| `clutch-indirect-abort` | Abort the indirect edit buffer |

### Mode / Buffer Entry Points

| Command | Description |
|---------|-------------|
| `clutch-mode` | SQL editing major mode |
| `clutch-result-mode` | Result-table major mode |
| `clutch-record-mode` | Single-record detail major mode |
| `clutch-result-insert-mode` | Insert-form major mode wrapper |
| `clutch-describe-mode` | Object describe major mode |
| `clutch-repl-mode` | REPL major mode |
| `clutch-repl` | Open the shared `*clutch REPL*` buffer |

### Result / Mutation Workflow

| Command | Description |
|---------|-------------|
| `clutch-result-open-record` | Open the Record buffer for the row at point |
| `clutch-result-edit-cell` | Stage an edit to the current cell |
| `clutch-result-delete-rows` | Stage the current row or region rows for deletion |
| `clutch-result-insert-row` | Open an insert buffer for the result table |
| `clutch-clone-row-to-insert` | Clone the current row/record into a prefilled insert buffer |
| `clutch-result-commit` | Confirm and execute staged INSERT/UPDATE/DELETE changes |
| `clutch-result-discard-pending-at-point` | Discard the staged change at point |
| `clutch-result-copy-pending-sql` | Copy the exact staged SQL batch |
| `clutch-result-save-pending-sql` | Save the exact staged SQL batch to a file |
| `clutch-result-rerun` | Re-execute the current result query |
| `clutch-result-count-total` | Query total row count for the current result |
| `clutch-result-aggregate` | Aggregate numeric values over the current cell/selection |
| `clutch-result-filter` | Apply a client-side fuzzy filter |
| `clutch-result-apply-filter` | Apply an SQL-backed WHERE filter |
| `clutch-result-sort-by-column` | Apply ascending SQL ORDER BY for the current column |
| `clutch-result-sort-by-column-desc` | Apply descending SQL ORDER BY for the current column |
| `clutch-result-column-info` | Show column type/default/nullability info at point |
| `clutch-result-view-value` | Open the value viewer for the current cell |
| `clutch-result-shell-command-on-cell` | Pipe the current cell through a shell command |
| `clutch-result-goto-column` | Jump to a result column by name |
| `clutch-result-scroll-right` / `clutch-result-scroll-left` | Horizontal result paging aligned to column borders |
| `clutch-result-widen-column` / `clutch-result-narrow-column` | Adjust current column width |
| `clutch-result-fullscreen-toggle` | Toggle fullscreen display of the current result |
| `clutch-result-next-page` / `clutch-result-prev-page` | Move to the next / previous SQL page |
| `clutch-result-first-page` / `clutch-result-last-page` | Jump to the first / last SQL page |
| `clutch-result-next-cell` / `clutch-result-prev-cell` | Move across cells |
| `clutch-result-down-cell` / `clutch-result-up-cell` | Move down/up within the current column |
| `clutch-result-copy-tsv` / `clutch-result-copy-csv` | Copy the current cell/selection as TSV / CSV |
| `clutch-result-copy-insert` / `clutch-result-copy-update` | Copy rows as INSERT / UPDATE statements |
| `clutch-result-copy-dispatch` | Open the copy transient |
| `clutch-result-export` | Export all rows as CSV / INSERT / UPDATE (copy or file) |
| `clutch-refine-mode` | Transient refine mode for excluding rows/columns before copy/aggregate |
| `clutch-refine-toggle-row` / `clutch-refine-toggle-col` | Refine-mode row/column exclusion toggles |
| `clutch-refine-confirm` / `clutch-refine-cancel` | Confirm / cancel refine mode |

### Insert / Edit Helpers

| Command | Description |
|---------|-------------|
| `clutch-result-edit-complete-field` | Complete enum/bool-like edit values |
| `clutch-result-edit-set-current-time` | Fill the current temporal edit field with “now” |
| `clutch-result-edit-json-field` | Open the JSON sub-editor for the current edit field |
| `clutch-result-edit-finish` / `clutch-result-edit-cancel` | Confirm / cancel the single-cell edit buffer |
| `clutch-result-insert-next-field` / `clutch-result-insert-prev-field` | Move between insert fields |
| `clutch-result-insert-submit-field` | Accept the current field and move forward |
| `clutch-result-insert-complete-field` | Complete enum/bool-like insert values |
| `clutch-result-insert-fill-current-time` | Fill the current temporal insert field with “now” |
| `clutch-result-insert-edit-json-field` | Open the insert-form JSON sub-editor |
| `clutch-result-insert-json-finish` / `clutch-result-insert-json-cancel` | Confirm / cancel the insert-form JSON editor |
| `clutch-result-insert-toggle-field-layout` | Toggle sparse vs all-column layout |
| `clutch-result-insert-import-delimited` | Import TSV / CSV into the current insert form |
| `clutch-result-insert-commit` / `clutch-result-insert-cancel` | Stage / cancel the insert form |

### Object Workflow

| Command | Description |
|---------|-------------|
| `clutch-jump` | Resolve an object and run its default action |
| `clutch-describe-dwim` | Resolve an object and open its describe view |
| `clutch-act-dwim` | Resolve an object and show object actions |
| `clutch-describe-refresh` | Refresh the current describe buffer |
| `clutch-object-show-ddl-or-source` | Show object DDL or source text |
| `clutch-object-describe` | Describe a specific object entry |
| `clutch-object-browse` | Insert `SELECT *` for a table-like object into a console |
| `clutch-object-jump-target` | Jump to the target object of a synonym/index/trigger |
| `clutch-object-default-action` | Run the resolved object’s default action |
| `clutch-copy-object-name` / `clutch-copy-object-fqname` | Copy object name / fully qualified name |
| `clutch-describe-table` / `clutch-describe-table-at-point` | Describe a table explicitly |
| `clutch-browse-table` | Browse rows for a table-like object |

### JDBC Management

| Command | Description |
|---------|-------------|
| `clutch-jdbc-ensure-agent` | Download and verify agent jar if missing |
| `clutch-jdbc-install-driver` | Download a JDBC driver from Maven Central |

---

## 7. Configuration (defcustom Variables)

### Connection Management

```elisp
(defcustom clutch-connection-alist nil
  :type '(alist :key-type string :value-type plist)
  :group 'clutch)
```

Connection profile plist keys:

| Key | Type | Description |
|-----|------|-------------|
| `:host` | string | Database host |
| `:port` | integer | Database port |
| `:user` | string | Database user |
| `:password` | string | Password (prefer auth-source instead) |
| `:database` | string | Database/schema name |
| `:sid` | string | Oracle SID when using `@host:port:SID` style connections |
| `:backend` | symbol | `mysql`, `pg`, `sqlite`, `jdbc`, `clickhouse`, `oracle`, `sqlserver`, `snowflake`, `redshift`, `db2` |
| `:sql-product` | symbol | SQL highlight product for `sql-mode` |
| `:pass-entry` | string | Pass store suffix for password lookup |
| `:display-name` | string | Friendly backend label shown in the UI |
| `:url` | string | Full JDBC URL (JDBC backends; overrides host/port/database) |
| `:props` | alist | Extra JDBC connection properties |
| `:manual-commit` | boolean | JDBC only: disable auto-commit for this connection |
| `:tls` | boolean | Convenience shorthand; maps to backend-native TLS settings |
| `:ssl-mode` | symbol | MySQL only: `disabled` forces plaintext and suppresses auto-TLS retry (`off` alias accepted) |
| `:sslmode` | symbol | PostgreSQL only: `disable`, `prefer`, `require`, or `verify-full` |
| `:connect-timeout` | natnum | Connection timeout (seconds) |
| `:read-idle-timeout` | natnum | Read idle timeout (seconds) |
| `:query-timeout` | natnum | Server-side statement timeout (seconds) |
| `:rpc-timeout` | natnum | JDBC agent RPC timeout (seconds; per-connection override) |

### Display

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-result-window-height` | `0.33` | float | Result window height as fraction of frame height |
| `clutch-result-max-rows` | `500` | natnum | Maximum rows per page |
| `clutch-column-width-max` | `30` | natnum | Maximum column display width |
| `clutch-column-width-step` | `5` | natnum | Column width adjustment step |
| `clutch-column-padding` | `1` | natnum | Padding spaces on each side of a cell |
| `clutch-column-displayers` | `nil` | alist | Per-table/per-column custom result displayers |

### SQL and Timeouts

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-sql-product` | `'mysql` | choice | SQL highlight mode: `mysql`, `postgres`, `mariadb`, etc. |
| `clutch-connect-timeout-seconds` | `10` | natnum | Connection establishment timeout |
| `clutch-read-idle-timeout-seconds` | `30` | natnum | Read idle timeout (MySQL, PG, JDBC) |
| `clutch-query-timeout-seconds` | `30` | natnum | Server-side statement timeout (PG, JDBC) |
| `clutch-jdbc-rpc-timeout-seconds` | `30` | natnum | Global JDBC agent RPC timeout |
| `clutch-object-warmup-idle-delay-seconds` | `0.5` | number | Idle delay before background object warmup starts |
| `clutch-primary-object-types` | `("TABLE" "VIEW" "SYNONYM")` | repeat string | Primary object types used by `clutch-jump` |
| `clutch-sql-completion-case-style` | `'preserve` | choice | Preserve, lowercase, or uppercase inserted completion text |
| `clutch-schema-cache-install-batch-size` | `500` | natnum | Batch size for idle schema-cache installation |
| `clutch-debug-event-limit` | `25` | natnum | Maximum debug events retained in `*clutch-debug*` |

`clutch-jdbc-cancel-timeout-seconds` and
`clutch-jdbc-disconnect-timeout-seconds` still exist in
`clutch-db-jdbc.el`, but they are plain internal `defvar`s rather than
user-facing `defcustom`s.

### Insert Buffer

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-insert-validation-idle-delay` | `0.2` | number | Idle seconds before validating complex fields |

### Persistence

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-console-directory` | `~/.emacs.d/clutch/` | directory | Directory for persisting query console buffers |
| `clutch-console-yank-cleanup` | `t` | boolean | Clean whitespace in pasted region after yank in query consoles |

### Export

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-csv-export-default-coding-system` | `'utf-8-with-signature` | coding-system | Default CSV encoding (UTF-8 BOM for Excel) |

### JDBC Agent

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `clutch-jdbc-agent-dir` | `~/.emacs.d/clutch-jdbc/` | directory | Directory for agent jar and `drivers/` |
| `clutch-jdbc-agent-version` | `"0.2.3"` | string | Agent version to download |
| `clutch-jdbc-agent-sha256` | (hash string) | string or nil | Expected SHA-256 of agent jar; nil to disable |
| `clutch-jdbc-agent-java-executable` | `"java"` | string | Java executable path |
| `clutch-jdbc-agent-jvm-args` | `'("-Xss512k")` | list of strings | Extra JVM arguments |
| `clutch-jdbc-fetch-size` | `500` | natnum | Rows per fetch batch from JDBC cursor |
| `clutch-jdbc-oracle-manual-commit` | `t` | boolean | Oracle JDBC default: manual-commit instead of auto-commit |

---

## 8. Faces

| Face | Inherits / Colors | Purpose |
|------|-------------------|---------|
| `clutch-header-face` | bold | Column headers in result tables |
| `clutch-header-active-face` | `hl-line`, bold | Column header under cursor |
| `clutch-border-face` | shadow | Table borders (│, separators) |
| `clutch-object-source-face` | shadow-ish accent | Object source/schema annotations |
| `clutch-object-public-source-face` | accent variant | Public-object source annotations |
| `clutch-object-type-face` | bold accent | Object type labels |
| `clutch-null-face` | shadow, italic | NULL cell values |
| `clutch-modified-face` | Light: #fff3cd bg / Dark: #3d2b00 bg | Pending-edit cell values |
| `clutch-fk-face` | `font-lock-type-face`, underlined | Foreign key column values |
| `clutch-marked-face` | `dired-marked` | Dired-style marked rows |
| `clutch-executed-sql-marker-face` | Light: #eaf5e9 bg / Dark: #223526 bg | Last executed SQL gutter marker |
| `clutch-pending-delete-face` | Light: #fde8e8 bg, #9b1c1c fg, strikethrough | Rows staged for deletion |
| `clutch-pending-insert-face` | Light: #e6f4ea bg, #1e4620 fg / Dark: #1a3320 bg | Rows staged for insertion |
| `clutch-error-position-face` | Light: #fde8e8 bg, wave red / Dark: #3b1212 bg | Character at SQL error position |
| `clutch-error-banner-face` | Light: #fee2e2 bg, #991b1b fg / Dark: #451a1a bg | Inline SQL execution error banner |
| `clutch-insert-field-name-face` | bold, `#b8d7ec` fg | Insert-buffer field names |
| `clutch-insert-field-tag-face` | shadow | Metadata tags (`[generated]`, `[default]`, ...) |
| `clutch-insert-field-error-face` | wave red underline | Invalid values in insert buffer |
| `clutch-insert-inline-error-face` | error | Inline insert-buffer validation messages |
| `clutch-insert-active-field-face` | `hl-line` | Active line in insert buffer |
| `clutch-insert-active-field-name-face` | `clutch-header-active-face`, `clutch-insert-field-name-face` | Active insert-buffer field prefix |

---

## 9. Connection Management

### Profile-Based Configuration

```elisp
(setq clutch-connection-alist
  '(("prod-mysql"    . (:host "db.prod.com"    :port 3306   :user "app"
                        :database "main"        :tls t))
    ("dev-pg"        . (:host "localhost"       :port 5432   :user "dev"
                        :database "testdb"      :backend pg))
    ("local-sqlite"  . (:backend sqlite         :database "/tmp/test.db"))
    ("oracle-uat"    . (:backend oracle          :host "oradb.uat.local"
                        :port 1521              :user "scott" :database "ORCL"))
    ("snowflake-prod". (:backend snowflake       :host "xy12345.snowflakecomputing.com"
                        :user "analyst"         :pass-entry "snow-prod"
                        :database "PROD"        :props (("db" . "ANALYTICS"))))))
```

### Password Resolution (Priority Order)

1. **`:password` key** — used as-is if non-empty string
2. **Pass store** (via `auth-source-pass`, if loaded) — connection name matched by suffix; override with `:pass-entry`
3. **`auth-source-search`** — matches by `:host`, `:user`, `:port` from `~/.authinfo` or `~/.authinfo.gpg`
4. **Interactive prompt** — `read-passwd` fallback

### Multi-Connection Support

- Each buffer has its own `clutch-connection` (buffer-local)
- Multiple database buffers can be open simultaneously with different connections
- Result buffers are scoped to their parent connection
- `clutch--connection-params` stored for auto-reconnect after drop

### TLS/SSL

```elisp
("secure-pg" . (:host "db.example.com" :port 5432 :user "app"
                :database "prod" :tls t))
```

---

## 10. Schema Caching

### Cache Layers

| Cache | What | Key |
|-------|------|-----|
| `clutch--schema-cache` | Table names, column names/types | `clutch--connection-key` string |
| `clutch--column-details-cache` | PK, FK, defaults, nullable, generated | `clutch--connection-key` string |
| `clutch--tables-in-buffer-cache` | Tables in current SQL buffer | `(tick . table-list)` |
| `clutch--tables-in-query-cache` | Tables in last executed query | `(tick beg end . table-list)` |
| `clutch--fk-info` | FK metadata for current result | Per-result buffer |

### Eager vs. Async Refresh

- **Eager (synchronous)**: SQLite — lightweight in-process metadata path
- **Async (deferred)**: MySQL, PostgreSQL, JDBC — deferred refresh with ticket-based stale-response guard and timeout/failure exit
  - Buffer status line shows: `[schema...]` (refreshing), `[schema~]` (stale), `[schema!]` (failed), `[schema Nt]` (ready)
  - `Nt` is the size of the current schema-cache snapshot; `schema 0t` means refresh succeeded but cached zero browseable objects
  - Native MySQL/PostgreSQL schedule passive metadata work on the main thread when Emacs is idle; JDBC keeps using the agent's metadata session
- **Explicit manual refresh**: `clutch-refresh-schema` (`C-c C-s`) bypasses the deferred path and runs a foreground refresh so users can immediately recover from a failed lazy refresh

### Cache Invalidation

- Manual: `clutch-refresh-schema` (`C-c C-s`)
- Auto: after successful DDL execution (CREATE/ALTER/DROP)
- Stale detection: all async-refresh backends reject stale responses via refresh tickets / generation checks

### Completion from Schema

- `clutch--tables-in-buffer`: tables referenced in current SQL (buffer-chars-modified-tick cached)
- `clutch--tables-in-query`: tables in last executed query (tick + region start/end cached)
- CAPF suggests: table names, column names (scoped to detected tables), SQL keywords
- Native MySQL/PostgreSQL CAPF/Eldoc are cache-first: cache misses queue background column-name / column-detail / table-comment preheat instead of blocking the editing hot path
- Explicit detail commands stay synchronous by design: describe / DDL / result `?` can block, but passive metadata display should not

---

## 11. Query Execution and Pagination

### Execution Flow

```
User types SQL in clutch-mode
  → C-c C-c / clutch-execute-dwim
  → Parse: selected region OR query at point
  → clutch-db-query (dispatched by cl-defgeneric to backend)
  → Display in clutch-result-mode buffer
  → Initialize pagination: page 0, first clutch-result-max-rows rows
```

### Row Pagination

- **Page size**: `clutch-result-max-rows` (default 500)
- **Page numbering**: 0-based (page 0 = offset 0)
- **Total count**: loaded via `COUNT(*)` query for progress indication
- **Navigation**: `N` / `P` (next/prev page), `M-<` / `M->` (first/last page)
- **Offset**: `offset = page * page-size`

### Horizontal Overflow

- **Layout**: every result column is rendered into one buffer
- **Navigation**: `]` / `[` page the window horizontally, snapping to column borders
- **Searchability**: `isearch` and `TAB` traversal work across all columns
- **Width adjustment**: `=` / `-` (increase/decrease by `clutch-column-width-step`)

### Cell Navigation

- **Text properties**: each cell has `clutch-row-idx`, `clutch-col-idx`, `clutch-full-value`
- **Row positions vector**: `clutch--row-start-positions` — O(1) jump to any row
- **Cursor preservation**: row/col index tracked across renders

### JDBC Cursor Model

- Server-side cursor remains open between `fetch` calls
- `clutch-jdbc-fetch-size` (default 500) rows per batch
- All rows fetched eagerly via `clutch-jdbc--fetch-all` (`push` + `nreverse` + `nconc`)
- Cursor state is released when `done=true`; the current Elisp client does not
  send a separate `close-cursor` RPC

---

## 12. Mutation Workflow

### Staged Edit/Delete/Insert

All mutations are **staged** and committed as a batch.

#### Stage → Preview → Commit

```
Stage (C-c '/d/i)      Preview (C-c C-p)          Commit / Discard
─────────────     →    ─────────────────    →      ───────────────
edit cell              Show SQL preview           Execute SQL batch
delete row             (readable rendered SQL)    or C-c C-k: discard
insert row             Confirm or cancel
```

Footer shows staging status: `E-2  D-1  I-3  commit:C-c C-c  discard:C-c C-k`

#### Edit Cell

1. `C-c '` on a cell → open a dedicated edit buffer
2. Pending edit stored in `clutch--pending-edits` keyed by PK + column
3. Cell shown with `clutch-modified-face` until committed
4. Confirmation preview renders literal SQL text, but native MySQL/PostgreSQL/SQLite execute staged DML through parameter binding via `clutch-db-execute-params`

#### Delete Row

1. `d` on a row → row shown with strike-through + `D` marker in left column
2. Stored by primary key in `clutch--pending-deletes`
3. Commit generates: `DELETE FROM table WHERE pk = id`

#### Insert Row

1. `i` → open `clutch-result-insert-mode` buffer
2. Default layout is sparse: required / no-default fields first, plus any prefilled values
3. `I` clones the current result/record row into a prefilled insert form without primary-key values
4. `C-c C-a` expands back to all columns without dropping existing values
5. `C-c C-y` imports TSV / CSV: one row prefills the form, multiple rows stage pending inserts immediately
6. Local validation runs on idle before staging
7. `C-c C-c` validates all visible/hidden field values and stages pending INSERT rows

### Mutation SQL Generation Rules

- **UPDATE/DELETE**: always keyed by primary key (mutations disabled without PK)
- **INSERT**: generated/default columns omitted from column list (let DB handle)
- **Composite PKs**: supported; WHERE clause uses all PK columns
- **NULL PK components**: WHERE templates keep `IS NULL` literals for null PK parts instead of binding `NULL = ?`

---

## 13. Insert Buffer UX

### Field Metadata Tags

Each field in the insert buffer is annotated:

| Tag | Condition | Behavior |
|-----|-----------|----------|
| `[generated]` | AUTO_INCREMENT, SERIAL, `GENERATED ALWAYS` | Hidden in sparse mode; shown in all-column layout for awareness |
| `[default=X]` | Column has explicit default | Hidden in sparse mode unless already prefilled; shown in all-column layout |
| `[required]` | NOT NULL with no default | Error if submitted empty |
| `[enum]` | ENUM or SET type | CAPF dropdown with allowed values |
| `[bool]` | BOOLEAN type | Toggle editor |
| `[json]` | JSON/JSONB type | Editor with syntax validation |
| (no tag) | Regular column | Normal text input |

### Sparse vs All-Column Layout

- Sparse mode is the default insert view
- Sparse mode shows non-generated columns that are required, have no default, or already carry a value
- `C-c C-a` toggles to all columns and back
- Hidden-field values are kept in canonical insert state, so toggling never drops edits

### Delimited Import

- `C-c C-y` reads from the active region, or the kill ring when no region is active
- Single-row import updates the current form in place
- Multi-row import stages pending inserts directly
- Header-based imports map by column name
- Header-less imports map positionally using the fields currently visible in the insert buffer

### Validation

Runs idle after `clutch-insert-validation-idle-delay` (default 0.2s):

- **JSON fields**: `json-parse-string` syntax check
- **Boolean fields**: must be `true`/`false`/`1`/`0`/`yes`/`no`
- **Required fields**: non-empty check at submit
- **Enum fields**: value must be in allowed set

Errors shown as:
- Red wave underline on the invalid value (`clutch-insert-field-error-face`)
- Inline error message below field (`clutch-insert-inline-error-face`)

### CAPF Completion

- **Enum fields**: `completing-read` with allowed values
- **FK fields**: completion from referenced table's values
- **Table names**: from schema cache

### Read-Only Prefix

Field names are read-only (`font-lock-face clutch-insert-field-name-face`, `read-only t`). Users cannot modify the field label. `post-command-hook` (`clutch-result-insert--normalize-point`) keeps cursor in the editable region.

---

## 14. Org-Babel Integration

### Supported Block Types

```org
#+begin_src mysql
  SELECT * FROM users LIMIT 10;
#+end_src

#+begin_src postgresql
  SELECT COUNT(*) FROM events;
#+end_src

#+begin_src sqlite
  SELECT name FROM sqlite_master WHERE type='table';
#+end_src

#+begin_src clutch :backend oracle
  SELECT * FROM USER_TABLES;
#+end_src

#+begin_src clutch :backend snowflake
  SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE();
#+end_src
```

### Header Arguments

| Argument | Description |
|----------|-------------|
| `:connection NAME` | Use named profile from `clutch-connection-alist`; this supplies the backend when using a saved connection |
| `:backend SYM` | Backend: `mysql`, `pg`, `postgresql`, `sqlite`, `oracle`, `sqlserver`, `snowflake`, `redshift`; required for inline params when `:connection` is absent |
| `:host HOST` | Database host (inline, without `:connection`) |
| `:port PORT` | Database port |
| `:user USER` | Database user |
| `:password PASS` | Password (not recommended; prefer `:pass-entry`) |
| `:database DB` | Database/schema name |
| `:pass-entry ENTRY` | Pass store entry for password resolution |
| `:results table` | Output format (`table` is default) |

### Connection Caching

Connections are cached in `ob-clutch--connection-cache` (hash-table keyed by `backend:params`):
- Reused across multiple blocks in the same session
- Liveness checked via `clutch-db-live-p` before reuse
- All connections disconnected on `kill-emacs-hook`

### Result Format

- **SELECT**: Org table with header row and `hline` separator
- **DML**: `"Affected rows: N"` string
- **Error**: error message as string

---

## 15. Export

### Supported Formats

| Format | MIME | Use Case |
|--------|------|----------|
| **CSV** | text/csv | Spreadsheets, Excel (UTF-8 BOM recommended) |
| **INSERT SQL** | text/plain | Replayable row inserts |
| **UPDATE SQL** | text/plain | Replayable row updates |

TSV remains available from the copy transient for cell/selection-oriented copy,
but full-buffer export currently targets CSV / INSERT / UPDATE only.

### CSV Encoding Options

Controlled by `clutch-csv-export-default-coding-system`:
- `utf-8-with-signature` (default) — UTF-8 BOM, best for Excel on Windows
- `utf-8` — Universal, no BOM
- `gbk` — Legacy CJK workflows (CP936)
- Any Emacs coding system

---

## 16. Generic Interface (cl-defgeneric Methods)

Current `clutch-db.el` generic surface, grouped by responsibility:

### Connection and transactions

| Method | Description |
|--------|-------------|
| `clutch-db-connect (backend params)` | Open a backend connection |
| `clutch-db-disconnect (conn)` | Close a connection |
| `clutch-db-live-p (conn)` | Check whether the connection is still live |
| `clutch-db-error-details (conn)` / `clutch-db-clear-error-details (conn)` | Structured backend error detail access |
| `clutch-db-init-connection (conn)` | Backend-specific post-connect initialization |
| `clutch-db-manual-commit-p (conn)` | Report whether the connection is in manual-commit mode |
| `clutch-db-commit (conn)` / `clutch-db-rollback (conn)` | Transaction control |
| `clutch-db-set-auto-commit (conn auto-commit)` | Toggle auto-commit |
| `clutch-db-interrupt-query (conn)` | Attempt recoverable interrupt for the active query |
| `clutch-db-busy-p (conn)` | Report whether the backend is busy |
| `clutch-db-user (conn)` / `clutch-db-host (conn)` / `clutch-db-port (conn)` / `clutch-db-database (conn)` / `clutch-db-display-name (conn)` | UI/identity accessors |

### Query execution and SQL helpers

| Method | Description |
|--------|-------------|
| `clutch-db-query (conn sql)` | Execute SQL and return a `clutch-db-result` |
| `clutch-db-execute-params (conn sql params)` | Execute parameterized SQL with bound values |
| `clutch-db-build-paged-sql (conn base-sql page-num page-size &optional order-by where-clause)` | Build paged SQL for a backend |
| `clutch-db-escape-identifier (conn name)` | Quote/escape identifiers |
| `clutch-db-escape-literal (conn value)` | Render literal SQL values for preview/fallback paths |

### Schema, completion, and metadata

| Method | Description |
|--------|-------------|
| `clutch-db-eager-schema-refresh-p (conn)` | Whether connect-time schema refresh stays synchronous |
| `clutch-db-completion-sync-columns-p (conn)` | Whether completion may still use synchronous column lookup |
| `clutch-db-refresh-schema-async (conn callback &optional errback)` | Defer schema snapshot refresh until Emacs is idle |
| `clutch-db-column-details-async (conn table callback &optional errback)` | Defer detailed column metadata load until Emacs is idle |
| `clutch-db-list-columns-async (conn table callback &optional errback)` | Defer column-name load until Emacs is idle |
| `clutch-db-table-comment-async (conn table callback &optional errback)` | Defer table-comment load until Emacs is idle |
| `clutch-db-list-objects-async (conn category callback &optional errback)` | Defer object warmup until Emacs is idle |
| `clutch-db-list-tables (conn)` / `clutch-db-list-schemas (conn)` | Enumerate tables and schemas/databases |
| `clutch-db-current-schema (conn)` / `clutch-db-set-current-schema (conn schema)` | Read/write current schema context |
| `clutch-db-list-table-entries (conn)` / `clutch-db-browseable-object-entries (conn)` | Return browseable table/object entries |
| `clutch-db-list-columns (conn table)` / `clutch-db-complete-columns (conn table prefix)` | Column metadata and completion |
| `clutch-db-complete-tables (conn prefix)` / `clutch-db-search-table-entries (conn prefix)` | Table completion and prefix search |
| `clutch-db-table-comment (conn table)` / `clutch-db-column-details (conn table)` | Synchronous table comment / detailed column metadata |
| `clutch-db-primary-key-columns (conn table)` / `clutch-db-foreign-keys (conn table)` / `clutch-db-referencing-objects (conn table)` | Relationship metadata |

### Object introspection

| Method | Description |
|--------|-------------|
| `clutch-db-show-create-table (conn table)` | Table DDL |
| `clutch-db-list-objects (conn category)` | Enumerate non-table objects by category |
| `clutch-db-object-details (conn entry)` | Rich object metadata |
| `clutch-db-object-source (conn entry)` | Source text for procedures/functions/triggers |
| `clutch-db-show-create-object (conn entry)` | DDL/source fallback for generic object definitions |

---

## 17. JDBC Agent Protocol

### Overview

The JDBC agent (`clutch-jdbc-agent.jar`) is a JVM sidecar process communicating via stdin/stdout with one JSON object per line.

### Request Format

```json
{"id":1,"op":"execute","params":{"conn-id":0,"sql":"SELECT 1"}}
```

### Response Format

```json
{"id":1,"ok":true,"result":{"cursor-id":0}}
{"id": 1, "ok": false, "error": "Unknown connection id: 5"}
```

### Supported Operations

| Op | Description |
|----|-------------|
| `connect` | Open JDBC connection (`auto-commit` optional), returns `conn-id` |
| `disconnect` | Close a connection |
| `commit` | Commit the current transaction on a connection |
| `rollback` | Roll back the current transaction on a connection |
| `set-auto-commit` | Toggle JDBC auto-commit |
| `set-current-schema` | Update the effective schema/database for both JDBC sessions |
| `cancel` | Cancel the currently running statement for a connection |
| `execute` | Execute SQL, returns `cursor-id` for SELECT |
| `fetch` | Fetch next batch from cursor, returns `rows`, `columns`, `done` |
| `get-schemas` | List available schemas/databases |
| `get-tables` | List schema/browser tables |
| `search-tables` | Prefix search for table/object entries |
| `get-columns` | List columns for table |
| `search-columns` | Prefix search for columns |
| `get-primary-keys` | List primary keys |
| `get-foreign-keys` | List foreign keys |
| `get-indexes` / `get-index-columns` | Index metadata |
| `get-sequences` / `get-procedures` / `get-functions` / `get-triggers` | Non-table object discovery |
| `get-procedure-params` / `get-function-params` | Routine parameter metadata |
| `get-object-source` / `get-object-ddl` | Source or DDL text |
| `get-referencing-objects` | Objects referencing a target table/object |

### Type Conversion (Java → JSON)

| Java Type | JSON Representation |
|-----------|---------------------|
| `null` | `null` |
| `Boolean` | `true` / `false` |
| `Integer`, `Long`, `Short`, `Byte` | number |
| `Double`, `Float` | number (NaN/Infinity → string) |
| `BigDecimal` | string (via `toPlainString()`) |
| `Timestamp` | ISO-8601 string |
| `Date` | ISO-8601 date string |
| `Time` | ISO-8601 time string |
| `Clob` | `{"__type":"clob","length":N,"preview":"..."}` (first 256 chars) |
| `Blob`, `byte[]` | `{"__type":"blob","length":N}` |
| Other | `rs.getString(col)` fallback |

### Driver Loading

1. Scan `drivers/` directory next to jar
2. `URLClassLoader` + `ServiceLoader<java.sql.Driver>` to discover drivers
3. Wrap in `DriverShim` (required for `DriverManager` acceptance from external classloader)
4. Register via `DriverManager.registerDriver()`
5. Log loaded driver class names to stderr

---

## 18. Known Limitations

### Open Issues (Confirmed, Not Yet Fixed)

| Issue | Severity | Description |
|-------|----------|-------------|
| SQL Server/DB2/Snowflake/Redshift coverage | Low | No live integration tests; behavior gaps may exist |

### Design Constraints

| Area | Limitation |
|------|-----------|
| **SQL rewriting** | ORDER BY/LIMIT/OFFSET injection uses top-level clause detection (regex); complex CTEs/UNIONs may rewrite incorrectly |
| **Mutations without PK** | Edit and delete are disabled when the result table has no primary key |
| **MySQL query timeout** | `clutch-query-timeout-seconds` is not enforced for MySQL (applied for PostgreSQL and JDBC only) |
| **Transaction control** | JDBC supports `commit`/`rollback`; Oracle defaults to manual-commit but can be flipped globally via `clutch-jdbc-oracle-manual-commit` or per connection via `:manual-commit` |
| **Prepared statements** | DML mutations use parameterized execution for native MySQL/PostgreSQL/SQLite backends; JDBC still falls back to literal SQL rendering |
| **CLOB/BLOB full content** | CLOBs show first 256 chars; BLOBs show length only; full streaming deferred |
| **Multiple result sets** | Stored procedures returning multiple result sets not supported |
| **Cancel/interrupt** | `C-g` is recoverable for JDBC and native PostgreSQL; backends without explicit interrupt support still fall back to disconnect/reconnect |

### Branch Features

- Deferred metadata loading now keeps MySQL/PostgreSQL schema and object warmup
  off the initial UI hot path via idle-time cache preheat on the main thread.
- Result buffers support per-table/per-column displayers through
  `clutch-register-column-displayer`.
- Result buffers can pipe the current cell through a shell command with `|`.

## 19. Development Guidelines

See `CLAUDE.md` in both repos for full rules. Key points:

- **Interface/implementation separation**: protocol layers never include UI; UI never imports protocol layers directly
- **Single file, single responsibility**: do not split files without a genuinely distinct responsibility
- **No side effects on load**: all behavior must be explicitly activated
- **Error conventions**: `user-error` for user problems, `error` for programmer bugs, `condition-case` for recovery
- **State**: `defvar-local` for per-buffer, `defcustom` for configurable, `defvar` for global/shared
- **Function size**: keep under ~30 lines; extract helpers by what they compute
- **Byte-compile clean**: `(byte-compile-file "clutch.el")` must produce zero warnings before any commit

### Postmortem Records

`postmortem/` directory contains design decision records (NNN-topic.md). Read before significant changes. Write when:
- Adding or changing a user-visible workflow
- Choosing between non-obvious architectural approaches
- Reverting or abandoning an approach

---

## 20. Entry Points

```elisp
;; Open a named query console
M-x clutch-query-console

;; Start interactive REPL
M-x clutch-repl

;; Connect (generic in clutch-mode/REPL; query consoles reconnect their own saved connection)
C-c C-e  (or  M-x clutch-connect)

;; Opt-in troubleshooting capture.
M-x clutch-debug-mode

;; Dedicated debug surface.  Enabling debug mode creates and resets this buffer.
*clutch-debug*

;; Object jump / describe / actions / schema switch
C-c C-j
C-c C-d
C-c C-o
C-c C-l

;; Org-Babel (install separate ob-clutch package, then add to init)
(require 'ob-clutch)
```

---

*Last updated: 2026-03-20*
