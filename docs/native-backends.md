# Native Backends

`clutch` ships three non-JDBC backends that do not require a sidecar process:

- `mysql-wire` — external pure Emacs Lisp MySQL wire protocol client
- `pg` — external PostgreSQL client from `pg-el`
- `clutch-db-sqlite.el` — SQLite adapter over Emacs 29+ built-in `sqlite-*`

Use this document for backend-specific connection, protocol, TLS, timeout, and
usage notes for the native backends.  The JDBC sidecar has its own document in
[`docs/jdbc-agent-protocol.md`](./jdbc-agent-protocol.md).

## MySQL (`mysql-wire`)

### Scope

- Pure Elisp implementation; no external CLI or client library
- HandshakeV10 + HandshakeResponse41
- `mysql_native_password` and `caching_sha2_password`
- `COM_QUERY` text protocol
- Prepared statements via `COM_STMT_PREPARE` / `COM_STMT_EXECUTE`
- TLS via Emacs GnuTLS

### Connection Example

```elisp
(require 'mysql-wire)

(setq conn (mysql-wire-connect :host "127.0.0.1"
                               :port 3306
                               :user "root"
                               :password "secret"
                               :database "mydb"))

(let ((result (mysql-wire-query conn "SELECT * FROM users LIMIT 10")))
  (mysql-wire-result-columns result)
  (mysql-wire-result-rows result))

(mysql-wire-disconnect conn)
```

### TLS

When `:tls t` is used, certificate and hostname verification are enabled by
default.  For MySQL, explicit `:ssl-mode disabled` is the canonical plaintext
opt-out; `:tls nil` remains a shorthand.

For MySQL only, `:ssl-mode disabled` is a compatibility spelling for that same
plaintext mode.  The older alias `off` is still accepted.  Either spelling
disables the automatic MySQL 8 TLS reconnect path.

Relevant variables:

- `mysql-wire-tls-trustfiles`
- `mysql-wire-tls-verify-server`
- `mysql-wire-tls-keylist`

For local MySQL 8 containers using `caching_sha2_password`, clutch may need TLS
for authentication.  For self-signed local dev certificates, either trust the
CA or set `mysql-tls-verify-server` to `nil` explicitly.

### Convenience API

- `with-mysql-wire-connection`
- `with-mysql-wire-transaction`
- `mysql-wire-ping`
- `mysql-wire-escape-identifier`
- `mysql-wire-escape-literal`
- `mysql-wire-connect-uri`

### Prepared Statements

```elisp
(let ((stmt (mysql-wire-prepare conn "SELECT * FROM users WHERE id = ?")))
  (let ((result (mysql-wire-execute stmt 42)))
    (mysql-wire-result-rows result))
  (mysql-wire-stmt-close stmt))
```

## PostgreSQL (`pg`)

### Scope

- Pure Elisp PostgreSQL protocol v3
- SCRAM-SHA-256 (SASL) and MD5 authentication
- TLS via Emacs GnuTLS
- OID-based type decoding for scalar, temporal, JSON, and `bytea` values

### Connection Example

```elisp
(require 'pg)

(setq conn (pg-connect-plist "mydb" "postgres"
                             :password "secret"
                             :host "127.0.0.1"
                             :port 5432))

(let ((result (pg-exec conn "SELECT * FROM users LIMIT 10")))
  (pg-result result :attributes)
  (pg-result result :tuples))

(pg-disconnect conn)
```

### TLS

PostgreSQL accepts the upstream `:sslmode` name with `disable`, `prefer`,
`require`, and `verify-full`.  `:tls t` and `:tls nil` remain convenience
shorthands for `require` and `disable`.

When `:sslmode require` or `:sslmode verify-full` is used, certificate and
hostname verification are enabled by default through `pg-tls-verify-server`.
When `:sslmode prefer` is used, clutch attempts TLS first and falls back to
plaintext if the server declines SSL or GnuTLS is unavailable.

Relevant variables:

- `pg-tls-trustfiles`
- `pg-tls-verify-server`
- `pg-tls-keylist`

### Convenience API

- `with-pg-connection`
- `with-pg-transaction`
- `pg-ping`
- `pg-escape-identifier`
- `pg-escape-literal`

### Transaction Example

```elisp
(with-pg-transaction conn
  (pg-exec conn "INSERT INTO users (name) VALUES ('alice')")
  (pg-exec conn "INSERT INTO users (name) VALUES ('bob')"))
```

### Interrupts and Timeouts

- `:query-timeout` maps to PostgreSQL `statement_timeout`.
- `pg-cancel` sends a wire-protocol `CancelRequest` on an auxiliary
  socket, then drains the main connection until `ReadyForQuery`.
- In clutch UI terms, `C-g` on native PostgreSQL uses that path, so a cancelled
  query keeps the same session usable for the next SQL.
- Native MySQL and SQLite do not currently provide the same recoverable
  interrupt path; clutch falls back to disconnect/reconnect semantics there.

## SQLite

### Scope

- Uses Emacs 29+ built-in `sqlite-*` functions
- No network stack, no TLS, no sidecar process
- Connection identity is the database file path

### Connection Example

```elisp
(setq clutch-connection-alist
      '(("dev-sqlite" . (:backend sqlite
                          :database "/tmp/demo.db"))))
```

### Notes

- SQLite does not use `:host`, `:port`, or `:user`
- Network timeout settings do not apply
- Schema/database switching is not part of the SQLite path

## Shared Native-Backend Notes

### Timeouts

- MySQL supports `:connect-timeout` and `:read-idle-timeout`
- PostgreSQL supports `:connect-timeout`, `:read-idle-timeout`, and `:query-timeout`
- SQLite does not use the network timeout parameters

### Completion and Schema Refresh

- Native backends integrate directly with clutch schema refresh and completion
- MySQL and PostgreSQL now refresh the initial schema snapshot in the background
  after connect/reconnect; SQLite keeps its synchronous in-process path
- Schema/database switch prompts remain synchronous, but the post-switch schema
  snapshot refresh runs in the background
- Completion and Eldoc remain statement-scoped where possible, but native
  MySQL/PostgreSQL hot paths are now cache-first and queue background metadata
  preheat on cache miss instead of blocking point motion or CAPF
- Result buffers render first and then opportunistically enrich cached column
  details in the background; explicit detail commands still load synchronously
- Object warmup keeps non-table categories off the first-open path and fills
  them lazily during idle time
- If Emacs lacks native thread primitives, clutch falls back to the older
  synchronous native metadata path rather than disabling metadata loading

### UI Layer

The interactive object workflow is shared across native and JDBC backends:

- `C-c C-j` — jump to object
- `C-c C-d` — describe object at point, or prompt with the shared object picker
- `C-c C-o` — object actions
- `C-c C-l` — switch current schema/database when supported
