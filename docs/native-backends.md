# Native Backends

`clutch` ships three non-JDBC backends that do not require a sidecar process:

- `mysql.el` — pure Emacs Lisp MySQL wire protocol client
- `pg.el` — pure Emacs Lisp PostgreSQL wire protocol v3 client
- `clutch-db-sqlite.el` — SQLite adapter over Emacs 29+ built-in `sqlite-*`

Use this document for backend-specific connection, protocol, TLS, timeout, and
usage notes for the native backends.  The JDBC sidecar has its own document in
[`docs/jdbc-agent-protocol.md`](./jdbc-agent-protocol.md).

## MySQL (`mysql.el`)

### Scope

- Pure Elisp implementation; no external CLI or client library
- HandshakeV10 + HandshakeResponse41
- `mysql_native_password` and `caching_sha2_password`
- `COM_QUERY` text protocol
- Prepared statements via `COM_STMT_PREPARE` / `COM_STMT_EXECUTE`
- TLS via Emacs GnuTLS

### Connection Example

```elisp
(require 'mysql)

(setq conn (mysql-connect :host "127.0.0.1"
                          :port 3306
                          :user "root"
                          :password "secret"
                          :database "mydb"))

(let ((result (mysql-query conn "SELECT * FROM users LIMIT 10")))
  (mysql-result-columns result)
  (mysql-result-rows result))

(mysql-disconnect conn)
```

### TLS

When `:tls t` is used, certificate and hostname verification are enabled by
default.

Relevant variables:

- `mysql-tls-trustfiles`
- `mysql-tls-verify-server`
- `mysql-tls-keylist`

For local MySQL 8 containers using `caching_sha2_password`, clutch may need TLS
for authentication.  For self-signed local dev certificates, either trust the
CA or set `mysql-tls-verify-server` to `nil` explicitly.

### Convenience API

- `with-mysql-connection`
- `with-mysql-transaction`
- `mysql-ping`
- `mysql-escape-identifier`
- `mysql-escape-literal`
- `mysql-connect/uri`

## PostgreSQL (`pg.el`)

### Scope

- Pure Elisp PostgreSQL protocol v3
- SCRAM-SHA-256 (SASL) and MD5 authentication
- TLS via Emacs GnuTLS
- OID-based type decoding for scalar, temporal, JSON, and `bytea` values

### Connection Example

```elisp
(require 'pg)

(setq conn (pg-connect :host "127.0.0.1"
                       :port 5432
                       :user "postgres"
                       :password "secret"
                       :database "mydb"))

(let ((result (pg-query conn "SELECT * FROM users LIMIT 10")))
  (pg-result-columns result)
  (pg-result-rows result))

(pg-disconnect conn)
```

### TLS

When `:tls t` is used, certificate and hostname verification are enabled by
default.

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
- Completion remains statement-scoped where possible
- Large-schema column loading stays conservative to avoid blocking completion

### UI Layer

The interactive object workflow is shared across native and JDBC backends:

- `C-c C-j` — jump to object
- `C-c C-d` — describe object
- `C-c C-o` — object actions
- `C-c C-l` — switch current schema/database when supported
