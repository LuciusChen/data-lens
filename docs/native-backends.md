# Native Backends

`clutch` ships three non-JDBC backends that do not require a sidecar process:

- `mysql` — external pure Emacs Lisp MySQL wire protocol client
- `pg` — external PostgreSQL client from `pg-el`
- `clutch-db-sqlite.el` — SQLite adapter over Emacs 29.1+ built-in `sqlite-*`

Use this document for backend-specific connection, protocol, TLS, timeout, and
usage notes for the native backends.  The JDBC sidecar has its own document in
[`docs/jdbc-agent-protocol.md`](./jdbc-agent-protocol.md).

## Live Testing

Live tests use ordinary host/port connection variables, so any local database
or Docker-compatible runtime works.  The examples below use Docker only as a
portable way to start test servers.

```sh
docker run --name clutch-mysql-80 -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8.0
docker run --name clutch-pg-16 -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
```

Run the standalone MySQL protocol live suite from the `mysql.el` checkout:

```sh
emacs -Q --batch -L . -l ert -l test/mysql-test.el \
  --eval '(setq mysql-test-password "test")' \
  --eval "(ert-run-tests-batch-and-exit '(tag :mysql-live))"
```

Run clutch native adapter live tests for MySQL and PostgreSQL:

```sh
emacs -Q --batch -L . -L ../mysql.el -L ../pg-el -l ert -l test/clutch-db-test.el \
  --eval '(setq clutch-db-test-mysql-password "test")' \
  --eval '(setq clutch-db-test-pg-password "test")' \
  --eval "(ert-run-tests-batch-and-exit '(tag :db-live))"
```

Current native MySQL validation targets are MySQL 5.6, 8.0, 8.4 LTS, and
MariaDB 10.11.  Re-run MySQL 8.0/8.4 TLS auth tests after touching handshake,
capability, or password-plugin code.

## SSH Tunnels

For saved clutch connections, `:ssh-host` starts a local SSH forward through
the named host in `~/.ssh/config` before the native backend connects.  The
database `:host` / `:port` remain the remote endpoint as seen from the bastion
host; clutch rewrites the live socket to `127.0.0.1:LOCAL-PORT` internally.

This SSH path is intentionally OpenSSH-first:

- use your normal `~/.ssh/config` host aliases
- let `ssh-agent` or the OpenSSH client handle SSH credentials
- keep database passwords in `auth-source`

If the first SSH use still needs a passphrase or host-key confirmation, run
`M-x clutch-prepare-ssh-host` for that alias once, or press `S` from
`clutch-dispatch`. After that, `clutch-connect` goes back to the normal batch
tunnel path.

The batch tunnel uses `ssh -o BatchMode=yes`, so it cannot prompt. If
interactive `ssh HOST` succeeds but clutch still reports SSH authentication
failure, configure OpenSSH to authenticate non-interactively too, usually by
loading the key into `ssh-agent` or enabling `AddKeysToAgent`.

## MySQL (`mysql`)

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
default.  For MySQL, explicit `:ssl-mode disabled` is the canonical plaintext
opt-out; `:tls nil` remains a shorthand.

For MySQL only, `:ssl-mode disabled` is a compatibility spelling for that same
plaintext mode.  The older alias `off` is still accepted.  Either spelling
disables the automatic MySQL 8 TLS reconnect path.

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
- `mysql-autocommit-p`
- `mysql-in-transaction-p`
- `mysql-set-autocommit`
- `mysql-commit`
- `mysql-rollback`
- `mysql-ping`
- `mysql-escape-identifier`
- `mysql-escape-literal`
- `mysql-connect-uri`

### Transaction Control in clutch

- `C-c C-a` toggles session autocommit with `SET autocommit = 0/1`
- `C-c C-m` issues `COMMIT`
- `C-c C-u` issues `ROLLBACK`
- clutch header-line state (`Tx: Auto` / `Tx: Manual` / `Tx: Manual*`) follows
  those session semantics

### Prepared Statements

```elisp
(let ((stmt (mysql-prepare conn "SELECT * FROM users WHERE id = ?")))
  (let ((result (mysql-execute stmt 42)))
    (mysql-result-rows result))
  (mysql-stmt-close stmt))
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

### Transaction Control in clutch

- PostgreSQL does not expose a session autocommit toggle like MySQL
- In clutch, `C-c C-a` enables a clutch-managed manual mode
- Manual mode uses lazy `BEGIN`: the first foreground statement opens the
  transaction
- `C-c C-m` issues `COMMIT`; `C-c C-u` issues `ROLLBACK`
- Transactional DDL also counts as uncommitted work, so `Tx: Manual*` remains
  accurate for native PostgreSQL

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

- Uses Emacs 29.1+ built-in `sqlite-*` functions
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
- MySQL and PostgreSQL now defer the initial schema snapshot until Emacs is
  idle after connect/reconnect; SQLite keeps its synchronous in-process path
- Schema/database switch prompts remain synchronous, but the post-switch schema
  snapshot refresh runs as deferred idle work
- Completion and Eldoc remain statement-scoped where possible, but native
  MySQL/PostgreSQL hot paths are now cache-first and schedule idle-time
  metadata preheat on cache miss instead of blocking point motion or CAPF
- Result buffers render first and then opportunistically enrich cached column
  details in the background; explicit detail commands still load synchronously
- Object warmup keeps non-table categories off the first-open path and fills
  them lazily during idle time
- Native MySQL/PostgreSQL deferred metadata now stays on the Emacs main thread
  via idle callbacks rather than using worker threads
- Native MySQL and PostgreSQL both support clutch transaction toggling (`C-c C-a`
  / `C-c C-m` / `C-c C-u`), while SQLite still has no native runtime
  auto-commit toggle

### Parameterized DML

- clutch preview buffers still show fully rendered SQL text for readability
- Native MySQL/PostgreSQL/SQLite staged `INSERT` / `UPDATE` / `DELETE` execute
  through backend parameter binding instead of literal SQL interpolation
- Staged `UPDATE` / `DELETE` use row identity metadata rather than requiring a
  primary key in every table: primary keys are preferred, non-null unique keys
  are used next, PostgreSQL can fall back to `ctid`, and SQLite rowid tables
  can fall back to `rowid`
- Physical row locators such as PostgreSQL `ctid` and SQLite `rowid` identify
  the current row version only.  They may change after `UPDATE`, and result
  refresh ordering remains defined only by the query's explicit `ORDER BY`
- JDBC currently keeps its literal-SQL fallback until the sidecar grows a
  prepared-execute operation

### UI Layer

The interactive object workflow is shared across native and JDBC backends:

- `C-c C-j` — jump to object
- `C-c C-d` — describe object at point, or prompt with the shared object picker
- `C-c C-o` — object actions
- `C-c C-l` — switch current schema/database when supported
