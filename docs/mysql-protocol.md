# MySQL Protocol Layer

`clutch-mysql.el` is the bundled MySQL wire protocol client used by
`clutch-db-mysql.el`.  It is intentionally maintained like a small standalone
library, but with a `clutch-mysql-` prefix while it remains inside `clutch`.

The split boundary is:

- `clutch-mysql.el`: sockets, packets, authentication, TLS, query execution,
  prepared statements, type decoding, and transaction status helpers.
- `clutch-db-mysql.el`: adapter from `clutch-db-*` generics to the protocol
  layer, schema SQL, object metadata, escaping, paging, and `clutch` errors.

## Naming

- Public protocol symbols use `clutch-mysql-*`.
- Private protocol helpers use `clutch-mysql--*`.
- Adapter-only helpers keep the `clutch-db-mysql-*` prefix.

If this layer becomes a separate package, the intended rename is mostly
mechanical: `clutch-mysql-*` to `mysql-*` and `clutch-mysql--*` to `mysql--*`.

## API

### Connections

    (clutch-mysql-connect &key host port user password database tls ssl-mode
                               read-idle-timeout connect-timeout) -> conn

Connect to a MySQL-family server.  `HOST` defaults to `127.0.0.1` and `PORT`
defaults to `3306`.  `USER` is required.  `DATABASE` is optional.

TLS can be requested with `:tls t`.  Plaintext can be forced with
`:ssl-mode 'disabled` or explicit `:tls nil`.  In default mode, a
`caching_sha2_password` full-auth failure retries over TLS when GnuTLS is
available.

    (clutch-mysql-disconnect conn) -> nil
    (clutch-mysql-ping conn) -> t
    (clutch-mysql-connect-uri uri) -> conn

### Query Execution

    (clutch-mysql-query conn sql) -> result

Execute a text-protocol `COM_QUERY` statement and return a
`clutch-mysql-result`.

Result accessors:

    (clutch-mysql-result-status result)
    (clutch-mysql-result-columns result)
    (clutch-mysql-result-rows result)
    (clutch-mysql-result-affected-rows result)
    (clutch-mysql-result-last-insert-id result)
    (clutch-mysql-result-warnings result)

### Prepared Statements

    (clutch-mysql-prepare conn sql) -> stmt
    (clutch-mysql-execute stmt &rest params) -> result
    (clutch-mysql-stmt-close stmt) -> nil

Prepared statements use `COM_STMT_PREPARE` and `COM_STMT_EXECUTE`.

### Transactions

    (clutch-mysql-autocommit-p conn) -> boolean
    (clutch-mysql-in-transaction-p conn) -> boolean
    (clutch-mysql-set-autocommit conn enabled) -> result
    (clutch-mysql-commit conn) -> result
    (clutch-mysql-rollback conn) -> result

Transaction state is read from MySQL server status flags reported in OK/EOF
packets.  `clutch-db-mysql.el` maps these helpers to the generic transaction
controls shown by the `clutch` UI.

### Escaping

    (clutch-mysql-escape-identifier name) -> string
    (clutch-mysql-escape-literal value) -> string

These helpers are used by the adapter for identifier and string-literal
generation.

### TLS Variables

    clutch-mysql-tls-trustfiles
    clutch-mysql-tls-keylist
    clutch-mysql-tls-verify-server

These map directly to Emacs GnuTLS connection options.

## Protocol Scope

Implemented:

- HandshakeV10 and HandshakeResponse41
- `mysql_native_password`
- `caching_sha2_password` fast auth
- `caching_sha2_password` full auth over TLS
- `COM_QUERY`
- text result sets
- `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, and `COM_STMT_CLOSE`
- binary result rows for prepared statements
- OK/ERR/EOF packet parsing
- connection status flags for autocommit and transaction state
- TLS upgrade through Emacs GnuTLS

Known gaps:

- `caching_sha2_password` full auth without TLS is not implemented because it
  requires RSA public-key exchange.
- `sha256_password` is not implemented.
- multi-factor authentication packets are not implemented.
- compression, zstd compression, query attributes, connection attributes,
  session tracking, optional result-set metadata, multi-statements, and
  multi-results are not advertised or implemented.
- `LOCAL INFILE` is detected but not implemented.

Most of these gaps are optional MySQL capability paths.  The client should not
receive optional packet formats that it did not advertise during capability
negotiation.

## Compatibility Notes

| Server family | Expected status | Notes |
| --- | --- | --- |
| MySQL 5.6 | Supported for core query/edit workflows | No native JSON type. |
| MySQL 8.0 | Supported for core query/edit workflows | Default `caching_sha2_password` works with cached auth or TLS full auth. |
| MySQL 8.4 LTS | Live-validated for core workflows | Keep regular coverage, especially auth/TLS paths. |
| MySQL 9.x | Expected compatible for negotiated core protocol | Treat as a watch target for new auth/capability behavior. |
| MariaDB 10.11 | Live-validated for core workflows | Uses MySQL-compatible protocol; JSON is text-based in MariaDB. |

When changing this layer, update this document, `CHANGELOG.md`, and the focused
tests in `test/clutch-mysql-test.el`.
