# 032 - MariaDB TLS and Authentication Matrix

## Context

`README.org` already documented that MariaDB 10.11 had been live-validated
through the native `mysql` backend, but that statement only covered the core
non-TLS workflow.

The remaining open questions were:

- whether MariaDB 10.11 also works over TLS through the same native backend
- whether the current client should claim support for MariaDB-specific older
  auth plugins

## What Was Verified

Live validation was run against a dedicated MariaDB 10.11 container with TLS
enabled and `require_secure_transport=ON`.

The native `mysql` backend successfully handled:

- `mysql_native_password` authentication over TLS
- query execution over TLS
- prepared statements over TLS

TLS hostname/certificate verification was also validated after regenerating the
local test certificate with a proper `subjectAltName` for `127.0.0.1`.

## Boundary That Matters

MariaDB 10.11 exposed these authentication plugins in the test environment:

- `mysql_native_password`
- `mysql_old_password`
- `unix_socket`

The current native client only implements:

- `mysql_native_password`
- `caching_sha2_password`

That means `mysql_old_password` is an explicit incompatibility boundary, not a
bug in the MariaDB compatibility claim. A live connection attempt against a
`mysql_old_password` user failed during authentication, which is the expected
result for the current implementation.

## Code Follow-Up

During this validation, an unrelated but real TLS bug surfaced: in batch Emacs
the TLS path could reach `gnutls-negotiate` without first loading `gnutls`.

The fix is simple and should stay symmetric across backends:

- `mysql.el` loads `gnutls` before upgrading to TLS
- `pg.el` now does the same

This keeps the runtime contract explicit instead of relying on the wider Emacs
session to have loaded `gnutls` already.

## Test Follow-Up

The existing MySQL TLS live tests were adjusted in two ways:

1. the admin connections used by the `caching_sha2_password` TLS test now also
   use TLS, so the test still works against servers that require secure
   transport
2. the `caching_sha2_password` TLS test now skips cleanly on servers that do
   not support that plugin, which includes the MariaDB 10.11 test environment

## Outcome

The documented support boundary is now sharper:

- MariaDB 10.11 is live-validated through the native `mysql` backend, including
  TLS with `mysql_native_password`
- `mysql_old_password` is not supported
- `caching_sha2_password` remains a MySQL-oriented auth path, not a MariaDB
  compatibility requirement
