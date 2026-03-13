# 038 - MySQL 8 caching_sha2 auto TLS retry

## Background

Official MySQL 8 defaults to `caching_sha2_password`.  On an insecure TCP
connection, the server may accept the fast-auth path, but when full
authentication is required it rejects the client unless the exchange moves onto
TLS or uses RSA public-key encryption.

`mysql.el` already supported the TLS path when callers opted into `:tls t`, but
the default non-TLS connection flow failed with:

- `caching_sha2_password full authentication requires TLS`

That left an awkward gap:

- MariaDB and older MySQL installations worked without extra knobs
- official MySQL 8 local/dev containers looked "broken"
- the codebase did not yet have a pure Elisp RSA public-key fallback

## Decision

When `mysql.el` sees the specific `caching_sha2_password` full-auth failure on a
non-TLS connection, it now retries the entire connection once with `:tls t`.

This is deliberately narrow:

- only that exact auth failure triggers the retry
- only the initial non-TLS attempt is retried
- verification behavior is unchanged

If TLS verification fails, the connection still fails.  Users must explicitly
configure `mysql-tls-trustfiles` or set `mysql-tls-verify-server` to `nil` for
local/self-signed development servers.

## Why this approach

Two options existed:

1. implement MySQL's RSA public-key retrieval and password encryption path
2. retry over TLS, which the client already supports

The TLS retry was chosen because it:

- reuses an existing, tested secure transport path
- avoids introducing external OpenSSL dependencies
- keeps the protocol logic small and debuggable
- matches the practical expectation for modern MySQL 8 servers

## Alternatives not chosen

### Add RSA public-key auth now

This would preserve plaintext TCP while satisfying MySQL 8 full auth, but it
would require new crypto machinery in Emacs Lisp or an external dependency.
That is a larger protocol surface than the current project needs.

### Silently disable TLS verification during retry

This would make local self-signed containers "just work", but it would also
change the security meaning of a normal connection attempt in a non-obvious way.
The project kept verification semantics explicit instead.

## Testing impact

Live test helpers for local MySQL integration now bind
`mysql-tls-verify-server` to `nil`, because the containerized local/dev matrix
uses self-signed certificates.  That matches the documented local/dev guidance
and lets the auto-retry path be exercised end-to-end.
