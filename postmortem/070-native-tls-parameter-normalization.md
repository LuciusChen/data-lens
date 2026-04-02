# 070 - Native TLS parameter normalization

## Context

`clutch` had grown two different configuration styles for transport security:

- a generic `:tls` boolean shortcut
- backend-specific names such as MySQL `:ssl-mode`

That worked for narrow cases, but it blurred the public API.  The official
database docs use backend-native names like MySQL `ssl-mode` and PostgreSQL
`sslmode`, while `:tls` in clutch was starting to carry backend-specific
meaning such as "force plaintext" on MySQL.

## Decision

Keep both layers, but give them clear roles:

- backend-native names are the canonical public forms
- `:tls` remains a convenience shorthand

Concretely:

- MySQL canonical plaintext opt-out is `:ssl-mode disabled`
- PostgreSQL canonical transport setting is `:sslmode`
- `:tls t` / `:tls nil` are normalized into those backend-native settings
  where clutch can do so faithfully

Inside the backend facade, those public forms are reduced again to a private
canonical transport mode so the wrappers do not have to branch directly on
every user-facing alias.

## Why Not Introduce `:tls-mode`

A cross-backend enum such as `:tls-mode require/disable/...` looks tidy
internally, but it is not what users see in upstream client docs.  That adds
translation cost right at configuration time.

Using official names at the plist boundary keeps migration easier:

- MySQL users can recognize `ssl-mode`
- PostgreSQL users can recognize `sslmode`
- JDBC backends can continue using driver-native `:props`

## Boundary Kept Explicit

This does not promise that every backend has one universal TLS surface.
`clutch` only normalizes the subset it can model honestly:

- MySQL: `disabled` plus the existing TLS shortcut
- PostgreSQL: `disable`, `prefer`, `require`, `verify-full`

Modes that clutch cannot currently implement faithfully, such as PostgreSQL
`allow` or `verify-ca`, should fail early rather than silently degrading into a
different transport policy.
