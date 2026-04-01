# 069 - MySQL plaintext controls

## Context

The native MySQL client already had two TLS knobs:

- `:tls t` on a connection to opt into TLS up front
- `mysql-tls-verify-server` to relax certificate/hostname verification

That still left one missing workflow.  Since postmortem 038, a plain MySQL 8
connection may auto-retry once over TLS when `caching_sha2_password` full auth
demands a secure channel.  Users who explicitly want plaintext had no
connection-level way to say "do not use TLS at all".

## Decision

Keep a single semantic for "force plaintext" and expose it in two ways:

- `:tls nil`
- `:ssl-mode disabled`

The older alias `:ssl-mode off` remains accepted for compatibility.

This mode does two things:

- forces plaintext instead of the default auto-upgrade path
- suppresses the automatic MySQL 8 TLS retry path

Conflicting configuration such as `:tls t` together with `:ssl-mode disabled`
now signals an error instead of silently choosing one side.

## Why keep `:ssl-mode`

The existing client does not yet model the full MySQL SSL mode matrix
(`required`, `verify_ca`, `verify_identity`, and so on) as first-class
connection semantics.  Adding the whole matrix now would expand the surface
area far beyond the immediate user need.

The real gap was smaller: there was no explicit opt-out once auto-TLS retry was
added.  `:tls nil` closes that gap in clutch's generic API, while
`:ssl-mode disabled` keeps a MySQL-shaped compatibility spelling for users who
expect that knob from upstream tooling.

## Boundary kept explicit

This plaintext mode is not a "make broken certs work" switch.  It is a strict
opt-out from TLS.  If a MySQL 8 server actually requires a secure channel for
`caching_sha2_password`, a connection with `:tls nil` or `:ssl-mode disabled`
should fail rather than silently downgrade or relax verification.
