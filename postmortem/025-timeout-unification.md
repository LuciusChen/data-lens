# 025 — Timeout Unification

## Background

Timeout handling had grown independently in three places:

- native MySQL protocol code
- native PostgreSQL protocol code
- the JDBC backend and its sidecar agent

That produced three kinds of drift:

1. same name, different meaning
2. different defaults at different layers
3. no clear separation between connect-time failures and query-time failures

The most obvious example was `:read-timeout`. In native MySQL/PG it meant
"idle time while waiting for more bytes from the server". In JDBC it was reused
for the agent RPC timeout and also mapped to JDBC network timeout. That made the
configuration surface hard to reason about and easy to misconfigure.

## Problems

### 1. Hidden multi-source defaults

MySQL and PostgreSQL structs carried their own timeout defaults, while
`clutch.el` also injected a separate top-level default. The effective value
depended on which layer touched the connection first.

This is exactly the kind of configuration drift users cannot debug from the UI.

### 2. Connect, read, query, and RPC were mixed together

These are separate failure modes:

- failing to establish a TCP/JDBC connection
- waiting too long for network I/O after a query has started
- a database statement that keeps running too long
- the Emacs ↔ agent RPC itself hanging

Treating them as one knob made the behavior inconsistent across backends.

### 3. JDBC could not stay "conceptually aligned" with native backends

Even if native MySQL/PG were cleaned up, JDBC would still remain confusing if:

- Elisp timed out waiting for the agent
- but the agent had no connect/login timeout
- or the agent had no statement timeout

The timeout model had to be coherent across the Emacs side and the JVM side.

## Decision

Adopt four explicit timeout concepts:

- `connect-timeout`
- `read-idle-timeout`
- `query-timeout`
- `rpc-timeout`

And expose them consistently in two layers:

- global defaults via `defcustom`
- per-connection overrides via connection plist keys

## Why these four

### `connect-timeout`

This is the timeout users expect when they say "connection timed out". It covers
the initial connection establishment path. It must not silently mean "query
read timeout", because that leads to misleading failures and user confusion.

### `read-idle-timeout`

Native protocol backends need a concept for "query started, but no more bytes
arrived for too long". That is not a connect timeout, and it is not the same as
database-side statement timeout.

The previous `read-timeout` behavior was really this concept, so the new name
states the actual semantics instead of the transport detail.

### `query-timeout`

This is a database-side statement deadline, mainly relevant to JDBC today.
It belongs in the public API even if native MySQL/PG do not yet implement it,
because the concept is real and users need one consistent place to configure it.

### `rpc-timeout`

The JDBC backend has one extra hop that native backends do not have:
Emacs waits for a JSON response from the sidecar process. That timeout is real
and should be explicit instead of being smuggled through another field.

## Why no alias for `:read-timeout`

The old name was not just old; it was semantically wrong and overloaded.

Keeping an alias would preserve the ambiguity:

- some users would keep using the old name
- code and docs would need to explain both names
- the project would continue carrying the old mental model

Because the package is still early and the number of users is small, a clean
break is cheaper than carrying dual vocabulary indefinitely.

The right trade-off here is fail fast, not compatibility padding.

## Why centralize defaults in `clutch.el`

`clutch.el` is the public configuration surface. Users should not need to know
that a lower protocol struct also contains a default.

Placing the authoritative defaults in `clutch.el` gives:

- one source of truth
- one place to document them
- predictable override rules

Protocol-layer struct defaults may still exist as defensive values, but they
must not act as a second public configuration system.

## JDBC-side consequence

Timeout unification could not stop at Elisp names. The JVM side also had to
understand the split explicitly:

- connect/login timeout on connection open
- network timeout on the JDBC `Connection`
- statement timeout on `Statement.execute`

Otherwise the Elisp API would claim semantics that the agent did not actually
enforce. That would be worse than the original inconsistency because the public
surface would look clean while the implementation remained partial.

## Alternatives considered

### 1. Keep `:read-timeout` and only improve docs

Rejected. The problem was not documentation alone; the semantics were genuinely
mixed across backends.

### 2. Keep one generic `:timeout` field and map it differently per backend

Rejected. This would simplify syntax while hiding the real model, which is the
opposite of what a database client should do in failure handling.

### 3. Add aliases and deprecate gradually

Rejected. For a mature package with a large install base this can be justified.
Here it would mostly preserve confusion and extend the migration window.

### 4. Fix native backends only and defer JDBC

Rejected. JDBC was already one of the main sources of timeout ambiguity, and
Oracle had already shown that missing timeout layers create real hangs.

## Accepted limitations

- Native MySQL/PostgreSQL now distinguish connect timeout and read-idle timeout,
  but still do not implement a true database-side `query-timeout`.
- Updating the Elisp-side API is not sufficient by itself; users need a newer
  `clutch-jdbc-agent` release before the JDBC semantics are fully available in
  packaged use.

## Lessons

- Timeout bugs are usually modeling bugs first, implementation bugs second.
- If a configuration name needs a paragraph to explain its meaning, the name is
  probably wrong.
- For multi-process systems, configuration refactors must be end-to-end. A clean
  API on one side is not enough if the other side still implements the old model.
