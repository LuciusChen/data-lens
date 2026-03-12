# 026 — Connect Timeout Default and Socket Compatibility

## Background

The timeout unification work in `025-timeout-unification.md` split timeout
semantics into four explicit concepts:

- `connect-timeout`
- `read-idle-timeout`
- `query-timeout`
- `rpc-timeout`

That clarified the API, but follow-up validation exposed two remaining issues:

1. the default `connect-timeout` was still effectively too conservative for an
   interactive SQL console
2. the native MySQL and PostgreSQL protocol layers forced
   `make-network-process` to use `:type 'plain`, which fails on some Emacs
   builds with `Unsupported connection type`

These were not theoretical concerns. They appeared during live regression runs
on the actual development machine.

## Problems

### 1. `connect-timeout` still behaved like a "large generic timeout"

Even after timeout concepts were separated, the default stayed at `30s`.
That undermined the whole point of the split:

- connection failures still took too long to surface
- the console felt stalled on bad hosts or refused ports
- `connect-timeout` was still acting like a conservative catch-all value rather
  than a specific user-facing deadline

This was especially mismatched with the actual usage pattern of clutch, which is
interactive and latency-sensitive.

### 2. JDBC direct backend entry still had partial fallback drift

`clutch--build-conn` normalized timeout defaults centrally, but
`clutch-db-jdbc-connect` still had its own fallback path. That meant direct use
of the JDBC backend could still inherit `connect-timeout` from `rpc-timeout`,
which is precisely the ambiguity the timeout refactor was meant to remove.

That was a smaller surface than the main UI flow, but it was still a real
consistency hole.

### 3. Native socket setup was not portable across Emacs builds

The native MySQL and PostgreSQL clients opened sockets with:

- `:nowait t`
- `:coding 'binary`
- `:type 'plain`

On the tested Emacs 31.0.50 build, that explicit `:type 'plain` caused
`make-network-process` to fail immediately with `Unsupported connection type`.

Removing `:type` while keeping the binary coding settings worked correctly.
So the issue was not network reachability, database startup, or protocol logic;
it was the process creation arguments themselves.

## Decision

Make three small, explicit follow-up corrections:

1. reduce the default `clutch-connect-timeout-seconds` from `30` to `10`
2. make `clutch-db-jdbc-connect` self-consistent when used directly, instead of
   relying on top-level normalization having already happened
3. stop forcing `:type 'plain` in `mysql.el` and `pg.el`

## Why `10s`

`10s` is short enough to feel interactive and long enough to tolerate ordinary
network jitter, VPN delay, or a slightly slow remote endpoint.

It was chosen over more aggressive values like `5s` because:

- clutch is often used against remote databases, not just localhost
- connection setup sometimes includes TLS negotiation and authentication work
- a too-small default would create false negatives in normal enterprise setups

It was chosen over keeping `30s` because:

- `connect-timeout` is not a query deadline
- users should get fast failure on bad connection parameters
- the other timeout classes already cover slower phases more appropriately

## Why fix direct JDBC fallback locally

The timeout model must hold at every public entry point, not just along the most
common UI path.

If the JDBC backend can be called directly and still mixes `connect-timeout`
with `rpc-timeout`, then the system is not actually unified; it only appears
that way from one layer up.

The correct trade-off here is to let `clutch-db-jdbc.el` stay self-consistent,
even if the main UI already normalizes parameters.

## Why remove `:type 'plain`

There was no project-specific reason to insist on that process type.
What the protocol layers actually require is:

- a TCP socket
- binary-safe I/O
- the existing process filter and timeout logic

Those requirements are satisfied without `:type 'plain`.

Keeping an explicit process type that breaks on some Emacs builds would be the
wrong kind of specificity: it narrows compatibility without buying correctness.

## Alternatives considered

### 1. Keep `connect-timeout` at `30s`

Rejected. That would preserve the user-visible lag that motivated the follow-up.
Once timeout semantics are explicit, the connect phase should behave like a
connect phase, not like a generic "be patient" knob.

### 2. Lower all timeout defaults together

Rejected. `read-idle-timeout`, `query-timeout`, and `rpc-timeout` protect
different phases with different expectations. They should not be tightened just
because `connect-timeout` was too large.

### 3. Keep `:type 'plain` and special-case the affected Emacs build

Rejected. That would turn a one-line compatibility fix into version- or
platform-specific branching with no strong justification.

### 4. Rely only on top-level timeout normalization

Rejected. It would leave a hidden correctness dependency between modules and
make direct backend use behave differently from the main connection flow.

## Known limitations

- Native MySQL and PostgreSQL still do not implement a true database-side
  statement timeout; that limitation remains documented in
  `025-timeout-unification.md`.
- TLS live tests still depend on container/server setup and are not universally
  exercised in the default local validation path.

## Lessons

- Configuration refactors are not finished when names are clean; every fallback
  path must also be audited.
- Live tests are valuable not only for backend logic, but also for low-level
  runtime compatibility assumptions.
- A process creation argument that is unnecessary and less portable should be
  removed, not defended.
