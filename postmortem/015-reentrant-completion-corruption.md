# 015 — Re-entrant Completion and throw-on-input Corrupt Query Responses

## Background

SQL completion used `completion-at-point-functions` to offer column names.
Loading column names required a `SHOW COLUMNS FROM <table>` query on the live
connection. This was lazy: columns were fetched and cached on first completion
invocation.

With corfu or company enabled, auto-completion fires on idle timers (typically
0.2–0.5 seconds after a keystroke). These timers run *inside*
`accept-process-output`, which Emacs uses to poll the network socket while
waiting for a query response.

## Root Cause — Stage 1: Re-entrancy

When `mysql-query` or `pg-query` called `(accept-process-output proc ...)` to
wait for a response, Emacs processed pending timer events — including the
completion idle timer. The timer called `data-lens-completion-at-point`, which
invoked `SHOW COLUMNS FROM t` on the same connection: a second query while the
first was still in flight.

The two queries interleaved in the connection buffer: the `SHOW COLUMNS`
response arrived mid-stream of the original query's response packets. The
response parser read the wrong headers, producing silently wrong result data —
correct row count, wrong cell values drawn from the other query's result set.

**Fix**: add a `busy` boolean field to `mysql-conn` and `pg-conn`. Wrap
`mysql-query` / `pg-query` in `unwind-protect` with `(setf conn-busy t)` /
`(setf conn-busy nil)`. Add `data-lens-db-busy-p` generic method.
Completion skips column loading when the connection is busy, falling back to
table-names-only candidates.

## Root Cause — Stage 2: throw-on-input

The busy flag closed re-entrant *new* queries, but a subtler mechanism remained.
Completion frameworks call their candidates function inside `while-no-input`,
which works by setting `throw-on-input` to a non-nil tag. When
`accept-process-output` processes input events with `throw-on-input` set, Emacs
throws out of whatever is currently running — including a mid-flight
`mysql-query` call.

The throw exited `mysql-query` after some protocol bytes had been read from the
buffer but before the full response was consumed. Those bytes remained in the
connection buffer. The next query's `read-packet` started from this stale
leftover, interpreting it as the beginning of a new response — again producing
silently wrong results.

**Fix**: bind `(let ((throw-on-input nil)) ...)` around the entire body of
`mysql-query` / `pg-query`. This makes query execution immune to
`while-no-input` interruption regardless of what completion or any other
framework does. Additionally, erase the connection buffer at the start of each
query (before setting `busy`) to flush any stale bytes left by any previously
interrupted query.

## Why Two Separate Commits

The two bugs were discovered sequentially. The busy flag (commit `1fac351`) was
the obvious first defense. Testing after that fix revealed that `throw-on-input`
could still corrupt responses even when the busy flag prevented new queries from
being dispatched — the completion framework was aborting the *existing* query,
not trying to start a new one. The second commit (`0673f36`) addressed this
independent mechanism.

## Lesson

In Emacs, `accept-process-output` does not provide exclusive access to the
event loop. Idle timers, `while-no-input`, and input event processing can all
interrupt synchronous-looking network I/O at any `accept-process-output` call
site. Protocol implementations that read multi-packet responses in a loop must
defend against both re-entrant dispatch (busy flag) and mid-read interruption
(`throw-on-input nil`). Both defenses are needed; neither alone is sufficient.
