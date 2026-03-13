# 036: Connect Wait Polling

## What happened

`pg_preview_demo` felt slow to connect even though the PostgreSQL container and
its queries were fast. Measured from Emacs:

- TCP open: about 10 seconds
- startup/authentication: about 200ms
- schema refresh: under 10ms

So the delay was not PostgreSQL server work. It was the client-side connect wait.

## Root cause

Both `pg.el` and `mysql.el` open sockets with `:nowait t` and then wait for the
process status to leave `connect`.

The old wait loop passed the entire remaining timeout to
`accept-process-output`. On this machine, a localhost PostgreSQL connect did not
wake that wait promptly when the process transitioned to `open`, so a fast
connect stretched to the full `connect-timeout` window.

## Fix

Poll `accept-process-output` in short slices (50ms max) while still enforcing
the overall deadline.

This keeps the timeout semantics intact, but avoids turning a fast local
connect into a full-timeout wait.

## Why MySQL changed too

MySQL did not reproduce the problem on this machine, but it used the same wait
shape. The code was aligned to the same short-polling approach so the same
platform-specific behavior does not reappear there later.

## Takeaway

For asynchronous socket connects in Emacs, timeout enforcement and wakeup
granularity need to be treated separately. A correct deadline is not enough if
the wait granularity is too coarse.
