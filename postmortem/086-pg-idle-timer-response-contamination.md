#+title: PG Idle Timer Response Contamination
#+date: 2026-04-09

* Context

PG backend users reported that eldoc stopped showing column names after the
schema cache warmed up.  Instead of column names, the schema hash stored
/table names/ as column values for the most recently loaded table.

Diagnostic output:

#+begin_example
section9_cases_wide → ("audit_events_wide" "customers_dim" "orders_large")
#+end_example

The correct value should have been the 20 columns returned by
=clutch-db-list-columns=.

* Root Cause

=pg-exec= calls =accept-process-output= internally (in =pg--read-char=) to
wait for the PostgreSQL server response.  =accept-process-output= can fire
pending idle timers as a side effect.

=clutch-db--schedule-idle-metadata-call= used =run-with-idle-timer 0= to
dispatch background metadata loading (table lists, column lists).  When two
idle timers fired close together on the same =pgcon=:

1. Timer A calls =pg-exec= for =clutch-db-list-tables=.
2. Inside =accept-process-output=, Timer B fires and calls =pg-exec= for
   =clutch-db-list-columns= on the same connection.
3. Timer B's =pg-exec= reads Timer A's response (the table list).
4. Timer A's =pg-exec= reads Timer B's response (the column list).
5. Both callers get the wrong result — table names land in the column hash
   slot, and vice versa.

The corruption was not random: it happened deterministically whenever two
metadata idle timers were pending simultaneously, which was the common case
during schema warmup.

* Fix

Added a =clutch-db-busy-p= guard in =clutch-db--schedule-idle-metadata-call=.
When the connection is busy, the call reschedules itself with a 0.1 s idle
timer instead of proceeding.  This serializes all metadata calls on a single
connection without blocking Emacs.

#+begin_src elisp
(cl-labels
    ((run ()
       (if (clutch-db-live-p conn)
           (if (clutch-db-busy-p conn)
               (run-with-idle-timer 0.1 nil #'run)
             (condition-case err
                 (when callback
                   (funcall callback (apply fn conn args)))
               (error
                (when errback
                  (funcall errback (error-message-string err))))))
         (when errback
           (funcall errback "Connection closed")))))
  (run-with-idle-timer 0 nil #'run))
#+end_src

A secondary fix changed the CAPF path: when =sync-columns-p= is =t= (PG),
the completion-at-point function now calls the synchronous
=clutch--ensure-columns= instead of =clutch--ensure-columns-async=, avoiding
an unnecessary idle-timer round-trip that could trigger the same reentry.

* Why Not Serialize at the =pg-exec= Level

The reentry happens inside =accept-process-output=, which is deep in the
=pg= library.  Serializing there would require patching an external dependency.
The idle-timer scheduling layer is the right place because it is the only
caller that issues discretionary background queries — interactive queries are
already sequential by nature.

* Why 0.1 s Retry Instead of a Queue

A queue adds complexity (ordering, cancellation, error propagation) for a
problem that only manifests during the brief schema-warmup burst.  A short
retry delay is simple, self-limiting, and sufficient.  If metadata loading
ever becomes latency-sensitive, a proper queue can replace the retry without
changing the public API.

* Lesson

Any Emacs function that calls =accept-process-output= is a reentry point for
idle timers.  When writing background work that uses process-based
connections, always check whether the connection is mid-flight before issuing
a new request — even if the two callers appear to be on separate idle timers.
