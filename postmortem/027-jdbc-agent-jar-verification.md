# JDBC Agent Jar Verification

## Background

`clutch-db-jdbc.el` selects the agent jar by filename:

- `clutch-jdbc-agent-0.1.2.jar`

That catches the obvious "jar missing" case, but not the more subtle one:
the file exists, the versioned filename looks right, and its contents are
actually stale or locally overwritten.

This became more important once the JDBC agent started shipping as a separate
release artifact. A user can update `clutch` first, keep an older local jar,
and only discover the mismatch later as confusing runtime behavior.

## Decision

Verify the local jar by SHA-256 before starting the JVM sidecar.

The expected checksum lives in `clutch` as a normal defcustom:

- `clutch-jdbc-agent-sha256`

`clutch-jdbc-ensure-agent` also cleans up stale versioned jars after a valid
current jar is present.

## Why This Approach

The key requirement is to detect "same filename, wrong contents". Version
comparison alone is not enough for that case.

The obvious alternative was to change the Java agent so its ready response
includes a build id. That helps only if the Emacs side also knows the expected
build id. Once we already have to record an expected identity in `clutch`,
checking the jar file directly is simpler and stricter:

- it catches mismatches before the JVM starts
- it does not require protocol changes
- it works even when the agent cannot start successfully
- it naturally covers the "same version, different bytes" case

## Escape Hatch

Some users intentionally run a locally built agent jar while developing.
Strict release checksum verification would block that workflow, so the check is
explicitly defeatable:

- set `clutch-jdbc-agent-sha256` to `nil`

That keeps the normal path safe while preserving a direct escape hatch for
development builds.

## Performance

Checksum verification happens only on agent startup, not on every query. The
cost is one local file read of a small jar, which is negligible compared to
starting a JVM.

Cleanup runs only in `clutch-jdbc-ensure-agent`, which is already a
maintenance command rather than a hot path.

## Alternatives Considered

### Runtime build id from the Java sidecar

Rejected for now. It adds cross-repo protocol coupling without improving the
main guarantee unless `clutch` also stores an expected build id.

### No verification, only download-by-version

Rejected. This is exactly the failure mode that leaves users with a versioned
jar that looks current but is not the intended artifact.

### Automatic silent redownload on every startup

Rejected. It hides state changes, adds network dependence to normal startup,
and makes local custom jars too easy to clobber accidentally.

## Known Limitation

If the published release jar is replaced without updating the checksum in
`clutch`, verification will fail until `clutch` is updated as well. That is an
acceptable constraint: replacing a release asset is a source-of-truth change,
so the consuming source should move in lockstep.
