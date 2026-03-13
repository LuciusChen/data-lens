# 039 - JDBC agent Java 17 baseline

## Background

`clutch-jdbc-agent` had drifted to a Java 21 build/runtime requirement even
though the sidecar does not materially benefit from a Java 21-only baseline.
That raised the barrier for JDBC users and made the setup story heavier than it
needed to be.

An aggressive downgrade to Java 11 was considered, but that path would have
forced broad mechanical rewrites of records, switch expressions, and tests
without changing the agent's protocol or behavior.

## Decision

Move the published `clutch-jdbc-agent` baseline to Java 17.

`clutch` now documents Java 17+ as the JDBC requirement and pins the new
release/checksum pair accordingly.

## Why this approach

Java 17 is the practical middle ground:

- materially easier for users to satisfy than Java 21
- modern enough to keep the agent code simple and readable
- avoids a large syntax-only downgrade whose only purpose would be Java 11
  compatibility

For a thin JDBC sidecar, reducing installation friction matters more than
squeezing the baseline all the way down to the oldest plausible LTS.

## Alternatives not chosen

### Stay on Java 21

Rejected because it imposed a newer JVM requirement without a matching feature
need in the agent itself.

### Lower all the way to Java 11

Rejected because the mechanical source downgrade would be larger and noisier
than the user benefit justified. Java 17 already covers the compatibility goal
for most practical setups.

## Verification

The Java 17 release was rebuilt and smoke-tested against the local Oracle 11g
and Oracle Free containers using the real `connect`, `search-tables`, and
`search-columns` paths.
