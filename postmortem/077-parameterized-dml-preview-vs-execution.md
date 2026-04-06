# 077. Parameterized DML needs a separate preview path

## Context

The staged mutation workflow in `clutch-edit.el` used to build final SQL
strings for `INSERT`, `UPDATE`, and `DELETE` by escaping values directly into
the statement text.

The goal of this change was to move native backends to prepared / parameterized
execution while keeping the existing confirmation and preview UX unchanged.

## Decision

- Mutation builders now return `(SQL-TEMPLATE . PARAMS)` pairs.
- Confirmation prompts and `clutch-preview-execution-sql` still render a fully
  interpolated SQL string for readability.
- Execution goes through `clutch-db-execute-params`.
- JDBC is allowed to keep using the generic fallback path that renders the
  template and calls `clutch-db-query`.

## Why

Trying to use one representation for both preview and execution causes the
wrong tradeoff in both directions:

- A rendered SQL string is readable to the user, but it throws away the
  parameter boundary we need for protocol-level binding.
- A parameterized template is the right execution payload, but it is a worse
  confirmation prompt because the user no longer sees the real values that will
  be sent.

Separating the two lets us keep the user-facing workflow stable while changing
only the execution semantics underneath.

## PostgreSQL note

The original plan was to use `pg-exec-prepared` directly for all PostgreSQL
parameterized DML.

That works for ordinary non-NULL values, but `pg-el` does not cleanly support
the combination we need for staged mutations:

- protocol-level NULL parameters
- unspecified / inferred parameter types

For that reason, the PostgreSQL adapter keeps the ordinary `pg-exec-prepared`
path for non-NULL argument lists, but uses a small null-safe wrapper for the
NULL case so that `nil` is sent as a real protocol NULL instead of being
degraded back into string interpolation.
