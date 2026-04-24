# 043 - SQL Server JDBC classifier downloads

## Background

`clutch-jdbc-install-driver 'sqlserver` relied on the generic Maven download
path builder in `clutch-db-jdbc.el`.

That builder assumes coordinates of the form:

- `group:artifact:version`

and then downloads:

- `artifact-version.jar`

This works for most JDBC drivers in the project, but Microsoft's SQL Server
driver is published with a JRE classifier in the version string, for example:

- `mssql-jdbc-13.4.0.jre11.jar`

The previous SQL Server entry used `12.6.0` without the classifier, which made
the generated Maven Central URL return `404`.

## Decision

The SQL Server driver source now points at the classifier-bearing Maven version:

- `com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11`

The local installed filename remains normalized to `mssql-jdbc.jar`.

## Why this approach

This keeps the current downloader simple:

- no special-case URL builder for SQL Server
- no extra `:classifier` field in the driver source table
- no change to local driver discovery

The only thing that needed to change was the source coordinate.

## Alternative not chosen

Add a more general artifact model with separate `:version`, `:classifier`, and
custom filename templating.

That would be more flexible, but it is unnecessary for the current supported
driver matrix.  One corrected coordinate is cheaper and easier to reason about.
