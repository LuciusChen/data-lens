# 054 — Object-Centric Workflow without a Second Provider Layer

## Background

clutch originally accumulated several metadata entry points:

- schema refresh for cached table names
- `C-c C-j` for browse-object selection
- `C-c C-d` for describe-object resolution
- a tree-style schema buffer for exploratory browsing
- optional Embark actions for tables

Once the tree buffer was removed, the remaining workflow had to converge around
one object-centric model.  At the same time, there was pressure to redesign the
metadata stack into a fresh multi-layer architecture (`introspection`,
`provider`, `model`, `UI`).

That would have introduced a second abstraction family on top of the existing
`clutch-db-*` generic interface.

## Decision

Keep `clutch-db.el` as the only backend abstraction and build the new workflow
directly on top of it.

The resulting shape is:

- `clutch-db-*` remains the backend/metadata interface
- `clutch.el` owns the interactive object workflow
- `C-c C-j` is the main object picker
- `C-c C-d` is the describe-focused wrapper over the shared object resolver
- `C-c C-o` is the no-Embark action fallback
- Embark extends the same object actions instead of defining a parallel path

Internally, the metadata helpers were also renamed from `table-entry-*` to
`object-entry-*` so the code matches the new mental model.

## Rationale

The existing `clutch-db-*` layer already solves the real backend separation
problem:

- the UI depends on one generic metadata interface
- native protocol backends and JDBC backends implement that interface
- the dependency flow stays one-directional

Adding a second `provider` or `introspection` layer would mostly rename the
same boundary while making the call chain longer and harder to reason about.

This also follows the project rule in `AGENTS.md`: question every abstraction
and split only when a boundary is already real.

The object workflow therefore favors:

- one metadata abstraction
- one user-facing object model
- one action vocabulary shared by default action, transient fallback, and
  Embark

instead of parallel stacks with overlapping responsibilities.

## Describe View versus Definition

The old tree workflow distinguished between two separate tasks:

- inspect object metadata such as columns, parameters, or index keys
- open the full DDL or source text

That distinction should remain even after the tree buffer is removed.  A
command named `describe` should answer "what is this object?" instead of
collapsing into the same DDL/source path as the default action.

The practical outcome is:

- `C-c C-d` remains a describe/inspect view, resolving at point when possible
  and otherwise prompting with the shared picker
- `clutch-object-show-ddl-or-source` remains a separate action
- Transient and Embark expose both through the same object-action model

This keeps the object workflow converged without flattening two genuinely
different tasks into one command.

## Alternatives considered

- **Introduce a new provider layer above `clutch-db-*`**
  Rejected because it duplicates backend dispatch without solving a new concrete
  problem.

- **Keep the tree buffer as a secondary browser**
  Rejected because it preserved two competing metadata workflows and kept the UX
  split.

- **Collapse describe into DDL/source**
  Rejected because it duplicates the default action for many object types and
  removes the first-class replacement for metadata inspection.

- **Build a heavier inspector framework first**
  Rejected because a simple `special-mode` describe buffer is sufficient for the
  current object workflow.  The important part is preserving the distinction
  between "inspect metadata" and "show definition", not building a larger UI
  subsystem.

## Accepted limitations

- The object workflow has since been extracted to `clutch-object.el`.
  This limitation is resolved.

- Object entries are still plists rather than a dedicated `cl-defstruct`.
  Stable identity matters now; a separate object model type can wait until it
  solves a concrete correctness or maintainability problem.

- The picker currently materializes object categories through the existing
  backend calls.  If large-schema performance becomes an issue, that should be
  solved by improving object discovery/search APIs rather than by adding a
  second abstraction layer.

- The describe view is intentionally text-based and simple.  It is a shared
  object view, not a second action architecture.

## Schema Switching (from 056)

After the object-centric workflow settled, the next pressure was "server objects"
(users, roles, tablespaces). This was rejected because the categories diverge by
backend and lacked a clear cross-backend workflow.

Instead, runtime schema switching was added: Oracle/JDBC switches business
schema, MySQL switches database via `USE`. This keeps `C-c C-j` / `C-c C-d` /
`C-c C-o` focused on schema objects within one active schema per connection.

Multi-schema workspace and PostgreSQL switching are deferred.
