# 054 — Object-Centric Workflow without a Second Provider Layer

## Background

clutch originally accumulated several metadata entry points:

- schema refresh for cached table names
- `C-c C-j` for browse-object selection
- `C-c C-d` for describe-at-point
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
- `C-c C-d` describes the current object
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

## Alternatives considered

- **Introduce a new provider layer above `clutch-db-*`**
  Rejected because it duplicates backend dispatch without solving a new concrete
  problem.

- **Immediately split object logic into several new files**
  Rejected for now because the responsibility boundary is still settling.  A
  large but coherent `clutch.el` is preferable to several tiny files with
  blurred ownership.

- **Keep the tree buffer as a secondary browser**
  Rejected because it preserved two competing metadata workflows and kept the UX
  split.

## Accepted limitations

- `clutch.el` still contains both the main SQL UI and the object workflow.
  Further extraction should wait until the object boundary is stable enough to
  justify a new file.

- Object entries are still plists rather than a dedicated `cl-defstruct`.
  Stable identity matters now; a separate object model type can wait until it
  solves a concrete correctness or maintainability problem.

- The picker currently materializes object categories through the existing
  backend calls.  If large-schema performance becomes an issue, that should be
  solved by improving object discovery/search APIs rather than by adding a
  second abstraction layer.
