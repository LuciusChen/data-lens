## Background

The tree-based schema browser used two different interactions:

- expanding a node to inspect metadata such as columns, parameters, or index keys
- pressing `RET` to open the object's DDL or source

After the tree buffer was removed, `C-c C-d` temporarily became a second path to
the same DDL/source buffer that the default action already opened for
non-table-like objects.  That flattened two different tasks into one command and
lost the old "inspect metadata first" workflow.

## Decision

Keep the object-centric picker/actions workflow, but restore a distinct
inspector command:

- `C-c C-d` opens a read-only metadata inspector buffer
- DDL/source stays a separate action (`clutch-object-show-ddl-or-source`)
- the transient fallback and Embark map expose both actions explicitly

## Why

This keeps the tree removal without throwing away the most useful part of the
old browser: quick metadata inspection.

It also fixes a UX problem in the first object-centric rewrite.  A command named
`describe` should answer "what is this object?" rather than immediately jumping
to full DDL/source text.  Once `describe` and `show definition` are separate,
the action model becomes clearer:

- default action: do the most common thing for the object type
- describe: inspect metadata
- show definition: open DDL or source text

## Alternatives considered

### Keep `describe` as DDL/source

Rejected because it duplicated the default action for many object types and left
no first-class replacement for the old expandable metadata view.

### Rebuild a full inspector framework first

Rejected as premature.  A single `special-mode` inspector buffer is enough to
restore the lost capability while keeping the current `clutch-db-*` backend
interfaces and object workflow intact.

## Limitations

- The inspector is currently text-based and intentionally simple.
- It restores the tree's metadata detail use-cases, but it is not yet a full
  relationship navigator.
- Additional actions such as FK/referenced-by jumps can be added later on top of
  the same object-centric model.
