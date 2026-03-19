# 052 — Browse Object Annotations over Source-First Labels

## Background

Oracle low-privilege support expanded `C-c C-j` from a plain table list into a
mixed object picker:

- business tables from accessible owners
- user synonyms
- public synonyms such as `USER_TABLES`

That created a UI problem.  A flat minibuffer list does not have DataGrip's
tree structure, so some provenance had to be shown inline.  The first attempt
made source/type the leading visual element and also added bespoke metadata
matching in the picker itself.

In practice that made the list harder to scan:

- the object name stopped being the primary visual anchor
- labels like `PUBLIC/SYNONYM` competed with the candidate text
- target owners such as `DATA_OWNER` were shown too often
- the custom matcher overlapped with what completion styles such as
  `orderless-annotation` already do well

## Decision

Keep the object name as the only primary candidate text.  Show provenance as a
gray annotation on the right:

- `USER_TABLES  PUBLIC/synonym`
- `ORDERS  APP/synonym`

Target owner detail is hidden by default and shown only when duplicate object
names need disambiguation.

Matching behavior is simplified accordingly:

- default completion stays object-name-driven
- annotation text remains available to completion styles such as Orderless via
  annotation matching (`&public`, `&synonym`, etc.)
- clutch no longer reimplements a general-purpose multi-field matcher in the
  browse reader itself

## Rationale

This keeps the picker readable in the default Emacs completion UI while still
exposing provenance when the user needs it.

The key principle is: **flat pickers should optimize for recognition first,
metadata second**.  DataGrip can show owner/source in a hierarchy.  clutch
cannot, so repeating owner/source before every candidate is noisier than
helpful.

Relying on annotation-aware completion styles is also the better layering:

- clutch provides accurate object metadata
- the completion style decides how to search it

That keeps the picker implementation small and avoids a second, partially
duplicated filtering system inside `clutch--browse-table-entry-reader`.

## Alternatives considered

- **Source-first affixation (`PUBLIC  USER_TABLES  synonym`)**
  Rejected because the object name stopped being the fastest thing to scan.

- **Always show target owner**
  Rejected because it adds noise for the common case.  Most candidates do not
  need disambiguation.

- **Custom multi-field search inside clutch**
  Rejected because it overlaps with completion-style behavior, especially
  Orderless annotation matching, while making the picker logic more complex.

## Known limitations

- Annotation-based metadata search depends on the user's completion style.  The
  default completion experience still prioritizes object-name matching.
- A flat picker can only approximate hierarchical database browsers.  If clutch
  grows a richer schema tree later, some of this inline provenance may become
  unnecessary.
