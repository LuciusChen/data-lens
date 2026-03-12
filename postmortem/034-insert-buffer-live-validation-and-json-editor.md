* 034. Insert Buffer: Live Validation, Visual Focus, and JSON Escape Hatch

The insert buffer had already moved in the right direction by behaving like a
form instead of a spreadsheet, but a few pieces were still missing:

- values started at uneven columns when metadata tags were long
- the active field was only implied by point
- JSON editing became uncomfortable as soon as the value stopped fitting on one line
- validation still leaned too heavily on the final staging step

**Decision**

Keep the insert buffer as the primary editing surface, and improve it locally:

- align field labels to a shared value column
- highlight the active field line
- validate the current field inline as the user edits
- keep =TAB= navigation, add =RET= as a faster "accept and move on" key
- add a JSON child editor only on demand via =C-c '=

This keeps the main workflow flat:

=result buffer -> insert buffer=

The JSON child buffer is an escape hatch, not a third mandatory layer.

**Validation scope**

Live validation stays local to the current field.  It does not rescan or rerender
the whole form.  JSON validation is delayed behind a short idle timer so typing
large values does not feel sticky.

That keeps the UX responsive while still surfacing mistakes close to the field
that caused them.

**JSON editing**

The child JSON editor is intentionally narrow in scope:

- open only for JSON fields
- save back into the insert form with =C-c C-c=
- cancel with =C-c C-k=
- write compact JSON back into the form so the parent form layout stays stable

This preserves the current single-line form model without forcing users to edit
larger JSON documents inline.
