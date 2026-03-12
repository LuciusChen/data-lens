* 033. Insert Buffer as a Form, Not a Spreadsheet

The insert workflow picked up three distinct needs:

- move quickly across fields
- complete constrained values such as enums
- repair a staged insert after the database rejects it

The wrong direction would have been to overload =TAB= with all three.
That makes the buffer unpredictable: sometimes it navigates, sometimes it
completes, and the user has to remember invisible context.

**Decision**

Treat the insert buffer like a small Emacs form:

- =TAB= / =S-TAB= move between fields
- =M-TAB= / =C-M-i= perform completion for the current field
- =C-c .= fills the current temporal field with "now"

This keeps navigation and completion separate in the normal Emacs way.

**Staged insert repair**

Database validation failures are common for enum / JSON / generated-default
columns. Requiring the user to discard the whole staged insert and start over
was avoidable friction.

The result buffer now treats a green ghost insert row as editable state:

- =C-c '= on that row re-opens the insert buffer
- previously entered values are restored
- =C-c C-c= updates the staged insert in place

This keeps the staged-commit model intact while removing the "throw it away and
retype everything" failure path.

**Display placeholders**

Pending insert rows can now show:

- =<generated>= for values the database will generate
- =<default>= for values supplied by database defaults

These are display-only hints. The staged INSERT SQL is still built from fields
the user explicitly entered.
