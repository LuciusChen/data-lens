#+title: Live Value Viewer Workflow
#+date: 2026-04-08

* Context

=clutch= already had a useful =v= command for inspecting one cell at a time,
but it had a clear browsing gap:

- JSON / XML / BLOB inspection is often a multi-cell workflow, not a one-cell workflow
- moving across rows and columns forced repeated =v= presses
- the static viewer was still the right behavior when the user wanted to pin one value

The question was not whether to replace =v=, but whether clutch should offer a
second inspection mode for "follow point" browsing.

* Decision

Keep =v= as the static one-shot viewer and add =V= as a live viewer.

- =v= keeps the current simple behavior
- =V= opens a single =*clutch-live-view*= buffer
- the live viewer follows point in result and record buffers
- the live viewer keeps the same JSON / XML / BLOB dispatch as =v=
- inside the live viewer, =f= freezes/unfreezes, =g= refreshes, and =q= closes

* Why We Did Not Replace =v=

Static inspection and follow-point inspection are different workflows.

- static view is good for pinning one payload while editing or comparing
- live view is good for scanning adjacent cells quickly
- changing =v= into a follow-mode command would make the simple case less predictable

Separate keys keep the workflow explicit instead of making one command branch
between two behaviors.

* Why We Did Not Use Indirect Buffers

The value viewer is not a second view onto the table text.

- result cells often show truncated or custom-rendered text
- the raw value lives in =clutch-full-value= text properties
- JSON / XML / BLOB views are derived renderings, not shared source text

An indirect buffer would therefore be the wrong abstraction.  The live viewer
should re-render from the raw cell value, not reuse the displayed table text.

* Scope We Deliberately Kept Small

This first slice does not add:

- editing inside the live viewer
- split viewers per type
- multiple synchronized live viewers
- result-buffer mutations driven from the viewer

The narrow goal is browsing velocity.  If that proves useful in normal use, we
can expand from a stable =v= / =V= split instead of overloading the first step.
