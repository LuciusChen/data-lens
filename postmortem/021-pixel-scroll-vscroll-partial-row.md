# 021 — Pixel Scroll Leaves Partial Row Under Fixed Header

## Background

`data-lens-result-mode` used a fixed `header-line-format` to display column
headers that stay visible regardless of scroll position. Below the header,
result rows are displayed as normal buffer text.

Emacs's `pixel-scroll-precision-mode` (available since Emacs 29) allows
scrolling by individual pixels rather than by full lines, enabling smooth
scrolling. It achieves this via `window-vscroll`, which offsets the buffer
display by a sub-line pixel amount.

## Root Cause

When `pixel-scroll-precision-mode` was active and the user scrolled, Emacs
applied a non-zero pixel vscroll. This shifted the first visible row upward by
some number of pixels, causing it to overlap with the header-line: the top few
pixel rows of the first data row became obscured by or merged with the bottom
edge of the header-line, making the top row appear partial — only its lower
portion visible below the header.

An earlier attempted fix added `(:underline t)` to the `header-line` face
remapping to provide a visual separator. This made the overlap visually
distinct but did not prevent the partial row — it was a cosmetic patch on top
of the real problem.

## Fix

Remove the underline face remap. Instead, add a buffer-local
`window-scroll-functions` hook:

```elisp
(defun data-lens--reset-vscroll (win _start)
  "Reset pixel-level vscroll on WIN to keep rows aligned with header."
  (when (> (window-vscroll win t) 0)
    (set-window-vscroll win 0 t)))
```

This hook fires after every scroll operation. If pixel vscroll is non-zero, it
is immediately snapped back to zero, ensuring the buffer always starts at a
whole-line boundary. `pixel-scroll-precision-mode` still scrolls smoothly
between lines but always lands on a pixel-exact line boundary within the result
buffer.

The hook is registered locally with `(add-hook 'window-scroll-functions
#'data-lens--reset-vscroll nil t)` and only affects `data-lens-result-mode`
buffers.

## Trade-off

Sub-pixel scrolling within the result buffer is disabled. Users who expect
smooth fractional-line scrolling will see the buffer snap to whole lines. This
is the correct behavior for a table view — partial rows under a fixed header are
confusing and alignment matters more than smooth scrolling within a cell row.

## Lesson

Fixed-header buffer layouts are incompatible with pixel-level vscroll. Any mode
that places a `header-line-format` over scrollable content and cares about row
alignment must snap vscroll to zero on every scroll event. The correct fix is in
the scroll layer (hook), not in the visual layer (face remapping). Cosmetic
patches that mask misalignment delay finding the real fix.
