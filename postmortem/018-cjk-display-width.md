# 018 — CJK Character Misalignment from Character-Count Padding

## Background

Table rendering aligned columns by padding cell values to a fixed width with
`string-pad`. Column widths were computed by taking the maximum `string-width`
of all values in a column — which is display-width-aware — but the padding step
used `string-pad`, which counts characters rather than display columns.

## Root Cause

In Emacs, CJK (Chinese, Japanese, Korean) characters occupy two terminal /
display columns but count as one character. `string-pad` pads to a target
*character count*. When a cell contains CJK characters, `string-pad` adds too
few spaces: it targets `N` characters but the string already occupies more than
`N` display columns.

For a cell `"名前"` (2 characters, 4 display columns) with a target width of 4,
`string-pad` sees `(length "名前")` = 2, computes 4 − 2 = 2 spaces to add, and
produces `"名前  "` — 6 display columns wide instead of 4. The adjacent column
starts two display columns too far to the right, destroying alignment for all
subsequent columns on that row.

The same mismatch affected: result table header rendering, result table row
rendering, and record view label padding. The width *measurement* was correct
(`string-width`); only the padding application was wrong.

## Fix

Replace all `string-pad` calls in rendering paths with `data-lens--string-pad`,
which pads based on `(string-width str)` instead of `(length str)`:

```elisp
(defun data-lens--string-pad (str width)
  "Pad STR with spaces to reach display WIDTH.
Unlike `string-pad', this accounts for wide characters (CJK)."
  (let ((sw (string-width str)))
    (if (>= sw width)
        str
      (concat str (make-string (- width sw) ?\s)))))
```

## Lesson

Any rendering code that aligns columns in a terminal or Emacs buffer must use
`string-width` (display-column count) for both measurement *and* padding, never
`length` (character count). The two are equivalent only for ASCII. CJK,
full-width punctuation, and combining characters all break the assumption. The
typical failure mode is subtle: headers and rows misalign only when CJK data is
present, making the bug easy to miss in ASCII-only test datasets.
