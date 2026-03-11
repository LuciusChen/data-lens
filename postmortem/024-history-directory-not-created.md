# 024 — History Save Fails When Data Directory Does Not Exist

## Background

SQL query history is persisted to disk via `clutch--save-history`, which writes
to `clutch-history-file`. The default path was changed from a flat location to
`~/.emacs.d/clutch/history` — a file inside a new `clutch/` subdirectory that
does not exist on a fresh Emacs installation.

## Root Cause

`with-temp-file path` calls `write-region`, which signals a `file-error` if any
parent directory in `path` does not exist. The `~/.emacs.d/clutch/` directory is
not created by Emacs, the package installer, or any init code. On first run
after the default was changed, `clutch--save-history` failed with a file-error.

A prior commit had already added a `condition-case` wrapper around the history
write to prevent failures from blocking Emacs exit. This masked the error —
the failure was swallowed without any user notification. The history ring
accumulated in memory and was discarded on exit, with no indication to the user
that persistence had silently stopped working.

## Fix

Call `(make-directory (file-name-directory file) t)` before `with-temp-file`.
The `t` argument creates all intermediate directories (equivalent to `mkdir -p`)
and does not signal an error if the directory already exists.

```elisp
(make-directory (file-name-directory file) t)
(with-temp-file file ...)
```

## Lesson

Any feature that writes to a configurable file path must ensure the parent
directory exists before writing. This is especially important when: (a) the
default path includes a package-specific subdirectory, (b) the write is inside a
`condition-case` that silently handles errors, and (c) the failure has no
user-visible symptom beyond data silently not persisting. The fix is one line and
should be reflexive whenever a file write path is introduced.
