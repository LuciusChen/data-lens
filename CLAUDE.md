# data-lens Development Guide

Elisp best practices distilled from llm.el, magit, consult, eglot, vertico/marginalia.

## Architecture

- **Interface / Implementation separation**: `mysql.el` (protocol layer) is pure library with no UI; `data-lens.el` (UI layer) depends on `mysql.el` but never the reverse. `data-lens-transient.el` depends on `data-lens.el` for transient menus. Keep the dependency flow strictly one-directional.
- **Single responsibility per file**: Each file has one job. Don't mix protocol code with rendering code.
- **No side effects on load**: Loading a file should not alter Emacs behavior. All behavior activation must be explicit (user calls a command or enables a mode).
- **Reuse Emacs infrastructure**: Use `completing-read` (not framework-specific APIs), `special-mode` for read-only buffers, `text-property-search-forward` for navigation, standard hooks, etc.

## Naming

- **Public API**: `data-lens-` prefix for UI layer, `mysql-` prefix for protocol layer. No double dash for public symbols.
- **Internal/private**: `data-lens--` or `mysql--` double-dash prefix. Never call from outside the defining file.
- **Predicates**: multi-word names end in `-p` (e.g., `data-lens--connection-alive-p`).
- **Unused args**: prefix with `_` (e.g., `(_ridx)`).

## Control Flow

- Avoid deep `let` → `if` → `let` chains. Favor flat, linear control flow using `if-let*`, `when-let*`, or similar constructs whenever possible.
- Use `pcase`/`pcase-let` for structured destructuring instead of nested `car`/`cdr`/`nth`.
- Prefer `cl-loop` over manual `dolist` + accumulator when building lists with complex iteration logic.

## Error Handling

- **`user-error`** for user-caused problems (no connection, no cell at point, invalid input). Does NOT trigger `debug-on-error`.
- **`error`** for programmer bugs only.
- **`condition-case`** to handle recoverable errors (network failures, query errors). Wrap non-essential operations (FK loading, schema caching) so errors never prevent primary results from being shown.
- Error messages should state what is wrong, not what should be (e.g., "Not connected" not "Must be connected").

## State Management

- **`defvar-local`** for all per-buffer state. Set with `setq-local` in mode bodies.
- **Plain `defvar`** for global/shared state (schema cache, history ring).
- **`defcustom`** for all user-configurable values. Always specify `:type` precisely (`natnum`, `string`, `boolean`, `(choice ...)`) and `:group`.
- Major modes must make all their state variables buffer-local.

## Mode Definitions

- Derive read-only UI buffers from `special-mode` (`data-lens-result-mode`, `data-lens-record-mode`, `data-lens-schema-mode`).
- Derive editing modes from appropriate parents (`sql-mode` for `data-lens-mode`, `comint-mode` for `data-lens-repl-mode`).
- `define-derived-mode` auto-creates `-map`, `-hook`, and `-syntax-table`. Use them.
- Register buffer-local hooks in the mode body (e.g., `post-command-hook`, `completion-at-point-functions`) with the LOCAL arg `t`.

## Rendering

- **Text properties** for data-bearing annotations (`data-lens-row-idx`, `data-lens-col-idx`, `data-lens-full-value`, `data-lens-header-col`). They are fast (interval tree) and travel with the text.
- **Overlays** only for ephemeral visual effects that should not be part of the text (e.g., header active-column highlight). Keep overlay count minimal (O(n) lookup).
- Build buffer content from scratch via `erase-buffer` + `insert` (magit-section pattern). Never parse buffer text to extract data — always read from the cached data structures (`--result-rows`, `--result-columns`).

## Function Design

- Keep functions under ~30 lines. Extract helpers when a function exceeds this.
- Name extracted helpers to describe WHAT they compute, not WHERE they're called from.
- Pure computation (no side effects) should be separate from display (buffer mutation).
- Interactive commands should be thin wrappers: validate input, call internal function, show feedback.

## Completion

- Always use standard `completing-read`. This works with vertico, ivy, helm, default, etc.
- For completion-at-point: return quickly (called often), use `:exclusive 'no` to allow fallback.
- Add capf functions buffer-locally via `add-hook` with LOCAL=t.

## Autoloads

- `;;;###autoload` only on: interactive commands (`data-lens-mode`, `data-lens-repl`, `data-lens-execute`, `data-lens-dispatch`), `auto-mode-alist` entries.
- Never autoload internal functions, defcustom, or defvar.
- Use `declare-function` for functions from optional dependencies to silence byte-compiler.

## Quality Checks

Before releasing, ensure:
- `(byte-compile-file "data-lens.el")` produces no warnings.
- All public functions have docstrings.
- Every file starts with `;;; -*- lexical-binding: t; -*-` and ends with `(provide 'pkg)` / `;;; pkg.el ends here`.
