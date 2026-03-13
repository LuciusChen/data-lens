# clutch Development Guide

Elisp best practices distilled from llm.el, magit, consult, eglot, vertico/marginalia.

## First Principles

- **Question every abstraction**: Before adding a layer, file, or indirection, ask "is this solving a real problem right now?" If the answer is hypothetical, don't add it.
- **Simplify relentlessly**: Three similar lines of code are better than a premature abstraction. A single large file (like eglot's 3500-line `eglot.el`) is better than five tiny files with unclear boundaries.
- **Fewer files, clearer boundaries**: Only split a file when it has a genuinely distinct responsibility (e.g., wire protocol vs. UI). Never split for cosmetic reasons or predicted future growth.
- **Delete, don't deprecate**: If something is unused, remove it entirely. No backward-compatibility shims, no re-exports, no "removed" comments.
- **Converge UX, avoid mode branches**: Prefer one clear entry point and one consistent behavior model over multiple overlapping commands or prefix-driven branches. If two commands do nearly the same thing, merge them and keep the simpler mental model.

## Architecture

- **Interface / Implementation separation**: `mysql.el` and `pg.el` (protocol layers) are pure libraries with no UI; `clutch.el` (UI layer) depends on `clutch-db.el` (generic interface) but never on protocol layers directly. Keep the dependency flow strictly one-directional.
- **Single responsibility per file**: Each file has one job. Don't mix protocol code with rendering code.
- **No side effects on load**: Loading a file should not alter Emacs behavior. All behavior activation must be explicit (user calls a command or enables a mode).
- **Reuse Emacs infrastructure**: Use `completing-read` (not framework-specific APIs), `special-mode` for read-only buffers, `text-property-search-forward` for navigation, standard hooks, etc.

## Version Baseline

- `clutch` targets **Emacs 28.1+** for the native MySQL/PostgreSQL backends.
- The SQLite backend requires **Emacs 29.1+** because it depends on the built-in
  `sqlite-*` functions.
- The JDBC path depends on `clutch-jdbc-agent`, whose published baseline is
  **Java 17+**.
- Do not silently raise any of these baselines in code, docs, or release assets.
  If a change requires a higher Emacs or Java version, update:
  - `README.org`
  - the relevant release/version metadata
  - a postmortem explaining why the higher baseline is justified

## Naming

- **Public API**: `clutch-` prefix for UI layer, `mysql-` / `pg-` prefix for protocol layers. No double dash for public symbols.
- **Internal/private**: `clutch--`, `mysql--`, or `pg--` double-dash prefix. Never call from outside the defining file.
- **Predicates**: multi-word names end in `-p` (e.g., `clutch--connection-alive-p`).
- **Unused args**: prefix with `_` (e.g., `(_ridx)`).

## Control Flow

- Avoid deep `let` → `if` → `let` chains. Favor flat, linear control flow using `if-let*`, `when-let*`, or similar constructs whenever possible.
- Use `pcase`/`pcase-let` for structured destructuring instead of nested `car`/`cdr`/`nth`.
- Prefer `cl-loop` over manual `dolist` + accumulator, or `cl-reduce`, when building lists with complex iteration logic. `cl-reduce` is acceptable for simple single-operation folds (e.g., summing a list), but `cl-loop` is clearer when the logic involves multiple steps, conditionals, or accumulating into a non-trivial structure.

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

- Derive read-only UI buffers from `special-mode` (`clutch-result-mode`, `clutch-record-mode`, `clutch-schema-mode`).
- Derive editing modes from appropriate parents (`sql-mode` for `clutch-mode`, `comint-mode` for `clutch-repl-mode`).
- `define-derived-mode` auto-creates `-map`, `-hook`, and `-syntax-table`. Use them.
- Register buffer-local hooks in the mode body (e.g., `post-command-hook`, `completion-at-point-functions`) with the LOCAL arg `t`.

## Rendering

- **Text properties** for data-bearing annotations (`clutch-row-idx`, `clutch-col-idx`, `clutch-full-value`, `clutch-header-col`). They are fast (interval tree) and travel with the text.
- **Overlays** only for ephemeral visual effects that should not be part of the text (e.g., header active-column highlight). Keep overlay count minimal (O(n) lookup).
- Build buffer content from scratch via `erase-buffer` + `insert` (magit-section pattern). Never parse buffer text to extract data — always read from the cached data structures (`--result-rows`, `--result-columns`).

## Mutation Workflow Convergence

- **One staged-mutation vocabulary everywhere**: If the UI exposes staged edits / deletes / inserts, the left marker column, footer, transient labels, mode help, and `README.org` must use the same terminology and the same behavior boundaries. Do not let `preview`, `discard`, or `commit` mean different things in different surfaces.
- **If mutation identity becomes PK-based, every lookup must become PK-based**: Once pending state is keyed by primary key, render paths, record view, discard actions, and summary/state lookups must also move to PK-based identity. Partial migration is not acceptable; it creates invisible staged state.
- **Preview must show what would really execute**: A command named `Preview execution` must preview the actual execution payload for the current workflow. Do not switch between query-preview and mutation-preview heuristics based on incidental UI state.
- **Nearby workflows should share helpers, not drift**: Insert and edit flows must reuse the same completion candidates, temporal helpers, and field validation rules when the write semantics are the same. Do not fork a second "almost the same" rule set for edit buffers.
- **UI symmetry must follow write semantics, not visual symmetry**: Do not copy insert-buffer metadata or controls into edit buffers unless the underlying SQL semantics really match. Tags such as `default` or `generated` need explicit update semantics before they belong in edit UI.
- **Validation must happen before the editing context is destroyed**: For insert/edit buffers, local validation errors should keep the user in the current buffer so they can repair the value immediately. Do not close the buffer first and then surface the error.

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

- `;;;###autoload` only on: interactive commands (`clutch-mode`, `clutch-repl`, `clutch-execute`, `clutch-dispatch`), `auto-mode-alist` entries.
- Never autoload internal functions, defcustom, or defvar.
- Use `declare-function` for functions from optional dependencies to silence byte-compiler.

## Pre-Submit Review

Before committing significant changes, step back and review the whole diff:

- **No heuristic shortcuts**: If a fix feels "good enough for now", it probably isn't. Either do it correctly or explicitly document why it's deferred in a postmortem.
- **No redundancy**: Check for duplicated logic, dead code, or overlapping abstractions introduced by the change. Remove them.
- **Long-term correctness**: Ask whether the approach holds up under edge cases (filtered views, missing PK, concurrent buffer edits, etc.).
- **Docs in sync**: Any change to key bindings, defaults, workflow, or data structures must update `README.org` and, where applicable, add or update a postmortem.
- **Byte-compile clean**: `(byte-compile-file "clutch.el")` must produce zero warnings.

## Quality Checks

Before releasing, ensure:
- `(byte-compile-file "clutch.el")` produces no warnings.
- All public functions have docstrings.
- Every file starts with `;;; -*- lexical-binding: t; -*-` and ends with `(provide 'pkg)` / `;;; pkg.el ends here`.

## SQL Rewrite Guardrails

- Do not rewrite SQL by brittle raw string insertion of `WHERE` / `ORDER BY` / `LIMIT`.
- Prefer top-level clause-aware transformations with safe fallback behavior.
- For complex queries (CTE / UNION / DISTINCT / window functions), prioritize semantic correctness over aggressive rewriting.
- Keep AST-level rewriting on the roadmap; do not force full AST complexity into small fixes.

## Docs Consistency

- Any change to key bindings, defaults, export behavior, or user-visible workflow must update `README.org` in the same change.
- If code and docs diverge, treat code as source of truth and fix docs immediately.

## JDBC Agent Release Discipline

- `clutch-jdbc-agent-version` and `clutch-jdbc-agent-sha256` are a pair. If one changes, review whether the other must change in the same commit.
- Do not assume a release asset is immutable just because the version string is unchanged. If the published jar bytes change, `clutch-jdbc-agent-sha256` must be updated immediately.
- Prefer bumping the agent version for any released jar content change. Replacing a GitHub release asset in place should be treated as an exceptional repair path, not normal workflow.
- Any release-asset change that affects JDBC startup or installation must update `README.org` and, when the tradeoff is non-obvious, add or update a postmortem.

## Postmortems

The `postmortem/` directory contains design decision records and lessons learned. **Read them before making significant changes.**

Each file is named `NNN-topic.md` and records: background, decision, rationale, alternatives considered, and known limitations.

**Write a postmortem when:**
- Adding or changing a user-visible workflow (e.g., how mutations are committed)
- Choosing between non-obvious architectural approaches (e.g., where to place a guard)
- Integrating an optional dependency (e.g., Embark)
- Reverting or abandoning an approach — especially document *why* it was wrong
- Discovering a known limitation that is deliberately deferred

**What to write:** focus on *why*, not *what*. The code already shows what was done. The record must explain why this approach was chosen over alternatives, what was tried and rejected, and what trade-offs were accepted. A record that only restates the code adds no value.

**Quality bar:** if a future contributor reads the record and still has to guess why a decision was made, it is incomplete.

## Export and Encoding

- Export features that write files must provide explicit encoding behavior and sensible defaults.
- Document Excel compatibility guidance clearly (prefer UTF-8 with BOM; use GBK/CP936 only for legacy workflows).
- Any export-path change must include regression tests for content correctness, and at least one encoding-related path.
