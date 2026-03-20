# clutch Development Guide

Elisp best practices distilled from llm.el, magit, consult, eglot, vertico/marginalia.

## Core Principles

- **Question every abstraction**: Before adding a layer, file, or indirection, ask whether it solves a current problem. If the answer is hypothetical, do not add it.
- **Simplify relentlessly**: Three similar lines are better than a premature abstraction. A single large file is better than several tiny files with unclear boundaries.
- **Fewer files, clearer boundaries**: Split only when a file has a genuinely distinct responsibility. Never split for cosmetic reasons.
- **Delete, don't deprecate**: Remove unused code entirely. No backward-compatibility shims, re-exports, or "removed" comments.
- **Converge UX**: Prefer one clear entry point and one consistent behavior model over overlapping commands or branchy mode-specific behavior. Wrapper commands are fine, but they must share one resolution path, one action registry, and one default-action model.

## Diagnosis and Change Discipline

- **Find the root cause before changing behavior**: Do not patch UI timing, cache invalidation, or command flow until you can name the failing layer and explain why it is responsible.
- **One failed fix narrows the hypothesis**: If the first attempted fix does not hold, reduce the hypothesis space and gather evidence. Do not stack another speculative patch on top.
- **Two failed fixes stop the patching loop**: After two failed fixes on the same issue, stop changing behavior and switch to diagnosis only.
- **Fix the right layer**: If the real problem belongs in the JDBC agent, protocol code, cache model, or connection lifecycle, move the fix there instead of compensating in the UI layer.
- **Stabilize workflow changes before coding**: For any change that alters a primary entry point, default action, or action menu, write a short design note first. Keep object resolution, action definition, and action presentation separate.
- **Keep experiments narrow**: Start new directions with the smallest slice that proves the workflow is worth having. Do not expand scope before the first slice shows real user value.

## Architecture and Implementation

- **Interface / implementation separation**: `mysql.el` and `pg.el` are pure protocol libraries with no UI. `clutch.el` depends on `clutch-db.el`, not protocol layers directly.
- **Single responsibility per file**: Do not mix protocol code with rendering code.
- **No side effects on load**: Loading a file must not alter Emacs behavior. Activation must be explicit.
- **Reuse Emacs infrastructure**: Use `completing-read`, `special-mode`, `text-property-search-forward`, standard hooks, and other stock primitives.
- **Public naming**: `clutch-` for UI, `mysql-` / `pg-` for protocol. No double dash for public API.
- **Private naming**: `clutch--`, `mysql--`, `pg--`. Never call private symbols from outside the defining file.
- **Predicates**: Multi-word predicate names end in `-p`.
- **Unused args**: Prefix with `_`.
- **Prefer flat control flow**: Avoid deep `let` → `if` → `let` nesting. Use `if-let*`, `when-let*`, `pcase`, and `pcase-let`.
- **Prefer `cl-loop` for non-trivial accumulation**: Use it instead of `dolist` + manual accumulators or over-clever folds.
- **Use the right error type**: `user-error` for user-caused problems; `error` for programmer bugs; `condition-case` for recoverable failures.
- **State placement**: `defvar-local` for buffer state, plain `defvar` for shared state, `defcustom` for user options. Major modes must make their state buffer-local.
- **Mode definitions**: Read-only UI buffers derive from `special-mode`; editing buffers derive from the right parent (`sql-mode`, `comint-mode`, etc.). Register buffer-local hooks in the mode body with LOCAL=`t`.
- **Rendering discipline**: Use text properties for data-bearing annotations and overlays only for ephemeral visuals. Build render buffers from cached data, not by reparsing displayed text.
- **Function design**: Keep functions short, separate pure computation from display mutation, and keep interactive commands thin.

## Completion and Object Workflow

- Always use standard `completing-read`.
- Completion-at-point functions must return quickly and use `:exclusive 'no`.
- Add CAPFs buffer-locally via `add-hook` with LOCAL=`t`.
- Keep object resolution, action definition, and action presentation separate. Embark and Transient are presentation layers, not independent business logic systems.

## Mutation Workflow Convergence

- **One staged-mutation vocabulary everywhere**: Footer, transient labels, help text, and `README.org` must use the same staged-edit / staged-delete / staged-insert terminology.
- **Identity must converge fully**: If pending state becomes PK-based, every lookup and render path must also become PK-based.
- **Preview must show real execution payload**: A command named `Preview execution` must preview what would actually run.
- **Nearby workflows should share helpers**: Insert and edit flows should reuse completion, temporal helpers, and validation rules when semantics match.
- **UI symmetry must follow SQL semantics**: Do not copy insert-buffer metadata or controls into edit buffers unless update semantics truly match.
- **Validation must happen before context is destroyed**: Keep the user in the current insert/edit buffer when local validation fails.

## Version Baseline

- `clutch` targets **Emacs 28.1+** for the native MySQL/PostgreSQL backends.
- The SQLite backend requires **Emacs 29.1+** because it depends on built-in `sqlite-*` functions.
- The JDBC path depends on `clutch-jdbc-agent`, whose published baseline is **Java 17+**.
- Do not silently raise any baseline. If a change requires a higher Emacs or Java version, update:
  - `README.org`
  - relevant release/version metadata
  - a postmortem explaining why the higher baseline is justified

## SQL Rewrite Guardrails

- Do not rewrite SQL by brittle raw string insertion of `WHERE`, `ORDER BY`, or `LIMIT`.
- Prefer top-level clause-aware transformations with safe fallback behavior.
- For CTE / UNION / DISTINCT / window-function queries, prioritize semantic correctness over aggressive rewriting.
- Keep AST-level rewriting on the roadmap; do not force full AST complexity into small fixes.

## Documentation and Release Records

- Any change to key bindings, defaults, export behavior, or user-visible workflow must update `README.org` in the same change.
- If code and docs diverge, treat code as source of truth and fix docs immediately.
- `clutch-jdbc-agent-version` and `clutch-jdbc-agent-sha256` are a pair. If one changes, review whether the other must change in the same commit.
- Do not assume a release asset is immutable just because the version string is unchanged. If the jar bytes change, update `clutch-jdbc-agent-sha256` immediately.
- Prefer bumping the agent version for released jar content changes. Replacing a GitHub release asset in place is an exceptional repair path, not normal workflow.
- Any release-asset change affecting JDBC startup or installation must update `README.org` and, when the tradeoff is non-obvious, add or update a postmortem.
- The `postmortem/` directory records design decisions and lessons learned. Read relevant records before significant changes.
- Write a postmortem when:
  - adding or changing a user-visible workflow
  - choosing between non-obvious architectural approaches
  - integrating an optional dependency
  - reverting or abandoning an approach
  - deliberately deferring a known limitation
- Postmortems must explain **why**, not restate the code.

## Quality and Release Checks

- `(byte-compile-file "clutch.el")` must produce zero warnings.
- All public functions must have docstrings.
- Every file must start with `;;; -*- lexical-binding: t; -*-` and end with `(provide 'pkg)` / `;;; pkg.el ends here`.
- Export features that write files must provide explicit encoding behavior and sensible defaults.
- Document Excel compatibility guidance clearly.
- Any export-path change must include regression tests for content correctness and at least one encoding-related path.

## Pre-Commit Checklist (Mandatory)

Every commit must pass all of these steps.

### 1. Read the full diff

```bash
git diff HEAD
```

Read every changed line before committing.

### 2. Run all test files

```bash
# Main UI/logic tests
emacs -batch -L . -l ert -l clutch \
  -l test/clutch-test.el \
  --eval '(ert-run-tests-batch-and-exit)'

# JDBC backend tests
emacs -batch -L . -l ert -l clutch-db-jdbc \
  -l test/clutch-db-test.el \
  --eval '(ert-run-tests-batch-and-exit "clutch-db-test-jdbc")'
```

### 3. Byte-compile with zero warnings

```bash
emacs -batch -L . -f batch-byte-compile clutch.el
```

### 4. Update tests when behavior changes

When a function's behavior changes intentionally, search all test files for existing tests of that function and update them before committing:

```bash
grep -n "function-name" test/clutch-test.el test/clutch-db-test.el
```
