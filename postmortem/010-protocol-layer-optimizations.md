# 010 — Protocol Layer Optimizations

## Background

Code review of `mysql.el` and `pg.el` identified four specific inefficiencies. All four were addressed with targeted changes, each accompanied by correctness tests and benchmark stubs.

---

## Opt 1 — IEEE 754 Double: Compile-Time Constants (`mysql.el`)

### Problem
`mysql--ieee754-double-to-float` called `(expt 2.0 N)` at runtime for N ∈ {8,16,24,32,40,48,52} on every double column decoded. `expt` is a general-purpose function not inlined by the byte compiler for float arguments.

### Decision
Replace with literal float constants computed once. No logic changes — purely a substitution of expressions with their pre-evaluated results.

### Why not touch single-precision
`mysql--ieee754-single-to-float` already uses `logior`/`ash` for integer bit assembly with a single `ldexp` call. No `expt`. It was already efficient.

### Expected benefit
10–25% fewer Lisp function calls per double decode. Compounds for result sets with many double-typed columns.

---

## Opt 2 — PG Type Dispatch: Hash Table (`pg.el`)

### Problem
`pg--parse-value` used `pcase` with `(pred (= pg-oid-N))` — O(n) scan per call. For a 10k-row × 20-column result set (200k calls), every call that doesn't match early walks up to 10 predicate evaluations.

### Decision
Pre-build a `defconst pg--oid-dispatch-table` hash table at load time with `:test 'eql`. Replace the pcase body with a single `gethash` call. `pg-type-parsers` alist is still checked first — it is the public user-extension point and must retain precedence.

### JSON availability
`json-parse-string` availability is captured at `defconst` eval time (load time). Users who load a JSON library after `pg.el` and need runtime override must use `pg-type-parsers`. Documented in the defconst docstring.

### Forward-reference
`pg--oid-dispatch-table` references `pg--parse-date`, `pg--parse-time`, `pg--parse-timestamp`. Placed after those three function definitions to avoid forward references.

### Expected benefit
O(1) per lookup vs O(n). Measurable for large mixed-type result sets.

---

## Opt 3 — PBKDF2 Outer Loop Condition (`pg.el`)

### Problem
The `while` condition reconstructed the full derived key string on every outer iteration to measure its length:
```elisp
(while (< (length (apply #'concat (nreverse (copy-sequence dk)))) key-length)
```
For SCRAM-SHA-256 (`key-length=32`, `hlen=32`), the loop runs exactly once. The reconstruction allocated and discarded 2 strings per iteration.

### Decision
Replace with a counter check: `(while (< (* (1- block) hlen) key-length))`. `block` starts at 1; the condition is equivalent to "have we produced enough hlen-byte blocks?" No string allocation needed.

### Honest performance note
The outer loop for the standard case runs exactly once. The fix eliminates 2 string allocations — sub-microsecond improvement. The inner loop (4095 iterations of `pg--hmac-sha256` calling C-level `secure-hash` twice each) dominates completely. The primary value of this change is: (a) correctness for future callers with `key-length > hlen`, (b) the condition now matches standard PBKDF2 pseudocode, and (c) no redundant allocation.

---

## Opt 4 — Buffer-as-Queue: Read-Position Tracking (`mysql.el` + `pg.el`)

### Problem
`mysql--read-bytes` and `pg--read-bytes` called `delete-region` on every read. The Emacs buffer gap implementation means each deletion may shift O(remaining-data) bytes. For a 10k-row × 10-column result set (~100k `delete-region` calls), total bytes moved is O(n²/2) where n = initial buffer size.

### Decision
Add a `read-offset` integer field to `mysql-conn` and `pg-conn`. `read-bytes` advances the offset without touching buffer content. A single bulk `delete-region` per complete packet/message (`mysql--read-packet`, `pg--read-message`) flushes all consumed bytes at once. This collapses O(n²) gap moves to O(n) — one move of n bytes total.

### Edge case audit
- **Process filter**: appends at `(point-max)` — unaffected by offset tracking in the consumed prefix.
- **Byte-by-byte reads** (`read-string-nul`): accumulate offset per byte; flushed at packet/message boundary.
- **Multi-packet MySQL messages** (len=0xFFFFFF): offset accumulates across all fragment reads; single flush at end of `mysql--read-packet`.
- **`erase-buffer`**: always paired with `(setf read-offset 0)` to keep the invariant: offset is always relative to current `(point-min)`.
- **TLS**: operates below the buffer layer — no interaction.

### What was considered but deferred
Replacing the process buffer entirely with a pure Elisp string queue (avoid buffer gap overhead entirely). Rejected: would require rewriting the process filter and `accept-process-output` integration. The bulk-delete approach achieves most of the benefit with minimal risk.

### Expected benefit
For large result sets: baseline O(n²/2) gap moves collapse to O(n). Expected 2x–10x wall-clock improvement for queries returning 10k+ rows. Negligible for single-row queries.
