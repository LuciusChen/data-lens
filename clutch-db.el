;;; clutch-db.el --- Database backend protocol facade -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;; This file is part of clutch.

;; clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Backend-agnostic database interface for clutch.
;;
;; Defines a generic API via `cl-defgeneric' that database backends
;; (MySQL, PostgreSQL, etc.) implement via `cl-defmethod'.
;;
;; Each backend provides a connection struct and methods dispatching
;; on that struct type.  clutch.el calls only these generics,
;; never backend-specific functions directly.

;;; Code:

(require 'cl-lib)

(declare-function auth-source-pass-entries "auth-source-pass" ())
(declare-function auth-source-pass-parse-entry "auth-source-pass" (entry))

;;;; Error types

(define-error 'clutch-db-error "Database error")
(define-error 'clutch-query-interrupted "Query interrupted" 'user-error)

;;;; Shared helpers

(defun clutch-db--pass-secret-by-suffix (suffix)
  "Return pass secret from the first entry whose path ends with SUFFIX.
Matches e.g. `dev-mysql' against `mysql/dev-mysql'.
Returns nil when no matching entry is found or auth-source-pass is absent."
  (when (and (fboundp 'auth-source-pass-entries)
             (fboundp 'auth-source-pass-parse-entry))
    (let* ((re (format "\\(^\\|/\\)%s$" (regexp-quote suffix)))
           (entry (cl-find-if (lambda (e) (string-match-p re e))
                              (auth-source-pass-entries))))
      (when entry
        (cdr (assq 'secret (auth-source-pass-parse-entry entry)))))))

(defun clutch-db--normalize-symbol-option (value)
  "Return VALUE normalized to a lowercase symbol, or nil when absent."
  (cond
   ((null value) nil)
   ((symbolp value)
    (intern (downcase (symbol-name value))))
   ((stringp value)
    (intern (downcase value)))
   (t value)))

(defun clutch-db--normalize-mysql-ssl-mode (ssl-mode)
  "Return canonical MySQL SSL-MODE, or signal `clutch-db-error'."
  (pcase (clutch-db--normalize-symbol-option ssl-mode)
    ('nil nil)
    ((or 'disabled 'off) 'disabled)
    (_
     (signal 'clutch-db-error
             (list (format "Unsupported MySQL :ssl-mode %S (supported: disabled)" ssl-mode))))))

(defun clutch-db--normalize-pg-sslmode (sslmode)
  "Return canonical PostgreSQL SSLMODE, or signal `clutch-db-error'."
  (pcase (clutch-db--normalize-symbol-option sslmode)
    ('nil nil)
    ((or 'disable 'prefer 'require 'verify-full)
     (clutch-db--normalize-symbol-option sslmode))
    (_
     (signal 'clutch-db-error
             (list (format
                    "Unsupported PostgreSQL :sslmode %S (supported: disable, prefer, require, verify-full)"
                    sslmode))))))

(defun clutch-db--normalize-mysql-connect-params (params)
  "Return PARAMS normalized for the MySQL backend."
  (let* ((params (copy-sequence params))
         (tls-specified-p (plist-member params :tls))
         (tls (plist-get params :tls))
         (ssl-mode (clutch-db--normalize-mysql-ssl-mode
                    (plist-get params :ssl-mode)))
         (tls-mode (cond
                    (ssl-mode 'disable)
                    (tls-specified-p (if tls 'require 'disable))
                    (t 'default))))
    (when ssl-mode
      (setq params (plist-put params :ssl-mode ssl-mode)))
    (when (and ssl-mode tls-specified-p tls)
      (signal 'clutch-db-error
              (list "Conflicting MySQL TLS options: :tls t cannot be combined with :ssl-mode disabled")))
    (setq params (plist-put params :clutch-tls-mode tls-mode))
    (when (and tls-specified-p (null tls))
      ;; Canonicalize the generic plaintext shortcut to MySQL's explicit name.
      (setq params (plist-put params :ssl-mode 'disabled))
      (cl-remf params :tls))
    params))

(defun clutch-db--normalize-pg-connect-params (params)
  "Return PARAMS normalized for the PostgreSQL backend."
  (let* ((params (copy-sequence params))
         (tls-specified-p (plist-member params :tls))
         (tls (plist-get params :tls))
         (sslmode (clutch-db--normalize-pg-sslmode
                   (plist-get params :sslmode)))
         (tls-mode (pcase sslmode
                     ((or 'disable 'prefer) sslmode)
                     ((or 'require 'verify-full) 'require)
                     (_ (if tls-specified-p
                            (if tls 'require 'disable)
                          'default)))))
    (pcase sslmode
      ('disable
       (when (and tls-specified-p tls)
         (signal 'clutch-db-error
                 (list "Conflicting PostgreSQL TLS options: :tls t cannot be combined with :sslmode disable"))))
      ('prefer
       (when tls-specified-p
         (signal 'clutch-db-error
                 (list "Conflicting PostgreSQL TLS options: :sslmode prefer cannot be combined with :tls"))))
      ((or 'require 'verify-full)
       (when (and tls-specified-p (null tls))
         (signal 'clutch-db-error
                 (list (format "Conflicting PostgreSQL TLS options: :tls nil cannot be combined with :sslmode %s"
                               sslmode))))))
    (setq params (plist-put params :clutch-tls-mode tls-mode))
    (cond
     (sslmode
      (setq params (plist-put params :sslmode sslmode))
      (when tls-specified-p
        (cl-remf params :tls)))
     (tls-specified-p
      ;; Canonicalize the generic boolean shortcut to PostgreSQL's official name.
      (setq params (plist-put params :sslmode (if tls 'require 'disable)))
      (cl-remf params :tls)))
    params))

(defun clutch-db--normalize-connect-params (backend params)
  "Return connection PARAMS normalized for BACKEND."
  (pcase backend
    ('mysql (clutch-db--normalize-mysql-connect-params params))
    ('pg (clutch-db--normalize-pg-connect-params params))
    (_ params)))

;;;; Result struct

(cl-defstruct clutch-db-result
  "A database query result.
CONNECTION is the backend connection object.
COLUMNS is a list of plists (:name STR :type-category SYM) where
:type-category is one of: numeric, blob, json, text, date, time,
datetime, other.
ROWS is a list of lists (one per row).
AFFECTED-ROWS, LAST-INSERT-ID, and WARNINGS are for DML results."
  connection columns rows affected-rows last-insert-id warnings)

;;;; SQL helpers (literal-or-comment awareness)

(defun clutch-db-sql-skip-literal-or-comment (sql pos)
  "If POS in SQL starts a string literal or comment, return position past it.
Handles single-quoted strings (with '' escape), -- line comments, and
/* block comments */.  Double-quoted identifiers and backticks are NOT
treated as literals.  Returns nil when POS is at normal code."
  (let ((len (length sql))
        (ch (and (< pos (length sql)) (aref sql pos))))
    (pcase ch
      (?\' ;; Single-quoted string: scan for unescaped closing quote.
       (cl-loop for i from (1+ pos) below len
                when (= (aref sql i) ?\')
                do (if (and (< (1+ i) len) (= (aref sql (1+ i)) ?\'))
                       (cl-incf i)          ; '' escape, skip pair
                     (cl-return (1+ i)))    ; past closing quote
                finally return len))
      (?-  ;; Possible -- line comment.
       (if (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?-))
           (or (cl-loop for i from (+ pos 2) below len
                        when (= (aref sql i) ?\n) return (1+ i))
               len)
         nil))
      (?/  ;; Possible /* block comment */.
       (if (and (< (1+ pos) len) (= (aref sql (1+ pos)) ?*))
           (let ((end (cl-loop for i from (+ pos 2) below (1- len)
                               when (and (= (aref sql i) ?*)
                                         (= (aref sql (1+ i)) ?/))
                               return (+ i 2))))
             (or end len))
         nil)))))

(defun clutch-db-sql-mask-literal-or-comment (sql)
  "Return a string the same length as SQL with literals/comments blanked.
Single-quoted content (between the quotes) and comment text become spaces.
Quote delimiters are preserved.  Double-quoted identifiers and backticks
are left intact.  Safe for multibyte strings (avoids `aset')."
  (let ((pieces nil)
        (copy-from 0)
        (pos 0)
        (len (length sql)))
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (if (= (aref sql pos) ?\')
              ;; String literal: preserve quote delimiters, blank content.
              (let* ((has-close (and (> skip (1+ pos))
                                    (= (aref sql (1- skip)) ?\')))
                     (content-end (if has-close (1- skip) skip)))
                (push (substring sql copy-from (1+ pos)) pieces)
                (push (make-string (max 0 (- content-end (1+ pos))) ?\s) pieces)
                (when has-close (push "'" pieces))
                (setq copy-from skip pos skip))
            ;; Comment: blank entirely.
            (push (substring sql copy-from pos) pieces)
            (push (make-string (- skip pos) ?\s) pieces)
            (setq copy-from skip pos skip))
        (cl-incf pos)))
    (push (substring sql copy-from) pieces)
    (apply #'concat (nreverse pieces))))

;;;; SQL helpers (top-level clause detection)

(defun clutch-db-sql-find-top-level-clause (sql pattern &optional start)
  "Return start position of top-level PATTERN in SQL, or nil.
PATTERN is matched case-insensitively with word boundaries.
START defaults to 0."
  (let ((pos (or start 0))
        (depth 0)
        (len (length sql))
        (case-fold-search t)
        (re (format "\\b%s\\b" pattern))
        found)
    (while (and (< pos len) (not found))
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (setq pos skip)
        (let ((ch (aref sql pos)))
          (cond
           ((= ch ?\() (cl-incf depth) (cl-incf pos))
           ((= ch ?\)) (cl-decf depth) (cl-incf pos))
           ((and (= depth 0)
                 (string-match re sql pos)
                 (= (match-beginning 0) pos))
            (setq found pos))
           (t (cl-incf pos))))))
    found))

(defun clutch-db-sql-has-top-level-clause-p (sql pattern &optional start)
  "Return non-nil when SQL has top-level PATTERN starting at START."
  (clutch-db-sql-find-top-level-clause sql pattern start))

(defun clutch-db-sql-has-top-level-limit-p (sql)
  "Return non-nil when SQL has a top-level LIMIT clause."
  (clutch-db-sql-has-top-level-clause-p sql "LIMIT"))

(defun clutch-db-sql-has-top-level-offset-p (sql)
  "Return non-nil when SQL has a top-level OFFSET clause."
  (clutch-db-sql-has-top-level-clause-p sql "OFFSET"))

(defun clutch-db-sql-strip-top-level-order-by (sql)
  "Strip a top-level ORDER BY tail from SQL.
Leaves nested ORDER BY clauses inside subqueries or window functions intact."
  (if-let* ((order-pos (clutch-db-sql-find-top-level-clause sql "ORDER\\s-+BY")))
      (string-trim-right (substring sql 0 order-pos))
    sql))

(defun clutch-db--build-limit-offset-paged-sql (base-sql page-num page-size
                                                         order-by escape-fn)
  "Build a LIMIT/OFFSET paginated query from BASE-SQL.
PAGE-NUM is zero-based and PAGE-SIZE is the row count per page.
ORDER-BY is (COL . DIR) or nil.  ESCAPE-FN escapes the column name."
  (if (clutch-db-sql-has-top-level-limit-p base-sql)
      base-sql
    (let* ((trimmed (string-trim-right
                     (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
           (sortable-sql (if order-by
                             (clutch-db-sql-strip-top-level-order-by trimmed)
                           trimmed))
           (offset (* page-num page-size))
           (order-clause (when order-by
                           (format " ORDER BY %s %s"
                                   (funcall escape-fn (car order-by))
                                   (cdr order-by)))))
      (format "%s%s LIMIT %d OFFSET %d"
              sortable-sql (or order-clause "") page-size offset))))

;;;; Generic interface — 18 methods dispatched on connection type

;; Lifecycle

(cl-defgeneric clutch-db-disconnect (conn)
  "Disconnect CONN from the database server.")

(cl-defgeneric clutch-db-live-p (conn)
  "Return non-nil if CONN is still connected and usable."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-error-details (conn)
  "Return structured error details for CONN, or nil."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-clear-error-details (conn)
  "Forget any backend-local structured error details for CONN."
  (ignore conn)
  nil)

(cl-defgeneric clutch-db-init-connection (conn)
  "Perform post-connect initialization on CONN.
For example, SET NAMES utf8mb4 on MySQL.")

(cl-defgeneric clutch-db-manual-commit-p (conn)
  "Return non-nil when CONN is in manual-commit mode.")

(cl-defmethod clutch-db-manual-commit-p ((_conn t))
  "Fallback implementation for backends without manual-commit mode."
  nil)

(cl-defgeneric clutch-db-commit (conn)
  "Commit the current transaction on CONN.")

(cl-defmethod clutch-db-commit ((_conn t))
  "Fallback implementation for backends without explicit commit support."
  nil)

(cl-defgeneric clutch-db-rollback (conn)
  "Roll back the current transaction on CONN.")

(cl-defmethod clutch-db-rollback ((_conn t))
  "Fallback implementation for backends without explicit rollback support."
  nil)

(cl-defgeneric clutch-db-set-auto-commit (conn auto-commit)
  "Set CONN's auto-commit mode.
AUTO-COMMIT non-nil enables auto-commit; nil enables manual-commit.")

(cl-defmethod clutch-db-set-auto-commit ((_conn t) _auto-commit)
  "Signal that the backend does not support runtime auto-commit changes."
  (user-error "This backend does not support runtime auto-commit toggle"))

(cl-defgeneric clutch-db-eager-schema-refresh-p (conn)
  "Return non-nil when CONN should refresh schema synchronously on connect.")

(cl-defmethod clutch-db-eager-schema-refresh-p ((_conn t))
  "Most backends refresh schema immediately after connect."
  t)

(cl-defgeneric clutch-db-completion-sync-columns-p (conn)
  "Return non-nil when completion may synchronously load column metadata for CONN.")

(cl-defmethod clutch-db-completion-sync-columns-p ((_conn t))
  "Most backends can synchronously load column metadata during completion."
  t)

(cl-defgeneric clutch-db-refresh-schema-async (conn callback &optional errback)
  "Start an asynchronous schema refresh for CONN.
CALLBACK receives the table name list on success.  ERRBACK receives
an error message string on failure.  Return non-nil when async refresh
was started, nil when unsupported.")

(cl-defmethod clutch-db-refresh-schema-async ((_conn t) _callback &optional _errback)
  "Backends without asynchronous schema refresh support return nil."
  nil)

(cl-defgeneric clutch-db-column-details-async (conn table callback &optional errback)
  "Start an asynchronous column-detail fetch for TABLE on CONN.
CALLBACK receives the column detail plist list on success.  ERRBACK
receives an error message string on failure.  Return non-nil when async
fetch was started, nil when unsupported.")

(cl-defmethod clutch-db-column-details-async ((_conn t) _table _callback
                                              &optional _errback)
  "Backends without asynchronous column detail support return nil."
  nil)

(cl-defgeneric clutch-db-list-columns-async (conn table callback &optional errback)
  "Start an asynchronous column-name fetch for TABLE on CONN.
CALLBACK receives the column name list on success.  ERRBACK receives an
error message string on failure.  Return non-nil when async fetch was
started, nil when unsupported.")

(cl-defmethod clutch-db-list-columns-async ((_conn t) _table _callback
                                            &optional _errback)
  "Backends without asynchronous column-name support return nil."
  nil)

(defun clutch-db--schedule-idle-metadata-call (conn callback errback fn
                                                    &rest args)
  "Schedule metadata FN for CONN on the main thread once Emacs is idle.
CALLBACK receives the result of calling FN with CONN and ARGS.
ERRBACK receives an error-message string when the work fails."
  (cl-labels
      ((run ()
         (if (clutch-db-live-p conn)
             (if (clutch-db-busy-p conn)
                 (run-with-idle-timer 0 nil #'run)
               (condition-case err
                   (when callback
                     (funcall callback (apply fn conn args)))
                 (error
                  (when errback
                    (funcall errback (error-message-string err))))))
           (when errback
             (funcall errback "Connection closed")))))
    (run-with-idle-timer 0 nil #'run)))

;; Query

(cl-defgeneric clutch-db-query (conn sql)
  "Execute SQL on CONN and return a `clutch-db-result'.")

(cl-defgeneric clutch-db-execute-params (conn sql params)
  "Execute SQL on CONN with positional PARAMS.
SQL uses `?' placeholders.  PARAMS is a list of Elisp values.
Return the same shape as `clutch-db-query'.")

(cl-defmethod clutch-db-execute-params ((conn t) sql params)
  "Fallback parameter execution for CONN by literal substitution.
Substitute PARAMS into SQL before calling `clutch-db-query'."
  (clutch-db-query conn (clutch-db-render-sql-params conn sql params)))

(cl-defgeneric clutch-db-interrupt-query (conn)
  "Interrupt the current query on CONN.
Return non-nil when the query was handed off to a backend-specific
interrupt path and the connection should remain usable.")

(cl-defmethod clutch-db-interrupt-query ((_conn t))
  "Backends without query interrupt support return nil."
  nil)

(cl-defgeneric clutch-db-build-paged-sql (conn base-sql page-num page-size
                                          &optional order-by)
  "Build a paginated SQL query for CONN's dialect.
BASE-SQL is the original query.  PAGE-NUM is 0-based, PAGE-SIZE is
the row limit.  ORDER-BY is (COL-NAME . DIRECTION) or nil.")

;; SQL dialect

(cl-defgeneric clutch-db-escape-identifier (conn name)
  "Escape NAME as a SQL identifier for CONN's dialect.")

(cl-defgeneric clutch-db-escape-literal (conn value)
  "Escape VALUE as a SQL string literal for CONN's dialect.")

(defun clutch-db--param-literal (conn value)
  "Return VALUE rendered as an SQL literal for CONN."
  (cond
   ((null value) "NULL")
   ((numberp value) (number-to-string value))
   ((stringp value) (clutch-db-escape-literal conn value))
   ((and (listp value)
         (clutch-db-format-temporal value))
    (clutch-db-escape-literal conn (clutch-db-format-temporal value)))
   ((or (hash-table-p value) (vectorp value))
    (clutch-db-escape-literal
     conn
     (condition-case nil
         (json-serialize value)
       (error (format "%S" value)))))
   (t
    (clutch-db-escape-literal conn (format "%S" value)))))

(defun clutch-db-substitute-params (sql params render-fn)
  "Return SQL with PARAMS substituted using RENDER-FN.
SQL uses `?' positional placeholders.  PARAMS is a list of parameter values.
RENDER-FN is called once per parameter and must return the replacement string."
  (let ((len (length sql))
        (pos 0)
        (remaining params)
        parts)
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (progn
            (push (substring sql pos skip) parts)
            (setq pos skip))
        (let ((ch (aref sql pos)))
          (if (= ch ??)
              (progn
                (unless remaining
                  (signal 'clutch-db-error
                          (list (format "Not enough parameters for SQL template: %s" sql))))
                (push (funcall render-fn (car remaining)) parts)
                (setq remaining (cdr remaining))
                (cl-incf pos))
            (push (string ch) parts)
            (cl-incf pos)))))
    (when remaining
      (signal 'clutch-db-error
              (list (format "Too many parameters for SQL template: %s" sql))))
    (apply #'concat (nreverse parts))))

(defun clutch-db-render-sql-params (conn sql params)
  "Return SQL rendered for CONN with PARAMS interpolated as SQL literals."
  (clutch-db-substitute-params
   sql params
   (lambda (value)
     (clutch-db--param-literal conn value))))

;; Schema

(cl-defgeneric clutch-db-list-tables (conn)
  "Return a list of table name strings for CONN's current database.")

(cl-defgeneric clutch-db-list-schemas (conn)
  "Return available schema names for CONN, or nil when unsupported.")

(cl-defmethod clutch-db-list-schemas ((_conn t))
  "Backends without schema enumeration support return nil."
  nil)

(cl-defgeneric clutch-db-current-schema (conn)
  "Return the effective current schema for CONN, or nil when not applicable.")

(cl-defmethod clutch-db-current-schema ((_conn t))
  "Default: no current schema abstraction."
  nil)

(cl-defgeneric clutch-db-set-current-schema (conn schema)
  "Switch CONN to SCHEMA for subsequent metadata and query context.")

(cl-defmethod clutch-db-set-current-schema ((_conn t) _schema)
  "Default: runtime schema switching is unsupported."
  (user-error "This backend does not support switching schemas"))

(cl-defgeneric clutch-db-list-table-entries (conn)
  "Return browseable table-like object entries for CONN.
Each entry is a plist containing at least :name and :type, and may also
include :schema, :source-schema, :target-schema, and :target-name.")

(cl-defmethod clutch-db-list-table-entries ((conn t))
  "Default table-entry implementation for CONN.
Derived from `clutch-db-list-tables'."
  (mapcar (lambda (table)
            (list :name table
                  :type "TABLE"))
          (clutch-db-list-tables conn)))

(cl-defgeneric clutch-db-list-columns (conn table)
  "Return a list of column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-complete-tables (conn prefix)
  "Return table name candidates for PREFIX on CONN, or nil when unsupported.")

(cl-defmethod clutch-db-complete-tables ((_conn t) _prefix)
  "Backends without direct completion support return nil."
  nil)

(cl-defgeneric clutch-db-search-table-entries (conn prefix)
  "Return table entry plists matching PREFIX on CONN, or nil when unsupported.")

(cl-defmethod clutch-db-search-table-entries ((conn t) prefix)
  "Default table entry search for CONN and PREFIX.
Derived from `clutch-db-complete-tables'."
  (mapcar (lambda (name) (list :name name :type "TABLE"))
          (or (clutch-db-complete-tables conn prefix) '())))

(cl-defgeneric clutch-db-browseable-object-entries (conn)
  "Return the base browseable object entry list for CONN.
This is the fast object-discovery snapshot used by clutch's object picker.")

(cl-defmethod clutch-db-browseable-object-entries ((conn t))
  "Default browseable-object snapshot for CONN.
Merges direct table-like entries with empty-prefix search-discovered entries."
  (append (clutch-db-list-table-entries conn)
          (clutch-db-search-table-entries conn "")))

(cl-defgeneric clutch-db-complete-columns (conn table prefix)
  "Return column candidates for TABLE and PREFIX on CONN.
Return nil when the backend does not support direct column completion.")

(cl-defmethod clutch-db-complete-columns ((_conn t) _table _prefix)
  "Backends without direct column completion support return nil."
  nil)

(cl-defgeneric clutch-db-show-create-table (conn table)
  "Return the DDL string for TABLE on CONN.")

(cl-defgeneric clutch-db-list-objects (conn category)
  "Return object entry plists for CATEGORY on CONN.
CATEGORY is one of: indexes, sequences, procedures, functions, triggers.")

(cl-defmethod clutch-db-list-objects ((_conn t) _category)
  "Default: return nil when CATEGORY is unsupported."
  nil)

(cl-defgeneric clutch-db-list-objects-async (conn category callback &optional errback)
  "Fetch object entry plists for CATEGORY on CONN asynchronously.
CALLBACK receives the entry plist list on success.  ERRBACK receives an error
message string on failure.  Return non-nil when an async fetch was started.")

(cl-defmethod clutch-db-list-objects-async ((_conn t) _category _callback &optional _errback)
  "Default: asynchronous object loading is unsupported."
  nil)

(cl-defgeneric clutch-db-object-details (conn entry)
  "Return detail data for object ENTRY on CONN.
ENTRY is the full entry plist so the backend can use :identity or
other backend-specific keys as needed.")

(cl-defmethod clutch-db-object-details ((_conn t) _entry)
  "Default: return nil when no detail loader is available."
  nil)

(cl-defgeneric clutch-db-object-source (conn entry)
  "Return source text for source-bearing object ENTRY on CONN.")

(cl-defmethod clutch-db-object-source ((_conn t) _entry)
  "Default: return nil when source is unavailable."
  nil)

(cl-defgeneric clutch-db-show-create-object (conn entry)
  "Return DDL text for non-table object ENTRY on CONN.")

(cl-defmethod clutch-db-show-create-object ((_conn t) _entry)
  "Default: return nil when DDL is unavailable."
  nil)

(cl-defgeneric clutch-db-table-comment (conn table)
  "Return the comment string for TABLE on CONN, or nil if none.")

(cl-defgeneric clutch-db-table-comment-async (conn table callback &optional errback)
  "Start an asynchronous table-comment fetch for TABLE on CONN.
CALLBACK receives the comment string or nil on success.  ERRBACK receives an
error message string on failure.  Return non-nil when async fetch was started,
nil when unsupported.")

(cl-defmethod clutch-db-table-comment-async ((_conn t) _table _callback
                                             &optional _errback)
  "Backends without asynchronous table-comment support return nil."
  nil)

(cl-defgeneric clutch-db-primary-key-columns (conn table)
  "Return a list of primary key column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-foreign-keys (conn table)
  "Return foreign key info for TABLE on CONN.
Returns an alist of (COLUMN-NAME . (:ref-table T :ref-column C)).")

(cl-defgeneric clutch-db-referencing-objects (conn table)
  "Return objects that reference TABLE on CONN.
Each element is an entry plist suitable for object navigation, typically
including at least :name and :type, and optionally :schema / :source-schema.")

(cl-defmethod clutch-db-referencing-objects ((_conn t) _table)
  "Default: return nil when reverse-reference lookup is unsupported."
  nil)

(cl-defgeneric clutch-db-column-details (conn table)
  "Return detailed column info for TABLE on CONN.
Returns a list of plists with keys:
  :name STR  :type STR  :nullable BOOL
  :primary-key BOOL  :foreign-key PLIST-OR-NIL  :comment STR-OR-NIL
Optional keys:
  :default STR-OR-NIL  :generated BOOL")

;; Re-entrancy guard

(cl-defgeneric clutch-db-busy-p (conn)
  "Return non-nil if CONN is currently executing a query.
Used to prevent re-entrant queries from completion timers.")

;; Metadata

(cl-defgeneric clutch-db-user (conn)
  "Return the username string for CONN.")

(cl-defgeneric clutch-db-host (conn)
  "Return the host string for CONN.")

(cl-defgeneric clutch-db-port (conn)
  "Return the port number for CONN.")

(cl-defgeneric clutch-db-database (conn)
  "Return the current database name string for CONN.")

(cl-defgeneric clutch-db-display-name (conn)
  "Return a display name string for CONN's backend type.
E.g., \"MySQL\" or \"PostgreSQL\".")

;;;; Connect dispatcher

(defvar clutch-db--backend-features
  '((mysql  . (:require clutch-db-mysql  :connect-fn clutch-db-mysql-connect))
    (pg     . (:require clutch-db-pg     :connect-fn clutch-db-pg-connect))
    (sqlite . (:require clutch-db-sqlite :connect-fn clutch-db-sqlite-connect)))
  "Alist mapping backend symbols to their feature plists.
Each plist has :require (the feature to load) and :connect-fn
\(a function taking a plist of connection params and returning a conn).")

(defun clutch-db-connect (backend params)
  "Connect to a database using BACKEND with PARAMS.
BACKEND is a symbol (e.g., \\='mysql, \\='pg).
PARAMS is a plist of connection parameters (:host, :port, :user,
:password, :database, etc.).
Returns a backend-specific connection object."
  (if-let* ((feature-plist (alist-get backend clutch-db--backend-features))
             (connect-fn (progn
                           (require (plist-get feature-plist :require))
                           (plist-get feature-plist :connect-fn))))
      (condition-case err
          (let ((conn (funcall connect-fn params)))
            (clutch-db-init-connection conn)
            conn)
        (clutch-db-error
         (signal (car err) (cdr err)))
        (error
         (signal 'clutch-db-error
                 (list (format "Connection failed (%s): %s"
                               backend (error-message-string err))))))
    (user-error "Unknown backend: %s" backend)))

;;;; Temporal value formatting

(defun clutch-db-format-temporal (val)
  "Format temporal plist VAL as a string, or nil if VAL is not temporal.
Handles datetime (with :year and :hours), date (with :year only), and
time (with :hours only) plists returned by the protocol layers."
  (when (listp val)
    (let ((year (plist-get val :year))
          (month (plist-get val :month))
          (day (plist-get val :day))
          (hours (plist-get val :hours))
          (minutes (plist-get val :minutes))
          (seconds (plist-get val :seconds))
          (negative (plist-get val :negative)))
      (cond
       ((and year hours)
        (format "%04d-%02d-%02d %02d:%02d:%02d"
                year month day hours minutes seconds))
       (year
        (format "%04d-%02d-%02d" year month day))
       (hours
        (format "%s%02d:%02d:%02d"
                (if negative "-" "")
                hours minutes seconds))))))

(provide 'clutch-db)
;;; clutch-db.el ends here
