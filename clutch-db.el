;;; clutch-db.el --- Generic database interface for clutch -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

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
      (let ((ch (aref sql pos)))
        (cond
         ((= ch ?\() (cl-incf depth) (cl-incf pos))
         ((= ch ?\)) (cl-decf depth) (cl-incf pos))
         ((and (= depth 0)
               (string-match re sql pos)
               (= (match-beginning 0) pos))
          (setq found pos))
         (t (cl-incf pos)))))
    found))

(defun clutch-db-sql-has-top-level-clause-p (sql pattern &optional start)
  "Return non-nil when SQL has top-level PATTERN."
  (not (null (clutch-db-sql-find-top-level-clause sql pattern start))))

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

;;;; Generic interface — 18 methods dispatched on connection type

;; Lifecycle

(cl-defgeneric clutch-db-disconnect (conn)
  "Disconnect CONN from the database server.")

(cl-defgeneric clutch-db-live-p (conn)
  "Return non-nil if CONN is still connected and usable.")

(cl-defgeneric clutch-db-init-connection (conn)
  "Perform post-connect initialization on CONN.
For example, SET NAMES utf8mb4 on MySQL.")

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

;; Query

(cl-defgeneric clutch-db-query (conn sql)
  "Execute SQL on CONN and return a `clutch-db-result'.")

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

;; Schema

(cl-defgeneric clutch-db-list-tables (conn)
  "Return a list of table name strings for CONN's current database.")

(cl-defgeneric clutch-db-list-columns (conn table)
  "Return a list of column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-complete-tables (conn prefix)
  "Return table name candidates for PREFIX on CONN, or nil when unsupported.")

(cl-defmethod clutch-db-complete-tables ((_conn t) _prefix)
  "Backends without direct completion support return nil."
  nil)

(cl-defgeneric clutch-db-complete-columns (conn table prefix)
  "Return column candidates for TABLE and PREFIX on CONN.
Return nil when the backend does not support direct column completion.")

(cl-defmethod clutch-db-complete-columns ((_conn t) _table _prefix)
  "Backends without direct column completion support return nil."
  nil)

(cl-defgeneric clutch-db-show-create-table (conn table)
  "Return the DDL string for TABLE on CONN.")

(cl-defgeneric clutch-db-table-comment (conn table)
  "Return the comment string for TABLE on CONN, or nil if none.")

(cl-defgeneric clutch-db-primary-key-columns (conn table)
  "Return a list of primary key column name strings for TABLE on CONN.")

(cl-defgeneric clutch-db-foreign-keys (conn table)
  "Return foreign key info for TABLE on CONN.
Returns an alist of (COLUMN-NAME . (:ref-table T :ref-column C)).")

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
  (cond
   ((and (listp val) (plist-get val :year) (plist-get val :hours))
    (format "%04d-%02d-%02d %02d:%02d:%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   ((and (listp val) (plist-get val :year))
    (format "%04d-%02d-%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)))
   ((and (listp val) (plist-get val :hours))
    (format "%s%02d:%02d:%02d"
            (if (plist-get val :negative) "-" "")
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))))

(provide 'clutch-db)
;;; clutch-db.el ends here
