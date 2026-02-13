;;; data-lens-db.el --- Generic database interface for data-lens -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; This file is part of data-lens.

;; data-lens is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; data-lens is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with data-lens.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Backend-agnostic database interface for data-lens.
;;
;; Defines a generic API via `cl-defgeneric' that database backends
;; (MySQL, PostgreSQL, etc.) implement via `cl-defmethod'.
;;
;; Each backend provides a connection struct and methods dispatching
;; on that struct type.  data-lens.el calls only these generics,
;; never backend-specific functions directly.

;;; Code:

(require 'cl-lib)

;;;; Error types

(define-error 'data-lens-db-error "Database error")

;;;; Result struct

(cl-defstruct data-lens-db-result
  "A database query result.
CONNECTION is the backend connection object.
COLUMNS is a list of plists (:name STR :type-category SYM) where
:type-category is one of: numeric, blob, json, text, date, time,
datetime, other.
ROWS is a list of lists (one per row).
AFFECTED-ROWS, LAST-INSERT-ID, and WARNINGS are for DML results."
  connection columns rows affected-rows last-insert-id warnings)

;;;; Generic interface — 18 methods dispatched on connection type

;; Lifecycle

(cl-defgeneric data-lens-db-disconnect (conn)
  "Disconnect CONN from the database server.")

(cl-defgeneric data-lens-db-live-p (conn)
  "Return non-nil if CONN is still connected and usable.")

(cl-defgeneric data-lens-db-init-connection (conn)
  "Perform post-connect initialization on CONN.
For example, SET NAMES utf8mb4 on MySQL.")

;; Query

(cl-defgeneric data-lens-db-query (conn sql)
  "Execute SQL on CONN and return a `data-lens-db-result'.")

(cl-defgeneric data-lens-db-build-paged-sql (conn base-sql page-num page-size
                                                  &optional order-by)
  "Build a paginated SQL query for CONN's dialect.
BASE-SQL is the original query.  PAGE-NUM is 0-based, PAGE-SIZE is
the row limit.  ORDER-BY is (COL-NAME . DIRECTION) or nil.")

;; SQL dialect

(cl-defgeneric data-lens-db-escape-identifier (conn name)
  "Escape NAME as a SQL identifier for CONN's dialect.")

(cl-defgeneric data-lens-db-escape-literal (conn value)
  "Escape VALUE as a SQL string literal for CONN's dialect.")

;; Schema

(cl-defgeneric data-lens-db-list-tables (conn)
  "Return a list of table name strings for CONN's current database.")

(cl-defgeneric data-lens-db-list-columns (conn table)
  "Return a list of column name strings for TABLE on CONN.")

(cl-defgeneric data-lens-db-show-create-table (conn table)
  "Return the DDL string for TABLE on CONN.")

(cl-defgeneric data-lens-db-primary-key-columns (conn table)
  "Return a list of primary key column name strings for TABLE on CONN.")

(cl-defgeneric data-lens-db-foreign-keys (conn table)
  "Return foreign key info for TABLE on CONN.
Returns an alist of (COLUMN-NAME . (:ref-table T :ref-column C)).")

;; Re-entrancy guard

(cl-defgeneric data-lens-db-busy-p (conn)
  "Return non-nil if CONN is currently executing a query.
Used to prevent re-entrant queries from completion timers.")

;; Metadata

(cl-defgeneric data-lens-db-user (conn)
  "Return the username string for CONN.")

(cl-defgeneric data-lens-db-host (conn)
  "Return the host string for CONN.")

(cl-defgeneric data-lens-db-port (conn)
  "Return the port number for CONN.")

(cl-defgeneric data-lens-db-database (conn)
  "Return the current database name string for CONN.")

(cl-defgeneric data-lens-db-display-name (conn)
  "Return a display name string for CONN's backend type.
E.g., \"MySQL\" or \"PostgreSQL\".")

;;;; Connect dispatcher

(defvar data-lens-db--backend-features
  '((mysql . (:require data-lens-db-mysql :connect-fn data-lens-db-mysql-connect))
    (pg    . (:require data-lens-db-pg    :connect-fn data-lens-db-pg-connect)))
  "Alist mapping backend symbols to their feature plists.
Each plist has :require (the feature to load) and :connect-fn
\(a function taking a plist of connection params and returning a conn).")

(defun data-lens-db-connect (backend params)
  "Connect to a database using BACKEND with PARAMS.
BACKEND is a symbol (e.g., \\='mysql, \\='pg).
PARAMS is a plist of connection parameters (:host, :port, :user,
:password, :database, etc.).
Returns a backend-specific connection object."
  (let ((feature-plist (alist-get backend data-lens-db--backend-features)))
    (unless feature-plist
      (signal 'data-lens-db-error
              (list (format "Unknown backend: %s" backend))))
    (require (plist-get feature-plist :require))
    (let ((connect-fn (plist-get feature-plist :connect-fn)))
      (condition-case err
          (let ((conn (funcall connect-fn params)))
            (data-lens-db-init-connection conn)
            conn)
        (error
         (signal 'data-lens-db-error
                 (list (format "Connection failed (%s): %s"
                               backend (error-message-string err)))))))))

(provide 'data-lens-db)
;;; data-lens-db.el ends here
