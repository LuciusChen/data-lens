;;; clutch-db-sqlite.el --- SQLite backend for clutch-db -*- lexical-binding: t; -*-

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

;; SQLite backend for the clutch generic database interface.
;; Requires Emacs 29+ (built-in sqlite support).
;; Implements all `clutch-db-*' generics by dispatching on
;; `clutch-db-sqlite-conn'.
;;
;; Connection profile example:
;;   ("my-db" . (:database "/path/to/my.db" :backend sqlite))

;;; Code:

(require 'clutch-db)

(declare-function sqlite-available-p "sqlite" ())
(declare-function sqlite-open        "sqlite" (file))
(declare-function sqlite-close       "sqlite" (db))
(declare-function sqlite-execute     "sqlite" (db query &optional values))
(declare-function sqlite-select      "sqlite" (db query &optional values return-type))
(declare-function sqlitep            "sqlite" (object))

;;;; Connection struct

(cl-defstruct clutch-db-sqlite-conn
  "A clutch SQLite connection."
  handle    ;; raw sqlite-handle from sqlite-open
  database  ;; file path string, for display
  busy      ;; boolean re-entrancy guard
  closed)   ;; boolean, t after sqlite-close

;;;; Connect function

(defun clutch-db-sqlite-connect (params)
  "Connect to SQLite using PARAMS plist.
PARAMS keys: :database (file path or \":memory:\", required).
Use \":memory:\" for a transient in-memory database."
  (unless (and (fboundp 'sqlite-available-p) (sqlite-available-p))
    (signal 'clutch-db-error (list "SQLite requires Emacs 29+")))
  (let ((db (plist-get params :database)))
    (unless db
      (signal 'clutch-db-error (list "Missing :database parameter")))
    (condition-case err
        (make-clutch-db-sqlite-conn
         :handle   (sqlite-open db)
         :database db
         :busy     nil
         :closed   nil)
      (sqlite-error
       (signal 'clutch-db-error (list (error-message-string err)))))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn clutch-db-sqlite-conn))
  "Disconnect SQLite CONN."
  (condition-case nil
      (sqlite-close (clutch-db-sqlite-conn-handle conn))
    (sqlite-error nil))
  (setf (clutch-db-sqlite-conn-closed conn) t))

(cl-defmethod clutch-db-live-p ((conn clutch-db-sqlite-conn))
  "Return non-nil if SQLite CONN is live."
  (and conn
       (not (clutch-db-sqlite-conn-closed conn))
       (fboundp 'sqlitep)
       (sqlitep (clutch-db-sqlite-conn-handle conn))))

(cl-defmethod clutch-db-init-connection ((conn clutch-db-sqlite-conn))
  "Initialize SQLite CONN: enable foreign key enforcement."
  (sqlite-execute (clutch-db-sqlite-conn-handle conn)
                  "PRAGMA foreign_keys = ON"))

;;;; Type helpers

(defun clutch-db-sqlite--infer-category (value)
  "Infer type-category symbol from a SQLite VALUE."
  (if (or (integerp value) (floatp value)) 'numeric 'text))

(defun clutch-db-sqlite--columns-from-names (col-names rows)
  "Build clutch-db column plists from COL-NAMES and data ROWS."
  (let ((first-row (car rows)))
    (cl-loop for name in col-names
             for i from 0
             for value = (and first-row (nth i first-row))
             collect (list :name name
                           :type-category
                           (clutch-db-sqlite--infer-category value)))))

;;;; Query helpers

(defun clutch-db-sqlite--select-p (sql)
  "Return non-nil if SQL is a statement that returns rows."
  (let ((case-fold-search t))
    (string-match-p "\\`\\s-*\\(SELECT\\|WITH\\|EXPLAIN\\|PRAGMA\\)" sql)))

(defun clutch-db-sqlite--run-select (handle sql)
  "Execute a SELECT-like SQL on HANDLE; return a `clutch-db-result'."
  (let* ((raw      (sqlite-select handle sql nil 'full))
         (col-names (car raw))
         (rows     (cdr raw))
         (cols     (clutch-db-sqlite--columns-from-names col-names rows)))
    (make-clutch-db-result
     :connection handle :columns cols :rows rows
     :affected-rows nil :last-insert-id nil :warnings nil)))

(defun clutch-db-sqlite--run-dml (handle sql)
  "Execute a DML SQL on HANDLE; return a `clutch-db-result'."
  (make-clutch-db-result
   :connection handle :columns nil :rows nil
   :affected-rows (sqlite-execute handle sql)
   :last-insert-id nil :warnings nil))

;;;; Query methods

(cl-defmethod clutch-db-query ((conn clutch-db-sqlite-conn) sql)
  "Execute SQL on SQLite CONN, returning a `clutch-db-result'."
  (let ((handle (clutch-db-sqlite-conn-handle conn)))
    (setf (clutch-db-sqlite-conn-busy conn) t)
    (unwind-protect
        (condition-case err
            (if (clutch-db-sqlite--select-p sql)
                (clutch-db-sqlite--run-select handle sql)
              (clutch-db-sqlite--run-dml handle sql))
          (sqlite-error
           (signal 'clutch-db-error (list (error-message-string err)))))
      (setf (clutch-db-sqlite-conn-busy conn) nil))))

(cl-defmethod clutch-db-build-paged-sql ((_conn clutch-db-sqlite-conn)
                                          base-sql page-num page-size
                                          &optional order-by)
  "Build a paginated SQL query for SQLite."
  (let ((case-fold-search t))
    (if (string-match-p "\\bLIMIT\\b" base-sql)
        base-sql
      (let* ((trimmed (string-trim-right
                       (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
             (offset (* page-num page-size))
             (order-clause
              (when order-by
                (format " ORDER BY \"%s\" %s"
                        (replace-regexp-in-string "\"" "\"\"" (car order-by))
                        (cdr order-by)))))
        (format "%s%s LIMIT %d OFFSET %d"
                trimmed (or order-clause "") page-size offset)))))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-db-sqlite-conn) name)
  "Escape NAME as a SQLite identifier (double-quoted)."
  (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name)))

(cl-defmethod clutch-db-escape-literal ((_conn clutch-db-sqlite-conn) value)
  "Escape VALUE as a SQLite string literal (single-quoted)."
  (format "'%s'" (replace-regexp-in-string "'" "''" value)))

;;;; Schema helpers

(defun clutch-db-sqlite--escape-id (name)
  "Double-quote SQLite identifier NAME."
  (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name)))

(defun clutch-db-sqlite--escape-lit (value)
  "Single-quote SQLite string literal VALUE."
  (format "'%s'" (replace-regexp-in-string "'" "''" value)))

(defun clutch-db-sqlite--pragma (handle pragma-sql)
  "Run PRAGMA-SQL on HANDLE; return rows (list of lists)."
  (condition-case _err
      (sqlite-select handle pragma-sql)
    (sqlite-error nil)))

;;;; Schema methods

(cl-defmethod clutch-db-list-tables ((conn clutch-db-sqlite-conn))
  "Return table names for the SQLite database in CONN."
  (condition-case err
      (let* ((handle (clutch-db-sqlite-conn-handle conn))
             (rows (sqlite-select
                    handle
                    "SELECT name FROM sqlite_master \
WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")))
        (mapcar #'car rows))
    (sqlite-error
     (signal 'clutch-db-error (list (error-message-string err))))))

(cl-defmethod clutch-db-list-columns ((conn clutch-db-sqlite-conn) table)
  "Return column names for TABLE on SQLite CONN."
  (condition-case err
      (let* ((handle (clutch-db-sqlite-conn-handle conn))
             (rows (clutch-db-sqlite--pragma
                    handle
                    (format "PRAGMA table_info(%s)"
                            (clutch-db-sqlite--escape-id table)))))
        (mapcar (lambda (row) (nth 1 row)) rows))
    (sqlite-error
     (signal 'clutch-db-error (list (error-message-string err))))))

(cl-defmethod clutch-db-show-create-table ((conn clutch-db-sqlite-conn) table)
  "Return the DDL for TABLE on SQLite CONN."
  (condition-case err
      (let* ((handle (clutch-db-sqlite-conn-handle conn))
             (rows (sqlite-select
                    handle
                    (format "SELECT sql FROM sqlite_master \
WHERE type='table' AND name=%s"
                            (clutch-db-sqlite--escape-lit table)))))
        (or (caar rows) (format "-- No DDL found for %s" table)))
    (sqlite-error
     (signal 'clutch-db-error (list (error-message-string err))))))

(cl-defmethod clutch-db-table-comment ((_conn clutch-db-sqlite-conn) _table)
  "Return nil; SQLite does not support table comments."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-db-sqlite-conn) table)
  "Return primary key column names for TABLE on SQLite CONN."
  ;; table_info row: (cid name type notnull dflt_value pk)
  ;; pk is 1-based position in composite PK; 0 means not in PK.
  (let* ((handle (clutch-db-sqlite-conn-handle conn))
         (rows   (clutch-db-sqlite--pragma
                  handle
                  (format "PRAGMA table_info(%s)"
                          (clutch-db-sqlite--escape-id table)))))
    (cl-loop for row in rows
             when (> (nth 5 row) 0)
             collect (nth 1 row))))

(defun clutch-db-sqlite--fk-alist (handle table)
  "Return FK alist for TABLE from HANDLE.
Result: ((from-col :ref-table T :ref-column C) ...)"
  ;; foreign_key_list row: (id seq table from to on_update on_delete match)
  (let ((rows (clutch-db-sqlite--pragma
               handle
               (format "PRAGMA foreign_key_list(%s)"
                       (clutch-db-sqlite--escape-id table)))))
    (cl-loop for row in rows
             collect (cons (nth 3 row)
                           (list :ref-table  (nth 2 row)
                                 :ref-column (nth 4 row))))))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-db-sqlite-conn) table)
  "Return foreign key info for TABLE on SQLite CONN."
  (condition-case _err
      (clutch-db-sqlite--fk-alist (clutch-db-sqlite-conn-handle conn) table)
    (sqlite-error nil)))

(defun clutch-db-sqlite--column-detail (row pk-cols fks)
  "Convert a table_info ROW to a clutch-db column plist.
PK-COLS is a list of pk column names.  FKS is an FK alist."
  ;; Row: (cid name type notnull dflt_value pk)
  (pcase-let ((`(,_cid ,name ,type ,notnull ,_dflt ,_pk) row))
    (list :name        name
          :type        (downcase (or type "text"))
          :nullable    (= notnull 0)
          :primary-key (and (member name pk-cols) t)
          :foreign-key (cdr (assoc name fks))
          :comment     nil)))

(cl-defmethod clutch-db-column-details ((conn clutch-db-sqlite-conn) table)
  "Return detailed column info for TABLE on SQLite CONN."
  (condition-case _err
      (let* ((handle  (clutch-db-sqlite-conn-handle conn))
             (rows    (clutch-db-sqlite--pragma
                       handle
                       (format "PRAGMA table_info(%s)"
                               (clutch-db-sqlite--escape-id table))))
             (pk-cols (clutch-db-primary-key-columns conn table))
             (fks     (clutch-db-sqlite--fk-alist handle table)))
        (mapcar (lambda (row)
                  (clutch-db-sqlite--column-detail row pk-cols fks))
                rows))
    (sqlite-error nil)))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn clutch-db-sqlite-conn))
  "Return non-nil if SQLite CONN is executing a query."
  (clutch-db-sqlite-conn-busy conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((_conn clutch-db-sqlite-conn))
  "Return nil; SQLite has no user concept."
  nil)

(cl-defmethod clutch-db-host ((_conn clutch-db-sqlite-conn))
  "Return nil; SQLite is file-based with no network host."
  nil)

(cl-defmethod clutch-db-port ((_conn clutch-db-sqlite-conn))
  "Return nil; SQLite has no network port."
  nil)

(cl-defmethod clutch-db-database ((conn clutch-db-sqlite-conn))
  "Return the database file path for SQLite CONN."
  (clutch-db-sqlite-conn-database conn))

(cl-defmethod clutch-db-display-name ((_conn clutch-db-sqlite-conn))
  "Return \"SQLite\" as the display name."
  "SQLite")

(provide 'clutch-db-sqlite)
;;; clutch-db-sqlite.el ends here
