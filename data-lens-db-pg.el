;;; data-lens-db-pg.el --- PostgreSQL backend for data-lens-db -*- lexical-binding: t; -*-

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

;; PostgreSQL backend for the data-lens generic database interface.
;; Implements all `data-lens-db-*' generics by dispatching on `pg-conn'.

;;; Code:

(require 'data-lens-db)
(require 'pg)

;;;; OID → type-category mapping

(defconst data-lens-db-pg--type-category-alist
  `((,pg-oid-int2      . numeric)
    (,pg-oid-int4      . numeric)
    (,pg-oid-int8      . numeric)
    (,pg-oid-float4    . numeric)
    (,pg-oid-float8    . numeric)
    (,pg-oid-numeric   . numeric)
    (,pg-oid-bool      . numeric)
    (,pg-oid-json      . json)
    (,pg-oid-jsonb     . json)
    (,pg-oid-bytea     . blob)
    (,pg-oid-date      . date)
    (,pg-oid-time      . time)
    (,pg-oid-timestamp . datetime)
    (,pg-oid-timestamptz . datetime))
  "Alist mapping PostgreSQL OIDs to type-category symbols.")

(defun data-lens-db-pg--type-category (oid)
  "Map a PostgreSQL type OID to a type-category symbol."
  (or (alist-get oid data-lens-db-pg--type-category-alist)
      'text))

(defun data-lens-db-pg--convert-columns (pg-columns)
  "Convert pg.el column plists to data-lens-db column plists."
  (mapcar (lambda (col)
            (list :name (plist-get col :name)
                  :type-category (data-lens-db-pg--type-category
                                  (plist-get col :type-oid))))
          pg-columns))

(defun data-lens-db-pg--wrap-result (pg-result)
  "Convert a `pg-result' to a `data-lens-db-result'."
  (let ((cols (pg-result-columns pg-result)))
    (make-data-lens-db-result
     :connection (pg-result-connection pg-result)
     :columns (when cols (data-lens-db-pg--convert-columns cols))
     :rows (pg-result-rows pg-result)
     :affected-rows (pg-result-affected-rows pg-result)
     :last-insert-id nil
     :warnings nil)))

;;;; Connect function

(defun data-lens-db-pg-connect (params)
  "Connect to PostgreSQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls."
  (condition-case err
      (apply #'pg-connect
             (cl-loop for (k v) on params by #'cddr
                      unless (memq k '(:sql-product :backend))
                      append (list k v)))
    (pg-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

;;;; Lifecycle methods

(cl-defmethod data-lens-db-disconnect ((conn pg-conn))
  "Disconnect PostgreSQL CONN."
  (condition-case nil
      (pg-disconnect conn)
    (pg-error nil)))

(cl-defmethod data-lens-db-live-p ((conn pg-conn))
  "Return non-nil if PostgreSQL CONN is live."
  (and conn
       (pg-conn-p conn)
       (process-live-p (pg-conn-process conn))))

(cl-defmethod data-lens-db-init-connection ((_conn pg-conn))
  "Initialize PostgreSQL CONN.
No special init needed — encoding is set in startup message.")

;;;; Query methods

(cl-defmethod data-lens-db-query ((conn pg-conn) sql)
  "Execute SQL on PostgreSQL CONN, returning a `data-lens-db-result'."
  (condition-case err
      (data-lens-db-pg--wrap-result (pg-query conn sql))
    (pg-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-build-paged-sql ((_conn pg-conn) base-sql
                                             page-num page-size
                                             &optional order-by)
  "Build a paginated SQL query for PostgreSQL."
  (let ((case-fold-search t))
    (if (string-match-p "\\bLIMIT\\b" base-sql)
        base-sql
      (let* ((trimmed (string-trim-right
                       (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
             (offset (* page-num page-size))
             (order-clause (when order-by
                             (format " ORDER BY %s %s"
                                     (pg-escape-identifier (car order-by))
                                     (cdr order-by)))))
        (format "SELECT * FROM (%s) AS _dl_t%s LIMIT %d OFFSET %d"
                trimmed (or order-clause "") page-size offset)))))

;;;; SQL dialect methods

(cl-defmethod data-lens-db-escape-identifier ((_conn pg-conn) name)
  "Escape NAME as a PostgreSQL identifier (double-quoted)."
  (pg-escape-identifier name))

(cl-defmethod data-lens-db-escape-literal ((_conn pg-conn) value)
  "Escape VALUE as a PostgreSQL string literal."
  (pg-escape-literal value))

;;;; Schema methods

(cl-defmethod data-lens-db-list-tables ((conn pg-conn))
  "Return table names for the current PostgreSQL database."
  (condition-case err
      (let ((result (pg-query
                     conn
                     "SELECT tablename FROM pg_tables \
WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
ORDER BY tablename")))
        (mapcar #'car (pg-result-rows result)))
    (pg-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-list-columns ((conn pg-conn) table)
  "Return column names for TABLE on PostgreSQL CONN."
  (condition-case err
      (let ((result (pg-query
                     conn
                     (format "SELECT column_name FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                             (pg-escape-literal table)))))
        (mapcar #'car (pg-result-rows result)))
    (pg-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(defun data-lens-db-pg--format-column-ddl (col)
  "Format a single column COL row as a DDL line."
  (let* ((name (nth 0 col))
         (dtype (nth 1 col))
         (max-len (nth 2 col))
         (default-val (nth 3 col))
         (nullable (nth 4 col))
         (type-str (if max-len (format "%s(%s)" dtype max-len) dtype))
         (parts (list (pg-escape-identifier name) type-str)))
    (when (string= nullable "NO")
      (push "NOT NULL" parts))
    (when default-val
      (push (format "DEFAULT %s" default-val) parts))
    (format "    %s" (mapconcat #'identity (nreverse parts) " "))))

(cl-defmethod data-lens-db-show-create-table ((conn pg-conn) table)
  "Return synthesized DDL for TABLE on PostgreSQL CONN.
PostgreSQL has no SHOW CREATE TABLE, so we build DDL from
information_schema."
  (condition-case err
      (let* ((cols-result
              (pg-query
               conn
               (format "SELECT column_name, data_type, \
character_maximum_length, column_default, is_nullable \
FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                       (pg-escape-literal table))))
             (lines (mapcar #'data-lens-db-pg--format-column-ddl
                            (pg-result-rows cols-result))))
        (format "CREATE TABLE %s (\n%s\n);"
                (pg-escape-identifier table)
                (mapconcat #'identity lines ",\n")))
    (pg-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-primary-key-columns ((conn pg-conn) table)
  "Return primary key column names for TABLE on PostgreSQL CONN."
  (condition-case _err
      (let ((result (pg-query
                     conn
                     (format "SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = %s::regclass AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)"
                             (pg-escape-literal table)))))
        (mapcar #'car (pg-result-rows result)))
    (pg-error nil)))

(cl-defmethod data-lens-db-foreign-keys ((conn pg-conn) table)
  "Return foreign key info for TABLE on PostgreSQL CONN."
  (condition-case _err
      (let* ((sql (format "SELECT
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name = %s
    AND tc.table_schema = current_schema()"
                          (pg-escape-literal table)))
             (result (pg-query conn sql)))
        (cl-loop for row in (pg-result-rows result)
                 collect (cons (nth 0 row)
                               (list :ref-table (nth 1 row)
                                     :ref-column (nth 2 row)))))
    (pg-error nil)))

;;;; Re-entrancy guard

(cl-defmethod data-lens-db-busy-p ((conn pg-conn))
  "Return non-nil if PostgreSQL CONN is executing a query."
  (pg-conn-busy conn))

;;;; Metadata methods

(cl-defmethod data-lens-db-user ((conn pg-conn))
  "Return the user for PostgreSQL CONN."
  (pg-conn-user conn))

(cl-defmethod data-lens-db-host ((conn pg-conn))
  "Return the host for PostgreSQL CONN."
  (pg-conn-host conn))

(cl-defmethod data-lens-db-port ((conn pg-conn))
  "Return the port for PostgreSQL CONN."
  (pg-conn-port conn))

(cl-defmethod data-lens-db-database ((conn pg-conn))
  "Return the database for PostgreSQL CONN."
  (pg-conn-database conn))

(cl-defmethod data-lens-db-display-name ((_conn pg-conn))
  "Return \"PostgreSQL\" as the display name."
  "PostgreSQL")

(provide 'data-lens-db-pg)
;;; data-lens-db-pg.el ends here
