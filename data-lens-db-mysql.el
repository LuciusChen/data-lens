;;; data-lens-db-mysql.el --- MySQL backend for data-lens-db -*- lexical-binding: t; -*-

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

;; MySQL backend for the data-lens generic database interface.
;; Implements all `data-lens-db-*' generics by dispatching on `mysql-conn'.

;;; Code:

(require 'data-lens-db)
(require 'mysql)

;;;; Type-category mapping

(defconst data-lens-db-mysql-type-category-alist
  `((,mysql-type-decimal    . numeric)
    (,mysql-type-tiny       . numeric)
    (,mysql-type-short      . numeric)
    (,mysql-type-long       . numeric)
    (,mysql-type-float      . numeric)
    (,mysql-type-double     . numeric)
    (,mysql-type-longlong   . numeric)
    (,mysql-type-int24      . numeric)
    (,mysql-type-year       . numeric)
    (,mysql-type-newdecimal . numeric)
    (,mysql-type-json       . json)
    (,mysql-type-blob       . blob)
    (,mysql-type-tiny-blob  . blob)
    (,mysql-type-medium-blob . blob)
    (,mysql-type-long-blob  . blob)
    (,mysql-type-date       . date)
    (,mysql-type-time       . time)
    (,mysql-type-datetime   . datetime)
    (,mysql-type-timestamp  . datetime))
  "Alist mapping MySQL type codes to type-category symbols.")

(defun data-lens-db-mysql-type-category (mysql-type)
  "Map a MySQL type code MYSQL-TYPE to a type-category symbol."
  (or (alist-get mysql-type data-lens-db-mysql-type-category-alist)
      'text))

(defun data-lens-db-mysql--convert-columns (mysql-columns)
  "Convert MySQL column plists to data-lens-db column plists.
Each output plist has :name and :type-category."
  (mapcar (lambda (col)
            (list :name (plist-get col :name)
                  :type-category (data-lens-db-mysql-type-category
                                  (plist-get col :type))))
          mysql-columns))

(defun data-lens-db-mysql--wrap-result (mysql-result)
  "Convert a `mysql-result' to a `data-lens-db-result'."
  (let ((cols (mysql-result-columns mysql-result)))
    (make-data-lens-db-result
     :connection (mysql-result-connection mysql-result)
     :columns (when cols (data-lens-db-mysql--convert-columns cols))
     :rows (mysql-result-rows mysql-result)
     :affected-rows (mysql-result-affected-rows mysql-result)
     :last-insert-id (mysql-result-last-insert-id mysql-result)
     :warnings (mysql-result-warnings mysql-result))))

;;;; Connect function

(defun data-lens-db-mysql-connect (params)
  "Connect to MySQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls."
  (condition-case err
      (apply #'mysql-connect
             (cl-loop for (k v) on params by #'cddr
                      unless (memq k '(:sql-product :backend))
                      append (list k v)))
    (mysql-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

;;;; Lifecycle methods

(cl-defmethod data-lens-db-disconnect ((conn mysql-conn))
  "Disconnect MySQL CONN."
  (condition-case nil
      (mysql-disconnect conn)
    (mysql-error nil)))

(cl-defmethod data-lens-db-live-p ((conn mysql-conn))
  "Return non-nil if MySQL CONN is live."
  (and conn
       (mysql-conn-p conn)
       (process-live-p (mysql-conn-process conn))))

(cl-defmethod data-lens-db-init-connection ((conn mysql-conn))
  "Initialize MySQL CONN with utf8mb4."
  (condition-case err
      (mysql-query conn "SET NAMES utf8mb4")
    (mysql-error
     (signal 'data-lens-db-error
             (list (format "Init failed: %s" (error-message-string err)))))))

;;;; Query methods

(cl-defmethod data-lens-db-query ((conn mysql-conn) sql)
  "Execute SQL on MySQL CONN, returning a `data-lens-db-result'."
  (condition-case err
      (data-lens-db-mysql--wrap-result (mysql-query conn sql))
    (mysql-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-build-paged-sql ((_conn mysql-conn) base-sql
                                             page-num page-size
                                             &optional order-by)
  "Build a paginated SQL query for MySQL.
Wraps BASE-SQL with LIMIT/OFFSET.  ORDER-BY is (COL . DIR) or nil."
  (let ((case-fold-search t))
    (if (string-match-p "\\bLIMIT\\b" base-sql)
        base-sql
      (let* ((trimmed (string-trim-right
                       (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
             (offset (* page-num page-size))
             (order-clause (when order-by
                             (format " ORDER BY %s %s"
                                     (mysql-escape-identifier (car order-by))
                                     (cdr order-by)))))
        (format "SELECT * FROM (%s) AS _dl_t%s LIMIT %d OFFSET %d"
                trimmed (or order-clause "") page-size offset)))))

;;;; SQL dialect methods

(cl-defmethod data-lens-db-escape-identifier ((_conn mysql-conn) name)
  "Escape NAME as a MySQL identifier (backtick-quoted)."
  (mysql-escape-identifier name))

(cl-defmethod data-lens-db-escape-literal ((_conn mysql-conn) value)
  "Escape VALUE as a MySQL string literal."
  (mysql-escape-literal value))

;;;; Schema methods

(cl-defmethod data-lens-db-list-tables ((conn mysql-conn))
  "Return table names for the current MySQL database."
  (condition-case err
      (let ((result (mysql-query conn "SHOW TABLES")))
        (mapcar #'car (mysql-result-rows result)))
    (mysql-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-list-columns ((conn mysql-conn) table)
  "Return column names for TABLE on MySQL CONN."
  (condition-case err
      (let ((result (mysql-query
                     conn
                     (format "SHOW COLUMNS FROM %s"
                             (mysql-escape-identifier table)))))
        (mapcar #'car (mysql-result-rows result)))
    (mysql-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-show-create-table ((conn mysql-conn) table)
  "Return DDL for TABLE on MySQL CONN."
  (condition-case err
      (let* ((result (mysql-query
                      conn
                      (format "SHOW CREATE TABLE %s"
                              (mysql-escape-identifier table))))
             (rows (mysql-result-rows result)))
        (nth 1 (car rows)))
    (mysql-error
     (signal 'data-lens-db-error
             (list (error-message-string err))))))

(cl-defmethod data-lens-db-primary-key-columns ((conn mysql-conn) table)
  "Return primary key column names for TABLE on MySQL CONN."
  (condition-case _err
      (let* ((result (mysql-query
                      conn
                      (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                              (mysql-escape-identifier table))))
             (rows (mysql-result-rows result)))
        (mapcar (lambda (row)
                  (let ((name (nth 4 row)))
                    (if (stringp name) name (format "%s" name))))
                rows))
    (mysql-error nil)))

(cl-defmethod data-lens-db-foreign-keys ((conn mysql-conn) table)
  "Return foreign key info for TABLE on MySQL CONN.
Returns alist of (COL-NAME . (:ref-table T :ref-column C))."
  (condition-case _err
      (let* ((sql (format
                   "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s \
AND REFERENCED_TABLE_NAME IS NOT NULL"
                   (mysql-escape-literal table)))
             (result (mysql-query conn sql))
             (rows (mysql-result-rows result)))
        (cl-loop for row in rows
                 for n = (nth 0 row)
                 for col-name = (if (stringp n) n (format "%s" n))
                 collect (cons col-name (list :ref-table (nth 1 row)
                                              :ref-column (nth 2 row)))))
    (mysql-error nil)))

;;;; Re-entrancy guard

(cl-defmethod data-lens-db-busy-p ((conn mysql-conn))
  "Return non-nil if MySQL CONN is executing a query."
  (mysql-conn-busy conn))

;;;; Metadata methods

(cl-defmethod data-lens-db-user ((conn mysql-conn))
  "Return the user for MySQL CONN."
  (mysql-conn-user conn))

(cl-defmethod data-lens-db-host ((conn mysql-conn))
  "Return the host for MySQL CONN."
  (mysql-conn-host conn))

(cl-defmethod data-lens-db-port ((conn mysql-conn))
  "Return the port for MySQL CONN."
  (mysql-conn-port conn))

(cl-defmethod data-lens-db-database ((conn mysql-conn))
  "Return the database for MySQL CONN."
  (mysql-conn-database conn))

(cl-defmethod data-lens-db-display-name ((_conn mysql-conn))
  "Return \"MySQL\" as the display name."
  "MySQL")

(provide 'data-lens-db-mysql)
;;; data-lens-db-mysql.el ends here
