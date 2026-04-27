;;; clutch-db-mysql.el --- Native backend over the MySQL wire client -*- lexical-binding: t; -*-

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

;; MySQL backend for the clutch generic database interface.
;; Implements all `clutch-db-*' generics by dispatching on `clutch-mysql-conn'.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'clutch-mysql)

(declare-function clutch-db--schedule-idle-metadata-call "clutch-db"
                  (conn callback errback fn &rest args))

;;;; Type-category mapping

(defconst clutch-db-mysql--type-category-alist
  `((,clutch-mysql-type-decimal    . numeric)
    (,clutch-mysql-type-tiny       . numeric)
    (,clutch-mysql-type-short      . numeric)
    (,clutch-mysql-type-long       . numeric)
    (,clutch-mysql-type-float      . numeric)
    (,clutch-mysql-type-double     . numeric)
    (,clutch-mysql-type-longlong   . numeric)
    (,clutch-mysql-type-int24      . numeric)
    (,clutch-mysql-type-year       . numeric)
    (,clutch-mysql-type-newdecimal . numeric)
    (,clutch-mysql-type-json       . json)
    (,clutch-mysql-type-blob       . blob)
    (,clutch-mysql-type-tiny-blob  . blob)
    (,clutch-mysql-type-medium-blob . blob)
    (,clutch-mysql-type-long-blob  . blob)
    (,clutch-mysql-type-date       . date)
    (,clutch-mysql-type-time       . time)
    (,clutch-mysql-type-datetime   . datetime)
    (,clutch-mysql-type-timestamp  . datetime))
  "Alist mapping MySQL type codes to type-category symbols.")

(defconst clutch-db-mysql--binary-charset 63
  "MySQL charset code for binary.
Blob-family types with this charset are true BLOBs; others are TEXT.")

(defconst clutch-db-mysql--blob-family-types
  (list clutch-mysql-type-blob clutch-mysql-type-tiny-blob
        clutch-mysql-type-medium-blob clutch-mysql-type-long-blob)
  "MySQL type codes that share BLOB/TEXT family encodings.")

(defun clutch-db-mysql--type-category (mysql-type charset)
  "Map a MySQL type code MYSQL-TYPE (with CHARSET) to a type-category symbol.
For the blob-family type codes, charset 63 (binary) means a true BLOB;
any other charset means a TEXT column."
  (if (memq mysql-type clutch-db-mysql--blob-family-types)
      (if (= charset clutch-db-mysql--binary-charset) 'blob 'text)
    (or (alist-get mysql-type clutch-db-mysql--type-category-alist)
        'text)))

(defun clutch-db-mysql--convert-columns (mysql-columns)
  "Convert MYSQL-COLUMNS to `clutch-db' column plists.
Each output plist has :name and :type-category."
  (mapcar (lambda (col)
            (list :name (plist-get col :name)
                  :type-category (clutch-db-mysql--type-category
                                  (plist-get col :type)
                                  (plist-get col :character-set))))
          mysql-columns))

(defun clutch-db-mysql--wrap-result (mysql-result)
  "Convert MYSQL-RESULT to a `clutch-db-result'."
  (let ((cols (clutch-mysql-result-columns mysql-result)))
    (make-clutch-db-result
     :connection (clutch-mysql-result-connection mysql-result)
     :columns (when cols (clutch-db-mysql--convert-columns cols))
     :rows (clutch-mysql-result-rows mysql-result)
     :affected-rows (clutch-mysql-result-affected-rows mysql-result)
     :last-insert-id (clutch-mysql-result-last-insert-id mysql-result)
     :warnings (clutch-mysql-result-warnings mysql-result))))

;;;; Connect function

(defun clutch-db-mysql-connect (params)
  "Connect to MySQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls,
:ssl-mode, :connect-timeout, :read-idle-timeout.
For MySQL, explicit `:tls nil' or `:ssl-mode disabled' forces plaintext."
  (setq params (clutch-db--normalize-connect-params 'mysql params))
  (let ((tls-mode (plist-get params :clutch-tls-mode)))
    (cl-remf params :clutch-tls-mode)
    (pcase tls-mode
      ('default
       (cl-remf params :tls)
       (cl-remf params :ssl-mode))
      ('require
       (setq params (plist-put params :tls t))
       (cl-remf params :ssl-mode))
      ('disable
       (setq params (plist-put params :ssl-mode 'disabled))
       (cl-remf params :tls)))
    (condition-case err
        (apply #'clutch-mysql-connect
               (cl-loop for (k v) on params by #'cddr
                        unless (memq k '(:sql-product :backend :pass-entry))
                        append (list k v)))
      (clutch-mysql-error
       (signal 'clutch-db-error
               (list (error-message-string err)))))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn clutch-mysql-conn))
  "Disconnect MySQL CONN."
  (condition-case nil
      (clutch-mysql-disconnect conn)
    (clutch-mysql-error nil)))

(cl-defmethod clutch-db-live-p ((conn clutch-mysql-conn))
  "Return non-nil if MySQL CONN is live."
  (and conn
       (clutch-mysql-conn-p conn)
       (process-live-p (clutch-mysql-conn-process conn))))

(cl-defmethod clutch-db-init-connection ((conn clutch-mysql-conn))
  "Initialize MySQL CONN with utf8mb4."
  (condition-case err
      (clutch-mysql-query conn "SET NAMES utf8mb4")
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (format "Init failed: %s" (error-message-string err)))))))

(cl-defmethod clutch-db-eager-schema-refresh-p ((_conn clutch-mysql-conn))
  "MySQL schema refresh should not block connect."
  nil)

;;;; Transaction methods

(cl-defmethod clutch-db-manual-commit-p ((conn clutch-mysql-conn))
  "Return non-nil when MySQL CONN runs with autocommit disabled."
  (not (clutch-mysql-autocommit-p conn)))

(cl-defmethod clutch-db-commit ((conn clutch-mysql-conn))
  "Commit the current transaction on MySQL CONN."
  (clutch-mysql-commit conn))

(cl-defmethod clutch-db-rollback ((conn clutch-mysql-conn))
  "Roll back the current transaction on MySQL CONN."
  (clutch-mysql-rollback conn))

(cl-defmethod clutch-db-set-auto-commit ((conn clutch-mysql-conn) auto-commit)
  "Set autocommit mode on MySQL CONN.
AUTO-COMMIT non-nil enables autocommit; nil enables manual commit."
  (clutch-mysql-set-autocommit conn auto-commit))

;;;; Query methods

(cl-defmethod clutch-db-query ((conn clutch-mysql-conn) sql)
  "Execute SQL on MySQL CONN, returning a `clutch-db-result'."
  (condition-case err
      (clutch-db-mysql--wrap-result (clutch-mysql-query conn sql))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-execute-params ((conn clutch-mysql-conn) sql params)
  "Execute parameterized SQL on MySQL CONN with PARAMS."
  (let (stmt result pending-error)
    (condition-case err
        (setq stmt (clutch-mysql-prepare conn sql))
      (clutch-mysql-error
       (setq pending-error err)))
    (when stmt
      (unwind-protect
          (condition-case err
              (setq result
                    (clutch-db-mysql--wrap-result
                     (apply #'clutch-mysql-execute stmt params)))
            (clutch-mysql-error
             (setq pending-error err)))
        (condition-case err
            (clutch-mysql-stmt-close stmt)
          (clutch-mysql-error
           (unless pending-error
             (setq pending-error err))))))
    (if pending-error
        (signal 'clutch-db-error
                (list (error-message-string pending-error)))
      result)))

(cl-defmethod clutch-db-build-paged-sql ((_conn clutch-mysql-conn) base-sql
                                             page-num page-size
                                             &optional order-by)
  "Build a paginated SQL query for MySQL from BASE-SQL.
PAGE-NUM is zero-based, PAGE-SIZE limits each page, and ORDER-BY
controls the optional sort clause."
  (clutch-db--build-limit-offset-paged-sql
   base-sql page-num page-size order-by #'clutch-mysql-escape-identifier))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-mysql-conn) name)
  "Escape NAME as a MySQL identifier (backtick-quoted)."
  (clutch-mysql-escape-identifier name))

(cl-defmethod clutch-db-escape-literal ((_conn clutch-mysql-conn) value)
  "Escape VALUE as a MySQL string literal."
  (clutch-mysql-escape-literal value))

;;;; Schema methods

(cl-defmethod clutch-db-refresh-schema-async ((conn clutch-mysql-conn) callback
                                              &optional errback)
  "Refresh MySQL schema names for CONN via CALLBACK on the main thread.
Call ERRBACK if the metadata refresh fails."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-tables))

(cl-defmethod clutch-db-list-columns-async ((conn clutch-mysql-conn) table callback
                                            &optional errback)
  "Fetch MySQL column names for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-columns
   table))

(cl-defmethod clutch-db-column-details-async ((conn clutch-mysql-conn) table callback
                                              &optional errback)
  "Fetch MySQL column details for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-column-details
   table))

(cl-defmethod clutch-db-table-comment-async ((conn clutch-mysql-conn) table callback
                                             &optional errback)
  "Fetch the MySQL comment for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-table-comment
   table))

(cl-defmethod clutch-db-list-tables ((conn clutch-mysql-conn))
  "Return table names for the current MySQL database on CONN."
  (condition-case err
      (let ((result (clutch-mysql-query conn "SHOW TABLES")))
        (mapcar #'car (clutch-mysql-result-rows result)))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-schemas ((conn clutch-mysql-conn))
  "Return visible MySQL schema/database names for CONN."
  (condition-case err
      (let ((result (clutch-mysql-query conn "SHOW DATABASES")))
        (sort (mapcar #'car (clutch-mysql-result-rows result)) #'string-collate-lessp))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-current-schema ((conn clutch-mysql-conn))
  "Return the current MySQL schema/database for CONN."
  (clutch-db-database conn))

(cl-defmethod clutch-db-set-current-schema ((conn clutch-mysql-conn) schema)
  "Switch MySQL CONN to SCHEMA."
  (condition-case err
      (let ((schema (string-trim schema)))
        (clutch-db-query
         conn
         (format "USE %s" (clutch-db-escape-identifier conn schema)))
        (setf (clutch-mysql-conn-database conn) schema)
        schema)
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-mysql-conn))
  "Return table/view entry plists for the current MySQL database on CONN."
  (condition-case err
      (let* ((result (clutch-mysql-query
                      conn
                      "SELECT TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
ORDER BY TABLE_NAME"))
             (schema (clutch-db-database conn)))
        (mapcar
         (lambda (row)
           (pcase-let ((`(,name ,table-type) row))
             (list :name name
                   :type (if (string= table-type "VIEW") "VIEW" "TABLE")
                   :schema schema
                   :source-schema schema)))
         (clutch-mysql-result-rows result)))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-columns ((conn clutch-mysql-conn) table)
  "Return column names for TABLE on MySQL CONN."
  (condition-case err
      (let ((result (clutch-mysql-query
                     conn
                     (format "SHOW COLUMNS FROM %s"
                             (clutch-mysql-escape-identifier table)))))
        (mapcar #'car (clutch-mysql-result-rows result)))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-show-create-table ((conn clutch-mysql-conn) table)
  "Return DDL for TABLE on MySQL CONN."
  (condition-case err
      (let* ((result (clutch-mysql-query
                      conn
                      (format "SHOW CREATE TABLE %s"
                              (clutch-mysql-escape-identifier table))))
             (rows (clutch-mysql-result-rows result)))
        (unless rows
          (signal 'clutch-db-error
                  (list (format "SHOW CREATE TABLE returned no rows for %s" table))))
        (pcase-let ((`(,_ ,ddl) (car rows)))
          ddl))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects ((conn clutch-mysql-conn) category)
  "Return object entry plists for CATEGORY on MySQL CONN."
  (condition-case err
      (let ((schema (clutch-db-database conn)))
        (pcase category
          ('indexes
           (let ((result (clutch-mysql-query
                          conn
                          "SELECT DISTINCT INDEX_NAME, TABLE_NAME, NON_UNIQUE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
ORDER BY TABLE_NAME, INDEX_NAME")))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,name ,table-name ,non-unique) row))
                  (list :name name :type "INDEX" :schema schema :source-schema schema
                        :target-table table-name :unique (equal non-unique 0))))
              (clutch-mysql-result-rows result))))
          ('sequences nil)
          ((or 'procedures 'functions)
           (let* ((routine-type (if (eq category 'procedures) "PROCEDURE" "FUNCTION"))
                  (result (clutch-mysql-query
                           conn
                           (format "SELECT ROUTINE_NAME, ROUTINE_TYPE
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = DATABASE()
  AND ROUTINE_TYPE = %s
ORDER BY ROUTINE_NAME"
                                   (clutch-mysql-escape-literal routine-type)))))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,name ,type) row))
                  (list :name name :type type :schema schema :source-schema schema)))
              (clutch-mysql-result-rows result))))
          ('triggers
           (let ((result (clutch-mysql-query
                          conn
                          "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING
FROM INFORMATION_SCHEMA.TRIGGERS
WHERE TRIGGER_SCHEMA = DATABASE()
ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME")))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,name ,table-name ,event ,timing) row))
                  (list :name name :type "TRIGGER" :schema schema :source-schema schema
                        :target-table table-name :event event :timing timing
                        :status "ENABLED")))
              (clutch-mysql-result-rows result))))
          (_ nil)))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects-async ((conn clutch-mysql-conn) category callback
                                            &optional errback)
  "Fetch MySQL object entries for CATEGORY on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-objects
   category))

(cl-defmethod clutch-db-object-details ((conn clutch-mysql-conn) entry)
  "Return detail plists for MySQL object ENTRY on CONN."
  (condition-case _err
      (let ((type (upcase (or (plist-get entry :type) ""))))
        (pcase type
          ("INDEX"
           (let* ((name (plist-get entry :name))
                  (result (clutch-mysql-query
                           conn
                           (format "SELECT COLUMN_NAME, SEQ_IN_INDEX, COLLATION
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND INDEX_NAME = %s
ORDER BY SEQ_IN_INDEX"
                                   (clutch-mysql-escape-literal name)))))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,column-name ,position ,collation) row))
                  (list :name column-name
                        :position position
                        :descend (if (string= collation "D") "DESC" "ASC"))))
              (clutch-mysql-result-rows result))))
          ((or "PROCEDURE" "FUNCTION")
           (let* ((specific-name (plist-get entry :name))
                  (result (clutch-mysql-query
                           conn
                           (format "SELECT PARAMETER_NAME, DTD_IDENTIFIER,
       COALESCE(PARAMETER_MODE, 'RETURN'), ORDINAL_POSITION
FROM INFORMATION_SCHEMA.PARAMETERS
WHERE SPECIFIC_SCHEMA = DATABASE()
  AND SPECIFIC_NAME = %s
ORDER BY ORDINAL_POSITION"
                                   (clutch-mysql-escape-literal specific-name)))))
             (mapcar
              (lambda (row)
                (pcase-let ((`(,param-name ,dtype ,mode ,position) row))
                  (list :name param-name :type dtype :mode mode :position position)))
              (clutch-mysql-result-rows result))))
          (_ nil)))
    (clutch-mysql-error nil)))

(cl-defmethod clutch-db-object-source ((conn clutch-mysql-conn) entry)
  "Return source text for MySQL object ENTRY on CONN."
  (condition-case err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("PROCEDURE"
         (let* ((result (clutch-mysql-query
                         conn
                         (format "SHOW CREATE PROCEDURE %s"
                                 (clutch-mysql-escape-identifier (plist-get entry :name)))))
                (row (car (clutch-mysql-result-rows result))))
           (nth 2 row)))
        ("FUNCTION"
         (let* ((result (clutch-mysql-query
                         conn
                         (format "SHOW CREATE FUNCTION %s"
                                 (clutch-mysql-escape-identifier (plist-get entry :name)))))
                (row (car (clutch-mysql-result-rows result))))
           (nth 2 row)))
        ("TRIGGER"
         (let* ((result (clutch-mysql-query
                         conn
                         (format "SHOW CREATE TRIGGER %s"
                                 (clutch-mysql-escape-identifier (plist-get entry :name)))))
                (row (car (clutch-mysql-result-rows result))))
           (nth 2 row)))
        (_ nil))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-show-create-object ((conn clutch-mysql-conn) entry)
  "Return DDL text for MySQL non-table ENTRY on CONN."
  (condition-case err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("VIEW"
         (let* ((result (clutch-mysql-query
                         conn
                         (format "SHOW CREATE VIEW %s"
                                 (clutch-mysql-escape-identifier (plist-get entry :name)))))
                (row (car (clutch-mysql-result-rows result))))
           (nth 1 row)))
        ("INDEX"
         (let* ((details (clutch-db-object-details conn entry))
                (columns (mapconcat
                          (lambda (col)
                            (format "%s %s"
                                    (clutch-mysql-escape-identifier (plist-get col :name))
                                    (plist-get col :descend)))
                          details
                          ", ")))
           (format "CREATE %sINDEX %s ON %s (%s);"
                   (if (plist-get entry :unique) "UNIQUE " "")
                   (clutch-mysql-escape-identifier (plist-get entry :name))
                   (clutch-mysql-escape-identifier (plist-get entry :target-table))
                   columns)))
        (_ nil))
    (clutch-mysql-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-table-comment ((conn clutch-mysql-conn) table)
  "Return the comment for TABLE on MySQL CONN, or nil if empty."
  (condition-case _err
      (let* ((result (clutch-mysql-query
                      conn
                      (format "SELECT TABLE_COMMENT \
FROM INFORMATION_SCHEMA.TABLES \
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
                              (clutch-mysql-escape-literal table))))
             (row (car (clutch-mysql-result-rows result)))
             (comment (car row)))
        (when (and comment (not (string-empty-p comment)))
          comment))
    (clutch-mysql-error nil)))

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-mysql-conn) table)
  "Return primary key column names for TABLE on MySQL CONN."
  (condition-case _err
      (let* ((result (clutch-mysql-query
                      conn
                      (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                              (clutch-mysql-escape-identifier table))))
             (rows (clutch-mysql-result-rows result)))
        (mapcar (lambda (row)
                  (pcase-let ((`(,_ ,_ ,_ ,_ ,name) row))
                    (if (stringp name) name (format "%s" name))))
                rows))
    (clutch-mysql-error nil)))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-mysql-conn) table)
  "Return foreign key info for TABLE on MySQL CONN.
Returns alist of (COL-NAME . (:ref-table T :ref-column C))."
  (condition-case _err
      (let* ((sql (format
                   "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s \
AND REFERENCED_TABLE_NAME IS NOT NULL"
                   (clutch-mysql-escape-literal table)))
             (result (clutch-mysql-query conn sql))
             (rows (clutch-mysql-result-rows result)))
        (cl-loop for row in rows
                 collect (pcase-let ((`(,n ,ref-table ,ref-column) row))
                           (let ((col-name (if (stringp n) n (format "%s" n))))
                             (cons col-name (list :ref-table ref-table
                                                  :ref-column ref-column))))))
    (clutch-mysql-error nil)))

(cl-defmethod clutch-db-referencing-objects ((conn clutch-mysql-conn) table)
  "Return table entries that reference TABLE on MySQL CONN."
  (condition-case _err
      (let* ((sql (format
                   "SELECT DISTINCT TABLE_NAME \
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = %s"
                   (clutch-mysql-escape-literal table)))
             (result (clutch-mysql-query conn sql))
             (rows (clutch-mysql-result-rows result)))
        (mapcar (lambda (row)
                  (pcase-let ((`(,name) row))
                    (list :name name :type "TABLE")))
                rows))
    (clutch-mysql-error nil)))

;;;; Column details

(cl-defmethod clutch-db-column-details ((conn clutch-mysql-conn) table)
  "Return detailed column info for TABLE on MySQL CONN."
  (condition-case _err
      (let* ((col-result (clutch-mysql-query
                          conn
                          (format "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, \
COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT \
FROM INFORMATION_SCHEMA.COLUMNS \
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s \
ORDER BY ORDINAL_POSITION"
                                  (clutch-mysql-escape-literal table))))
             (col-rows (clutch-mysql-result-rows col-result))
             (pk-cols (clutch-db-primary-key-columns conn table))
             (fks (clutch-db-foreign-keys conn table)))
        (mapcar
         (lambda (row)
           (pcase-let ((`(,name ,type ,nullable-str ,default-val ,extra ,comment) row))
             (let* ((nullable (string= nullable-str "YES"))
                    (pk-p (member name pk-cols))
                    (fk (cdr (assoc name fks)))
                    (generated (and extra
                                    (string-match-p
                                     "\\_<\\(auto_increment\\|VIRTUAL GENERATED\\|STORED GENERATED\\)\\_>"
                                     extra))))
               (list :name name :type type :nullable nullable
                     :primary-key (and pk-p t)
                     :foreign-key fk
                     :default (and default-val (not generated) default-val)
                     :generated (and generated t)
                     :comment (and comment (not (string-empty-p comment)) comment)))))
         col-rows))
    (clutch-mysql-error nil)))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn clutch-mysql-conn))
  "Return non-nil if MySQL CONN is executing a query."
  (clutch-mysql-conn-busy conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn clutch-mysql-conn))
  "Return the user for MySQL CONN."
  (clutch-mysql-conn-user conn))

(cl-defmethod clutch-db-host ((conn clutch-mysql-conn))
  "Return the host for MySQL CONN."
  (clutch-mysql-conn-host conn))

(cl-defmethod clutch-db-port ((conn clutch-mysql-conn))
  "Return the port for MySQL CONN."
  (clutch-mysql-conn-port conn))

(cl-defmethod clutch-db-database ((conn clutch-mysql-conn))
  "Return the database for MySQL CONN."
  (clutch-mysql-conn-database conn))

(cl-defmethod clutch-db-display-name ((_conn clutch-mysql-conn))
  "Return \"MySQL\" as the display name."
  "MySQL")

(provide 'clutch-db-mysql)
;;; clutch-db-mysql.el ends here
