;;; clutch-db-pg.el --- Native backend over the PostgreSQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1") (pg "0.40"))
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

;; PostgreSQL backend for the clutch generic database interface.
;; Implements all `clutch-db-*' generics by dispatching on `pgcon'.
;; Adapted for upstream pg-el (Eric Marsden, https://github.com/emarsden/pg-el).

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'pg)

(declare-function clutch-db--schedule-idle-metadata-call "clutch-db"
                  (conn callback errback fn &rest args))

(defvar pg-connect-timeout)
(defvar pg-read-timeout)

(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-exec "pg" (con &rest args))
(declare-function pg-exec-prepared "pg" (con query typed-arguments &rest args))
(declare-function pg-connect-plist "pg" (dbname user &rest args))
(declare-function pg-prepare "pg" (con query argument-types &rest args))
(declare-function pg-bind "pg" (con statement-name typed-arguments &rest args))
(declare-function pg-describe-portal "pg" (con portal))
(declare-function pg-fetch "pg" (con result &rest args))
(declare-function pg-disconnect "pg" (con))
(declare-function pg-cancel "pg" (con))
(declare-function pg-connection-busy-p "pg" (con))
(declare-function make-pgresult "pg" (&rest slot-value-pairs))
(declare-function pgcon-process "pg" (object))
(declare-function pgcon-dbname "pg" (object))
(declare-function pgcon-connect-plist "pg" (object))
(declare-function pg-escape-identifier "pg" (identifier))
(declare-function pg-escape-literal "pg" (string))
;;;; OID → type-category mapping

(defconst clutch-db-pg--oid-bool 16)
(defconst clutch-db-pg--oid-bytea 17)
(defconst clutch-db-pg--oid-int8 20)
(defconst clutch-db-pg--oid-int2 21)
(defconst clutch-db-pg--oid-int4 23)
(defconst clutch-db-pg--oid-json 114)
(defconst clutch-db-pg--oid-float4 700)
(defconst clutch-db-pg--oid-float8 701)
(defconst clutch-db-pg--oid-varchar 1043)
(defconst clutch-db-pg--oid-date 1082)
(defconst clutch-db-pg--oid-time 1083)
(defconst clutch-db-pg--oid-timestamp 1114)
(defconst clutch-db-pg--oid-timestamptz 1184)
(defconst clutch-db-pg--oid-numeric 1700)
(defconst clutch-db-pg--oid-jsonb 3802)

(defconst clutch-db-pg--type-category-alist
  `((,clutch-db-pg--oid-int2 . numeric)
    (,clutch-db-pg--oid-int4 . numeric)
    (,clutch-db-pg--oid-int8 . numeric)
    (,clutch-db-pg--oid-float4 . numeric)
    (,clutch-db-pg--oid-float8 . numeric)
    (,clutch-db-pg--oid-numeric . numeric)
    (,clutch-db-pg--oid-bool . text)
    (,clutch-db-pg--oid-json . json)
    (,clutch-db-pg--oid-jsonb . json)
    (,clutch-db-pg--oid-bytea . blob)
    (,clutch-db-pg--oid-date . date)
    (,clutch-db-pg--oid-time . time)
    (,clutch-db-pg--oid-timestamp . datetime)
    (,clutch-db-pg--oid-timestamptz . datetime))
  "Alist mapping PostgreSQL OIDs to type-category symbols.")

(defun clutch-db-pg--type-category (oid)
  "Map a PostgreSQL type OID to a type-category symbol."
  (or (alist-get oid clutch-db-pg--type-category-alist)
      'text))

(defun clutch-db-pg--convert-columns (pg-columns)
  "Convert PG-COLUMNS to `clutch-db' column plists."
  (mapcar (lambda (col)
            (pcase-let ((`(,name ,type-oid . ,_) col))
              (list :name name
                    :type-category (clutch-db-pg--type-category type-oid))))
          pg-columns))

(defun clutch-db-pg--normalize-date-value (value)
  "Normalize PostgreSQL DATE VALUE to clutch's date plist representation."
  (cond
   ((null value) nil)
   ((and (listp value)
         (plist-get value :year)
         (not (plist-member value :hours)))
    value)
   ((and (stringp value)
         (string-match "\\`\\([0-9]+\\)-\\([0-9][0-9]\\)-\\([0-9][0-9]\\)\\'" value))
    (list :year (string-to-number (match-string 1 value))
          :month (string-to-number (match-string 2 value))
          :day (string-to-number (match-string 3 value))))
   (t
    (pcase-let ((`(,_seconds ,_minutes ,_hours ,day ,month ,year . ,_)
                  (decode-time value)))
      (list :year year
            :month month
            :day day)))))

(defun clutch-db-pg--normalize-time-value (value)
  "Normalize PostgreSQL TIME VALUE to clutch's time plist representation."
  (cond
   ((null value) nil)
   ((and (listp value) (plist-member value :hours))
    value)
   ((stringp value)
    (let* ((negative (string-prefix-p "-" value))
           (rest (if negative (substring value 1) value))
           (rest (replace-regexp-in-string "[+-][0-9:]+\\'" "" rest))
           (dot-pos (string-search "." rest))
           (time-part (if dot-pos (substring rest 0 dot-pos) rest))
           (parts (split-string time-part ":")))
      (pcase parts
        (`(,hours ,minutes ,seconds)
         (list :hours (string-to-number hours)
               :minutes (string-to-number minutes)
               :seconds (string-to-number seconds)
               :negative negative))
        (_ value))))
   (t
    (pcase-let ((`(,seconds ,minutes ,hours . ,_)
                  (decode-time value)))
      (list :hours hours
            :minutes minutes
            :seconds seconds
            :negative nil)))))

(defun clutch-db-pg--normalize-datetime-value (value)
  "Normalize PostgreSQL DATETIME VALUE to clutch's datetime plist representation."
  (cond
   ((null value) nil)
   ((and (listp value)
         (plist-get value :year)
         (plist-member value :hours))
    value)
   (t
    (pcase-let ((`(,seconds ,minutes ,hours ,day ,month ,year . ,_)
                  (decode-time value)))
      (list :year year
            :month month
            :day day
            :hours hours
            :minutes minutes
            :seconds seconds)))))

(defun clutch-db-pg--normalize-value (value col-def)
  "Normalize PG VALUE according to COL-DEF's clutch type category."
  (pcase (plist-get col-def :type-category)
    ('date (clutch-db-pg--normalize-date-value value))
    ('time (clutch-db-pg--normalize-time-value value))
    ('datetime (clutch-db-pg--normalize-datetime-value value))
    (_ value)))

(defun clutch-db-pg--normalize-row (row columns)
  "Normalize PG ROW using clutch column metadata COLUMNS."
  (cl-mapcar #'clutch-db-pg--normalize-value row columns))

(defun clutch-db-pg--wrap-result (pg-result)
  "Convert PG-RESULT to a `clutch-db-result'."
  (let* ((raw-cols (clutch-db-pg--columns pg-result))
         (cols (when raw-cols (clutch-db-pg--convert-columns raw-cols)))
         (rows (if cols
                   (mapcar (lambda (row)
                             (clutch-db-pg--normalize-row row cols))
                           (clutch-db-pg--rows pg-result))
                 (clutch-db-pg--rows pg-result))))
    (make-clutch-db-result
     :connection (clutch-db-pg--result-connection pg-result)
     :columns cols
     :rows rows
     :affected-rows (clutch-db-pg--affected-rows pg-result)
     :last-insert-id nil
     :warnings nil)))

(defun clutch-db-pg--rows (pg-result)
  "Return tuple rows from PG-RESULT."
  (pg-result pg-result :tuples))

(defun clutch-db-pg--columns (pg-result)
  "Return attribute metadata from PG-RESULT."
  (pg-result pg-result :attributes))

(defun clutch-db-pg--result-connection (pg-result)
  "Return the originating connection from PG-RESULT."
  (pg-result pg-result :connection))

(defun clutch-db-pg--affected-rows (pg-result)
  "Return affected row count parsed from PG-RESULT status, or nil."
  (when-let* ((status (pg-result pg-result :status))
              (parts (split-string status))
              (tail (car (last parts))))
    (when (string-match-p "\\`[0-9]+\\'" tail)
      (string-to-number tail))))

(defun clutch-db-pg--connect-value (conn key)
  "Return connection plist KEY for CONN."
  (plist-get (pgcon-connect-plist conn) key))

(defconst clutch-db-pg--current-schema-cache-key :clutch-current-schema
  "Connection-local cache key for the effective PostgreSQL schema.")

(defun clutch-db-pg--cached-current-schema (conn)
  "Return cached current schema for CONN, or nil."
  (plist-get (pgcon-connect-plist conn) clutch-db-pg--current-schema-cache-key))

(defun clutch-db-pg--cache-current-schema (conn schema)
  "Cache SCHEMA as the current schema for CONN."
  (let ((plist (plist-put (pgcon-connect-plist conn)
                          clutch-db-pg--current-schema-cache-key
                          schema)))
    (setf (slot-value conn 'connect-plist) plist))
  schema)

(defun clutch-db-pg--set-statement-timeout (conn timeout-seconds)
  "Set CONN statement_timeout to TIMEOUT-SECONDS, or reset when nil."
  (pg-exec conn
           (if timeout-seconds
               (format "SET statement_timeout = %d" (* timeout-seconds 1000))
             "SET statement_timeout = DEFAULT")))

(defun clutch-db-pg--set-search-path (conn schema)
  "Set CONN search_path to SCHEMA and update the local cache."
  (let ((schema (string-trim schema)))
    (pg-exec conn
             (format "SET search_path TO %s"
                     (pg-escape-identifier schema)))
    (clutch-db-pg--cache-current-schema conn schema)))

(defun clutch-db-pg--strip-leading-comments (sql)
  "Strip leading SQL comments and whitespace from SQL."
  (let ((trimmed (string-trim-left sql)))
    (while (or (string-prefix-p "--" trimmed)
               (string-prefix-p "/*" trimmed))
      (setq trimmed
            (string-trim-left
             (cond
              ((string-prefix-p "--" trimmed)
               (if-let* ((nl (string-search "\n" trimmed)))
                   (substring trimmed (1+ nl))
                 ""))
              ((string-prefix-p "/*" trimmed)
               (if-let* ((end (string-search "*/" trimmed)))
                   (substring trimmed (+ end 2))
                 ""))))))
    trimmed))

(defvar clutch-db-pg--manual-commit-cache (make-hash-table :test 'eq :weakness 'key)
  "Connections whose foreground SQL should run in manual-commit mode.")

(defvar clutch-db-pg--tx-open-cache (make-hash-table :test 'eq :weakness 'key)
  "Connections whose foreground transaction is currently open.")

(defvar clutch-db-pg--tx-failed-cache (make-hash-table :test 'eq :weakness 'key)
  "Connections whose foreground transaction is currently failed/aborted.")

(defun clutch-db-pg--manual-commit-enabled-p (conn)
  "Return non-nil when CONN is in clutch-managed manual-commit mode."
  (and conn (gethash conn clutch-db-pg--manual-commit-cache)))

(defun clutch-db-pg--tx-open-p (conn)
  "Return non-nil when CONN has an open foreground transaction."
  (and conn (gethash conn clutch-db-pg--tx-open-cache)))

(defun clutch-db-pg--tx-failed-p (conn)
  "Return non-nil when CONN's foreground transaction is failed/aborted."
  (and conn (gethash conn clutch-db-pg--tx-failed-cache)))

(defun clutch-db-pg--clear-tx-state (conn)
  "Clear PostgreSQL foreground transaction state for CONN."
  (when conn
    (remhash conn clutch-db-pg--tx-open-cache)
    (remhash conn clutch-db-pg--tx-failed-cache)))

(defun clutch-db-pg--set-manual-commit-enabled (conn enabled)
  "Set clutch-managed manual-commit mode on CONN to ENABLED."
  (when conn
    (if enabled
        (puthash conn t clutch-db-pg--manual-commit-cache)
      (remhash conn clutch-db-pg--manual-commit-cache)
      (clutch-db-pg--clear-tx-state conn))))

(defun clutch-db-pg--mark-tx-open (conn)
  "Mark CONN as having an open, usable foreground transaction."
  (when conn
    (puthash conn t clutch-db-pg--tx-open-cache)
    (remhash conn clutch-db-pg--tx-failed-cache)))

(defun clutch-db-pg--mark-tx-failed (conn)
  "Mark CONN as having an open failed/aborted foreground transaction."
  (when conn
    (puthash conn t clutch-db-pg--tx-open-cache)
    (puthash conn t clutch-db-pg--tx-failed-cache)))

(defun clutch-db-pg--begin-query-p (sql)
  "Return non-nil when SQL starts a PostgreSQL transaction."
  (let ((case-fold-search t)
        (trimmed (clutch-db-pg--strip-leading-comments sql)))
    (string-match-p
     "\\`\\s-*\\(?:BEGIN\\|START\\s-+TRANSACTION\\)\\b"
     trimmed)))

(defun clutch-db-pg--end-query-p (sql)
  "Return non-nil when SQL ends a PostgreSQL transaction."
  (let ((case-fold-search t)
        (trimmed (clutch-db-pg--strip-leading-comments sql)))
    (or (string-match-p
         "\\`\\s-*\\(?:COMMIT\\|END\\|ABORT\\)\\(?:\\s-+\\(?:TRANSACTION\\|WORK\\)\\)?\\(?:\\s-*;\\|\\s-*$\\)"
         trimmed)
        (string-match-p
         "\\`\\s-*ROLLBACK\\(?:\\s-+\\(?:TRANSACTION\\|WORK\\)\\)?\\(?:\\s-*;\\|\\s-*$\\)"
         trimmed))))

(defun clutch-db-pg--transaction-control-query-p (sql)
  "Return non-nil when SQL is explicit PostgreSQL transaction control."
  (let ((trimmed (clutch-db-pg--strip-leading-comments sql)))
    (or (clutch-db-pg--begin-query-p trimmed)
        (clutch-db-pg--end-query-p trimmed)
        (let ((case-fold-search t))
          (or (string-match-p "\\`\\s-*SAVEPOINT\\b" trimmed)
              (string-match-p "\\`\\s-*RELEASE\\b" trimmed)
              (string-match-p "\\`\\s-*ROLLBACK\\s-+TO\\b" trimmed))))))

(defun clutch-db-pg--ensure-foreground-transaction (conn sql)
  "Lazily open a foreground transaction on CONN before running SQL."
  (when (and (clutch-db-pg--manual-commit-enabled-p conn)
             (not (clutch-db-pg--transaction-control-query-p sql))
             (not (clutch-db-pg--tx-open-p conn)))
    (pg-exec conn "BEGIN")
    (clutch-db-pg--mark-tx-open conn)))

(defun clutch-db-pg--note-query-success (conn sql)
  "Update CONN foreground transaction state after successful SQL."
  (when (clutch-db-pg--manual-commit-enabled-p conn)
    (cond
     ((clutch-db-pg--begin-query-p sql)
      (clutch-db-pg--mark-tx-open conn))
     ((clutch-db-pg--end-query-p sql)
      (clutch-db-pg--clear-tx-state conn))
     ((clutch-db-pg--transaction-control-query-p sql)
      (clutch-db-pg--mark-tx-open conn))
     (t
      (clutch-db-pg--mark-tx-open conn)))))

(defun clutch-db-pg--note-query-error (conn sql)
  "Update CONN foreground transaction state after failed SQL."
  (when (and (clutch-db-pg--manual-commit-enabled-p conn)
             (clutch-db-pg--tx-open-p conn)
             (not (clutch-db-pg--end-query-p sql)))
    (clutch-db-pg--mark-tx-failed conn)))

(defun clutch-db-pg--run-query-with-transaction-state (conn sql thunk)
  "Run THUNK for SQL on CONN and keep manual-commit state in sync."
  (condition-case err
      (progn
        (clutch-db-pg--ensure-foreground-transaction conn sql)
        (prog1 (funcall thunk)
          (clutch-db-pg--note-query-success conn sql)))
    (pg-error
     (clutch-db-pg--note-query-error conn sql)
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(defun clutch-db-pg--prefer-fallback-p (err)
  "Return non-nil when ERR indicates the server refused TLS for prefer mode."
  (string-match-p
   "Couldn't establish TLS connection to PostgreSQL: read char"
   (error-message-string err)))

(defun clutch-db-pg--tls-options (sslmode)
  "Return upstream pg-el TLS options for canonical SSLMODE."
  (pcase sslmode
    ('verify-full '(:verify-error t :verify-hostname-error t))
    ((or 'require 'prefer) t)
    (_ nil)))

(defun clutch-db-pg--connect-args (params)
  "Return `pg-connect-plist' keyword arguments from PARAMS."
  (cl-loop for key in '(:password :host :port)
           when (plist-member params key)
           append (list key (plist-get params key))))

(defun clutch-db-pg--connect-with-sslmode (dbname user connect-args sslmode)
  "Connect to DBNAME as USER using CONNECT-ARGS and SSLMODE."
  (pcase sslmode
    ('prefer
     (if (and (fboundp 'gnutls-available-p)
              (gnutls-available-p))
         (condition-case err
             (apply #'pg-connect-plist
                    dbname user
                    (append connect-args
                            (list :tls-options
                                  (clutch-db-pg--tls-options sslmode))))
           (pg-protocol-error
            (if (clutch-db-pg--prefer-fallback-p err)
                (apply #'pg-connect-plist dbname user connect-args)
              (signal (car err) (cdr err)))))
       (apply #'pg-connect-plist dbname user connect-args)))
    ((or 'require 'verify-full)
     (apply #'pg-connect-plist
            dbname user
            (append connect-args
                    (list :tls-options
                          (clutch-db-pg--tls-options sslmode)))))
    (_
     (apply #'pg-connect-plist dbname user connect-args))))

;;;; Connect function

(defun clutch-db-pg-connect (params)
  "Connect to PostgreSQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls,
:sslmode, :schema, :connect-timeout, :read-idle-timeout, :query-timeout.
`:tls' is a convenience shortcut; `:sslmode' is the canonical PostgreSQL name."
  (setq params (clutch-db--normalize-connect-params 'pg params))
  (let ((schema (plist-get params :schema))
        (sslmode (plist-get params :sslmode))
        (connect-timeout (plist-get params :connect-timeout))
        (read-idle-timeout (plist-get params :read-idle-timeout))
        (query-timeout (plist-get params :query-timeout))
        conn)
    (condition-case err
        (progn
          (let* ((pg-connect-timeout (or connect-timeout pg-connect-timeout))
                 (pg-read-timeout (or read-idle-timeout pg-read-timeout))
                 (dbname (plist-get params :database))
                 (user (plist-get params :user))
                 (connect-args (clutch-db-pg--connect-args params)))
            (setq conn
                  (clutch-db-pg--connect-with-sslmode dbname user
                                                      connect-args sslmode)))
          (when query-timeout
            (clutch-db-pg--set-statement-timeout conn query-timeout))
          (when schema
            (clutch-db-pg--set-search-path conn schema))
          conn)
      (pg-error
       (when conn
         (ignore-errors (pg-disconnect conn)))
       (signal 'clutch-db-error
               (list (error-message-string err)))))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn pgcon))
  "Disconnect PostgreSQL CONN."
  (clutch-db-pg--set-manual-commit-enabled conn nil)
  (condition-case nil
      (pg-disconnect conn)
    (pg-error nil)))

(cl-defmethod clutch-db-live-p ((conn pgcon))
  "Return non-nil if PostgreSQL CONN is live."
  (and conn
       (cl-typep conn 'pgcon)
       (process-live-p (pgcon-process conn))))

(cl-defmethod clutch-db-init-connection ((_conn pgcon))
  "Initialize PostgreSQL CONN.
No special init needed — encoding is set in startup message.")

(cl-defmethod clutch-db-eager-schema-refresh-p ((_conn pgcon))
  "PostgreSQL schema refresh should not block connect."
  nil)

;;;; Transaction methods

(cl-defmethod clutch-db-manual-commit-p ((conn pgcon))
  "Return non-nil when PostgreSQL CONN is in manual-commit mode."
  (clutch-db-pg--manual-commit-enabled-p conn))

(cl-defmethod clutch-db-commit ((conn pgcon))
  "Commit the current foreground transaction on PostgreSQL CONN."
  (condition-case err
      (progn
        (when (clutch-db-pg--tx-open-p conn)
          (pg-exec conn "COMMIT"))
        (clutch-db-pg--clear-tx-state conn))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-rollback ((conn pgcon))
  "Roll back the current foreground transaction on PostgreSQL CONN."
  (condition-case err
      (progn
        (when (clutch-db-pg--tx-open-p conn)
          (pg-exec conn "ROLLBACK"))
        (clutch-db-pg--clear-tx-state conn))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-set-auto-commit ((conn pgcon) auto-commit)
  "Set foreground autocommit mode on PostgreSQL CONN.
AUTO-COMMIT non-nil enables autocommit; nil enables clutch-managed
manual-commit mode via lazy `BEGIN'."
  (condition-case err
      (if auto-commit
          (progn
            (when (clutch-db-pg--tx-open-p conn)
              (pg-exec conn (if (clutch-db-pg--tx-failed-p conn)
                                "ROLLBACK"
                              "COMMIT")))
            (clutch-db-pg--set-manual-commit-enabled conn nil))
        (clutch-db-pg--set-manual-commit-enabled conn t))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

;;;; Query methods

(cl-defmethod clutch-db-query ((conn pgcon) sql)
  "Execute SQL on PostgreSQL CONN, returning a `clutch-db-result'."
  (clutch-db-pg--run-query-with-transaction-state
   conn sql
   (lambda ()
     (clutch-db-pg--wrap-result (pg-exec conn sql)))))

(defun clutch-db-pg--rewrite-param-sql (sql)
  "Return SQL with `?' placeholders rewritten to PostgreSQL `$N' form."
  (let ((len (length sql))
        (pos 0)
        (index 1)
        parts)
    (while (< pos len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sql pos)))
          (progn
            (push (substring sql pos skip) parts)
            (setq pos skip))
        (let ((ch (aref sql pos)))
          (if (= ch ??)
              (progn
                (push (format "$%d" index) parts)
                (cl-incf index)
                (cl-incf pos))
            (push (string ch) parts)
            (cl-incf pos)))))
    (apply #'concat (nreverse parts))))

(defun clutch-db-pg--typed-arguments (params)
  "Return PARAMS as pg-el typed arguments using unspecified types."
  (mapcar (lambda (value)
            (cons value nil))
          params))

(defun clutch-db-pg--bind-with-null-params (conn statement-name typed-arguments)
  "Bind TYPED-ARGUMENTS to STATEMENT-NAME on CONN, preserving nil as SQL NULL."
  (let ((orig-format (symbol-function 'format))
        (orig-encode (symbol-function 'encode-coding-string)))
    (cl-letf (((symbol-function 'format)
               (lambda (fmt &rest args)
                 (if (and (string= fmt "%s")
                          (= (length args) 1)
                          (null (car args)))
                     nil
                   (apply orig-format fmt args))))
              ((symbol-function 'encode-coding-string)
               (lambda (string coding-system &optional nocopy)
                 (if (null string)
                     nil
                   (funcall orig-encode string coding-system nocopy)))))
      (pg-bind conn statement-name typed-arguments :portal ""))))

(defun clutch-db-pg--exec-prepared-with-nulls (conn sql typed-arguments)
  "Execute SQL with TYPED-ARGUMENTS on CONN, preserving nil parameters."
  (let* ((statement-name (pg-prepare conn sql (make-list (length typed-arguments) nil)))
         (portal-name (clutch-db-pg--bind-with-null-params
                       conn statement-name typed-arguments))
         (result (make-pgresult :connection conn :portal portal-name)))
    (pg-describe-portal conn portal-name)
    (pg-fetch conn result)))

(cl-defmethod clutch-db-execute-params ((conn pgcon) sql params)
  "Execute parameterized SQL on PostgreSQL CONN with PARAMS."
  (clutch-db-pg--run-query-with-transaction-state
   conn sql
   (lambda ()
     (let* ((pg-sql (clutch-db-pg--rewrite-param-sql sql))
            (typed-arguments (clutch-db-pg--typed-arguments params))
            (result (if (memq nil params)
                        (clutch-db-pg--exec-prepared-with-nulls
                         conn pg-sql typed-arguments)
                      (pg-exec-prepared conn pg-sql typed-arguments))))
       (clutch-db-pg--wrap-result result)))))

(cl-defmethod clutch-db-interrupt-query ((conn pgcon))
  "Interrupt the current PostgreSQL query on CONN without dropping the session."
  (condition-case nil
      (progn
        (pg-cancel conn)
        t)
    (pg-error nil)))

(cl-defmethod clutch-db-build-paged-sql ((_conn pgcon) base-sql
                                             page-num page-size
                                             &optional order-by)
  "Build a paginated SQL query for PostgreSQL from BASE-SQL.
PAGE-NUM is zero-based, PAGE-SIZE limits each page, and ORDER-BY
controls the optional sort clause."
  (clutch-db--build-limit-offset-paged-sql
   base-sql page-num page-size order-by #'pg-escape-identifier))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn pgcon) name)
  "Escape NAME as a PostgreSQL identifier (double-quoted)."
  (pg-escape-identifier name))

(cl-defmethod clutch-db-escape-literal ((_conn pgcon) value)
  "Escape VALUE as a PostgreSQL string literal."
  (pg-escape-literal value))

;;;; Schema methods

(cl-defmethod clutch-db-list-schemas ((conn pgcon))
  "Return visible schema names for PostgreSQL CONN."
  (condition-case err
      (let ((result (pg-exec
                     conn
                     "SELECT schema_name FROM information_schema.schemata \
WHERE schema_name <> 'information_schema' \
  AND schema_name NOT LIKE 'pg\\_%' ESCAPE '\\' \
ORDER BY schema_name")))
        (mapcar #'car (clutch-db-pg--rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-current-schema ((conn pgcon))
  "Return the current effective schema for PostgreSQL CONN."
  (or (clutch-db-pg--cached-current-schema conn)
      (condition-case err
          (let* ((result (pg-exec conn "SELECT current_schema()"))
                 (schema (caar (clutch-db-pg--rows result))))
            (when schema
              (clutch-db-pg--cache-current-schema conn schema)))
        (pg-error
         (signal 'clutch-db-error
                 (list (error-message-string err)))))))

(cl-defmethod clutch-db-set-current-schema ((conn pgcon) schema)
  "Switch PostgreSQL CONN to SCHEMA via search_path."
  (condition-case err
      (clutch-db-pg--set-search-path conn schema)
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-refresh-schema-async ((conn pgcon) callback
                                              &optional errback)
  "Refresh PostgreSQL schema names for CONN via CALLBACK on the main thread.
Call ERRBACK if the metadata refresh fails."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-tables))

(cl-defmethod clutch-db-list-columns-async ((conn pgcon) table callback
                                            &optional errback)
  "Fetch PostgreSQL column names for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-columns
   table))

(cl-defmethod clutch-db-column-details-async ((conn pgcon) table callback
                                              &optional errback)
  "Fetch PostgreSQL column details for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-column-details
   table))

(cl-defmethod clutch-db-table-comment-async ((conn pgcon) table callback
                                             &optional errback)
  "Fetch the PostgreSQL comment for TABLE on CONN on the main thread when idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-table-comment
   table))

(cl-defmethod clutch-db-list-tables ((conn pgcon))
  "Return table names for the current PostgreSQL database on CONN."
  (condition-case err
      (let ((result (pg-exec
                     conn
                     "SELECT tablename FROM pg_tables \
WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
ORDER BY tablename")))
        (mapcar #'car (clutch-db-pg--rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-table-entries ((conn pgcon))
  "Return table/view entry plists for the current PostgreSQL schema on CONN."
  (condition-case err
      (let* ((schema (clutch-db-current-schema conn))
             (result (pg-exec
                      conn
                      "SELECT name, type
FROM (
  SELECT tablename AS name, 'TABLE' AS type
  FROM pg_tables
  WHERE schemaname = current_schema()
  UNION ALL
  SELECT viewname AS name, 'VIEW' AS type
  FROM pg_views
  WHERE schemaname = current_schema()
) objects
ORDER BY name")))
        (mapcar
         (lambda (row)
           (pcase-let ((`(,name ,type) row))
             (list :name name
                   :type type
                   :schema schema
                   :source-schema schema)))
         (clutch-db-pg--rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-columns ((conn pgcon) table)
  "Return column names for TABLE on PostgreSQL CONN."
  (condition-case err
      (let ((result (pg-exec
                     conn
                     (format "SELECT column_name FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                             (pg-escape-literal table)))))
        (mapcar #'car (clutch-db-pg--rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(defun clutch-db-pg--format-column-ddl (col)
  "Format a single column COL row as a DDL line."
  (pcase-let ((`(,name ,dtype ,max-len ,default-val ,nullable) col))
    (let* ((type-str (if max-len (format "%s(%s)" dtype max-len) dtype))
           (parts (list (pg-escape-identifier name) type-str)))
      (when (string= nullable "NO")
        (push "NOT NULL" parts))
      (when default-val
        (push (format "DEFAULT %s" default-val) parts))
      (format "    %s" (mapconcat #'identity (nreverse parts) " ")))))

(cl-defmethod clutch-db-show-create-table ((conn pgcon) table)
  "Return synthesized DDL for TABLE on PostgreSQL CONN.
PostgreSQL has no SHOW CREATE TABLE, so we build DDL from
information_schema."
  (condition-case err
      (let* ((cols-result
              (pg-exec
               conn
               (format "SELECT column_name, data_type, \
character_maximum_length, column_default, is_nullable \
FROM information_schema.columns \
WHERE table_name = %s AND table_schema = current_schema() \
ORDER BY ordinal_position"
                       (pg-escape-literal table))))
             (lines (mapcar #'clutch-db-pg--format-column-ddl
                            (clutch-db-pg--rows cols-result))))
        (format "CREATE TABLE %s (\n%s\n);"
                (pg-escape-identifier table)
                (mapconcat #'identity lines ",\n")))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects ((conn pgcon) category)
  "Return object entry plists for CATEGORY on PostgreSQL CONN."
  (condition-case err
      (let ((schema (clutch-db-current-schema conn)))
      (pcase category
        ('indexes
         (let ((result
                (pg-exec
                 conn
                 "SELECT i.indexname, i.tablename, ix.indisunique
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
JOIN pg_index ix ON ix.indexrelid = c.oid
WHERE i.schemaname = current_schema()
ORDER BY i.tablename, i.indexname")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,table-name ,unique) row))
                (list :name name :type "INDEX" :schema schema
                      :source-schema schema
                      :target-table table-name :unique unique)))
            (clutch-db-pg--rows result))))
        ('sequences
         (let ((result
                (pg-exec
                 conn
                 "SELECT sequencename, min_value, max_value, increment_by, last_value
FROM pg_sequences
WHERE schemaname = current_schema()
ORDER BY sequencename")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,min ,max ,increment ,last) row))
                (list :name name :type "SEQUENCE" :schema schema
                      :source-schema schema
                      :min min :max max :increment increment :last last)))
            (clutch-db-pg--rows result))))
        ('procedures
         (let ((result
                (pg-exec
                 conn
                 "SELECT p.proname, p.oid
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = current_schema()
  AND p.prokind = 'p'
ORDER BY p.proname")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,oid) row))
                (list :name name :type "PROCEDURE" :schema schema
                      :source-schema schema
                      :identity (format "OID:%s" oid))))
            (clutch-db-pg--rows result))))
        ('functions
         (let ((result
                (pg-exec
                 conn
                 "SELECT p.proname, p.oid
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = current_schema()
  AND p.prokind = 'f'
ORDER BY p.proname")))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,oid) row))
                (list :name name :type "FUNCTION" :schema schema
                      :source-schema schema
                      :identity (format "OID:%s" oid))))
            (clutch-db-pg--rows result))))
        ('triggers
         (let ((result
                (pg-exec
                 conn
                 "SELECT t.trigger_name, t.event_object_table, t.event_manipulation,
        t.action_timing, pg_t.oid
FROM information_schema.triggers t
JOIN pg_class c ON c.relname = t.event_object_table
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_trigger pg_t ON pg_t.tgrelid = c.oid
                    AND pg_t.tgname = t.trigger_name
WHERE t.trigger_schema = current_schema()
  AND NOT pg_t.tgisinternal
ORDER BY t.event_object_table, t.trigger_name")))
           (let ((rows (clutch-db-pg--rows result))
                 grouped)
             (dolist (row rows (nreverse grouped))
               (pcase-let ((`(,name ,table-name ,event ,timing ,oid) row))
                 (if-let* ((existing (cl-find-if
                                      (lambda (entry)
                                        (and (string= (plist-get entry :name) name)
                                             (string= (plist-get entry :target-table) table-name)))
                                      grouped)))
                     (unless (string-match-p (regexp-quote event)
                                             (or (plist-get existing :event) ""))
                       (setf (plist-get existing :event)
                             (concat (plist-get existing :event) " OR " event)))
                   (push (list :name name :type "TRIGGER" :schema schema
                               :source-schema schema
                               :target-table table-name :event event :timing timing
                               :status "ENABLED"
                               :identity (format "OID:%s" oid))
                         grouped)))))))
        (_ nil)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects-async ((conn pgcon) category callback
                                            &optional errback)
  "Fetch PostgreSQL object entries for CATEGORY on CONN when Emacs is idle."
  (clutch-db--schedule-idle-metadata-call
   conn callback errback
   #'clutch-db-list-objects
   category))

(cl-defmethod clutch-db-object-details ((conn pgcon) entry)
  "Return detail plists for PostgreSQL object ENTRY on CONN."
  (condition-case _err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("INDEX"
         (let* ((result
                 (pg-exec
                  conn
                  (format "SELECT a.attname, k.ordinality,
       CASE WHEN ((pi.indoption::int2[])[k.ordinality] & 1) = 1
            THEN 'DESC' ELSE 'ASC' END AS descend
FROM pg_class idx
JOIN pg_namespace n ON n.oid = idx.relnamespace
JOIN pg_index pi ON pi.indexrelid = idx.oid
JOIN LATERAL unnest(pi.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
JOIN pg_attribute a ON a.attrelid = pi.indrelid AND a.attnum = k.attnum
WHERE idx.relkind = 'i'
  AND idx.relname = %s
  AND n.nspname = current_schema()
ORDER BY k.ordinality"
                          (pg-escape-literal (plist-get entry :name))))))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,position ,descend) row))
                (list :name name :position position :descend descend)))
            (clutch-db-pg--rows result))))
        ((or "PROCEDURE" "FUNCTION")
         (let* ((oid (substring (plist-get entry :identity) 4))
                (sql (concat
                      "SELECT name, type, mode, position FROM ("
                      (if (string= (upcase (plist-get entry :type)) "FUNCTION")
                          (format "SELECT NULL::text AS name,
       pg_catalog.format_type(p.prorettype, NULL) AS type,
       'RETURN' AS mode, 0 AS position
FROM pg_proc p
WHERE p.oid = %s
UNION ALL " oid)
                        "")
                      (format "SELECT (p.proargnames::text[])[s.n] AS name,
       pg_catalog.format_type(COALESCE((p.proallargtypes)[s.n],
                                       (p.proargtypes::oid[])[s.n]), NULL) AS type,
       CASE COALESCE((p.proargmodes::text[])[s.n], 'i')
         WHEN 'i' THEN 'IN'
         WHEN 'o' THEN 'OUT'
         WHEN 'b' THEN 'INOUT'
         WHEN 'v' THEN 'VARIADIC'
         WHEN 't' THEN 'TABLE'
         ELSE 'IN'
       END AS mode,
       s.n AS position
FROM pg_proc p
JOIN LATERAL generate_subscripts(COALESCE(p.proallargtypes,
                                          p.proargtypes::oid[]), 1) AS s(n) ON true
WHERE p.oid = %s) args
ORDER BY position" oid)))
                (result (pg-exec conn sql)))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,type ,mode ,position) row))
                (list :name name :type type :mode mode :position position)))
            (clutch-db-pg--rows result))))
        (_ nil))
    (pg-error nil)))

(cl-defmethod clutch-db-object-source ((conn pgcon) entry)
  "Return source text for PostgreSQL object ENTRY on CONN."
  (condition-case err
      (let ((oid (substring (plist-get entry :identity) 4)))
        (pcase (upcase (or (plist-get entry :type) ""))
          ((or "PROCEDURE" "FUNCTION")
           (caar (clutch-db-pg--rows
                  (pg-exec conn (format "SELECT pg_get_functiondef(%s::oid)" oid)))))
          ("TRIGGER"
           (caar (clutch-db-pg--rows
                  (pg-exec conn (format "SELECT pg_get_triggerdef(%s::oid, true)" oid)))))
          (_ nil)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-show-create-object ((conn pgcon) entry)
  "Return DDL text for PostgreSQL non-table ENTRY on CONN."
  (condition-case err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("INDEX"
         (caar (clutch-db-pg--rows
                (pg-exec
                 conn
                 (format "SELECT pg_get_indexdef(idx.oid)
FROM pg_class idx
JOIN pg_namespace n ON n.oid = idx.relnamespace
WHERE idx.relkind = 'i'
  AND idx.relname = %s
  AND n.nspname = current_schema()"
                         (pg-escape-literal (plist-get entry :name)))))))
        ("VIEW"
         (caar (clutch-db-pg--rows
                (pg-exec
                 conn
                 (format "SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(viewname) || E' AS\n' ||
       pg_get_viewdef((quote_ident(schemaname) || '.' || quote_ident(viewname))::regclass, true)
FROM pg_views
WHERE schemaname = current_schema()
  AND viewname = %s"
                         (pg-escape-literal (plist-get entry :name)))))))
        ("SEQUENCE"
         (caar (clutch-db-pg--rows
                (pg-exec
                 conn
                 (format "SELECT format(
  'CREATE SEQUENCE %%I.%%I INCREMENT BY %%s MINVALUE %%s MAXVALUE %%s START WITH %%s;',
  schemaname, sequencename, increment_by, min_value, max_value, start_value)
FROM pg_sequences
WHERE schemaname = current_schema()
  AND sequencename = %s"
                         (pg-escape-literal (plist-get entry :name)))))))
        (_ nil))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-table-comment ((conn pgcon) table)
  "Return the comment for TABLE on PostgreSQL CONN, or nil if none."
  (condition-case _err
      (let* ((result (pg-exec
                      conn
                      (format "SELECT obj_description(c.oid) \
FROM pg_class c \
JOIN pg_namespace n ON n.oid = c.relnamespace \
WHERE c.relname = %s AND n.nspname = current_schema()"
                              (pg-escape-literal table))))
             (row (car (clutch-db-pg--rows result)))
             (comment (car row)))
        (when (and comment (not (string-empty-p comment)))
          comment))
    (pg-error nil)))

(cl-defmethod clutch-db-primary-key-columns ((conn pgcon) table)
  "Return primary key column names for TABLE on PostgreSQL CONN."
  (condition-case _err
      (let ((result (pg-exec
                     conn
                     (format "SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = %s::regclass AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)"
                             (pg-escape-literal table)))))
        (mapcar #'car (clutch-db-pg--rows result)))
    (pg-error nil)))

(defun clutch-db-pg--unique-not-null-identities (conn table)
  "Return unique-not-null row identity candidates for TABLE on CONN."
  (condition-case _err
      (let* ((sql (format "SELECT idx.relname,
       string_agg(a.attname, E'\\x1f' ORDER BY keys.ord) AS columns
FROM pg_index i
JOIN pg_class idx ON idx.oid = i.indexrelid
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = keys.attnum
WHERE i.indrelid = %s::regclass
  AND i.indisunique
  AND NOT i.indisprimary
  AND i.indpred IS NULL
  AND i.indexprs IS NULL
GROUP BY idx.relname
HAVING bool_and(a.attnotnull)
ORDER BY idx.relname"
                          (pg-escape-literal table)))
             (result (pg-exec conn sql)))
        (mapcar (lambda (row)
                  (pcase-let ((`(,name ,columns) row))
                    (list :kind 'unique-key
                          :name name
                          :columns (split-string columns "\x1f" t))))
                (clutch-db-pg--rows result)))
    (pg-error nil)))

(defun clutch-db-pg--ctid-identity (conn table)
  "Return a CTID row locator candidate for TABLE on CONN, or nil."
  (condition-case _err
      (let* ((sql (format "SELECT c.relkind::text
FROM pg_class c
WHERE c.oid = %s::regclass"
                          (pg-escape-literal table)))
             (result (pg-exec conn sql))
             (relkind (car (car (clutch-db-pg--rows result)))))
        (when (or (equal relkind "r")
                  (equal relkind ?r))
          (list :kind 'row-locator
                :name "ctid"
                :select-expressions '("ctid::text")
                :where-sql "ctid = ?::tid")))
    (pg-error nil)))

(cl-defmethod clutch-db-row-identity-candidates ((conn pgcon) table)
  "Return row identity candidates for TABLE on PostgreSQL CONN."
  (append (cl-call-next-method)
          (clutch-db-pg--unique-not-null-identities conn table)
          (when-let* ((ctid (clutch-db-pg--ctid-identity conn table)))
            (list ctid))))

(cl-defmethod clutch-db-foreign-keys ((conn pgcon) table)
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
             (result (pg-exec conn sql)))
        (cl-loop for row in (clutch-db-pg--rows result)
                 collect (pcase-let ((`(,col-name ,ref-table ,ref-column) row))
                           (cons col-name
                                 (list :ref-table ref-table
                                       :ref-column ref-column)))))
    (pg-error nil)))

(cl-defmethod clutch-db-referencing-objects ((conn pgcon) table)
  "Return table entries that reference TABLE on PostgreSQL CONN."
  (condition-case _err
      (let* ((sql (format "SELECT DISTINCT tc.table_name
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
 AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = current_schema()
  AND ccu.table_schema = current_schema()
  AND ccu.table_name = %s"
                          (pg-escape-literal table)))
             (result (pg-exec conn sql)))
        (mapcar (lambda (row)
                  (pcase-let ((`(,name) row))
                    (list :name name :type "TABLE")))
                (clutch-db-pg--rows result)))
    (pg-error nil)))

;;;; Column details

(defun clutch-db-pg--format-type (data-type max-len num-prec num-scale)
  "Build a concise type string for DATA-TYPE.
MAX-LEN, NUM-PREC, and NUM-SCALE refine the rendered PostgreSQL
information_schema type."
  (cond
   ((member data-type '("character varying" "varchar"))
    (if max-len (format "varchar(%s)" max-len) "varchar"))
   ((member data-type '("character" "char"))
    (if max-len (format "char(%s)" max-len) "char"))
   ((string= data-type "numeric")
    (cond ((and num-prec num-scale) (format "numeric(%s,%s)" num-prec num-scale))
          (num-prec                 (format "numeric(%s)" num-prec))
          (t                        "numeric")))
   (t data-type)))

(defun clutch-db-pg--column-details-row (row pk-cols fks)
  "Convert a column-details ROW to a clutch-db column plist.
PK-COLS is a list of primary key column names.
FKS is an alist of (column-name . fk-plist)."
  (pcase-let ((`(,name ,dtype ,nullable-str ,max-len ,num-prec ,num-scale
                 ,default-val ,identity-str ,comment) row))
    (let* ((type     (clutch-db-pg--format-type dtype max-len num-prec num-scale))
           (nullable (string= nullable-str "YES"))
           (pk-p     (member name pk-cols))
           (fk       (cdr (assoc name fks)))
           (generated (or (string= identity-str "YES")
                          (and default-val
                               (string-match-p "\\`nextval(" default-val)))))
      (list :name name :type type :nullable nullable
            :primary-key (and pk-p t)
            :foreign-key fk
            :default (and default-val (not generated) default-val)
            :generated (and generated t)
            :comment (and comment (not (string-empty-p comment)) comment)))))

(cl-defmethod clutch-db-column-details ((conn pgcon) table)
  "Return detailed column info for TABLE on PostgreSQL CONN."
  (condition-case _err
      (let* ((col-result
              (pg-exec
               conn
               (format "SELECT c.column_name, c.data_type, c.is_nullable, \
c.character_maximum_length, c.numeric_precision, c.numeric_scale, \
c.column_default, c.is_identity, col_description(pc.oid, a.attnum) \
FROM information_schema.columns c \
JOIN pg_class pc ON pc.relname = c.table_name \
JOIN pg_namespace pn ON pn.oid = pc.relnamespace \
  AND pn.nspname = c.table_schema \
JOIN pg_attribute a ON a.attrelid = pc.oid AND a.attname = c.column_name \
WHERE c.table_name = %s AND c.table_schema = current_schema() \
ORDER BY c.ordinal_position"
                       (pg-escape-literal table))))
             (col-rows (clutch-db-pg--rows col-result))
             (pk-cols  (clutch-db-primary-key-columns conn table))
             (fks      (clutch-db-foreign-keys conn table)))
        (mapcar (lambda (row) (clutch-db-pg--column-details-row row pk-cols fks))
                col-rows))
    (pg-error nil)))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn pgcon))
  "Return non-nil if PostgreSQL CONN is executing a query."
  (pg-connection-busy-p conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn pgcon))
  "Return the user for PostgreSQL CONN."
  (clutch-db-pg--connect-value conn 'user))

(cl-defmethod clutch-db-host ((conn pgcon))
  "Return the host for PostgreSQL CONN."
  (clutch-db-pg--connect-value conn 'host))

(cl-defmethod clutch-db-port ((conn pgcon))
  "Return the port for PostgreSQL CONN."
  (clutch-db-pg--connect-value conn 'port))

(cl-defmethod clutch-db-database ((conn pgcon))
  "Return the database for PostgreSQL CONN."
  (pgcon-dbname conn))

(cl-defmethod clutch-db-display-name ((_conn pgcon))
  "Return \"PostgreSQL\" as the display name."
  "PostgreSQL")

(provide 'clutch-db-pg)
;;; clutch-db-pg.el ends here
