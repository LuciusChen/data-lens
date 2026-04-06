;;; clutch-db-pg.el --- Native backend over the PostgreSQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
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
(require 'clutch-worker)
(require 'pg)

(declare-function clutch--connection-context "clutch-connection" (conn))

(defvar pg-connect-timeout)
(defvar pg-read-timeout)

(declare-function pg-result "pg" (result what &rest arg))
(declare-function pg-exec "pg" (con &rest args))
(declare-function pg-connect-plist "pg" (dbname user &rest args))
(declare-function pg-disconnect "pg" (con))
(declare-function pg-cancel "pg" (con))
(declare-function pg-connection-busy-p "pg" (con))
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

(defun clutch-db-pg--wrap-result (pg-result)
  "Convert PG-RESULT to a `clutch-db-result'."
  (let ((cols (clutch-db-pg--columns pg-result)))
    (make-clutch-db-result
     :connection (clutch-db-pg--result-connection pg-result)
     :columns (when cols (clutch-db-pg--convert-columns cols))
     :rows (clutch-db-pg--rows pg-result)
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
                 (connect-args
                  (cl-loop for key in '(:password :host :port)
                           when (plist-member params key)
                           append (list key (plist-get params key)))))
            (setq conn
                  (pcase sslmode
                    ('prefer
                     (if (and (fboundp 'gnutls-available-p)
                              (gnutls-available-p))
                         (condition-case tls-err
                             (apply #'pg-connect-plist
                                    dbname user
                                    (append connect-args
                                            (list :tls-options
                                                  (clutch-db-pg--tls-options sslmode))))
                           (pg-protocol-error
                            (if (clutch-db-pg--prefer-fallback-p tls-err)
                                (apply #'pg-connect-plist dbname user connect-args)
                              (signal (car tls-err) (cdr tls-err)))))
                       (apply #'pg-connect-plist dbname user connect-args)))
                    ((or 'require 'verify-full)
                     (apply #'pg-connect-plist
                            dbname user
                            (append connect-args
                                    (list :tls-options
                                          (clutch-db-pg--tls-options sslmode)))))
                    (_
                     (apply #'pg-connect-plist dbname user connect-args)))))
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

;;;; Query methods

(cl-defmethod clutch-db-query ((conn pgcon) sql)
  "Execute SQL on PostgreSQL CONN, returning a `clutch-db-result'."
  (condition-case err
      (clutch-db-pg--wrap-result (pg-exec conn sql))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

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

(defun clutch-db-pg--metadata-params (conn)
  "Return reconnect PARAMS for PostgreSQL metadata work on CONN."
  (if-let* ((context (clutch--connection-context conn))
            (params (car context)))
      params
    (signal 'clutch-db-error
            (list "PostgreSQL metadata refresh requires stored connection params"))))

(cl-defmethod clutch-db-open-metadata-context ((_conn pgcon) params)
  "Return a dedicated PostgreSQL metadata context built from PARAMS."
  (let ((context (clutch-db-pg-connect params)))
    (clutch-db-init-connection context)
    context))

(cl-defmethod clutch-db-close-metadata-context ((_conn pgcon) (context pgcon))
  "Close dedicated PostgreSQL metadata CONTEXT."
  (clutch-db-disconnect context))

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
  "Refresh PostgreSQL schema names for CONN on a background worker."
  (clutch--worker-submit
   conn
   (lambda ()
     (let (context)
       (unwind-protect
           (progn
             (setq context
                   (clutch-db-open-metadata-context
                    conn (clutch-db-pg--metadata-params conn)))
             (clutch-db-list-tables context))
         (when context
           (clutch-db-close-metadata-context conn context)))))
   callback
   errback))

(cl-defmethod clutch-db-list-columns-async ((conn pgcon) table callback
                                            &optional errback)
  "Fetch PostgreSQL column names for TABLE on CONN on a background worker."
  (clutch--worker-submit
   conn
   (lambda ()
     (let (context)
       (unwind-protect
           (progn
             (setq context
                   (clutch-db-open-metadata-context
                    conn (clutch-db-pg--metadata-params conn)))
             (clutch-db-list-columns context table))
         (when context
           (clutch-db-close-metadata-context conn context)))))
   callback
   errback))

(cl-defmethod clutch-db-column-details-async ((conn pgcon) table callback
                                              &optional errback)
  "Fetch PostgreSQL column details for TABLE on CONN on a background worker."
  (clutch--worker-submit
   conn
   (lambda ()
     (let (context)
       (unwind-protect
           (progn
             (setq context
                   (clutch-db-open-metadata-context
                    conn (clutch-db-pg--metadata-params conn)))
             (clutch-db-column-details context table))
         (when context
           (clutch-db-close-metadata-context conn context)))))
   callback
   errback))

(cl-defmethod clutch-db-table-comment-async ((conn pgcon) table callback
                                             &optional errback)
  "Fetch the PostgreSQL comment for TABLE on CONN on a background worker."
  (clutch--worker-submit
   conn
   (lambda ()
     (let (context)
       (unwind-protect
           (progn
             (setq context
                   (clutch-db-open-metadata-context
                    conn (clutch-db-pg--metadata-params conn)))
             (clutch-db-table-comment context table))
         (when context
           (clutch-db-close-metadata-context conn context)))))
   callback
   errback))

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
      (let ((result (pg-exec
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
                   :schema "current_schema"
                   :source-schema "current_schema")))
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
                (list :name name :type "INDEX" :schema "current_schema"
                      :source-schema "current_schema"
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
                (list :name name :type "SEQUENCE" :schema "current_schema"
                      :source-schema "current_schema"
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
                (list :name name :type "PROCEDURE" :schema "current_schema"
                      :source-schema "current_schema"
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
                (list :name name :type "FUNCTION" :schema "current_schema"
                      :source-schema "current_schema"
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
                   (push (list :name name :type "TRIGGER" :schema "current_schema"
                               :source-schema "current_schema"
                               :target-table table-name :event event :timing timing
                               :status "ENABLED"
                               :identity (format "OID:%s" oid))
                         grouped)))))))
        (_ nil))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects-async ((conn pgcon) category callback
                                            &optional errback)
  "Fetch PostgreSQL object entries for CATEGORY on CONN on a background worker."
  (clutch--worker-submit
   conn
   (lambda ()
     (let (context)
       (unwind-protect
           (progn
             (setq context
                   (clutch-db-open-metadata-context
                    conn (clutch-db-pg--metadata-params conn)))
             (clutch-db-list-objects context category))
         (when context
           (clutch-db-close-metadata-context conn context)))))
   callback
   errback))

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
