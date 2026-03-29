;;; clutch-db-pg.el --- PostgreSQL backend for clutch-db -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

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
;; Implements all `clutch-db-*' generics by dispatching on `pg-conn'.

;;; Code:

(require 'clutch-db)
(require 'pg)

;;;; OID → type-category mapping

(defconst clutch-db-pg--type-category-alist
  `((,pg-oid-int2      . numeric)
    (,pg-oid-int4      . numeric)
    (,pg-oid-int8      . numeric)
    (,pg-oid-float4    . numeric)
    (,pg-oid-float8    . numeric)
    (,pg-oid-numeric   . numeric)
    (,pg-oid-bool      . text)
    (,pg-oid-json      . json)
    (,pg-oid-jsonb     . json)
    (,pg-oid-bytea     . blob)
    (,pg-oid-date      . date)
    (,pg-oid-time      . time)
    (,pg-oid-timestamp . datetime)
    (,pg-oid-timestamptz . datetime))
  "Alist mapping PostgreSQL OIDs to type-category symbols.")

(defun clutch-db-pg--type-category (oid)
  "Map a PostgreSQL type OID to a type-category symbol."
  (or (alist-get oid clutch-db-pg--type-category-alist)
      'text))

(defun clutch-db-pg--convert-columns (pg-columns)
  "Convert PG-COLUMNS to `clutch-db' column plists."
  (mapcar (lambda (col)
            (list :name (plist-get col :name)
                  :type-category (clutch-db-pg--type-category
                                  (plist-get col :type-oid))))
          pg-columns))

(defun clutch-db-pg--wrap-result (pg-result)
  "Convert PG-RESULT to a `clutch-db-result'."
  (let ((cols (pg-result-columns pg-result)))
    (make-clutch-db-result
     :connection (pg-result-connection pg-result)
     :columns (when cols (clutch-db-pg--convert-columns cols))
     :rows (pg-result-rows pg-result)
     :affected-rows (pg-result-affected-rows pg-result)
     :last-insert-id nil
     :warnings nil)))

(defconst clutch-db-pg--current-schema-cache-key "clutch_current_schema"
  "Connection-local cache key for the effective PostgreSQL schema.")

(defun clutch-db-pg--cached-current-schema (conn)
  "Return cached current schema for CONN, or nil."
  (cdr (assoc-string clutch-db-pg--current-schema-cache-key
                     (pg-conn-parameters conn)
                     t)))

(defun clutch-db-pg--cache-current-schema (conn schema)
  "Cache SCHEMA as the current schema for CONN."
  (setf (pg-conn-parameters conn)
        (cons (cons clutch-db-pg--current-schema-cache-key schema)
              (cl-remove-if
               (lambda (entry)
                 (string-equal (car entry) clutch-db-pg--current-schema-cache-key))
               (pg-conn-parameters conn))))
  schema)

(defun clutch-db-pg--set-search-path (conn schema)
  "Set CONN search_path to SCHEMA and update the local cache."
  (let ((schema (string-trim schema)))
    (pg-query conn
              (format "SET search_path TO %s"
                      (pg-escape-identifier schema)))
    (clutch-db-pg--cache-current-schema conn schema)))

;;;; Connect function

(defun clutch-db-pg-connect (params)
  "Connect to PostgreSQL using PARAMS plist.
PARAMS keys: :host, :port, :user, :password, :database, :tls,
:connect-timeout, :read-idle-timeout, :query-timeout."
  (let ((schema (plist-get params :schema))
        conn)
    (condition-case err
        (progn
          (setq conn
                (apply #'pg-connect
                       (cl-loop for (k v) on params by #'cddr
                                unless (memq k '(:sql-product :backend :schema))
                                append (list k v))))
          (when schema
            (clutch-db-pg--set-search-path conn schema))
          conn)
      (pg-error
       (when conn
         (ignore-errors (pg-disconnect conn)))
       (signal 'clutch-db-error
               (list (error-message-string err)))))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn pg-conn))
  "Disconnect PostgreSQL CONN."
  (condition-case nil
      (pg-disconnect conn)
    (pg-error nil)))

(cl-defmethod clutch-db-live-p ((conn pg-conn))
  "Return non-nil if PostgreSQL CONN is live."
  (and conn
       (pg-conn-p conn)
       (process-live-p (pg-conn-process conn))))

(cl-defmethod clutch-db-init-connection ((_conn pg-conn))
  "Initialize PostgreSQL CONN.
No special init needed — encoding is set in startup message.")

;;;; Query methods

(cl-defmethod clutch-db-query ((conn pg-conn) sql)
  "Execute SQL on PostgreSQL CONN, returning a `clutch-db-result'."
  (condition-case err
      (clutch-db-pg--wrap-result (pg-query conn sql))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-interrupt-query ((conn pg-conn))
  "Interrupt the current PostgreSQL query on CONN without dropping the session."
  (condition-case nil
      (pg-cancel-query conn)
    (pg-error nil)))

(cl-defmethod clutch-db-build-paged-sql ((_conn pg-conn) base-sql
                                             page-num page-size
                                             &optional order-by)
  "Build a paginated SQL query for PostgreSQL from BASE-SQL.
PAGE-NUM is zero-based, PAGE-SIZE limits each page, and ORDER-BY
controls the optional sort clause."
  (clutch-db--build-limit-offset-paged-sql
   base-sql page-num page-size order-by #'pg-escape-identifier))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn pg-conn) name)
  "Escape NAME as a PostgreSQL identifier (double-quoted)."
  (pg-escape-identifier name))

(cl-defmethod clutch-db-escape-literal ((_conn pg-conn) value)
  "Escape VALUE as a PostgreSQL string literal."
  (pg-escape-literal value))

;;;; Schema methods

(cl-defmethod clutch-db-list-schemas ((conn pg-conn))
  "Return visible schema names for PostgreSQL CONN."
  (condition-case err
      (let ((result (pg-query
                     conn
                     "SELECT schema_name FROM information_schema.schemata \
WHERE schema_name <> 'information_schema' \
  AND schema_name NOT LIKE 'pg\\_%' ESCAPE '\\' \
ORDER BY schema_name")))
        (mapcar #'car (pg-result-rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-current-schema ((conn pg-conn))
  "Return the current effective schema for PostgreSQL CONN."
  (or (clutch-db-pg--cached-current-schema conn)
      (condition-case err
          (let* ((result (pg-query conn "SELECT current_schema()"))
                 (schema (caar (pg-result-rows result))))
            (when schema
              (clutch-db-pg--cache-current-schema conn schema)))
        (pg-error
         (signal 'clutch-db-error
                 (list (error-message-string err)))))))

(cl-defmethod clutch-db-set-current-schema ((conn pg-conn) schema)
  "Switch PostgreSQL CONN to SCHEMA via search_path."
  (condition-case err
      (clutch-db-pg--set-search-path conn schema)
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-tables ((conn pg-conn))
  "Return table names for the current PostgreSQL database on CONN."
  (condition-case err
      (let ((result (pg-query
                     conn
                     "SELECT tablename FROM pg_tables \
WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
ORDER BY tablename")))
        (mapcar #'car (pg-result-rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-table-entries ((conn pg-conn))
  "Return table/view entry plists for the current PostgreSQL schema on CONN."
  (condition-case err
      (let ((result (pg-query
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
         (pg-result-rows result)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-columns ((conn pg-conn) table)
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

(cl-defmethod clutch-db-show-create-table ((conn pg-conn) table)
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
             (lines (mapcar #'clutch-db-pg--format-column-ddl
                            (pg-result-rows cols-result))))
        (format "CREATE TABLE %s (\n%s\n);"
                (pg-escape-identifier table)
                (mapconcat #'identity lines ",\n")))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-list-objects ((conn pg-conn) category)
  "Return object entry plists for CATEGORY on PostgreSQL CONN."
  (condition-case err
      (pcase category
        ('indexes
         (let ((result
                (pg-query
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
            (pg-result-rows result))))
        ('sequences
         (let ((result
                (pg-query
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
            (pg-result-rows result))))
        ('procedures
         (let ((result
                (pg-query
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
            (pg-result-rows result))))
        ('functions
         (let ((result
                (pg-query
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
            (pg-result-rows result))))
        ('triggers
         (let ((result
                (pg-query
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
           (let ((rows (pg-result-rows result))
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

(cl-defmethod clutch-db-object-details ((conn pg-conn) entry)
  "Return detail plists for PostgreSQL object ENTRY on CONN."
  (condition-case _err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("INDEX"
         (let* ((result
                 (pg-query
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
            (pg-result-rows result))))
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
                (result (pg-query conn sql)))
           (mapcar
            (lambda (row)
              (pcase-let ((`(,name ,type ,mode ,position) row))
                (list :name name :type type :mode mode :position position)))
            (pg-result-rows result))))
        (_ nil))
    (pg-error nil)))

(cl-defmethod clutch-db-object-source ((conn pg-conn) entry)
  "Return source text for PostgreSQL object ENTRY on CONN."
  (condition-case err
      (let ((oid (substring (plist-get entry :identity) 4)))
        (pcase (upcase (or (plist-get entry :type) ""))
          ((or "PROCEDURE" "FUNCTION")
           (caar (pg-result-rows
                  (pg-query conn (format "SELECT pg_get_functiondef(%s::oid)" oid)))))
          ("TRIGGER"
           (caar (pg-result-rows
                  (pg-query conn (format "SELECT pg_get_triggerdef(%s::oid, true)" oid)))))
          (_ nil)))
    (pg-error
     (signal 'clutch-db-error
             (list (error-message-string err))))))

(cl-defmethod clutch-db-show-create-object ((conn pg-conn) entry)
  "Return DDL text for PostgreSQL non-table ENTRY on CONN."
  (condition-case err
      (pcase (upcase (or (plist-get entry :type) ""))
        ("INDEX"
         (caar (pg-result-rows
                (pg-query
                 conn
                 (format "SELECT pg_get_indexdef(idx.oid)
FROM pg_class idx
JOIN pg_namespace n ON n.oid = idx.relnamespace
WHERE idx.relkind = 'i'
  AND idx.relname = %s
  AND n.nspname = current_schema()"
                         (pg-escape-literal (plist-get entry :name)))))))
        ("VIEW"
         (caar (pg-result-rows
                (pg-query
                 conn
                 (format "SELECT 'CREATE OR REPLACE VIEW ' || quote_ident(viewname) || E' AS\n' ||
       pg_get_viewdef((quote_ident(schemaname) || '.' || quote_ident(viewname))::regclass, true)
FROM pg_views
WHERE schemaname = current_schema()
  AND viewname = %s"
                         (pg-escape-literal (plist-get entry :name)))))))
        ("SEQUENCE"
         (caar (pg-result-rows
                (pg-query
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

(cl-defmethod clutch-db-table-comment ((conn pg-conn) table)
  "Return the comment for TABLE on PostgreSQL CONN, or nil if none."
  (condition-case _err
      (let* ((result (pg-query
                      conn
                      (format "SELECT obj_description(c.oid) \
FROM pg_class c \
JOIN pg_namespace n ON n.oid = c.relnamespace \
WHERE c.relname = %s AND n.nspname = current_schema()"
                              (pg-escape-literal table))))
             (row (car (pg-result-rows result)))
             (comment (car row)))
        (when (and comment (not (string-empty-p comment)))
          comment))
    (pg-error nil)))

(cl-defmethod clutch-db-primary-key-columns ((conn pg-conn) table)
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

(cl-defmethod clutch-db-foreign-keys ((conn pg-conn) table)
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
                 collect (pcase-let ((`(,col-name ,ref-table ,ref-column) row))
                           (cons col-name
                                 (list :ref-table ref-table
                                       :ref-column ref-column)))))
    (pg-error nil)))

(cl-defmethod clutch-db-referencing-objects ((conn pg-conn) table)
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
             (result (pg-query conn sql)))
        (mapcar (lambda (row)
                  (pcase-let ((`(,name) row))
                    (list :name name :type "TABLE")))
                (pg-result-rows result)))
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

(cl-defmethod clutch-db-column-details ((conn pg-conn) table)
  "Return detailed column info for TABLE on PostgreSQL CONN."
  (condition-case _err
      (let* ((col-result
              (pg-query
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
             (col-rows (pg-result-rows col-result))
             (pk-cols  (clutch-db-primary-key-columns conn table))
             (fks      (clutch-db-foreign-keys conn table)))
        (mapcar (lambda (row) (clutch-db-pg--column-details-row row pk-cols fks))
                col-rows))
    (pg-error nil)))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn pg-conn))
  "Return non-nil if PostgreSQL CONN is executing a query."
  (pg-conn-busy conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn pg-conn))
  "Return the user for PostgreSQL CONN."
  (pg-conn-user conn))

(cl-defmethod clutch-db-host ((conn pg-conn))
  "Return the host for PostgreSQL CONN."
  (pg-conn-host conn))

(cl-defmethod clutch-db-port ((conn pg-conn))
  "Return the port for PostgreSQL CONN."
  (pg-conn-port conn))

(cl-defmethod clutch-db-database ((conn pg-conn))
  "Return the database for PostgreSQL CONN."
  (pg-conn-database conn))

(cl-defmethod clutch-db-display-name ((_conn pg-conn))
  "Return \"PostgreSQL\" as the display name."
  "PostgreSQL")

(provide 'clutch-db-pg)
;;; clutch-db-pg.el ends here
