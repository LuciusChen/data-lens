;;; clutch-db-jdbc.el --- JDBC backend for clutch-db via clutch-jdbc-agent -*- lexical-binding: t; -*-

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

;; JDBC backend for the clutch generic database interface.
;; Delegates to clutch-jdbc-agent (a JVM sidecar process) via a
;; single-line JSON protocol on stdin/stdout.
;;
;; Usage:
;;   (require 'clutch-db-jdbc)
;;   (clutch-connect '(:driver oracle :host db.corp.com :port 1521
;;                     :database ORCL :user scott :pass-entry my-entry))
;;
;; The agent jar is downloaded on first use.  JDBC drivers must be placed
;; manually in `clutch-jdbc-drivers-dir' (Oracle, DB2) or installed via
;; `clutch-jdbc-install-driver' (drivers available on Maven Central).

;;; Code:

(require 'clutch-db)
(require 'json)

;;;; Configuration

(defgroup clutch-jdbc nil
  "JDBC backend for clutch."
  :group 'clutch)

(defcustom clutch-jdbc-agent-dir
  (expand-file-name "clutch-jdbc" user-emacs-directory)
  "Directory containing clutch-jdbc-agent.jar and drivers/ subdirectory."
  :type 'directory
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-version "0.1.0"
  "Version of clutch-jdbc-agent to use."
  :type 'string
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-java-executable "java"
  "Java executable used to launch clutch-jdbc-agent."
  :type 'string
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-fetch-size 500
  "Number of rows fetched per batch from the agent."
  :type 'natnum
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-rpc-timeout 30
  "Seconds to wait for a response from the agent before giving up."
  :type 'natnum
  :group 'clutch-jdbc)

;;;; Driver sources (for automatic installation from Maven Central)

(defconst clutch-jdbc--driver-sources
  '((sqlserver . (:maven "com.microsoft.sqlserver:mssql-jdbc:12.6.0"
                  :filename "mssql-jdbc.jar"))
    (snowflake . (:maven "net.snowflake:snowflake-jdbc:3.14.4"
                  :filename "snowflake-jdbc.jar"))
    (oracle    . (:maven "com.oracle.database.jdbc:ojdbc11:21.13.0.0"
                  :filename "ojdbc11.jar"))
    (oracle-i18n . (:maven "com.oracle.database.nls:orai18n:21.13.0.0"
                    :filename "orai18n.jar"))
    (db2       . (:manual "https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
                  :filename "db2jcc4.jar")))
  "Known JDBC driver sources.
All entries support auto-download via `clutch-jdbc-install-driver'.")

;;;; Drivers that default to JDBC backend

(defconst clutch-jdbc--jdbc-drivers
  '(oracle sqlserver db2 snowflake redshift)
  "Driver symbols that are automatically routed to the JDBC backend.")

;;;; Connection struct

(cl-defstruct clutch-jdbc-conn
  "A JDBC connection managed by clutch-jdbc-agent."
  process   ; the shared agent process
  conn-id   ; integer handle in the agent's ConnectionManager
  params    ; original connection plist (for metadata)
  busy)     ; non-nil while a query is running

;;;; Agent process (one shared process for all JDBC connections)

(defvar clutch-jdbc--agent-process nil
  "The running clutch-jdbc-agent process, or nil if not started.")

(defvar clutch-jdbc--response-queue nil
  "List of JSON response strings received from the agent, oldest first.")

(defun clutch-jdbc--agent-jar ()
  "Return the path to the clutch-jdbc-agent jar."
  (expand-file-name
   (format "clutch-jdbc-agent-%s.jar" clutch-jdbc-agent-version)
   clutch-jdbc-agent-dir))

(defun clutch-jdbc--drivers-dir ()
  "Return the drivers/ directory path."
  (expand-file-name "drivers" clutch-jdbc-agent-dir))

(defun clutch-jdbc--agent-live-p ()
  "Return non-nil if the agent process is running."
  (and clutch-jdbc--agent-process
       (process-live-p clutch-jdbc--agent-process)))

(defun clutch-jdbc--agent-filter (proc string)
  "Process filter: collect complete JSON lines from PROC output STRING."
  (let ((buf (process-buffer proc)))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (goto-char (point-max))
        (insert string)
        ;; Collect complete lines into the response queue.
        (goto-char (point-min))
        (while (search-forward "\n" nil t)
          (let ((line (string-trim (buffer-substring (point-min) (point)))))
            (delete-region (point-min) (point))
            (goto-char (point-min))
            (unless (string-empty-p line)
              (setq clutch-jdbc--response-queue
                    (nconc clutch-jdbc--response-queue (list line))))))))))

(defun clutch-jdbc--start-agent ()
  "Start the clutch-jdbc-agent process and wait for its ready signal."
  (let ((jar (clutch-jdbc--agent-jar)))
    (unless (file-exists-p jar)
      (user-error "clutch-jdbc-agent jar not found: %s\nRun M-x clutch-jdbc-ensure-agent" jar))
    (unless (executable-find clutch-jdbc-agent-java-executable)
      (user-error "Java not found.  Set `clutch-jdbc-agent-java-executable'"))
    (let* ((buf (generate-new-buffer " *clutch-jdbc-agent*"))
           (proc (make-process
                  :name "clutch-jdbc-agent"
                  :buffer buf
                  :command (list clutch-jdbc-agent-java-executable
                                 "-jar" jar
                                 (clutch-jdbc--drivers-dir))
                  :connection-type 'pipe
                  :filter #'clutch-jdbc--agent-filter
                  :stderr (get-buffer-create "*clutch-jdbc-agent-stderr*")
                  :noquery t)))
      (setq clutch-jdbc--agent-process proc)
      (setq clutch-jdbc--response-queue nil)
      ;; Wait for the ready message (id=0).
      (let ((ready (clutch-jdbc--recv-response 0)))
        (unless (plist-get ready :ok)
          (error "clutch-jdbc-agent failed to start: %s" (plist-get ready :error))))
      proc)))

(defun clutch-jdbc--ensure-agent ()
  "Ensure the agent process is running, starting it if necessary."
  (unless (clutch-jdbc--agent-live-p)
    (setq clutch-jdbc--response-queue nil)
    (clutch-jdbc--start-agent)))

;;;; Synchronous RPC

(defvar clutch-jdbc--next-request-id 1
  "Auto-incrementing request id counter.")

(defun clutch-jdbc--send (op params)
  "Send a request to the agent and return the request id."
  (let* ((id (cl-incf clutch-jdbc--next-request-id))
         (msg (json-encode `((id . ,id) (op . ,op) (params . ,params)))))
    (process-send-string clutch-jdbc--agent-process (concat msg "\n"))
    id))

(defun clutch-jdbc--recv-response (id)
  "Wait for and return the response with matching ID as a plist."
  (let ((deadline (+ (float-time) clutch-jdbc-rpc-timeout))
        response)
    (while (and (not response) (< (float-time) deadline))
      ;; Drain any queued lines.
      (while (and (not response) clutch-jdbc--response-queue)
        (let* ((line (pop clutch-jdbc--response-queue))
               (parsed (condition-case nil
                           (json-parse-string line :object-type 'plist
                                              :array-type 'list
                                              :null-object nil
                                              :false-object :false)
                         (error nil))))
          (when (and parsed (eql (plist-get parsed :id) id))
            (setq response parsed))))
      (unless response
        (accept-process-output clutch-jdbc--agent-process 0.05)))
    (unless response
      (error "clutch-jdbc-agent: timeout waiting for response to request %d" id))
    response))

(defun clutch-jdbc--rpc (op params)
  "Send OP with PARAMS to the agent and return the result plist.
Signals `clutch-db-error' on agent-reported errors."
  (clutch-jdbc--ensure-agent)
  (let* ((id (clutch-jdbc--send op params))
         (response (clutch-jdbc--recv-response id)))
    (if (plist-get response :ok)
        (plist-get response :result)
      (signal 'clutch-db-error
              (list (or (plist-get response :error)
                        (format "agent error on op %s" op)))))))

;;;; JDBC URL builder

(defun clutch-jdbc--build-url (driver params)
  "Build a JDBC URL for DRIVER using connection PARAMS plist.
If :url is present in PARAMS it is used as-is (allows full override).
Otherwise constructs a URL from :host, :port, and :database (service name)
or :sid (Oracle SID-style connection)."
  (or (plist-get params :url)
      (let ((host     (or (plist-get params :host) "localhost"))
            (port     (plist-get params :port))
            (database (plist-get params :database))
            (sid      (plist-get params :sid)))
        (pcase driver
          ('oracle
           (if sid
               (format "jdbc:oracle:thin:@%s:%d:%s"
                       host (or port 1521) sid)
             (format "jdbc:oracle:thin:@//%s:%d/%s"
                     host (or port 1521) database)))
          ('sqlserver
           (format "jdbc:sqlserver://%s:%d;databaseName=%s"
                   host (or port 1433) database))
          ('db2
           (format "jdbc:db2://%s:%d/%s"
                   host (or port 50000) database))
          ('snowflake
           (format "jdbc:snowflake://%s.snowflakecomputing.com/?db=%s"
                   host database))
          ('redshift
           (format "jdbc:redshift://%s:%d/%s"
                   host (or port 5439) database))
          (_
           (error "clutch-db-jdbc: unknown driver %s; provide :url directly" driver))))))

;;;; Connect function

(defun clutch-db-jdbc-connect (driver params)
  "Connect to a JDBC data source of type DRIVER using PARAMS plist.
DRIVER is a symbol (e.g. \\='oracle, \\='sqlserver) captured by the
registration closure — users do not pass it directly.
Returns a `clutch-jdbc-conn'."
  (clutch-jdbc--ensure-agent)
  (let* ((url      (clutch-jdbc--build-url driver params))
         (user     (plist-get params :user))
         (password (plist-get params :password))
         (props    (plist-get params :props))
         (result   (clutch-jdbc--rpc
                    "connect"
                    `((url      . ,url)
                      (user     . ,user)
                      (password . ,password)
                      ,@(when props `((props . ,props)))))))
    (make-clutch-jdbc-conn
     :process  clutch-jdbc--agent-process
     :conn-id  (plist-get result :conn-id)
     :params   (plist-put (copy-sequence params) :driver driver)
     :busy     nil)))

;;;; Register backend

;; Each JDBC driver gets its own closure so the driver type is available
;; inside clutch-db-jdbc-connect without requiring a redundant :driver key
;; in the user's params plist (:backend is stripped by clutch--build-conn
;; before the connect-fn is called).
(with-eval-after-load 'clutch-db
  (dolist (driver clutch-jdbc--jdbc-drivers)
    (unless (alist-get driver clutch-db--backend-features)
      (let ((drv driver))
        (push (cons drv
                    (list :require 'clutch-db-jdbc
                          :connect-fn (lambda (p) (clutch-db-jdbc-connect drv p))))
              clutch-db--backend-features)))))

;;;; Lifecycle methods

(cl-defmethod clutch-db-disconnect ((conn clutch-jdbc-conn))
  "Disconnect JDBC CONN, releasing it in the agent."
  (condition-case nil
      (clutch-jdbc--rpc "disconnect"
                        `((conn-id . ,(clutch-jdbc-conn-conn-id conn))))
    (clutch-db-error nil)))

(cl-defmethod clutch-db-live-p ((conn clutch-jdbc-conn))
  "Return non-nil if the agent process is running and conn-id is valid."
  (and (clutch-jdbc-conn-p conn)
       (process-live-p (clutch-jdbc-conn-process conn))))

(cl-defmethod clutch-db-init-connection ((_conn clutch-jdbc-conn))
  "No post-connect initialization needed for JDBC connections.")

;;;; Query methods

(defun clutch-jdbc--fetch-all (cursor-id)
  "Fetch all remaining rows for CURSOR-ID, returning a flat list of rows."
  (let (all-rows done)
    (while (not done)
      (let ((result (clutch-jdbc--rpc "fetch"
                                      `((cursor-id  . ,cursor-id)
                                        (fetch-size . ,clutch-jdbc-fetch-size)))))
        (setq all-rows (nconc all-rows (plist-get result :rows)))
        (setq done (plist-get result :done))))
    all-rows))

(defun clutch-jdbc--type-category (jdbc-type-name)
  "Map a JDBC type name string to a clutch-db type-category symbol."
  (let ((t-upper (upcase (or jdbc-type-name ""))))
    (cond
     ((string-match-p "INT\\|SMALLINT\\|BIGINT\\|TINYINT\\|NUMBER\\|NUMERIC\\|DECIMAL\\|FLOAT\\|DOUBLE\\|REAL" t-upper) 'numeric)
     ((string-match-p "BOOL" t-upper)                           'text)
     ((string-match-p "JSON" t-upper)                           'json)
     ((string-match-p "BLOB\\|BINARY\\|VARBINARY\\|RAW\\|IMAGE" t-upper) 'blob)
     ((string-match-p "TIMESTAMP\\|DATETIME" t-upper)           'datetime)
     ((string-match-p "DATE" t-upper)                           'date)
     ((string-match-p "TIME$" t-upper)                          'time)
     (t                                                         'text))))

(defun clutch-jdbc--make-columns (col-names col-types)
  "Build clutch-db column plists from agent COL-NAMES and COL-TYPES lists."
  (cl-mapcar (lambda (name type)
               (list :name name
                     :type-category (clutch-jdbc--type-category type)))
             col-names col-types))

(defun clutch-jdbc--normalize-row (row)
  "Convert JDBC-specific value representations in ROW to generic forms.
Blob plists with :text content become plain strings."
  (mapcar (lambda (val)
            (if (and (listp val)
                     (equal (plist-get val :__type) "blob")
                     (plist-get val :text))
                (plist-get val :text)
              val))
          row))

(cl-defmethod clutch-db-query ((conn clutch-jdbc-conn) sql)
  "Execute SQL on JDBC CONN and return a `clutch-db-result'."
  (setf (clutch-jdbc-conn-busy conn) t)
  (unwind-protect
      (condition-case err
          (let* ((result (clutch-jdbc--rpc
                          "execute"
                          `((conn-id    . ,(clutch-jdbc-conn-conn-id conn))
                            (sql        . ,sql)
                            (fetch-size . ,clutch-jdbc-fetch-size))))
                 (type   (plist-get result :type)))
            (if (equal type "dml")
                ;; DML: no rows, just affected-rows.
                (make-clutch-db-result
                 :connection    conn
                 :affected-rows (plist-get result :affected-rows))
              ;; SELECT: consume remaining pages, return full result.
              (let* ((first-rows  (plist-get result :rows))
                     (cursor-id   (plist-get result :cursor-id))
                     (done        (plist-get result :done))
                     (all-rows    (if done first-rows
                                    (nconc first-rows
                                           (clutch-jdbc--fetch-all cursor-id))))
                     (columns     (clutch-jdbc--make-columns
                                   (plist-get result :columns)
                                   (plist-get result :col-types))))
                (make-clutch-db-result
                 :connection conn
                 :columns    columns
                 :rows       (mapcar #'clutch-jdbc--normalize-row all-rows)))))
        (clutch-db-error (signal (car err) (cdr err))))
    (setf (clutch-jdbc-conn-busy conn) nil)))

(defun clutch-jdbc--build-oracle-paged-sql (conn base offset page-size order-by)
  "Build Oracle ROWNUM-based pagination SQL.
Compatible with all Oracle versions (9i+).
Page N (OFFSET>0) adds an rn column as a side effect."
  (let ((inner (if order-by
                   (format "%s ORDER BY %s %s" base
                           (clutch-db-escape-identifier conn (car order-by))
                           (cdr order-by))
                 base)))
    (if (= offset 0)
        (format "SELECT * FROM (%s) WHERE ROWNUM <= %d" inner page-size)
      (format (concat "SELECT * FROM ("
                      "SELECT t.*, ROWNUM rn FROM (%s) t "
                      "WHERE ROWNUM <= %d"
                      ") WHERE rn > %d")
              inner (+ offset page-size) offset))))

(cl-defmethod clutch-db-build-paged-sql ((conn clutch-jdbc-conn) base-sql
                                         page-num page-size
                                         &optional order-by)
  "Build a paginated SQL query for JDBC connections.
Oracle uses ROWNUM subquery (compatible with all Oracle versions).
Other databases use SQL:2011 OFFSET/FETCH (Oracle 12c+, SQL Server 2012+, DB2)."
  (if (clutch-db-sql-has-top-level-limit-p base-sql)
      base-sql
    (let* ((trimmed (string-trim-right
                     (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
           (offset  (* page-num page-size))
           (oracle-p (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)))
      (if oracle-p
          (clutch-jdbc--build-oracle-paged-sql conn trimmed offset page-size order-by)
        (let ((order-clause (if order-by
                                (format " ORDER BY %s %s"
                                        (clutch-db-escape-identifier conn (car order-by))
                                        (cdr order-by))
                              " ORDER BY (SELECT NULL)")))
          (format "%s%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
                  trimmed order-clause offset page-size))))))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-jdbc-conn) name)
  "Escape NAME as a SQL identifier using double quotes (ANSI standard)."
  (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name)))

(cl-defmethod clutch-db-escape-literal ((_conn clutch-jdbc-conn) value)
  "Escape VALUE as a SQL string literal using single quotes (ANSI standard)."
  (format "'%s'" (replace-regexp-in-string "'" "''" value)))

;;;; Schema methods

(defun clutch-jdbc--default-schema (conn)
  "Return a default schema filter for CONN, or nil for no filtering.
Oracle uses the username as schema (uppercased).  Other backends return nil."
  (when (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)
    (when-let* ((user (plist-get (clutch-jdbc-conn-params conn) :user)))
      (upcase user))))

(cl-defmethod clutch-db-list-tables ((conn clutch-jdbc-conn))
  "Return table names for JDBC CONN using DatabaseMetaData.
For Oracle, defaults the schema filter to the connected username to avoid
returning tables from SYS/SYSTEM and other visible schemas."
  (let* ((params  (clutch-jdbc-conn-params conn))
         (schema  (or (plist-get params :schema)
                      (clutch-jdbc--default-schema conn)))
         (result  (clutch-jdbc--rpc
                   "get-tables"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     ,@(when schema `((schema . ,schema)))))))
    (mapcar (lambda (tbl) (plist-get tbl :name))
            (plist-get result :tables))))

(cl-defmethod clutch-db-list-columns ((conn clutch-jdbc-conn) table)
  "Return column names for TABLE on JDBC CONN using DatabaseMetaData."
  (let* ((params  (clutch-jdbc-conn-params conn))
         (schema  (or (plist-get params :schema)
                      (clutch-jdbc--default-schema conn)))
         (result  (clutch-jdbc--rpc
                   "get-columns"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     (table   . ,table)
                     ,@(when schema `((schema . ,schema)))))))
    (mapcar (lambda (col) (plist-get col :name))
            (plist-get result :columns))))

(cl-defmethod clutch-db-show-create-table ((conn clutch-jdbc-conn) table)
  "Return a best-effort DDL for TABLE on JDBC CONN.
Built from DatabaseMetaData column info; not a true SHOW CREATE TABLE."
  (let* ((params (clutch-jdbc-conn-params conn))
         (schema (or (plist-get params :schema) (clutch-jdbc--default-schema conn)))
         (result (clutch-jdbc--rpc
                  "get-columns"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(when schema `((schema . ,schema))))))
         (cols   (plist-get result :columns)))
    (format "-- DDL reconstructed from DatabaseMetaData\nCREATE TABLE %s (\n%s\n);"
            (clutch-db-escape-identifier conn table)
            (mapconcat
             (lambda (col)
               (format "    %s %s%s"
                       (clutch-db-escape-identifier conn (plist-get col :name))
                       (plist-get col :type)
                       (if (plist-get col :nullable) "" " NOT NULL")))
             cols
             ",\n"))))

(cl-defmethod clutch-db-table-comment ((_conn clutch-jdbc-conn) _table)
  "Return nil — table comments are not available via standard DatabaseMetaData."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-jdbc-conn) table)
  "Return primary key columns for TABLE on JDBC CONN."
  (let* ((params (clutch-jdbc-conn-params conn))
         (schema (or (plist-get params :schema) (clutch-jdbc--default-schema conn)))
         (result (clutch-jdbc--rpc
                  "get-primary-keys"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(when schema `((schema . ,schema)))))))
    (plist-get result :primary-keys)))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-jdbc-conn) table)
  "Return foreign key info for TABLE on JDBC CONN."
  (let* ((params (clutch-jdbc-conn-params conn))
         (schema (or (plist-get params :schema) (clutch-jdbc--default-schema conn)))
         (result (clutch-jdbc--rpc
                  "get-foreign-keys"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(when schema `((schema . ,schema)))))))
    (mapcar (lambda (fk)
              (cons (plist-get fk :fk-column)
                    (list :ref-table  (plist-get fk :pk-table)
                          :ref-column (plist-get fk :pk-column))))
            (plist-get result :foreign-keys))))

(cl-defmethod clutch-db-column-details ((conn clutch-jdbc-conn) table)
  "Return detailed column info for TABLE on JDBC CONN."
  (let* ((params  (clutch-jdbc-conn-params conn))
         (schema  (or (plist-get params :schema) (clutch-jdbc--default-schema conn)))
         (pk-cols (clutch-db-primary-key-columns conn table))
         (fks     (clutch-db-foreign-keys conn table))
         (result  (clutch-jdbc--rpc
                   "get-columns"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     (table   . ,table)
                     ,@(when schema `((schema . ,schema))))))
         (cols    (plist-get result :columns)))
    (mapcar (lambda (col)
              (let ((name (plist-get col :name)))
                (list :name        name
                      :type        (plist-get col :type)
                      :nullable    (plist-get col :nullable)
                      :primary-key (and (member name pk-cols) t)
                      :foreign-key (cdr (assoc name fks))
                      :comment     nil)))
            cols)))

;;;; Re-entrancy guard

(cl-defmethod clutch-db-busy-p ((conn clutch-jdbc-conn))
  "Return non-nil if JDBC CONN is executing a query."
  (clutch-jdbc-conn-busy conn))

;;;; Metadata methods

(cl-defmethod clutch-db-user ((conn clutch-jdbc-conn))
  "Return the user for JDBC CONN."
  (plist-get (clutch-jdbc-conn-params conn) :user))

(cl-defmethod clutch-db-host ((conn clutch-jdbc-conn))
  "Return the host for JDBC CONN."
  (plist-get (clutch-jdbc-conn-params conn) :host))

(cl-defmethod clutch-db-port ((conn clutch-jdbc-conn))
  "Return the port for JDBC CONN."
  (plist-get (clutch-jdbc-conn-params conn) :port))

(cl-defmethod clutch-db-database ((conn clutch-jdbc-conn))
  "Return the database for JDBC CONN."
  (plist-get (clutch-jdbc-conn-params conn) :database))

(cl-defmethod clutch-db-display-name ((conn clutch-jdbc-conn))
  "Return a display name based on the JDBC driver type."
  (pcase (plist-get (clutch-jdbc-conn-params conn) :driver)
    ('oracle    "Oracle")
    ('sqlserver "SQL Server")
    ('db2       "DB2")
    ('snowflake "Snowflake")
    (_          "JDBC")))

;;;; Agent installation helpers

(defun clutch-jdbc-ensure-agent ()
  "Download clutch-jdbc-agent.jar if not present.
Fetches from GitHub Releases."
  (interactive)
  (let ((jar (clutch-jdbc--agent-jar)))
    (if (file-exists-p jar)
        (message "clutch-jdbc-agent already at %s" jar)
      (make-directory clutch-jdbc-agent-dir t)
      (make-directory (clutch-jdbc--drivers-dir) t)
      (let ((url (format
                  "https://github.com/LuciusChen/clutch-jdbc-agent/releases/download/v%s/clutch-jdbc-agent-%s.jar"
                  clutch-jdbc-agent-version
                  clutch-jdbc-agent-version)))
        (message "Downloading clutch-jdbc-agent %s..." clutch-jdbc-agent-version)
        (url-copy-file url jar)
        (message "Downloaded to %s" jar)))))

(defun clutch-jdbc-install-driver (driver)
  "Download the JDBC driver for DRIVER symbol from Maven Central."
  (interactive
   (list (intern (completing-read "Driver: "
                                  (mapcar #'car clutch-jdbc--driver-sources)
                                  nil t))))
  (let* ((spec     (alist-get driver clutch-jdbc--driver-sources))
         (filename (plist-get spec :filename))
         (dest     (expand-file-name filename (clutch-jdbc--drivers-dir))))
    (make-directory (clutch-jdbc--drivers-dir) t)
    (if (file-exists-p dest)
        (message "Driver already installed: %s" dest)
      (if (plist-get spec :maven)
          (clutch-jdbc--download-maven-driver (plist-get spec :maven) dest)
        (message "Manual download required for %s.\nURL: %s\nPlace as: %s"
                 driver (plist-get spec :manual) dest)))))

(defun clutch-jdbc--download-maven-driver (coords dest)
  "Download a Maven artifact at COORDS (\"group:artifact:version\") to DEST."
  (pcase-let ((`(,group ,artifact ,version)
               (split-string coords ":")))
    (let* ((group-path (replace-regexp-in-string "\\." "/" group))
           (url (format "https://repo1.maven.org/maven2/%s/%s/%s/%s-%s.jar"
                        group-path artifact version artifact version)))
      (message "Downloading %s from Maven Central..." coords)
      (url-copy-file url dest)
      (message "Downloaded driver to %s" dest))))

(provide 'clutch-db-jdbc)
;;; clutch-db-jdbc.el ends here
