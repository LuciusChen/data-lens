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
(require 'sql)

;; `clutch--connection-key' and `clutch--schema-cache' live in clutch.el (the UI
;; layer).  They are always loaded before any JDBC method is dispatched, so we
;; declare them here to silence the byte-compiler without creating a hard
;; `require' dependency that would invert the dependency graph.
(declare-function clutch--connection-key "clutch" (conn))
(declare-function clutch--schema-status-entry "clutch" (conn))
(defvar clutch--schema-cache)

;;;; Configuration

(defgroup clutch-jdbc nil
  "JDBC backend for clutch."
  :group 'clutch)

(defcustom clutch-jdbc-agent-dir
  (expand-file-name "clutch-jdbc" user-emacs-directory)
  "Directory containing clutch-jdbc-agent.jar and drivers/ subdirectory."
  :type 'directory
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-version "0.1.6"
  "Version of clutch-jdbc-agent to use."
  :type 'string
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-sha256
  nil
  "Expected SHA-256 for the bundled clutch-jdbc-agent jar.
Set this to nil to disable checksum verification for a locally built jar."
  :type '(choice (const :tag "Disable verification" nil) string)
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-java-executable "java"
  "Java executable used to launch clutch-jdbc-agent."
  :type 'string
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-jvm-args '("-Xss512k")
  "Extra JVM arguments passed when starting clutch-jdbc-agent.
Examples:
  (\"-Xss512k\")          — smaller thread stack, faster startup (default)
  (\"-Xss512k\" \"-Xmx256m\") — also cap heap at 256 MB"
  :type '(repeat string)
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-fetch-size 500
  "Number of rows fetched per batch from the agent."
  :type 'natnum
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-oracle-manual-commit t
  "When non-nil, Oracle JDBC connections default to manual-commit mode.
Set this to nil to keep Oracle in auto-commit by default.  Per-connection
`:manual-commit' still overrides this default when explicitly present."
  :type 'boolean
  :group 'clutch-jdbc)

(defvar clutch-connect-timeout-seconds 10
  "Forward declaration; defined as `defcustom' in clutch.el.")

(defvar clutch-read-idle-timeout-seconds 30
  "Forward declaration; defined as `defcustom' in clutch.el.")

(defvar clutch-query-timeout-seconds 30
  "Forward declaration; defined as `defcustom' in clutch.el.")

(defvar clutch-jdbc-rpc-timeout-seconds 30
  "Forward declaration; defined as `defcustom' in clutch.el.")

;;;; Driver sources (for automatic installation from Maven Central)

(defconst clutch-jdbc--driver-sources
  '((sqlserver . (:maven "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11"
                  :filename "mssql-jdbc.jar"))
    (snowflake . (:maven "net.snowflake:snowflake-jdbc:3.14.4"
                  :filename "snowflake-jdbc.jar"))
    ;; ojdbc8 (19c driver) is the safest default across Oracle 11g/12c/19c.
    (oracle    . (:maven "com.oracle.database.jdbc:ojdbc8:19.21.0.0"
                  :filename "ojdbc8.jar"))
    (oracle-8  . (:maven "com.oracle.database.jdbc:ojdbc8:19.21.0.0"
                  :filename "ojdbc8.jar"))
    ;; ojdbc11 remains available for users who explicitly want the newer line.
    (oracle-11 . (:maven "com.oracle.database.jdbc:ojdbc11:21.13.0.0"
                  :filename "ojdbc11.jar"))
    (oracle-i18n . (:maven "com.oracle.database.nls:orai18n:21.13.0.0"
                    :filename "orai18n.jar"))
    (db2       . (:manual "https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
                  :filename "db2jcc4.jar"))
    (redshift  . (:maven "com.amazon.redshift:redshift-jdbc42:2.1.0.30"
                  :filename "redshift-jdbc42.jar")))
  "Known JDBC driver sources.
All entries support auto-download via `clutch-jdbc-install-driver'.")

;;;; Drivers that default to JDBC backend

(defconst clutch-jdbc--jdbc-drivers
  '(oracle sqlserver db2 snowflake redshift)
  "Driver symbols that are automatically routed to the JDBC backend.")

(defconst clutch-jdbc--driver-companions
  '((oracle oracle-i18n)
    (oracle-8 oracle-i18n)
    (oracle-11 oracle-i18n))
  "Optional companion driver artifacts to install alongside a primary driver.")

(defconst clutch-jdbc--oracle-driver-filenames
  '("ojdbc8.jar" "ojdbc11.jar")
  "Oracle JDBC driver jar filenames that conflict with each other.")

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
  "List of parsed synchronous JSON responses, oldest first.")

(defvar clutch-jdbc--async-callbacks (make-hash-table :test 'eql)
  "Map of JDBC request ids to asynchronous callbacks.")

(defconst clutch-jdbc--json-false (make-symbol "clutch-jdbc-json-false")
  "Sentinel used to represent JSON false distinctly from nil.")

(defun clutch-jdbc--agent-jar ()
  "Return the path to the clutch-jdbc-agent jar."
  (expand-file-name
   (format "clutch-jdbc-agent-%s.jar" clutch-jdbc-agent-version)
   clutch-jdbc-agent-dir))

(defun clutch-jdbc--drivers-dir ()
  "Return the drivers/ directory path."
  (expand-file-name "drivers" clutch-jdbc-agent-dir))

(defun clutch-jdbc--agent-jar-sha256 (&optional jar)
  "Return the SHA-256 of JAR.
JAR defaults to `clutch-jdbc--agent-jar'."
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (insert-file-contents-literally (or jar (clutch-jdbc--agent-jar)))
    (secure-hash 'sha256 (current-buffer))))

(defun clutch-jdbc--agent-jar-valid-p (&optional jar)
  "Return non-nil if JAR matches `clutch-jdbc-agent-sha256'.
If verification is disabled, return non-nil."
  (or (null clutch-jdbc-agent-sha256)
      (string-equal (clutch-jdbc--agent-jar-sha256 jar)
                    clutch-jdbc-agent-sha256)))

(defun clutch-jdbc--validate-agent-jar (&optional jar)
  "Signal `user-error' unless JAR exists and passes checksum verification.
JAR defaults to `clutch-jdbc--agent-jar'."
  (let ((jar (or jar (clutch-jdbc--agent-jar))))
    (unless (file-exists-p jar)
      (user-error "clutch-jdbc-agent jar not found: %s\nRun M-x clutch-jdbc-ensure-agent" jar))
    (unless (clutch-jdbc--agent-jar-valid-p jar)
      (user-error (concat "clutch-jdbc-agent checksum mismatch: %s\n"
                          "Run M-x clutch-jdbc-ensure-agent to refresh it,\n"
                          "or set `clutch-jdbc-agent-sha256' to nil for a custom jar")
                  jar))))

(defun clutch-jdbc--cleanup-stale-agent-jars ()
  "Delete stale versioned clutch-jdbc-agent jars from `clutch-jdbc-agent-dir'."
  (let ((current (file-name-nondirectory (clutch-jdbc--agent-jar)))
        (files (directory-files clutch-jdbc-agent-dir t
                                "\\`clutch-jdbc-agent-.*\\.jar\\'")))
    (dolist (file files)
      (unless (string-equal (file-name-nondirectory file) current)
        (delete-file file)))))

(defun clutch-jdbc--agent-live-p ()
  "Return non-nil if the agent process is running."
  (and clutch-jdbc--agent-process
       (process-live-p clutch-jdbc--agent-process)))

(defun clutch-jdbc--clear-async-callbacks ()
  "Cancel and clear all pending asynchronous JDBC callbacks."
  (maphash (lambda (_id entry)
             (when-let* ((timer (plist-get entry :timer)))
               (cancel-timer timer)))
           clutch-jdbc--async-callbacks)
  (clrhash clutch-jdbc--async-callbacks))

(defun clutch-jdbc--dispatch-async-response (response)
  "Dispatch asynchronous RESPONSE when a callback is registered.
Return non-nil when RESPONSE was consumed asynchronously."
  (let* ((id (plist-get response :id))
         (entry (and id (gethash id clutch-jdbc--async-callbacks))))
    (when entry
      (remhash id clutch-jdbc--async-callbacks)
      (when-let* ((timer (plist-get entry :timer)))
        (cancel-timer timer))
      (let ((callback (plist-get entry :callback))
            (errback (plist-get entry :errback)))
        (run-at-time
         0 nil
         (lambda ()
           (condition-case err
               (if (eq t (plist-get response :ok))
                   (when callback
                     (funcall callback (plist-get response :result)))
                 (let ((message (or (plist-get response :error)
                                    "Unknown JDBC agent error")))
                   (if errback
                       (funcall errback message)
                     (message "clutch-jdbc async error: %s" message))))
             (error
              (message "clutch-jdbc async callback failed: %s"
                       (error-message-string err)))))))
      t)))

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
              (let ((parsed (condition-case nil
                                (json-parse-string line :object-type 'plist
                                                   :array-type 'list
                                                   :null-object nil
                                                   :false-object clutch-jdbc--json-false)
                              (error nil))))
                (unless (and parsed
                             (clutch-jdbc--dispatch-async-response parsed))
                  (setq clutch-jdbc--response-queue
                        (nconc clutch-jdbc--response-queue (list parsed))))))))))))

(defun clutch-jdbc--start-agent ()
  "Start the clutch-jdbc-agent process and wait for its ready signal."
  (let ((jar (clutch-jdbc--agent-jar)))
    (clutch-jdbc--validate-agent-jar jar)
    (unless (executable-find clutch-jdbc-agent-java-executable)
      (user-error "Java not found.  Set `clutch-jdbc-agent-java-executable'"))
    (let* ((buf (generate-new-buffer " *clutch-jdbc-agent*"))
           (proc (make-process
                  :name "clutch-jdbc-agent"
                  :buffer buf
                  :command (append (list clutch-jdbc-agent-java-executable)
                                  clutch-jdbc-agent-jvm-args
                                  (list "-jar" jar (clutch-jdbc--drivers-dir)))
                  :connection-type 'pipe
                  :filter #'clutch-jdbc--agent-filter
                  :stderr (get-buffer-create "*clutch-jdbc-agent-stderr*")
                  :noquery t)))
      (setq clutch-jdbc--agent-process proc)
      (setq clutch-jdbc--response-queue nil)
      (clutch-jdbc--clear-async-callbacks)
      ;; Wait for the ready message (id=0).
      (let ((ready (clutch-jdbc--recv-response 0)))
        (unless (plist-get ready :ok)
          (error "clutch-jdbc-agent failed to start: %s" (plist-get ready :error))))
      proc)))

(defun clutch-jdbc--stop-agent ()
  "Stop the shared clutch-jdbc-agent process, if running."
  (when (clutch-jdbc--agent-live-p)
    (delete-process clutch-jdbc--agent-process))
  (setq clutch-jdbc--agent-process nil
        clutch-jdbc--response-queue nil)
  (clutch-jdbc--clear-async-callbacks))

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

(defun clutch-jdbc--recv-response (id &optional timeout-seconds)
  "Wait for and return the response with matching ID as a plist.
TIMEOUT-SECONDS defaults to `clutch-jdbc-rpc-timeout-seconds'."
  (let ((deadline (+ (float-time)
                     (or timeout-seconds clutch-jdbc-rpc-timeout-seconds)))
        response)
    (while (and (not response) (< (float-time) deadline))
      ;; Drain any queued responses while preserving unmatched entries.
      (when clutch-jdbc--response-queue
        (let (remaining)
          (while (and (not response) clutch-jdbc--response-queue)
            (let ((parsed (pop clutch-jdbc--response-queue)))
              (if (and parsed (eql (plist-get parsed :id) id))
                  (setq response parsed)
                (push parsed remaining))))
          (setq clutch-jdbc--response-queue
                (nconc (nreverse remaining) clutch-jdbc--response-queue))))
      (unless response
        (accept-process-output clutch-jdbc--agent-process 0.05)))
    (unless response
      (error "clutch-jdbc-agent: timeout waiting for response to request %d" id))
    response))

(defun clutch-jdbc--rpc (op params &optional timeout-seconds)
  "Send OP with PARAMS to the agent and return the result plist.
Signals `clutch-db-error' on agent-reported errors."
  (clutch-jdbc--ensure-agent)
  (let* ((id (clutch-jdbc--send op params))
         (response (clutch-jdbc--recv-response id timeout-seconds)))
    (if (eq t (plist-get response :ok))
        (plist-get response :result)
      (signal 'clutch-db-error
              (list (or (plist-get response :error)
                        (format "agent error on op %s" op)))))))

(defun clutch-jdbc--rpc-async (op params callback &optional errback timeout-seconds)
  "Send OP with PARAMS to the agent asynchronously.
CALLBACK receives the result plist on success.  ERRBACK receives a
string error message on failure.  TIMEOUT-SECONDS defaults to
`clutch-jdbc-rpc-timeout-seconds'.  Return the request id."
  (clutch-jdbc--ensure-agent)
  (let* ((id (clutch-jdbc--send op params))
         (timeout (or timeout-seconds clutch-jdbc-rpc-timeout-seconds))
         (timer (run-at-time
                 timeout nil
                 (lambda ()
                   (let ((entry (gethash id clutch-jdbc--async-callbacks)))
                     (when entry
                       (remhash id clutch-jdbc--async-callbacks)
                       (when-let* ((timeout-errback (plist-get entry :errback)))
                         (funcall timeout-errback
                                  (format "clutch-jdbc-agent: timeout waiting for async response to request %d"
                                          id)))))))))
    (puthash id (list :callback callback :errback errback :timer timer)
             clutch-jdbc--async-callbacks)
    id))

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

(defun clutch-jdbc--manual-commit-mode (driver params)
  "Return non-nil when DRIVER with PARAMS should use manual-commit mode.
Oracle defaults to manual-commit when `clutch-jdbc-oracle-manual-commit' is
non-nil.  Any driver opts in explicitly via `:manual-commit t' in PARAMS."
  (if (plist-member params :manual-commit)
      (plist-get params :manual-commit)
    (and (eq driver 'oracle)
         clutch-jdbc-oracle-manual-commit)))

(defun clutch-jdbc--apply-timeout-defaults (params)
  "Return a copy of PARAMS with absent timeout keys set to their global defaults."
  (let ((p (copy-sequence params)))
    (setq p (plist-put p :connect-timeout
                       (or (plist-get p :connect-timeout) clutch-connect-timeout-seconds)))
    (setq p (plist-put p :read-idle-timeout
                       (or (plist-get p :read-idle-timeout) clutch-read-idle-timeout-seconds)))
    (setq p (plist-put p :query-timeout
                       (or (plist-get p :query-timeout) clutch-query-timeout-seconds)))
    (setq p (plist-put p :rpc-timeout
                       (or (plist-get p :rpc-timeout) clutch-jdbc-rpc-timeout-seconds)))
    p))

(defun clutch-db-jdbc-connect (driver params)
  "Connect to a JDBC data source of type DRIVER using PARAMS plist.
DRIVER is a symbol (e.g. \\='oracle, \\='sqlserver) captured by the
registration closure — users do not pass it directly.
Returns a `clutch-jdbc-conn'."
  (clutch-jdbc--ensure-agent)
  (let* ((normalized-params (clutch-jdbc--apply-timeout-defaults params))
         (manual-commit-p (clutch-jdbc--manual-commit-mode driver normalized-params))
         (url      (clutch-jdbc--build-url driver normalized-params))
         (user     (plist-get normalized-params :user))
         (password (plist-get normalized-params :password))
         (props    (clutch-jdbc--normalize-props (plist-get normalized-params :props)))
         (connect-timeout (plist-get normalized-params :connect-timeout))
         (read-idle-timeout (plist-get normalized-params :read-idle-timeout))
         (result   (clutch-jdbc--rpc
                    "connect"
                    `((url      . ,url)
                      (user     . ,user)
                      (password . ,password)
                      (auto-commit . ,(if manual-commit-p
                                          clutch-jdbc--json-false
                                        t))
                      (connect-timeout-seconds . ,connect-timeout)
                      ,@(when read-idle-timeout
                          `((network-timeout-seconds . ,read-idle-timeout)))
                      ,@(when props `((props . ,props))))
                    connect-timeout)))
    (make-clutch-jdbc-conn
     :process  clutch-jdbc--agent-process
     :conn-id  (plist-get result :conn-id)
     :params   (plist-put normalized-params :driver driver)
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

(cl-defmethod clutch-db-manual-commit-p ((conn clutch-jdbc-conn))
  "Return non-nil when JDBC CONN runs with auto-commit disabled."
  (let ((params (clutch-jdbc-conn-params conn)))
    (clutch-jdbc--manual-commit-mode (plist-get params :driver) params)))

(cl-defmethod clutch-db-commit ((conn clutch-jdbc-conn))
  "Commit the current transaction on JDBC CONN."
  (let ((rpc-timeout (or (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout)
                         clutch-jdbc-rpc-timeout-seconds)))
    (clutch-jdbc--rpc "commit"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                      rpc-timeout)))

(cl-defmethod clutch-db-rollback ((conn clutch-jdbc-conn))
  "Roll back the current transaction on JDBC CONN."
  (let ((rpc-timeout (or (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout)
                         clutch-jdbc-rpc-timeout-seconds)))
    (clutch-jdbc--rpc "rollback"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                      rpc-timeout)))

(cl-defmethod clutch-db-eager-schema-refresh-p ((conn clutch-jdbc-conn))
  "Oracle JDBC schema enumeration is too slow to block connect."
  (not (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)))

(cl-defmethod clutch-db-completion-sync-columns-p ((conn clutch-jdbc-conn))
  "Oracle/JDBC completion should not synchronously load columns in the hot path."
  (not (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)))

;;;; Query methods

(defun clutch-jdbc--fetch-all (conn cursor-id)
  "Fetch all remaining rows for CURSOR-ID on CONN, returning a flat list."
  (let ((rpc-timeout (or (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout)
                         clutch-jdbc-rpc-timeout-seconds))
        batches done)
    (while (not done)
      (let ((result (clutch-jdbc--rpc "fetch"
                                      `((cursor-id  . ,cursor-id)
                                        (fetch-size . ,clutch-jdbc-fetch-size))
                                      rpc-timeout)))
        (push (plist-get result :rows) batches)
        (setq done (eq t (plist-get result :done)))))
    (apply #'nconc (nreverse batches))))

(defun clutch-jdbc--collect-table-rows (conn result)
  "Drain all rows from a cursor-format get-tables RESULT on CONN.
Returns the flat list of rows; each row is a list of column values.
If RESULT already has :done t, returns its :rows directly.
Otherwise fetches remaining cursor pages via `clutch-jdbc--fetch-all'."
  (let* ((first-rows (plist-get result :rows))
         (cursor-id  (plist-get result :cursor-id)))
    (if (eq t (plist-get result :done))
        first-rows
      (nconc first-rows (clutch-jdbc--fetch-all conn cursor-id)))))

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

(defun clutch-jdbc--normalize-props (props)
  "Return PROPS as an alist with string keys, suitable for JSON encoding.
Accepts either an alist ((\"key\" . val) ...) or a plist (:key val ...)."
  (when props
    (if (consp (car props))
        props
      (cl-loop for (k v) on props by #'cddr
               collect (cons (substring (symbol-name k) 1) v)))))

(defun clutch-jdbc--normalize-row (row)
  "Convert JDBC-specific value representations in ROW to generic forms.
Blob plists with :text content become plain strings.
Clob plists become their :preview string."
  (mapcar (lambda (val)
            (cond
             ((and (listp val)
                   (equal (plist-get val :__type) "blob")
                   (plist-get val :text))
              (plist-get val :text))
             ((and (listp val)
                   (equal (plist-get val :__type) "clob"))
              (plist-get val :preview))
             (t val)))
          row))

(cl-defmethod clutch-db-query ((conn clutch-jdbc-conn) sql)
  "Execute SQL on JDBC CONN and return a `clutch-db-result'."
  (setf (clutch-jdbc-conn-busy conn) t)
  (unwind-protect
      (condition-case err
          (let* ((query-timeout (plist-get (clutch-jdbc-conn-params conn) :query-timeout))
                 (rpc-timeout (or (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout)
                                  clutch-jdbc-rpc-timeout-seconds))
                 (result (clutch-jdbc--rpc
                          "execute"
                          `((conn-id    . ,(clutch-jdbc-conn-conn-id conn))
                            (sql        . ,sql)
                            (fetch-size . ,clutch-jdbc-fetch-size)
                            ,@(when query-timeout
                                `((query-timeout-seconds . ,query-timeout))))
                          rpc-timeout))
                 (type   (plist-get result :type)))
            (if (equal type "dml")
                ;; DML: no rows, just affected-rows.
                (make-clutch-db-result
                 :connection    conn
                 :affected-rows (plist-get result :affected-rows))
              ;; SELECT: consume remaining pages, return full result.
              (let* ((first-rows  (plist-get result :rows))
                     (cursor-id   (plist-get result :cursor-id))
                     (done        (eq t (plist-get result :done)))
                     (all-rows    (if done first-rows
                                    (nconc first-rows
                                           (clutch-jdbc--fetch-all conn cursor-id))))
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
           (sortable-sql (if order-by
                             (clutch-db-sql-strip-top-level-order-by trimmed)
                           trimmed))
           (offset  (* page-num page-size))
           (oracle-p (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)))
      (if oracle-p
          (clutch-jdbc--build-oracle-paged-sql conn sortable-sql offset page-size order-by)
        (let ((order-clause (if order-by
                                (format " ORDER BY %s %s"
                                        (clutch-db-escape-identifier conn (car order-by))
                                        (cdr order-by))
                              " ORDER BY (SELECT NULL)")))
          (format "%s%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
                  sortable-sql order-clause offset page-size))))))

;;;; SQL dialect methods

(cl-defmethod clutch-db-escape-identifier ((_conn clutch-jdbc-conn) name)
  "Escape NAME as a SQL identifier using double quotes (ANSI standard)."
  (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name)))

(defun clutch-jdbc--oracle-display-identifier (name)
  "Return Oracle DDL display form for identifier NAME.
Leave simple uppercase identifiers unquoted so reconstructed DDL reads
closer to Oracle's native style."
  (let ((case-fold-search nil))
    (if (and (string-match-p "\\`[A-Z][A-Z0-9_$#]*\\'" name)
             (not (clutch-jdbc--oracle-display-keyword-p name)))
        name
      (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name)))))

(defvar clutch-jdbc--oracle-display-keyword-cache (make-hash-table :test 'equal)
  "Cache of Oracle identifiers that should remain quoted in display DDL.")

(defun clutch-jdbc--oracle-display-keyword-p (name)
  "Return non-nil when bare NAME would be fontified as Oracle syntax.
Such identifiers should remain quoted in reconstructed Oracle DDL."
  (let ((cached (gethash name clutch-jdbc--oracle-display-keyword-cache 'missing)))
    (if (eq cached 'missing)
        (let ((keywordp
               (with-temp-buffer
                 (sql-mode)
                 (sql-set-product 'oracle)
                 (insert name)
                 (font-lock-ensure)
                 (let ((face (get-text-property (point-min) 'face)))
                   (or (eq face 'font-lock-keyword-face)
                       (eq face 'font-lock-builtin-face)
                       (and (listp face)
                            (or (memq 'font-lock-keyword-face face)
                                (memq 'font-lock-builtin-face face))))))))
          (puthash name keywordp clutch-jdbc--oracle-display-keyword-cache)
          keywordp)
      cached)))

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

(defun clutch-jdbc--conn-schema (conn)
  "Return the effective schema for CONN: explicit :schema or the Oracle default."
  (or (plist-get (clutch-jdbc-conn-params conn) :schema)
      (clutch-jdbc--default-schema conn)))

(cl-defmethod clutch-db-list-tables ((conn clutch-jdbc-conn))
  "Return table names for JDBC CONN using DatabaseMetaData.
For Oracle, defaults the schema filter to the connected username to avoid
returning tables from SYS/SYSTEM and other visible schemas."
  (let* ((schema  (clutch-jdbc--conn-schema conn))
         (result  (clutch-jdbc--rpc
                   "get-tables"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     ,@(when schema `((schema . ,schema))))))
         (all-rows (clutch-jdbc--collect-table-rows conn result)))
    (mapcar #'car all-rows)))

(cl-defmethod clutch-db-refresh-schema-async ((conn clutch-jdbc-conn) callback
                                              &optional errback)
  "Refresh JDBC table names for CONN asynchronously."
  (let* ((params      (clutch-jdbc-conn-params conn))
         (schema      (clutch-jdbc--conn-schema conn))
         (rpc-timeout (or (plist-get params :rpc-timeout)
                          clutch-jdbc-rpc-timeout-seconds)))
    (clutch-jdbc--rpc-async
     "get-tables"
     `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
       ,@(when schema `((schema . ,schema))))
     (lambda (result)
       (when callback
         (funcall callback
                  (mapcar #'car (clutch-jdbc--collect-table-rows conn result)))))
     errback
     rpc-timeout)
    t))

(cl-defmethod clutch-db-column-details-async ((conn clutch-jdbc-conn) table callback
                                              &optional errback)
  "Fetch JDBC column details for TABLE on CONN asynchronously."
  (let* ((params (clutch-jdbc-conn-params conn))
         (schema (clutch-jdbc--conn-schema conn))
         (rpc-timeout (or (plist-get params :rpc-timeout)
                          clutch-jdbc-rpc-timeout-seconds)))
    (clutch-jdbc--rpc-async
     "get-columns"
     `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
       (table . ,table)
       ,@(when schema `((schema . ,schema))))
     (lambda (result)
       (when callback
         (funcall callback (plist-get result :columns))))
     errback
     rpc-timeout)
    t))

(cl-defmethod clutch-db-list-columns ((conn clutch-jdbc-conn) table)
  "Return column names for TABLE on JDBC CONN using DatabaseMetaData."
  (let* ((schema  (clutch-jdbc--conn-schema conn))
         (result  (clutch-jdbc--rpc
                   "get-columns"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     (table   . ,table)
                     ,@(when schema `((schema . ,schema)))))))
    (mapcar (lambda (col) (plist-get col :name))
            (plist-get result :columns))))

(cl-defmethod clutch-db-complete-tables ((conn clutch-jdbc-conn) prefix)
  "Return table name candidates matching PREFIX for JDBC CONN.
For Oracle: uses the schema cache when available (no RPC); falls back to a
`search-tables' RPC when the cache is absent or marked stale."
  (when (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)
    (if-let* ((schema-ready
               (and (fboundp 'clutch--schema-status-entry)
                    (eq (plist-get (clutch--schema-status-entry conn) :state) 'ready)))
              (schema (and schema-ready
                           (gethash (clutch--connection-key conn) clutch--schema-cache))))
        (seq-filter (lambda (name)
                      (string-prefix-p (upcase prefix) (upcase name)))
                    (hash-table-keys schema))
      (let* ((s      (clutch-jdbc--conn-schema conn))
             (result (clutch-jdbc--rpc
                      "search-tables"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                        (prefix  . ,prefix)
                        ,@(when s `((schema . ,s)))))))
        (mapcar (lambda (tbl) (plist-get tbl :name))
                (plist-get result :tables))))))

(cl-defmethod clutch-db-complete-columns ((conn clutch-jdbc-conn) table prefix)
  "Return column name candidates for TABLE matching PREFIX on JDBC CONN."
  (let* ((params (clutch-jdbc-conn-params conn))
         (driver (plist-get params :driver)))
    (when (eq driver 'oracle)
      (let* ((schema (clutch-jdbc--conn-schema conn))
             (result (clutch-jdbc--rpc
                      "search-columns"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                        (table   . ,table)
                        (prefix  . ,prefix)
                        ,@(when schema `((schema . ,schema)))))))
        (mapcar (lambda (col) (plist-get col :name))
                (plist-get result :columns))))))

(cl-defmethod clutch-db-show-create-table ((conn clutch-jdbc-conn) table)
  "Return a best-effort DDL for TABLE on JDBC CONN.
Built from DatabaseMetaData column info; not a true SHOW CREATE TABLE."
  (let* ((params (clutch-jdbc-conn-params conn))
         (driver (plist-get params :driver))
         (schema (clutch-jdbc--conn-schema conn))
         (result (clutch-jdbc--rpc
                  "get-columns"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(when schema `((schema . ,schema))))))
         (cols   (plist-get result :columns))
         (display-ident
          (if (eq driver 'oracle)
              #'clutch-jdbc--oracle-display-identifier
            (lambda (name) (clutch-db-escape-identifier conn name)))))
    (format "-- DDL reconstructed from DatabaseMetaData\nCREATE TABLE %s (\n%s\n);"
            (funcall display-ident table)
            (mapconcat
             (lambda (col)
               (format "    %s %s%s"
                       (funcall display-ident (plist-get col :name))
                       (plist-get col :type)
                       (if (plist-get col :nullable) "" " NOT NULL")))
             cols
             ",\n"))))

(cl-defmethod clutch-db-table-comment ((_conn clutch-jdbc-conn) _table)
  "Return nil — table comments are not available via standard DatabaseMetaData."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-jdbc-conn) table)
  "Return primary key columns for TABLE on JDBC CONN."
  (let* ((schema (clutch-jdbc--conn-schema conn))
         (result (clutch-jdbc--rpc
                  "get-primary-keys"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(when schema `((schema . ,schema)))))))
    (plist-get result :primary-keys)))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-jdbc-conn) table)
  "Return foreign key info for TABLE on JDBC CONN."
  (let* ((schema (clutch-jdbc--conn-schema conn))
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
  (let* ((schema  (clutch-jdbc--conn-schema conn))
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
    ('redshift  "Redshift")
    (_          "JDBC")))

;;;; Agent installation helpers

(defun clutch-jdbc-ensure-agent ()
  "Download clutch-jdbc-agent.jar if not present.
Fetches from GitHub Releases."
  (interactive)
  (let ((jar (clutch-jdbc--agent-jar)))
    (make-directory clutch-jdbc-agent-dir t)
    (make-directory (clutch-jdbc--drivers-dir) t)
    (if (and (file-exists-p jar)
             (clutch-jdbc--agent-jar-valid-p jar))
        (progn
          (clutch-jdbc--cleanup-stale-agent-jars)
          (message "clutch-jdbc-agent already at %s" jar))
      (let ((url (format
                  "https://github.com/LuciusChen/clutch-jdbc-agent/releases/download/v%s/clutch-jdbc-agent-%s.jar"
                  clutch-jdbc-agent-version
                  clutch-jdbc-agent-version)))
        (message "Downloading clutch-jdbc-agent %s..." clutch-jdbc-agent-version)
        (url-copy-file url jar t)
        (unless (clutch-jdbc--agent-jar-valid-p jar)
          (error "Downloaded clutch-jdbc-agent checksum mismatch: %s" jar))
        (clutch-jdbc--cleanup-stale-agent-jars)
        (message "Downloaded to %s" jar)))))

(defun clutch-jdbc-install-driver (driver)
  "Download the JDBC driver for DRIVER symbol from Maven Central."
  (interactive
   (list (intern (completing-read "Driver: "
                                  (mapcar #'car clutch-jdbc--driver-sources)
                                  nil t))))
  (let* ((spec       (alist-get driver clutch-jdbc--driver-sources))
         (filename   (plist-get spec :filename))
         (dest       (expand-file-name filename (clutch-jdbc--drivers-dir)))
         (companions (alist-get driver clutch-jdbc--driver-companions)))
    (make-directory (clutch-jdbc--drivers-dir) t)
    (cond
     ((file-exists-p dest)
      (message "Driver already installed: %s" dest))
     ((plist-get spec :maven)
      (clutch-jdbc--download-maven-driver (plist-get spec :maven) dest))
     (t
      (message "Manual download required for %s.\nURL: %s\nPlace as: %s"
               driver (plist-get spec :manual) dest)))
    (when (clutch-jdbc--oracle-driver-symbol-p driver)
      (clutch-jdbc--disable-conflicting-oracle-jars filename))
    (dolist (companion companions)
      (unless (file-exists-p
               (expand-file-name
                (plist-get (alist-get companion clutch-jdbc--driver-sources) :filename)
                (clutch-jdbc--drivers-dir)))
        (clutch-jdbc-install-driver companion)))
    (when (clutch-jdbc--agent-live-p)
      (clutch-jdbc--stop-agent)
      (message "Installed JDBC driver(s); shared clutch-jdbc-agent restarted on next use"))))

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

(defun clutch-jdbc--oracle-driver-symbol-p (driver)
  "Return non-nil when DRIVER selects an Oracle JDBC jar."
  (memq driver '(oracle oracle-8 oracle-11)))

(defun clutch-jdbc--disable-conflicting-oracle-jars (selected-filename)
  "Remove Oracle JDBC jars that conflict with SELECTED-FILENAME."
  (dolist (filename clutch-jdbc--oracle-driver-filenames)
    (unless (string-equal filename selected-filename)
      (let ((path (expand-file-name filename (clutch-jdbc--drivers-dir))))
        (when (file-exists-p path)
          (delete-file path))))))

(provide 'clutch-db-jdbc)
;;; clutch-db-jdbc.el ends here
