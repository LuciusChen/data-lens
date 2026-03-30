;;; clutch-db-jdbc.el --- JDBC backend for clutch-db via clutch-jdbc-agent -*- lexical-binding: t; -*-

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

;; `clutch--schema-cache' lives in clutch.el, and `clutch--connection-key' now
;; lives in clutch-connection.el.  They are always loaded before any JDBC
;; method is dispatched, so we declare them here to silence the byte-compiler
;; without creating a hard `require' dependency that would invert the
;; dependency graph.
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--schema-status-entry "clutch-schema" (conn))
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

(defcustom clutch-jdbc-agent-version "0.2.3"
  "Version of clutch-jdbc-agent to use."
  :type 'string
  :group 'clutch-jdbc)

(defcustom clutch-jdbc-agent-sha256
  "2ff7efcd77240c76c4dd8a5806b3880fb45c9f08695f5a3c932899806f814591"
  "Expected SHA-256 for the configured clutch-jdbc-agent jar.
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

(defvar clutch-jdbc-cancel-timeout-seconds 5
  "Seconds to wait for a cancel acknowledgement from the JDBC agent.
Shorter than `clutch-jdbc-rpc-timeout-seconds' because a slow cancel
should degrade to disconnect, not block the user.")

(defvar clutch-jdbc-disconnect-timeout-seconds 5
  "Seconds to wait for a disconnect acknowledgement from the JDBC agent.
A stuck disconnect should not block the user or kill the agent.")

(defvar clutch-debug-mode nil
  "Forward declaration; defined as a global minor mode in clutch.el.")

(defvar clutch-debug-buffer-name "*clutch-debug*"
  "Forward declaration; defined in clutch.el.")

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
                  :filename "redshift-jdbc42.jar"))
    (clickhouse . (:maven "com.clickhouse:clickhouse-jdbc:0.9.8:all"
                   :filename "clickhouse-jdbc.jar"))
    (slf4j-api  . (:maven "org.slf4j:slf4j-api:2.0.16"
                   :filename "slf4j-api.jar"))
    (slf4j-nop  . (:maven "org.slf4j:slf4j-nop:2.0.16"
                   :filename "slf4j-nop.jar")))
  "Known JDBC driver sources.
All entries support auto-download via `clutch-jdbc-install-driver'.")

;;;; Drivers that default to JDBC backend

(defconst clutch-jdbc--jdbc-drivers
  '(jdbc oracle sqlserver db2 snowflake redshift clickhouse)
  "Backend/driver symbols that are routed to the JDBC backend.")

(defconst clutch-jdbc--driver-companions
  '((oracle oracle-i18n)
    (oracle-8 oracle-i18n)
    (oracle-11 oracle-i18n)
    (clickhouse slf4j-api slf4j-nop))
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

(defvar clutch-jdbc--busy-request-ids (make-hash-table :test 'eq)
  "Map of JDBC connection objects to their current in-flight request id.")

(defvar clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql)
  "Set of JDBC response ids to drop because the request was interrupted.")

(defvar clutch-jdbc--connections-by-id (make-hash-table :test 'eql)
  "Map of JDBC connection ids to their live connection structs.")

(defvar clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)
  "Map of JDBC connection objects to their latest structured error details.")

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
      (user-error "JDBC agent jar not found: %s\nRun M-x clutch-jdbc-ensure-agent" jar))
    (unless (clutch-jdbc--agent-jar-valid-p jar)
      (user-error (concat "JDBC agent checksum mismatch: %s\n"
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

(defun clutch-jdbc--clear-request-state ()
  "Clear in-flight and ignored JDBC request bookkeeping."
  (clrhash clutch-jdbc--busy-request-ids)
  (clrhash clutch-jdbc--ignored-response-ids))

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
            (errback (plist-get entry :errback))
            (conn (plist-get entry :conn))
            (op (plist-get entry :op)))
        (run-at-time
         0 nil
         (lambda ()
           (condition-case err
              (if (eq t (plist-get response :ok))
                  (when callback
                    (funcall callback (plist-get response :result)))
                (clutch-jdbc--remember-error-response conn op response)
                (let ((message (clutch-jdbc--rpc-error-message op response)))
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
                (when parsed
                  (let ((id (plist-get parsed :id)))
                    (cond
                     ((clutch-jdbc--dispatch-async-response parsed)
                      nil)
                     ((and id (gethash id clutch-jdbc--ignored-response-ids))
                      (remhash id clutch-jdbc--ignored-response-ids))
                     (t
                      (setq clutch-jdbc--response-queue
                            (nconc clutch-jdbc--response-queue
                                   (list parsed)))))))))))))))

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
      (clutch-jdbc--clear-request-state)
      ;; Wait for the ready message (id=0).
      (let ((ready (clutch-jdbc--recv-response 0)))
        (unless (plist-get ready :ok)
          (error "JDBC agent failed to start: %s" (plist-get ready :error))))
      proc)))

(defun clutch-jdbc--stop-agent ()
  "Stop the shared clutch-jdbc-agent process, if running."
  (when (clutch-jdbc--agent-live-p)
    (delete-process clutch-jdbc--agent-process))
  (setq clutch-jdbc--agent-process nil
        clutch-jdbc--response-queue nil)
  (clutch-jdbc--clear-async-callbacks)
  (clutch-jdbc--clear-request-state))

(defun clutch-jdbc--ensure-agent ()
  "Ensure the agent process is running, starting it if necessary."
  (unless (clutch-jdbc--agent-live-p)
    (setq clutch-jdbc--response-queue nil)
    (clutch-jdbc--start-agent)))

(defun clutch-jdbc--agent-stderr-buffer ()
  "Return the JDBC agent stderr buffer, or nil when absent."
  (get-buffer "*clutch-jdbc-agent-stderr*"))

(defun clutch-jdbc--agent-stderr-string ()
  "Return the full JDBC agent stderr buffer as a string, or nil when empty."
  (when-let* ((buf (clutch-jdbc--agent-stderr-buffer))
              ((buffer-live-p buf)))
    (with-current-buffer buf
      (let ((text (string-trim (buffer-string))))
        (unless (string-empty-p text)
          text)))))

(defun clutch-jdbc--agent-stderr-tail (&optional max-lines)
  "Return the last non-empty MAX-LINES of agent stderr as a string.
Defaults to 8 lines.  Return nil when stderr is empty."
  (when-let* ((text (clutch-jdbc--agent-stderr-string)))
    (let* ((lines (seq-filter
                   (lambda (line) (not (string-empty-p (string-trim line))))
                   (split-string text "\n" t)))
           (count (or max-lines 8)))
      (when lines
        (string-join
         (last lines (min count (length lines)))
         "\n")))))

(defun clutch-jdbc--agent-exit-error-message ()
  "Return a user-facing error string when the JDBC agent exited early."
  (let ((stderr (clutch-jdbc--agent-stderr-string))
        (stderr-tail (clutch-jdbc--agent-stderr-tail)))
    (cond
     ((and stderr
           (string-match-p "UnsupportedClassVersionError" stderr))
      (format (concat "clutch-jdbc-agent requires a newer Java runtime than `%s'. "
                      "Update `clutch-jdbc-agent-java-executable' or JAVA_HOME.\n%s")
              clutch-jdbc-agent-java-executable
              stderr-tail))
     (stderr-tail
      (format "clutch-jdbc-agent exited before replying:\n%s" stderr-tail))
     (t
      "clutch-jdbc-agent exited before replying"))))

;;;; Synchronous RPC

(defvar clutch-jdbc--next-request-id 1
  "Auto-incrementing request id counter.")

(defun clutch-jdbc--request-params (params)
  "Return PARAMS with opt-in debug capture flags when enabled."
  (if (or (not clutch-debug-mode)
          (assq 'debug params))
      params
    (append params '((debug . t)))))

(defun clutch-jdbc--send (op params)
  "Send OP with PARAMS to the agent and return the request id."
  (let* ((id (cl-incf clutch-jdbc--next-request-id))
         (msg (json-encode `((id . ,id)
                             (op . ,op)
                             (params . ,(clutch-jdbc--request-params params))))))
    (process-send-string clutch-jdbc--agent-process (concat msg "\n"))
    id))

(defun clutch-jdbc--recv-response (id &optional timeout-seconds op)
  "Wait for and return the response with matching ID as a plist.
TIMEOUT-SECONDS defaults to `clutch-jdbc-rpc-timeout-seconds'.
OP, when non-nil, names the RPC for context-sensitive timeout errors."
  (let ((deadline (+ (float-time)
                     (or timeout-seconds clutch-jdbc-rpc-timeout-seconds)))
        response
        failure-message)
    (while (and (not response) (< (float-time) deadline))
      ;; Drain any queued responses while preserving unmatched entries.
      (when clutch-jdbc--response-queue
        (let (remaining)
          (while (and (not response) clutch-jdbc--response-queue)
            (let ((parsed (pop clutch-jdbc--response-queue)))
              (cond
               ((and parsed
                     (gethash (plist-get parsed :id) clutch-jdbc--ignored-response-ids))
                (remhash (plist-get parsed :id) clutch-jdbc--ignored-response-ids))
               ((and parsed (eql (plist-get parsed :id) id))
                (setq response parsed))
               (t
                (push parsed remaining)))))
          (setq clutch-jdbc--response-queue
                (nconc (nreverse remaining) clutch-jdbc--response-queue))))
      (unless response
        (if (and clutch-jdbc--agent-process
                 (not (process-live-p clutch-jdbc--agent-process)))
            (setq failure-message (clutch-jdbc--agent-exit-error-message)
                  response :agent-exited)
          (accept-process-output clutch-jdbc--agent-process 0.05)
          (sit-for 0 t))))
    (when (eq response :agent-exited)
      (setq response nil))
    (when (and (not response)
               (not failure-message)
               clutch-jdbc--agent-process
               (not (process-live-p clutch-jdbc--agent-process)))
      (setq failure-message (clutch-jdbc--agent-exit-error-message)))
    (unless response
      ;; The agent is likely blocked on a dead JDBC call.  Kill the process so
      ;; it does not remain wedged — subsequent requests would otherwise pile up
      ;; behind the stuck op and all fail with "Closed Connection".
      (when (process-live-p clutch-jdbc--agent-process)
        (delete-process clutch-jdbc--agent-process))
      (clutch-jdbc--clear-async-callbacks)
      (clutch-jdbc--clear-request-state)
      (setq clutch-jdbc--agent-process nil
            clutch-jdbc--response-queue nil)
      (signal 'clutch-db-error
              (list (or failure-message
                        (if (equal op "connect")
                            "Connection attempt timed out or JDBC agent became unresponsive"
                          "Connection lost — reconnect with C-c C-e")))))
    response))

(defun clutch-jdbc--recv-response-nonfatal (id timeout-seconds)
  "Wait for response ID up to TIMEOUT-SECONDS.
Return the response plist, or nil on timeout/quit.
Unlike `clutch-jdbc--recv-response', this never kills the agent process."
  (let ((inhibit-quit t))
    (let ((deadline (+ (float-time) timeout-seconds))
          response
          agent-exited
          gave-up)
      (while (and (not response) (not agent-exited) (not gave-up)
                  (< (float-time) deadline))
        ;; Drain any queued responses while preserving unmatched entries.
        (when clutch-jdbc--response-queue
          (let (remaining)
            (while (and (not response) clutch-jdbc--response-queue)
              (let ((parsed (pop clutch-jdbc--response-queue)))
                (cond
                 ((and parsed
                       (gethash (plist-get parsed :id) clutch-jdbc--ignored-response-ids))
                  (remhash (plist-get parsed :id) clutch-jdbc--ignored-response-ids))
                 ((and parsed (eql (plist-get parsed :id) id))
                  (setq response parsed))
                 (t
                  (push parsed remaining)))))
            (setq clutch-jdbc--response-queue
                  (nconc (nreverse remaining) clutch-jdbc--response-queue))))
        (unless response
          (if (or (null clutch-jdbc--agent-process)
                  (not (process-live-p clutch-jdbc--agent-process)))
              (setq agent-exited t)
            (let ((output
                   (with-local-quit
                     (accept-process-output clutch-jdbc--agent-process 0.05))))
              (sit-for 0 t)
              (when (and (not output) quit-flag)
                (setq gave-up t
                      quit-flag nil))))))
      (when (and (not response)
                 (not agent-exited)
                 clutch-jdbc--agent-process
                 (not (process-live-p clutch-jdbc--agent-process)))
        (setq agent-exited t))
      (unless agent-exited
        response))))
(defun clutch-jdbc--rpc (op params &optional timeout-seconds)
  "Send OP with PARAMS to the agent and return the result plist.
TIMEOUT-SECONDS overrides the default wait time.  Signals
`clutch-db-error' on agent-reported errors."
  (clutch-jdbc--ensure-agent)
  (let* ((conn (clutch-jdbc--conn-from-params params))
         (id (clutch-jdbc--send op params))
         (response (clutch-jdbc--recv-response id timeout-seconds op)))
    (if (eq t (plist-get response :ok))
        (plist-get response :result)
      (let ((details (clutch-jdbc--remember-error-response conn op response)))
      (signal 'clutch-db-error
              (if details
                  (list (clutch-jdbc--rpc-error-message op response) details)
                (list (clutch-jdbc--rpc-error-message op response))))))))

(defun clutch-jdbc--rpc-error-message (op response)
  "Return a user-facing error string for OP from RESPONSE."
  (let ((message (or (plist-get response :error)
                     (format "agent error on op %s" op))))
    (if (and (plist-get response :diag)
             (not (string-match-p (regexp-quote clutch-debug-buffer-name) message)))
        (if clutch-debug-mode
            (format "%s See %s for details." message clutch-debug-buffer-name)
          (format
           "%s Enable clutch-debug-mode, reproduce the failure, then inspect %s."
           message clutch-debug-buffer-name))
      message)))

(defun clutch-jdbc--error-details-from-response (op response)
  "Build a structured error-details plist for OP from RESPONSE."
  (when-let* ((diag (plist-get response :diag)))
    (list :backend 'jdbc
          :summary (or (plist-get response :error)
                       (and op (format "agent error on op %s" op)))
          :diag (copy-tree diag)
          :debug (copy-tree (plist-get response :debug))
          :stderr-tail (clutch-jdbc--agent-stderr-tail))))

(defun clutch-jdbc--remember-error-response (conn op response)
  "Remember JDBC error RESPONSE for CONN and OP, and return its details plist.
When CONN is nil, just return the details snapshot for the current failure."
  (when-let* ((details (clutch-jdbc--error-details-from-response op response)))
    (when conn
      (puthash conn details clutch-jdbc--error-details-by-conn))
    details))

(defun clutch-jdbc--conn-from-params (params)
  "Return the JDBC connection object referenced by PARAMS, or nil."
  (when-let* ((conn-id (alist-get 'conn-id params)))
    (gethash conn-id clutch-jdbc--connections-by-id)))

(defun clutch-jdbc--rpc-on-conn (conn op params &optional timeout-seconds)
  "Send OP with PARAMS while tracking the in-flight request for CONN."
  (clutch-jdbc--ensure-agent)
  (let* ((id (clutch-jdbc--send op params))
         (clear-request-id t)
         response)
    (puthash conn id clutch-jdbc--busy-request-ids)
    (unwind-protect
        (condition-case err
            (progn
              (setq response (clutch-jdbc--recv-response id timeout-seconds op))
              (if (eq t (plist-get response :ok))
                  (plist-get response :result)
                (let ((details (clutch-jdbc--remember-error-response conn op response)))
                  (signal 'clutch-db-error
                          (if details
                              (list (clutch-jdbc--rpc-error-message op response) details)
                            (list (clutch-jdbc--rpc-error-message op response)))))))
          (quit
           (setq clear-request-id nil)
           (signal 'quit nil))
          (clutch-db-error
           (signal (car err) (cdr err))))
      (when (and clear-request-id
                 (eql (gethash conn clutch-jdbc--busy-request-ids) id))
        (remhash conn clutch-jdbc--busy-request-ids)))))

(defun clutch-jdbc--rpc-async (op params callback &optional errback timeout-seconds conn)
  "Send OP with PARAMS to the agent asynchronously.
CALLBACK receives the result plist on success.  ERRBACK receives a
string error message on failure.  TIMEOUT-SECONDS defaults to
`clutch-jdbc-rpc-timeout-seconds'.  CONN tracks connection-scoped
diagnostics when non-nil.  Return the request id."
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
    (puthash id (list :callback callback :errback errback :timer timer
                      :conn conn :op op)
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
          ('clickhouse
           (format "jdbc:clickhouse://%s:%d/%s"
                   host (or port 8123) database))
          (_
           (error "Unknown JDBC driver %s; provide :url directly" driver))))))

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

(defun clutch-jdbc--setup-prerequisites (driver)
  "Ensure agent jar and DRIVER jar are present, offering to download if missing."
  (let ((jar (clutch-jdbc--agent-jar)))
    (unless (and (file-exists-p jar) (clutch-jdbc--agent-jar-valid-p jar))
      (if (y-or-n-p "JDBC agent not found.  Download from GitHub? ")
          (clutch-jdbc--download-agent-jar)
        (user-error "Run M-x clutch-jdbc-ensure-agent to install the JDBC agent"))))
  (when-let* ((spec (alist-get driver clutch-jdbc--driver-sources))
              (filename (plist-get spec :filename))
              (dest (expand-file-name filename (clutch-jdbc--drivers-dir))))
    (unless (file-exists-p dest)
      (cond
       ((plist-get spec :maven)
        (if (y-or-n-p (format "%s driver not found.  Download from Maven Central? "
                              (capitalize (symbol-name driver))))
            (clutch-jdbc-install-driver driver)
          (user-error "Run M-x clutch-jdbc-install-driver RET %s" driver)))
       ((plist-get spec :manual)
        (user-error "%s driver requires manual download.\nURL: %s\nPlace as: %s"
                    (capitalize (symbol-name driver))
                    (plist-get spec :manual) dest))))))

(defun clutch-db-jdbc-connect (driver params)
  "Connect to a JDBC data source of type DRIVER using PARAMS plist.
DRIVER is a symbol (e.g. \\='oracle, \\='sqlserver) captured by the
registration closure — users do not pass it directly.
Returns a `clutch-jdbc-conn'."
  (clutch-jdbc--setup-prerequisites driver)
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
    (let ((conn (make-clutch-jdbc-conn
                 :process  clutch-jdbc--agent-process
                 :conn-id  (plist-get result :conn-id)
                 :params   (plist-put normalized-params :driver driver)
                 :busy     nil)))
      (puthash (clutch-jdbc-conn-conn-id conn) conn clutch-jdbc--connections-by-id)
      conn)))

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
  (remhash conn clutch-jdbc--busy-request-ids)
  (remhash conn clutch-jdbc--error-details-by-conn)
  (remhash (clutch-jdbc-conn-conn-id conn) clutch-jdbc--connections-by-id)
  (when (clutch-jdbc--agent-live-p)
    (condition-case nil
        (let ((id (clutch-jdbc--send
                   "disconnect"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))))))
          (clutch-jdbc--recv-response-nonfatal
           id clutch-jdbc-disconnect-timeout-seconds))
      (error nil))))

(cl-defmethod clutch-db-live-p ((conn clutch-jdbc-conn))
  "Return non-nil if the agent process is running and CONN belongs to it.
Checks both that the stored process is live AND that it is still the
current agent process.  After a timeout kill, `clutch-jdbc--agent-process'
is set to nil before the old JVM fully exits, so the old process may still
pass `process-live-p' briefly; the identity check closes that window."
  (and (clutch-jdbc-conn-p conn)
       clutch-jdbc--agent-process
       (eq (clutch-jdbc-conn-process conn) clutch-jdbc--agent-process)
       (process-live-p (clutch-jdbc-conn-process conn))))

(cl-defmethod clutch-db-error-details ((conn clutch-jdbc-conn))
  "Return the latest structured error details snapshot for JDBC CONN."
  (when-let* ((details (gethash conn clutch-jdbc--error-details-by-conn)))
    (copy-tree details)))

(cl-defmethod clutch-db-clear-error-details ((conn clutch-jdbc-conn))
  "Forget the latest structured error details snapshot for JDBC CONN."
  (remhash conn clutch-jdbc--error-details-by-conn))

(cl-defmethod clutch-db-init-connection ((_conn clutch-jdbc-conn))
  "No post-connect initialization needed for JDBC connections.")

(defun clutch-jdbc--conn-rpc-timeout (conn)
  "Return the RPC timeout in seconds for CONN.
Always non-nil: `clutch-jdbc--apply-timeout-defaults' ensures the value is
stored in params at connect time."
  (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout))

(defun clutch-jdbc--conn-effective-query-timeout (conn)
  "Return the effective query timeout in seconds for CONN, or nil.
The timeout is clamped so the agent-side timeout fires before the outer
Emacs RPC timeout."
  (let* ((query-timeout (plist-get (clutch-jdbc-conn-params conn) :query-timeout))
         (rpc-timeout   (clutch-jdbc--conn-rpc-timeout conn)))
    (when (and query-timeout (> query-timeout 0))
      (min query-timeout (max 1 (- rpc-timeout 5))))))

(defun clutch-jdbc--oracle-conn-p (conn)
  "Return non-nil when CONN is an Oracle JDBC connection."
  (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle))

(defun clutch-jdbc--clickhouse-conn-p (conn)
  "Return non-nil when CONN is a ClickHouse JDBC connection."
  (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'clickhouse))

(defun clutch-jdbc--clickhouse-simple-identifier-p (name)
  "Return non-nil when NAME is a bare ClickHouse identifier."
  (and (stringp name)
       (string-match-p "\\`[A-Za-z_][A-Za-z0-9_]*\\'" name)))

(defun clutch-jdbc--clickhouse-escape-identifier (name)
  "Escape ClickHouse identifier NAME.
Leave simple identifiers bare so generated SQL matches common ClickHouse usage;
fall back to backticks when quoting is required."
  (if (clutch-jdbc--clickhouse-simple-identifier-p name)
      name
    (format "`%s`" (replace-regexp-in-string "`" "``" name))))

(defun clutch-jdbc--url-metadata (url)
  "Return endpoint metadata parsed from JDBC URL, or nil.
Supports common `jdbc:subprotocol://host[:port]/database' URLs."
  (when (and url
             (string-match
              "\\`jdbc:[^:]+://\\([^/:;?]+\\)\\(?::\\([0-9]+\\)\\)?/\\([^/?;]+\\)"
              url))
    (list :host (match-string 1 url)
          :port (when-let* ((port (match-string 2 url)))
                  (string-to-number port))
          :database (match-string 3 url))))

(defconst clutch-jdbc--oracle-system-schemas
  '("SYS" "SYSTEM" "XDB" "MDSYS" "CTXSYS" "LBACSYS" "OLAPSYS"
    "WMSYS" "DBSNMP" "APPQOSSYS" "AUDSYS" "DVSYS"
    "GSMADMIN_INTERNAL" "OJVMSYS" "OUTLN")
  "Oracle schemas hidden from interactive schema switching by default.")

(cl-defmethod clutch-db-manual-commit-p ((conn clutch-jdbc-conn))
  "Return non-nil when JDBC CONN runs with auto-commit disabled."
  (let ((params (clutch-jdbc-conn-params conn)))
    (clutch-jdbc--manual-commit-mode (plist-get params :driver) params)))

(cl-defmethod clutch-db-commit ((conn clutch-jdbc-conn))
  "Commit the current transaction on JDBC CONN."
  (clutch-jdbc--rpc "commit"
                    `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                    (clutch-jdbc--conn-rpc-timeout conn)))

(cl-defmethod clutch-db-rollback ((conn clutch-jdbc-conn))
  "Roll back the current transaction on JDBC CONN."
  (clutch-jdbc--rpc "rollback"
                    `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                    (clutch-jdbc--conn-rpc-timeout conn)))

(cl-defmethod clutch-db-set-auto-commit ((conn clutch-jdbc-conn) auto-commit)
  "Set auto-commit mode on JDBC CONN.
AUTO-COMMIT non-nil enables auto-commit (disables manual-commit); nil
enables manual-commit.  When switching to auto-commit, the JDBC driver
commits any pending transaction per the JDBC specification."
  (clutch-jdbc--rpc "set-auto-commit"
                    `((conn-id    . ,(clutch-jdbc-conn-conn-id conn))
                      (auto-commit . ,(if auto-commit t clutch-jdbc--json-false)))
                    (clutch-jdbc--conn-rpc-timeout conn))
  (setf (clutch-jdbc-conn-params conn)
        (plist-put (clutch-jdbc-conn-params conn) :manual-commit (not auto-commit))))

(cl-defmethod clutch-db-eager-schema-refresh-p ((conn clutch-jdbc-conn))
  "Return non-nil when CONN should refresh schema eagerly.
Oracle JDBC schema enumeration is too slow to block connect."
  (not (clutch-jdbc--oracle-conn-p conn)))

(cl-defmethod clutch-db-completion-sync-columns-p ((conn clutch-jdbc-conn))
  "Return non-nil when CONN may synchronously load completion columns.
This is allowed in the hot path."
  (not (clutch-jdbc--oracle-conn-p conn)))

;;;; Query methods

(defun clutch-jdbc--fetch-all (conn cursor-id)
  "Fetch all remaining rows for CURSOR-ID on CONN, returning a flat list."
  (let ((rpc-timeout (clutch-jdbc--conn-rpc-timeout conn))
        (effective-qt (clutch-jdbc--conn-effective-query-timeout conn))
        batches done)
    (while (not done)
      (let ((result (clutch-jdbc--rpc-on-conn
                     conn
                     "fetch"
                     `((cursor-id  . ,cursor-id)
                       (fetch-size . ,clutch-jdbc-fetch-size)
                       ,@(when effective-qt
                           `((query-timeout-seconds . ,effective-qt))))
                     rpc-timeout)))
        (push (plist-get result :rows) batches)
        (setq done (eq t (plist-get result :done)))))
    (apply #'nconc (nreverse batches))))

(defun clutch-jdbc--collect-table-entries (conn result)
  "Return normalized table entry plists from get-tables RESULT on CONN.
Supports both the current plist-list payload under :tables and the older
cursor-style :rows format used in tests."
  (or (plist-get result :tables)
      (mapcar
       #'clutch-jdbc--table-entry-from-row
       (let* ((first-rows (plist-get result :rows))
              (cursor-id  (plist-get result :cursor-id)))
         (if (eq t (plist-get result :done))
             first-rows
           (nconc first-rows (clutch-jdbc--fetch-all conn cursor-id)))))))

(defun clutch-jdbc--entry-type= (entry type)
  "Return non-nil when ENTRY has TYPE, case-insensitively."
  (string-equal (upcase (or (plist-get entry :type) ""))
                (upcase type)))

(defun clutch-jdbc--table-entry-from-row (row)
  "Convert a get-tables ROW into a table entry plist."
  (pcase-let ((`(,name ,type ,schema ,src-schema) row))
    (list :name name
          :type type
          :schema schema
          :source-schema (or src-schema schema))))

(defun clutch-jdbc--type-category (jdbc-type-name)
  "Map JDBC-TYPE-NAME to a `clutch-db' type-category symbol."
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

(defun clutch-jdbc--json-bool (value)
  "Normalize VALUE decoded from JSON into an Elisp boolean."
  (and value (not (eq value clutch-jdbc--json-false))))

(defun clutch-jdbc--normalize-object-entry (entry)
  "Normalize agent ENTRY field names for the generic clutch object schema."
  (let ((normalized (copy-sequence entry)))
    (when-let* ((table (plist-get normalized :table)))
      (setq normalized (plist-put normalized :target-table table)))
    (when (plist-member normalized :unique)
      (setq normalized
            (plist-put normalized :unique
                       (clutch-jdbc--json-bool (plist-get normalized :unique)))))
    normalized))

(cl-defmethod clutch-db-query ((conn clutch-jdbc-conn) sql)
  "Execute SQL on JDBC CONN and return a `clutch-db-result'."
  (setf (clutch-jdbc-conn-busy conn) t)
  (unwind-protect
      (condition-case err
          (let* ((rpc-timeout   (clutch-jdbc--conn-rpc-timeout conn))
                 (effective-qt  (clutch-jdbc--conn-effective-query-timeout conn))
                 (result (clutch-jdbc--rpc-on-conn
                          conn
                          "execute"
                          `((conn-id    . ,(clutch-jdbc-conn-conn-id conn))
                            (sql        . ,sql)
                            (fetch-size . ,clutch-jdbc-fetch-size)
                            ,@(when effective-qt
                                `((query-timeout-seconds . ,effective-qt))))
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

(cl-defmethod clutch-db-interrupt-query ((conn clutch-jdbc-conn))
  "Interrupt the active JDBC request on CONN without dropping the session."
  (when-let* ((request-id (gethash conn clutch-jdbc--busy-request-ids)))
    (puthash request-id t clutch-jdbc--ignored-response-ids)
    (remhash conn clutch-jdbc--busy-request-ids)
    (let* ((id (clutch-jdbc--send "cancel"
                                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))))
           (response (clutch-jdbc--recv-response-nonfatal
                      id clutch-jdbc-cancel-timeout-seconds)))
      (and response (eq t (plist-get response :ok))))))

(defun clutch-jdbc--build-oracle-paged-sql (conn base offset page-size order-by)
  "Build Oracle ROWNUM-based pagination SQL for CONN.
BASE is the unpaginated SQL, and PAGE-SIZE bounds each page.
Compatible with all Oracle versions (9i+).  Page N (OFFSET>0) adds
an rn column as a side effect."
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
  "Build a paginated SQL query for JDBC CONN from BASE-SQL.
PAGE-NUM is zero-based, and PAGE-SIZE limits each page.  Oracle uses
ROWNUM subquery syntax compatible with all Oracle versions.  ORDER-BY
controls the optional sort clause.  Other
databases use SQL:2011 OFFSET/FETCH (Oracle 12c+, SQL Server 2012+,
DB2)."
  (if (clutch-db-sql-has-top-level-limit-p base-sql)
      base-sql
    (let* ((trimmed (string-trim-right
                     (replace-regexp-in-string ";\\s-*\\'" "" base-sql)))
           (sortable-sql (if order-by
                             (clutch-db-sql-strip-top-level-order-by trimmed)
                           trimmed))
           (offset  (* page-num page-size))
           (oracle-p (clutch-jdbc--oracle-conn-p conn)))
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

(cl-defmethod clutch-db-escape-identifier ((conn clutch-jdbc-conn) name)
  "Escape NAME as a SQL identifier for CONN using double quotes (ANSI standard)."
  (if (clutch-jdbc--clickhouse-conn-p conn)
      (clutch-jdbc--clickhouse-escape-identifier name)
    (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" name))))

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
  (when (clutch-jdbc--oracle-conn-p conn)
    (when-let* ((user (plist-get (clutch-jdbc-conn-params conn) :user)))
      (upcase user))))

(defun clutch-jdbc--conn-schema (conn)
  "Return the effective schema for CONN: explicit :schema or the Oracle default."
  (or (plist-get (clutch-jdbc-conn-params conn) :schema)
      (clutch-jdbc--default-schema conn)))

(defun clutch-jdbc--conn-catalog (conn)
  "Return the effective catalog for CONN, or nil when unused.
ClickHouse maps its current database to JDBC catalog, not schema."
  (or (plist-get (clutch-jdbc-conn-params conn) :catalog)
      (when (clutch-jdbc--clickhouse-conn-p conn)
        (or (plist-get (clutch-jdbc-conn-params conn) :database)
            "default"))))

(defun clutch-jdbc--metadata-scope-params (conn)
  "Return optional JDBC metadata scope params for CONN.
ClickHouse omits catalog because its JDBC metadata filter drops rows
when a catalog is supplied."
  (let ((catalog (unless (clutch-jdbc--clickhouse-conn-p conn)
                   (clutch-jdbc--conn-catalog conn)))
        (schema (clutch-jdbc--conn-schema conn)))
    (append (when catalog `((catalog . ,catalog)))
            (when schema `((schema . ,schema))))))

(defun clutch-jdbc--visible-schemas (conn schemas)
  "Normalize visible SCHEMAS for CONN."
  (let* ((current (clutch-jdbc--conn-schema conn))
         (schemas (delete-dups
                   (seq-filter
                    (lambda (schema)
                      (and (stringp schema)
                           (not (string-empty-p schema))
                           (or (not (clutch-jdbc--oracle-conn-p conn))
                               (not (member-ignore-case schema
                                                        clutch-jdbc--oracle-system-schemas)))))
                    schemas))))
    (sort schemas
          (lambda (a b)
            (cond
             ((and current (string= (downcase a) (downcase current))) t)
             ((and current (string= (downcase b) (downcase current))) nil)
             (t (string-collate-lessp a b)))))))

(cl-defmethod clutch-db-list-tables ((conn clutch-jdbc-conn))
  "Return table names for JDBC CONN using DatabaseMetaData.
For Oracle, defaults the schema filter to the connected username to avoid
returning tables from SYS/SYSTEM and other visible schemas."
  (mapcar (lambda (entry) (plist-get entry :name))
          (clutch-db-list-table-entries conn)))

(cl-defmethod clutch-db-list-table-entries ((conn clutch-jdbc-conn))
  "Return table entry plists for JDBC CONN."
  (let* ((result  (clutch-jdbc--rpc
                   "get-tables"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     ,@(clutch-jdbc--metadata-scope-params conn))))
         (entries (clutch-jdbc--collect-table-entries conn result)))
    entries))

(cl-defmethod clutch-db-list-schemas ((conn clutch-jdbc-conn))
  "Return visible schema names for JDBC CONN when supported."
  (when (clutch-jdbc--oracle-conn-p conn)
    (let* ((rpc-timeout (clutch-jdbc--conn-rpc-timeout conn))
           (result (clutch-jdbc--rpc
                    "get-schemas"
                    `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                    rpc-timeout)))
      (clutch-jdbc--visible-schemas conn (plist-get result :schemas)))))

(cl-defmethod clutch-db-current-schema ((conn clutch-jdbc-conn))
  "Return the effective schema for JDBC CONN."
  (clutch-jdbc--conn-schema conn))

(cl-defmethod clutch-db-set-current-schema ((conn clutch-jdbc-conn) schema)
  "Switch JDBC CONN to SCHEMA."
  (unless (clutch-jdbc--oracle-conn-p conn)
    (user-error "Schema switching is currently supported only for Oracle JDBC"))
  (let ((schema (upcase schema)))
    (clutch-jdbc--rpc
     "set-current-schema"
     `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
       (schema . ,schema))
     (clutch-jdbc--conn-rpc-timeout conn))
    (setf (clutch-jdbc-conn-params conn)
          (plist-put (clutch-jdbc-conn-params conn) :schema schema))
    schema))

(cl-defmethod clutch-db-browseable-object-entries ((conn clutch-jdbc-conn))
  "Return the fast browseable object snapshot for JDBC CONN.
Oracle/JDBC already surfaces synonym-aware entries through `get-tables', so do
not issue an additional empty-prefix `search-tables' scan here."
  (clutch-db-list-table-entries conn))

(cl-defmethod clutch-db-refresh-schema-async ((conn clutch-jdbc-conn) callback
                                              &optional errback)
  "Refresh JDBC table names for CONN asynchronously.
Call CALLBACK on success or ERRBACK on failure."
  (let ((rpc-timeout (clutch-jdbc--conn-rpc-timeout conn)))
    (clutch-jdbc--rpc-async
     "get-tables"
     `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
       ,@(clutch-jdbc--metadata-scope-params conn))
     (lambda (result)
       (when callback
         (funcall callback
                  (mapcar (lambda (entry) (plist-get entry :name))
                          (seq-filter (lambda (entry)
                                        (clutch-jdbc--entry-type= entry "TABLE"))
                                      (clutch-jdbc--collect-table-entries conn result))))))
     errback
     rpc-timeout
     conn)
    t))

(cl-defmethod clutch-db-column-details-async ((conn clutch-jdbc-conn) table callback
                                              &optional errback)
  "Fetch JDBC column details for TABLE on CONN asynchronously."
  (let ((rpc-timeout (clutch-jdbc--conn-rpc-timeout conn)))
    (clutch-jdbc--rpc-async
     "get-columns"
     `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
       (table . ,table)
       ,@(clutch-jdbc--metadata-scope-params conn))
     (lambda (result)
       (when callback
         (funcall callback (plist-get result :columns))))
     errback
     rpc-timeout
     conn)
    t))

(cl-defmethod clutch-db-list-columns ((conn clutch-jdbc-conn) table)
  "Return column names for TABLE on JDBC CONN using DatabaseMetaData."
  (let* ((result  (clutch-jdbc--rpc
                   "get-columns"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     (table   . ,table)
                     ,@(clutch-jdbc--metadata-scope-params conn)))))
    (mapcar (lambda (col) (plist-get col :name))
            (plist-get result :columns))))

(cl-defmethod clutch-db-complete-tables ((conn clutch-jdbc-conn) prefix)
  "Return table name candidates matching PREFIX for JDBC CONN.
For Oracle: uses the schema cache when available (no RPC); falls back to a
`search-tables' RPC when the cache is absent or marked stale."
  (when (clutch-jdbc--oracle-conn-p conn)
    (if-let* ((schema-ready
               (and (fboundp 'clutch--schema-status-entry)
                    (eq (plist-get (clutch--schema-status-entry conn) :state) 'ready)))
              (schema (and schema-ready
                           (gethash (clutch--connection-key conn) clutch--schema-cache))))
        (let ((cached
               (seq-filter (lambda (name)
                             (string-prefix-p (upcase prefix) (upcase name)))
                           (hash-table-keys schema))))
          (if cached
              cached
            (let* ((result (clutch-jdbc--rpc
                            "search-tables"
                            `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                              (prefix  . ,prefix)
                              ,@(clutch-jdbc--metadata-scope-params conn)))))
              (mapcar (lambda (tbl) (plist-get tbl :name))
                      (plist-get result :tables)))))
      (let* ((result (clutch-jdbc--rpc
                      "search-tables"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                        (prefix  . ,prefix)
                        ,@(clutch-jdbc--metadata-scope-params conn)))))
        (mapcar (lambda (tbl) (plist-get tbl :name))
                (plist-get result :tables))))))

(cl-defmethod clutch-db-search-table-entries ((conn clutch-jdbc-conn) prefix)
  "Return table entry plists matching PREFIX for JDBC CONN."
  (let* ((result (clutch-jdbc--rpc
                  "search-tables"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (prefix  . ,prefix)
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (plist-get result :tables)))

(cl-defmethod clutch-db-complete-columns ((conn clutch-jdbc-conn) table prefix)
  "Return column name candidates for TABLE matching PREFIX on JDBC CONN."
  (let* ((params (clutch-jdbc-conn-params conn))
         (driver (plist-get params :driver)))
    (when (eq driver 'oracle)
      (let* ((result (clutch-jdbc--rpc
                      "search-columns"
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                        (table   . ,table)
                        (prefix  . ,prefix)
                        ,@(clutch-jdbc--metadata-scope-params conn)))))
        (mapcar (lambda (col) (plist-get col :name))
                (plist-get result :columns))))))

(cl-defmethod clutch-db-show-create-table ((conn clutch-jdbc-conn) table)
  "Return a best-effort DDL for TABLE on JDBC CONN.
Built from DatabaseMetaData column info; not a true SHOW CREATE TABLE."
  (let* ((params (clutch-jdbc-conn-params conn))
         (driver (plist-get params :driver)))
    (let* ((result (clutch-jdbc--rpc
                    "get-columns"
                    `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                      (table   . ,table)
                      ,@(clutch-jdbc--metadata-scope-params conn))))
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
                         (if (clutch-jdbc--json-bool (plist-get col :nullable))
                             ""
                           " NOT NULL")))
               cols
               ",\n")))))

(cl-defmethod clutch-db-list-objects ((conn clutch-jdbc-conn) category)
  "Return object entry plists for CATEGORY on JDBC CONN."
  (let* ((op (pcase category
               ('indexes "get-indexes")
               ('sequences "get-sequences")
               ('procedures "get-procedures")
               ('functions "get-functions")
               ('triggers "get-triggers")
               (_ nil))))
    (when op
      (let* ((result (clutch-jdbc--rpc
                      op
                      `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                        ,@(clutch-jdbc--metadata-scope-params conn))))
             (key (pcase category
                    ('indexes :indexes)
                    ('sequences :sequences)
                    ('procedures :procedures)
                    ('functions :functions)
                    ('triggers :triggers))))
        (mapcar #'clutch-jdbc--normalize-object-entry
                (plist-get result key))))))

(cl-defmethod clutch-db-list-objects-async ((conn clutch-jdbc-conn) category callback
                                            &optional errback)
  "Fetch object entry plists for CATEGORY on JDBC CONN asynchronously."
  (let* ((op (pcase category
               ('indexes "get-indexes")
               ('sequences "get-sequences")
               ('procedures "get-procedures")
               ('functions "get-functions")
               ('triggers "get-triggers")
               (_ nil)))
         (key (pcase category
                ('indexes :indexes)
                ('sequences :sequences)
                ('procedures :procedures)
                ('functions :functions)
                ('triggers :triggers)
                (_ nil))))
    (when (and op key)
      (clutch-jdbc--rpc-async
       op
       `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
         ,@(clutch-jdbc--metadata-scope-params conn))
       (lambda (result)
         (when callback
           (funcall callback
                    (mapcar #'clutch-jdbc--normalize-object-entry
                            (plist-get result key)))))
       errback
       (clutch-jdbc--conn-rpc-timeout conn)
       conn)
      t)))

(cl-defmethod clutch-db-object-details ((conn clutch-jdbc-conn) entry)
  "Return detail plists for JDBC object ENTRY on CONN."
  (let* ((type (upcase (or (plist-get entry :type) ""))))
    (pcase type
      ("INDEX"
       (let ((result
              (clutch-jdbc--rpc
               "get-index-columns"
               `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                 (index   . ,(plist-get entry :name))
                 ,@(when (plist-get entry :target-table)
                     `((table . ,(plist-get entry :target-table))))
                 ,@(clutch-jdbc--metadata-scope-params conn)))))
         (plist-get result :columns)))
      ((or "PROCEDURE" "FUNCTION")
       (let ((result
              (clutch-jdbc--rpc
               (if (string= type "PROCEDURE")
                   "get-procedure-params"
                 "get-function-params")
               `((conn-id  . ,(clutch-jdbc-conn-conn-id conn))
                 (name     . ,(plist-get entry :name))
                 ,@(when (plist-get entry :identity)
                     `((identity . ,(plist-get entry :identity))))
                 ,@(clutch-jdbc--metadata-scope-params conn)))))
         (plist-get result :params)))
      (_ nil))))

(cl-defmethod clutch-db-object-source ((conn clutch-jdbc-conn) entry)
  "Return source text for JDBC object ENTRY on CONN."
  (let* ((result (clutch-jdbc--rpc
                  "get-object-source"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (name    . ,(plist-get entry :name))
                    (type    . ,(plist-get entry :type))
                    ,@(when (plist-get entry :identity)
                        `((identity . ,(plist-get entry :identity))))
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (plist-get result :source)))

(cl-defmethod clutch-db-show-create-object ((conn clutch-jdbc-conn) entry)
  "Return DDL text for JDBC non-table ENTRY on CONN."
  (let* ((result (clutch-jdbc--rpc
                  "get-object-ddl"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (name    . ,(plist-get entry :name))
                    (type    . ,(plist-get entry :type))
                    ,@(when (plist-get entry :identity)
                        `((identity . ,(plist-get entry :identity))))
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (plist-get result :ddl)))

(cl-defmethod clutch-db-table-comment ((_conn clutch-jdbc-conn) _table)
  "Return nil — table comments are not available via standard DatabaseMetaData."
  nil)

(cl-defmethod clutch-db-primary-key-columns ((conn clutch-jdbc-conn) table)
  "Return primary key columns for TABLE on JDBC CONN."
  (let* ((result (clutch-jdbc--rpc
                  "get-primary-keys"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (plist-get result :primary-keys)))

(cl-defmethod clutch-db-foreign-keys ((conn clutch-jdbc-conn) table)
  "Return foreign key info for TABLE on JDBC CONN."
  (let* ((result (clutch-jdbc--rpc
                  "get-foreign-keys"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (mapcar (lambda (fk)
              (cons (plist-get fk :fk-column)
                    (list :ref-table  (plist-get fk :pk-table)
                          :ref-column (plist-get fk :pk-column))))
            (plist-get result :foreign-keys))))

(cl-defmethod clutch-db-referencing-objects ((conn clutch-jdbc-conn) table)
  "Return objects that reference TABLE on JDBC CONN."
  (let* ((result (clutch-jdbc--rpc
                  "get-referencing-objects"
                  `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                    (table   . ,table)
                    ,@(clutch-jdbc--metadata-scope-params conn)))))
    (mapcar (lambda (entry)
              (list :name (plist-get entry :name)
                    :type "TABLE"
                    :schema (plist-get entry :schema)
                    :source-schema (or (plist-get entry :source-schema)
                                       (plist-get entry :schema))))
            (plist-get result :objects))))

(cl-defmethod clutch-db-column-details ((conn clutch-jdbc-conn) table)
  "Return detailed column info for TABLE on JDBC CONN."
  (let* ((pk-cols (clutch-db-primary-key-columns conn table))
         (fks     (clutch-db-foreign-keys conn table))
         (result  (clutch-jdbc--rpc
                   "get-columns"
                   `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                     (table   . ,table)
                     ,@(clutch-jdbc--metadata-scope-params conn))))
         (cols    (plist-get result :columns)))
    (mapcar (lambda (col)
              (let ((name (plist-get col :name)))
                (list :name        name
                      :type        (plist-get col :type)
                      :nullable    (clutch-jdbc--json-bool (plist-get col :nullable))
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
  (or (plist-get (clutch-jdbc-conn-params conn) :host)
      (plist-get (clutch-jdbc--url-metadata
                  (plist-get (clutch-jdbc-conn-params conn) :url))
                 :host)))

(cl-defmethod clutch-db-port ((conn clutch-jdbc-conn))
  "Return the port for JDBC CONN."
  (or (plist-get (clutch-jdbc-conn-params conn) :port)
      (plist-get (clutch-jdbc--url-metadata
                  (plist-get (clutch-jdbc-conn-params conn) :url))
                 :port)))

(cl-defmethod clutch-db-database ((conn clutch-jdbc-conn))
  "Return the database for JDBC CONN."
  (or (plist-get (clutch-jdbc-conn-params conn) :database)
      (plist-get (clutch-jdbc--url-metadata
                  (plist-get (clutch-jdbc-conn-params conn) :url))
                 :database)))

(cl-defmethod clutch-db-display-name ((conn clutch-jdbc-conn))
  "Return a display name for CONN based on the JDBC driver type."
  (or (plist-get (clutch-jdbc-conn-params conn) :display-name)
      (pcase (plist-get (clutch-jdbc-conn-params conn) :driver)
        ('oracle    "Oracle")
        ('sqlserver "SQL Server")
        ('db2       "DB2")
        ('snowflake "Snowflake")
        ('redshift  "Redshift")
        ('clickhouse "ClickHouse")
        (_          "JDBC"))))

;;;; Agent installation helpers

(defun clutch-jdbc--download-agent-jar ()
  "Download the agent jar from GitHub Releases and verify its checksum."
  (let ((jar (clutch-jdbc--agent-jar))
        (url (format
              "https://github.com/LuciusChen/clutch-jdbc-agent/releases/download/v%s/clutch-jdbc-agent-%s.jar"
              clutch-jdbc-agent-version
              clutch-jdbc-agent-version)))
    (make-directory clutch-jdbc-agent-dir t)
    (make-directory (clutch-jdbc--drivers-dir) t)
    (message "Downloading clutch-jdbc-agent %s..." clutch-jdbc-agent-version)
    (url-copy-file url jar t)
    (unless (clutch-jdbc--agent-jar-valid-p jar)
      (error "Downloaded clutch-jdbc-agent checksum mismatch: %s" jar))
    (clutch-jdbc--cleanup-stale-agent-jars)
    (message "Downloaded to %s" jar)))

(defun clutch-jdbc-ensure-agent ()
  "Download clutch-jdbc-agent.jar if not present.
Fetches from GitHub Releases."
  (interactive)
  (let ((jar (clutch-jdbc--agent-jar)))
    (if (and (file-exists-p jar)
             (clutch-jdbc--agent-jar-valid-p jar))
        (progn
          (clutch-jdbc--cleanup-stale-agent-jars)
          (message "clutch-jdbc-agent already at %s" jar))
      (clutch-jdbc--download-agent-jar))))

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
  "Download a Maven artifact at COORDS to DEST.
COORDS is \"group:artifact:version\" or \"group:artifact:version:classifier\"."
  (pcase-let ((`(,group ,artifact ,version ,classifier . ,_)
               (split-string coords ":")))
    (let* ((group-path (replace-regexp-in-string "\\." "/" group))
           (jar-name   (if classifier
                           (format "%s-%s-%s.jar" artifact version classifier)
                         (format "%s-%s.jar" artifact version)))
           (url        (format "https://repo1.maven.org/maven2/%s/%s/%s/%s"
                               group-path artifact version jar-name)))
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
