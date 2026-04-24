;;; clutch-connection.el --- Connection lifecycle and transaction commands -*- lexical-binding: t; -*-

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

;; Connection lifecycle management, transaction state tracking, backend
;; detection, header-line rendering, and authentication for clutch.
;;
;; This module is required by `clutch.el' — do not require `clutch' here.

;;; Code:

(require 'clutch-db)
(require 'clutch-schema)
(require 'auth-source)
(require 'cl-lib)
(require 'comint)

(declare-function auth-source-pass-parse-entry "auth-source-pass" (entry))

;; Forward declarations — variables defined in clutch.el
(defvar clutch-connection)
(defvar clutch--buffer-error-details)
(defvar clutch--executing-p)
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar clutch--console-name)
(defvar clutch-connection-alist nil)
(defvar clutch-connect-timeout-seconds 10)
(defvar clutch-read-idle-timeout-seconds 30)
(defvar clutch-query-timeout-seconds 30)
(defvar clutch-jdbc-rpc-timeout-seconds 30)
(defvar clutch--dml-result)
(defvar clutch-debug-mode nil)
(defvar clutch--spinner-timer nil
  "Timer driving the mode-line spinner animation, or nil.")
(defvar clutch--spinner-index 0
  "Current frame index into `clutch--spinner-frames'.")

(defconst clutch--spinner-frames
  ["⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏"]
  "Braille spinner frames.")

(defconst clutch--spinner-interval 0.1
  "Seconds between spinner frame advances.")

(defconst clutch--ssh-tunnel-ready-poll-interval 0.05
  "Seconds between SSH tunnel readiness checks.")

;; Forward declarations — functions defined in clutch.el
(declare-function clutch--effective-sql-product "clutch" (params))
(declare-function clutch--clear-connection-problem-capture "clutch" (connection))
(declare-function clutch--forget-problem-record "clutch" (&optional buffer connection))
(declare-function clutch--remember-debug-event "clutch" (&rest event))
(declare-function clutch--remember-problem-record "clutch" (&rest args))
(declare-function clutch--update-console-buffer-name "clutch" ())
(declare-function clutch--refresh-result-status-line "clutch" ())
(declare-function clutch--refresh-schema-status-ui "clutch" (conn))
(declare-function clutch--update-position-indicator "clutch" ())
(declare-function clutch--strip-leading-comments "clutch-query" (sql))
(declare-function clutch--schema-affecting-query-p "clutch-query" (sql))
(declare-function clutch--sql-main-op-keyword "clutch-query" (sql))
(declare-function clutch--debug-workflow-message "clutch-query" (message))
(declare-function clutch--humanize-db-error "clutch-query" (msg))

;; Forward declarations — functions defined in other modules
(declare-function clutch-jdbc-conn-p "clutch-db-jdbc" (conn))
(declare-function clutch-jdbc-conn-params "clutch-db-jdbc" (conn))
(declare-function clutch--icon "clutch-ui" (name &optional fallback &rest icon-args))
(declare-function clutch--icon-with-face "clutch-ui"
                  (name fallback face &rest icon-args))
(declare-function clutch--nerd-icons-available-p "clutch-ui" ())

;;;; Connection identity

(defvar clutch--connection-remote-params-cache
  (make-hash-table :test 'eq :weakness 'key)
  "Original remote connection params keyed by live connection object.")

(defvar clutch--connection-ssh-tunnel-cache
  (make-hash-table :test 'eq :weakness 'key)
  "SSH tunnel process plists keyed by live connection object.")

(defun clutch--connection-remote-param (conn key)
  "Return remote KEY for CONN when clutch cached transport metadata."
  (when conn
    (plist-get (gethash conn clutch--connection-remote-params-cache) key)))

(defun clutch--connection-remote-host (conn)
  "Return the remote host label for CONN."
  (or (clutch--connection-remote-param conn :host)
      (clutch-db-host conn)))

(defun clutch--connection-remote-port (conn)
  "Return the remote port label for CONN."
  (or (clutch--connection-remote-param conn :port)
      (clutch-db-port conn)))

(defun clutch--connection-ssh-host (conn)
  "Return the configured SSH host alias for CONN, or nil."
  (clutch--connection-remote-param conn :ssh-host))

(defun clutch--remember-connection-transport (conn params &optional tunnel)
  "Remember original PARAMS and optional SSH TUNNEL for CONN."
  (when conn
    (let ((remote nil))
      (when-let* ((host (plist-get params :host)))
        (setq remote (plist-put remote :host host)))
      (when-let* ((port (plist-get params :port)))
        (setq remote (plist-put remote :port port)))
      (when-let* ((ssh-host (plist-get params :ssh-host)))
        (setq remote (plist-put remote :ssh-host ssh-host)))
      (puthash conn remote clutch--connection-remote-params-cache))
    (if tunnel
        (puthash conn tunnel clutch--connection-ssh-tunnel-cache)
      (remhash conn clutch--connection-ssh-tunnel-cache))))

(defun clutch--release-connection-transport (conn)
  "Stop any SSH tunnel and forget cached transport metadata for CONN."
  (when conn
    (when-let* ((tunnel (gethash conn clutch--connection-ssh-tunnel-cache))
                (proc (plist-get tunnel :process)))
      (when (process-live-p proc)
        (delete-process proc)))
    (remhash conn clutch--connection-ssh-tunnel-cache)
    (remhash conn clutch--connection-remote-params-cache)))

(defun clutch--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (format "%s@%s:%s/%s"
          (or (clutch-db-user conn) "?")
          (or (clutch--connection-remote-host conn) "?")
          (or (clutch--connection-remote-port conn) "?")
          (or (clutch-db-database conn) "")))

(defun clutch--default-port-for-connection (conn)
  "Return the default port for CONN's backend, or nil when not applicable."
  (pcase (downcase (or (clutch-db-display-name conn) ""))
    ("mysql" 3306)
    ("postgresql" 5432)
    ("oracle" 1521)
    ("sql server" 1433)
    (_ nil)))

(defun clutch--connection-display-key (conn)
  "Return a compact display identity for CONN for use in UI only."
  (let* ((user (or (clutch-db-user conn) "?"))
         (host (or (clutch--connection-remote-host conn) "?"))
         (port (clutch--connection-remote-port conn))
         (ssh-host (clutch--connection-ssh-host conn))
         (default-port (clutch--default-port-for-connection conn)))
    (concat
     (format "%s@%s%s"
             user
             host
             (if (and port default-port (equal port default-port))
                 ""
               (if port
                   (format ":%s" port)
                 "")))
     (if ssh-host
         (format " via %s" ssh-host)
       ""))))

(defun clutch--ensure-clutch-loaded ()
  "Load the `clutch' entrypoint before module-autoloaded commands run.
This ensures user setup attached to feature `clutch' has executed before
interactive readers inspect shared customization such as
`clutch-connection-alist'."
  (unless (featurep 'clutch)
    (require 'clutch)))

(defun clutch--connection-oracle-jdbc-p (conn)
  "Return non-nil when CONN is a JDBC Oracle connection."
  (and conn
       (fboundp 'clutch-jdbc-conn-p)
       (clutch-jdbc-conn-p conn)
       (eq (plist-get (clutch-jdbc-conn-params conn) :driver) 'oracle)))

(defun clutch--connection-clickhouse-p (conn)
  "Return non-nil when CONN is a ClickHouse connection."
  (and conn
       (eq (ignore-errors (clutch--backend-key-from-conn conn)) 'clickhouse)))

(defun clutch--params-clickhouse-p (params)
  "Return non-nil when connection PARAMS target ClickHouse."
  (eq (and params (clutch--backend-key-from-params params)) 'clickhouse))

;;;; SQL helpers for transaction state

(defun clutch--sql-leading-keyword (sql)
  "Return the leading SQL keyword for SQL, or nil."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (when (string-match "\\`\\([[:alpha:]]+\\)" trimmed)
      (upcase (match-string 1 trimmed)))))

(defun clutch--manual-commit-dirtying-query-p (sql)
  "Return non-nil when SQL should mark a manual-commit transaction dirty."
  (member (clutch--sql-main-op-keyword sql)
          '("INSERT" "UPDATE" "DELETE" "MERGE" "REPLACE")))

(defun clutch--transactional-schema-query-p (conn sql)
  "Return non-nil when schema-affecting SQL stays pending on CONN."
  (and (clutch--schema-affecting-query-p sql)
       (eq (ignore-errors (clutch--backend-key-from-conn conn)) 'pg)))

(defun clutch--transaction-control-query-p (sql)
  "Return non-nil when SQL is explicit transaction control."
  (member (clutch--sql-leading-keyword sql)
          '("COMMIT" "ROLLBACK" "END" "ABORT")))

;;;; Transaction state

(defvar clutch--tx-dirty-cache (make-hash-table :test 'eq :weakness 'key)
  "Connections with uncommitted DML.")

(defun clutch--tx-dirty-p (conn)
  "Return non-nil when CONN has uncommitted DML."
  (and conn (gethash conn clutch--tx-dirty-cache)))

(defun clutch--refresh-transaction-ui (conn)
  "Refresh transaction indicators for buffers attached to CONN."
  (when conn
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and clutch-connection
                     (eq clutch-connection conn)
                     (derived-mode-p 'clutch-mode 'clutch-repl-mode))
            (clutch--update-mode-line)))))))

(defun clutch--set-tx-dirty (conn)
  "Mark CONN as having uncommitted DML."
  (when conn
    (puthash conn t clutch--tx-dirty-cache)
    (clutch--refresh-transaction-ui conn)))

(defun clutch--clear-tx-dirty (conn)
  "Forget any pending transaction state for CONN."
  (when conn
    (remhash conn clutch--tx-dirty-cache)
    (clutch--refresh-transaction-ui conn)))

(defun clutch--annotate-dml-result-buffers (conn banner)
  "Set BANNER as header-line on all open DML result buffers for CONN."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (when (and (derived-mode-p 'clutch-result-mode)
                 (eq clutch-connection conn)
                 (bound-and-true-p clutch--dml-result))
        (setq-local header-line-format banner)))))

(defun clutch--mark-dml-results-rolled-back (conn)
  "Add a rollback warning banner to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ⚠  Transaction rolled back — changes not persisted"
               'face '(:inherit warning :weight bold))))

(defun clutch--mark-dml-results-committed (conn)
  "Add a committed confirmation banner to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ✓  Transaction committed"
               'face '(:inherit success :weight bold))))

(defun clutch--mark-dml-results-connection-closed (conn)
  "Add a connection-closed notice to open DML result buffers for CONN."
  (clutch--annotate-dml-result-buffers
   conn
   (propertize "  ✕  Connection closed"
               'face '(:inherit shadow))))

(defun clutch--tx-header-line-segment (conn)
  "Return a header-line segment for CONN transaction state, or nil.
Shows Tx: Auto, Tx: Manual, or Tx: Manual* (dirty)."
  (when conn
    (let* ((state-face (if (clutch-db-manual-commit-p conn)
                           (if (clutch--tx-dirty-p conn) 'error 'warning)
                         'success))
           (icon (clutch--icon-with-face '(mdicon . "nf-md-database_lock")
                                         "⛁" state-face))
           (label (if (clutch-db-manual-commit-p conn)
                      (if (clutch--tx-dirty-p conn) "Tx: Manual*" "Tx: Manual")
                    "Tx: Auto")))
      (concat (unless (string-empty-p icon)
                (concat icon " "))
              (propertize label 'face state-face)))))

(defun clutch--record-tx-state-after-query (conn sql)
  "Update transaction dirty state for successful SQL on CONN."
  (when (clutch-db-manual-commit-p conn)
    (cond
     ((clutch--transaction-control-query-p sql)
      (clutch--clear-tx-dirty conn))
     ((and (clutch--connection-oracle-jdbc-p conn)
           (clutch--schema-affecting-query-p sql))
      (clutch--clear-tx-dirty conn))
     ((clutch--transactional-schema-query-p conn sql)
      (clutch--set-tx-dirty conn))
     ((clutch--manual-commit-dirtying-query-p sql)
      (clutch--set-tx-dirty conn)))))

(defun clutch--run-db-query (conn sql &optional params)
  "Execute SQL on CONN with optional PARAMS and keep transaction UI state in sync."
  (let ((result (if params
                    (clutch-db-execute-params conn sql params)
                  (clutch-db-query conn sql))))
    (clutch--clear-connection-problem-capture conn)
    (clutch--record-tx-state-after-query conn sql)
    result))

(defun clutch--confirm-disconnect-transaction-loss (conn prompt)
  "Require confirmation with PROMPT before dropping dirty manual-commit CONN."
  (when (and (clutch-db-manual-commit-p conn)
             (clutch--tx-dirty-p conn)
             (not (yes-or-no-p prompt)))
    (user-error "Disconnect cancelled")))

;;;; Connection lifecycle

(defun clutch--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn (clutch-db-live-p conn)))

(defun clutch--connection-context (conn)
  "Return `(PARAMS PRODUCT)' for CONN from any attached buffer, or nil."
  (when conn
    (or (and (eq clutch-connection conn)
             clutch--connection-params
             (list clutch--connection-params clutch--conn-sql-product))
        (cl-loop for buf in (buffer-list)
                 when (and (buffer-live-p buf)
                           (eq (buffer-local-value 'clutch-connection buf) conn)
                           (buffer-local-value 'clutch--connection-params buf))
                 return (list (buffer-local-value 'clutch--connection-params buf)
                              (buffer-local-value 'clutch--conn-sql-product buf))))))

(defun clutch--bind-connection-context (conn &optional params product)
  "Bind CONN and related reconnect context in the current buffer.
Also store PARAMS and PRODUCT when present."
  (setq-local clutch-connection conn)
  (setq-local clutch--buffer-error-details nil)
  (when params
    (setq-local clutch--connection-params params))
  (when (or params product)
    (setq-local clutch--conn-sql-product
                (or product
                    (and params (clutch--effective-sql-product params))
                    clutch--conn-sql-product))))

(defun clutch--rebind-connection-buffers (old-conn new-conn params product)
  "Replace OLD-CONN with NEW-CONN across attached buffers using PARAMS and PRODUCT."
  (dolist (buf (buffer-list))
    (when (and (buffer-live-p buf)
               (eq (buffer-local-value 'clutch-connection buf) old-conn))
      (with-current-buffer buf
        (clutch--bind-connection-context new-conn params product)
        (cond
         ((derived-mode-p 'clutch-mode)
          (when clutch--console-name
            (clutch--update-console-buffer-name))
          (clutch--update-mode-line))
         ((derived-mode-p 'clutch-result-mode)
          (clutch--refresh-result-status-line)
          (clutch--update-position-indicator)))))))

(defun clutch--activate-current-buffer-connection (conn params &optional product)
  "Bind CONN as the current buffer connection and prime local UI state.
Also remember PARAMS and PRODUCT."
  (clutch--bind-connection-context conn params product)
  (clutch--prime-schema-cache conn)
  (clutch--update-mode-line)
  conn)

(defun clutch--finalize-rebound-connection (conn)
  "Prime metadata and refresh UI after CONN has been rebound to buffers."
  (clutch--prime-schema-cache conn)
  (clutch--refresh-schema-status-ui conn)
  (clutch--refresh-transaction-ui conn)
  conn)

(defun clutch--try-reconnect ()
  "Attempt to re-establish the connection for the current logical session.
Find reconnect params from the current buffer or any attached buffer that
still references the same dead connection, then rebind all attached buffers
to the new connection on success.
Pending staged changes in result buffers are preserved — they are
client-side SQL that has not been sent to the database, so they remain
valid on the new connection.  Only a query re-execution (which changes
the underlying result rows) should discard pending changes.
Returns non-nil on success, nil on failure."
  (when-let* ((old-conn clutch-connection)
              (context (clutch--connection-context old-conn))
              (params (car context))
              (product (cadr context)))
    (condition-case err
        (let ((conn (clutch--build-conn params)))
          (clutch--clear-tx-dirty old-conn)
          (clutch--release-connection-transport old-conn)
          (clutch--rebind-connection-buffers old-conn conn params product)
          (clutch--finalize-rebound-connection conn)
          (message "Reconnected to %s" (clutch--connection-key conn))
          t)
      (error
       (message "Reconnect failed: %s" (error-message-string err))
       nil))))

(defun clutch--replace-connection (old-conn params &optional product)
  "Replace OLD-CONN with a new connection built from PARAMS.
PRODUCT is the effective SQL product for the new logical session."
  (let* ((product (or product (clutch--effective-sql-product params)))
         (old-key (clutch--connection-key old-conn))
         (new-conn (clutch--build-conn params)))
    (clutch--clear-tx-dirty old-conn)
    (unwind-protect
        (when (clutch--connection-alive-p old-conn)
          (clutch-db-disconnect old-conn))
      (clutch--release-connection-transport old-conn))
    (unless (clutch--connection-alive-p new-conn)
      (setq new-conn (clutch--build-conn params)))
    (clutch--rebind-connection-buffers old-conn new-conn params product)
    (clutch--clear-connection-metadata-caches old-conn old-key)
    (clutch--clear-connection-metadata-caches new-conn)
    (clutch--finalize-rebound-connection new-conn)))

(defun clutch--ensure-connection ()
  "Ensure current buffer has a live connection.
If the connection has dropped, attempts to reconnect automatically
using the stored params.  Signals a user-error if not recoverable."
  (unless (clutch--connection-alive-p clutch-connection)
    (unless (clutch--try-reconnect)
      (user-error
       (if (derived-mode-p 'clutch-result-mode 'clutch-record-mode
                           'clutch-describe-mode)
           "Connection closed.  Reconnect from the SQL buffer or REPL"
         "Not connected.  Use C-c C-e to connect")))))

;;;; Schema status header-line

(defun clutch--schema-status-header-line-segment (conn)
  "Return a header-line segment for CONN schema status, or nil.
Returns nil when the schema is ready (no noise for the happy path)."
  (when-let* ((entry (clutch--schema-status-entry conn))
              (state (plist-get entry :state)))
    (pcase state
      ('refreshing (propertize "schema…" 'face 'shadow))
      ('stale      (propertize "schema~" 'face 'warning))
      ('failed     (propertize "schema!" 'face 'error))
      (_ nil))))

(defun clutch--current-namespace-name (conn)
  "Return the current schema/database label for CONN, or nil."
  (or (and (clutch--connection-clickhouse-p conn)
           (clutch-db-database conn))
      (clutch-db-current-schema conn)))

(defun clutch--current-schema-header-line-segment (conn)
  "Return a header-line segment for CONN's current schema or database, or nil."
  (when-let* ((schema (clutch--current-namespace-name conn)))
    (let ((icon (clutch--icon-with-face '(mdicon . "nf-md-sitemap_outline")
                                        "≣" 'header-line)))
      (if (string-empty-p icon)
          schema
        (format "%s %s" icon schema)))))

;;;; Backend detection and icons

(defconst clutch--db-icon-specs
  ;; Each entry: (BACKEND . (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS))
  ;; :color sets the icon foreground; remaining ICON-ARGS (e.g. :height) are
  ;; forwarded to the nerd-icons function.
  '((mysql      . ((devicon . "nf-dev-mysql")              ""  :color "#469AD7"))
    (pg         . ((devicon . "nf-dev-postgresql")         ""  :color "#336791"))
    (sqlite     . ((devicon . "nf-dev-sqlite")             ""  :color "#3A7EC6"))
    (jdbc       . ((mdicon  . "nf-md-database_cog_outline") "" :color "#59636e"))
    (oracle     . ((mdicon  . "nf-md-alpha_o_circle")      "O" :color "#C74634"))
    (sqlserver  . ((devicon . "nf-dev-microsoftsqlserver") ""  :color "#CC2927"))
    (snowflake  . ((mdicon  . "nf-md-snowflake")           "❄" :color "#29B5E8"))
    (db2        . ((mdicon  . "nf-md-database")            ""  :color "#1F70C1"))
    (redshift   . ((mdicon  . "nf-md-database")            ""  :color "#8C4FFF"))
    (clickhouse . ((faicon  . "nf-fa-barcode")             ""  :color "#FFCC00")))
  "Alist mapping backend symbols to icon specs.
Each value is (ICON-SPEC FALLBACK :color COLOR &rest ICON-ARGS).
ICON-ARGS beyond :color are forwarded to the nerd-icons render function.")

(defun clutch--db-backend-icon-for-key (key)
  "Return a colored backend icon for KEY, or nil."
  (when-let* ((spec (alist-get key clutch--db-icon-specs)))
    (let* ((rest      (cddr spec))
           (color     (plist-get rest :color))
           (icon-args (cl-loop for (k v) on rest by #'cddr
                               unless (eq k :color) nconc (list k v)))
           (icon      (apply #'clutch--icon (car spec) (cadr spec) icon-args)))
      (if (and color (not (string-empty-p icon)))
          (propertize icon 'face `(:foreground ,color :inherit ,(get-text-property 0 'face icon)))
        icon))))

(defun clutch--backend-key-from-conn (conn)
  "Return backend icon key for live connection CONN, or nil."
  (or (and (fboundp 'clutch-jdbc-conn-p)
           (condition-case nil
               (and (clutch-jdbc-conn-p conn)
                    (plist-get (clutch-jdbc-conn-params conn) :driver))
             ((clutch-db-error wrong-type-argument) nil)))
      (pcase (condition-case nil
                 (clutch-db-display-name conn)
               ((cl-no-applicable-method wrong-type-argument) nil))
        ("MySQL" 'mysql)
        ("PostgreSQL" 'pg)
        ("SQLite" 'sqlite)
        (_ nil))))

(defun clutch--backend-key-from-params (params)
  "Return backend icon key for connection PARAMS, or nil."
  (let ((backend (plist-get params :backend))
        (driver  (plist-get params :driver)))
    (or driver
        (pcase backend
          ('jdbc 'jdbc)
          ((or 'pg 'postgresql) 'pg)
          ((or 'mysql 'mariadb) 'mysql)
          ('sqlite 'sqlite)
          ('oracle 'oracle)
          ('sqlserver 'sqlserver)
          ('snowflake 'snowflake)
          ('db2 'db2)
          ('redshift 'redshift)
          ('clickhouse 'clickhouse)
          (_ nil)))))

(defun clutch--backend-display-name-from-params (params)
  "Return UI backend name for connection PARAMS, or nil."
  (or (plist-get params :display-name)
      (pcase (clutch--backend-key-from-params params)
        ('mysql "MySQL")
        ('pg "PostgreSQL")
        ('sqlite "SQLite")
        ('jdbc "JDBC")
        ('oracle "Oracle")
        ('sqlserver "SQL Server")
        ('snowflake "Snowflake")
        ('db2 "DB2")
        ('redshift "Redshift")
        ('clickhouse "ClickHouse")
        (_ nil))))

(defun clutch--connection-backend-segment (&optional conn params)
  "Return the shared backend segment for CONN or PARAMS, or nil.
When nerd-icons is available, show only the icon; otherwise fall back
to the display name (e.g. \"MySQL\")."
  (let* ((icon (clutch--db-backend-icon-for-key
                (or (and conn (clutch--backend-key-from-conn conn))
                    (and params (clutch--backend-key-from-params params)))))
         (name (or (and conn (clutch-db-display-name conn))
                   (and params (clutch--backend-display-name-from-params params)))))
    (cond
     ((and icon (not (string-empty-p icon))
           (clutch--nerd-icons-available-p))
      icon)
     (name (propertize name 'face 'bold)))))

(defun clutch--connection-state-icon (connected)
  "Return a connection state icon for CONNECTED."
  (if connected
      (clutch--icon '(mdicon . "nf-md-database_check_outline") "⬢")
    (clutch--icon '(mdicon . "nf-md-database_off") "⨯")))

;;;; Header-line and mode-line

(defun clutch--header-line-indent ()
  "Return leading spaces to align header-line text with the buffer text area.
Accounts for the line-number gutter when `display-line-numbers-mode' is on."
  (make-string (max 1 (line-number-display-width)) ?\s))

(defun clutch--build-connection-header-line ()
  "Build the header-line string for the current clutch buffer."
  (let ((indent (clutch--header-line-indent)))
    (if (not (clutch--connection-alive-p clutch-connection))
        (let* ((sep          (propertize "  •  " 'face 'shadow))
               (backend      (clutch--connection-backend-segment
                              clutch-connection clutch--connection-params))
               (disconnect   (propertize
                              (concat (clutch--connection-state-icon nil)
                                      " Disconnect")
                              'face 'warning))
               (parts        (delq nil (list (if backend
                                                 backend
                                               nil)
                                             disconnect))))
          (concat indent
                  (if parts
                      (mapconcat #'identity parts sep)
                    disconnect)))
      (let* ((sep     (propertize "  •  " 'face 'shadow))
             (backend (clutch--connection-backend-segment clutch-connection))
             (key     (concat (clutch--connection-state-icon t)
                              " "
                              (clutch--connection-display-key clutch-connection)))
             (current-schema
              (clutch--current-schema-header-line-segment clutch-connection))
             (schema  (clutch--schema-status-header-line-segment clutch-connection))
             (tx      (clutch--tx-header-line-segment clutch-connection))
             (parts   (delq nil (list backend
                                      key current-schema schema tx))))
        (concat indent (mapconcat #'identity parts sep))))))

(defun clutch--spinner-start ()
  "Start the spinner timer if not already running."
  (unless clutch--spinner-timer
    (setq clutch--spinner-index 0)
    (setq clutch--spinner-timer
          (run-at-time 0 clutch--spinner-interval
                       #'clutch--spinner-tick))))

(defun clutch--spinner-stop ()
  "Stop the spinner timer and reset state."
  (when clutch--spinner-timer
    (cancel-timer clutch--spinner-timer)
    (setq clutch--spinner-timer nil)
    (setq clutch--spinner-index 0)))

(defun clutch--spinner-tick ()
  "Advance spinner frame and update mode-lines of busy buffers."
  (setq clutch--spinner-index
        (mod (1+ clutch--spinner-index)
             (length clutch--spinner-frames)))
  (let ((any-busy nil))
    (dolist (buf (buffer-list))
      (when (and (buffer-live-p buf)
                 (buffer-local-value 'clutch--executing-p buf))
        (setq any-busy t)
        (with-current-buffer buf
          (clutch--update-mode-line))))
    (if any-busy
        (redisplay)
      (clutch--spinner-stop))))

(defun clutch--spinner-string ()
  "Return the current spinner frame string, or nil when idle."
  (when clutch--spinner-timer
    (aref clutch--spinner-frames clutch--spinner-index)))

(defun clutch--update-mode-line ()
  "Update buffer-local execution UI with connection status."
  (let* ((base (if (derived-mode-p 'clutch-repl-mode) "clutch-repl" "clutch"))
         (spinner (clutch--spinner-string)))
    (setq mode-name
          (if (and clutch--executing-p spinner)
              (concat base " " (propertize spinner 'face 'success))
            base)))
  (when (derived-mode-p 'clutch-result-mode)
    (when (fboundp 'clutch--refresh-footer-timing)
      (clutch--refresh-footer-timing)))
  (when (derived-mode-p 'clutch-mode 'clutch-repl-mode)
    ;; Use :eval so line-number-display-width is recomputed on each redraw,
    ;; keeping alignment correct when display-line-numbers-mode is toggled.
    (setq header-line-format '((:eval (clutch--build-connection-header-line)))))
  (force-mode-line-update))

;;;; JDBC backend detection

(defconst clutch--jdbc-backends
  '(jdbc oracle sqlserver db2 snowflake redshift)
  "Backends routed through the JDBC agent.")

(defun clutch--jdbc-backend-p (backend)
  "Return non-nil when BACKEND is handled by JDBC."
  (memq backend clutch--jdbc-backends))

;;;; Timeout normalization

(defun clutch--normalize-timeout-params (backend params)
  "Return PARAMS with unified timeout defaults for BACKEND.
Signals a `user-error' when removed timeout keys are present."
  (when (plist-member params :read-timeout)
    (user-error "Connection parameter :read-timeout was removed; use :read-idle-timeout"))
  (let ((normalized (copy-sequence params)))
    (cond
     ((eq backend 'mysql)
      (setq normalized
            (plist-put normalized :connect-timeout
                       (or (plist-get normalized :connect-timeout)
                           clutch-connect-timeout-seconds)))
      (setq normalized
            (plist-put normalized :read-idle-timeout
                       (or (plist-get normalized :read-idle-timeout)
                           clutch-read-idle-timeout-seconds))))
     ((eq backend 'pg)
      (setq normalized
            (plist-put normalized :connect-timeout
                       (or (plist-get normalized :connect-timeout)
                           clutch-connect-timeout-seconds)))
      (setq normalized
            (plist-put normalized :read-idle-timeout
                       (or (plist-get normalized :read-idle-timeout)
                           clutch-read-idle-timeout-seconds)))
      (setq normalized
            (plist-put normalized :query-timeout
                       (or (plist-get normalized :query-timeout)
                           clutch-query-timeout-seconds))))
     ((clutch--jdbc-backend-p backend)
      (setq normalized
            (plist-put normalized :connect-timeout
                       (or (plist-get normalized :connect-timeout)
                           clutch-connect-timeout-seconds)))
      (setq normalized
            (plist-put normalized :read-idle-timeout
                       (or (plist-get normalized :read-idle-timeout)
                           clutch-read-idle-timeout-seconds)))
      (setq normalized
            (plist-put normalized :query-timeout
                       (or (plist-get normalized :query-timeout)
                           clutch-query-timeout-seconds)))
      (setq normalized
            (plist-put normalized :rpc-timeout
                       (or (plist-get normalized :rpc-timeout)
                           clutch-jdbc-rpc-timeout-seconds)))))
    normalized))

;;;; Password resolution and connection building

(defun clutch--auth-source-target (params)
  "Return a human-readable auth-source target string for PARAMS."
  (let ((user (plist-get params :user))
        (host (plist-get params :host))
        (port (plist-get params :port)))
    (cond
     ((and user host port) (format "%s@%s:%s" user host port))
     ((and user host) (format "%s@%s" user host))
     (host host)
     (t "the configured credential source"))))

(defun clutch--password-lookup-error (message)
  "Signal MESSAGE as a user-facing password lookup failure."
  (user-error "%s" message))

(defun clutch--resolve-pass-entry-password (entry)
  "Return the password from pass ENTRY.
Signal `user-error' when a matching pass entry exists but cannot be read."
  (when-let* ((path (clutch-db--pass-entry-by-suffix entry)))
    (let ((parsed (auth-source-pass-parse-entry path)))
      (cond
       ((null parsed)
        (clutch--password-lookup-error
         (format
          "Database password lookup failed for pass entry %s. Unlock pass/auth-source-pass and retry"
          path)))
       ((not (assq 'secret parsed))
        (clutch--password-lookup-error
         (format
          "Database password lookup failed for pass entry %s because it does not contain a secret"
          path)))
       (t
        (cdr (assq 'secret parsed)))))))

(defun clutch--auth-source-first-match (params target)
  "Return the first auth-source match for PARAMS targeting TARGET."
  (condition-case err
      (car (auth-source-search
            :host (plist-get params :host)
            :user (plist-get params :user)
            :port (plist-get params :port)
            :max 1))
    (error
     (clutch--password-lookup-error
      (format "Database password lookup failed via auth-source for %s: %s"
              target
              (error-message-string err))))))

(defun clutch--auth-source-secret-value (secret target)
  "Return auth-source SECRET for TARGET, or signal `user-error'."
  (cond
   ((null secret)
    (clutch--password-lookup-error
     (format
      "Database password lookup failed via auth-source for %s. The matching credential has no secret"
      target)))
   ((functionp secret)
    (let ((value
           (condition-case secret-err
               (funcall secret)
             (error
              (clutch--password-lookup-error
               (format
                "Database password lookup failed via auth-source for %s: %s"
                target
                (error-message-string secret-err)))))))
      (or value
          (clutch--password-lookup-error
           (format
            "Database password lookup failed via auth-source for %s. Unlock the credential store and retry"
            target)))))
   (t secret)))

(defun clutch--resolve-auth-source-password (params)
  "Return a password from `auth-source' for PARAMS, or nil when absent.
Signal `user-error' when auth-source finds a credential but cannot
read its secret."
  (let ((target (clutch--auth-source-target params)))
    (when-let* ((found (clutch--auth-source-first-match params target)))
      (clutch--auth-source-secret-value (plist-get found :secret) target))))

(defun clutch--resolve-password (params)
  "Return the password for connection PARAMS.
Checks in order:
  1. :password key (non-empty string) — used as-is.
  2. :pass-entry key — suffix-matched against all pass entries, so
     \\='dev-mysql\\=' finds \\='mysql/dev-mysql\\='.  Automatically set to the
     connection name by callers; override in `clutch-connection-alist'.
  3. `auth-source-search' by :host/:user/:port (authinfo / pass).
Returns nil when nothing is found (caller should prompt if needed).
Signals `user-error' when a configured credential source matches but
cannot be read."
  (let ((pw    (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (not (string-empty-p pw))) pw)
     (t
      (or (and entry (clutch--resolve-pass-entry-password entry))
          (clutch--resolve-auth-source-password params))))))

(defun clutch--debug-connection-context (backend params)
  "Return a redacted connect context for BACKEND and PARAMS."
  (let ((context nil))
    (when-let* ((user (plist-get params :user)))
      (setq context (plist-put context :user user)))
    (when-let* ((host (plist-get params :host)))
      (setq context (plist-put context :host host)))
    (when-let* ((port (plist-get params :port)))
      (setq context (plist-put context :port port)))
    (when-let* ((database (plist-get params :database)))
      (setq context (plist-put context :database database)))
    (when-let* ((display-name (plist-get params :display-name)))
      (setq context (plist-put context :display-name display-name)))
    (when-let* ((ssh-host (plist-get params :ssh-host)))
      (setq context (plist-put context :ssh-host ssh-host)))
    (setq context (plist-put context :backend backend))
    context))

(defun clutch--ssh-tunnel-enabled-p (params)
  "Return non-nil when PARAMS request an SSH tunnel."
  (let ((ssh-host (plist-get params :ssh-host)))
    (and (stringp ssh-host)
         (not (string-empty-p ssh-host)))))

(defun clutch--allocate-local-port ()
  "Return an available local TCP port for an SSH tunnel."
  (condition-case err
      (let* ((listener (make-network-process :name "clutch-ssh-port-reserve"
                                             :server t
                                             :host "127.0.0.1"
                                             :service t
                                             :family 'ipv4
                                             :noquery t))
             (port (process-contact listener :service)))
        (delete-process listener)
        port)
    (file-error
     (signal 'clutch-db-error
             (list (format "Cannot allocate a local port for the SSH tunnel: %s"
                           (error-message-string err)))))))

(defun clutch--ssh-tunnel-buffer-name (ssh-host)
  "Return the process buffer name for SSH-HOST."
  (format " *clutch-ssh %s*" ssh-host))

(defun clutch--ssh-prepare-buffer-name (ssh-host)
  "Return the interactive SSH prepare buffer name for SSH-HOST."
  (format "*clutch-ssh-prepare %s*" ssh-host))

(defun clutch--default-ssh-host ()
  "Return the current buffer's default SSH host alias, or nil."
  (let* ((params (or clutch--connection-params
                     (car-safe (clutch--connection-context clutch-connection))))
         (ssh-host (plist-get params :ssh-host)))
    (when (and (stringp ssh-host)
               (not (string-empty-p ssh-host)))
      ssh-host)))

(defun clutch--read-ssh-host-alias ()
  "Prompt for an SSH host alias from OpenSSH config."
  (let* ((default (clutch--default-ssh-host))
         (prompt (if default
                     (format "SSH host from ~/.ssh/config (%s): " default)
                   "SSH host from ~/.ssh/config: "))
         (ssh-host (read-string prompt nil nil default)))
    (if (string-empty-p ssh-host)
        (user-error "An SSH host alias is required")
      ssh-host)))

(defun clutch--ssh-buffer-output (buffer)
  "Return BUFFER contents as a trimmed string, or an empty string."
  (if (buffer-live-p buffer)
      (with-current-buffer buffer
        (string-trim (buffer-substring-no-properties (point-min) (point-max))))
    ""))

(defun clutch--ssh-output-last-line (output)
  "Return the last non-empty line from SSH OUTPUT."
  (when (and output
             (not (string-empty-p output)))
    (let ((lines (split-string output "\n" t "[ \t\r]+")))
      (car (last lines)))))

(defun clutch--ssh-diagnose-output (ssh-host output)
  "Return a user-facing diagnosis for SSH-HOST using SSH OUTPUT."
  (let* ((cleaned (string-trim (or output "")))
         (last-line (or (clutch--ssh-output-last-line cleaned)
                        "the ssh process exited before the tunnel became ready"))
         (case-fold-search t))
    (cond
     ((or (string-match-p "enter passphrase for key" cleaned)
          (string-match-p "incorrect passphrase" cleaned)
          (string-match-p "agent refused operation" cleaned)
          (string-match-p "sign_and_send_pubkey" cleaned))
      (format
       "the SSH key for %s is locked. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once to unlock it"
       ssh-host ssh-host))
     ((or (string-match-p "host key verification failed" cleaned)
          (string-match-p "host identification has changed" cleaned)
          (string-match-p "are you sure you want to continue connecting" cleaned))
      (format
       "SSH host verification for %s needs attention. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once to confirm the host key"
       ssh-host ssh-host))
     ((string-match-p "permission denied" cleaned)
      (format
       "SSH authentication to %s was rejected. Run M-x clutch-prepare-ssh-host once to complete any key, passphrase, or host-key setup; if SSH already works interactively, check the remote username and ~/.ssh/authorized_keys"
       ssh-host))
     ((or (string-match-p "could not resolve hostname" cleaned)
          (string-match-p "name or service not known" cleaned))
      (format "OpenSSH could not resolve host %s. Check the alias in ~/.ssh/config"
              ssh-host))
     ((or (string-match-p "connection refused" cleaned)
          (string-match-p "operation timed out" cleaned)
          (string-match-p "connection timed out" cleaned)
          (string-match-p "no route to host" cleaned)
          (string-match-p "network is unreachable" cleaned))
      (format "OpenSSH could not reach %s (%s)" ssh-host last-line))
     ((or (string-match-p "administratively prohibited" cleaned)
          (string-match-p "open failed" cleaned))
      (format "the remote side rejected SSH port forwarding via %s" ssh-host))
     ((string-empty-p cleaned)
      (format
       "OpenSSH could not use host %s in batch mode. Run M-x clutch-prepare-ssh-host or `ssh %s exit` once first"
       ssh-host ssh-host))
     (t last-line))))

(defun clutch--ssh-prepare-sentinel (proc _event)
  "Report completion state for SSH prepare PROC."
  (when (memq (process-status proc) '(exit signal))
    (let* ((ssh-host (process-get proc :clutch-ssh-host))
           (buffer (process-buffer proc))
           (buffer-name (and (buffer-live-p buffer) (buffer-name buffer))))
      (if (and (eq (process-status proc) 'exit)
               (zerop (process-exit-status proc)))
          (message "SSH host %s is ready for batch use" ssh-host)
        (message "SSH prepare for %s exited. If prompts completed, retry clutch-connect; otherwise inspect %s"
                 ssh-host
                 (or buffer-name "the SSH prepare buffer"))))))

(defun clutch--start-ssh-prepare-session (ssh-host)
  "Start an interactive SSH prepare session for SSH-HOST."
  (let* ((buffer-name (clutch--ssh-prepare-buffer-name ssh-host))
         (buffer (get-buffer-create buffer-name))
         (proc (get-buffer-process buffer)))
    (if (process-live-p proc)
        buffer
      (with-current-buffer buffer
        (let ((inhibit-read-only t))
          (erase-buffer)))
      (setq buffer (make-comint-in-buffer
                    (format "clutch-ssh-prepare-%s" ssh-host)
                    buffer
                    "ssh"
                    nil
                    ssh-host
                    "exit"))
      (setq proc (get-buffer-process buffer))
      (process-put proc :clutch-ssh-host ssh-host)
      (set-process-query-on-exit-flag proc nil)
      (set-process-sentinel proc #'clutch--ssh-prepare-sentinel)
      buffer)))

(defun clutch--ssh-prepare-session-live-p (buffer)
  "Return non-nil when BUFFER hosts a live SSH prepare process."
  (when-let* ((proc (and (buffer-live-p buffer)
                         (get-buffer-process buffer))))
    (process-live-p proc)))

(defun clutch--prepare-ssh-host-message (ssh-host buffer)
  "Display a status message after opening SSH-HOST prepare BUFFER."
  (if (clutch--ssh-prepare-session-live-p buffer)
      (message "Complete any SSH prompts in %s, then retry clutch-connect"
               (buffer-name buffer))
    (message "SSH host %s is ready for batch use" ssh-host)))

(defun clutch--ssh-tunnel-error (params buffer reason)
  "Signal a `clutch-db-error' for PARAMS using BUFFER and REASON."
  (let* ((ssh-host (plist-get params :ssh-host))
         (buffer-name (and (buffer-live-p buffer) (buffer-name buffer)))
         (summary (format "SSH tunnel to %s failed" ssh-host))
         (message (if buffer-name
                      (format "%s: %s. Inspect %s for SSH output"
                              summary reason buffer-name)
                    (format "%s: %s" summary reason))))
    (signal 'clutch-db-error
            (list message
                  (list :summary summary
                        :diag (list :raw-message message
                                    :context (list :ssh-host ssh-host
                                                   :ssh-buffer buffer-name
                                                   :host (plist-get params :host)
                                                   :port (plist-get params :port))))))))

(defun clutch--ssh-local-port-open-p (port)
  "Return non-nil when localhost PORT accepts TCP connections."
  (condition-case nil
      (let ((probe (make-network-process :name "clutch-ssh-ready-probe"
                                         :host "127.0.0.1"
                                         :service port
                                         :family 'ipv4
                                         :noquery t)))
        (delete-process probe)
        t)
    (error nil)))

(defun clutch--wait-for-ssh-tunnel (proc port params buffer timeout)
  "Wait until PROC forwards localhost PORT or signal a tunnel error.
PARAMS describe the original connection, BUFFER captures SSH output,
and TIMEOUT is the maximum wait in seconds."
  (let ((deadline (+ (float-time) (max 1 timeout))))
    (while (and (process-live-p proc)
                (< (float-time) deadline)
                (not (clutch--ssh-local-port-open-p port)))
      (accept-process-output proc clutch--ssh-tunnel-ready-poll-interval))
    (cond
     ((clutch--ssh-local-port-open-p port) t)
     ((process-live-p proc)
      (delete-process proc)
      (clutch--ssh-tunnel-error params buffer "the local forward did not become ready in time"))
     (t
      (let* ((ssh-host (plist-get params :ssh-host))
             (output (clutch--ssh-buffer-output buffer))
             (reason (clutch--ssh-diagnose-output ssh-host output)))
        (clutch--ssh-tunnel-error params buffer reason))))))

(defun clutch--start-ssh-tunnel (params)
  "Start an SSH tunnel for PARAMS using the user's OpenSSH config."
  (unless (executable-find "ssh")
    (user-error "SSH tunnels require the OpenSSH client executable `ssh'"))
  (when (eq (plist-get params :backend) 'sqlite)
    (user-error "SQLite connections do not support SSH tunnels"))
  (when (plist-get params :url)
    (user-error "SSH tunnels currently require structured :host/:port params, not :url"))
  (unless (plist-get params :host)
    (user-error "SSH tunnels require :host for the remote database endpoint"))
  (unless (plist-get params :port)
    (user-error "SSH tunnels require :port for the remote database endpoint"))
  (let* ((ssh-host (plist-get params :ssh-host))
         (local-port (clutch--allocate-local-port))
         (buffer (get-buffer-create (clutch--ssh-tunnel-buffer-name ssh-host)))
         (timeout (or (plist-get params :connect-timeout)
                      clutch-connect-timeout-seconds))
         (proc nil))
    (with-current-buffer buffer
      (erase-buffer))
    (setq proc (make-process
                :name (format "clutch-ssh-%s" ssh-host)
                :buffer buffer
                :command (list "ssh"
                               "-N"
                               "-o" "BatchMode=yes"
                               "-o" "ExitOnForwardFailure=yes"
                               "-L" (format "127.0.0.1:%d:%s:%s"
                                            local-port
                                            (plist-get params :host)
                                            (plist-get params :port))
                               ssh-host)
                :coding 'utf-8
                :noquery t))
    (set-process-query-on-exit-flag proc nil)
    (clutch--wait-for-ssh-tunnel proc local-port params buffer timeout)
    (list :process proc
          :local-port local-port
          :buffer buffer
          :ssh-host ssh-host)))

(defun clutch--prepare-connect-params (params)
  "Return `(CONNECT-PARAMS TUNNEL)' for PARAMS.
When PARAMS request SSH, CONNECT-PARAMS targets the local forwarded port
and TUNNEL contains the live process metadata."
  (if (not (clutch--ssh-tunnel-enabled-p params))
      (list params nil)
    (let* ((tunnel (clutch--start-ssh-tunnel params))
           (connect-params (copy-sequence params)))
      (setq connect-params (plist-put connect-params :host "127.0.0.1"))
      (setq connect-params (plist-put connect-params :port
                                      (plist-get tunnel :local-port)))
      (list connect-params tunnel))))

(defun clutch--make-connection-error-details (params err)
  "Return structured error details for a failed connection attempt.
PARAMS describe the attempted connection and ERR is the original
signaled condition."
  (let* ((message (or (cadr err) (error-message-string err)))
         (backend (clutch--backend-key-from-params params))
         (details (copy-tree (nth 2 err)))
         (diag (copy-tree (plist-get details :diag)))
         (context (copy-tree (plist-get diag :context)))
         (default-context (clutch--debug-connection-context backend params)))
    (unless details
      (setq details (list :summary (clutch--humanize-db-error message))))
    (unless (plist-member details :backend)
      (setq details (plist-put details :backend backend)))
    (unless (plist-get details :summary)
      (setq details (plist-put details :summary
                               (clutch--humanize-db-error message))))
    (unless diag
      (setq diag (list :raw-message message)))
    (unless (plist-get diag :raw-message)
      (setq diag (plist-put diag :raw-message message)))
    (cl-loop for (key val) on default-context by #'cddr
             unless (plist-member context key)
             do (setq context (plist-put context key val)))
    (setq diag (plist-put diag :context context))
    (plist-put details :diag diag)))

(defun clutch--materialize-connection-params (params)
  "Return effective connection PARAMS with resolved credentials included.
The returned plist keeps the original backend-facing keys, but fills in the
password that `clutch--resolve-password' produced so later reconnects reuse the
same credentials as the successful foreground connection."
  (let* ((backend (or (plist-get params :backend)
                      (user-error "Connection params require :backend")))
         (params (clutch--normalize-timeout-params backend params))
         (password (clutch--resolve-password params)))
    (when (and (clutch--jdbc-backend-p backend)
               (plist-get params :pass-entry)
               (null password))
      (user-error
       (concat "No password resolved for JDBC connection %s (:pass-entry %s). "
               "Enable auth-source-pass/auth-source, or set :password explicitly")
       backend
       (plist-get params :pass-entry)))
    (if password
        (plist-put (copy-sequence params) :password password)
      params)))

(defun clutch--build-conn (params)
  "Connect to a database using PARAMS, resolving the password via auth-source.
Returns a live connection object or signals a `user-error'."
  (let* ((effective-params params)
         (backend (plist-get params :backend))
         (ssh-tunnel nil))
    (condition-case err
        (progn
          (setq effective-params (clutch--materialize-connection-params params))
          (setq backend (plist-get effective-params :backend))
          (let* ((prepared (clutch--prepare-connect-params effective-params))
                 (connect-params (car prepared))
                 (password (plist-get connect-params :password))
                 (db-params (cl-loop for (k v) on connect-params by #'cddr
                                     unless (memq k '(:sql-product :backend :password :pass-entry
                                                                   :ssh-host))
                                     append (list k v)))
                 (db-params (if password
                                (append db-params (list :password password))
                              db-params)))
            (setq ssh-tunnel (cadr prepared))
            (let ((conn (clutch-db-connect backend db-params)))
              (clutch--remember-connection-transport conn effective-params ssh-tunnel)
              (when clutch-debug-mode
                (clutch--remember-debug-event
                 :connection conn
                 :op "connect"
                 :phase "success"
                 :backend backend
                 :summary (condition-case nil
                              (format "Connected to %s" (clutch--connection-key conn))
                            (error "Connected"))
                 :context (clutch--debug-connection-context backend effective-params)))
              conn)))
      (clutch-db-error
       (when-let* ((proc (and ssh-tunnel (plist-get ssh-tunnel :process))))
         (when (process-live-p proc)
           (delete-process proc)))
       (clutch--remember-problem-record
        :buffer (current-buffer)
        :problem (clutch--make-connection-error-details effective-params err))
       (let ((message (clutch--humanize-db-error
                       (or (car (cdr err))
                           (error-message-string err)))))
         (when clutch-debug-mode
           (clutch--remember-debug-event
            :op "connect"
            :phase "error"
            :backend backend
            :summary message
            :context (clutch--debug-connection-context backend effective-params)))
         (user-error "%s" (clutch--debug-workflow-message message)))))))

(defun clutch--inject-entry-name (params name)
  "Return PARAMS with :pass-entry defaulting to NAME.
Leaves PARAMS unchanged when :password or :pass-entry is already set."
  (if (or (plist-get params :pass-entry) (plist-get params :password))
      params
    (append params (list :pass-entry name))))

(defun clutch--saved-connection-params (name)
  "Return saved connection params for NAME, or nil when missing."
  (when-let* ((params (cdr (assoc name clutch-connection-alist))))
    (clutch--inject-entry-name params name)))

(defun clutch--connect-params-for-current-buffer ()
  "Return connection params appropriate for the current buffer."
  (if clutch--console-name
      (or (clutch--saved-connection-params clutch--console-name)
          (user-error "Saved connection %s for this query console no longer exists"
                      clutch--console-name))
    (clutch--read-connection-params)))

(defun clutch--read-connection-params ()
  "Prompt the user for connection parameters and return a params plist.
Offers saved connections from `clutch-connection-alist' when non-empty,
otherwise prompts for :backend first, then for the backend-specific
connection parameters.
The password is resolved via `auth-source' before falling back to `read-passwd'."
  (clutch--ensure-clutch-loaded)
  (if clutch-connection-alist
      (let* ((name   (completing-read "Connection: "
                                      (mapcar #'car clutch-connection-alist)
                                      nil t))
             (params (clutch--saved-connection-params name)))
        params)
    (let* ((backend (intern
                     (completing-read
                      "Backend: "
                      (mapcar #'symbol-name
                              '(mysql pg sqlite oracle sqlserver db2 snowflake redshift))
                      nil t nil nil "mysql"))))
      (if (eq backend 'sqlite)
          (list :backend 'sqlite
                :database (read-string "Database (:memory:): " nil nil ":memory:"))
        (let* ((port-default (pcase backend
                               ('mysql 3306)
                               ('pg 5432)
                               ('oracle 1521)
                               ('sqlserver 1433)
               ('db2 50000)
                               ('redshift 5439)
                               ('snowflake 443)))
               (host (read-string "Host (127.0.0.1): " nil nil "127.0.0.1"))
               (port (read-number (format "Port (%d): " port-default) port-default))
               (user (read-string "User: "))
               (ssh-host (read-string "SSH host from ~/.ssh/config (optional): "))
               (manual-params (append (list :backend backend
                                            :host host :port port :user user)
                                      (unless (string-empty-p ssh-host)
                                        (list :ssh-host ssh-host))))
               (pw (or (clutch--resolve-password manual-params)
                       (read-passwd "Password: ")))
               (db (read-string "Database (optional): ")))
          (append manual-params
                  (list :password pw
                        :database (unless (string-empty-p db) db))))))))

;;;; Interactive connect/disconnect

;;;###autoload
(defun clutch-connect ()
  "Connect to a database server interactively.
If `clutch-connection-alist' is non-empty, offer saved connections via
  `completing-read'.  Otherwise prompt for each parameter.
The password is resolved via `auth-source' when not in the connection
params; see `clutch-connection-alist' for details."
  (interactive)
  (let ((old-conn clutch-connection)
        (old-live-p (clutch--connection-alive-p clutch-connection)))
    (when old-live-p
      (clutch--confirm-disconnect-transaction-loss
       old-conn
       "Uncommitted changes will be lost.  Disconnect? "))
    (let* ((params  (clutch--connect-params-for-current-buffer))
           (effective-params (clutch--materialize-connection-params params))
           (product (clutch--effective-sql-product effective-params))
           (conn    (clutch--build-conn params)))
      (when old-live-p
        (clutch--do-disconnect old-conn))
      (unless (clutch--connection-alive-p conn)
        (setq conn (clutch--build-conn params)))
      (clutch--activate-current-buffer-connection conn effective-params product)
      (message "Connected to %s" (clutch--connection-key conn)))))

;;;###autoload
(defun clutch-prepare-ssh-host (&optional ssh-host)
  "Open an interactive SSH session so OpenSSH can finish host/key setup.
This is useful before `clutch-connect' when a host alias in `~/.ssh/config'
still needs an initial passphrase entry or host-key confirmation."
  (interactive (list (clutch--read-ssh-host-alias)))
  (unless (executable-find "ssh")
    (user-error "SSH preparation requires the OpenSSH client executable `ssh'"))
  (let* ((ssh-host (or ssh-host
                       (clutch--read-ssh-host-alias)))
         (buffer (clutch--start-ssh-prepare-session ssh-host)))
    (pop-to-buffer buffer)
    (clutch--prepare-ssh-host-message ssh-host buffer)))

;;;###autoload
(defun clutch-disconnect ()
  "Disconnect from the current database server."
  (interactive)
  (when (clutch--connection-alive-p clutch-connection)
    (clutch--confirm-disconnect-transaction-loss
     clutch-connection
     "Uncommitted changes will be lost.  Disconnect? ")
    (clutch--do-disconnect clutch-connection)
    (message "Disconnected"))
  (setq clutch-connection nil)
  (when clutch--console-name
    (clutch--update-console-buffer-name))
  (clutch--update-mode-line))

(defun clutch--invalidate-derived-buffers (conn)
  "Nil out `clutch-connection' in all non-current buffers sharing CONN.
Also refreshes their mode-line/header-line to reflect the disconnected state."
  (dolist (buf (buffer-list))
    (when (and (buffer-live-p buf)
               (not (eq buf (current-buffer)))
               (eq (buffer-local-value 'clutch-connection buf) conn))
      (with-current-buffer buf
        (setq-local clutch-connection nil)
        (force-mode-line-update)))))

(defun clutch--do-disconnect (conn)
  "Perform full disconnect sequence for CONN.
Marks DML results, invalidates derived buffers, clears transaction
state, and disconnects the underlying connection."
  (clutch--mark-dml-results-connection-closed conn)
  (clutch--invalidate-derived-buffers conn)
  (clutch--clear-tx-dirty conn)
  (when clutch-debug-mode
    (clutch--remember-debug-event
     :connection conn
     :op "disconnect"
     :phase "success"
     :backend (clutch--backend-key-from-conn conn)
     :summary (condition-case nil
                  (format "Disconnected from %s" (clutch--connection-key conn))
                (error "Disconnected"))))
  (clutch--forget-problem-record nil conn)
  (unwind-protect
      (clutch-db-disconnect conn)
    (clutch--release-connection-transport conn)))

(defun clutch--disconnect-on-kill ()
  "Disconnect the connection owned by this buffer.
Invalidates all derived buffers that share the same connection.
Does nothing in indirect SQL buffers (`clutch--indirect-mode')."
  (when (and (not (bound-and-true-p clutch--indirect-mode))
             (clutch--connection-alive-p clutch-connection))
    (clutch--confirm-disconnect-transaction-loss
     clutch-connection
     "Uncommitted changes will be lost.  Kill buffer? ")
    (clutch--do-disconnect clutch-connection)))

;;;; Transaction commands

;;;###autoload
(defun clutch-commit ()
  "Commit the current transaction."
  (interactive)
  (clutch--ensure-connection)
  (unless (clutch-db-manual-commit-p clutch-connection)
    (user-error "Connection is in autocommit mode"))
  (clutch-db-commit clutch-connection)
  (clutch--mark-dml-results-committed clutch-connection)
  (clutch--clear-tx-dirty clutch-connection)
  (message "Transaction committed"))

;;;###autoload
(defun clutch-rollback ()
  "Roll back the current transaction."
  (interactive)
  (clutch--ensure-connection)
  (unless (clutch-db-manual-commit-p clutch-connection)
    (user-error "Connection is in autocommit mode"))
  (clutch-db-rollback clutch-connection)
  (clutch--mark-dml-results-rolled-back clutch-connection)
  (clutch--clear-tx-dirty clutch-connection)
  (message "Transaction rolled back"))

;;;###autoload
(defun clutch-toggle-auto-commit ()
  "Toggle auto-commit mode for the current connection.
When switching from manual-commit to auto-commit, the backend finishes
any open transaction according to its own semantics."
  (interactive)
  (clutch--ensure-connection)
  (let ((manual-now (clutch-db-manual-commit-p clutch-connection)))
    (when (and manual-now (clutch--tx-dirty-p clutch-connection))
      (user-error "Cannot toggle: commit or roll back pending changes first"))
    (clutch-db-set-auto-commit clutch-connection manual-now)
    (when manual-now
      (clutch--clear-tx-dirty clutch-connection))
    (clutch--update-mode-line)
    (message "Auto-commit %s" (if manual-now "enabled" "disabled"))))

(provide 'clutch-connection)
;;; clutch-connection.el ends here
