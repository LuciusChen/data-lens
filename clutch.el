;;; clutch.el --- Interactive database client -*- lexical-binding: t; -*-
;; Copyright (C) 2025-2026 Lucius Chen
;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1") (transient "0.3.7"))
;; Keywords: comm, data, tools
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

;; Interactive database client with native and JDBC backends.
;;
;; Provides:
;; - `clutch-mode': SQL editing major mode (derived from `sql-mode')
;; - `clutch-repl': REPL via `comint-mode'
;; - Query execution with horizontally scrollable result tables
;; - Object discovery and completion
;;
;; Entry points:
;;   M-x clutch-mode      — open a SQL editing buffer
;;   M-x clutch-repl      — open a REPL
;;   Open a .mysql file   — activates clutch-mode automatically

;;; Code:

(require 'clutch-db)
(require 'clutch-connection)
(require 'clutch-query)
(require 'sql)
(require 'comint)
(require 'cl-lib)

(defvar embark-general-map)
(defvar embark-target-finders)
(defvar embark-keymap-alist)
(require 'transient)
(require 'clutch-schema)
(require 'clutch-ui)
(require 'clutch-object)
(require 'clutch-edit)
(require 'xref)

(defvar clutch--describe-object-entry)

(declare-function clutch--cached-column-details "clutch-schema" (conn table))
(declare-function clutch--cached-table-comment "clutch-schema" (conn table))
(declare-function clutch--table-comment-cached-p "clutch-schema" (conn table))
(declare-function clutch--ensure-column-details-async "clutch-schema" (conn table))
(declare-function clutch--ensure-columns-async "clutch-schema" (conn schema table))
(declare-function clutch--ensure-point-visible-horizontally "clutch-ui" ())
(declare-function clutch--ensure-table-comment-async "clutch-schema" (conn table))
(declare-function clutch--refresh-footer-cursor "clutch-ui" ())
(declare-function clutch--refresh-footer-line "clutch-ui" ())
(declare-function clutch--refresh-header-line "clutch-ui" ())
(declare-function clutch--replace-row-at-index "clutch-ui" (ridx))
(declare-function clutch--column-border-position "clutch-ui" (cidx))
(declare-function clutch--column-info-string "clutch-ui" (cidx))
(declare-function clutch--resolve-result-column-details "clutch-ui" (conn sql col-names))

;;;; Customization

(defgroup clutch nil
  "Interactive database lens."
  :group 'comm
  :prefix "clutch-")

(defface clutch-header-face
  '((t :inherit bold))
  "Face for column headers in result tables."
  :group 'clutch)

(defface clutch-insert-field-name-face
  '((t :inherit clutch-header-face :foreground "#b8d7ec"))
  "Face for `insert-buffer' field names."
  :group 'clutch)

(defface clutch-insert-field-tag-face
  '((t :inherit shadow))
  "Face for metadata tags in the insert buffer."
  :group 'clutch)

(defface clutch-insert-field-error-face
  '((((class color) (background light))
     :underline (:color "#b91c1c" :style wave))
    (((class color) (background dark))
     :underline (:color "#fca5a5" :style wave))
    (t :inherit error))
  "Face for invalid values in the insert buffer."
  :group 'clutch)

(defface clutch-insert-inline-error-face
  '((t :inherit error))
  "Face for inline `insert-buffer' validation messages."
  :group 'clutch)

(defface clutch-insert-active-field-face
  '((t :inherit hl-line))
  "Face for the active line in the insert buffer."
  :group 'clutch)

(defface clutch-insert-active-field-name-face
  '((t :inherit (clutch-header-active-face clutch-insert-field-name-face)))
  "Face for the active `insert-buffer' field prefix."
  :group 'clutch)

(defface clutch-header-active-face
  '((t :inherit hl-line :weight bold))
  "Face for the column header under the cursor."
  :group 'clutch)

(defface clutch-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'clutch)

(defface clutch-object-source-face
  '((t :inherit shadow))
  "Face for object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-public-source-face
  '((t :inherit shadow))
  "Face for PUBLIC object-source annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-object-type-face
  '((t :inherit shadow))
  "Face for object-type annotations in minibuffer completions."
  :group 'clutch)

(defface clutch-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'clutch)

(defconst clutch--cell-generated-placeholder :clutch-generated-placeholder
  "Sentinel for values generated by the database at insert time.")

(defconst clutch--cell-default-placeholder :clutch-default-placeholder
  "Sentinel for values populated by database defaults at insert time.")

(defface clutch-modified-face
  '((((class color) (background light))
     :inherit warning :background "#fff3cd")
    (((class color) (background dark))
     :inherit warning :background "#3d2b00")
    (t :inherit warning))
  "Face for pending-edit cell values."
  :group 'clutch)

(defface clutch-fk-face
  '((t :inherit font-lock-type-face :underline t))
  "Face for foreign key column values.
Underlined to indicate clickable (RET to follow)."
  :group 'clutch)

(defface clutch-marked-face
  '((t :inherit dired-marked))
  "Face for marked rows in result buffer."
  :group 'clutch)

(defface clutch-executed-sql-marker-face
  '((t :inherit success))
  "Face for the executed SQL gutter marker."
  :group 'clutch)

(define-fringe-bitmap 'clutch-executed-sql-dot
  [0 0 24 60 126 126 60 24]
  nil nil 'center)

(defface clutch-pending-delete-face
  '((((class color) (background light))
     :background "#fde8e8" :foreground "#9b1c1c" :strike-through t)
    (((class color) (background dark))
     :background "#3b1212" :foreground "#fca5a5" :strike-through t)
    (t :strike-through t))
  "Face for rows staged for deletion."
  :group 'clutch)

(defface clutch-pending-insert-face
  '((((class color) (background light))
     :background "#e6f4ea" :foreground "#1e4620")
    (((class color) (background dark))
     :background "#1a3320" :foreground "#86efac")
    (t :inherit success))
  "Face for rows staged for insertion."
  :group 'clutch)

(defface clutch-error-position-face
  '((((class color) (background light))
     :background "#fde8e8" :underline (:color "red" :style wave))
    (((class color) (background dark))
     :background "#3b1212" :underline (:color "#fca5a5" :style wave))
    (t :underline t))
  "Face for the character at the SQL error position."
  :group 'clutch)

(defface clutch-error-banner-face
  '((((class color) (background light))
     :background "#fee2e2" :foreground "#991b1b" :extend t)
    (((class color) (background dark))
     :background "#451a1a" :foreground "#fecaca" :extend t)
    (t :inherit error))
  "Face for the inline SQL execution error banner."
  :group 'clutch)

(defcustom clutch-connection-alist nil
  "Alist of saved database connections.
Each entry has the form:
  (NAME . (:host H :port P :user U [:password P] :database D
           [:backend SYM] [:sql-product SYM] [:pass-entry STR]
           [:url STR] [:display-name STR] [:props ALIST]
           [:tls BOOLEAN] [:ssl-mode disabled] [:sslmode require]
           [:connect-timeout N] [:read-idle-timeout N]
           [:query-timeout N] [:rpc-timeout N]))
NAME is a string used for `completing-read'.
:backend is required and names the backend symbol (\\='mysql, \\='pg,
\\='sqlite, or a JDBC backend such as \\='oracle).
:sql-product overrides `clutch-sql-product' for this connection.
:tls is a convenience shortcut for backend TLS defaults.  For MySQL,
an explicit `:tls nil' forces plaintext and suppresses the automatic
MySQL 8 TLS retry path; for PostgreSQL, `:tls t' maps to `:sslmode require'
and `:tls nil' maps to `:sslmode disable'.
:ssl-mode is currently MySQL-only; `disabled' is a compatibility spelling for
the same explicit plaintext mode.  The older alias `off' is also accepted.
:sslmode is PostgreSQL-only and follows the upstream naming.  Supported values
are `disable', `prefer', `require', and `verify-full'.

Password resolution order:
  1. :password — used as-is when present.
  2. Pass store by connection name — when `auth-source-pass' is loaded,
     clutch automatically looks up a pass entry whose name matches NAME
     (the car of this alist entry).  The password is on the first line.
     Use :pass-entry STR to override the entry name if it differs.
  3. `auth-source-search' — searches ~/.authinfo / ~/.authinfo.gpg / pass
     by :host, :user, and :port (standard auth-source matching)."
  :type '(alist :key-type string
                :value-type (plist :options
                                   ((:host string)
                                    (:port integer)
                                    (:user string)
                                    (:password string)
                                    (:database string)
                                    (:backend symbol)
                                    (:sql-product symbol)
                                    (:pass-entry string)
                                    (:url string)
                                    (:display-name string)
                                    (:props (alist :key-type string :value-type string))
                                    (:ssl-mode (choice (const :tag "Disabled" disabled)
                                                       (const :tag "Off (alias)" off)))
                                    (:sslmode (choice (const :tag "Disable" disable)
                                                      (const :tag "Prefer" prefer)
                                                      (const :tag "Require" require)
                                                      (const :tag "Verify Full" verify-full)))
                                    (:connect-timeout natnum)
                                    (:read-idle-timeout natnum)
                                    (:query-timeout natnum)
                                    (:rpc-timeout natnum)
                                    (:tls boolean))))
  :group 'clutch)

(defcustom clutch-insert-validation-idle-delay 0.2
  "Idle seconds before validating heavier insert fields such as JSON."
  :type 'number
  :group 'clutch)

(defcustom clutch-console-directory
  (expand-file-name "clutch" user-emacs-directory)
  "Directory for persisting query console buffer content."
  :type 'directory
  :group 'clutch)

(defcustom clutch-result-window-height 0.33
  "Height of the result window as a fraction of the frame height.
A float between 0.0 and 1.0.  Only applies when creating a new result
window; an existing result window is reused at its current height."
  :type 'float
  :group 'clutch)

(defcustom clutch-result-max-rows 500
  "Maximum number of rows to display in result tables."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-width-max 30
  "Maximum display width for a single column in the result table."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-width-step 5
  "Step size for widening/narrowing columns with +/-."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-column-padding 1
  "Number of padding spaces on each side of a cell."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-sql-product 'mysql
  "SQL product used for syntax highlighting.
Must be a symbol recognized by `sql-mode' (e.g. mysql, postgres)."
  :type '(choice (const :tag "MySQL" mysql)
                 (const :tag "PostgreSQL" postgres)
                 (const :tag "MariaDB" mariadb)
                 (symbol :tag "Other"))
  :group 'clutch)

(defcustom clutch-connect-timeout-seconds 10
  "Timeout in seconds for establishing a database connection.
Applies to networked backends.  SQLite ignores this setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-read-idle-timeout-seconds 30
  "Idle timeout in seconds while waiting for query I/O.
Applies to MySQL, PostgreSQL, and JDBC network I/O.  SQLite ignores this
setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-query-timeout-seconds 30
  "Timeout in seconds for database-side query execution.
Currently applied by the JDBC backend.  Native MySQL/PostgreSQL backends do
not yet enforce a server-side statement timeout."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-jdbc-rpc-timeout-seconds 30
  "Timeout in seconds for round-trips to the JDBC agent process."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-object-warmup-idle-delay-seconds 0.5
  "Idle delay before warming non-table object metadata.
A small non-zero delay keeps connect and initial UI painting responsive before
background object discovery starts."
  :type 'number
  :group 'clutch)

(defcustom clutch-primary-object-types '("TABLE" "VIEW" "SYNONYM")
  "Object types preferred by clutch's primary object entrypoint.
When nil, the primary entrypoint includes all schema object types."
  :type '(repeat string)
  :group 'clutch)

(defcustom clutch-sql-completion-case-style 'preserve
  "How SQL completion inserts keywords and identifiers.
`preserve' keeps backend-provided identifier case and uppercase SQL keywords.
`lower' inserts lowercase keywords and lowercases completion identifiers.
`upper' inserts uppercase keywords and uppercases completion identifiers."
  :type '(choice (const :tag "Preserve backend / default keyword case" preserve)
                 (const :tag "Lowercase keywords and identifiers" lower)
                 (const :tag "Uppercase keywords and identifiers" upper))
  :group 'clutch)

(defcustom clutch-schema-cache-install-batch-size 500
  "Maximum number of schema entries to install per idle slice.
Large schema snapshots are installed incrementally to keep Emacs responsive
after async metadata refreshes."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-csv-export-default-coding-system 'utf-8-with-signature
  "Default coding system when exporting CSV files."
  :type '(choice (const :tag "UTF-8 (with BOM)" utf-8-with-signature)
                 (const :tag "UTF-8" utf-8)
                 (const :tag "GBK" gbk)
                 (coding-system :tag "Other coding system"))
  :group 'clutch)

(defcustom clutch-debug-event-limit 25
  "Maximum number of recent debug events kept per buffer or connection.
Only recorded while `clutch-debug-mode' is enabled."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-console-yank-cleanup t
  "When non-nil, clean whitespace in pasted text in query consoles.
After `yank', `yank-pop', or `clipboard-yank' in a query console buffer,
trailing whitespace, mixed indentation, and CRLF line endings are
cleaned up in the pasted region only."
  :type 'boolean
  :group 'clutch)

(defconst clutch-debug-buffer-name "*clutch-debug*"
  "Name of the dedicated clutch debug buffer.")

;;;; Buffer-local variables

(defvar-local clutch-connection nil
  "Current database connection for this buffer.")

(defvar-local clutch--buffer-error-details nil
  "Current problem record scoped to this buffer.")

(defvar clutch--problem-records-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Current problem records keyed by live connection object.")

(defvar-local clutch--executing-p nil
  "Non-nil while a query is executing in this buffer.
Used to update the mode-line with a spinner during execution.")

(defvar-local clutch--error-position-overlay nil
  "Overlay marking the error position in the last failed query, or nil.")

(defvar-local clutch--error-banner-overlay nil
  "Overlay showing the last SQL execution error banner, or nil.")

(defvar-local clutch--tables-in-buffer-cache nil
  "Cached result for `clutch--tables-in-buffer' in the current buffer.")

(defvar-local clutch--tables-in-query-cache nil
  "Cached result for `clutch--tables-in-query' in the current buffer.")

(defvar-local clutch--row-start-positions nil
  "Vector mapping rendered row indices to their line start positions.")

(defconst clutch--schema-inline-table-limit 3
  "Maximum number of statement tables for synchronous schema hints.")

(defconst clutch--schema-inline-min-prefix-length 2
  "Minimum symbol prefix length before loading column hints synchronously.")

(defvar clutch--source-window nil
  "Window that initiated the current query execution.
Dynamically bound by `clutch--execute' so result buffers open
adjacent to the correct console window.")

(defvar clutch--executing-sql-start nil
  "Buffer position where the currently executing SQL begins, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(defvar clutch--executing-sql-end nil
  "Buffer position where the currently executing SQL ends, or nil.
Dynamically bound by `clutch--execute-and-mark'.")

(defvar-local clutch--conn-sql-product nil
  "SQL product for the current connection, or nil to use the default.")

(defvar clutch--oracle-i18n-warning-shown nil
  "Non-nil after showing the Oracle orai18n completion warning once.")

(defvar clutch--completion-metadata-warning-cache (make-hash-table :test 'equal)
  "Completion metadata errors already surfaced in this session.")

(defvar-local clutch--last-query nil
  "Last executed SQL query string.")

(defvar-local clutch--live-view-buffer nil
  "Live value viewer buffer attached to the current source buffer, or nil.")

(defvar-local clutch--debug-events nil
  "Recent redacted debug events captured for this buffer.")

(defvar clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key)
  "Recent redacted debug events keyed by live connection object.")

(defvar clutch--debug-buffer-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "q") #'quit-window)
    map)
  "Keymap for `clutch--debug-buffer-mode'.")

(define-derived-mode clutch--debug-buffer-mode special-mode "clutch-debug"
  "Mode for inspecting the dedicated clutch debug buffer.")

(defun clutch--debug-buffer ()
  "Return the dedicated clutch debug buffer, creating it if needed."
  (let ((buf (get-buffer-create clutch-debug-buffer-name)))
    (with-current-buffer buf
      (unless (derived-mode-p 'clutch--debug-buffer-mode)
        (clutch--debug-buffer-mode))
      (setq-local header-line-format " Clutch debug capture"))
    buf))

(defun clutch--reset-debug-buffer ()
  "Reset the dedicated clutch debug buffer for a new capture window."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (format "Clutch Debug\n============\nStarted: %s\n"
                      (format-time-string "%Y-%m-%d %H:%M:%S"))))))

(defun clutch--debug-buffer-source-label (buffer)
  "Return a human-readable source label for BUFFER."
  (when (buffer-live-p buffer)
    (buffer-name buffer)))

(defun clutch--debug-buffer-connection-label (connection)
  "Return a human-readable connection label for CONNECTION."
  (when connection
    (condition-case nil
        (clutch--connection-key connection)
      (error nil))))

(defun clutch--debug-format-label (key)
  "Return a human-readable label for KEY."
  (capitalize
   (replace-regexp-in-string
    "-" " "
    (string-remove-prefix ":" (format "%s" key)))))

(defun clutch--debug-indent-block (text spaces)
  "Indent TEXT by SPACES."
  (let ((prefix (make-string spaces ?\s)))
    (string-join
     (mapcar (lambda (line)
               (if (string-empty-p line)
                   line
                 (concat prefix line)))
             (split-string (string-trim-right text) "\n"))
     "\n")))

(defun clutch--debug-format-plist-data (plist)
  "Return a human-readable string for PLIST."
  (with-temp-buffer
    (cl-loop for (key val) on plist by #'cddr
             when val
             do (let ((rendered (clutch--debug-format-data val)))
                  (when rendered
                    (if (string-match-p "\n" rendered)
                        (insert (format "%s:\n%s\n"
                                        (clutch--debug-format-label key)
                                        (clutch--debug-indent-block rendered 2)))
                      (insert (format "%s: %s\n"
                                      (clutch--debug-format-label key)
                                      rendered))))))
    (string-trim-right (buffer-string))))

(defun clutch--debug-format-list-data (items)
  "Return a human-readable string for ITEMS."
  (string-join
   (mapcar
    (lambda (item)
      (let ((rendered (clutch--debug-format-data item)))
        (if (string-match-p "\n" rendered)
            (concat "-\n" (clutch--debug-indent-block rendered 2))
          (concat "- " rendered))))
    items)
   "\n"))

(defun clutch--debug-format-data (data)
  "Return a human-readable string for DATA."
  (cond
   ((null data) nil)
   ((stringp data) data)
   ((vectorp data) (clutch--debug-format-data (append data nil)))
   ((and (listp data) (keywordp (car-safe data)))
    (clutch--debug-format-plist-data data))
   ((listp data)
    (clutch--debug-format-list-data data))
   (t (format "%s" data))))

(defun clutch--debug-insert-field (label value)
  "Insert LABEL and VALUE into the current debug output buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (if (string-match-p "\n" rendered)
        (insert (format "%s:\n%s\n"
                        label
                        (clutch--debug-indent-block rendered 2)))
      (insert (format "%s: %s\n" label rendered)))))

(defun clutch--debug-insert-section (title value)
  "Insert a TITLE section containing VALUE into the current debug buffer."
  (when-let* ((rendered (clutch--debug-format-data value)))
    (insert "\n" title "\n")
    (insert (clutch--debug-indent-block rendered 2) "\n")))

(defun clutch--debug-context-without-inline-sql (context)
  "Return CONTEXT plist without inline SQL payload entries."
  (when context
    (cl-loop for (key val) on context by #'cddr
             unless (memq key '(:generated-sql :sql))
             append (list key val))))

(defun clutch--append-debug-buffer-entry (heading body)
  "Append HEADING and BODY to the dedicated debug buffer."
  (with-current-buffer (clutch--debug-buffer)
    (let ((inhibit-read-only t))
      (goto-char (point-max))
      (unless (bobp)
        (insert "\n\n"))
      (insert heading "\n")
      (insert (make-string (length heading) ?-) "\n")
      (when body
        (insert body))
      (unless (or (bobp) (eq (char-before) ?\n))
        (insert "\n")))))

(defun clutch--append-problem-record-to-debug-buffer (buffer connection problem)
  "Append PROBLEM for BUFFER and CONNECTION to the dedicated debug buffer."
  (when (and clutch-debug-mode problem)
    (let* ((backend (plist-get problem :backend))
           (diag (plist-get problem :diag))
           (debug-payload (plist-get problem :debug))
           (stderr-tail (plist-get problem :stderr-tail))
           (context (copy-tree (plist-get diag :context)))
           (sql (plist-get context :sql))
           (generated-sql (plist-get context :generated-sql))
           (display-context
            (clutch--debug-context-without-inline-sql context))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (format-time-string "%Y-%m-%d %H:%M:%S"))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (clutch--debug-insert-field "Summary"
                                          (plist-get problem :summary))
              (clutch--debug-insert-field "Category" (plist-get diag :category))
              (clutch--debug-insert-field "Operation" (plist-get diag :op))
              (clutch--debug-insert-field "Request ID" (plist-get diag :request-id))
              (clutch--debug-insert-field "Conn ID" (plist-get diag :conn-id))
              (clutch--debug-insert-field "Exception"
                                          (plist-get diag :exception-class))
              (clutch--debug-insert-field "SQLState" (plist-get diag :sql-state))
              (clutch--debug-insert-field "Vendor code" (plist-get diag :vendor-code))
              (clutch--debug-insert-field "Raw message"
                                          (plist-get diag :raw-message))
              (clutch--debug-insert-section "SQL" sql)
              (clutch--debug-insert-section "Generated SQL" generated-sql)
              (clutch--debug-insert-section "Context" display-context)
              (clutch--debug-insert-section "Cause chain"
                                            (plist-get diag :cause-chain))
              (clutch--debug-insert-section "Backend debug" debug-payload)
              (clutch--debug-insert-section "Agent stderr tail" stderr-tail)
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Problem" body))))

(defun clutch--append-debug-event-to-buffer (buffer connection event)
  "Append EVENT for BUFFER and CONNECTION to the dedicated debug buffer."
  (when clutch-debug-mode
    (let* ((backend (plist-get event :backend))
           (body
            (with-temp-buffer
              (clutch--debug-insert-field "Recorded"
                                          (or (plist-get event :time)
                                              (format-time-string "%Y-%m-%d %H:%M:%S")))
              (clutch--debug-insert-field "Operation" (plist-get event :op))
              (clutch--debug-insert-field "Phase" (plist-get event :phase))
              (clutch--debug-insert-field "Backend"
                                          (and backend
                                               (upcase (symbol-name backend))))
              (clutch--debug-insert-field "Source"
                                          (clutch--debug-buffer-source-label buffer))
              (clutch--debug-insert-field "Connection"
                                          (clutch--debug-buffer-connection-label connection))
              (when-let* ((elapsed (plist-get event :elapsed)))
                (clutch--debug-insert-field "Elapsed"
                                            (clutch--format-elapsed elapsed)))
              (clutch--debug-insert-field "Summary" (plist-get event :summary))
              (clutch--debug-insert-field "SQL preview"
                                          (plist-get event :sql-preview))
              (clutch--debug-insert-section "Context"
                                            (plist-get event :context))
              (string-trim-right (buffer-string)))))
      (clutch--append-debug-buffer-entry "Trace Event" body))))

(defun clutch--clear-problem-capture ()
  "Forget all captured problem records across buffers and connections."
  (let (connections)
    (setq clutch--problem-records-by-conn
          (make-hash-table :test 'eq :weakness 'key))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when clutch-connection
            (push clutch-connection connections))
          (setq-local clutch--buffer-error-details nil))))
    (dolist (conn (cl-delete-duplicates connections :test #'eq))
      (clutch-db-clear-error-details conn))))

(defun clutch--clear-debug-capture ()
  "Forget captured debug events and reset the dedicated debug buffer."
  (setq clutch--debug-events-by-conn (make-hash-table :test 'eq :weakness 'key))
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (setq-local clutch--debug-events nil))))
  (clutch--reset-debug-buffer))

(defun clutch--replay-problem-records-to-debug-buffer ()
  "Replay stored problem records into the dedicated debug buffer.
This preserves historical failure context when debug capture starts after a
problem was already recorded."
  (let (records seen)
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when clutch--buffer-error-details
            (let ((entry (list :buffer buf
                               :connection clutch-connection
                               :problem (copy-tree clutch--buffer-error-details))))
              (unless (member entry seen)
                (push entry seen)
                (push entry records)))))))
    (maphash
     (lambda (connection problem)
       (let ((entry (list :buffer (clutch--attached-buffer-for-connection connection)
                          :connection connection
                          :problem (copy-tree problem))))
         (unless (member entry seen)
           (push entry seen)
           (push entry records))))
    clutch--problem-records-by-conn)
    (when records
      (clutch--append-debug-buffer-entry
       "Historical Problems"
       "Recorded before debug mode was enabled.")
      (dolist (entry (nreverse records))
        (clutch--append-problem-record-to-debug-buffer
         (plist-get entry :buffer)
         (plist-get entry :connection)
         (plist-get entry :problem))))))

;;;###autoload
(define-minor-mode clutch-debug-mode
  "Capture additional redacted troubleshooting data for clutch workflows.
When enabled, clutch records a bounded recent-event trace per buffer and per
connection.  JDBC requests also ask the agent for an optional debug payload,
and captured output is appended to the dedicated `*clutch-debug*' buffer."
  :global t
  :lighter " ClutchDbg"
  (when clutch-debug-mode
    (clutch--clear-debug-capture)
    (clutch--replay-problem-records-to-debug-buffer)))

(defun clutch--remember-problem-record (&rest args)
  "Store the current problem record described by ARGS.
Recognized keys are :buffer, :connection, and :problem.  Problem records are
stored buffer-locally and, when CONNECTION is non-nil, in the shared
connection-scoped registry."
  (let* ((buffer (or (plist-get args :buffer) (current-buffer)))
         (connection (plist-get args :connection))
         (problem (copy-tree (plist-get args :problem))))
    (when (and buffer (buffer-live-p buffer))
      (with-current-buffer buffer
        (setq-local clutch--buffer-error-details problem)))
    (when connection
      (if problem
          (puthash connection problem clutch--problem-records-by-conn)
        (remhash connection clutch--problem-records-by-conn)))
    (when problem
      (clutch--append-problem-record-to-debug-buffer buffer connection problem))
    problem))

(defun clutch--forget-problem-record (&optional buffer connection)
  "Forget the current problem record for BUFFER and CONNECTION."
  (when (and buffer (buffer-live-p buffer))
    (with-current-buffer buffer
      (setq-local clutch--buffer-error-details nil)))
  (when connection
    (remhash connection clutch--problem-records-by-conn)))

(defun clutch--forget-problem-records-for-connection (connection)
  "Forget problem records for CONNECTION across all attached buffers."
  (when connection
    (remhash connection clutch--problem-records-by-conn)
    (dolist (buf (buffer-list))
      (when (and (buffer-live-p buf)
                 (eq (buffer-local-value 'clutch-connection buf) connection))
        (with-current-buffer buf
          (setq-local clutch--buffer-error-details nil))))))

(defun clutch--clear-connection-problem-capture (connection)
  "Forget problem records and backend-local diagnostics for CONNECTION."
  (when connection
    (clutch--forget-problem-records-for-connection connection)
    (clutch-db-clear-error-details connection)))

(defun clutch--problem-record-for-connection (connection)
  "Return the current problem record for CONNECTION, or nil."
  (when connection
    (copy-tree (gethash connection clutch--problem-records-by-conn))))

(defun clutch--attached-buffer-for-connection (connection)
  "Return one live buffer attached to CONNECTION, or nil."
  (when connection
    (or (and (eq clutch-connection connection)
             (current-buffer))
        (cl-loop for buf in (buffer-list)
                 when (and (buffer-live-p buf)
                           (eq (buffer-local-value 'clutch-connection buf) connection))
                 return buf))))

(defun clutch--debug-sql-preview (sql)
  "Return a compact single-line preview of SQL."
  (when sql
    (truncate-string-to-width
     (replace-regexp-in-string "[\n\r\t ]+" " " (string-trim sql))
     160 0 nil "...")))

(defun clutch--debug-trim-events (events)
  "Return EVENTS truncated to `clutch-debug-event-limit'."
  (let ((limit (max 1 clutch-debug-event-limit)))
    (cl-subseq events 0 (min limit (length events)))))

(defun clutch--normalize-debug-event (event)
  "Return EVENT normalized for storage."
  (let ((normalized (copy-tree event)))
    (unless (plist-get normalized :time)
      (setq normalized
            (plist-put normalized :time
                       (format-time-string "%Y-%m-%d %H:%M:%S"))))
    (when-let* ((sql (plist-get normalized :sql)))
      (setq normalized (plist-put normalized :sql-preview
                                  (clutch--debug-sql-preview sql)))
      (setq normalized (plist-put normalized :sql-length (length sql)))
      (cl-remf normalized :sql))
    normalized))

(defun clutch--remember-debug-event (&rest event)
  "Record EVENT for the current buffer and optional connection.
Recognized keys include :buffer, :connection, :op, :phase, :summary, :sql,
:backend, :context, and :elapsed.  Recording is disabled unless
`clutch-debug-mode' is non-nil."
  (when clutch-debug-mode
    (let* ((buffer (or (plist-get event :buffer) (current-buffer)))
           (conn (plist-get event :connection))
           (normalized (clutch--normalize-debug-event event)))
      (cl-remf normalized :buffer)
      (cl-remf normalized :connection)
      (when (buffer-live-p buffer)
        (with-current-buffer buffer
          (setq-local clutch--debug-events
                      (clutch--debug-trim-events
                       (cons normalized clutch--debug-events)))))
      (when conn
        (puthash conn
                 (clutch--debug-trim-events
                  (cons normalized (gethash conn clutch--debug-events-by-conn)))
                 clutch--debug-events-by-conn))
      (clutch--append-debug-event-to-buffer buffer conn normalized)
      normalized)))

(defun clutch--effective-sql-product (params)
  "Return the SQL product to use for connection PARAMS."
  (or (plist-get params :sql-product)
      (pcase (plist-get params :backend)
        ((or 'pg 'postgresql) 'postgres)
        ((or 'mysql 'mariadb) 'mysql)
        ('sqlite 'sqlite)
        ('oracle 'oracle)
        (_ nil))))

(defun clutch--oracle-i18n-missing-p (err)
  "Return non-nil when ERR indicates Oracle needs orai18n.jar."
  (string-match-p
   "orai18n\\.jar\\|Non supported character set"
   (error-message-string err)))

(defun clutch--warn-oracle-i18n-once ()
  "Warn once that Oracle completion needs orai18n.jar for this session."
  (unless clutch--oracle-i18n-warning-shown
    (setq clutch--oracle-i18n-warning-shown t)
    (message (concat "Oracle completion needs orai18n.jar for this character set. "
                     "Run M-x clutch-jdbc-install-driver RET oracle "
                     "(or oracle-i18n if you already manage ojdbc manually)."))))

(defun clutch--warn-completion-metadata-error-once (message-text)
  "Warn once that completion metadata is unavailable with MESSAGE-TEXT."
  (unless (gethash message-text clutch--completion-metadata-warning-cache)
    (puthash message-text t clutch--completion-metadata-warning-cache)
    (message "Completion metadata unavailable: %s" message-text)))

(defun clutch--remember-recoverable-metadata-warning (connection op err &optional context)
  "Record a recoverable metadata warning for CONNECTION and operation OP.
ERR is the original condition object.  Optional CONTEXT is attached to the
debug event when `clutch-debug-mode' is enabled."
  (when clutch-debug-mode
    (let* ((buffer (or (clutch--attached-buffer-for-connection connection)
                       (current-buffer)))
           (summary (clutch--humanize-db-error (error-message-string err)))
           (backend (and connection
                         (condition-case nil
                             (clutch--backend-key-from-conn connection)
                           (error nil)))))
      (clutch--remember-debug-event
       :buffer buffer
       :connection connection
       :op op
       :phase "warning"
       :backend backend
       :summary summary
       :context context))))

(defun clutch--safe-completion-call (thunk)
  "Call THUNK for completion and swallow recoverable metadata errors."
  (condition-case err
      (funcall thunk)
    (clutch-db-error
     (clutch--remember-recoverable-metadata-warning
      clutch-connection "completion" err)
     (if (clutch--oracle-i18n-missing-p err)
         (clutch--warn-oracle-i18n-once)
       (clutch--warn-completion-metadata-error-once
        (error-message-string err)))
     nil)))

(defvar clutch--aggregate-summary)
(defvar clutch--column-widths)
(defvar clutch--filter-pattern)
(defvar clutch--fk-info)
(defvar clutch--filtered-rows)
(defvar clutch--header-active-col)
(defvar clutch--marked-rows)
(defvar clutch--order-by)
(defvar clutch--page-current)
(defvar clutch--page-total-rows)
(defvar clutch--pending-deletes)
(defvar clutch--pending-edits)
(defvar clutch--pending-inserts)
(defvar clutch--query-elapsed)
(defvar clutch--refine-callback)
(defvar clutch--refine-excluded-cols)
(defvar clutch--refine-excluded-rows)
(defvar clutch--refine-overlays)
(defvar clutch--refine-rect)
(defvar clutch--refine-saved-mode-line)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-rows)
(defvar clutch--sql-keywords)
(defvar clutch--sort-column)
(defvar clutch--sort-descending)
(defvar clutch--where-filter)
(defvar clutch-record--expanded-fields)
(defvar clutch-record--result-buffer)
(defvar clutch-record--row-idx)
(defvar-local clutch--live-view-source-buffer nil
  "Source buffer currently driving this live viewer.")
(defvar-local clutch--live-view-frozen nil
  "Non-nil when this live viewer is frozen in place.")
(defvar-local clutch--live-view-source-cell-id nil
  "Identifier for the source cell currently rendered in this viewer.")

(defvar-local clutch--base-query nil
  "The original unfiltered SQL query, used by WHERE filtering.")

(defvar-local clutch--connection-params nil
  "Params plist used to establish the current connection.
Stored at connect time so the connection can be re-established
automatically when it drops.")

(defvar-local clutch--console-name nil
  "Connection name if this buffer is a query console, nil otherwise.
Set by `clutch-query-console'; used to save/restore buffer content.")

(defun clutch--console-buffer-base-name (name)
  "Return canonical buffer name for console NAME."
  (format "*clutch: %s*" name))

(defun clutch--console-buffer-name (&optional name conn)
  "Return display buffer name for console NAME and schema state on CONN."
  (let* ((console-name (or name clutch--console-name "?"))
         (base (clutch--console-buffer-base-name console-name))
         (entry (and conn (clutch--schema-status-entry conn)))
         (state (plist-get entry :state))
         (tables (plist-get entry :tables))
         (suffix (pcase state
                   ('refreshing " [schema...]")
                   ('stale " [schema~]")
                   ('failed " [schema!]")
                   ('ready (format " [schema %dt]" (or tables 0)))
                   (_ ""))))
    (concat base suffix)))

(defun clutch--find-console-buffer (name)
  "Return the live console buffer for NAME, or nil."
  (cl-find-if
   (lambda (buf)
     (and (buffer-live-p buf)
          (with-current-buffer buf
            (and (eq major-mode 'clutch-mode)
                 (equal clutch--console-name name)))))
   (buffer-list)))

(defun clutch--update-console-buffer-name ()
  "Rename the current console buffer to reflect schema status."
  (when clutch--console-name
    (rename-buffer (clutch--console-buffer-name clutch--console-name clutch-connection)
                   t)))

(defun clutch--refresh-current-schema (&optional quiet force-sync)
  "Refresh schema for the current connection and report the outcome.
When QUIET is non-nil, do not emit a minibuffer message.
When FORCE-SYNC is non-nil, bypass any background refresh path.
Returns non-nil on success, nil on failure."
  (clutch--ensure-connection)
  (let* ((conn clutch-connection)
         (entry (clutch--schema-status-entry conn)))
    (if (eq (plist-get entry :state) 'refreshing)
        (progn
          (unless quiet
            (message "Schema refresh already in progress"))
          nil)
      (if (or force-sync
              (clutch-db-eager-schema-refresh-p conn))
          (let* ((ok (clutch--refresh-schema-cache conn))
                 (entry (clutch--schema-status-entry conn))
                 (tables (plist-get entry :tables))
                 (err (plist-get entry :error)))
            (unless quiet
              (message (if ok
                           (format "Schema refreshed%s"
                                   (if tables (format " (%d tables)" tables) ""))
                         (format "Schema refresh failed%s"
                                 (if err (format ": %s" err) "")))))
            ok)
        (let ((started (clutch--refresh-schema-cache-async conn)))
          (if started
              (progn
                (unless quiet
                  (message "Schema refresh started in background"))
                t)
            (let* ((ok (clutch--refresh-schema-cache conn))
                   (entry (clutch--schema-status-entry conn))
                   (tables (plist-get entry :tables))
                   (err (plist-get entry :error)))
              (unless quiet
                (message (if ok
                             (format "Schema refreshed%s"
                                     (if tables (format " (%d tables)" tables) ""))
                           (format "Schema refresh failed%s"
                                   (if err (format ": %s" err) "")))))
              ok)))))))

(defun clutch--schema-cache-guidance (conn)
  "Return a recovery hint for CONN schema cache state, or nil."
  (when-let* ((entry (clutch--schema-status-entry conn))
              (state (plist-get entry :state)))
    (pcase state
      ('stale
       "Schema cache is stale — press C-c C-s before relying on cached table names or object discovery metadata")
      ('failed
       "Schema refresh failed earlier — press C-c C-s to retry before relying on cached table names or object discovery metadata")
      ('refreshing
       "Schema refresh is in progress — cached table names or object discovery metadata may still be behind")
      (_ nil))))

(defun clutch--warn-schema-cache-state (&optional conn)
  "Show a recovery hint when CONN schema cache is not ready."
  (when-let* ((hint (clutch--schema-cache-guidance (or conn clutch-connection))))
    (message "%s" hint)))

;;;; Console persistence

(defun clutch--console-file (name)
  "Return the persistence file path for console NAME."
  (expand-file-name
   (concat (replace-regexp-in-string "[/:\\*?\"<>|]" "_" name) ".sql")
   clutch-console-directory))

(defun clutch--save-console ()
  "Save console buffer content to its persistence file."
  (when clutch--console-name
    (condition-case err
        (progn
          (make-directory clutch-console-directory t)
          (let ((coding-system-for-write 'utf-8-unix))
            (write-region (point-min) (point-max)
                          (clutch--console-file clutch--console-name)
                          nil 'silent)))
      (error
       (message "Failed to save console %s: %s"
                clutch--console-name
                (error-message-string err))))))

(defun clutch--save-all-consoles ()
  "Save content of all open query console buffers.
Run from `kill-emacs-hook' to persist consoles on Emacs exit."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (clutch--save-console))))

;;;; Schema cache + completion

;; NOTE: clutch--connection-key, clutch--connection-alive-p, etc. are now
;; in clutch-connection.el.  Query execution, error handling, value
;; formatting, and indirect edit are now in clutch-query.el.

(defun clutch--refresh-result-status-line ()
  "Refresh the result buffer status line without rebuilding the table body."
  (when (derived-mode-p 'clutch-result-mode)
    (let* ((rows (or clutch--filtered-rows clutch--result-rows))
           (visible-cols (clutch--visible-columns))
           (widths (clutch--effective-widths))
           (nw (clutch--row-number-digits)))
      (clutch--update-result-line-formats rows visible-cols widths nw))))

(defun clutch--refresh-schema-status-ui (conn)
  "Refresh mode-line or status line in buffers attached to CONN."
  (when conn
    (let ((key (clutch--connection-key conn)))
      (dolist (buf (buffer-list))
        (when (buffer-live-p buf)
          (with-current-buffer buf
            (when (and clutch-connection
                       (string= (clutch--connection-key clutch-connection) key))
              (cond
               ((derived-mode-p 'clutch-describe-mode)
                (when clutch--describe-object-entry
                  (clutch--render-object-describe clutch-connection
                                                 clutch--describe-object-entry
                                                 clutch--connection-params
                                                 clutch--conn-sql-product)))
               ((derived-mode-p 'clutch-result-mode)
                (clutch--refresh-result-status-line)
                (clutch--update-position-indicator))
               ((derived-mode-p 'clutch-mode)
                (clutch--update-console-buffer-name)
                (clutch--update-mode-line))))))))))

(defun clutch--set-schema-status (conn state &optional table-count error-message)
  "Record schema STATE for CONN and refresh connected UI.
TABLE-COUNT is the number of known tables when STATE is \\='ready.
ERROR-MESSAGE is stored when STATE is \\='failed."
  (when conn
    (puthash (clutch--connection-key conn)
             (list :state state
                   :tables table-count
                   :error error-message)
             clutch--schema-status-cache)
    (clutch--refresh-schema-status-ui conn)))

;;;###autoload
(defun clutch-refresh-schema ()
  "Refresh the schema cache for the current connection.
Useful after DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
executed outside clutch that would otherwise leave stale completions."
  (interactive)
  (clutch--refresh-current-schema nil t))

(defun clutch--update-connection-params-for-buffers (conn update-fn)
  "Apply UPDATE-FN to buffer-local connection params for buffers attached to CONN."
  (dolist (buf (buffer-list))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (when (eq clutch-connection conn)
          (setq-local clutch--connection-params
                      (funcall update-fn clutch--connection-params))
          (clutch--update-mode-line))))))

(defun clutch--list-clickhouse-databases (conn)
  "Return sorted database names visible to ClickHouse CONN."
  (sort
   (delete-dups
    (cl-loop for row in (clutch-db-result-rows (clutch-db-query conn "SHOW DATABASES"))
             for db = (car row)
             when (and (stringp db) (not (string-empty-p db)))
             collect db))
   #'string-collate-lessp))

;;;###autoload
(defun clutch-switch-database ()
  "Switch the current ClickHouse connection to another database by reconnecting."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (params (or (plist-get context :params)
                     (car (clutch--connection-context conn))
                     (user-error "No reconnect parameters for this connection")))
         (current (or (plist-get params :database) "default"))
         (databases (clutch--list-clickhouse-databases conn)))
    (unless (or (clutch--connection-clickhouse-p conn)
                (clutch--params-clickhouse-p params))
      (user-error "Runtime database switching is currently available only for ClickHouse"))
    (unless databases
      (user-error "No databases returned by SHOW DATABASES"))
    (let ((database (completing-read
                     (if current
                         (format "Switch to database (current %s): " current)
                       "Switch to database: ")
                     databases nil t nil nil current)))
      (unless (string-empty-p database)
        (if (string-equal database current)
            (message "Already on database %s" current)
          (when (clutch--connection-alive-p conn)
            (clutch--confirm-disconnect-transaction-loss
             conn
             "Uncommitted changes will be lost.  Switch database? "))
          (clutch--replace-connection
           conn
           (plist-put (copy-sequence params) :database database)
           (plist-get context :product))
          (message "Current database: %s" database))))))

;;;###autoload
(defun clutch-switch-schema ()
  "Switch the current schema or database on the active connection."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (params (or clutch--connection-params
                     (plist-get context :params))))
    (if (or (clutch--connection-clickhouse-p conn)
            (clutch--params-clickhouse-p params))
        (clutch-switch-database)
      (let* ((schemas (clutch-db-list-schemas conn))
             (current (clutch-db-current-schema conn))
             (old-key (clutch--connection-key conn)))
        (unless schemas
          (user-error "Runtime schema switching is not available for this connection"))
        (let ((schema (completing-read
                       (if current
                           (format "Switch to schema (current %s): " current)
                         "Switch to schema: ")
                       schemas nil t nil nil current)))
          (unless (string-empty-p schema)
            (if (and current
                     (string= (downcase schema) (downcase current)))
                (message "Already on schema %s" current)
              (condition-case err
                  (progn
                    (clutch-db-set-current-schema conn schema)
                    (clutch--clear-connection-problem-capture conn)
                    (clutch--update-connection-params-for-buffers
                     conn
                     (lambda (params)
                       (if (eq (plist-get params :backend) 'mysql)
                           (plist-put params :database schema)
                         (plist-put params :schema schema))))
                    (clutch--clear-connection-metadata-caches conn old-key)
                    (clutch--clear-connection-metadata-caches conn)
                    (clutch--refresh-current-schema t)
                    (message "Current schema: %s" schema))
                (clutch-db-error
                 (let* ((message (error-message-string err))
                        (summary (clutch--humanize-db-error message)))
                   (clutch--remember-buffer-query-error-details
                    (current-buffer) conn nil err)
                   (when clutch-debug-mode
                     (clutch--remember-debug-event
                      :connection conn
                      :op "schema-switch"
                      :phase "error"
                      :backend (clutch--backend-key-from-conn conn)
                      :summary summary
                      :context (list :schema schema
                                     :current-schema current)))
                   (user-error "%s"
                               (clutch--debug-workflow-message summary))))))))))))

(defun clutch--eldoc-column-extras (col)
  "Return a space-joined string of constraint annotations for COL plist."
  (string-join
   (delq nil
         (list (when (not (plist-get col :nullable))
                 (propertize "NOT NULL" 'face 'font-lock-keyword-face))
               (when (plist-get col :primary-key)
                 (propertize "PK" 'face 'font-lock-builtin-face))
               (when-let* ((fk (plist-get col :foreign-key)))
                 (propertize (format "FK→%s.%s"
                                     (plist-get fk :ref-table)
                                     (plist-get fk :ref-column))
                             'face 'font-lock-constant-face))))
   "  "))

(defun clutch--eldoc-column-string (conn table col-name)
  "Format an eldoc string for COL-NAME in TABLE using CONN."
  (let* ((details (clutch--cached-column-details conn table))
         (col (and details
                   (cl-find col-name details
                            :key (lambda (d) (plist-get d :name))
                            :test (lambda (needle candidate)
                                    (string-equal (downcase needle)
                                                  (downcase candidate))))))
         (canonical-name (or (and col (plist-get col :name))
                             col-name))
         (header (concat (propertize table 'face 'font-lock-type-face)
                         "."
                         (propertize canonical-name
                                     'face 'font-lock-variable-name-face))))
    (unless details
      (clutch--ensure-column-details-async conn table))
    (if col
        (let ((type (plist-get col :type))
              (comment (plist-get col :comment))
              (extras (clutch--eldoc-column-extras col)))
          (string-join
           (delq nil (list header
                           (propertize type 'face 'font-lock-type-face)
                           (unless (string-empty-p extras) extras)
                           (when comment
                             (propertize (format "— %s" comment) 'face 'shadow))))
           "  "))
      header)))

(defun clutch--tables-in-buffer (schema)
  "Return table names from SCHEMA that appear in the current buffer."
  (let ((tick (buffer-chars-modified-tick)))
    (if (and clutch--tables-in-buffer-cache
             (eq (plist-get clutch--tables-in-buffer-cache :schema) schema)
             (= (plist-get clutch--tables-in-buffer-cache :tick) tick))
        (plist-get clutch--tables-in-buffer-cache :tables)
      (let ((text (buffer-substring-no-properties (point-min) (point-max)))
            tables)
        (maphash (lambda (tbl _cols)
                   (when (string-match-p (regexp-quote tbl) text)
                     (push tbl tables)))
                 schema)
        (setq tables (nreverse tables))
        (setq clutch--tables-in-buffer-cache
              (list :schema schema :tick tick :tables tables))
        tables))))

(defun clutch--statement-bounds ()
  "Return (BEG . END) for the SQL statement surrounding point."
  (let ((delim "\\(;\\|^[[:space:]]*$\\)"))
    (cons
     (save-excursion
       (if (re-search-backward delim nil t)
           (match-end 0)
         (point-min)))
     (save-excursion
       (if (re-search-forward delim nil t)
           (match-beginning 0)
         (point-max))))))

(defun clutch--compute-tables-in-query-cache (schema)
  "Return a fresh cache plist for table-name analysis on SCHEMA."
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
               (tick (buffer-chars-modified-tick))
               (text (buffer-substring-no-properties beg end))
               (`(,found . ,aliases)
                (clutch--extract-tables-and-aliases text 0 (length text)))
               (statement-tables (delete-dups found)))
    (list :schema schema
          :tick tick
          :beg beg
          :end end
          :statement-tables statement-tables
          :statement-aliases aliases
          :tables (or statement-tables
                      (clutch--tables-in-buffer schema)))))

(defun clutch--tables-in-query-cache-entry (schema)
  "Return the cache entry for table analysis on SCHEMA, refreshing if needed."
  (pcase-let* ((`(,beg . ,end) (clutch--statement-bounds))
               (tick (buffer-chars-modified-tick))
               (cached clutch--tables-in-query-cache))
    (if (and cached
             (eq (plist-get cached :schema) schema)
             (= (plist-get cached :tick) tick)
             (= (plist-get cached :beg) beg)
             (= (plist-get cached :end) end))
        cached
      (setq clutch--tables-in-query-cache
            (clutch--compute-tables-in-query-cache schema)))))

(defun clutch--tables-in-current-statement (schema)
  "Return known table names mentioned in the current statement for SCHEMA."
  (plist-get (clutch--tables-in-query-cache-entry schema) :statement-tables))

(defun clutch--innermost-paren-range (text point-offset)
  "Return the innermost parenthesized range in TEXT.
The result is (BEG . END) containing POINT-OFFSET, or (0 . LEN) when
POINT-OFFSET is at the top level."
  (let* ((len (length text))
         (stack nil)
         (result (cons 0 len))
         (i 0))
    (while (< i len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment text i)))
          (setq i skip)
        (pcase (aref text i)
          (?\( (push i stack)
               (cl-incf i))
          (?\) (when stack
                 (let ((open (pop stack)))
                   (when (and (<= open point-offset)
                              (>= i point-offset))
                     (when (< (- i open) (- (cdr result) (car result)))
                       (setq result (cons (1+ open) i))))))
               (cl-incf i))
          (_ (cl-incf i)))))
    result))

(defun clutch--union-branch-range (text point-offset)
  "Return (BEG . END) of the UNION branch in TEXT containing POINT-OFFSET.
First narrows to the innermost parenthesized scope, then splits by
UNION / UNION ALL at depth 0 within that scope."
  (pcase-let* ((`(,scope-beg . ,scope-end)
                (clutch--innermost-paren-range text point-offset))
               (sub (substring text scope-beg scope-end))
               (sub-offset (- point-offset scope-beg))
               (sub-len (length sub))
               (depth 0)
               (boundaries (list 0))
               (case-fold-search t)
               (i 0))
    ;; Collect positions of depth-0 UNION keywords within the scope.
    (while (< i sub-len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment sub i)))
          (setq i skip)
        (pcase (aref sub i)
          (?\( (cl-incf depth) (cl-incf i))
          (?\) (cl-decf depth) (cl-incf i))
          (_
           (if (and (zerop depth)
                    (string-match "\\bunion\\b\\(?:\\s-+all\\b\\)?" sub i)
                    (= (match-beginning 0) i))
               (progn
                 (push (match-beginning 0) boundaries)
                 (push (match-end 0) boundaries)
                 (setq i (match-end 0)))
             (cl-incf i))))))
    (push sub-len boundaries)
    (setq boundaries (nreverse boundaries))
    ;; Find the segment containing sub-offset, translate back to text coords.
    (let ((beg 0)
          (end sub-len))
      (while boundaries
        (let ((b (pop boundaries)))
          (cond
           ((<= b sub-offset) (setq beg b))
           (t (setq end b boundaries nil)))))
      (cons (+ scope-beg beg) (+ scope-beg end)))))

(defun clutch--extract-tables-and-aliases (text beg end)
  "Extract tables and alias mappings from TEXT between BEG and END.
Return (TABLES . ALIASES) where TABLES is a list of table names and
ALIASES is an alist of (alias . table) pairs.
String literals and comments are ignored via masking."
  (let ((case-fold-search t)
        (masked (clutch-db-sql-mask-literal-or-comment text))
        (pos beg)
        tables aliases)
    (while (and (< pos end)
                (string-match
                 "\\b\\(from\\|join\\|update\\|into\\)[ \t\n\r]+\\([[:alnum:]_$#.`\"]+\\)"
                 masked pos))
      (when (< (match-beginning 0) end)
        (let* ((table-end (match-end 2))
               (table-token (and table-end (match-string 2 text)))
               (table (clutch--normalize-statement-table-token table-token))
               (alias-consumed-end table-end))
          (setq pos table-end)
          (when (and (string-match
                      "[ \t\n\r]+\\(?:as[ \t\n\r]+\\)?\\([[:alnum:]_$#`\"]+\\)"
                      masked table-end)
                     (= (match-beginning 0) table-end)
                     (< (match-beginning 0) end))
            (let ((alias-consumed-match-end (match-end 0)))
              (when-let* ((alias-token (match-string 1 text))
                          (alias (clutch--normalize-statement-table-token alias-token))
                          ((not (member (upcase alias) clutch--sql-keywords))))
                (setq alias-consumed-end alias-consumed-match-end)
                (push (cons alias table) aliases))))
          (setq pos alias-consumed-end)
          (when table (push table tables)))))
    (cons (nreverse tables) (nreverse aliases))))

(defun clutch--table-aliases-in-current-statement (schema)
  "Return alias-to-table mappings for the UNION branch in SCHEMA containing point.
When the statement has no UNION, returns all aliases."
  (let* ((entry (clutch--tables-in-query-cache-entry schema))
         (all-aliases (plist-get entry :statement-aliases))
         (stmt-beg (plist-get entry :beg))
         (stmt-end (plist-get entry :end))
         (text (buffer-substring-no-properties stmt-beg stmt-end))
         (point-offset (- (point) stmt-beg))
         (range (clutch--union-branch-range text point-offset)))
    (if (and (= (car range) 0) (= (cdr range) (length text)))
        all-aliases
      (cdr (clutch--extract-tables-and-aliases text (car range) (cdr range))))))

(defun clutch--toplevel-union-branch-range (text point-offset)
  "Return (BEG . END) of the depth-0 UNION branch in TEXT containing POINT-OFFSET.
Unlike `clutch--union-branch-range', does not narrow into parenthesized
scopes first, so FROM/JOIN clauses remain visible from inside expressions."
  (let ((len (length text))
        (depth 0)
        (boundaries (list 0))
        (case-fold-search t)
        (i 0))
    (while (< i len)
      (if-let* ((skip (clutch-db-sql-skip-literal-or-comment text i)))
          (setq i skip)
        (pcase (aref text i)
          (?\( (cl-incf depth) (cl-incf i))
          (?\) (cl-decf depth) (cl-incf i))
          (_
           (if (and (zerop depth)
                    (string-match "\\bunion\\b\\(?:[ \t\n\r]+all\\b\\)?" text i)
                    (= (match-beginning 0) i))
               (progn
                 (push (match-beginning 0) boundaries)
                 (push (match-end 0) boundaries)
                 (setq i (match-end 0)))
             (cl-incf i))))))
    (push len boundaries)
    (setq boundaries (nreverse boundaries))
    (let ((beg 0) (end len))
      (while boundaries
        (let ((b (pop boundaries)))
          (cond
           ((<= b point-offset) (setq beg b))
           (t (setq end b boundaries nil)))))
      (cons beg end))))

(defun clutch--find-alias-in-range (text masked alias stmt-beg search-beg search-end)
  "Search for ALIAS definition in TEXT between SEARCH-BEG and SEARCH-END.
MASKED is the literal/comment-masked version of TEXT.
Returns the buffer position (offset by STMT-BEG), or nil."
  (let ((case-fold-search t)
        (pos search-beg))
    (catch 'found
      (while (and (< pos search-end)
                  (string-match
                   "\\b\\(from\\|join\\|update\\|into\\)[ \t\n\r]+\\([[:alnum:]_$#.`\"]+\\)"
                   masked pos))
        (when (>= (match-beginning 0) search-end)
          (throw 'found nil))
        (let ((table-end (match-end 2))
              (alias-pos nil))
          (setq pos table-end)
          (when (and (string-match
                      "[ \t\n\r]+\\(?:as[ \t\n\r]+\\)?\\(\"[^\"]+\"\\|`[^`]+`\\|[[:alnum:]_$#`\"]+\\)"
                      masked table-end)
                     (= (match-beginning 0) table-end)
                     (< (match-beginning 0) search-end))
            (let* ((token (match-string 1 text))
                   (normalized (clutch--normalize-statement-table-token token)))
              (when (and normalized
                         (not (member (upcase normalized) clutch--sql-keywords))
                         (string= (downcase normalized) (downcase alias)))
                (setq alias-pos (+ stmt-beg (match-beginning 1))))))
          (when alias-pos
            (throw 'found alias-pos))
          (setq pos (max pos (if (match-end 0) (match-end 0) (1+ pos)))))))))

(defun clutch--find-alias-definition-position (alias)
  "Return buffer position of ALIAS definition in the current statement.
First searches the innermost paren scope (subquery), then falls back to
the top-level UNION branch so expression parens like SUM(...) don't hide
outer FROM/JOIN clauses."
  (let* ((bounds (clutch--statement-bounds))
         (stmt-beg (car bounds))
         (text (buffer-substring-no-properties stmt-beg (cdr bounds)))
         (point-offset (- (point) stmt-beg))
         (masked (clutch-db-sql-mask-literal-or-comment text))
         (inner (clutch--union-branch-range text point-offset))
         (outer (clutch--toplevel-union-branch-range text point-offset)))
    (or (clutch--find-alias-in-range text masked alias stmt-beg
                                     (car inner) (cdr inner))
        (clutch--find-alias-in-range text masked alias stmt-beg
                                     (car outer) (cdr outer)))))

;;; xref backend — alias jump-to-definition

(defun clutch--xref-backend ()
  "Return `clutch' as xref backend in clutch SQL buffers.
Always claims the backend to prevent fallthrough to etags, which
triggers syntax_table errors in `sql-mode' derived buffers."
  (when clutch-connection 'clutch))

(defun clutch--xref-bare-identifier-char-p (ch)
  "Return non-nil when CH is part of an unquoted SQL identifier."
  (or (and (>= ch ?0) (<= ch ?9))
      (and (>= ch ?A) (<= ch ?Z))
      (and (>= ch ?a) (<= ch ?z))
      (memq ch '(?_ ?$ ?#))))

(defun clutch--xref-symbol-at-point ()
  "Return the SQL identifier at point without relying on syntax tables.
Returns (BEG . SYMBOL) or nil.  Uses SQL-aware token scanning so comments,
single-quoted strings, and multi-word quoted identifiers are handled
without consulting syntax tables."
  (pcase-let* ((`(,stmt-beg . ,stmt-end) (clutch--statement-bounds))
               (text (buffer-substring-no-properties stmt-beg stmt-end))
               (len (length text))
               (target (- (point) stmt-beg))
               (i 0))
    (when (and (>= target 0) (< target len))
      (catch 'hit
        (while (< i len)
          (if-let* ((skip (clutch-db-sql-skip-literal-or-comment text i)))
              (progn
                (when (and (<= i target) (< target skip))
                  (throw 'hit nil))
                (setq i skip))
            (let ((ch (aref text i)))
              (cond
               ((memq ch '(?\" ?`))
                (let* ((quote ch)
                       (end (or (cl-loop for j from (1+ i) below len
                                         when (= (aref text j) quote)
                                         return (1+ j))
                                len)))
                  (when (and (<= i target) (< target end))
                    (throw 'hit (cons (+ stmt-beg i)
                                      (substring text i end))))
                  (setq i end)))
               ((clutch--xref-bare-identifier-char-p ch)
                (let ((end (1+ i)))
                  (while (and (< end len)
                              (clutch--xref-bare-identifier-char-p
                               (aref text end)))
                    (cl-incf end))
                  (when (and (<= i target) (< target end))
                    (throw 'hit (cons (+ stmt-beg i)
                                      (substring text i end))))
                  (setq i end)))
               (t
                (cl-incf i))))))))))

(defun clutch--xref-qualified-identifier-qualifier (beg)
  "Return the qualifier immediately preceding identifier start BEG."
  (save-excursion
    (goto-char beg)
    (when (eq (char-before) ?.)
      (goto-char (max (point-min) (- beg 2)))
      (cdr (clutch--xref-symbol-at-point)))))

(defun clutch--xref-alias-at-point ()
  "Return the normalized alias name at point, or nil.
Handles both bare aliases (`u') and qualified references (`u.name').
Normalizes quoted identifiers to match the alias cache."
  (when-let* ((hit (clutch--xref-symbol-at-point))
              (beg (car hit))
              (sym (cdr hit))
              (raw (or (clutch--xref-qualified-identifier-qualifier beg) sym)))
    (clutch--normalize-statement-table-token raw)))

(cl-defmethod xref-backend-identifier-at-point ((_backend (eql 'clutch)))
  "Return the identifier at point, preferring normalized alias names."
  (or (clutch--xref-alias-at-point)
      (cdr (clutch--xref-symbol-at-point))
      ""))

(cl-defmethod xref-backend-definitions ((_backend (eql 'clutch)) identifier)
  "Return xref location of alias IDENTIFIER definition in the current statement."
  (when-let* ((pos (clutch--find-alias-definition-position identifier)))
    (list (xref-make (format "%s (alias)" identifier)
                     (xref-make-buffer-location (current-buffer) pos)))))

(cl-defmethod xref-backend-references ((_backend (eql 'clutch)) _identifier)
  "Not yet implemented."
  nil)

(defun clutch--tables-in-query (schema)
  "Return known table names for SCHEMA in the current statement.
This scans FROM/JOIN/UPDATE clauses in the SQL statement around point,
bounded by semicolons or blank lines.  Falls back to
`clutch--tables-in-buffer' when none are found."
  (plist-get (clutch--tables-in-query-cache-entry schema) :tables))

(defun clutch--cached-columns (schema table)
  "Return cached columns for TABLE from SCHEMA, or nil if not loaded."
  (let ((cols (and schema (gethash table schema 'missing))))
    (unless (eq cols 'missing) cols)))

(defun clutch--identifier-match (identifier candidates)
  "Return canonical match for IDENTIFIER from string CANDIDATES, or nil.
Matching is case-insensitive so unquoted SQL identifiers still resolve when
buffer text and cached metadata differ only by case."
  (cl-find identifier candidates
           :test (lambda (needle candidate)
                   (string-equal (downcase needle)
                                 (downcase candidate)))))

(defun clutch--normalize-statement-table-token (token)
  "Normalize a raw table TOKEN parsed from SQL into a bare table name.
Handles schema-qualified names like \"HR\".\"EMPLOYEES\" or `db`.`table`."
  (when token
    (let* ((stripped (replace-regexp-in-string "[\"`]" "" token))
           (parts (split-string stripped "\\." t)))
      (car (last parts)))))

(defun clutch--statement-table-identifiers ()
  "Return raw table identifiers referenced in the current statement."
  (pcase-let ((`(,beg . ,end) (clutch--statement-bounds)))
    (let* ((text (buffer-substring-no-properties beg end))
           (masked (clutch-db-sql-mask-literal-or-comment text))
           (case-fold-search t)
           found
           (pos 0))
      (while (string-match
              "\\b\\(from\\|join\\|update\\|into\\)[ \t\n\r]+\\([[:alnum:]_$#.`\"]+\\)"
              masked pos)
        (setq pos (match-end 0))
        (when-let* ((tbl (clutch--normalize-statement-table-token (match-string 2 text))))
          (push tbl found)))
      (nreverse (delete-dups found)))))

(defun clutch--qualified-identifier-qualifier (beg)
  "Return the qualifier token immediately preceding BEG, or nil.
For input like `u.name', returns `u' when BEG starts at `name'."
  (save-excursion
    (goto-char beg)
    (when (eq (char-before) ?.)
      (backward-char)
      (buffer-substring-no-properties
       (save-excursion
         (skip-chars-backward "[:alnum:]_$#`\"")
         (point))
       (point)))))

(defun clutch--qualified-identifier-table (schema beg)
  "Return the table referenced by the qualifier before BEG in SCHEMA, or nil.
Resolve both statement aliases like `u.name' and direct qualified table names
like `orders.id' within the current statement."
  (when-let* ((qualifier (and schema
                              (clutch--qualified-identifier-qualifier beg))))
    (or (cdr (assoc-string qualifier
                           (clutch--table-aliases-in-current-statement schema)
                           t))
        (let ((normalized (clutch--normalize-statement-table-token qualifier)))
          (when (member normalized
                        (or (clutch--tables-in-current-statement schema)
                            (clutch--statement-table-identifiers)))
            normalized)))))

(defconst clutch--sql-keywords
  '("SELECT" "FROM" "WHERE" "AND" "OR" "NOT" "IN" "IS" "NULL" "LIKE"
    "BETWEEN" "EXISTS" "CASE" "WHEN" "THEN" "ELSE" "END" "AS" "ON"
    "USING" "JOIN" "INNER" "LEFT" "RIGHT" "OUTER" "CROSS" "FULL"
    "INSERT" "INTO" "VALUES" "UPDATE" "SET" "DELETE"
    "CREATE" "ALTER" "DROP" "TABLE" "INDEX" "VIEW" "DATABASE"
    "GROUP" "BY" "ORDER" "ASC" "DESC" "HAVING" "LIMIT" "OFFSET"
    "UNION" "ALL" "DISTINCT" "COUNT" "SUM" "AVG" "MIN" "MAX"
    "IF" "IFNULL" "COALESCE" "CAST" "CONCAT" "SUBSTRING"
    "PRIMARY" "KEY" "FOREIGN" "REFERENCES" "CONSTRAINT" "DEFAULT"
    "UNIQUE" "CHECK" "AUTO_INCREMENT"
    "TRUNCATE" "EXPLAIN" "SHOW" "DESCRIBE"
    "BEGIN" "COMMIT" "ROLLBACK" "TRANSACTION"
    "GRANT" "REVOKE" "WITH" "RECURSIVE" "TEMPORARY" "TEMP")
  "SQL keywords for completion.")

(defconst clutch--sql-function-docs
  (let ((ht (make-hash-table :test 'equal :size 160)))
    (dolist (entry
             '(;; Aggregate
               ("COUNT"        "COUNT(expr)"
                "Non-NULL row count; COUNT(*) for all rows")
               ("SUM"          "SUM(expr)"
                "Sum of non-NULL values")
               ("AVG"          "AVG(expr)"
                "Average of non-NULL values")
               ("MIN"          "MIN(expr)"
                "Minimum non-NULL value")
               ("MAX"          "MAX(expr)"
                "Maximum non-NULL value")
               ("GROUP_CONCAT" "GROUP_CONCAT([DISTINCT] expr [ORDER BY …] [SEPARATOR sep])"
                "Aggregate strings into one  [MySQL]")
               ("STRING_AGG"   "STRING_AGG(expr, sep [ORDER BY …])"
                "Aggregate strings into one  [PG]")
               ("ARRAY_AGG"    "ARRAY_AGG(expr [ORDER BY …])"
                "Aggregate values into array  [PG]")
               ("JSON_ARRAYAGG"  "JSON_ARRAYAGG(expr)"
                "Aggregate values into JSON array  [MySQL 8+/PG]")
               ("JSON_OBJECTAGG" "JSON_OBJECTAGG(key, val)"
                "Aggregate key-value pairs into JSON object  [MySQL 8+/PG]")
               ;; String
               ("CONCAT"       "CONCAT(str1, str2, …)"
                "Concatenate strings (NULL-safe variant: CONCAT_WS)  [MySQL]")
               ("CONCAT_WS"    "CONCAT_WS(sep, str1, str2, …)"
                "Concatenate with separator, skipping NULLs  [MySQL]")
               ("SUBSTRING"    "SUBSTRING(str, pos [, len])"
                "Extract substring; also SUBSTRING(str FROM pos FOR len)")
               ("SUBSTR"       "SUBSTR(str, pos [, len])"
                "Alias for SUBSTRING")
               ("LEFT"         "LEFT(str, len)"
                "Leftmost len characters")
               ("RIGHT"        "RIGHT(str, len)"
                "Rightmost len characters")
               ("LENGTH"       "LENGTH(str)"
                "Byte length  [MySQL]; character length in PG — use CHAR_LENGTH for characters")
               ("CHAR_LENGTH"  "CHAR_LENGTH(str)"
                "Number of characters in string")
               ("UPPER"        "UPPER(str)"
                "Convert string to uppercase")
               ("LOWER"        "LOWER(str)"
                "Convert string to lowercase")
               ("TRIM"         "TRIM([[BOTH|LEADING|TRAILING] [remstr] FROM] str)"
                "Remove leading/trailing characters (default: spaces)")
               ("LTRIM"        "LTRIM(str)"
                "Remove leading spaces")
               ("RTRIM"        "RTRIM(str)"
                "Remove trailing spaces")
               ("REPLACE"      "REPLACE(str, from_str, to_str)"
                "Replace all occurrences of from_str with to_str")
               ("INSTR"        "INSTR(str, substr)"
                "1-based position of first substr occurrence  [MySQL]")
               ("POSITION"     "POSITION(substr IN str)"
                "1-based position of first substr occurrence")
               ("STRPOS"       "STRPOS(str, substr)"
                "1-based position of first substr occurrence  [PG]")
               ("LOCATE"       "LOCATE(substr, str [, pos])"
                "Position of substr starting from pos  [MySQL]")
               ("LPAD"         "LPAD(str, len [, padstr])"
                "Left-pad string to length len")
               ("RPAD"         "RPAD(str, len [, padstr])"
                "Right-pad string to length len")
               ("REPEAT"       "REPEAT(str, n)"
                "Repeat string n times")
               ("REVERSE"      "REVERSE(str)"
                "Reverse a string")
               ("SPLIT_PART"   "SPLIT_PART(str, delim, n)"
                "n-th field after splitting on delim  [PG]")
               ("REGEXP_REPLACE" "REGEXP_REPLACE(str, pattern, repl [, flags])"
                "Replace regex matches in string")
               ("REGEXP_LIKE"  "REGEXP_LIKE(str, pattern [, match_type])"
                "TRUE if str matches regex pattern  [MySQL 8+]")
               ("CHR"          "CHR(n)"
                "Character from integer code point  [PG]")
               ("ASCII"        "ASCII(str)"
                "ASCII code of first character")
               ("HEX"          "HEX(str_or_num)"
                "Hexadecimal representation  [MySQL]")
               ("UNHEX"        "UNHEX(hex_str)"
                "Decode hex string to binary  [MySQL]")
               ;; Date / time
               ("NOW"          "NOW()"
                "Current date and time")
               ("CURRENT_TIMESTAMP" "CURRENT_TIMESTAMP"
                "Current date and time")
               ("CURDATE"      "CURDATE()"
                "Current date  [MySQL]")
               ("CURRENT_DATE" "CURRENT_DATE"
                "Current date")
               ("CURTIME"      "CURTIME()"
                "Current time  [MySQL]")
               ("DATE"         "DATE(expr)"
                "Extract date part from datetime  [MySQL]")
               ("TIME"         "TIME(expr)"
                "Extract time part from datetime  [MySQL]")
               ("DATE_FORMAT"  "DATE_FORMAT(date, format)"
                "Format date using strftime-like format  [MySQL]")
               ("TO_CHAR"      "TO_CHAR(val, fmt)"
                "Format date or number as string  [PG]")
               ("TO_DATE"      "TO_DATE(str, fmt)"
                "Parse string to date  [PG]")
               ("TO_TIMESTAMP" "TO_TIMESTAMP(str, fmt)"
                "Parse string to timestamp  [PG]")
               ("STR_TO_DATE"  "STR_TO_DATE(str, format)"
                "Parse string to date/time  [MySQL]")
               ("DATE_ADD"     "DATE_ADD(date, INTERVAL n unit)"
                "Add interval to date  [MySQL]")
               ("DATE_SUB"     "DATE_SUB(date, INTERVAL n unit)"
                "Subtract interval from date  [MySQL]")
               ("DATEDIFF"     "DATEDIFF(date1, date2)"
                "Days between date1 and date2 (date1 − date2)  [MySQL]")
               ("TIMESTAMPDIFF" "TIMESTAMPDIFF(unit, dt1, dt2)"
                "Difference in unit between dt1 and dt2  [MySQL]")
               ("EXTRACT"      "EXTRACT(unit FROM date)"
                "Extract field: YEAR MONTH DAY HOUR MINUTE SECOND …")
               ("YEAR"         "YEAR(date)"
                "Year part of date (1000–9999)")
               ("MONTH"        "MONTH(date)"
                "Month part of date (1–12)")
               ("DAY"          "DAY(date)"
                "Day part of date (1–31)")
               ("HOUR"         "HOUR(time)"
                "Hour part (0–23)")
               ("MINUTE"       "MINUTE(time)"
                "Minute part (0–59)")
               ("SECOND"       "SECOND(time)"
                "Second part (0–59)")
               ("UNIX_TIMESTAMP" "UNIX_TIMESTAMP([date])"
                "Seconds since 1970-01-01 UTC  [MySQL]")
               ("FROM_UNIXTIME" "FROM_UNIXTIME(ts [, format])"
                "Convert Unix timestamp to datetime  [MySQL]")
               ("CONVERT_TZ"   "CONVERT_TZ(dt, from_tz, to_tz)"
                "Convert datetime between timezones  [MySQL]")
               ("AGE"          "AGE(ts1 [, ts2])"
                "Interval between timestamps  [PG]")
               ;; Numeric
               ("ABS"          "ABS(x)"
                "Absolute value")
               ("CEIL"         "CEIL(x)"
                "Smallest integer ≥ x")
               ("CEILING"      "CEILING(x)"
                "Smallest integer ≥ x  [MySQL]")
               ("FLOOR"        "FLOOR(x)"
                "Largest integer ≤ x")
               ("ROUND"        "ROUND(x [, d])"
                "Round x to d decimal places (default 0)")
               ("TRUNCATE"     "TRUNCATE(x, d)"
                "Truncate x to d decimal places  [MySQL]")
               ("TRUNC"        "TRUNC(x [, d])"
                "Truncate x to d decimal places  [PG]")
               ("MOD"          "MOD(x, y)"
                "Remainder of x / y  (also: x % y)")
               ("POWER"        "POWER(x, y)"
                "x raised to the power y")
               ("POW"          "POW(x, y)"
                "x raised to the power y  [MySQL]")
               ("SQRT"         "SQRT(x)"
                "Square root of x")
               ("EXP"          "EXP(x)"
                "e raised to the power x")
               ("LN"           "LN(x)"
                "Natural logarithm of x")
               ("LOG"          "LOG([base, ] x)"
                "Logarithm of x (base e or specified base)")
               ("LOG2"         "LOG2(x)"
                "Base-2 logarithm  [MySQL]")
               ("LOG10"        "LOG10(x)"
                "Base-10 logarithm")
               ("SIGN"         "SIGN(x)"
                "-1, 0, or 1 depending on sign of x")
               ("GREATEST"     "GREATEST(val1, val2, …)"
                "Largest value among arguments")
               ("LEAST"        "LEAST(val1, val2, …)"
                "Smallest value among arguments")
               ("RAND"         "RAND([seed])"
                "Random float in [0, 1)  [MySQL]")
               ("RANDOM"       "RANDOM()"
                "Random float in [0, 1)  [PG]")
               ("PI"           "PI()"
                "Value of π (3.141593)")
               ;; Conditional / null-handling
               ("IF"           "IF(cond, true_val, false_val)"
                "Return true_val if cond is true, else false_val  [MySQL]")
               ("IFNULL"       "IFNULL(expr, alt)"
                "Return alt if expr is NULL  [MySQL]")
               ("NULLIF"       "NULLIF(expr1, expr2)"
                "Return NULL if expr1 = expr2, else expr1")
               ("COALESCE"     "COALESCE(val1, val2, …)"
                "First non-NULL value in list")
               ("NVL"          "NVL(expr, alt)"
                "Return alt if expr is NULL  (Oracle-compatible)")
               ;; Type conversion
               ("CAST"         "CAST(expr AS type)"
                "Explicit type conversion")
               ("CONVERT"      "CONVERT(expr, type) or CONVERT(expr USING charset)"
                "Convert type or character set  [MySQL]")
               ;; Window functions
               ("ROW_NUMBER"   "ROW_NUMBER() OVER (…)"
                "Sequential row number within partition (no ties)")
               ("RANK"         "RANK() OVER (…)"
                "Rank with gaps on ties")
               ("DENSE_RANK"   "DENSE_RANK() OVER (…)"
                "Rank without gaps on ties")
               ("NTILE"        "NTILE(n) OVER (…)"
                "Divide rows into n ranked buckets")
               ("PERCENT_RANK" "PERCENT_RANK() OVER (…)"
                "Relative rank: (rank − 1) / (rows − 1)")
               ("CUME_DIST"    "CUME_DIST() OVER (…)"
                "Cumulative distribution of row within partition")
               ("LAG"          "LAG(expr [, n [, default]]) OVER (…)"
                "Value from n rows before current row")
               ("LEAD"         "LEAD(expr [, n [, default]]) OVER (…)"
                "Value from n rows after current row")
               ("FIRST_VALUE"  "FIRST_VALUE(expr) OVER (…)"
                "First value in window frame")
               ("LAST_VALUE"   "LAST_VALUE(expr) OVER (…)"
                "Last value in window frame")
               ("NTH_VALUE"    "NTH_VALUE(expr, n) OVER (…)"
                "n-th value in window frame  [PG/MySQL 8+]")
               ;; JSON
               ("JSON_EXTRACT" "JSON_EXTRACT(json, path)"
                "Extract value at JSON path  [MySQL]  (also: json->>'$.key')")
               ("JSON_UNQUOTE" "JSON_UNQUOTE(json_val)"
                "Remove quoting from JSON string value  [MySQL]")
               ("JSON_OBJECT"  "JSON_OBJECT(key, val, …)"
                "Create JSON object  [MySQL]")
               ("JSON_ARRAY"   "JSON_ARRAY(val, …)"
                "Create JSON array  [MySQL]")
               ("JSON_CONTAINS" "JSON_CONTAINS(target, candidate [, path])"
                "TRUE if target contains candidate  [MySQL]")
               ;; Misc / info
               ("DATABASE"     "DATABASE()"
                "Current database name  [MySQL]")
               ("CURRENT_DATABASE" "CURRENT_DATABASE()"
                "Current database name  [PG]")
               ("USER"         "USER()"
                "Current user as user@host  [MySQL]")
               ("CURRENT_USER" "CURRENT_USER"
                "Current authenticated user")
               ("VERSION"      "VERSION()"
                "Server version string")
               ("LAST_INSERT_ID" "LAST_INSERT_ID([expr])"
                "Auto-increment ID from last INSERT  [MySQL]")
               ("ROW_COUNT"    "ROW_COUNT()"
                "Rows affected by last DML statement  [MySQL]")
               ("UUID"         "UUID()"
                "Generate a version-1 UUID  [MySQL]")
               ("SLEEP"        "SLEEP(n)"
                "Sleep n seconds  [MySQL]")
               ;; Clauses / keywords with syntax notes
               ("EXPLAIN"      "EXPLAIN [ANALYZE] query"
                "Show query execution plan")
               ("BETWEEN"      "expr BETWEEN low AND high"
                "Inclusive range test — equivalent to low ≤ expr ≤ high")
               ("EXISTS"       "EXISTS (subquery)"
                "TRUE if subquery returns at least one row")
               ("LIKE"         "str LIKE pattern"
                "Pattern match: % = any sequence, _ = exactly one character")
               ("ILIKE"        "str ILIKE pattern"
                "Case-insensitive pattern match  [PG]")
               ("REGEXP"       "str REGEXP pattern"
                "Regular expression match  [MySQL]")
               ("RLIKE"        "str RLIKE pattern"
                "Alias for REGEXP  [MySQL]")
               ("OVER"         "OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE frame])"
                "Window function clause")
               ("PARTITION"    "PARTITION BY col1, col2, …"
                "Divide rows into groups for window functions")
               ("WITH"         "WITH name [(cols)] AS (subquery) SELECT …"
                "Common Table Expression (CTE); prefix WITH RECURSIVE for recursive CTEs")
               ("RETURNING"    "INSERT/UPDATE/DELETE … RETURNING col, …"
                "Return values of modified rows  [PG]")
               ;; CASE expression
               ("CASE"         "CASE WHEN cond THEN val … [ELSE default] END"
                "Conditional expression; simple form: CASE expr WHEN val THEN res … [ELSE def] END")
               ("WHEN"         "WHEN condition THEN result"
                "Branch condition inside CASE expression")
               ("THEN"         "THEN result"
                "Result value for a matched CASE/WHEN branch")
               ("ELSE"         "ELSE default"
                "Fallback value when no CASE/WHEN branch matches")
               ("END"          "END"
                "Terminates a CASE expression")
               ;; Membership / set
               ("IN"           "expr IN (val1, val2, …) or expr IN (subquery)"
                "TRUE if expr equals any value in the list or subquery")
               ("NOT"          "NOT expr"
                "Logical negation")
               ("ANY"          "expr op ANY (subquery)"
                "TRUE if comparison holds for at least one subquery row")
               ("ALL"          "expr op ALL (subquery)"
                "TRUE if comparison holds for every subquery row")
               ;; JOIN keywords
               ("JOIN"         "table JOIN other ON condition"
                "INNER JOIN — return rows with matches in both tables")
               ("INNER"        "INNER JOIN table ON condition"
                "Return only rows with matches in both tables (default JOIN)")
               ("LEFT"         "LEFT [OUTER] JOIN table ON condition"
                "Return all left rows; NULL-fill unmatched right rows")
               ("RIGHT"        "RIGHT [OUTER] JOIN table ON condition"
                "Return all right rows; NULL-fill unmatched left rows")
               ("FULL"         "FULL [OUTER] JOIN table ON condition"
                "Return all rows from both sides, NULL-fill unmatched  [PG]")
               ("CROSS"        "CROSS JOIN table"
                "Cartesian product of both tables — no ON clause")
               ("ON"           "ON condition"
                "Join condition: ON t1.col = t2.col")
               ("USING"        "USING (col1, col2, …)"
                "Join on identically-named columns; equivalent to ON t1.col = t2.col")
               ;; Set operations
               ("UNION"        "query UNION [ALL] query"
                "Combine rows; ALL keeps duplicates; without ALL deduplicates")
               ("INTERSECT"    "query INTERSECT [ALL] query"
                "Rows present in both result sets  [PG/MySQL 8.0.31+]")
               ("EXCEPT"       "query EXCEPT [ALL] query"
                "Rows in first set not in second  [PG]; MySQL: EXCEPT")
               ("MINUS"        "query MINUS query"
                "Rows in first set not in second (Oracle/older MySQL synonym for EXCEPT)")
               ;; DML clause keywords
               ("INTO"         "INSERT INTO table (cols) VALUES (…)"
                "Target table for INSERT")
               ("VALUES"       "VALUES (val1, val2, …) [, (…)]"
                "Row value list for INSERT")
               ("SET"          "UPDATE table SET col = val, …"
                "Assignment list for UPDATE")
               ("FROM"         "FROM table [alias] [JOIN …]"
                "Source table(s) for SELECT / DELETE")
               ("WHERE"        "WHERE condition"
                "Filter rows; applied before GROUP BY")
               ("GROUP"        "GROUP BY col1, col2, …"
                "Aggregate rows into groups")
               ("HAVING"       "HAVING condition"
                "Filter groups after GROUP BY; may reference aggregates")
               ("ORDER"        "ORDER BY col [ASC|DESC] [NULLS FIRST|LAST]"
                "Sort result rows")
               ("LIMIT"        "LIMIT n [OFFSET m]"
                "Return at most n rows, skip m rows")
               ("OFFSET"       "OFFSET n"
                "Skip n rows before returning results")
               ("DISTINCT"     "SELECT DISTINCT col, …"
                "Eliminate duplicate rows from result set")
               ("ASC"          "ORDER BY col ASC"
                "Sort ascending (default)")
               ("DESC"         "ORDER BY col DESC"
                "Sort descending")
               ("NULLS"        "ORDER BY col NULLS FIRST|LAST"
                "Control NULL sort position  [PG/MySQL 8+]")))
      (puthash (car entry)
               (list :sig (cadr entry) :desc (caddr entry))
               ht))
    ht)
  "Hash table mapping uppercase SQL function/keyword names to doc plists.
Each value is a plist (:sig SIGNATURE :desc DESCRIPTION).")

(defun clutch--eldoc-keyword-string (sym)
  "Return an eldoc string for SQL keyword/function SYM, or nil."
  (when-let* ((doc (gethash (upcase sym) clutch--sql-function-docs))
              (sig  (plist-get doc :sig))
              (desc (plist-get doc :desc)))
    (concat (propertize sig  'face 'font-lock-function-name-face)
            (propertize (concat "  — " desc) 'face 'shadow))))

(defun clutch--completion-finished-status-p (status)
  "Return non-nil when completion STATUS means candidate was accepted."
  (memq status '(finished exact sole)))

(defun clutch--apply-sql-completion-case-style (text)
  "Return TEXT transformed by `clutch-sql-completion-case-style'."
  (pcase clutch-sql-completion-case-style
    ('lower (downcase text))
    ('upper (upcase text))
    (_ text)))

(defun clutch--sql-keyword-completion-candidates ()
  "Return SQL keyword completion candidates honoring case style."
  (mapcar #'clutch--apply-sql-completion-case-style clutch--sql-keywords))

(defun clutch--sql-identifier-completion-candidates (candidates)
  "Return completion CANDIDATES honoring identifier case style."
  (delete-dups
   (mapcar #'clutch--apply-sql-completion-case-style candidates)))


(defun clutch--sql-keyword-prefix-p (prefix)
  "Return non-nil when PREFIX matches the start of any SQL keyword."
  (let ((upcase-prefix (upcase prefix)))
    (seq-some (lambda (keyword)
                (string-prefix-p upcase-prefix keyword))
              clutch--sql-keywords)))


(defun clutch-sql-keyword-completion-at-point ()
  "Completion-at-point function for SQL keywords.
Works without a database connection."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol)))
    (list (car bounds) (cdr bounds)
          (clutch--sql-keyword-completion-candidates)
          :exclusive 'no
          :exit-function (lambda (_str status)
                           (when (and (clutch--completion-finished-status-p status)
                                      (not (looking-at-p "\\s-")))
                             (insert " "))))))

(defun clutch--install-completion-capfs ()
  "Install completion CAPFs for the current buffer in priority order.
Identifier completion must run before SQL keyword completion so table names in
contexts like FROM/JOIN are not shadowed by keywords such as ORDER."
  (add-hook 'completion-at-point-functions
            #'clutch-completion-at-point nil t)
  (add-hook 'completion-at-point-functions
            #'clutch-sql-keyword-completion-at-point t t))

(defun clutch-completion-at-point ()
  "Completion-at-point function for SQL identifiers.
Skips column loading if the connection is busy (prevents re-entrancy
when completion triggers during an in-flight query)."
  (when-let* ((conn clutch-connection)
              (bounds (bounds-of-thing-at-point 'symbol)))
    (let* ((beg (car bounds))
           (end (cdr bounds))
           (prefix (buffer-substring-no-properties beg end))
           (prefix-len (- end beg))
           (schema (clutch--schema-for-connection))
           (qualifier (and schema
                           (clutch--qualified-identifier-qualifier beg)))
           (line-before (buffer-substring-no-properties
                         (line-beginning-position) beg))
           (table-context-p
            (string-match-p
             "\\b\\(FROM\\|JOIN\\|INTO\\|UPDATE\\|TABLE\\|DESCRIBE\\|DESC\\)\\s-+\\S-*\\'"
             (upcase line-before)))
           (busy (clutch-db-busy-p conn))
           (sync-columns-p (clutch-db-completion-sync-columns-p conn))
           (direct-table-candidates
            (when (and table-context-p
                       (>= prefix-len clutch--schema-inline-min-prefix-length))
              (clutch--safe-completion-call
               (lambda () (clutch-db-complete-tables conn prefix)))))
           (qualified-table (and qualifier
                                 (clutch--qualified-identifier-table schema beg)))
           (context-tables
            (unless (or table-context-p busy
                        (< prefix-len clutch--schema-inline-min-prefix-length))
              (let ((tables (or (and qualified-table (list qualified-table))
                                (and schema (clutch--tables-in-current-statement schema))
                                (clutch--statement-table-identifiers))))
                (when (and tables
                           (<= (length tables) clutch--schema-inline-table-limit))
                  tables))))
           (prefer-keyword-p
            (and (not table-context-p)
                 (null context-tables)
                 (clutch--sql-keyword-prefix-p prefix)))
           (candidates nil))
      (setq candidates
            (if prefer-keyword-p
                nil
              (if context-tables
                (let ((all (unless qualified-table
                             (copy-sequence context-tables))))
                  (dolist (tbl context-tables)
                    (let ((cols
                           (if sync-columns-p
                               (or (clutch--cached-columns schema tbl)
                                   (and schema
                                        (clutch--ensure-columns conn schema tbl)))
                             (or (clutch--cached-columns schema tbl)
                                 (clutch--safe-completion-call
                                  (lambda ()
                                    (clutch-db-complete-columns conn tbl prefix)))))))
                      (when cols
                        (setq all (nconc all (copy-sequence cols))))))
                  (clutch--sql-identifier-completion-candidates all))
                (clutch--sql-identifier-completion-candidates
                 (append direct-table-candidates
                         (and schema (hash-table-keys schema)))))))
      (when candidates
        (list beg end candidates
              :exclusive 'no
              :exit-function
              (lambda (str status)
                (when (and (clutch--completion-finished-status-p status)
                           (member (upcase str) clutch--sql-keywords)
                           (not (looking-at-p "\\s-")))
                  (insert " "))))))))

(defun clutch--eldoc-schema-string (conn schema sym &optional qualified-table)
  "Return an eldoc string for SYM via SCHEMA on CONN, or nil.
Matches SYM as a table name first, then as a column in any visible table.
When QUALIFIED-TABLE is non-nil, resolve field metadata against that table
even if the current statement exceeds `clutch--schema-inline-table-limit'."
  (let ((sync-columns-p (clutch-db-completion-sync-columns-p conn)))
    (cond
     ((not (eq (gethash sym schema 'missing) 'missing))
      (let* ((cols    (clutch--cached-columns schema sym))
             (_       (when (and sync-columns-p (not cols))
                        (clutch--ensure-columns-async conn schema sym)))
             (comment (clutch--cached-table-comment conn sym))
             (_       (when (not (clutch--table-comment-cached-p conn sym))
                        (clutch--ensure-table-comment-async conn sym)))
             (n       (length cols)))
        (concat (propertize (format "[%s] " (clutch-db-database conn)) 'face 'shadow)
                (propertize sym 'face 'font-lock-type-face)
                (when cols
                  (propertize (format "  (%d col%s)" n (if (= n 1) "" "s"))
                              'face 'shadow))
                (when comment
                  (propertize (format "  — %s" comment) 'face 'shadow)))))
     ((>= (length sym) clutch--schema-inline-min-prefix-length)
      (let ((tables (or (and qualified-table (list qualified-table))
                        (clutch--tables-in-current-statement schema))))
        (when (and tables
                   (or qualified-table
                       (<= (length tables) clutch--schema-inline-table-limit)))
          (cl-loop for tbl in tables
                   for cached-cols = (clutch--cached-columns schema tbl)
                   for cols = (cond
                               (cached-cols cached-cols)
                               ((not sync-columns-p) nil)
                               ((clutch-db-busy-p conn)
                                (clutch--ensure-columns-async conn schema tbl)
                                nil)
                               (t
                                (clutch--ensure-columns conn schema tbl)))
                   for matched-col = (and cols
                                          (clutch--identifier-match sym cols))
                   when matched-col
                   return (clutch--eldoc-column-string conn tbl matched-col)))))
     (t nil))))

(defun clutch--eldoc-effective-symbol-at-point (sym schema)
  "Return the effective eldoc symbol at point for raw SYM and SCHEMA.
When point is on the schema qualifier of a schema-qualified table reference
like `schema.table', return the table part if it exists in SCHEMA.  Otherwise
return SYM unchanged."
  (or
   (save-excursion
     (when-let* ((bounds (bounds-of-thing-at-point 'symbol)))
       (goto-char (cdr bounds))
       (when (eq (char-after) ?`) (forward-char 1))
       (when (eq (char-after) ?.)
         (forward-char 1)
         (when (eq (char-after) ?`) (forward-char 1))
         (when-let* ((next-sym (thing-at-point 'symbol t))
                     (schema schema)
                     ((not (eq (gethash next-sym schema 'missing) 'missing))))
           next-sym))))
   sym))

(defun clutch--eldoc-function (&rest _)
  "Eldoc backend for `clutch-mode'.
Returns a documentation string for the SQL identifier at point.
Schema-based info (tables, columns) requires an active connection.
SQL keyword/function docs are shown even without a connection."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol))
              (sym (buffer-substring-no-properties (car bounds) (cdr bounds))))
    (let* ((schema (clutch--schema-for-connection))
           (qualified-table (clutch--qualified-identifier-table
                             schema (car bounds)))
           (effective-sym (clutch--eldoc-effective-symbol-at-point sym schema)))
    (or
     (when-let* ((conn clutch-connection)
                 (schema schema)
                 ((not (clutch-db-busy-p conn))))
       (clutch--eldoc-schema-string conn schema effective-sym qualified-table))
     (when-let* ((conn clutch-connection)
                 ((clutch--connection-alive-p conn))
                 ((not (clutch-db-busy-p conn)))
                 ((string= "MySQL" (clutch-db-display-name conn))))
       (clutch--ensure-help-doc conn effective-sym))
     (clutch--eldoc-keyword-string effective-sym)))))


;;;; clutch-mode (SQL editing major mode)

(defvar clutch-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'clutch-execute-dwim)
    (define-key map (kbd "C-c C-r") #'clutch-execute-region)
    (define-key map (kbd "C-c C-b") #'clutch-execute-buffer)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-m") #'clutch-commit)
    (define-key map (kbd "C-c C-u") #'clutch-rollback)
    (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
    (define-key map (kbd "C-c C-j") #'clutch-jump)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map (kbd "C-c C-s") #'clutch-refresh-schema)
    (define-key map (kbd "C-c ?") #'clutch-dispatch)
    map)
  "Keymap for `clutch-mode'.")

;;;###autoload
(define-derived-mode clutch-mode sql-mode "clutch"
  "Major mode for editing and executing SQL queries.

\\<clutch-mode-map>
Key bindings:
  \\[clutch-execute-dwim]	Execute region or statement/query at point
  \\[clutch-execute-region]	Execute region
  \\[clutch-execute-buffer]	Execute buffer
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch schema/database
  \\[clutch-preview-execution-sql]	Preview execution"
  (set-buffer-file-coding-system 'utf-8-unix nil t)
  (add-hook 'kill-emacs-hook #'clutch--save-all-consoles)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t)
  (add-hook 'kill-buffer-hook #'clutch--save-console nil t)
  (clutch--install-completion-capfs)
  (add-hook 'eldoc-documentation-functions
            #'clutch--eldoc-function nil t)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t)
  (clutch--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . clutch-mode))

;;;###autoload
(autoload 'clutch-query-console "clutch" nil t)
;;;###autoload
(autoload 'clutch-switch-console "clutch" nil t)
;;;###autoload
(autoload 'clutch-execute "clutch" nil t)
;;;###autoload
(autoload 'clutch-edit-indirect "clutch" nil t)

;;;; Cell navigation

;;;###autoload
(defun clutch-result-next-cell ()
  "Move point to the next cell (right, then wrap to next row)."
  (interactive)
  (let ((start (point)))
    (goto-char (next-single-property-change (point) 'clutch-col-idx
                                            nil (point-max)))
    (if-let* ((m (text-property-search-forward 'clutch-col-idx nil
                                               (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-prev-cell ()
  "Move point to the previous cell (left, then wrap to prev row)."
  (interactive)
  (let ((start (point)))
    (when-let* ((beg (previous-single-property-change
                      (1+ (point)) 'clutch-col-idx nil (point-min))))
      (goto-char beg))
    (if-let* ((m (text-property-search-backward 'clutch-col-idx nil
                                                (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start)))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-down-cell ()
  "Move to the same column in the next row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx)))
    (clutch--goto-cell (1+ ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;###autoload
(defun clutch-result-up-cell ()
  "Move to the same column in the previous row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx))
              ((> ridx 0)))
    (clutch--goto-cell (1- ridx) cidx))
  (clutch--ensure-point-visible-horizontally)
  (when (use-region-p)
    (setq deactivate-mark nil)))

;;;; Row selection (region-based)

(defun clutch-result--selected-row-indices ()
  "Return row indices for row-oriented batch operations.
Priority: region rows > current row."
  (or (when (use-region-p)
        (clutch-result--rows-in-region (region-beginning) (region-end)))
      (when-let* ((ridx (clutch-result--row-idx-at-line)))
        (list ridx))))

;;;###autoload
(defun clutch-result-discard-pending-at-point ()
  "Discard the staged change at point."
  (interactive)
  (let ((ridx (or (clutch-result--row-idx-at-line)
                  (user-error "No row at point")))
        (nrows (length clutch--result-rows)))
    (cond
     ((>= ridx nrows)
      (let ((iidx (- ridx nrows)))
        (setq clutch--pending-inserts
              (delq (nth iidx clutch--pending-inserts) clutch--pending-inserts))
        (clutch--refresh-display)
        (message "Pending insertion discarded")))
     (t
      (let* ((pk-indices (or clutch--cached-pk-indices
                             (clutch-result--detect-primary-key)))
             (display-rows (or clutch--filtered-rows clutch--result-rows))
             (row (nth ridx display-rows))
             (cidx (clutch--col-idx-at-point))
             (pv (when pk-indices
                   (clutch-result--extract-pk-vec row pk-indices)))
             (edit-key (and pv cidx (cons pv cidx)))
             (was-edit (and edit-key
                            (cl-assoc edit-key clutch--pending-edits :test #'equal)))
             (was-delete (and pv (cl-find pv clutch--pending-deletes :test #'equal))))
        (cond
         (was-edit
          (setq clutch--pending-edits
                (cl-remove edit-key clutch--pending-edits :test #'equal :key #'car))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Pending edit discarded"))
         (was-delete
          (setq clutch--pending-deletes
                (cl-remove pv clutch--pending-deletes :test #'equal))
          (clutch--replace-row-at-index ridx)
          (clutch--refresh-footer-line)
          (force-mode-line-update)
          (message "Pending deletion discarded"))
         (t
          (user-error "No pending change at point"))))))))

;;;; clutch-result-mode

(defvar clutch-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "C-c '") #'clutch-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'clutch-result-commit)
    (define-key map "g" #'clutch-result-rerun)
    (define-key map "e" #'clutch-result-export)
    (define-key map "C" #'clutch-result-goto-column)
    (define-key map "n" #'clutch-result-down-cell)
    (define-key map "p" #'clutch-result-up-cell)
    (define-key map "N" #'clutch-result-next-page)
    (define-key map "P" #'clutch-result-prev-page)
    (define-key map (kbd "M->") #'clutch-result-last-page)
    (define-key map (kbd "M-<") #'clutch-result-first-page)
    (define-key map "#" #'clutch-result-count-total)
    (define-key map "A" #'clutch-result-aggregate)
    (define-key map "s" #'clutch-result-sort-by-column)
    (define-key map "S" #'clutch-result-sort-by-column-desc)
    (define-key map "c" #'clutch-result-copy-dispatch)
    (define-key map "v" #'clutch-result-view-value)
    (define-key map "V" #'clutch-result-live-view-value)
    (define-key map "|" #'clutch-result-shell-command-on-cell)
    (define-key map "?" #'clutch-result-column-info)
    (define-key map "W" #'clutch-result-apply-filter)
    (define-key map (kbd "RET") #'clutch-result-open-record)
    (define-key map "]" #'clutch-result-scroll-right)
    (define-key map "[" #'clutch-result-scroll-left)
    (define-key map "=" #'clutch-result-widen-column)
    (define-key map "-" #'clutch-result-narrow-column)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map "f" #'clutch-result-fullscreen-toggle)
    (define-key map (kbd "C-c ?") #'clutch-result-dispatch)
    ;; Cell navigation
    (define-key map (kbd "TAB") #'clutch-result-next-cell)
    (define-key map (kbd "<backtab>") #'clutch-result-prev-cell)
    (define-key map (kbd "M-n") #'clutch-result-down-cell)
    (define-key map (kbd "M-p") #'clutch-result-up-cell)
    ;; n/p are down/up cell (special-mode convention); M-n/M-p are aliases
    ;; Client-side filter
    (define-key map "/" #'clutch-result-filter)
    ;; Delete / Insert
    (define-key map "d" #'clutch-result-delete-rows)
    (define-key map "i" #'clutch-result-insert-row)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map (kbd "C-c C-k") #'clutch-result-discard-pending-at-point)
    map)
  "Keymap for `clutch-result-mode'.")

(defun clutch--position-indicator-parts (ridx cidx)
  "Return a formatted mode-line position string for RIDX and CIDX."
  (let* ((page-offset (* clutch--page-current clutch-result-max-rows))
         (global-row  (+ page-offset ridx))
         (rows        (or clutch--filtered-rows clutch--result-rows))
         (row-count   (length rows))
         (ncols       (length clutch--result-columns))
         (col-name    (when cidx (nth cidx clutch--result-columns)))
         (parts       nil))
    (push (format "R%d/%s C%d/%d"
                  (1+ global-row)
                  (if clutch--page-total-rows
                      (number-to-string clutch--page-total-rows)
                    (number-to-string row-count))
                  (if cidx (1+ cidx) 0) ncols)
          parts)
    (when col-name  (push (format "[%s]" col-name) parts))
    (push (format "pg %d" (1+ clutch--page-current)) parts)
    (when clutch--query-elapsed
      (push (clutch--format-elapsed clutch--query-elapsed) parts))
    (when clutch--filter-pattern
      (push (format "/:%s" clutch--filter-pattern) parts))
    (when clutch--where-filter
      (push (format "W:%s" clutch--where-filter) parts))
    (format " %s" (mapconcat #'identity parts " | "))))

(defun clutch--update-position-indicator ()
  "Update mode-line with current cursor position in the result grid."
  (let ((cidx (clutch--col-idx-at-point))
        (ridx (get-text-property (point) 'clutch-row-idx)))
    (setq mode-line-position
          (when ridx (clutch--position-indicator-parts ridx cidx)))))

(defun clutch--update-row-highlight ()
  "Highlight the entire row under the cursor.
Reuses the existing overlay via `move-overlay' when possible."
  (let ((beg (line-beginning-position))
        (end (line-end-position)))
    (if (get-text-property (point) 'clutch-row-idx)
        (if (and clutch--row-overlay (overlay-buffer clutch--row-overlay))
            (move-overlay clutch--row-overlay beg end)
          (when clutch--row-overlay
            (delete-overlay clutch--row-overlay))
          (let ((ov (make-overlay beg end)))
            (overlay-put ov 'face 'hl-line)
            (overlay-put ov 'priority -1)
            (setq clutch--row-overlay ov)))
      (when clutch--row-overlay
        (delete-overlay clutch--row-overlay)
        (setq clutch--row-overlay nil)))))

(defun clutch--update-header-highlight ()
  "Highlight the header cell for the column under the cursor.
Rebuilds `header-line-format' with the active column highlighted.
Skips work for scroll commands that do not move point."
  (when (and clutch--column-widths
             (not (memq this-command
                        '(scroll-down-line scroll-up-line
                          scroll-down scroll-up
                          scroll-down-command scroll-up-command
                          mwheel-scroll))))
    (clutch--update-position-indicator)
    (clutch--update-row-highlight)
    (clutch--refresh-footer-cursor)
    (force-mode-line-update)
    (let ((cidx (clutch--col-idx-at-point)))
      (unless (eql cidx clutch--header-active-col)
        (setq clutch--header-active-col cidx)
        (clutch--refresh-header-line)))))

;;;###autoload
(define-derived-mode clutch-result-mode special-mode "clutch-result"
  "Mode for displaying database query results as one scrollable table.

\\<clutch-result-mode-map>
Navigate:
  \\[clutch-result-next-cell]	Next cell (Tab)
  \\[clutch-result-prev-cell]	Previous cell (S-Tab)
  \\[clutch-result-down-cell]	Down in same column
  \\[clutch-result-up-cell]	Up in same column
  \\[clutch-result-open-record]	Open record view for row
  \\[clutch-result-goto-column]	Jump to column by name
Pages:
  \\[clutch-result-next-page]	Next data page
  \\[clutch-result-prev-page]	Previous data page
Navigate (row):
  \\[clutch-result-down-cell]	Next row (same column)
  \\[clutch-result-up-cell]	Previous row (same column)
  \\[clutch-result-first-page]	First data page
  \\[clutch-result-last-page]	Last data page
  \\[clutch-result-count-total]	Query total row count
  \\[clutch-result-aggregate]	Aggregate current/selected column values
  \\[clutch-result-scroll-right]	Page right (snap to next column border)
  \\[clutch-result-scroll-left]	Page left (snap to previous column border)
Copy:
  \\[clutch-result-copy-dispatch]	Copy… (transient: choose format, -r to refine)
  \\[clutch-result-export]	Export all rows (copy/file)
  \\[clutch-preview-execution-sql]	Preview execution
Inspect:
  \\[clutch-result-view-value]	View current cell once
  \\[clutch-result-live-view-value]	Open live viewer that follows point
Edit:
  \\[clutch-result-edit-cell]	Edit / re-edit at point
  \\[clutch-result-commit]	Commit pending changes
  \\[clutch-result-apply-filter]	Apply WHERE filter
  \\[clutch-result-sort-by-column]	Sort ascending (SQL ORDER BY)
  \\[clutch-result-sort-by-column-desc]	Sort descending (SQL ORDER BY)
  \\[clutch-result-widen-column]	Widen column
  \\[clutch-result-narrow-column]	Narrow column
  \\[clutch-result-rerun]	Re-execute the query"
  (setq truncate-lines t)
  (hl-line-mode 1)
  (setq-local scroll-step 1)
  (setq-local hscroll-step 1)
  ;; Make mode-line use default background so footer renders cleanly
  (face-remap-add-relative 'mode-line :inherit 'default)
  (face-remap-add-relative 'mode-line-inactive :inherit 'default)
  (setq-local revert-buffer-function #'clutch-result--revert)
  (add-hook 'post-command-hook
            #'clutch--update-header-highlight nil t)
  (add-hook 'kill-buffer-hook #'clutch--result-buffer-cleanup nil t)
  (add-hook 'change-major-mode-hook #'clutch--result-buffer-cleanup nil t)
  (clutch--enable-window-size-hook))

;;;###autoload
(defun clutch-result-next-page ()
  "Go to the next data page."
  (interactive)
  (let ((rows-on-page (length clutch--result-rows)))
    (when (< rows-on-page clutch-result-max-rows)
      (user-error "Already on last page (fewer rows than page size)"))
    (clutch--execute-page (1+ clutch--page-current))))

;;;###autoload
(defun clutch-result-prev-page ()
  "Go to the previous data page."
  (interactive)
  (when (<= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch--execute-page (1- clutch--page-current)))

;;;###autoload
(defun clutch-result-first-page ()
  "Go to the first data page."
  (interactive)
  (when (= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch--execute-page 0))

;;;###autoload
(defun clutch-result-last-page ()
  "Go to the last data page.
Triggers a COUNT(*) query if total rows are not yet known."
  (interactive)
  (unless clutch--page-total-rows
    (clutch-result-count-total))
  (when clutch--page-total-rows
    (let* ((page-size clutch-result-max-rows)
           (last-page (max 0 (1- (ceiling clutch--page-total-rows
                                           (float page-size))))))
      (if (= clutch--page-current (truncate last-page))
          (user-error "Already on last page")
        (clutch--execute-page (truncate last-page))))))

(defun clutch--sql-strip-top-level-tail (sql)
  "Strip top-level ORDER/LIMIT/OFFSET tail clauses from SQL."
  (let* ((order-pos (clutch--sql-find-top-level-clause sql "ORDER\\s-+BY"))
         (limit-pos (clutch--sql-find-top-level-clause sql "LIMIT"))
         (offset-pos (clutch--sql-find-top-level-clause sql "OFFSET"))
         (cut-pos (car (sort (delq nil (list order-pos limit-pos offset-pos)) #'<))))
    (if cut-pos
        (string-trim-right (substring sql 0 cut-pos))
      sql)))

(defun clutch--sql-rewrite-fallback (sql op arg)
  "Fallback SQL rewrite for OP with ARG when structured rewrite fails."
  (let ((trimmed (string-trim-right
                  (replace-regexp-in-string ";\\s-*\\'" "" sql))))
    (pcase op
      ('where (format "SELECT * FROM (%s) %s WHERE %s"
                      trimmed
                      (clutch--sql-derived-table-alias "_clutch_filter")
                      arg))
      ('count (format "SELECT COUNT(*) FROM (%s) %s"
                      (clutch--sql-strip-top-level-tail trimmed)
                      (clutch--sql-derived-table-alias "_clutch_count")))
      (_ (error "Unsupported rewrite op: %s" op)))))

(defun clutch--sql-rewrite (sql op &optional arg)
  "Rewrite SQL for OP with optional ARG.
Current implementation uses top-level clause awareness and keeps a
fallback path; AST-based rewrite can replace this later."
  (condition-case nil
      (let ((normalized (clutch--sql-normalize-for-rewrite sql)))
        (pcase op
          ('where
           (format "SELECT * FROM (%s) %s WHERE %s"
                   normalized
                   (clutch--sql-derived-table-alias "_clutch_filter")
                   arg))
          ('count
           (format "SELECT COUNT(*) FROM (%s) %s"
                   (clutch--sql-strip-top-level-tail normalized)
                   (clutch--sql-derived-table-alias "_clutch_count")))
          (_ (error "Unsupported rewrite op: %s" op))))
    (error
     (clutch--sql-rewrite-fallback sql op arg))))

(defun clutch--build-count-sql (sql)
  "Rewrite SQL as a COUNT(*) query.
Uses the rewrite layer so complex SQL is handled via derived-table count."
  (let ((normalized (clutch--sql-normalize-for-rewrite sql)))
    (if (clutch--sql-has-page-tail-p normalized)
        (format "SELECT COUNT(*) FROM (%s) %s"
                normalized
                (clutch--sql-derived-table-alias "_clutch_count"))
      (clutch--sql-rewrite normalized 'count))))

;;;###autoload
(defun clutch-result-count-total ()
  "Query the total row count for the current base query."
  (interactive)
  (let* ((conn clutch-connection)
         (base (clutch-result--effective-query)))
    (clutch--ensure-connection)
    (setq conn clutch-connection)
    (let* ((count-sql (clutch--build-count-sql base))
           (result (condition-case err
                       (clutch--run-db-query conn count-sql)
                     (clutch-db-error
                      (clutch--remember-problem-record
                       :buffer (current-buffer)
                       :connection conn
                       :problem (list :backend (clutch--backend-key-from-conn conn)
                                      :summary (clutch--humanize-db-error
                                                (error-message-string err))
                                      :diag (list :category "query"
                                                  :op "count"
                                                  :raw-message (error-message-string err)
                                                  :context (list :generated-sql count-sql))))
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :buffer (current-buffer)
                         :connection conn
                         :op "count"
                         :phase "error"
                         :backend (clutch--backend-key-from-conn conn)
                         :summary (error-message-string err)
                         :sql count-sql))
                      (user-error "%s"
                                  (clutch--debug-workflow-message
                                   (format "COUNT query error: %s"
                                           (clutch--humanize-db-error
                                            (error-message-string err))))))))
           (count-val (caar (clutch-db-result-rows result))))
      (setq-local clutch--page-total-rows
                  (if (numberp count-val) count-val
                    (string-to-number (format "%s" count-val))))
      (clutch--refresh-footer-line)
      (force-mode-line-update)
      (message "Total rows: %d" clutch--page-total-rows))))

;;;###autoload
(defun clutch-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql (clutch-result--effective-query)))
      (clutch--execute sql)
    (user-error "No query to re-execute")))

(defun clutch-result--revert (_ignore-auto _noconfirm)
  "Revert function for result buffer — re-executes the query."
  (clutch-result-rerun))


;;;; Sort

(defun clutch-result--sort (col-name descending)
  "Sort result rows by COL-NAME using SQL ORDER BY.
If DESCENDING, sort in descending order.
Re-executes from the first page."
  (unless clutch--result-columns
    (user-error "No result data"))
  (let* ((col-names clutch--result-columns)
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (user-error "Column %s not found" col-name))
    (let ((direction (if descending "DESC" "ASC")))
      (setq clutch--sort-column col-name)
      (setq clutch--sort-descending descending)
      (setq clutch--order-by (cons col-name direction))
      (setq clutch--page-current 0)
      (clutch--execute-page 0)
      (message "Sorted by %s %s" col-name direction))))

(defun clutch-result--read-column ()
  "Read a column name, defaulting to column at point."
  (let* ((col-names clutch--result-columns)
         (cidx (get-text-property (point) 'clutch-col-idx))
         (default (when cidx (nth cidx col-names))))
    (completing-read (if default
                         (format "Sort by column (default %s): " default)
                       "Sort by column: ")
                     col-names nil t nil nil default)))

;;;###autoload
(defun clutch-result-sort-by-column ()
  "Sort results by a column.
If the column is already sorted, toggle the direction."
  (interactive)
  (let* ((col-name (clutch-result--read-column))
         (descending (if (and clutch--sort-column
                              (string= col-name clutch--sort-column))
                         (not clutch--sort-descending)
                       nil)))
    (clutch-result--sort col-name descending)))

;;;###autoload
(defun clutch-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (clutch-result--sort (clutch-result--read-column) t))

;;;; WHERE filtering

;;;###autoload
(defun clutch-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
When columns are available, prompts to pick a column first (defaulting
to the column at point), then asks for the condition.  Enter an empty
string at the column prompt to write a raw WHERE clause; enter an
empty string at the condition prompt to clear the filter."
  (interactive)
  (unless clutch--last-query
    (user-error "No query to filter"))
  (let* ((base (or clutch--base-query
                   clutch--last-query))
         (current clutch--where-filter)
         (columns clutch--result-columns)
         (default-col (and columns clutch--header-active-col
                           (nth clutch--header-active-col columns)))
         (input
          (if (and columns (not current))
              (let* ((col (completing-read
                           (if default-col
                               (format "Filter column (default %s, empty for raw): "
                                       default-col)
                             "Filter column (empty for raw): ")
                           columns nil nil nil nil default-col))
                     (cond-str
                      (if (string-empty-p col)
                          (string-trim
                           (read-string "WHERE filter (e.g., age > 18): "))
                        (string-trim
                         (read-string
                          (format "%s (e.g., 42, 'foo', > 18, IS NULL): " col))))))
                (if (string-empty-p cond-str)
                    ""
                  (if (string-empty-p col)
                      cond-str
                    (let ((expr (if (string-match-p
                                    "\\`\\(?:[=<>!]\\|IN\\b\\|IS\\b\\|NOT\\b\\|LIKE\\b\\|BETWEEN\\b\\)"
                                    (upcase cond-str))
                                   cond-str
                                 (concat "= " cond-str))))
                      (format "%s %s" col expr)))))
            (string-trim
             (read-string
              (if current
                  (format "WHERE filter (current: %s, empty to clear): "
                          current)
                "WHERE filter (e.g., age > 18): ")
              nil nil current))))
         (filtered-sql (unless (string-empty-p input)
                         (clutch--apply-where base input))))
    (clutch--execute (or filtered-sql base)
                     clutch-connection)
    (setq clutch--base-query (when filtered-sql base))
    (setq clutch--where-filter (when filtered-sql input))
    (message (if filtered-sql
                 (format "Filter applied: WHERE %s" input)
               "Filter cleared"))))

;;;; Client-side filter

(defun clutch-result--apply-filter (input)
  "Apply INPUT as a client-side substring filter and re-render."
  (let* ((pattern  (downcase input))
         (matching (cl-loop for row in clutch--result-rows
                            when (cl-some
                                  (lambda (val)
                                    (and val
                                         (string-match-p
                                          (regexp-quote pattern)
                                          (downcase (clutch--format-value val)))))
                                  row)
                            collect row)))
    (setq clutch--filter-pattern input
          clutch--filtered-rows matching
          clutch--marked-rows nil)
    (clutch--render-result)
    (message "Filter: %d/%d rows match \"%s\""
             (length matching) (length clutch--result-rows) input)))

;;;###autoload
(defun clutch-result-filter ()
  "Filter visible rows by substring match (client-side).
Prompts for a pattern; enter empty string to clear."
  (interactive)
  (let ((input (string-trim
                (read-string
                 (if clutch--filter-pattern
                     (format "Filter (current: %s, empty to clear): "
                             clutch--filter-pattern)
                   "Filter (empty to clear): ")))))
    (if (string-empty-p input)
        (progn
          (setq clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--marked-rows nil)
          (clutch--render-result)
          (message "Filter cleared"))
      (clutch-result--apply-filter input))))

;;;; Yank cell / Copy row as INSERT

;;;; Refine minor mode

(defvar clutch-refine-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "m") #'clutch-refine-toggle-row)
    (define-key map (kbd "x") #'clutch-refine-toggle-col)
    (define-key map (kbd "RET") #'clutch-refine-confirm)
    (define-key map (kbd "C-g") #'clutch-refine-cancel)
    map)
  "Keymap for `clutch-refine-mode'.")

;;;###autoload
(define-minor-mode clutch-refine-mode
  "Transient minor mode for visually refining a rectangular selection.
\\<clutch-refine-mode-map>
\\[clutch-refine-toggle-row]: toggle row exclusion at point
\\[clutch-refine-toggle-col]: toggle column exclusion at point
\\[clutch-refine-confirm]: confirm and execute
\\[clutch-refine-cancel]: cancel"
  :keymap clutch-refine-mode-map
  :lighter " [REFINE: m=row x=col RET=ok C-g=cancel]"
  (unless clutch-refine-mode
    (clutch-refine--clear-overlays)))

(defun clutch-refine--clear-overlays ()
  "Delete all overlays created during refine mode."
  (mapc #'delete-overlay clutch--refine-overlays)
  (setq clutch--refine-overlays nil))

(defun clutch-refine--make-overlay (beg end face priority &optional tag-prop tag-val)
  "Create a refine overlay from BEG to END with FACE and PRIORITY.
Optionally tag with TAG-PROP = TAG-VAL for incremental removal."
  (let ((ov (make-overlay beg end)))
    (overlay-put ov 'face face)
    (overlay-put ov 'priority priority)
    (when tag-prop (overlay-put ov tag-prop tag-val))
    (push ov clutch--refine-overlays)))

(defun clutch-refine--init-overlays ()
  "Apply layer-1 selection overlays for the rect.  Called once on refine start."
  (clutch-refine--clear-overlays)
  (save-excursion
    (pcase-let ((`(,row-indices . ,col-indices) clutch--refine-rect))
      (dolist (cidx col-indices)
        (goto-char (point-min))
        (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
                 while match
                 do (let ((beg (prop-match-beginning match))
                          (end (prop-match-end match)))
                      (when (memq (get-text-property beg 'clutch-row-idx) row-indices)
                        (clutch-refine--make-overlay beg end 'secondary-selection 0))))))))

(defun clutch-refine--add-row-exclusion (ridx)
  "Add exclusion overlays for row RIDX within the rect's columns.
Finds the row's line first, then scans only that line — O(buffer-to-row + line)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-row-idx ridx #'eql)))
      (goto-char (prop-match-beginning match))
      (let ((bol (line-beginning-position))
            (eol (line-end-position))
            (col-set (cdr clutch--refine-rect)))
        (cl-loop with p = bol
                 while (< p eol)
                 do (let ((cidx (get-text-property p 'clutch-col-idx)))
                      (if (and cidx (memq cidx col-set))
                          (let ((end (or (next-single-property-change
                                         p 'clutch-col-idx nil eol)
                                        eol)))
                            (clutch-refine--make-overlay
                             p end '(:inherit shadow :strike-through t) 1
                             'clutch-refine-row ridx)
                            (setq p end))
                        (setq p (1+ p)))))))))

(defun clutch-refine--remove-row-exclusion (ridx)
  "Remove exclusion overlays tagged with RIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-row) ridx)
                 do (delete-overlay ov)
                 else collect ov)))

(defun clutch-refine--add-col-exclusion (cidx)
  "Add exclusion overlays for column CIDX (header + rect rows).
Scans buffer once for this column — O(buffer)."
  (save-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-header-col cidx #'eql)))
      (clutch-refine--make-overlay (prop-match-beginning match) (prop-match-end match)
                                   '(:inherit shadow :strike-through t) 1
                                   'clutch-refine-col cidx))
    (goto-char (point-min))
    (cl-loop for match = (text-property-search-forward 'clutch-col-idx cidx #'eql)
             while match
             do (let ((beg (prop-match-beginning match))
                      (end (prop-match-end match)))
                  (when (memq (get-text-property beg 'clutch-row-idx)
                              (car clutch--refine-rect))
                    (clutch-refine--make-overlay beg end
                                                 '(:inherit shadow :strike-through t) 1
                                                 'clutch-refine-col cidx))))))

(defun clutch-refine--remove-col-exclusion (cidx)
  "Remove exclusion overlays tagged with CIDX."
  (setq clutch--refine-overlays
        (cl-loop for ov in clutch--refine-overlays
                 if (eql (overlay-get ov 'clutch-refine-col) cidx)
                 do (delete-overlay ov)
                 else collect ov)))

;;;###autoload
(defun clutch-refine-toggle-row ()
  "Toggle exclusion of the row at point."
  (interactive)
  (if-let* ((ridx (clutch-result--row-idx-at-line)))
      (if (memq ridx (car clutch--refine-rect))
          (if (memq ridx clutch--refine-excluded-rows)
              (progn
                (setq clutch--refine-excluded-rows
                      (delq ridx clutch--refine-excluded-rows))
                (clutch-refine--remove-row-exclusion ridx)
                (message "Row %d included" (1+ ridx)))
            (push ridx clutch--refine-excluded-rows)
            (clutch-refine--add-row-exclusion ridx)
            (message "Row %d excluded" (1+ ridx)))
        (user-error "Row not in selection"))
    (user-error "No row at point")))

;;;###autoload
(defun clutch-refine-toggle-col ()
  "Toggle exclusion of the column at point."
  (interactive)
  (if-let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                      (get-text-property (point) 'clutch-header-col))))
      (if (memq cidx (cdr clutch--refine-rect))
          (if (memq cidx clutch--refine-excluded-cols)
              (progn
                (setq clutch--refine-excluded-cols
                      (delq cidx clutch--refine-excluded-cols))
                (clutch-refine--remove-col-exclusion cidx)
                (message "Column \"%s\" included" (nth cidx clutch--result-columns)))
            (push cidx clutch--refine-excluded-cols)
            (clutch-refine--add-col-exclusion cidx)
            (message "Column \"%s\" excluded" (nth cidx clutch--result-columns)))
        (user-error "Column not in selection"))
    (user-error "No column at point")))

;;;###autoload
(defun clutch-refine-confirm ()
  "Confirm the current refine selection and execute the callback."
  (interactive)
  (let* ((row-indices (cl-loop for ridx in (car clutch--refine-rect)
                               unless (memq ridx clutch--refine-excluded-rows)
                               collect ridx))
         (col-indices (cl-loop for cidx in (cdr clutch--refine-rect)
                               unless (memq cidx clutch--refine-excluded-cols)
                               collect cidx)))
    (unless row-indices
      (user-error "No rows left after exclusion"))
    (unless col-indices
      (user-error "No columns left after exclusion"))
    (let ((cb clutch--refine-callback)
          (final-rect (cons row-indices col-indices)))
      (clutch-refine--clear-overlays)
      (clutch-refine-mode -1)
      (setq mode-line-format clutch--refine-saved-mode-line
            clutch--refine-rect nil
            clutch--refine-excluded-rows nil
            clutch--refine-excluded-cols nil
            clutch--refine-callback nil
            clutch--refine-saved-mode-line nil)
      (funcall cb final-rect))))

;;;###autoload
(defun clutch-refine-cancel ()
  "Cancel refine mode without executing the callback."
  (interactive)
  (clutch-refine--clear-overlays)
  (clutch-refine-mode -1)
  (setq mode-line-format clutch--refine-saved-mode-line
        clutch--refine-rect nil
        clutch--refine-excluded-rows nil
        clutch--refine-excluded-cols nil
        clutch--refine-callback nil
        clutch--refine-saved-mode-line nil)
  (message "Refine cancelled"))

(defun clutch-result--start-refine (rect callback)
  "Enter refine mode for RECT with CALLBACK called with final rect on confirm.
RECT is (ROW-INDICES . COL-INDICES)."
  (deactivate-mark)
  (setq-local clutch--refine-rect rect
              clutch--refine-excluded-rows nil
              clutch--refine-excluded-cols nil
              clutch--refine-callback callback
              clutch--refine-saved-mode-line mode-line-format
              mode-line-format
              (concat
               (propertize " " 'display '(space :align-to 0))
               (propertize "REFINE  " 'face 'font-lock-warning-face)
               (propertize "m" 'face 'font-lock-keyword-face)
               (propertize " row   " 'face 'font-lock-comment-face)
               (propertize "x" 'face 'font-lock-keyword-face)
               (propertize " col   " 'face 'font-lock-comment-face)
               (propertize "RET" 'face 'font-lock-keyword-face)
               (propertize " confirm   " 'face 'font-lock-comment-face)
               (propertize "C-g" 'face 'font-lock-keyword-face)
               (propertize " cancel" 'face 'font-lock-comment-face)))
  (clutch-refine-mode 1)
  (clutch-refine--init-overlays))

(defun clutch-result-copy (format &optional rect)
  "Unified copy entry point for result buffer.
FORMAT is one of symbols: `tsv', `csv', `insert', `update'.
When RECT is non-nil, use it as precomputed rectangle bounds.  If region
is active, copy rectangle bounds from region endpoints.
Otherwise, copy the current cell."
  (pcase format
    ('tsv
     (if rect
         (clutch-result--yank-rectangle-cells rect)
       (if (use-region-p)
           (clutch-result--yank-region-cells)
         (pcase-let* ((`(,_ridx ,_cidx ,val) (or (clutch-result--cell-at-point)
                                               (user-error "No cell at point"))))
           (clutch-result--yank-cell-value val)))))
    ('csv
     (clutch-result--copy-rows-as-csv rect))
    ('insert
     (clutch-result--copy-rows-as-insert rect))
    ('update
     (clutch-result--copy-rows-as-update rect))
    (_
     (user-error "Unsupported copy format: %s" format))))

(defun clutch-result--copy-fmt (fmt)
  "Copy in FMT, entering refine mode first if --refine switch is set."
  (if (transient-arg-value "--refine" (transient-args 'clutch-result-copy-dispatch))
      (progn
        (unless (use-region-p)
          (user-error "Set a region before using refine mode"))
        (clutch-result--start-refine
         (clutch-result--region-rectangle-indices)
         (lambda (final-rect) (clutch-result-copy fmt final-rect))))
    (clutch-result-copy fmt)))

;;;###autoload
(defun clutch-result-copy-tsv ()
  "Copy as TSV."
  (interactive)
  (clutch-result--copy-fmt 'tsv))

;;;###autoload
(defun clutch-result-copy-csv ()
  "Copy as CSV with header."
  (interactive)
  (clutch-result--copy-fmt 'csv))

;;;###autoload
(defun clutch-result-copy-insert ()
  "Copy as INSERT statements."
  (interactive)
  (clutch-result--copy-fmt 'insert))

;;;###autoload
(defun clutch-result-copy-update ()
  "Copy as UPDATE statements."
  (interactive)
  (clutch-result--copy-fmt 'update))

(transient-define-prefix clutch-result-copy-dispatch ()
  "Copy result buffer data.
Enable --refine to exclude rows/columns interactively before copying
\(requires an active region set with C-x SPC or mouse)."
  ["Options"
   :pad-keys t
   ("-r" "Exclude rows/cols interactively (needs region)" "--refine")]
  ["Copy as"
   :pad-keys t
   ("t" "TSV"             clutch-result-copy-tsv)
   ("c" "CSV with header" clutch-result-copy-csv)
   ("i" "INSERT"          clutch-result-copy-insert)
   ("u" "UPDATE"          clutch-result-copy-update)])


(defun clutch-result--yank-cell-value (val)
  "Copy VAL to kill ring and show a compact preview message."
  (let ((text (clutch--format-value val)))
    (kill-new text)
    (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…"))))

(defun clutch-result--cell-at-or-near (pos)
  "Return cell triple at POS, or nearest cell on the same line."
  (or (clutch-result--cell-at pos)
      (save-excursion
        (goto-char pos)
        (let ((bol (line-beginning-position))
              (eol (line-end-position)))
          (or (cl-loop for p downfrom (max bol (1- pos)) to bol
                       thereis (clutch-result--cell-at p))
              (cl-loop for p from (min eol (1+ pos)) to eol
                       thereis (clutch-result--cell-at p)))))))

(defun clutch-result--region-cells ()
  "Return cells in active region as a rectangle of (ROW-IDX COL-IDX VALUE)."
  (pcase-let* ((`(,r1 ,c1 ,_v1) (or (clutch-result--cell-at-or-near (region-beginning))
                                    (user-error "No cell at region start")))
               (`(,r2 ,c2 ,_v2) (or (clutch-result--cell-at-or-near (max (point-min)
                                                                       (1- (region-end))))
                                    (user-error "No cell at region end")))
               (row-min (min r1 r2))
               (row-max (max r1 r2))
               (col-min (min c1 c2))
               (col-max (max c1 c2))
               (rows clutch--result-rows))
    (cl-loop for ridx from row-min to row-max
             append
             (let ((row (nth ridx rows)))
               (cl-loop for cidx from col-min to col-max
                        collect (list ridx cidx (nth cidx row)))))))

(defun clutch-result--region-rectangle-indices ()
  "Return rectangle row/column indices from active region.
Result is a cons cell (ROW-INDICES . COL-INDICES)."
  (unless (use-region-p)
    (user-error "Set a region to select rows and columns"))
  (pcase-let* ((`(,r1 ,c1 ,_v1) (or (clutch-result--cell-at-or-near (region-beginning))
                                    (user-error "No cell at region start")))
               (`(,r2 ,c2 ,_v2) (or (clutch-result--cell-at-or-near
                                     (max (point-min) (1- (region-end))))
                                    (user-error "No cell at region end")))
               (row-min (min r1 r2))
               (row-max (max r1 r2))
               (col-min (min c1 c2))
               (col-max (max c1 c2)))
    (cons (cl-loop for ridx from row-min to row-max collect ridx)
          (cl-loop for cidx from col-min to col-max collect cidx))))

(defun clutch-result--yank-region-cells ()
  "Copy cell values from region as TSV-like text."
  (unless (use-region-p)
    (user-error "Set a region to copy multiple cells"))
  (let* ((cells (clutch-result--region-cells))
         (lines nil)
         (current-row nil)
         current-values)
    (unless cells
      (user-error "No cells in region"))
    (dolist (cell cells)
      (pcase-let ((`(,ridx ,_cidx ,val) cell))
        (if (or (null current-row) (= ridx current-row))
            (progn
              (setq current-row ridx)
              (push (clutch--format-value val) current-values))
          (push (string-join (nreverse current-values) "\t") lines)
          (setq current-row ridx
                current-values (list (clutch--format-value val))))))
    (when current-values
      (push (string-join (nreverse current-values) "\t") lines))
    (let ((text (string-join (nreverse lines) "\n")))
      (kill-new text)
      (deactivate-mark)
      (message "Copied %d cell%s from region"
               (length cells)
               (if (= (length cells) 1) "" "s")))))

(defun clutch-result--yank-rectangle-cells (rect)
  "Copy cells from RECT as TSV-like text."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (cells
                (cl-loop for ridx in row-indices
                         append
                         (let ((row (nth ridx clutch--result-rows)))
                           (cl-loop for cidx in col-indices
                                    collect (list ridx cidx (nth cidx row)))))))
    (let ((lines nil)
          (current-row nil)
          current-values)
      (dolist (cell cells)
        (pcase-let ((`(,ridx ,_cidx ,val) cell))
          (if (or (null current-row) (= ridx current-row))
              (progn
                (setq current-row ridx)
                (push (clutch--format-value val) current-values))
            (push (string-join (nreverse current-values) "\t") lines)
            (setq current-row ridx
                  current-values (list (clutch--format-value val))))))
      (when current-values
        (push (string-join (nreverse current-values) "\t") lines))
      (let ((text (string-join (nreverse lines) "\n")))
        (kill-new text)
        (message "Copied %d cell%s from region"
                 (length cells)
                 (if (= (length cells) 1) "" "s"))))))

(defun clutch-result--aggregate-target (&optional rect)
  "Return aggregate target as (ROW-INDICES COL-INDICES).
When RECT is non-nil, use it directly.  With region: use all selected columns.
Without region: use current cell."
  (if (or rect (use-region-p))
      (pcase-let* ((`(,row-indices . ,col-indices)
                    (or rect (clutch-result--region-rectangle-indices))))
        (unless col-indices
          (user-error "No columns selected for aggregate"))
        (list row-indices col-indices))
    (pcase-let* ((`(,ridx ,cidx ,_val) (or (clutch-result--cell-at-point)
                                           (user-error "No cell at point"))))
      (list (list ridx) (list cidx)))))

(defun clutch-result--parse-number (val)
  "Parse VAL into a number or return nil."
  (cond
   ((numberp val) val)
   ((stringp val)
    (let ((s (string-trim val)))
      (when (and (not (string-empty-p s))
                 (string-match-p
                  "\\`[+-]?\\(?:[0-9]+\\(?:\\.[0-9]*\\)?\\|\\.[0-9]+\\)\\'" s))
        (string-to-number s))))
   (t nil)))

(defun clutch-result--compute-aggregate (row-indices col-indices)
  "Compute aggregate stats for ROW-INDICES across COL-INDICES."
  (let* ((rows (length row-indices))
         (cells (* rows (length col-indices)))
         (count 0)
        (sum 0.0)
        min-val
        max-val)
    (dolist (ridx row-indices)
      (let ((row (nth ridx clutch--result-rows)))
        (dolist (cidx col-indices)
          (let ((num (clutch-result--parse-number (nth cidx row))))
            (when num
              (setq count (1+ count)
                    sum (+ sum num))
              (setq min-val (if min-val (min min-val num) num))
              (setq max-val (if max-val (max max-val num) num)))))))
    (list :rows rows
          :cells cells
          :count count
          :skipped (- cells count)
          :sum sum
          :avg (if (> count 0) (/ sum count) 0)
          :min min-val
          :max max-val)))

(defun clutch-result--format-aggregate-summary (label stats)
  "Return aggregate summary string for LABEL with STATS."
  (let ((count (plist-get stats :count)))
    (if (> count 0)
        (format "Aggregate [%s]: sum=%g avg=%g min=%g max=%g [rows=%d cells=%d skipped=%d]"
                label
                (plist-get stats :sum)
                (plist-get stats :avg)
                (plist-get stats :min)
                (plist-get stats :max)
                (plist-get stats :rows)
                (plist-get stats :cells)
                (plist-get stats :skipped))
      (format "Aggregate [%s]: n/a [rows=%d cells=%d skipped=%d]"
              label
              (plist-get stats :rows)
              (plist-get stats :cells)
              (plist-get stats :skipped)))))

(defun clutch-result--do-aggregate (rect)
  "Perform aggregate on RECT (ROW-INDICES . COL-INDICES) and update display."
  (pcase-let* ((`(,row-indices . ,col-indices) rect)
               (label (if (= (length col-indices) 1)
                          (nth (car col-indices) clutch--result-columns)
                        "selection"))
               (stats (clutch-result--compute-aggregate row-indices col-indices))
               (summary (clutch-result--format-aggregate-summary label stats)))
    (setq-local clutch--aggregate-summary
                (list :label label
                      :rows (plist-get stats :rows)
                      :cells (plist-get stats :cells)
                      :skipped (plist-get stats :skipped)
                      :sum (plist-get stats :sum)
                      :avg (plist-get stats :avg)
                      :min (plist-get stats :min)
                      :max (plist-get stats :max)
                      :count (plist-get stats :count)))
    (clutch--refresh-display)
    (kill-new summary)))

;;;###autoload
(defun clutch-result-aggregate (&optional refine)
  "Aggregate numeric values from selected columns or current cell.
With prefix arg REFINE and an active region, enter visual refine mode."
  (interactive "P")
  (if (and refine (use-region-p))
      (clutch-result--start-refine
       (clutch-result--region-rectangle-indices)
       #'clutch-result--do-aggregate)
    (pcase-let* ((`(,row-indices ,col-indices)
                  (clutch-result--aggregate-target nil)))
      (clutch-result--do-aggregate (cons row-indices col-indices)))))

(defun clutch--render-view-buffer (buffer val setup-fn)
  "Render string VAL into BUFFER, then call SETUP-FN there.
SETUP-FN is called with no args; it should activate a mode and may also
reformat the current buffer."
  (with-current-buffer buffer
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert val)
      (funcall setup-fn)
      (goto-char (point-min))
      (setq buffer-read-only t)))
  buffer)

(defun clutch--view-in-buffer (val buf-name setup-fn)
  "Insert string VAL into BUF-NAME, call SETUP-FN, then pop to it."
  (pop-to-buffer
   (clutch--render-view-buffer (get-buffer-create buf-name) val setup-fn)))

(defun clutch--setup-json-view-buffer ()
  "Enable JSON display mode for the current buffer."
  (condition-case nil (json-pretty-print-buffer) (error nil))
  (cond ((fboundp 'json-ts-mode) (json-ts-mode))
        ((fboundp 'json-mode)    (json-mode))
        (t                       (special-mode))))

(defun clutch--decode-xml-char-refs-string (text)
  "Return TEXT with numeric XML character references decoded for display."
  (replace-regexp-in-string
   "&#\\(x[[:xdigit:]]+\\|X[[:xdigit:]]+\\|[[:digit:]]+\\);"
   (lambda (ref)
     (let* ((body (substring ref 2 -1))
            (hex (memq (aref body 0) '(?x ?X)))
            (code (string-to-number (if hex (substring body 1) body)
                                    (if hex 16 10)))
            (char (and (> code 0) (decode-char 'ucs code))))
       (if char
           (char-to-string char)
         ref)))
   text t t))

(defun clutch--decode-xml-char-refs-in-buffer ()
  "Decode numeric XML character references in the current buffer for display."
  (let ((decoded (clutch--decode-xml-char-refs-string (buffer-string))))
    (unless (equal decoded (buffer-string))
      (erase-buffer)
      (insert decoded))))

(defun clutch--setup-xml-view-buffer (val &optional quiet)
  "Pretty-print XML VAL in the current buffer and enable XML mode.
When QUIET is non-nil, suppress informational fallback messages."
  (if (executable-find "xmllint")
      (let ((raw (buffer-string))
            (err-file (make-temp-file "clutch-xmllint-")))
        (unwind-protect
            (unless (eq 0 (call-process-region
                           (point-min) (point-max)
                           "xmllint" t (list t err-file) nil "--format" "-"))
              (erase-buffer)
              (insert raw)
              (unless quiet
                (message "xmllint: %s"
                         (string-trim (with-temp-buffer
                                        (insert-file-contents err-file)
                                        (buffer-string))))))
          (delete-file err-file)))
    (unless quiet
      (message "xmllint not found — showing raw XML without formatting")))
  ;; Readability matters more than preserving numeric character references in
  ;; the transient viewer buffer; keep the raw XML value unchanged elsewhere.
  (clutch--decode-xml-char-refs-in-buffer)
  (cond ((fboundp 'nxml-mode) (nxml-mode))
        ((fboundp 'xml-mode) (xml-mode))
        (t (special-mode)))
  (setq-local header-line-format
              (format " XML  |  %d bytes" (string-bytes val)))
  ;; Force fontification so XML is highlighted immediately in popup buffers.
  (when (fboundp 'font-lock-ensure)
    (font-lock-ensure (point-min) (point-max)))
  (when (fboundp 'jit-lock-fontify-now)
    (jit-lock-fontify-now (point-min) (point-max))))

(defun clutch--setup-plain-view-buffer ()
  "Enable plain text view mode for the current buffer."
  (special-mode))

(defun clutch--view-json-value (val)
  "Display VAL as formatted JSON in a pop-up buffer."
  (unless (and (stringp val) (not (string-empty-p val)))
    (user-error "No JSON value at point"))
  (clutch--view-in-buffer val "*clutch-json*" #'clutch--setup-json-view-buffer))

(defun clutch--view-xml-value (val)
  "Display VAL as formatted XML in a pop-up buffer.
Uses xmllint for pretty-printing when available; shows a message otherwise."
  (unless (and (stringp val) (not (string-empty-p val)))
    (user-error "No XML value at point"))
  (clutch--view-in-buffer val "*clutch-xml*"
    (lambda () (clutch--setup-xml-view-buffer val))))

(defun clutch--blob-bytes (val)
  "Return a unibyte string for blob-like VAL."
  (cond
   ((stringp val) (encode-coding-string val 'binary))
   ((vectorp val) (apply #'unibyte-string (append val nil)))
   (t (encode-coding-string (clutch--format-value val) 'binary))))

(defun clutch--blob-hexdump-lines (bytes &optional max-bytes)
  "Return hex dump lines for BYTES, up to MAX-BYTES bytes."
  (let* ((total (length bytes))
         (limit (min total (or max-bytes total)))
         (offset 0)
         lines)
    (while (< offset limit)
      (let* ((line-len (min 16 (- limit offset)))
             (hex-parts nil)
             (ascii-parts nil))
        (dotimes (i line-len)
          (let* ((b (aref bytes (+ offset i)))
                 (ch (if (and (>= b 32) (<= b 126)) b ?.)))
            (push (format "%02x" b) hex-parts)
            (push (char-to-string ch) ascii-parts)))
        (push (format "%08x  %-47s  |%s|"
                      offset
                      (mapconcat #'identity (nreverse hex-parts) " ")
                      (mapconcat #'identity (nreverse ascii-parts) ""))
              lines)
        (setq offset (+ offset line-len))))
    (nreverse lines)))

(defun clutch--blob-likely-text-p (bytes &optional sample-size)
  "Return non-nil when BYTES appears mostly text-like within SAMPLE-SIZE bytes."
  (let* ((n (min (length bytes) (or sample-size 512)))
         (printable 0))
    (if (= n 0)
        t
      (dotimes (i n)
        (let ((b (aref bytes i)))
          (when (or (and (>= b 32) (<= b 126))
                    (memq b '(9 10 13)))
            (setq printable (1+ printable)))))
      (>= (/ (float printable) n) 0.85))))

(defun clutch--blob-view-string (val)
  "Build a concise DataGrip-like display string for blob VAL."
  (let* ((bytes (clutch--blob-bytes val))
         (size (length bytes))
         (text-like (clutch--blob-likely-text-p bytes))
         (max-bytes (if text-like 1024 256))
         (shown (min size max-bytes))
         (truncated (> size max-bytes)))
    (concat
     (format "BLOB size: %d bytes\n\n" size)
     (if text-like
         (let ((preview (condition-case nil
                            (decode-coding-string (substring bytes 0 shown) 'utf-8 t)
                          (error ""))))
           (concat "Text preview:\n"
                   (if (string-empty-p preview) "<empty>" preview)))
       (concat "Hex preview:\n"
               (mapconcat #'identity
                          (clutch--blob-hexdump-lines bytes max-bytes)
                          "\n")))
     (if truncated
         (format "\n\n... truncated, showing first %d bytes" max-bytes)
       ""))))

(defun clutch--view-binary-as-string (val)
  "Display blob-like VAL in a DataGrip-style preview buffer."
  (let ((s (clutch--blob-view-string val)))
    (when (string-empty-p s)
      (user-error "No value at point"))
    (clutch--view-in-buffer s "*clutch-blob*"
      (lambda () (special-mode)))))

(defun clutch--view-plain-value (val)
  "Display VAL as plain text in a pop-up buffer."
  (let ((s (clutch--format-value val)))
    (clutch--view-in-buffer s "*clutch-value*" #'clutch--setup-plain-view-buffer)))

(defun clutch--view-spec (val col-def &optional quiet)
  "Return rendering spec for VAL with column metadata COL-DEF.
When QUIET is non-nil, suppress viewer fallback chatter where possible."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json)
          (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))
      (list :kind "JSON"
            :content (if (stringp val) val
                       (clutch--json-value-to-string val))
            :setup #'clutch--setup-json-view-buffer))
     ((clutch--xml-like-string-p val)
      (list :kind "XML"
            :content val
            :setup (lambda () (clutch--setup-xml-view-buffer val quiet))))
     ((eq cat 'blob)
      (list :kind "BLOB"
            :content (clutch--blob-view-string val)
            :setup #'clutch--setup-plain-view-buffer))
     (t
      (list :kind "Value"
            :content (clutch--format-value val)
            :setup #'clutch--setup-plain-view-buffer)))))

(defun clutch--dispatch-view (val col-def)
  "Open the appropriate viewer for VAL given column metadata COL-DEF.
Dispatch order: JSON content → JSON viewer; XML content → XML viewer;
blob type with non-text value → binary string; otherwise plain text."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json)
          (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))
      ;; Pass raw string directly when available — avoids json-serialize
      ;; escaping non-ASCII characters (e.g. CJK) as \uXXXX.
      (clutch--view-json-value (if (stringp val) val
                                 (clutch--json-value-to-string val))))
     ((clutch--xml-like-string-p val)
      (clutch--view-xml-value val))
     ((eq cat 'blob)
      (clutch--view-binary-as-string val))
     (t
      (clutch--view-plain-value val)))))

(defvar clutch--live-view-follow-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "f" #'clutch--live-view-toggle-freeze)
    (define-key map "g" #'clutch--live-view-refresh)
    (define-key map "q" #'clutch--live-view-quit)
    map)
  "Keymap for `clutch--live-view-follow-mode'.")

(define-minor-mode clutch--live-view-follow-mode
  "Minor mode for a clutch live-follow value viewer."
  :init-value nil
  :lighter " LiveView"
  :keymap clutch--live-view-follow-mode-map)

(defun clutch--live-view-current-context ()
  "Return the current live-view source context, or nil when none is available."
  (cond
   ((derived-mode-p 'clutch-result-mode)
    (when-let* ((ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'result
            :cell-id (list 'result (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (length (or clutch--filtered-rows clutch--result-rows))
            :table clutch--result-source-table
            :column (nth cidx clutch--result-columns)
            :col-def (nth cidx clutch--result-column-defs)
            :value (get-text-property (point) 'clutch-full-value))))
   ((derived-mode-p 'clutch-record-mode)
    (when-let* ((result-buf clutch-record--result-buffer)
                ((buffer-live-p result-buf))
                (ridx (get-text-property (point) 'clutch-row-idx))
                (cidx (get-text-property (point) 'clutch-col-idx)))
      (list :source-buffer (current-buffer)
            :source-kind 'record
            :cell-id (list 'record (buffer-chars-modified-tick) ridx cidx)
            :ridx ridx
            :cidx cidx
            :row-count (with-current-buffer result-buf
                         (length clutch--result-rows))
            :table (with-current-buffer result-buf clutch--result-source-table)
            :column (with-current-buffer result-buf
                      (nth cidx clutch--result-columns))
            :col-def (with-current-buffer result-buf
                       (nth cidx clutch--result-column-defs))
            :value (get-text-property (point) 'clutch-full-value))))))

(defun clutch--live-view-header (kind context frozen)
  "Return live viewer header for KIND using CONTEXT and FROZEN state."
  (let* ((table (plist-get context :table))
         (column (or (plist-get context :column) "?"))
         (label (if table (format "%s.%s" table column) column))
         (ridx (plist-get context :ridx))
         (cidx (plist-get context :cidx))
         (row-count (or (plist-get context :row-count) 0)))
    (format " %s | %s | %s | R%d/%d C%d | f freeze  g refresh  q quit"
            kind
            (if frozen "FROZEN" "FOLLOW")
            label
            (1+ ridx)
            row-count
            (1+ cidx))))

(defun clutch--live-view-detach-source (source-buf &optional viewer-buf)
  "Detach live viewer state from SOURCE-BUF.
When VIEWER-BUF is non-nil, only detach if SOURCE-BUF points at VIEWER-BUF."
  (when (buffer-live-p source-buf)
    (with-current-buffer source-buf
      (when (or (null viewer-buf)
                (eq clutch--live-view-buffer viewer-buf))
        (setq-local clutch--live-view-buffer nil)
        (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
        (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
        (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t)))))

(defun clutch--live-view-buffer-killed ()
  "Clean up source-buffer hooks when the live viewer is killed."
  (let ((viewer (current-buffer))
        (source clutch--live-view-source-buffer))
    (setq-local clutch--live-view-source-buffer nil)
    (clutch--live-view-detach-source source viewer)))

(defun clutch--live-view-source-killed ()
  "Dispose of any live viewer attached to the current source buffer."
  (when (buffer-live-p clutch--live-view-buffer)
    (let ((viewer clutch--live-view-buffer))
      (setq-local clutch--live-view-buffer nil)
      (when (buffer-live-p viewer)
        (with-current-buffer viewer
          (setq-local clutch--live-view-source-buffer nil))
        (kill-buffer viewer))))
  (remove-hook 'post-command-hook #'clutch--live-view-source-post-command t)
  (remove-hook 'kill-buffer-hook #'clutch--live-view-source-killed t)
  (remove-hook 'change-major-mode-hook #'clutch--live-view-source-killed t))

(defun clutch--render-live-view (viewer-buf context &optional force)
  "Render CONTEXT into VIEWER-BUF.
When FORCE is non-nil, refresh even if the source cell has not changed."
  (with-current-buffer viewer-buf
    (let ((frozen clutch--live-view-frozen)
          (source (plist-get context :source-buffer))
          (cell-id (plist-get context :cell-id)))
      (unless (and (not force)
                   (equal cell-id clutch--live-view-source-cell-id))
        (pcase-let* ((`(:kind ,kind :content ,content :setup ,setup)
                      (clutch--view-spec (plist-get context :value)
                                         (plist-get context :col-def)
                                         t)))
          (clutch--render-view-buffer viewer-buf content setup)
          (setq-local clutch--live-view-source-buffer source)
          (setq-local clutch--live-view-source-cell-id cell-id)
          (setq-local clutch--live-view-frozen frozen)
          (setq-local header-line-format
                      (clutch--live-view-header kind context frozen))
          (clutch--live-view-follow-mode 1))))))

(defun clutch--live-view-source-post-command ()
  "Refresh the attached live viewer after point moves in a source buffer."
  (if (not (buffer-live-p clutch--live-view-buffer))
      (clutch--live-view-detach-source (current-buffer))
    (let ((viewer clutch--live-view-buffer)
          (context (clutch--live-view-current-context)))
      (with-current-buffer viewer
        (unless clutch--live-view-frozen
          (when context
            (clutch--render-live-view viewer context)))))))

(defun clutch--live-view-refresh ()
  "Refresh the current clutch live viewer from its source point."
  (interactive)
  (unless (buffer-live-p clutch--live-view-source-buffer)
    (user-error "Live viewer source buffer is no longer available"))
  (when-let* ((context (with-current-buffer clutch--live-view-source-buffer
                         (clutch--live-view-current-context))))
    (clutch--render-live-view (current-buffer) context t)
    (message "Live viewer refreshed")))

(defun clutch--live-view-toggle-freeze ()
  "Toggle whether the live viewer follows source-buffer point changes."
  (interactive)
  (setq-local clutch--live-view-frozen (not clutch--live-view-frozen))
  (setq-local header-line-format
              (replace-regexp-in-string
               (if clutch--live-view-frozen "FOLLOW" "FROZEN")
               (if clutch--live-view-frozen "FROZEN" "FOLLOW")
               (format "%s" header-line-format)
               t t))
  (unless clutch--live-view-frozen
    (clutch--live-view-refresh))
  (message "Live viewer %s"
           (if clutch--live-view-frozen "frozen" "following point")))

(defun clutch--live-view-quit ()
  "Close the current clutch live viewer."
  (interactive)
  (kill-buffer (current-buffer)))

(defun clutch--open-live-view ()
  "Open or refresh a live-follow viewer for the current clutch cell."
  (let* ((source (current-buffer))
         (context (or (clutch--live-view-current-context)
                      (user-error "No cell at point")))
         (viewer (get-buffer-create "*clutch-live-view*")))
    (with-current-buffer viewer
      (when (and (buffer-live-p clutch--live-view-source-buffer)
                 (not (eq clutch--live-view-source-buffer source)))
        (clutch--live-view-detach-source clutch--live-view-source-buffer viewer))
      (add-hook 'kill-buffer-hook #'clutch--live-view-buffer-killed nil t))
    (setq-local clutch--live-view-buffer viewer)
    (add-hook 'post-command-hook #'clutch--live-view-source-post-command nil t)
    (add-hook 'kill-buffer-hook #'clutch--live-view-source-killed nil t)
    (add-hook 'change-major-mode-hook #'clutch--live-view-source-killed nil t)
    (with-current-buffer viewer
      (setq-local clutch--live-view-frozen nil)
      (setq-local clutch--live-view-source-buffer source))
    (clutch--render-live-view viewer context t)
    (display-buffer viewer '(display-buffer-at-bottom . ((window-height . 0.33))))
    viewer))

;;;###autoload
(defun clutch-result-view-value ()
  "Display the cell value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (pcase-let ((`(,_ridx ,cidx ,val) (or (clutch-result--cell-at-point)
                                         (user-error "No cell at point"))))
    (clutch--dispatch-view val (nth cidx clutch--result-column-defs))))

;;;###autoload
(defun clutch-result-live-view-value ()
  "Open a live-follow viewer for the result cell at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-result-shell-command-on-cell (command)
  "Pipe the cell value at point through shell COMMAND and display the output."
  (interactive "sShell command on cell: ")
  (pcase-let ((`(,_ridx ,_cidx ,val) (or (clutch-result--cell-at-point)
                                          (user-error "No cell at point"))))
    (let ((input (if (stringp val)
                     val
                   (clutch--format-value val))))
      (clutch--view-in-buffer
       (with-temp-buffer
         (insert input)
         (shell-command-on-region (point-min) (point-max) command t t)
         (buffer-string))
       "*clutch-shell-output*"
       #'special-mode))))

(defun clutch-result--build-insert-statements (indices col-indices table)
  "Return INSERT statement strings for INDICES rows using COL-INDICES into TABLE."
  (let* ((conn      clutch-connection)
         (col-names (mapcar (lambda (i) (nth i clutch--result-columns)) col-indices))
         (rows      clutch--result-rows)
         (cols      (mapconcat (lambda (c) (clutch-db-escape-identifier conn c))
                               col-names ", ")))
    (cl-loop for ridx in indices
             for row = (nth ridx rows)
             for vals = (mapcar (lambda (i) (nth i row)) col-indices)
             collect (format "INSERT INTO %s (%s) VALUES (%s);"
                             (clutch-db-escape-identifier conn table)
                             cols
                             (mapconcat #'clutch--value-to-literal vals ", ")))))

(defconst clutch--insert-placeholder-table "MY_TABLE"
  "Placeholder target table used for ambiguous INSERT copy/export output.")

(defun clutch--next-top-level-clause-position (sql start patterns)
  "Return earliest top-level clause match in SQL after START for PATTERNS.
PATTERNS is a list of case-insensitive regex fragments passed to
`clutch-db-sql-find-top-level-clause'.  Return nil when none are found."
  (car (sort (delq nil
                   (mapcar (lambda (pattern)
                             (clutch-db-sql-find-top-level-clause sql pattern start))
                           patterns))
             #'<)))

(defun clutch--simple-insert-source-table (&optional sql)
  "Return the source table for simple single-table INSERT output, or nil.
SQL defaults to the current result query.  Joined, derived, UNION, and
other ambiguous result queries return nil so INSERT copy/export can fall
back to a placeholder table name instead of inventing a wrong target."
  (let* ((sql (or sql clutch--last-query))
         (normalized (and sql
                          (string-trim-right
                           (replace-regexp-in-string ";\\s-*\\'" "" sql)))))
    (when normalized
      (let* ((case-fold-search t)
             (masked (clutch-db-sql-mask-literal-or-comment normalized))
             (from-pos (clutch-db-sql-find-top-level-clause masked "FROM")))
        (when (and from-pos
                   (not (clutch-db-sql-find-top-level-clause masked "JOIN"))
                   (not (clutch-db-sql-find-top-level-clause
                         masked "UNION\\b\\(?:\\s-+ALL\\b\\)?"))
                   (not (clutch-db-sql-find-top-level-clause masked "INTERSECT"))
                   (not (clutch-db-sql-find-top-level-clause masked "EXCEPT"))
                   (string-match "\\bFROM\\b" masked from-pos))
          (let* ((from-body-start (match-end 0))
                 (from-body-end
                  (or (clutch--next-top-level-clause-position
                       masked from-body-start
                       '("WHERE" "GROUP\\s-+BY" "HAVING" "ORDER\\s-+BY"
                         "LIMIT" "OFFSET" "FETCH" "FOR"))
                      (length masked)))
                 (from-body (string-trim
                             (substring masked from-body-start from-body-end))))
            (when (and (not (string-prefix-p "(" from-body))
                       (not (string-match-p "," from-body)))
              (clutch-result--table-from-sql normalized))))))))

(defun clutch--insert-target-table ()
  "Return a safe target table name for INSERT copy/export.
Simple single-table result sets use the detected table name.  Ambiguous
results use `clutch--insert-placeholder-table' instead."
  (or (clutch--simple-insert-source-table)
      clutch--insert-placeholder-table))

(defun clutch-result--selected-update-col-indices (pk-indices col-indices op)
  "Return non-PK update column indices from COL-INDICES for OP.
PK-INDICES are excluded."
  (let ((set-col-indices
         (cl-loop for cidx in col-indices
                  unless (memq cidx pk-indices)
                  collect cidx)))
    (unless set-col-indices
      (user-error "Cannot %s: no non-primary-key columns selected" op))
    set-col-indices))

(defun clutch-result--ensure-update-source-columns (table col-indices op)
  "Ensure COL-INDICES map to writable source columns for TABLE during OP."
  (let* ((details (or (clutch--ensure-column-details clutch-connection table t)
                      (user-error "Cannot %s: source column metadata is unavailable"
                                  op)))
         (detail-map
          (cl-loop for detail in details
                   collect (cons (plist-get detail :name) detail)))
         (invalid (cl-loop for cidx in col-indices
                           for col-name = (nth cidx clutch--result-columns)
                           for detail = (cdr (assoc col-name detail-map))
                           unless (and detail (not (plist-get detail :generated)))
                           collect col-name)))
    (when invalid
      (user-error "Cannot %s: selected columns are not writable source columns: %s"
                  op
                  (string-join invalid ", ")))))

(defun clutch-result--build-update-statements-for-rows (rows col-indices op)
  "Return UPDATE preview statements for ROWS using COL-INDICES.
OP is a short operation description used in user-facing error messages."
  (let* ((table (clutch--result-source-table-or-user-error op))
         (pk-indices (clutch--result-pk-indices-or-user-error table op))
         (set-col-indices (clutch-result--selected-update-col-indices
                           pk-indices col-indices op))
         (pk-names (mapcar (lambda (i) (nth i clutch--result-columns)) pk-indices))
         (col-names clutch--result-columns)
         statements)
    (clutch-result--ensure-update-source-columns table set-col-indices op)
    (dolist (row rows)
      (let* ((pk-vec (clutch-result--extract-pk-vec row pk-indices))
             (edits (cl-loop for cidx in set-col-indices
                             collect (cons cidx (nth cidx row)))))
        (push (clutch-result--build-update-stmt
               table pk-vec edits col-names pk-names)
              statements)))
    (clutch-result--render-statements (nreverse statements))))

(defun clutch-result--copy-rows-as-insert (&optional rect)
  "Copy row(s) as INSERT statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((rect (or rect
                   (if (use-region-p)
                       (clutch-result--region-rectangle-indices)
                 (pcase-let ((`(,ridx ,cidx ,_v)
                              (or (clutch-result--cell-at-point)
                                  (user-error "No cell at point"))))
                     (cons (list ridx) (list cidx))))))
         (indices (or (car-safe rect)
                      (clutch-result--selected-row-indices)
                      (user-error "No row at point")))
         (col-indices (or (cdr-safe rect)
                          (cl-loop for i below (length clutch--result-columns)
                                   collect i)))
         (table (clutch--insert-target-table))
         (stmts (clutch-result--build-insert-statements indices col-indices table)))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %d INSERT statement%s (%d col%s)"
             (length stmts) (if (= (length stmts) 1) "" "s")
             (length col-indices) (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--copy-rows-as-update (&optional rect)
  "Copy row(s) as UPDATE statement(s) to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell."
  (let* ((rect (or rect
                   (if (use-region-p)
                       (clutch-result--region-rectangle-indices)
                     (pcase-let ((`(,ridx ,cidx ,_v)
                                  (or (clutch-result--cell-at-point)
                                      (user-error "No cell at point"))))
                       (cons (list ridx) (list cidx))))))
         (indices (or (car-safe rect)
                      (clutch-result--selected-row-indices)
                      (user-error "No row at point")))
         (col-indices (or (cdr-safe rect)
                          (cl-loop for i below (length clutch--result-columns)
                                   collect i)))
         (rows (mapcar (lambda (ridx) (nth ridx clutch--result-rows)) indices))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "copy UPDATE SQL")))
    (kill-new (mapconcat #'identity stmts "\n"))
    (deactivate-mark)
    (message "Copied %d UPDATE statement%s (%d col%s)"
             (length stmts) (if (= (length stmts) 1) "" "s")
             (length col-indices) (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--build-csv-lines (indices col-indices)
  "Return CSV lines (header + data) for INDICES rows using COL-INDICES columns."
  (let* ((col-names  (mapcar (lambda (i) (nth i clutch--result-columns)) col-indices))
         (rows       clutch--result-rows)
         (csv-escape (lambda (val)
                       (let ((s (clutch--format-value val)))
                         (if (string-match-p "[,\"\n]" s)
                             (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
                           s)))))
    (cons (mapconcat #'identity col-names ",")
          (cl-loop for ridx in indices
                   for row = (nth ridx rows)
                   for vals = (mapcar (lambda (i) (nth i row)) col-indices)
                   collect (mapconcat csv-escape vals ",")))))

(defun clutch-result--copy-rows-as-csv (&optional rect)
  "Copy row(s) as CSV to the kill ring.
Use RECT when non-nil.  Rows/columns: region rectangle > current cell.
Includes a header row with column names."
  (let* ((rect (or rect
                   (if (use-region-p)
                       (clutch-result--region-rectangle-indices)
                 (pcase-let ((`(,ridx ,cidx ,_v)
                              (or (clutch-result--cell-at-point)
                                  (user-error "No cell at point"))))
                     (cons (list ridx) (list cidx))))))
         (indices (or (car-safe rect)
                      (clutch-result--selected-row-indices)
                      (user-error "No row at point")))
         (col-indices (or (cdr-safe rect)
                          (cl-loop for i below (length clutch--result-columns)
                                   collect i)))
         (lines (clutch-result--build-csv-lines indices col-indices)))
    (kill-new (mapconcat #'identity lines "\n"))
    (deactivate-mark)
    (message "Copied %d row%s as CSV (%d col%s)"
             (length indices) (if (= (length indices) 1) "" "s")
             (length col-indices) (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--goto-col-idx (col-idx)
  "Move point to COL-IDX in the current row, preserving the row position.
When point is at line-end or a border, scan backward to find the row."
  (let ((ridx (or (get-text-property (point) 'clutch-row-idx)
                   (and (> (point) (line-beginning-position))
                        (get-text-property (1- (point)) 'clutch-row-idx))
                   (save-excursion
                     (let ((prev (previous-single-property-change
                                  (point) 'clutch-row-idx)))
                       (when prev
                         (get-text-property (max (1- prev) (point-min))
                                            'clutch-row-idx)))))))
    (if ridx
        (clutch--goto-cell ridx col-idx)
      (goto-char (point-min))
      (when-let* ((found (text-property-search-forward
                          'clutch-col-idx col-idx #'eq)))
        (goto-char (prop-match-beginning found))))
    (clutch--ensure-point-visible-horizontally)))

;;;###autoload
(defun clutch-result-column-info ()
  "Show type information for the column at point.
When details are not yet cached, attempts to load them from the database."
  (interactive)
  (let* ((cidx (or (get-text-property (point) 'clutch-col-idx)
                   (get-text-property (point) 'clutch-header-col))))
    (unless cidx
      (user-error "No column at point"))
    ;; Try to populate details on demand if missing.
    (unless clutch--result-column-details
      (when-let* ((sql (or clutch--last-query))
                  (cols clutch--result-columns))
        (setq-local clutch--result-column-details
                    (clutch--resolve-result-column-details
                     clutch-connection sql cols))))
    (if-let* ((info (clutch--column-info-string cidx)))
        (message "%s" (replace-regexp-in-string "\n" " | " info))
      (message "%s (no detail info)" (nth cidx clutch--result-columns)))))

;;;###autoload
(defun clutch-result-goto-column ()
  "Jump to a specific column in the current row."
  (interactive)
  (unless clutch--result-columns
    (user-error "No result columns"))
  (let* ((col-names clutch--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      (clutch-result--goto-col-idx idx))))

;;;###autoload
(defun clutch-result-export ()
  "Export the current result.
Prompts for format:
- csv-copy: all rows to clipboard as CSV text
- csv-file: all rows to CSV file
- insert-copy: all rows to clipboard as INSERT statements
- insert-file: all rows to a .sql file as INSERT statements
- update-copy: all rows to clipboard as UPDATE statements
- update-file: all rows to a .sql file as UPDATE statements."
  (interactive)
  (let ((fmt (completing-read "Export format: "
                              '("csv-copy" "csv-file"
                                "insert-copy" "insert-file"
                                "update-copy" "update-file")
                              nil t)))
    (pcase fmt
      ("csv-copy" (clutch--export-csv-all-to-clipboard))
      ("csv-file" (clutch--export-csv-all-file))
      ("insert-copy" (clutch--export-insert-all-to-clipboard))
      ("insert-file" (clutch--export-insert-all-file))
      ("update-copy" (clutch--export-update-all-to-clipboard))
      ("update-file" (clutch--export-update-all-file)))))

(defun clutch--csv-escape (val)
  "Return CSV-escaped string for VAL."
  (let ((s (clutch--format-value val)))
    (if (string-match-p "[,\"\n]" s)
        (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
      s)))

(defun clutch--csv-content (rows)
  "Return CSV text for ROWS using current result columns."
  (let ((header (mapconcat #'clutch--csv-escape clutch--result-columns ","))
        (body (mapconcat (lambda (row)
                           (mapconcat #'clutch--csv-escape row ","))
                         rows "\n")))
    (if (string-empty-p body)
        (concat header "\n")
      (concat header "\n" body "\n"))))

(defun clutch--csv-export-coding-choices ()
  "Return alist of CSV export coding labels to coding systems."
  (let ((pairs '(("utf-8-bom" . utf-8-with-signature)
                 ("utf-8" . utf-8)
                 ("gbk" . gbk)
                 ("cp936" . cp936))))
    (cl-loop for (label . coding) in pairs
             when (coding-system-p coding)
             collect (cons label coding))))

(defun clutch--read-csv-export-coding-system ()
  "Read coding system for CSV file export."
  (let* ((choices (clutch--csv-export-coding-choices))
         (default (if (coding-system-p clutch-csv-export-default-coding-system)
                      clutch-csv-export-default-coding-system
                    'utf-8-with-signature))
         (default-label (car (rassoc default choices)))
         (label (completing-read
                 (format "CSV encoding (default %s): "
                         (or default-label (symbol-name default)))
                 (mapcar #'car choices) nil t nil nil default-label)))
    (or (cdr (assoc label choices)) default)))

(defun clutch--export-text-to-file (text rows prompt default-file message-fmt
                                         &rest message-args)
  "Write TEXT for ROWS to a user-selected file and report success.
PROMPT and DEFAULT-FILE are passed to `read-file-name'.  MESSAGE-FMT and
MESSAGE-ARGS are forwarded to `message' after the row count data and path."
  (let ((path (read-file-name prompt nil nil nil default-file)))
    (with-temp-buffer
      (insert text)
      (write-region (point-min) (point-max) path nil 'silent))
    (apply #'message message-fmt
           (length rows) (if (= (length rows) 1) "" "s")
           path message-args)))

(defun clutch--export-text-to-clipboard (text rows message-fmt &rest message-args)
  "Copy TEXT for ROWS to the kill ring.
Report success with MESSAGE-FMT and MESSAGE-ARGS."
  (kill-new text)
  (apply #'message message-fmt
         (length rows) (if (= (length rows) 1) "" "s")
         message-args))

(defun clutch--export-csv-rows-to-file (rows)
  "Export ROWS as CSV to a file."
  (let* ((coding (clutch--read-csv-export-coding-system))
         (coding-system-for-write coding)
         (text (clutch--csv-content rows)))
    (clutch--export-text-to-file
     text rows "Export CSV to file: " "export.csv"
     "Exported %d row%s to %s (%s)" coding)))

(defun clutch--export-csv-rows-to-clipboard (rows)
  "Copy ROWS as CSV to the kill ring."
  (clutch--export-text-to-clipboard
   (clutch--csv-content rows) rows
   "Copied %d row%s as CSV"))

(defun clutch-result--collect-all-export-rows ()
  "Return all rows for current result by auto-paging when needed."
  (clutch--ensure-connection)
  (let ((effective-sql (clutch-result--effective-query)))
    (cond
     ((null effective-sql)
      clutch--result-rows)
     ((or (null clutch--base-query)
          (clutch--sql-has-limit-p effective-sql))
      (clutch-db-result-rows
       (clutch--run-db-query clutch-connection effective-sql)))
     (t
      (let ((page-num 0)
            (page-size clutch-result-max-rows)
            (rows nil)
            done)
        (while (not done)
          (let* ((paged-sql (clutch--build-paged-sql
                             effective-sql page-num page-size clutch--order-by))
                 (result (clutch--run-db-query clutch-connection paged-sql))
                 (batch (clutch-db-result-rows result)))
            (setq rows (nconc rows (copy-sequence batch)))
            (if (< (length batch) page-size)
                (setq done t)
              (cl-incf page-num))))
        rows)))))

(defun clutch--export-csv-all-file ()
  "Export all query rows as CSV to a file."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-csv-rows-to-file rows)))

(defun clutch--export-csv-all-to-clipboard ()
  "Copy all query rows as CSV text to the kill ring."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-csv-rows-to-clipboard rows)))

(defun clutch--insert-content (rows)
  "Return INSERT statement text for ROWS using current result metadata."
  (let* ((table (clutch--insert-target-table))
         (col-indices (cl-loop for i below (length clutch--result-columns) collect i))
         (stmts (clutch-result--build-insert-statements
                 (cl-loop for i below (length rows) collect i)
                 col-indices
                 table)))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--update-content (rows)
  "Return UPDATE statement text for ROWS using current result metadata."
  (let* ((col-indices (cl-loop for i below (length clutch--result-columns) collect i))
         (stmts (clutch-result--build-update-statements-for-rows
                 rows col-indices "export UPDATE SQL")))
    (if stmts
        (concat (mapconcat #'identity stmts "\n") "\n")
      "")))

(defun clutch--export-insert-rows-to-file (rows)
  "Export ROWS as INSERT statements to a SQL file."
  (clutch--export-text-to-file
   (clutch--insert-content rows) rows
   "Export SQL to file: " "export.sql"
   "Exported %d row%s as INSERT SQL to %s"))

(defun clutch--export-insert-rows-to-clipboard (rows)
  "Copy ROWS as INSERT statements to the kill ring."
  (clutch--export-text-to-clipboard
   (clutch--insert-content rows) rows
   "Copied %d row%s as INSERT SQL"))

(defun clutch--export-insert-all-file ()
  "Export all query rows as INSERT statements to a SQL file."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-insert-rows-to-file rows)))

(defun clutch--export-insert-all-to-clipboard ()
  "Copy all query rows as INSERT statements to the kill ring."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-insert-rows-to-clipboard rows)))

(defun clutch--export-update-rows-to-file (rows)
  "Export ROWS as UPDATE statements to a SQL file."
  (clutch--export-text-to-file
   (clutch--update-content rows) rows
   "Export SQL to file: " "export.sql"
   "Exported %d row%s as UPDATE SQL to %s"))

(defun clutch--export-update-rows-to-clipboard (rows)
  "Copy ROWS as UPDATE statements to the kill ring."
  (clutch--export-text-to-clipboard
   (clutch--update-content rows) rows
   "Copied %d row%s as UPDATE SQL"))

(defun clutch--export-update-all-file ()
  "Export all query rows as UPDATE statements to a SQL file."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-update-rows-to-file rows)))

(defun clutch--export-update-all-to-clipboard ()
  "Copy all query rows as UPDATE statements to the kill ring."
  (let ((rows (clutch-result--collect-all-export-rows)))
    (clutch--export-update-rows-to-clipboard rows)))

(defun clutch-result--pending-sql-statements ()
  "Return the staged SQL statements that would run on commit."
  (unless (or clutch--pending-inserts clutch--pending-edits clutch--pending-deletes)
    (user-error "No pending SQL"))
  (clutch-result--render-statements
   (append
    (when clutch--pending-inserts
      (clutch-result--build-pending-insert-statements))
    (when clutch--pending-edits
      (clutch-result--build-update-statements))
    (when clutch--pending-deletes
      (clutch-result--build-pending-delete-statements)))))

(defun clutch-result--pending-sql-content (&optional stmts)
  "Return STMTS as a trailing-newline-terminated staged SQL batch string.
When STMTS is nil, build statements from the current pending state."
  (let ((stmts (or stmts (clutch-result--pending-sql-statements))))
    (if stmts
        (concat (mapconcat (lambda (s) (concat s ";")) stmts "\n") "\n")
      "")))

;;;###autoload
(defun clutch-result-copy-pending-sql ()
  "Copy the staged SQL batch to the kill ring."
  (interactive)
  (let ((stmts (clutch-result--pending-sql-statements)))
    (kill-new (clutch-result--pending-sql-content stmts))
    (message "Copied %d pending SQL statement%s"
             (length stmts) (if (= (length stmts) 1) "" "s"))))

;;;###autoload
(defun clutch-result-save-pending-sql ()
  "Save the staged SQL batch to a file."
  (interactive)
  (let* ((stmts (clutch-result--pending-sql-statements))
         (sql (clutch-result--pending-sql-content stmts))
         (path (read-file-name "Save pending SQL to file: " nil nil nil "pending.sql")))
    (with-temp-buffer
      (insert sql)
      (write-region (point-min) (point-max) path nil 'silent))
    (message "Saved %d pending SQL statement%s to %s"
             (length stmts) (if (= (length stmts) 1) "" "s")
             path)))

;;;; Horizontal scrolling and width adjustment

;;;###autoload
(defun clutch-result-scroll-right ()
  "Page the result window right with one-column overlap.
The last column whose border falls within the current viewport becomes
the first column of the new view, so partially visible edge columns
remain visible after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (right-edge (+ hs width))
           (ncols (length clutch--result-columns))
           (last-in-view nil)
           (first-past nil))
      (dotimes (i ncols)
        (let ((border (clutch--column-border-position i)))
          (cond
           ((and (> border hs) (< border right-edge))
            (setq last-in-view border))
           ((and (>= border right-edge) (null first-past))
            (setq first-past border)))))
      (cond
       (last-in-view (set-window-hscroll win last-in-view))
       (first-past   (set-window-hscroll win first-past))
       (t (message "Already at rightmost columns"))))))

;;;###autoload
(defun clutch-result-scroll-left ()
  "Page the result window left with one-column overlap.
The column at the current left edge remains visible near the right
edge of the new view, so partially visible edge columns stay visible
after paging."
  (interactive)
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((hs (window-hscroll win))
           (width (window-body-width win))
           (ncols (length clutch--result-columns)))
      (when (> hs 0)
        ;; Column at the current left edge (largest border <= hs).
        (let ((first-border 0)
              (target nil))
          (dotimes (i ncols)
            (let ((border (clutch--column-border-position i)))
              (when (<= border hs)
                (setq first-border border))))
          ;; Smallest column border that keeps first-border in the new view:
          ;; new-hs + width > first-border  →  new-hs > first-border - width
          (let ((min-new (- first-border width)))
            (dotimes (i ncols)
              (let ((border (clutch--column-border-position i)))
                (when (and (> border min-new) (< border hs) (null target))
                  (setq target border)))))
          (set-window-hscroll win (max 0 (or target 0))))))))

;;;###autoload
(defun clutch-result-widen-column ()
  "Widen the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (cl-incf (aref clutch--column-widths cidx)
                 clutch-column-width-step)
        (clutch--refresh-display))
    (user-error "No column at point")))

;;;###autoload
(defun clutch-result-narrow-column ()
  "Narrow the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (let ((new-w (max 5 (- (aref clutch--column-widths cidx)
                              clutch-column-width-step))))
        (aset clutch--column-widths cidx new-w)
        (clutch--refresh-display))
    (user-error "No column at point")))

;;;; Fullscreen toggle

(defvar-local clutch--pre-fullscreen-config nil
  "Window configuration saved before entering fullscreen.")

;;;###autoload
(defun clutch-result-fullscreen-toggle ()
  "Toggle fullscreen display for the result buffer.
Expands the result buffer to fill the frame, or restores the
previous window layout."
  (interactive)
  (if clutch--pre-fullscreen-config
      (progn
        (set-window-configuration clutch--pre-fullscreen-config)
        (setq clutch--pre-fullscreen-config nil)
        (message "Restored window layout"))
    (setq clutch--pre-fullscreen-config
          (current-window-configuration))
    (delete-other-windows)
    (clutch--refresh-display)
    (message "Fullscreen (press f again to restore)")))

;;;; Record buffer

(defvar clutch-record-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'clutch-record-toggle-expand)
    (define-key map "n" #'clutch-record-next-row)
    (define-key map "p" #'clutch-record-prev-row)
    (define-key map "v" #'clutch-record-view-value)
    (define-key map "V" #'clutch-record-live-view-value)
    (define-key map "I" #'clutch-clone-row-to-insert)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'clutch-record-refresh)
    (define-key map (kbd "C-c ?") #'clutch-record-dispatch)
    map)
  "Keymap for `clutch-record-mode'.")
;;;###autoload
(define-derived-mode clutch-record-mode special-mode "clutch-record"
  "Mode for displaying a single database row in detail.

\\<clutch-record-mode-map>
  \\[clutch-record-toggle-expand]	Expand/collapse field or follow FK
  \\[clutch-record-next-row]	Next row
  \\[clutch-record-prev-row]	Previous row
  \\[clutch-record-view-value]	View current field once
  \\[clutch-record-live-view-value]	Open live viewer that follows point
  \\[clutch-record-refresh]	Refresh"
  (setq truncate-lines nil))

;;;###autoload
(defun clutch-result-open-record ()
  "Open the Record buffer showing the row at point.
Reuses a single *clutch-record* buffer, updating it in place."
  (interactive)
  (let* ((ridx (or (clutch-result--row-idx-at-line)
                   (user-error "No row at point")))
         (result-buf (current-buffer))
         (buf (get-buffer-create "*clutch-record*")))
    (with-current-buffer buf
      (unless (eq major-mode 'clutch-record-mode)
        (clutch-record-mode))
      (setq-local clutch-record--result-buffer result-buf)
      (setq-local clutch-record--row-idx ridx)
      (setq-local clutch-record--expanded-fields nil)
      (clutch-record--render))
    (pop-to-buffer buf '(display-buffer-at-bottom))))

(defun clutch-record--render-field (name cidx val col-def row ridx pk-indices
                                        edits fk-info expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, ROW the full row.
RIDX is the row index.  PK-INDICES, EDITS, FK-INFO, and EXPANDED-FIELDS
provide edit/FK/expand state.  MAX-NAME-W is the label column width."
  (let* ((pk-indices pk-indices)
         (pk-vec (and pk-indices row
                      (clutch-result--extract-pk-vec row pk-indices)))
         (edited (and pk-vec (assoc (cons pk-vec cidx) edits)))
         (display-val (if edited (cdr edited) val))
         (long-p (clutch--long-field-type-p col-def))
         (expanded-p (memq cidx expanded-fields))
         (fk (cdr (assq cidx fk-info)))
         (formatted (clutch--format-value display-val))
         (display (if (and long-p (not expanded-p) (> (length formatted) 80))
                      (concat (substring formatted 0 80) "…")
                    formatted))
         (face (cond (edited 'clutch-modified-face)
                     ((null val) 'clutch-null-face)
                     (fk 'clutch-fk-face)
                     (t nil))))
    (insert (propertize (clutch--string-pad name max-name-w)
                        'face 'clutch-header-face)
            (propertize " : " 'face 'clutch-border-face)
            (propertize display
                        'clutch-row-idx ridx
                        'clutch-col-idx cidx
                        'clutch-full-value (if edited (cdr edited) val)
                        'face face)
            "\n")))

(defun clutch-record--render ()
  "Render the current row in the Record buffer."
  (unless (buffer-live-p clutch-record--result-buffer)
    (user-error "Result buffer no longer exists"))
  (let* ((result-buf clutch-record--result-buffer)
         (ridx clutch-record--row-idx)
         (col-names (buffer-local-value 'clutch--result-columns result-buf))
         (col-defs (buffer-local-value 'clutch--result-column-defs result-buf))
         (rows (buffer-local-value 'clutch--result-rows result-buf))
         (pk-indices (buffer-local-value 'clutch--cached-pk-indices result-buf))
         (fk-info (buffer-local-value 'clutch--fk-info result-buf))
         (edits (buffer-local-value 'clutch--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (user-error "Row %d no longer exists" ridx))
    (clutch--bind-connection-context
     (buffer-local-value 'clutch-connection result-buf)
     (buffer-local-value 'clutch--connection-params result-buf)
     (buffer-local-value 'clutch--conn-sql-product result-buf))
    (erase-buffer)
    (setq-local clutch-record--header-base
                (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                            'face 'clutch-header-face))
    (setq header-line-format
          '(:eval (clutch--header-with-disconnect-badge clutch-record--header-base)))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for cidx from 0
               do (clutch-record--render-field
                   name cidx (nth cidx row) (nth cidx col-defs)
                   row ridx pk-indices edits fk-info clutch-record--expanded-fields
                   max-name-w)))
    (goto-char (point-min))))

(defun clutch-record--follow-fk (fk val result-buf)
  "Navigate to the FK-referenced row for VAL using FK plist, via RESULT-BUF."
  (when (null val)
    (user-error "NULL value — cannot follow"))
  (with-current-buffer result-buf
    (let ((c (buffer-local-value 'clutch-connection result-buf)))
      (clutch--execute
       (format "SELECT * FROM %s WHERE %s = %s"
               (clutch-db-escape-identifier c (plist-get fk :ref-table))
               (clutch-db-escape-identifier c (plist-get fk :ref-column))
               (clutch--value-to-literal val))
       clutch-connection))))

;;;###autoload
(defun clutch-record-toggle-expand ()
  "Toggle expand/collapse for long fields, or follow FK."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'clutch-col-idx))
            (ridx (get-text-property (point) 'clutch-row-idx)))
      (let* ((result-buf clutch-record--result-buffer)
             (fk-info  (buffer-local-value 'clutch--fk-info result-buf))
             (fk       (cdr (assq cidx fk-info)))
             (col-defs (buffer-local-value 'clutch--result-column-defs result-buf))
             (col-def  (nth cidx col-defs))
             (val      (get-text-property (point) 'clutch-full-value)))
        (cond
         (fk
          (clutch-record--follow-fk fk val result-buf))
         ((clutch--long-field-type-p col-def)
          (if (memq cidx clutch-record--expanded-fields)
              (setq clutch-record--expanded-fields
                    (delq cidx clutch-record--expanded-fields))
            (push cidx clutch-record--expanded-fields))
          (clutch-record--render))
         (t
          (message "%s" (clutch--format-value val)))))
    (user-error "No field at point")))

;;;###autoload
(defun clutch-record-next-row ()
  "Show the next row in the Record buffer."
  (interactive)
  (let ((total (with-current-buffer clutch-record--result-buffer
                 (length clutch--result-rows))))
    (if (>= (1+ clutch-record--row-idx) total)
        (user-error "Already at last row")
      (cl-incf clutch-record--row-idx)
      (setq clutch-record--expanded-fields nil)
      (clutch-record--render))))

;;;###autoload
(defun clutch-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= clutch-record--row-idx 0)
      (user-error "Already at first row")
    (cl-decf clutch-record--row-idx)
    (setq clutch-record--expanded-fields nil)
    (clutch-record--render)))

;;;###autoload
(defun clutch-record-view-value ()
  "Display the field value at point in an appropriate pop-up buffer.
Selects JSON, XML, or binary string view based on column type and content."
  (interactive)
  (let* ((cidx (get-text-property (point) 'clutch-col-idx))
         (_ridx (get-text-property (point) 'clutch-row-idx))
         (val  (if cidx
                   (get-text-property (point) 'clutch-full-value)
                 (user-error "No field at point")))
         (col-def (when (and cidx (buffer-live-p clutch-record--result-buffer))
                    (with-current-buffer clutch-record--result-buffer
                      (nth cidx clutch--result-column-defs)))))
    (clutch--dispatch-view val col-def)))

;;;###autoload
(defun clutch-record-live-view-value ()
  "Open a live-follow viewer for the record field at point."
  (interactive)
  (clutch--open-live-view))

;;;###autoload
(defun clutch-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (clutch-record--render))

;;;; REPL mode

(defvar clutch-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-m") #'clutch-commit)
    (define-key map (kbd "C-c C-u") #'clutch-rollback)
    (define-key map (kbd "C-c C-a") #'clutch-toggle-auto-commit)
    (define-key map (kbd "C-c C-j") #'clutch-jump)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "C-c C-l") #'clutch-switch-schema)
    map)
  "Keymap for `clutch-repl-mode'.")

(defvar-local clutch-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")
;;;###autoload
(define-derived-mode clutch-repl-mode comint-mode "clutch-repl"
  "Major mode for database REPL.

\\<clutch-repl-mode-map>
  \\[clutch-connect]	Connect to server
  \\[clutch-jump]	Object jump
  \\[clutch-describe-dwim]	Describe object
  \\[clutch-act-dwim]	Object actions
  \\[clutch-switch-schema]	Switch schema/database"
  (setq comint-prompt-regexp "^db> \\|^    -> ")
  (setq comint-input-sender #'clutch-repl--input-sender)
  (clutch--install-completion-capfs)
  (add-hook 'xref-backend-functions #'clutch--xref-backend nil t)
  (add-hook 'kill-buffer-hook #'clutch--disconnect-on-kill nil t))

(defun clutch-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat clutch-repl--pending-input
                          (unless (string-empty-p clutch-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        ;; Complete statement — execute
        (progn
          (setq clutch-repl--pending-input "")
          (clutch-repl--execute-and-print (string-trim combined)))
      ;; Incomplete — accumulate and show continuation prompt
      (setq clutch-repl--pending-input combined)
      (clutch-repl--output "    -> "))))

(defun clutch-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((inhibit-read-only t)
        (proc (get-buffer-process (current-buffer))))
    (goto-char (process-mark proc))
    (insert text)
    (set-marker (process-mark proc) (point))))

(defun clutch-repl--format-dml-result (result elapsed)
  "Format a DML RESULT with ELAPSED time as a string for the REPL."
  (let ((msg (format "\nAffected rows: %s"
                     (or (clutch-db-result-affected-rows result) 0))))
    (when-let* ((id (clutch-db-result-last-insert-id result))
                ((> id 0)))
      (setq msg (concat msg (format ", Last insert ID: %s" id))))
    (when-let* ((w (clutch-db-result-warnings result))
                ((> w 0)))
      (setq msg (concat msg (format ", Warnings: %s" w))))
    (format "%s (%.3fs)\n\ndb> " msg elapsed)))

(defun clutch-repl--execute-and-print (sql)
  "Execute SQL and print results inline in the REPL."
  (setq-local clutch--buffer-error-details nil)
  (condition-case err
      (progn
        (clutch--ensure-connection)
        (setq clutch--last-query sql)
        (let* ((start (float-time))
               (result (clutch--run-db-query clutch-connection sql))
               (elapsed (- (float-time) start))
               (columns (clutch-db-result-columns result))
               (rows (clutch-db-result-rows result)))
          (if columns
              (let* ((col-names (clutch--column-names columns))
                     (table-str (clutch--render-static-table
                                 col-names rows columns)))
                (clutch-repl--output
                 (format "\n%s\n%d row%s in %.3fs\n\ndb> "
                         table-str (length rows)
                         (if (= (length rows) 1) "" "s")
                         elapsed)))
            (clutch-repl--output (clutch-repl--format-dml-result result elapsed)))))
    (quit
     (condition-case nil
         (clutch--handle-query-quit clutch-connection)
       (clutch-query-interrupted
        (clutch-repl--output "\nERROR: Query interrupted\n\ndb> "))))
    (error
     (clutch--remember-buffer-query-error-details
      (current-buffer) clutch-connection sql err)
     (clutch-repl--output
      (format "\nERROR: %s\n\ndb> "
              (clutch--humanize-db-error (error-message-string err)))))))

;;;###autoload
(defun clutch-repl ()
  "Start a database REPL buffer."
  (interactive)
  (let* ((buf-name "*clutch REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        ;; Start a dummy process for comint
        (let ((proc (start-process "clutch-repl" buf "cat")))
          (set-process-query-on-exit-flag proc nil)
          (clutch-repl-mode)
          (clutch-repl--output "db> "))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

;;;; Transient dispatch menus

(transient-define-suffix clutch--dispatch-toggle-auto-commit ()
  "Transient suffix for `clutch-toggle-auto-commit' with a dynamic label."
  :description (lambda ()
                 (if (and clutch-connection
                          (clutch-db-manual-commit-p clutch-connection))
                     "Enable auto-commit"
                   "Disable auto-commit"))
  :inapt-if (lambda ()
               (and clutch-connection
                    (clutch-db-manual-commit-p clutch-connection)
                    (clutch--tx-dirty-p clutch-connection)))
  (interactive)
  (call-interactively #'clutch-toggle-auto-commit))

;;;###autoload
(transient-define-prefix clutch-dispatch ()
  "Main dispatch menu for clutch."
  [ :pad-keys t
   ["Connection"
    ("c" "Connect"    clutch-connect)
    ("d" "Disconnect" clutch-disconnect)
    ("m" "Commit"            clutch-commit)
    ("u" "Rollback"          clutch-rollback)
    ("a" clutch--dispatch-toggle-auto-commit)
    ("R" "REPL"              clutch-repl)]
   ["Execute"
    ("x" "Query at point" clutch-execute-query-at-point)
    ("X" "Statement (;-only)" clutch-execute-statement-at-point)
    ("r" "Region"         clutch-execute-region)
    ("b" "Buffer"         clutch-execute-buffer)
    ("p" "Preview execution" clutch-preview-execution-sql)]
   ["Edit"
    ("'" "Indirect edit"  clutch-edit-indirect)]
   ["Objects"
    ("j" "Jump to object"     clutch-jump)
    ("D" "Describe object"    clutch-describe-dwim)
    ("o" "Object actions"     clutch-act-dwim)
    ("l" "Switch schema/db"   clutch-switch-schema)
    ("s" "Refresh schema"   clutch-refresh-schema)]])

(transient-define-prefix clutch-result-dispatch ()
  "Dispatch menu for clutch result buffer."
  [ :pad-keys t
   ["Navigate"
    ("TAB" "Next cell"       clutch-result-next-cell)
    ("<backtab>" "Prev cell" clutch-result-prev-cell)
    ("n" "Down row"          clutch-result-down-cell)
    ("p" "Up row"            clutch-result-up-cell)
    ("RET" "Open record"     clutch-result-open-record)
    ("C" "Go to column"      clutch-result-goto-column)
    ("?" "Column info"       clutch-result-column-info)]
   ["Query"
    ("g" "Re-execute"        clutch-result-rerun)
    ("x" "Preview execution" clutch-preview-execution-sql)
    ("#" "Count total"       clutch-result-count-total)
    ("A" "Aggregate"         clutch-result-aggregate)]
   ["Pending"
    ("y" "Copy pending SQL"  clutch-result-copy-pending-sql)
    ("Y" "Save pending SQL"  clutch-result-save-pending-sql)]
   ["Filter / Sort"
    ("/" "Filter rows"       clutch-result-filter)
    ("W" "WHERE filter"      clutch-result-apply-filter)
    ("s" "Sort ASC"          clutch-result-sort-by-column)
    ("S" "Sort DESC"         clutch-result-sort-by-column-desc)]]
  [ :pad-keys t
   ["Pages"
    ("N" "Next page"         clutch-result-next-page)
    ("P" "Prev page"         clutch-result-prev-page)
    ("M-<" "First page"      clutch-result-first-page)
    ("M->" "Last page"       clutch-result-last-page)
    ("]" "Page right →│"     clutch-result-scroll-right)
    ("[" "Page left │←"      clutch-result-scroll-left)]
   ["Mutate"
    ("C-c '" "Edit / re-edit" clutch-result-edit-cell)
    ("i" "Stage insert"      clutch-result-insert-row)
    ("I" "Clone row → insert" clutch-clone-row-to-insert)
    ("d" "Stage delete"      clutch-result-delete-rows)
    ("C-c C-c" "Commit pending" clutch-result-commit)
    ("C-c C-k" "Discard pending at point" clutch-result-discard-pending-at-point)]
   ["Layout"
    ("=" "Widen column"      clutch-result-widen-column)
    ("-" "Narrow column"     clutch-result-narrow-column)
    ("f" "Fullscreen"        clutch-result-fullscreen-toggle)]]
  [ :pad-keys t
   ["Inspect"
    ("v" "View value" clutch-result-view-value)
    ("V" "Live view (follow point)" clutch-result-live-view-value)]
   ["Copy / Export (region/rect: C-x SPC)"
    ("c" "Copy… (-r to refine rows/cols)" clutch-result-copy-dispatch)
    ("e" "Export" clutch-result-export)]])

(transient-define-prefix clutch-record-dispatch ()
  "Dispatch menu for clutch record buffer."
  [ :pad-keys t
   ["Navigate"
    ("n" "Next row"     clutch-record-next-row)
    ("p" "Prev row"     clutch-record-prev-row)
    ("RET" "Expand/FK"  clutch-record-toggle-expand)]
   ["Inspect"
    ("v" "View value" clutch-record-view-value)
    ("V" "Live view (follow point)" clutch-record-live-view-value)]
   ["Other"
    ("I" "Clone row → insert" clutch-clone-row-to-insert)
    ("g" "Refresh" clutch-record-refresh)
    ("q" "Quit"    quit-window)]])


(provide 'clutch)
;;; clutch.el ends here
