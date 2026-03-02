;;; clutch.el --- Interactive database lens -*- lexical-binding: t; -*-

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
;; along with mysql.el.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Interactive SQL client built on mysql.el.
;;
;; Provides:
;; - `clutch-mode': SQL editing major mode (derived from `sql-mode')
;; - `clutch-repl': REPL via `comint-mode'
;; - Query execution with column-paged result tables
;; - Schema browsing and completion
;;
;; Entry points:
;;   M-x clutch-mode      — open a SQL editing buffer
;;   M-x clutch-repl      — open a REPL
;;   Open a .mysql file   — activates clutch-mode automatically

;;; Code:

(require 'clutch-db)
(require 'sql)
(require 'comint)
(require 'cl-lib)
(require 'transient)
(require 'auth-source)

(declare-function nerd-icons-mdicon "nerd-icons")
(declare-function nerd-icons-codicon "nerd-icons")

;;;; Customization

(defgroup clutch nil
  "Interactive database lens."
  :group 'comm
  :prefix "clutch-")

(defface clutch-header-face
  '((t :inherit bold))
  "Face for column headers in result tables."
  :group 'clutch)

(defface clutch-pinned-header-face
  '((t :inherit clutch-header-face))
  "Face for pinned column headers."
  :group 'clutch)

(defface clutch-header-active-face
  '((t :inherit hl-line :weight bold))
  "Face for the column header under the cursor."
  :group 'clutch)

(defface clutch-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'clutch)

(defface clutch-col-page-face
  '((t :inherit warning :weight bold))
  "Face for column-page scroll indicators (◂ and ▸) at table edges."
  :group 'clutch)

(defface clutch-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'clutch)

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

(defface clutch-executed-sql-face
  '((((class color) (background light))
     :background "#eaf5e9")
    (((class color) (background dark))
     :background "#223526")
    (t :inherit highlight))
  "Face for the last successfully executed SQL text."
  :group 'clutch)

(defcustom clutch-connection-alist nil
  "Alist of saved database connections.
Each entry has the form:
  (NAME . (:host H :port P :user U [:password P] :database D
           [:backend SYM] [:sql-product SYM] [:pass-entry STR]
           [:read-timeout N]))
NAME is a string used for `completing-read'.
:backend is a symbol (\\='mysql, \\='pg, or \\='sqlite, default \\='mysql).
:sql-product overrides `clutch-sql-product' for this connection.

Password resolution order:
  1. :password — used as-is when present.
  2. Pass store by connection name — when `auth-source-pass' is loaded,
     clutch automatically looks up a pass entry whose name matches NAME
     (the car of this alist entry).  The password is on the first line.
     Use :pass-entry STR to override the entry name if it differs.
  3. auth-source-search — searches ~/.authinfo / ~/.authinfo.gpg / pass
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
                                    (:read-timeout natnum)
                                    (:tls boolean))))
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

(defcustom clutch-query-timeout-seconds 30
  "Idle timeout in seconds while waiting for query I/O.
Applies to MySQL and PostgreSQL connections.  SQLite ignores this setting."
  :type 'natnum
  :group 'clutch)

(defcustom clutch-csv-export-default-coding-system 'utf-8-with-signature
  "Default coding system when exporting CSV files."
  :type '(choice (const :tag "UTF-8 (with BOM)" utf-8-with-signature)
                 (const :tag "UTF-8" utf-8)
                 (const :tag "GBK" gbk)
                 (coding-system :tag "Other coding system"))
  :group 'clutch)

;;;; Buffer-local variables

(defvar-local clutch-connection nil
  "Current database connection for this buffer.")

(defvar-local clutch--executing-p nil
  "Non-nil while a query is executing in this buffer.
Used to update the mode-line with a spinner during execution.")

(defvar-local clutch--executed-sql-overlay nil
  "Overlay highlighting the last successfully executed SQL region.")

(defvar clutch--source-window nil
  "Window that initiated the current query execution.
Dynamically bound by `clutch--execute' so result buffers open
adjacent to the correct console window.")

(defvar-local clutch--conn-sql-product nil
  "SQL product for the current connection, or nil to use the default.")

(defvar-local clutch--last-query nil
  "Last executed SQL query string.")

(defvar-local clutch--result-columns nil
  "Column names from the last result, as a list of strings.")

(defvar-local clutch--result-rows nil
  "Row data from the last result, as a list of lists.")

(defvar-local clutch--column-widths nil
  "Vector of integers — display width for each column.")

(defvar-local clutch--column-pages nil
  "Vector of vectors — each page contains non-pinned column indices.")

(defvar-local clutch--current-col-page 0
  "Current column page index.")

(defvar-local clutch--pinned-columns nil
  "List of column indices that are pinned (visible on all pages).")

(defvar-local clutch--last-window-width nil
  "Last known window body width, to avoid redundant refreshes.")

(defvar-local clutch--header-active-col nil
  "Col-idx currently highlighted in the header, or nil.")

(defvar-local clutch--row-overlay nil
  "Overlay used to highlight the current row.")

(defvar-local clutch--refine-rect nil
  "Rectangle (ROW-INDICES . COL-INDICES) being refined, or nil.")

(defvar-local clutch--refine-excluded-rows nil
  "Row indices (0-based) excluded during refine mode.")

(defvar-local clutch--refine-excluded-cols nil
  "Col indices (0-based) excluded during refine mode.")

(defvar-local clutch--refine-overlays nil
  "Overlays created during refine mode.")

(defvar-local clutch--refine-callback nil
  "Callback called with final rect when refine is confirmed.")

(defvar-local clutch--refine-saved-tab-line nil
  "Saved tab-line-format to restore after refine mode exits.")

(defvar-local clutch--page-current 0
  "Current data page number (0-based).")

(defvar-local clutch--page-total-rows nil
  "Total row count from COUNT(*), or nil if not yet queried.")

(defvar-local clutch--order-by nil
  "Current ORDER BY state as (COL-NAME . DIRECTION) or nil.
DIRECTION is \"ASC\" or \"DESC\".")

(defvar-local clutch--result-column-defs nil
  "Full column definition plists from the last result.")

(defvar-local clutch--pending-edits nil
  "Alist of pending edits: ((ROW-IDX . COL-IDX) . NEW-VALUE).")

(defvar-local clutch--fk-info nil
  "Foreign key info for the current result.
Alist of (COL-IDX . (:ref-table TABLE :ref-column COLUMN)).")

(defvar-local clutch--sort-column nil
  "Column name currently sorted by, or nil.")

(defvar-local clutch--sort-descending nil
  "Non-nil if the current sort is descending.")

(defvar-local clutch--where-filter nil
  "Current WHERE filter string, or nil if no filter active.")

(defvar-local clutch--base-query nil
  "The original unfiltered SQL query, used by WHERE filtering.")

(defvar-local clutch--marked-rows nil
  "List of marked row indices (dired-style selection).")

(defvar-local clutch--query-elapsed nil
  "Elapsed time in seconds for the last query execution.")

(defvar-local clutch--filter-pattern nil
  "Current client-side filter string, or nil.")

(defvar-local clutch--filtered-rows nil
  "Filtered subset of `clutch--result-rows', or nil when unfiltered.")

(defvar-local clutch--aggregate-summary nil
  "Last aggregate summary plist for result footer, or nil.
Plist keys: :label, :rows, :cells, :skipped, :sum, :avg, :min, :max, :count.")

(defvar-local clutch-record--result-buffer nil
  "Reference to the parent result buffer (Record buffer local).")

(defvar-local clutch-record--row-idx nil
  "Current row index being displayed (Record buffer local).")

(defvar-local clutch-record--expanded-fields nil
  "List of column indices with expanded long fields (Record buffer local).")

(defvar-local clutch--connection-params nil
  "Params plist used to establish the current connection.
Stored at connect time so the connection can be re-established
automatically when it drops.")

(defvar-local clutch--console-name nil
  "Connection name if this buffer is a query console, nil otherwise.
Set by `clutch-query-console'; used to save/restore buffer content.")

;;;; Console persistence

(defun clutch--console-file (name)
  "Return the persistence file path for console NAME."
  (expand-file-name
   (concat (replace-regexp-in-string "[/:\\*?\"<>|]" "_" name) ".sql")
   clutch-console-directory))

(defun clutch--save-console ()
  "Save console buffer content to its persistence file."
  (when clutch--console-name
    (condition-case nil
        (progn
          (make-directory clutch-console-directory t)
          (let ((coding-system-for-write 'utf-8-unix))
            (write-region (point-min) (point-max)
                          (clutch--console-file clutch--console-name)
                          nil 'silent)))
      (error nil))))

(defun clutch--save-all-consoles ()
  "Save content of all open query console buffers.
Run from `kill-emacs-hook' to persist consoles on Emacs exit."
  (dolist (buf (buffer-list))
    (with-current-buffer buf
      (clutch--save-console))))

;;;; Connection management

(defun clutch--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (format "%s@%s:%s/%s"
          (or (clutch-db-user conn) "?")
          (or (clutch-db-host conn) "?")
          (or (clutch-db-port conn) "?")
          (or (clutch-db-database conn) "")))

(defun clutch--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn (clutch-db-live-p conn)))

(defun clutch--try-reconnect ()
  "Attempt to re-establish the connection using `clutch--connection-params'.
Updates `clutch-connection' and refreshes the mode-line on success.
Returns non-nil on success, nil on failure."
  (when clutch--connection-params
    (condition-case err
        (let ((conn (clutch--build-conn clutch--connection-params)))
          (setq clutch-connection conn)
          (clutch--update-mode-line)
          (message "Reconnected to %s" (clutch--connection-key conn))
          t)
      (error
       (message "Reconnect failed: %s" (error-message-string err))
       nil))))

(defun clutch--ensure-connection ()
  "Ensure current buffer has a live connection.
If the connection has dropped, attempts to reconnect automatically
using the stored params.  Signals a user-error if not recoverable."
  (unless (clutch--connection-alive-p clutch-connection)
    (unless (clutch--try-reconnect)
      (user-error "Not connected.  Use C-c C-e to connect"))))

(defun clutch--update-mode-line ()
  "Update mode-line lighter with connection status."
  (setq mode-name
        (cond
         ((and clutch--executing-p (clutch--connection-alive-p clutch-connection))
          (format "%s[%s …]"
                  (clutch-db-display-name clutch-connection)
                  (clutch--connection-key clutch-connection)))
         ((clutch--connection-alive-p clutch-connection)
          (format "%s[%s]"
                  (clutch-db-display-name clutch-connection)
                  (clutch--connection-key clutch-connection)))
         (t "DB[disconnected]")))
  (force-mode-line-update))

(defun clutch--pass-secret-by-suffix (suffix)
  "Return the password from the first pass entry whose path ends with SUFFIX.
Matches e.g. \\='dev-mysql\\=' against \\='mysql/dev-mysql\\='.
Returns nil when no matching entry is found or auth-source-pass is absent."
  (when (and (fboundp 'auth-source-pass-entries)
             (fboundp 'auth-source-pass-parse-entry))
    (let* ((re    (format "\\(^\\|/\\)%s$" (regexp-quote suffix)))
           (entry (cl-find-if (lambda (e) (string-match-p re e))
                              (auth-source-pass-entries))))
      (when entry
        (cdr (assq 'secret (auth-source-pass-parse-entry entry)))))))

(defun clutch--resolve-password (params)
  "Return the password for connection PARAMS.
Checks in order:
  1. :password key (non-empty string) — used as-is.
  2. :pass-entry key — suffix-matched against all pass entries, so
     \\='dev-mysql\\=' finds \\='mysql/dev-mysql\\='.  Automatically set to the
     connection name by callers; override in `clutch-connection-alist'.
  3. auth-source-search by :host/:user/:port (authinfo / pass).
Returns nil when nothing is found (caller should prompt if needed)."
  (let ((pw    (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (not (string-empty-p pw))) pw)
     (t
      (or (and entry (clutch--pass-secret-by-suffix entry))
          (when-let* ((found  (car (auth-source-search
                                    :host (plist-get params :host)
                                    :user (plist-get params :user)
                                    :port (plist-get params :port)
                                    :max 1)))
                      (secret (plist-get found :secret)))
            (if (functionp secret) (funcall secret) secret)))))))

(defun clutch--build-conn (params)
  "Connect to a database using PARAMS, resolving the password via auth-source.
Returns a live connection object or signals a `user-error'."
  (let* ((backend  (or (plist-get params :backend) 'mysql))
         (password (clutch--resolve-password params))
         (db-params (cl-loop for (k v) on params by #'cddr
                             unless (memq k '(:sql-product :backend :password :pass-entry))
                             append (list k v)))
         (db-params (if password
                        (append db-params (list :password password))
                      db-params))
         (db-params (if (memq backend '(mysql pg))
                        (append db-params
                                (list :read-timeout clutch-query-timeout-seconds))
                      db-params)))
    (condition-case err
        (clutch-db-connect backend db-params)
      (clutch-db-error
       (user-error "Connection failed: %s" (error-message-string err))))))

(defun clutch--inject-entry-name (params name)
  "Return PARAMS with :pass-entry defaulting to NAME.
Leaves PARAMS unchanged when :password or :pass-entry is already set."
  (if (or (plist-get params :pass-entry) (plist-get params :password))
      params
    (append params (list :pass-entry name))))

(defun clutch--read-connection-params ()
  "Prompt the user for connection parameters and return a params plist.
Offers saved connections from `clutch-connection-alist' when non-empty,
otherwise prompts for host/port/user/password/database individually.
The password is resolved via `auth-source' before falling back to `read-passwd'."
  (if clutch-connection-alist
      (let* ((name   (completing-read "Connection: "
                                      (mapcar #'car clutch-connection-alist)
                                      nil t))
             (params (cdr (assoc name clutch-connection-alist))))
        (clutch--inject-entry-name params name))
    (let* ((host          (read-string "Host (127.0.0.1): " nil nil "127.0.0.1"))
           (port          (read-number "Port (3306): " 3306))
           (user          (read-string "User: "))
           (manual-params (list :host host :port port :user user))
           (pw            (or (clutch--resolve-password manual-params)
                              (read-passwd "Password: ")))
           (db            (read-string "Database (optional): ")))
      (append manual-params
              (list :password pw
                    :database (unless (string-empty-p db) db))))))

(defun clutch-connect ()
  "Connect to a database server interactively.
If `clutch-connection-alist' is non-empty, offer saved connections via
`completing-read'.  Otherwise prompt for each parameter.
The password is resolved via `auth-source' when not in the connection
params; see `clutch-connection-alist' for details."
  (interactive)
  (when (clutch--connection-alive-p clutch-connection)
    (clutch-db-disconnect clutch-connection)
    (setq clutch-connection nil))
  (let* ((params  (clutch--read-connection-params))
         (product (plist-get params :sql-product))
         (conn    (clutch--build-conn params)))
    (setq clutch-connection conn
          clutch--connection-params params
          clutch--conn-sql-product product)
    (clutch--update-mode-line)
    (clutch--refresh-schema-cache conn)
    (message "Connected to %s" (clutch--connection-key conn))))

(defun clutch-disconnect ()
  "Disconnect from the current database server."
  (interactive)
  (when (clutch--connection-alive-p clutch-connection)
    (clutch-db-disconnect clutch-connection)
    (message "Disconnected"))
  (setq clutch-connection nil)
  (clutch--update-mode-line))

;;;; Query console

(defun clutch--console-window-for (buf)
  "Return the best window to display BUF.
Priority: (1) window already showing BUF; (2) any visible clutch
console window; (3) nil, meaning use the selected window."
  (or (get-buffer-window buf)
      (cl-find-if (lambda (w)
                    (string-prefix-p "*clutch: "
                                     (buffer-name (window-buffer w))))
                  (window-list))))

;;;###autoload
(defun clutch-query-console (name)
  "Open or switch to the query console for saved connection NAME.
Creates a dedicated buffer *clutch: NAME* with `clutch-mode' enabled
and connects automatically if not already connected.
Repeated calls with the same NAME switch to the existing buffer.
When called from outside a clutch buffer, reuses any visible clutch
window rather than replacing the current window."
  (interactive
   (list (if clutch-connection-alist
             (completing-read "Console: "
                              (mapcar #'car clutch-connection-alist)
                              nil t)
           (user-error "No saved connections.  Populate `clutch-connection-alist' first"))))
  (let* ((buf     (get-buffer-create (format "*clutch: %s*" name)))
         (is-new  (zerop (buffer-size buf))))
    (select-window (or (clutch--console-window-for buf) (selected-window)))
    (switch-to-buffer buf)
    (unless (eq major-mode 'clutch-mode)
      (clutch-mode))
    (setq-local clutch--console-name name)
    (when is-new
      (let* ((file (clutch--console-file name))
             (coding-system-for-read 'utf-8))
        (when (file-readable-p file)
          (insert-file-contents file))))
    (unless (clutch--connection-alive-p clutch-connection)
      (when-let* ((params (clutch--inject-entry-name
                           (cdr (assoc name clutch-connection-alist)) name))
                  (conn   (clutch--build-conn params)))
        (setq clutch-connection conn
              clutch--connection-params params
              clutch--conn-sql-product (plist-get params :sql-product))
        (clutch--update-mode-line)
        (clutch--refresh-schema-cache conn)))))

;;;###autoload
(defun clutch-switch-console ()
  "Switch to an open clutch query console using `completing-read'."
  (interactive)
  (let ((consoles (cl-loop for buf in (buffer-list)
                            when (string-prefix-p "*clutch: " (buffer-name buf))
                            collect (buffer-name buf))))
    (if consoles
        (switch-to-buffer (completing-read "Switch to console: " consoles nil t))
      (user-error "No clutch consoles open.  Use M-x clutch-query-console"))))

;;;; Value formatting

(defun clutch--format-value (val)
  "Format VAL for display in a result table.
nil → \"NULL\", plists → formatted date/time strings."
  (cond
   ((null val) "NULL")
   ((stringp val) val)
   ((numberp val) (number-to-string val))
   ;; datetime plist: has :year and :hours
   ((and (listp val) (plist-get val :year) (plist-get val :hours))
    (format "%04d-%02d-%02d %02d:%02d:%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   ;; date plist: has :year but no :hours
   ((and (listp val) (plist-get val :year))
    (format "%04d-%02d-%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)))
   ;; time plist: has :hours but no :year
   ((and (listp val) (plist-get val :hours))
    (format "%s%02d:%02d:%02d"
            (if (plist-get val :negative) "-" "")
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   (t (format "%S" val))))

(defun clutch--truncate-cell (str max-width)
  "Truncate STR to MAX-WIDTH, replacing embedded pipes to protect org tables."
  (let ((clean (replace-regexp-in-string "|" "¦" (replace-regexp-in-string "\n" "↵" str))))
    (if (> (length clean) max-width)
        (concat (substring clean 0 (- max-width 1)) "…")
      clean)))

(defun clutch--column-names (columns)
  "Extract column names from COLUMNS as a list of strings.
Handles the case where the driver returns non-string names
\(e.g., SELECT 1 produces an integer column name)."
  (mapcar (lambda (c)
            (let ((name (plist-get c :name)))
              (if (stringp name) name (format "%s" name))))
          columns))

(defun clutch--value-to-literal (val)
  "Convert Elisp VAL to a SQL literal string.
nil → \"NULL\", numbers unquoted, strings escaped."
  (cond
   ((null val) "NULL")
   ((numberp val) (number-to-string val))
   ((stringp val) (clutch-db-escape-literal clutch-connection val))
   (t (clutch-db-escape-literal clutch-connection
                                   (clutch--format-value val)))))

(defun clutch--string-pad (str width &optional right-align)
  "Pad STR with spaces to reach display WIDTH.
Unlike `string-pad', this accounts for wide characters (CJK).
When RIGHT-ALIGN is non-nil, pad on the left instead of the right."
  (let ((sw (string-width str)))
    (if (>= sw width)
        str
      (let ((spaces (make-string (- width sw) ?\s)))
        (if right-align
            (concat spaces str)
          (concat str spaces))))))

(defun clutch--center-padding-widths (content-width width)
  "Return (LEFT . RIGHT) padding widths to center CONTENT-WIDTH in WIDTH."
  (let* ((extra (max 0 (- width content-width)))
         (left (/ extra 2)))
    (cons left (- extra left))))

;;;; Column width computation and paging

(defun clutch--numeric-type-p (col-def)
  "Return non-nil if COL-DEF is a numeric column type."
  (eq (plist-get col-def :type-category) 'numeric))

(defun clutch--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB)."
  (memq (plist-get col-def :type-category) '(json blob)))

(defun clutch--long-field-placeholder (col-def)
  "Return a placeholder string for a long field type COL-DEF."
  (pcase (plist-get col-def :type-category)
    ('json "<JSON>")
    (_ "<BLOB>")))

(defun clutch--compute-column-widths (col-names rows column-defs
                                                      &optional max-width)
  "Compute display width for each column.
COL-NAMES is a list of header strings, ROWS is the data,
COLUMN-DEFS is the column metadata list.
MAX-WIDTH caps individual column width (default `clutch-column-width-max').
Pass a large value or nil to use the default.
Returns a vector of integers."
  (let* ((ncols (length col-names))
         (max-w (or max-width clutch-column-width-max))
         (widths (make-vector ncols 0))
         (sample (seq-take rows 50)))
    (dotimes (i ncols)
      (if (and (clutch--long-field-type-p (nth i column-defs))
               (<= max-w clutch-column-width-max))
          (aset widths i 10)
        (let ((header-w (string-width (nth i col-names)))
              (data-w 0))
          (dolist (row sample)
            (let ((formatted (clutch--format-value (nth i row))))
              (setq data-w (max data-w (string-width formatted)))))
          (aset widths i (max 5 (min max-w (max header-w data-w)))))))
    widths))

(defun clutch--compute-column-pages (widths pinned window-width)
  "Compute column pages based on WIDTHS, PINNED columns, and WINDOW-WIDTH.
Each column occupies width + 2*padding + 1 (pipe separator).
PINNED columns are always shown and their width is deducted first.
Returns a vector of vectors, each containing non-pinned column indices."
  (let* ((padding clutch-column-padding)
         (ncols (length widths))
         (pinned-total (+ 1 ;; leading pipe
                          (cl-reduce #'+ (mapcar (lambda (i)
                                                   (+ (aref widths i) (* 2 padding) 1))
                                                 pinned)
                                     :initial-value 0)))
         (available (max 10 (- window-width pinned-total)))
         (pages nil)
         (current-page nil)
         (used 0))
    (dotimes (i ncols)
      (unless (memq i pinned)
        (let ((col-w (+ (aref widths i) (* 2 padding) 1)))
          (when (and current-page (> (+ used col-w) available))
            (push (vconcat (nreverse current-page)) pages)
            (setq current-page nil
                  used 0))
          (push i current-page)
          (cl-incf used col-w))))
    (when current-page
      (push (vconcat (nreverse current-page)) pages))
    (if pages
        (vconcat (nreverse pages))
      (vector (vector)))))

(defun clutch--visible-columns ()
  "Return list of column indices visible on the current page.
Pinned columns come first, followed by the current page's columns."
  (let ((page-cols (when (and clutch--column-pages
                              (< clutch--current-col-page
                                 (length clutch--column-pages)))
                     (append (aref clutch--column-pages
                                   clutch--current-col-page)
                             nil))))
    (append clutch--pinned-columns page-cols)))

;;;; Result display

(defun clutch--result-window ()
  "Return the window currently showing a clutch result buffer, or nil.
Searches all windows on the current frame."
  (cl-find-if (lambda (w)
                (string-prefix-p "*clutch-result:"
                                 (buffer-name (window-buffer w))))
              (window-list nil 'no-minibuf)))

(defun clutch--show-result-buffer (buf)
  "Display BUF in the result window slot.
Reuses the existing result window when one is visible, replacing its
buffer in place.  Creates a new window below `clutch--source-window'
when no result window exists yet."
  (let ((result-win (clutch--result-window)))
    (if result-win
        (progn
          (set-window-buffer result-win buf)
          (select-window result-win))
      (pop-to-buffer buf `(display-buffer-in-direction
                           (window . ,(or clutch--source-window
                                          (selected-window)))
                           (direction . below)
                           (window-height . ,clutch-result-window-height))))))

(defun clutch--result-buffer-name ()
  "Return the result buffer name based on current connection.
Uses the full connection key so each console gets its own result buffer."
  (if (clutch--connection-alive-p clutch-connection)
      (format "*clutch-result: %s*" (clutch--connection-key clutch-connection))
    "*clutch-result: results*"))

(defun clutch--render-static-table (col-names rows &optional column-defs)
  "Render a table string from COL-NAMES and ROWS.
Uses the same visual style as the column-paged result renderer.
COLUMN-DEFS, if provided, is used for long-field detection.
Returns a string (with text properties)."
  (let* ((clutch--result-columns col-names)
         (clutch--result-column-defs column-defs)
         (clutch--pending-edits nil)
         (clutch--fk-info nil)
         (ncols (length col-names))
         (all-cols (number-sequence 0 (1- ncols)))
         (widths (clutch--compute-column-widths col-names rows column-defs 1000))
         (bface 'clutch-border-face)
         (sep-top (propertize (clutch--render-separator all-cols widths 'top)
                              'face bface))
         (sep-mid (propertize (clutch--render-separator all-cols widths 'middle)
                              'face bface))
         (sep-bot (propertize (clutch--render-separator all-cols widths 'bottom)
                              'face bface))
         (header (clutch--render-header all-cols widths))
         (lines nil))
    (push sep-top lines)
    (push header lines)
    (push sep-mid lines)
    (cl-loop for row in rows
             for ridx from 0
             do (push (clutch--render-row row ridx all-cols widths) lines))
    (push sep-bot lines)
    (mapconcat #'identity (nreverse lines) "\n")))

;;;; Column-paged renderer

(defun clutch--replace-edge-borders (str has-prev has-next)
  "Replace edge border characters in STR with page indicators.
When HAS-PREV is non-nil, replace the first border char with `◂'.
When HAS-NEXT is non-nil, replace the last border char with `▸'."
  (when has-prev
    (let ((c (aref str 0)))
      (when (memq c '(?│ ?┌ ?├ ?└ ?┬ ?┼ ?┴))
        (setq str (concat (propertize "◂" 'face 'clutch-col-page-face)
                          (substring str 1))))))
  (when has-next
    (let* ((len (length str))
           (c (aref str (1- len))))
      (when (memq c '(?│ ?┐ ?┤ ?┘))
        (setq str (concat (substring str 0 (1- len))
                           (propertize "▸" 'face 'clutch-col-page-face))))))
  str)

(defun clutch--render-separator (visible-cols widths &optional position)
  "Render a separator line for VISIBLE-COLS with WIDTHS.
POSITION is `top', `middle', or `bottom' (default `middle')."
  (let* ((padding clutch-column-padding)
         (pos (or position 'middle))
         (left  (pcase pos ('top "┌") ('bottom "└") (_ "├")))
         (cross (pcase pos ('top "┬") ('bottom "┴") (_ "┼")))
         (right (pcase pos ('top "┐") ('bottom "┘") (_ "┤")))
         (parts nil))
    (dolist (cidx visible-cols)
      (push (concat cross (make-string (+ (aref widths cidx) (* 2 padding)) ?─))
            parts))
    ;; Replace the leading cross of the first column with the left edge
    (let ((line (concat (mapconcat #'identity (nreverse parts) "") right)))
      (concat left (substring line 1)))))

(defun clutch--icon (name &rest fallback)
  "Return a nerd-icons icon for NAME, or FALLBACK string.
NAME is a cons (FAMILY . ICON-NAME) where FAMILY is one of
`mdicon', `codicon', etc.  Falls back gracefully when
nerd-icons is not installed."
  (pcase-let ((`(,family . ,icon-name) name))
    (or (and (require 'nerd-icons nil t)
             (pcase family
               ('mdicon (nerd-icons-mdicon icon-name))
               ('codicon (nerd-icons-codicon icon-name))))
        (car fallback)
        "")))

(defun clutch--fixed-width-icon (spec fallback &optional face)
  "Return icon with `string-width' matching actual display width.
SPEC is (FAMILY . ICON-NAME) for `clutch--icon'.
FALLBACK is the Unicode char when nerd-icons is unavailable.
Optional FACE is applied to the result.

When `string-pixel-width' is available, measures the icon glyph
pixel width and wraps it in a display property over the correct
number of space characters.  This ensures `string-width' matches
the real rendered width, preventing column misalignment."
  (let* ((raw (clutch--icon spec fallback))
         (raw (if (string-empty-p raw) fallback raw))
         (result
          (if (and (fboundp 'string-pixel-width)
                   (fboundp 'default-font-width)
                   (display-graphic-p))
              (let* ((pw (string-pixel-width raw))
                     (fw (default-font-width))
                     (cells (if (> fw 0)
                                (max 1 (round (/ (float pw) fw)))
                              (string-width raw))))
                (if (= cells (string-width raw))
                    raw
                  (propertize (make-string cells ?\s) 'display raw)))
            raw)))
    (if face (propertize result 'face face) result)))

(defun clutch--header-label (name cidx)
  "Build the display label for column NAME at index CIDX.
Prepends sort indicator and pin marker before the name."
  (let* ((hi 'font-lock-keyword-face)
         (pin-face 'clutch-col-page-face)
         (sort (when (and clutch--sort-column
                          (string= name clutch--sort-column))
                 (let ((s (clutch--fixed-width-icon
                           (if clutch--sort-descending
                               '(codicon . "nf-cod-arrow_down")
                             '(codicon . "nf-cod-arrow_up"))
                           (if clutch--sort-descending "▼" "▲")
                           hi)))
                   (when s
                     (propertize s 'clutch-header-icon t)))))
         (pin (when (memq cidx clutch--pinned-columns)
                (let ((s (clutch--icon '(mdicon . "nf-md-pin") "∎")))
                  (add-face-text-property 0 (length s) pin-face 'append s)
                  (propertize s 'clutch-header-icon t)))))
    (if (or sort pin)
        (concat (or sort "") (or pin "") (if (or sort pin) " " "") name)
      name)))

(defun clutch--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each column name carries a `clutch-header-col' text property
so the active-column overlay can find it."
  (let ((padding clutch-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((name (nth cidx clutch--result-columns))
             (w (aref widths cidx))
             (label (clutch--header-label name cidx))
             (truncated (if (> (string-width label) w)
                            (truncate-string-to-width label w)
                          label))
             (pads (clutch--center-padding-widths (string-width truncated) w))
             (lead (make-string (car pads) ?\s))
             (trail (make-string (cdr pads) ?\s))
             (cell (concat lead truncated trail))
             (face (if (memq cidx clutch--pinned-columns)
                       'clutch-pinned-header-face
                     'clutch-header-face))
             (pad-str (make-string padding ?\s)))
        ;; Append base face so icon-specific face (e.g. pin color) is preserved.
        (add-face-text-property 0 (length cell) face 'append cell)
        (push (concat (propertize "│" 'face 'clutch-border-face)
                      pad-str
                      (propertize cell
                                  'clutch-header-col cidx)
                      pad-str)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "│" 'face 'clutch-border-face))))

(defun clutch--cell-face (val edited cidx)
  "Return the display face for a cell with VAL at CIDX, EDITED if modified."
  (cond (edited 'clutch-modified-face)
        ((null val) 'clutch-null-face)
        ((assq cidx clutch--fk-info) 'clutch-fk-face)
        (t nil)))

(defun clutch--cell-display-value (val w col-def edited)
  "Return the padded display string for a cell value VAL in column width W.
COL-DEF is the column definition plist, EDITED is the pending edit cons or nil."
  (let* ((display-val (if edited (cdr edited) val))
         (s (replace-regexp-in-string "\n" "↵" (clutch--format-value display-val)))
         (formatted (if (and (not edited)
                             (clutch--long-field-type-p col-def)
                             (> (length s) w)
                             (not (stringp display-val)))
                        (clutch--long-field-placeholder col-def)
                      s)))
    (clutch--string-pad
     (if (> (string-width formatted) w)
         (truncate-string-to-width formatted w)
       formatted)
     w
     (clutch--numeric-type-p col-def))))

(defun clutch--render-cell (row ridx cidx widths)
  "Render cell at column CIDX of ROW at row index RIDX.
WIDTHS is the width vector.  Returns a propertized string
including the leading border and padding."
  (let* ((val     (nth cidx row))
         (col-def (nth cidx clutch--result-column-defs))
         (edited  (assoc (cons ridx cidx) clutch--pending-edits))
         (w       (aref widths cidx))
         (padded  (clutch--cell-display-value val w col-def edited))
         (face    (clutch--cell-face val edited cidx))
         (pad-str (make-string clutch-column-padding ?\s)))
    (concat (propertize "│" 'face 'clutch-border-face)
            pad-str
            (propertize padded
                        'clutch-row-idx ridx
                        'clutch-col-idx cidx
                        'clutch-full-value (if edited (cdr edited) val)
                        'face face)
            pad-str)))

(defun clutch--render-row (row ridx visible-cols widths)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector.
Returns a propertized string."
  (concat (mapconcat (lambda (cidx)
                       (clutch--render-cell row ridx cidx widths))
                     visible-cols "")
          (propertize "│" 'face 'clutch-border-face)))

(defun clutch--footer-row-summary (row-count total-rows)
  "Build propertized row count display for the footer.
ROW-COUNT is the current page count, TOTAL-ROWS is the overall total or nil."
  (let ((hi 'font-lock-keyword-face)
        (dim 'font-lock-comment-face))
    (if (and total-rows (= total-rows row-count))
        (concat (propertize (format "%d" total-rows) 'face hi)
                (propertize " rows" 'face dim))
      (concat (propertize (format "%d" row-count) 'face hi)
              (propertize " of " 'face dim)
              (propertize (if total-rows (format "%d" total-rows) "?")
                          'face hi)
              (propertize " rows" 'face dim)))))

(defun clutch--footer-aggregate-part ()
  "Build footer part for the last aggregate summary."
  (when-let* ((stats clutch--aggregate-summary))
    (let* ((dim 'font-lock-comment-face)
           (hi 'font-lock-keyword-face)
           (label (plist-get stats :label))
           (label-part (unless (string= label "selection")
                         (propertize (format "[%s] " label) 'face dim))))
      (if (> (plist-get stats :count) 0)
          (concat
           (propertize (concat (clutch--icon '(mdicon . "nf-md-calculator_variant") "∑") " ")
                       'face dim)
           label-part
           (propertize (format " sum=%g avg=%g min=%g max=%g"
                               (plist-get stats :sum)
                               (plist-get stats :avg)
                               (plist-get stats :min)
                               (plist-get stats :max))
                       'face hi)
           (propertize (format " [r%d c%d s%d]"
                               (plist-get stats :rows)
                               (plist-get stats :cells)
                               (plist-get stats :skipped))
                       'face dim))
        (concat
         (propertize (concat (clutch--icon '(mdicon . "nf-md-calculator_variant") "∑") " ")
                     'face dim)
         label-part
         (propertize " n/a" 'face hi)
         (propertize (format " [r%d c%d s%d]"
                             (plist-get stats :rows)
                             (plist-get stats :cells)
                             (plist-get stats :skipped))
                     'face dim))))))

(defun clutch--footer-filter-parts ()
  "Build footer parts for active filters and aggregate summary.
Returns a list of propertized strings (may be empty)."
  (delq nil
        (list
         (clutch--footer-aggregate-part)
         (when clutch--where-filter
           (let ((icon (clutch--icon '(codicon . "nf-cod-filter") "W:")))
             (concat (propertize (concat icon " ") 'face 'font-lock-warning-face)
                     (propertize clutch--where-filter
                                 'face 'font-lock-warning-face))))
         (when clutch--filter-pattern
           (let ((icon (clutch--icon '(codicon . "nf-cod-search") "/:")))
             (concat (propertize (concat icon " ") 'face 'font-lock-string-face)
                     (propertize clutch--filter-pattern
                                 'face 'font-lock-string-face)))))))

(defun clutch--footer-main-parts (row-count page-num page-size
                                             total-rows col-num-pages col-cur-page)
  "Return list of main footer part strings for pagination state."
  (let* ((hi 'font-lock-keyword-face)
         (dim 'font-lock-comment-face)
         (total-pages (when total-rows
                        (max 1 (ceiling total-rows (float page-size))))))
    (delq nil
          (list
           (concat (propertize (concat (clutch--icon '(mdicon . "nf-md-sigma") "Σ") " ")
                               'face dim)
                   (clutch--footer-row-summary row-count total-rows))
           (concat (propertize (concat (clutch--icon '(codicon . "nf-cod-files") "⊞") " ")
                               'face dim)
                   (propertize (format "%d" (1+ page-num)) 'face hi)
                   (propertize " / " 'face dim)
                   (propertize (format "%d" (or total-pages 1)) 'face hi))
           (when (> col-num-pages 1)
             (concat (propertize (concat (clutch--icon '(codicon . "nf-cod-split_horizontal") "⫼") " ")
                                 'face dim)
                     (propertize (format "%d/%d" col-cur-page col-num-pages) 'face hi)))
           (when clutch--query-elapsed
             (concat (propertize (concat (clutch--icon '(mdicon . "nf-md-timer_outline") "⏱") " ")
                                 'face dim)
                     (propertize (clutch--format-elapsed clutch--query-elapsed)
                                 'face hi)))))))

(defun clutch--render-footer (row-count page-num page-size
                                        total-rows col-num-pages col-cur-page)
  "Return the tab-line footer string for pagination state."
  (let ((sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (mapconcat #'identity
               (append (clutch--footer-main-parts row-count page-num page-size
                                                  total-rows col-num-pages col-cur-page)
                       (clutch--footer-filter-parts))
               sep)))

(defun clutch--effective-widths ()
  "Return column widths adjusted for header indicator icons.
Columns with sort or pin indicators get wider to fit the label."
  (let ((widths (copy-sequence clutch--column-widths)))
    (dotimes (cidx (length widths))
      (let* ((name (nth cidx clutch--result-columns))
             (label (clutch--header-label name cidx))
             (label-w (string-width label)))
        (when (> label-w (aref widths cidx))
          (aset widths cidx label-w))))
    widths))

(defun clutch--header-cell (cidx widths &optional active-cidx)
  "Build a single header cell string for column CIDX.
WIDTHS is the effective width vector.
ACTIVE-CIDX is the highlighted column index, if any."
  (let* ((name (nth cidx clutch--result-columns))
         (w (aref widths cidx))
         (label (clutch--header-label name cidx))
         (truncated (if (> (string-width label) w)
                        (truncate-string-to-width label w)
                      label))
         (pads (clutch--center-padding-widths (string-width truncated) w))
         (lead (make-string (car pads) ?\s))
         (trail (make-string (cdr pads) ?\s))
         (label (copy-sequence truncated))
         (face (cond
                ((eql cidx active-cidx) 'clutch-header-active-face)
                ((memq cidx clutch--pinned-columns)
                 'clutch-pinned-header-face)
                (t 'clutch-header-face)))
         (pad-str (make-string clutch-column-padding ?\s)))
    ;; Append base/underline style without overwriting icon-specific face.
    (add-face-text-property 0 (length label)
                            (list :inherit face :underline t)
                            'append label)
    ;; Keep sort/pin icons un-underlined for cleaner visual hierarchy.
    (dotimes (i (length label))
      (when (get-text-property i 'clutch-header-icon label)
        (let ((icon-face (or (get-text-property i 'face label) face)))
          (put-text-property i (1+ i) 'face
                             (list '(:underline nil) icon-face)
                             label))))
    (concat (propertize "│" 'face 'clutch-border-face)
            pad-str
            (propertize lead 'face face)
            (propertize label
                        'clutch-header-col cidx)
            (propertize trail 'face face)
            pad-str)))

(defun clutch--build-header-line (visible-cols widths nw
                                                  has-prev has-next
                                                  &optional active-cidx)
  "Build the header-line-format string for the column header row.
VISIBLE-COLS, WIDTHS describe columns.
NW is the digit width for the row number column.
HAS-PREV/HAS-NEXT control edge border indicators.
ACTIVE-CIDX highlights that column when non-nil."
  (let* ((edge (lambda (s) (clutch--replace-edge-borders s has-prev has-next)))
         (bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (cells (mapcar (lambda (cidx)
                          (clutch--header-cell cidx widths active-cidx))
                        visible-cols))
         (data-header (funcall edge
                               (concat (apply #'concat cells)
                                       (propertize "│" 'face bface)))))
    ;; 1 char for mark column + nw for row number + padding
    (concat (propertize "│" 'face bface)
            " " (make-string nw ?\s) pad-str
            data-header)))

(defun clutch--build-separator (visible-cols widths position
                                                   nw edge-fn)
  "Build a table separator line with row-number column.
VISIBLE-COLS and WIDTHS describe data columns.
POSITION is \\='top, \\='middle, or \\='bottom.
NW is the row-number digit width.
EDGE-FN applies column-page edge indicators."
  (let* ((bface 'clutch-border-face)
         ;; +1 for the mark column char
         (rn-dash (+ 1 nw clutch-column-padding))
         (raw (clutch--render-separator visible-cols widths position))
         (cross (pcase position ('top "┬") ('bottom "┴") (_ "┼")))
         (left (pcase position ('top "┌") ('bottom "└") (_ "├")))
         (data-part (concat cross (substring raw 1)))
         (data-edged (funcall edge-fn (propertize data-part 'face bface)))
         (rn (propertize (concat left (make-string rn-dash ?─)) 'face bface)))
    (concat rn data-edged)))

(defun clutch--insert-data-rows (rows visible-cols widths nw
                                         global-first-row edge-fn)
  "Insert data ROWS into the current buffer.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
GLOBAL-FIRST-ROW is the 0-based offset for numbering.
EDGE-FN applies column-page edge indicators."
  (let ((bface 'clutch-border-face)
        (pad-str (make-string clutch-column-padding ?\s))
        (marked clutch--marked-rows))
    (cl-loop for row in rows
             for ridx from 0
             for data-row = (funcall edge-fn
                                     (clutch--render-row
                                      row ridx visible-cols widths))
             for mark-char = (if (memq ridx marked) "*" " ")
             for num-label = (string-pad
                              (number-to-string
                               (1+ (+ global-first-row ridx)))
                              nw nil t)
             for num-face = (if (memq ridx marked)
                                'clutch-marked-face 'shadow)
             do (insert (propertize "│" 'face bface)
                        (propertize mark-char 'face num-face)
                        (propertize num-label 'face num-face)
                        pad-str
                        data-row "\n"))))

(defun clutch--update-result-line-formats (rows col-num-pages cur-page
                                                has-prev has-next
                                                visible-cols widths nw)
  "Set tab-line-format and header-line-format for the result buffer."
  (setq tab-line-format
        (concat (propertize " " 'display '(space :align-to 0))
                (clutch--render-footer
                 (length rows) clutch--page-current
                 clutch-result-max-rows clutch--page-total-rows
                 col-num-pages (1+ cur-page))))
  (setq header-line-format
        (concat (propertize " " 'display '(space :align-to 0))
                (clutch--build-header-line visible-cols widths nw
                                           has-prev has-next
                                           clutch--header-active-col))))

(defun clutch--render-pending-edits-header ()
  "Insert a pending edits notification line if there are pending edits."
  (when clutch--pending-edits
    (insert (propertize
             (format "-- %d pending edit%s\n"
                     (length clutch--pending-edits)
                     (if (= (length clutch--pending-edits) 1) "" "s"))
             'face 'clutch-modified-face))))

(defun clutch--render-result ()
  "Render the result buffer content using column paging.
Preserves point position (row + column) across the render."
  (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
         (save-cidx (get-text-property (point) 'clutch-col-idx))
         (inhibit-read-only t)
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (rows (or clutch--filtered-rows clutch--result-rows))
         (col-num-pages (length clutch--column-pages))
         (cur-page clutch--current-col-page)
         (has-prev (> cur-page 0))
         (has-next (< cur-page (1- col-num-pages)))
         (edge-fn (lambda (s) (clutch--replace-edge-borders s has-prev has-next)))
         (nw (clutch--row-number-digits))
         (global-first-row (* clutch--page-current clutch-result-max-rows)))
    (erase-buffer)
    (clutch--update-result-line-formats rows col-num-pages cur-page
                                        has-prev has-next visible-cols widths nw)
    (clutch--render-pending-edits-header)
    (clutch--insert-data-rows rows visible-cols widths nw global-first-row edge-fn)
    (if save-ridx
        (clutch--goto-cell save-ridx save-cidx)
      (goto-char (point-min)))))

(defun clutch--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'clutch-col-idx))

(defun clutch--goto-cell (ridx cidx)
  "Move point to the cell at ROW-IDX RIDX and COL-IDX CIDX.
Falls back to the same row (any column), then point-min."
  (goto-char (point-min))
  (let ((found nil))
    (while (and (not found)
                (setq found (text-property-search-forward
                             'clutch-row-idx ridx #'eq)))
      (let ((beg (prop-match-beginning found)))
        (if (eq (get-text-property beg 'clutch-col-idx) cidx)
            (goto-char beg)
          (setq found nil))))
    (unless found
      ;; Fall back: find the same row, any column
      (goto-char (point-min))
      (if-let* ((m (text-property-search-forward 'clutch-row-idx ridx #'eq)))
          (goto-char (prop-match-beginning m))
        (goto-char (point-min))))))

(defun clutch--row-number-digits ()
  "Return the digit width needed for row numbers."
  (let* ((row-count (length clutch--result-rows))
         (global-last (+ (* clutch--page-current
                            clutch-result-max-rows)
                         row-count)))
    (max 3 (length (number-to-string global-last)))))

(defun clutch--refresh-display ()
  "Recompute column pages for current window width and re-render.
Preserves cursor position (row + column) across the refresh."
  (when clutch--column-widths
    (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
           (save-cidx (get-text-property (point) 'clutch-col-idx))
           (win (get-buffer-window (current-buffer)))
           (win-width (if win (window-body-width win) 80))
           (nw (clutch--row-number-digits))
           (width (- win-width 1 (* 2 clutch-column-padding) nw)))
      (setq clutch--column-pages
            (clutch--compute-column-pages
             (clutch--effective-widths)
             clutch--pinned-columns
             width))
      (let ((max-page (1- (length clutch--column-pages))))
        (setq clutch--current-col-page
              (max 0 (min clutch--current-col-page max-page))))
      (setq clutch--last-window-width win-width)
      (setq clutch--header-active-col nil)
      (when clutch--row-overlay
        (delete-overlay clutch--row-overlay)
        (setq clutch--row-overlay nil))
      (clutch--render-result)
      (when save-ridx
        (clutch--goto-cell save-ridx save-cidx)))))

(defun clutch--window-size-change (frame)
  "Handle window size changes for result buffers in FRAME."
  (dolist (win (window-list frame 'no-mini))
    (let ((buf (window-buffer win)))
      (when (buffer-local-value 'clutch--column-widths buf)
        (let ((new-width (window-body-width win)))
          (unless (eq new-width
                      (buffer-local-value 'clutch--last-window-width buf))
            (with-current-buffer buf
              (clutch--refresh-display))))))))

(defvar clutch--window-size-hook-enabled nil
  "Non-nil when `clutch--window-size-change' is installed globally.")

(defun clutch--enable-window-size-hook ()
  "Ensure `clutch--window-size-change' is installed once."
  (unless clutch--window-size-hook-enabled
    (add-hook 'window-size-change-functions #'clutch--window-size-change)
    (setq clutch--window-size-hook-enabled t)))

(defun clutch--has-live-result-buffer-p (&optional ignore-buffer)
  "Return non-nil if any live result buffer exists.
IGNORE-BUFFER, when non-nil, is excluded from the check."
  (cl-some (lambda (buf)
             (and (buffer-live-p buf)
                  (not (eq buf ignore-buffer))
                  (with-current-buffer buf
                    (derived-mode-p 'clutch-result-mode))))
           (buffer-list)))

(defun clutch--disable-window-size-hook-if-unused (&optional ignore-buffer)
  "Remove global window-size hook when no result buffers remain.
IGNORE-BUFFER is excluded from liveness checks."
  (when (and clutch--window-size-hook-enabled
             (not (clutch--has-live-result-buffer-p ignore-buffer)))
    (remove-hook 'window-size-change-functions #'clutch--window-size-change)
    (setq clutch--window-size-hook-enabled nil)))

(defun clutch--result-buffer-cleanup ()
  "Cleanup hook state when a result buffer is being removed."
  (clutch--disable-window-size-hook-if-unused (current-buffer)))

(defun clutch--display-select-result (col-names rows columns)
  "Render a SELECT result with COL-NAMES, ROWS, and COLUMNS metadata."
  (let ((inhibit-read-only t))
    (setq-local clutch--column-widths
                (clutch--compute-column-widths
                 col-names rows columns))
    (clutch--refresh-display)))

(defun clutch--display-dml-result (result sql elapsed)
  "Render a DML RESULT (INSERT/UPDATE/DELETE) with SQL and ELAPSED time."
  (let ((inhibit-read-only t))
    (erase-buffer)
    (setq-local clutch--column-widths nil)
    (insert (propertize (format "-- %s\n" (string-trim sql))
                        'face 'font-lock-comment-face))
    (insert (format "Affected rows: %s\n"
                    (or (clutch-db-result-affected-rows result) 0)))
    (when-let* ((id (clutch-db-result-last-insert-id result))
                ((> id 0)))
      (insert (format "Last insert ID: %s\n" id)))
    (when-let* ((w (clutch-db-result-warnings result))
                ((> w 0)))
      (insert (format "Warnings: %s\n" w)))
    (insert (propertize (format "\nCompleted in %s\n"
                                (clutch--format-elapsed elapsed))
                        'face 'font-lock-comment-face))
    (goto-char (point-min))))

(defun clutch--init-select-result-state (col-names columns rows)
  "Initialize buffer-local state for a non-paginated SELECT result."
  (setq-local clutch--base-query nil)
  (setq-local clutch--result-columns col-names)
  (setq-local clutch--result-column-defs columns)
  (setq-local clutch--result-rows rows)
  (setq-local clutch--pending-edits nil)
  (setq-local clutch--marked-rows nil)
  (setq-local clutch--current-col-page 0)
  (setq-local clutch--pinned-columns nil)
  (setq-local clutch--sort-column nil)
  (setq-local clutch--sort-descending nil)
  (setq-local clutch--page-current 0)
  (setq-local clutch--page-total-rows (length rows))
  (setq-local clutch--order-by nil)
  (setq-local clutch--aggregate-summary nil))

(defun clutch--display-result (result sql elapsed)
  "Display RESULT in the result buffer.
SQL is the query text, ELAPSED the time in seconds.
If the result has columns, shows a table; otherwise shows DML summary."
  (let* ((buf-name (clutch--result-buffer-name))
         (buf      (get-buffer-create buf-name))
         (columns  (clutch-db-result-columns result))
         (col-names (when columns (clutch--column-names columns)))
         (rows     (clutch-db-result-rows result)))
    (with-current-buffer buf
      (clutch-result-mode)
      (setq-local clutch--last-query sql)
      (setq-local clutch-connection (clutch-db-result-connection result))
      (if col-names
          (progn
            (clutch--init-select-result-state col-names columns rows)
            (clutch--display-select-result col-names rows columns))
        (clutch--display-dml-result result sql elapsed)))
    (clutch--show-result-buffer buf)))

;;;; SQL pagination helpers

(defun clutch--sql-has-limit-p (sql)
  "Return non-nil if SQL has a top-level LIMIT clause."
  (clutch-db-sql-has-top-level-limit-p sql))

(defun clutch--build-paged-sql (base-sql page-num page-size &optional order-by)
  "Build a paged SQL query wrapping BASE-SQL.
PAGE-NUM is 0-based, PAGE-SIZE is the row limit.
ORDER-BY is a cons (COL-NAME . DIRECTION) or nil.
If BASE-SQL already has LIMIT, return it unchanged.
Delegates to the backend for dialect-specific pagination."
  (if (clutch--sql-has-limit-p base-sql)
      base-sql
    (clutch-db-build-paged-sql clutch-connection base-sql
                                  page-num page-size order-by)))

(defun clutch--update-page-state (columns rows elapsed page-num)
  "Update buffer-local state for a new page of results.
COLUMNS, ROWS, ELAPSED, and PAGE-NUM describe the new page."
  (let ((col-names (clutch--column-names columns)))
    (setq-local clutch--result-columns col-names
                clutch--result-column-defs columns
                clutch--result-rows rows
                clutch--page-current page-num
                clutch--pending-edits nil
                clutch--marked-rows nil
                clutch--query-elapsed elapsed
                clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--column-widths
                (clutch--compute-column-widths col-names rows columns))))

(defun clutch--execute-page (page-num)
  "Execute the query for PAGE-NUM and refresh the result buffer display.
Uses `clutch--base-query' as the base SQL.
Signals an error if pagination is not available."
  (unless clutch--base-query
    (user-error "Pagination not available for this query"))
  (unless (clutch--connection-alive-p clutch-connection)
    (user-error "Not connected"))
  (let* ((paged-sql (clutch--build-paged-sql
                     clutch--base-query page-num
                     clutch-result-max-rows clutch--order-by))
         (start (float-time))
         (result (condition-case err
                     (clutch-db-query clutch-connection paged-sql)
                   (clutch-db-error
                    (user-error "Query error: %s"
                                (error-message-string err)))))
         (elapsed (- (float-time) start))
         (rows (clutch-db-result-rows result)))
    (clutch--update-page-state
     (clutch-db-result-columns result) rows elapsed page-num)
    (clutch--refresh-display)
    (message "Page %d loaded (%s, %d row%s)"
             (1+ page-num)
             (clutch--format-elapsed elapsed)
             (length rows)
             (if (= (length rows) 1) "" "s"))))

;;;; Query execution engine

(defun clutch--strip-leading-comments (sql)
  "Strip leading SQL comments and whitespace from SQL.
Handles single-line (--) and multi-line (/* */) comments."
  (let ((s (string-trim-left sql)))
    (while (or (string-prefix-p "--" s)
               (string-prefix-p "/*" s))
      (setq s (string-trim-left
               (cond
                ((string-prefix-p "--" s)
                 (if-let* ((nl (string-search "\n" s)))
                     (substring s (1+ nl))
                   ""))
                ((string-prefix-p "/*" s)
                 (if-let* ((end (string-search "*/" s)))
                     (substring s (+ end 2))
                   ""))))))
    s))

(defun clutch--destructive-query-p (sql)
  "Return non-nil if SQL is a destructive operation.
Leading SQL comments are stripped before checking."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:DELETE\\|DROP\\|TRUNCATE\\|ALTER\\)\\b"
                    (upcase trimmed))))

(defun clutch--sql-main-op-keyword (sql)
  "Return main top-level operation keyword for SQL, or nil."
  (let* ((normalized (clutch--sql-normalize-for-rewrite sql))
         (candidates
          (cl-loop for kw in '("UPDATE" "DELETE" "SELECT" "INSERT" "REPLACE" "MERGE")
                   for pos = (clutch--sql-find-top-level-clause normalized kw)
                   when pos collect (cons kw pos)))
         (first (car (sort candidates (lambda (a b) (< (cdr a) (cdr b)))))))
    (car-safe first)))

(defun clutch--risky-dml-p (sql)
  "Return non-nil for top-level UPDATE/DELETE statements without WHERE."
  (let ((normalized (clutch--sql-normalize-for-rewrite sql))
        (main-op (clutch--sql-main-op-keyword sql)))
    (and (member main-op '("UPDATE" "DELETE"))
         (not (clutch--sql-find-top-level-clause normalized "WHERE")))))

(defun clutch--require-risky-dml-confirmation (sql)
  "Require explicit typed confirmation for risky DML SQL."
  (when (clutch--risky-dml-p sql)
    (let ((token (read-string "High-risk DML (no WHERE). Type YES to continue: ")))
      (unless (string= token "YES")
        (user-error "Query cancelled")))))

(defun clutch--select-query-p (sql)
  "Return non-nil if SQL is a SELECT query.
Leading SQL comments are stripped before checking."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:SELECT\\|WITH\\)\\b"
                    (upcase trimmed))))

(defun clutch--init-result-state (conn sql columns rows elapsed)
  "Initialize buffer-local state for a fresh query result.
CONN is the connection, SQL the original query, COLUMNS and ROWS
the result data, ELAPSED the query time.  Returns column names."
  (let ((col-names (clutch--column-names columns)))
    (setq-local clutch--last-query sql
                clutch--base-query sql
                clutch-connection conn
                clutch--result-columns col-names
                clutch--result-column-defs columns
                clutch--result-rows rows
                clutch--pending-edits nil
                clutch--marked-rows nil
                clutch--current-col-page 0
                clutch--pinned-columns nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows nil
                clutch--order-by nil
                clutch--query-elapsed elapsed
                clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--aggregate-summary nil)
    col-names))

(defun clutch--execute-select (sql connection)
  "Execute a SELECT SQL query with pagination on CONNECTION.
Returns the query result."
  (let* ((page-size clutch-result-max-rows)
         (paged-sql (clutch-db-build-paged-sql connection sql 0 page-size))
         (start (float-time))
         (result (condition-case err
                     (clutch-db-query connection paged-sql)
                   (clutch-db-error
                    (user-error "Query error: %s"
                                (error-message-string err)))))
         (elapsed (- (float-time) start))
         (buf (get-buffer-create (clutch--result-buffer-name)))
         (columns (clutch-db-result-columns result))
         (rows (clutch-db-result-rows result)))
    (with-current-buffer buf
      (clutch-result-mode)
      (let ((col-names (clutch--init-result-state
                        connection sql columns rows elapsed)))
        (when col-names
          (clutch--display-select-result col-names rows columns)))
      (clutch--load-fk-info))
    (clutch--show-result-buffer buf)
    result))

(defun clutch--execute-dml (sql connection)
  "Execute a DML SQL query on CONNECTION and display results.
Returns the query result."
  (setq clutch--last-query sql)
  (let* ((start (float-time))
         (result (condition-case err
                     (clutch-db-query connection sql)
                   (clutch-db-error
                    (user-error "Query error: %s"
                                (error-message-string err)))))
         (elapsed (- (float-time) start)))
    (clutch--display-result result sql elapsed)
    result))

(defun clutch--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Times execution and displays results.
For SELECT queries, applies pagination (LIMIT/OFFSET).
Prompts for confirmation on destructive operations."
  (clutch--ensure-connection)
  (let ((connection (or conn clutch-connection))
        (source-win (selected-window)))
    (when (clutch--destructive-query-p sql)
      (unless (yes-or-no-p
               (format "Execute destructive query?\n  %s\n"
                       (truncate-string-to-width (string-trim sql) 80)))
        (user-error "Query cancelled")))
    (clutch--require-risky-dml-confirmation sql)
    (setq clutch--executing-p t)
    (clutch--update-mode-line)
    (redisplay t)
    (unwind-protect
        (condition-case nil
            (let ((clutch--source-window source-win))
              (if (clutch--select-query-p sql)
                  (clutch--execute-select sql connection)
                (clutch--execute-dml sql connection)))
          (quit
           ;; A quit during network read can leave protocol state indeterminate.
           ;; Drop the connection so the next command reconnects cleanly.
           (when (clutch--connection-alive-p connection)
             (clutch-db-disconnect connection))
           (when (eq connection clutch-connection)
             (setq clutch-connection nil))
           (user-error "Query interrupted")))
      (when (window-live-p source-win)
        (select-window source-win))
      (setq clutch--executing-p nil)
      (clutch--update-mode-line))))

(defun clutch--trim-sql-bounds (beg end)
  "Return (BEG . END) trimmed to non-whitespace between BEG and END."
  (save-excursion
    (goto-char beg)
    (skip-chars-forward " \t\r\n" end)
    (let ((tbeg (point)))
      (goto-char end)
      (skip-chars-backward " \t\r\n" beg)
      (let ((tend (point)))
        (when (< tbeg tend)
          (cons tbeg tend))))))

(defun clutch--mark-executed-sql-region (beg end)
  "Highlight the last successfully executed SQL region BEG..END."
  (when-let* ((trimmed (clutch--trim-sql-bounds beg end))
              (tbeg (car trimmed))
              (tend (cdr trimmed)))
    (when (overlayp clutch--executed-sql-overlay)
      (delete-overlay clutch--executed-sql-overlay))
    (setq clutch--executed-sql-overlay (make-overlay tbeg tend))
    (overlay-put clutch--executed-sql-overlay 'face 'clutch-executed-sql-face)
    (overlay-put clutch--executed-sql-overlay 'priority 1000)
    (overlay-put clutch--executed-sql-overlay 'evaporate t)))

(defun clutch--execute-and-mark (sql beg end &optional conn)
  "Execute SQL and mark BEG..END on success."
  (clutch--execute sql conn)
  (clutch--mark-executed-sql-region beg end))

;;;; Query-at-point detection

(defun clutch--query-bounds-at-point ()
  "Return the SQL statement bounds around point as (BEG . END)."
  (let ((delimiter "\\(;\\|^[[:space:]]*$\\)"))
    (cons (save-excursion
            (if (re-search-backward delimiter nil t)
                (match-end 0)
              (point-min)))
          (save-excursion
            (if (re-search-forward delimiter nil t)
                (match-beginning 0)
              (point-max))))))

(defun clutch--preview-sql-buffer (sql)
  "Display SQL in the *clutch-preview* buffer."
  (let ((buf (get-buffer-create "*clutch-preview*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert sql)
        (goto-char (point-min))
        (if (derived-mode-p 'sql-mode)
            (setq buffer-read-only t)
          (when (fboundp 'sql-mode)
            (sql-mode))
          (setq buffer-read-only t))))
    (pop-to-buffer buf)))

(defun clutch-preview-execution-sql ()
  "Preview SQL that would execute in the current workflow."
  (interactive)
  (let ((sql
         (cond
          ((derived-mode-p 'clutch-result-mode)
           (cond
            (clutch--pending-edits
             (mapconcat (lambda (s) (concat s ";"))
                        (nreverse (clutch-result--build-update-statements))
                        "\n"))
            ((use-region-p)
             (mapconcat (lambda (s) (concat s ";"))
                        (clutch-result--build-delete-statements)
                        "\n"))
            (clutch--last-query
             clutch--last-query)))
          ((use-region-p)
           (string-trim (buffer-substring-no-properties
                         (region-beginning) (region-end))))
          (t
           (pcase-let* ((`(,beg . ,end) (clutch--query-bounds-at-point)))
             (string-trim (buffer-substring-no-properties beg end)))))))
    (when (or (null sql) (string-empty-p sql))
      (user-error "No SQL to preview"))
    (clutch--preview-sql-buffer sql)))

;;;; Interactive commands

(defun clutch-execute-query-at-point ()
  "Execute the SQL query at point."
  (interactive)
  (pcase-let* ((`(,beg . ,end) (clutch--query-bounds-at-point))
               (sql (string-trim (buffer-substring-no-properties beg end))))
    (when (string-empty-p sql)
      (user-error "No query at point"))
    (clutch--ensure-connection)
    (clutch--execute-and-mark sql beg end)))

(defun clutch--split-statements (sql)
  "Split SQL into individual statements on unquoted semicolons.
Skips semicolons inside single-quoted strings, -- line comments,
and /* */ block comments."
  (let ((stmts nil) (start 0) (in-string nil) (i 0) (len (length sql)))
    (while (< i len)
      (let ((ch (aref sql i)))
        (cond
         (in-string
          (when (= ch in-string)
            (setq in-string nil)))
         ((= ch ?')  (setq in-string ?'))
         ((= ch ?\") (setq in-string ?\"))
         ((and (= ch ?-) (< (1+ i) len) (= (aref sql (1+ i)) ?-))
          (while (and (< i len) (/= (aref sql i) ?\n)) (cl-incf i)))
         ((and (= ch ?/) (< (1+ i) len) (= (aref sql (1+ i)) ?*))
          (cl-incf i 2)
          (while (and (< (1+ i) len)
                      (not (and (= (aref sql i) ?*) (= (aref sql (1+ i)) ?/))))
            (cl-incf i))
          (cl-incf i))
         ((= ch ?\;)
          (let ((stmt (string-trim (substring sql start i))))
            (unless (string-empty-p stmt) (push stmt stmts)))
          (setq start (1+ i)))))
      (cl-incf i))
    (let ((tail (string-trim (substring sql start))))
      (unless (string-empty-p tail) (push tail stmts)))
    (nreverse stmts)))

(defun clutch--execute-statements (stmts)
  "Execute STMTS sequentially.
DML/DDL statements run silently; the final SELECT (if any) opens a
result buffer.  Stops and reports on the first error."
  (let* ((last (car (last stmts)))
         (before-last (butlast stmts))
         (done 0))
    (dolist (stmt before-last)
      (condition-case err
          (progn (clutch-db-query clutch-connection stmt) (cl-incf done))
        (quit
         (when (clutch--connection-alive-p clutch-connection)
           (clutch-db-disconnect clutch-connection))
         (setq clutch-connection nil)
         (user-error "Query interrupted"))
        (clutch-db-error
         (user-error "Statement %d failed: %s" (1+ done)
                     (error-message-string err)))))
    (if (clutch--select-query-p last)
        (progn
          (when (> done 0)
            (message "%d statement%s executed" done (if (= done 1) "" "s")))
          (clutch--execute last))
      (condition-case err
          (progn (clutch-db-query clutch-connection last) (cl-incf done)
                 (message "%d statement%s executed"
                          done (if (= done 1) "" "s")))
        (quit
         (when (clutch--connection-alive-p clutch-connection)
           (clutch-db-disconnect clutch-connection))
         (setq clutch-connection nil)
         (user-error "Query interrupted"))
        (clutch-db-error
         (user-error "Statement %d failed: %s" (1+ done)
                     (error-message-string err)))))))

(defun clutch-execute-dwim (beg end)
  "Execute region if active, otherwise execute query at point.
When the region contains multiple semicolon-separated statements,
they are executed sequentially."
  (interactive
   (if (use-region-p)
       (list (region-beginning) (region-end))
     (list (point) (point))))
  (clutch--ensure-connection)
  (if (use-region-p)
      (let* ((sql   (string-trim (buffer-substring-no-properties beg end)))
             (stmts (clutch--split-statements sql)))
        (if (cdr stmts)
            (clutch--execute-statements stmts)
          (clutch--execute-and-mark sql beg end)))
    (pcase-let* ((`(,qb . ,qe) (clutch--query-bounds-at-point))
                 (sql (string-trim (buffer-substring-no-properties qb qe))))
      (when (string-empty-p sql)
        (user-error "No query at point"))
      (clutch--execute-and-mark sql qb qe))))

(defun clutch-execute-region (beg end)
  "Execute SQL in the region from BEG to END."
  (interactive "r")
  (clutch--ensure-connection)
  (clutch--execute-and-mark
   (string-trim (buffer-substring-no-properties beg end))
   beg end))

(defun clutch-execute-buffer ()
  "Execute the entire buffer as a SQL query."
  (interactive)
  (clutch--ensure-connection)
  (clutch--execute-and-mark
   (string-trim (buffer-substring-no-properties (point-min) (point-max)))
   (point-min) (point-max)))

(defun clutch--find-connection ()
  "Find a live database connection from any clutch-mode buffer.
Returns the connection or nil."
  (cl-loop for buf in (buffer-list)
           for conn = (buffer-local-value 'clutch-connection buf)
           when (clutch--connection-alive-p conn)
           return conn))

;;;###autoload
(defun clutch-execute (sql)
  "Execute SQL from any buffer.
With an active region, execute the region.  Otherwise execute the
current line.  Uses the connection from any clutch-mode buffer."
  (interactive
   (list (string-trim
          (if (use-region-p)
              (buffer-substring-no-properties (region-beginning) (region-end))
            (buffer-substring-no-properties
             (line-beginning-position) (line-end-position))))))
  (when (string-empty-p sql)
    (user-error "No SQL to execute"))
  (let* ((conn (or clutch-connection
                   (clutch--find-connection)
                   (user-error "No active connection.  Use M-x clutch-mode then C-c C-e to connect")))
         (beg (if (use-region-p) (region-beginning) (line-beginning-position)))
         (end (if (use-region-p) (region-end) (line-end-position))))
    (clutch--execute-and-mark sql beg end conn)))

;;;; Indirect edit buffer

(defun clutch--string-at-point ()
  "Return the string literal content at point, or nil.
Uses `syntax-ppss' to detect string boundaries, so it works in
any mode that has a proper syntax table (Java, Kotlin, Python,
Go, Ruby, etc.)."
  (let ((ppss (syntax-ppss)))
    (when (nth 3 ppss)
      (let ((str-start (nth 8 ppss)))
        (save-excursion
          (goto-char str-start)
          (forward-sexp 1)
          (buffer-substring-no-properties (1+ str-start) (1- (point))))))))

(defvar clutch-indirect-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'clutch-indirect-execute)
    (define-key map (kbd "C-c C-k") #'clutch-indirect-abort)
    map)
  "Keymap for `clutch-indirect-mode'.")

(define-minor-mode clutch-indirect-mode
  "Minor mode active in indirect SQL edit buffers.
\\<clutch-indirect-mode-map>
Key bindings:
  \\[clutch-indirect-execute]	Execute and close
  \\[clutch-indirect-abort]	Abort and close"
  :lighter " Indirect")

(defun clutch-indirect-execute ()
  "Execute the SQL in the indirect buffer, then close it."
  (interactive)
  (let ((sql (string-trim
              (buffer-substring-no-properties (point-min) (point-max))))
        (conn (or clutch-connection
                  (clutch--find-connection))))
    (when (string-empty-p sql)
      (user-error "No SQL to execute"))
    (unless conn
      (user-error "No active connection"))
    (quit-window 'kill)
    (clutch--execute sql conn)))

(defun clutch-indirect-abort ()
  "Abort the indirect edit buffer."
  (interactive)
  (quit-window 'kill))

(defun clutch--extract-indirect-sql-text ()
  "Return SQL text to populate an indirect edit buffer.
Uses region if active, string literal at point if inside one,
or the current line otherwise."
  (string-trim
   (cond
    ((use-region-p)
     (buffer-substring-no-properties (region-beginning) (region-end)))
    ((clutch--string-at-point))
    (t
     (buffer-substring-no-properties
      (line-beginning-position) (line-end-position))))))

;;;###autoload
(defun clutch-edit-indirect ()
  "Open an indirect `clutch-mode' buffer with SQL extracted from context.
With an active region, use the region.  When point is inside a
string literal (DAO code, etc.), extract the string content.
Otherwise use the current line.

The indirect buffer inherits the connection from any live
`clutch-mode' buffer.  Edit the SQL freely, then press
\\<clutch-indirect-mode-map>\\[clutch-indirect-execute] \
to execute or \\[clutch-indirect-abort] to abort."
  (interactive)
  (let* ((text (clutch--extract-indirect-sql-text))
         (conn (or (bound-and-true-p clutch-connection)
                   (clutch--find-connection)))
         (buf  (generate-new-buffer "*clutch: indirect*")))
    (pop-to-buffer buf)
    (clutch-mode)
    (when conn
      (setq-local clutch-connection conn)
      (clutch--update-mode-line))
    (clutch-indirect-mode 1)
    (insert text)
    (goto-char (point-min))
    (message "Edit SQL, then C-c ' to execute, C-c C-k to abort")))

;;;; Schema cache + completion

(defvar clutch--schema-cache (make-hash-table :test 'equal)
  "Global schema cache.  Keys are connection-key strings.
Values are hash-tables mapping table-name → list of column-name strings.")

(defvar clutch--column-details-cache (make-hash-table :test 'equal)
  "Cache for full column details.  Keys are connection-key strings.
Values are hash-tables mapping table-name → list of column-detail plists
as returned by `clutch-db-column-details'.")

(defvar clutch--table-comment-cache (make-hash-table :test 'equal)
  "Cache for table comments.  Keys are connection-key strings.
Values are hash-tables mapping table-name → comment string or nil.")

(defvar clutch--help-doc-cache (make-hash-table :test 'equal)
  "Cache for live function docs fetched from the database server.
Keys are connection-key strings.
Values are hash-tables mapping UPCASE-NAME → doc plist or \\='not-found.")

(defun clutch--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (condition-case nil
      (let* ((key (clutch--connection-key conn))
             (table-names (clutch-db-list-tables conn))
             (schema (make-hash-table :test 'equal)))
        (dolist (tbl table-names)
          (puthash tbl nil schema))
        (puthash key schema clutch--schema-cache)
        (remhash key clutch--column-details-cache)
        (remhash key clutch--table-comment-cache)
        (remhash key clutch--help-doc-cache)
        (message "Connected — %d tables" (hash-table-count schema)))
    (clutch-db-error nil)))

(defun clutch--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA.
Fetches from the backend if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing)))
    (unless (eq cols 'missing)
      (or cols
          (condition-case nil
              (let ((col-names (clutch-db-list-columns conn table)))
                (puthash table col-names schema)
                col-names)
            (clutch-db-error nil))))))

(defun clutch--ensure-column-details (conn table)
  "Return column details for TABLE on CONN, loading lazily if needed.
Returns a list of plists with :name :type :nullable :primary-key :foreign-key,
or nil on error."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--column-details-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--column-details-cache)
                      h)))
         (cached (gethash table cache 'missing)))
    (if (not (eq cached 'missing))
        cached
      (condition-case nil
          (let ((details (clutch-db-column-details conn table)))
            (puthash table details cache)
            details)
        (clutch-db-error nil)))))

(defun clutch--ensure-table-comment (conn table)
  "Return the comment for TABLE on CONN, loading lazily if needed.
Returns a string or nil."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--table-comment-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--table-comment-cache)
                      h)))
         (cached (gethash table cache 'missing)))
    (if (not (eq cached 'missing))
        cached
      (let ((comment (condition-case nil
                         (clutch-db-table-comment conn table)
                       (clutch-db-error nil))))
        (puthash table comment cache)
        comment))))

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
  (when-let* ((details (clutch--ensure-column-details conn table))
              (col (cl-find col-name details
                            :key (lambda (d) (plist-get d :name))
                            :test #'string=)))
    (let* ((type    (plist-get col :type))
           (comment (plist-get col :comment))
           (extras  (clutch--eldoc-column-extras col))
           (header  (concat (propertize table    'face 'font-lock-type-face)
                            "."
                            (propertize col-name 'face 'font-lock-variable-name-face))))
      (string-join
       (delq nil (list header
                       (propertize type 'face 'font-lock-type-face)
                       (unless (string-empty-p extras) extras)
                       (when comment
                         (propertize (format "— %s" comment) 'face 'shadow))))
       "  "))))

(defun clutch--schema-for-connection ()
  "Return the schema hash-table for the current connection, or nil."
  (when (clutch--connection-alive-p clutch-connection)
    (gethash (clutch--connection-key clutch-connection)
             clutch--schema-cache)))

(defun clutch--tables-in-buffer (schema)
  "Return table names from SCHEMA that appear in the current buffer."
  (let ((text (buffer-substring-no-properties (point-min) (point-max))))
    (cl-loop for tbl in (hash-table-keys schema)
             when (string-match-p (regexp-quote tbl) text)
             collect tbl)))

(defun clutch--tables-in-query (schema)
  "Return known table names in FROM/JOIN/UPDATE clauses of current statement.
Scans only the SQL statement surrounding point, bounded by semicolons or
blank lines.  Falls back to `clutch--tables-in-buffer' when none are found."
  (let* ((delim "\\(;\\|^[[:space:]]*$\\)")
         (beg   (save-excursion
                  (if (re-search-backward delim nil t)
                      (match-end 0) (point-min))))
         (end   (save-excursion
                  (if (re-search-forward delim nil t)
                      (match-beginning 0) (point-max))))
         (text  (buffer-substring-no-properties beg end))
         (case-fold-search t)
         (found (cl-loop for tbl in (hash-table-keys schema)
                         when (string-match-p
                               (format "\\b\\(from\\|join\\|update\\|into\\)[ \t\n]+%s\\b"
                                       (regexp-quote tbl))
                               text)
                         collect tbl)))
    (or found (clutch--tables-in-buffer schema))))

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

;;;; Live function documentation via MySQL HELP

(defun clutch--parse-mysql-help-text (text)
  "Parse a MySQL HELP description TEXT into a (:sig SIG :desc DESC) plist.
Returns nil if TEXT cannot be parsed (no Syntax: section)."
  (let* ((lines (split-string text "\n"))
         (pos   (cl-position-if (lambda (l) (string-match-p "\\`Syntax:" l))
                                lines)))
    (when pos
      (let ((i (1+ pos)) sig desc)
        ;; Skip blank lines immediately after the "Syntax:" header
        (while (and (< i (length lines)) (string-empty-p (nth i lines)))
          (cl-incf i))
        ;; Collect signature lines (until first blank line)
        (let (sig-lines)
          (while (and (< i (length lines)) (not (string-empty-p (nth i lines))))
            (push (string-trim (nth i lines)) sig-lines)
            (cl-incf i))
          (setq sig (string-join (nreverse sig-lines) " / ")))
        ;; Skip blank separator between signature and description
        (while (and (< i (length lines)) (string-empty-p (nth i lines)))
          (cl-incf i))
        ;; Collect first paragraph as description, stopping at URL: or blank line
        (let (desc-lines)
          (while (and (< i (length lines))
                      (not (string-empty-p (nth i lines)))
                      (not (string-prefix-p "URL:" (nth i lines))))
            (push (nth i lines) desc-lines)
            (cl-incf i))
          (setq desc (string-join (nreverse desc-lines) " ")))
        (when (and sig (not (string-empty-p sig)))
          (list :sig sig :desc (or desc "")))))))

(defun clutch--mysql-help-query (conn sym)
  "Query MySQL HELP for SYM on CONN and return a doc plist or nil.
Returns nil when the symbol is unrecognised or the query fails."
  (condition-case nil
      (let* ((result  (clutch-db-query conn (format "HELP '%s'" (upcase sym))))
             (columns (clutch-db-result-columns result))
             (rows    (clutch-db-result-rows result)))
        ;; A single-topic HELP result has 3 columns: name, description, example.
        ;; A list result has 2 columns: name, is_it_category.
        ;; An empty result means nothing was found.
        (when (and rows (>= (length columns) 3))
          (pcase-let ((`(,_name ,desc ,_example) (car rows)))
            (when (stringp desc)
              (clutch--parse-mysql-help-text desc)))))
    (error nil)))

(defun clutch--format-help-doc (doc)
  "Format a DOC plist (:sig SIG :desc DESC) as a propertized eldoc string."
  (let ((sig  (plist-get doc :sig))
        (desc (plist-get doc :desc)))
    (concat (propertize sig 'face 'font-lock-function-name-face)
            (when (and desc (not (string-empty-p desc)))
              (propertize (concat "  — " desc) 'face 'shadow)))))

(defun clutch--ensure-help-doc (conn sym)
  "Return a live HELP eldoc string for SYM from CONN, with caching.
Queries the server on first access; subsequent calls read from cache.
Returns nil when SYM is not a known built-in on this server."
  (let* ((key   (clutch--connection-key conn))
         (cache (or (gethash key clutch--help-doc-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--help-doc-cache)
                      h)))
         (uname (upcase sym))
         (entry (gethash uname cache 'missing)))
    (cond
     ((eq entry 'missing)
      (let ((doc (clutch--mysql-help-query conn sym)))
        (puthash uname (or doc 'not-found) cache)
        (when doc (clutch--format-help-doc doc))))
     ((eq entry 'not-found) nil)
     (t (clutch--format-help-doc entry)))))

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


(defun clutch-sql-keyword-completion-at-point ()
  "Completion-at-point function for SQL keywords.
Works without a database connection."
  (when-let* ((bounds (bounds-of-thing-at-point 'symbol)))
    (list (car bounds) (cdr bounds)
          clutch--sql-keywords
          :exclusive 'no
          :exit-function (lambda (_str status)
                           (when (and (clutch--completion-finished-status-p status)
                                      (not (looking-at-p "\\s-")))
                             (insert " "))))))

(defun clutch-completion-at-point ()
  "Completion-at-point function for SQL identifiers.
Skips column loading if the connection is busy (prevents re-entrancy
when completion triggers during an in-flight query)."
  (when-let* ((schema (clutch--schema-for-connection))
              (conn clutch-connection)
              (bounds (bounds-of-thing-at-point 'symbol)))
    (let* ((beg (car bounds))
           (end (cdr bounds))
           (line-before (buffer-substring-no-properties
                         (line-beginning-position) beg))
           ;; After FROM/JOIN/INTO/UPDATE → table names only
           (table-context-p
            (string-match-p
             "\\b\\(FROM\\|JOIN\\|INTO\\|UPDATE\\|TABLE\\|DESCRIBE\\|DESC\\)\\s-+\\S-*\\'"
             (upcase line-before)))
           (busy (clutch-db-busy-p conn))
           (candidates
            (if (or table-context-p busy)
                (hash-table-keys schema)
              ;; Load columns only for tables in the current statement
              (let ((all (copy-sequence (hash-table-keys schema))))
                (dolist (tbl (clutch--tables-in-query schema))
                  (when-let* ((cols (clutch--ensure-columns
                                     conn schema tbl)))
                    (setq all (nconc all (copy-sequence cols)))))
                (delete-dups all)))))
      (list beg end candidates
            :exclusive 'no
            :exit-function (lambda (str status)
                             (when (and (clutch--completion-finished-status-p status)
                                        (member (upcase str) clutch--sql-keywords)
                                        (not (looking-at-p "\\s-")))
                               (insert " ")))))))

(defun clutch--eldoc-schema-string (conn schema sym)
  "Return an eldoc string for SYM via SCHEMA on CONN, or nil.
Matches SYM as a table name first, then as a column in any visible table."
  (cond
   ((not (eq (gethash sym schema 'missing) 'missing))
    (let* ((cols    (clutch--ensure-columns conn schema sym))
           (n       (length cols))
           (comment (clutch--ensure-table-comment conn sym)))
      (concat (propertize (format "[%s] " (clutch-db-database conn)) 'face 'shadow)
              (propertize sym 'face 'font-lock-type-face)
              (propertize (format "  (%d col%s)" n (if (= n 1) "" "s")) 'face 'shadow)
              (when comment
                (propertize (format "  — %s" comment) 'face 'shadow)))))
   (t
    (cl-loop for tbl in (clutch--tables-in-buffer schema)
             for cols = (clutch--ensure-columns conn schema tbl)
             when (and cols (member sym cols))
             return (clutch--eldoc-column-string conn tbl sym)))))

(defun clutch--eldoc-function (&rest _)
  "Eldoc backend for `clutch-mode'.
Returns a documentation string for the SQL identifier at point.
Schema-based info (tables, columns) requires an active connection.
SQL keyword/function docs are shown even without a connection."
  (when-let* ((sym (thing-at-point 'symbol t)))
    (or
     (when-let* ((schema (clutch--schema-for-connection))
                 (conn   clutch-connection)
                 ((not (clutch-db-busy-p conn))))
       (clutch--eldoc-schema-string conn schema sym))
     (when-let* ((conn clutch-connection)
                 ((clutch--connection-alive-p conn))
                 ((not (clutch-db-busy-p conn)))
                 ((string= "MySQL" (clutch-db-display-name conn))))
       (clutch--ensure-help-doc conn sym))
     (clutch--eldoc-keyword-string sym))))

;;;; Schema browser

(defvar-local clutch-schema--expanded-tables nil
  "List of table names currently expanded in the schema browser.")

(defvar-local clutch-schema--tables nil
  "Cached list of table names for the schema browser.")

(defvar clutch-schema-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "g" #'clutch-schema-refresh)
    (define-key map (kbd "RET") #'clutch-schema-describe-at-point)
    (define-key map (kbd "TAB") #'clutch-schema-toggle-expand)
    (define-key map "E" #'clutch-schema-expand-all)
    (define-key map "C" #'clutch-schema-collapse-all)
    (define-key map "v" #'clutch-schema-browse-at-point)
    map)
  "Keymap for `clutch-schema-mode'.")

(define-derived-mode clutch-schema-mode special-mode "Clutch-Schema"
  "Mode for browsing database schema.

\\<clutch-schema-mode-map>
  \\[clutch-schema-toggle-expand]	Expand/collapse table columns
  \\[clutch-schema-expand-all]	Expand all tables
  \\[clutch-schema-collapse-all]	Collapse all tables
  \\[clutch-schema-describe-at-point]	Show DDL for table
  \\[clutch-schema-browse-at-point]	Browse table data (insert SELECT into console)
  \\[clutch-schema-refresh]	Refresh"
  (setq truncate-lines t)
  (setq-local revert-buffer-function #'clutch-schema--revert))

(defun clutch-schema--revert (_ignore-auto _noconfirm)
  "Revert function for schema browser."
  (clutch-schema-refresh))

(defun clutch-schema--table-at-point ()
  "Return the table name at the current line, or nil."
  (get-text-property (line-beginning-position) 'clutch-schema-table))

(defun clutch-schema--column-annotation (col)
  "Return the annotation suffix for column detail plist COL."
  (let ((pk (plist-get col :primary-key))
        (fk (plist-get col :foreign-key)))
    (concat (when pk " PK")
            (when fk
              (format " FK → %s.%s"
                      (plist-get fk :ref-table)
                      (plist-get fk :ref-column))))))

(defun clutch-schema--insert-columns (conn tbl)
  "Insert indented column detail lines for TBL using CONN."
  (condition-case nil
      (let ((details (clutch-db-column-details conn tbl)))
        (when details
          (let ((last-idx (1- (length details))))
            (cl-loop for col in details
                     for i from 0
                     for branch = (if (= i last-idx) "└── " "├── ")
                     do (insert
                         (propertize
                          (format "  %s%-20s %s%s\n"
                                  branch
                                  (plist-get col :name)
                                  (plist-get col :type)
                                  (clutch-schema--column-annotation col))
                          'clutch-schema-table tbl
                          'face 'font-lock-comment-face))))))
    (clutch-db-error nil)))

(defun clutch-schema--insert-table (conn tbl expanded-p)
  "Insert a table line for TBL.
If EXPANDED-P, also insert column detail lines using CONN."
  (let* ((arrow (if expanded-p "▾" "▸")))
    (insert (propertize (format "%s %s\n" arrow tbl)
                        'clutch-schema-table tbl))
    (when expanded-p
      (clutch-schema--insert-columns conn tbl))))

(defun clutch-schema--render ()
  "Render the schema browser content."
  (let* ((inhibit-read-only t)
         (conn clutch-connection)
         (tables clutch-schema--tables)
         (expanded clutch-schema--expanded-tables))
    (erase-buffer)
    (insert (propertize
             (format "-- Tables in %s (%d)\n\n"
                     (or (clutch-db-database conn) "?")
                     (length tables))
             'face 'font-lock-comment-face))
    (dolist (tbl tables)
      (clutch-schema--insert-table
       conn tbl (member tbl expanded)))))

(defun clutch-list-tables ()
  "Show a list of tables in the current database."
  (interactive)
  (clutch--ensure-connection)
  (let* ((conn clutch-connection)
         (tables (clutch-db-list-tables conn))
         (buf (get-buffer-create
               (format "*clutch: %s tables*"
                       (or (clutch-db-database conn) "?")))))
    (with-current-buffer buf
      (clutch-schema-mode)
      (setq-local clutch-connection conn
                  clutch-schema--tables tables)
      (clutch-schema--render)
      (goto-char (point-min)))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch-schema-toggle-expand ()
  "Toggle column expansion for the table at point."
  (interactive)
  (if-let* ((tbl (clutch-schema--table-at-point)))
      (let ((line (line-number-at-pos)))
        (if (member tbl clutch-schema--expanded-tables)
            (setq clutch-schema--expanded-tables
                  (delete tbl clutch-schema--expanded-tables))
          (push tbl clutch-schema--expanded-tables))
        (clutch-schema--render)
        (goto-char (point-min))
        (forward-line (1- line)))
    (user-error "No table at point")))

(defun clutch-schema-expand-all ()
  "Expand all tables in the schema browser."
  (interactive)
  (setq clutch-schema--expanded-tables
        (copy-sequence clutch-schema--tables))
  (let ((line (line-number-at-pos)))
    (clutch-schema--render)
    (goto-char (point-min))
    (forward-line (1- line))))

(defun clutch-schema-collapse-all ()
  "Collapse all tables in the schema browser."
  (interactive)
  (setq clutch-schema--expanded-tables nil)
  (let ((line (line-number-at-pos)))
    (clutch-schema--render)
    (goto-char (point-min))
    (forward-line (1- (min line (line-number-at-pos (point-max)))))))

(defun clutch-describe-table (table)
  "Show the DDL of TABLE using SHOW CREATE TABLE."
  (interactive
   (list (if-let* ((schema (clutch--schema-for-connection)))
             (completing-read "Table: " (hash-table-keys schema) nil t)
           (read-string "Table: "))))
  (clutch--ensure-connection)
  (let* ((conn clutch-connection)
         (product (or clutch--conn-sql-product clutch-sql-product))
         (ddl (clutch-db-show-create-table conn table))
         (buf (get-buffer-create (format "*clutch: %s*" table))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (sql-mode)
        (sql-set-product product)
        (setq-local clutch-connection conn)
        (erase-buffer)
        (insert ddl)
        (insert "\n")
        (font-lock-ensure)
        (setq buffer-read-only t)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch-describe-table-at-point ()
  "Describe the table name at point."
  (interactive)
  (if-let* ((table (thing-at-point 'symbol t)))
      (clutch-describe-table table)
    (call-interactively #'clutch-describe-table)))

(defun clutch-schema-describe-at-point ()
  "In schema browser, describe the table on the current line."
  (interactive)
  (if-let* ((tbl (clutch-schema--table-at-point)))
      (clutch-describe-table tbl)
    (user-error "No table at point")))

(defun clutch-schema-refresh ()
  "Refresh the schema browser and cache."
  (interactive)
  (clutch--ensure-connection)
  (clutch--refresh-schema-cache clutch-connection)
  (setq clutch-schema--tables
        (clutch-db-list-tables clutch-connection))
  (let ((line (line-number-at-pos)))
    (clutch-schema--render)
    (goto-char (point-min))
    (forward-line (1- line))))

(defun clutch--find-console-for-conn (conn)
  "Return the clutch-mode buffer that owns CONN, or nil."
  (cl-loop for buf in (buffer-list)
           when (and (eq (buffer-local-value 'major-mode buf) 'clutch-mode)
                     (eq (buffer-local-value 'clutch-connection buf) conn))
           return buf))

(defun clutch-schema-browse-at-point ()
  "Insert SELECT * FROM <table> into the query console and switch to it."
  (interactive)
  (if-let* ((tbl (clutch-schema--table-at-point)))
      (let* ((conn clutch-connection)
             (sql (format "SELECT * FROM %s"
                          (clutch-db-escape-identifier conn tbl)))
             (console (or (clutch--find-console-for-conn conn)
                          (user-error "No query console open for this connection"))))
        (pop-to-buffer console)
        (goto-char (point-max))
        (unless (bolp) (insert "\n"))
        (insert "\n" sql))
    (user-error "No table at point")))

(defun clutch-browse-table (table)
  "Insert SELECT * FROM TABLE at end of the current console buffer.
Prompts for TABLE via `completing-read' using schema cache table names."
  (interactive
   (list (let* ((schema (clutch--schema-for-connection))
                (tables (or (and schema (hash-table-keys schema))
                            (progn (clutch--ensure-connection)
                                   (clutch-db-list-tables clutch-connection)))))
           (completing-read "Browse table: " tables nil t))))
  (clutch--ensure-connection)
  (let ((sql (format "SELECT * FROM %s"
                     (clutch-db-escape-identifier clutch-connection table))))
    (goto-char (point-max))
    (unless (bolp) (insert "\n"))
    (insert "\n" sql)))

;;;; clutch-mode (SQL editing major mode)

(defvar clutch-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'clutch-execute-dwim)
    (define-key map (kbd "C-c C-r") #'clutch-execute-region)
    (define-key map (kbd "C-c C-b") #'clutch-execute-buffer)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-t") #'clutch-list-tables)
    (define-key map (kbd "C-c C-j") #'clutch-browse-table)
    (define-key map (kbd "C-c C-d") #'clutch-describe-table-at-point)
    (define-key map (kbd "C-c C-p") #'clutch-preview-execution-sql)
    (define-key map (kbd "C-c ?") #'clutch-dispatch)
    map)
  "Keymap for `clutch-mode'.")

;;;###autoload
(define-derived-mode clutch-mode sql-mode "Clutch"
  "Major mode for editing and executing SQL queries.

\\<clutch-mode-map>
Key bindings:
  \\[clutch-execute-query-at-point]	Execute query at point
  \\[clutch-execute-region]	Execute region
  \\[clutch-execute-buffer]	Execute buffer
  \\[clutch-connect]	Connect to server
  \\[clutch-list-tables]	List tables
  \\[clutch-describe-table-at-point]	Describe table at point
  \\[clutch-browse-table]	Browse table data
  \\[clutch-preview-execution-sql]	Preview SQL to execute"
  (set-buffer-file-coding-system 'utf-8-unix nil t)
  (add-hook 'kill-emacs-hook #'clutch--save-all-consoles)
  (add-hook 'kill-buffer-hook #'clutch--save-console nil t)
  (add-hook 'completion-at-point-functions
            #'clutch-completion-at-point nil t)
  (add-hook 'completion-at-point-functions
            #'clutch-sql-keyword-completion-at-point nil t)
  (add-hook 'eldoc-documentation-functions
            #'clutch--eldoc-function nil t)
  (clutch--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . clutch-mode))

;;;; Cell navigation

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
  (when (use-region-p)
    (setq deactivate-mark nil)))

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
  (when (use-region-p)
    (setq deactivate-mark nil)))

(defun clutch-result-down-cell ()
  "Move to the same column in the next row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx)))
    (clutch--goto-cell (1+ ridx) cidx))
  (when (use-region-p)
    (setq deactivate-mark nil)))

(defun clutch-result-up-cell ()
  "Move to the same column in the previous row."
  (interactive)
  (when-let* ((cidx (clutch--col-idx-at-point))
              (ridx (get-text-property (point) 'clutch-row-idx))
              ((> ridx 0)))
    (clutch--goto-cell (1- ridx) cidx))
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
    (define-key map "c" #'clutch-result-copy-command)
    (define-key map "v" #'clutch-result-view-json)
    (define-key map "W" #'clutch-result-apply-filter)
    (define-key map (kbd "RET") #'clutch-result-open-record)
    (define-key map "]" #'clutch-result-next-col-page)
    (define-key map "[" #'clutch-result-prev-col-page)
    (define-key map "=" #'clutch-result-widen-column)
    (define-key map "-" #'clutch-result-narrow-column)
    (define-key map (kbd "C-c p") #'clutch-result-pin-column)
    (define-key map (kbd "C-c P") #'clutch-result-unpin-column)
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
    map)
  "Keymap for `clutch-result-mode'.")

(defun clutch--format-elapsed (seconds)
  "Format SECONDS as a human-readable duration."
  (if (< seconds 1.0)
      (format "%dms" (round (* seconds 1000)))
    (format "%.3fs" seconds)))

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
  "Highlight the entire row under the cursor."
  (when clutch--row-overlay
    (delete-overlay clutch--row-overlay)
    (setq clutch--row-overlay nil))
  (when (get-text-property (point) 'clutch-row-idx)
    (let ((ov (make-overlay (line-beginning-position)
                            (line-end-position))))
      (overlay-put ov 'face 'hl-line)
      (overlay-put ov 'priority -1)
      (setq clutch--row-overlay ov))))

(defun clutch--update-header-highlight ()
  "Highlight the header cell for the column under the cursor.
Rebuilds `header-line-format' with the active column highlighted."
  (when clutch--column-widths
    (clutch--update-position-indicator)
    (clutch--update-row-highlight)
    (let ((cidx (clutch--col-idx-at-point)))
      (unless (eql cidx clutch--header-active-col)
        (setq clutch--header-active-col cidx)
        (let* ((visible-cols (clutch--visible-columns))
               (widths (clutch--effective-widths))
               (col-num-pages (length clutch--column-pages))
               (cur-page clutch--current-col-page)
               (has-prev (> cur-page 0))
               (has-next (< cur-page (1- col-num-pages)))
               (nw (clutch--row-number-digits)))
          (setq header-line-format
                (concat (propertize " " 'display '(space :align-to 0))
                        (clutch--build-header-line
                         visible-cols widths nw
                         has-prev has-next cidx))))))))

(define-derived-mode clutch-result-mode special-mode "Clutch-Result"
  "Mode for displaying database query results with SQL pagination.

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
  \\[clutch-result-next-col-page]	Next column page
  \\[clutch-result-prev-col-page]	Previous column page
Copy:
  \\[clutch-result-copy-command]	Copy... (tsv/csv/insert)
  \\[clutch-result-export]	Export all rows (copy/file)
  \\[clutch-preview-execution-sql]	Preview SQL to execute
Edit:
  \\[clutch-result-edit-cell]	Edit cell value
  \\[clutch-result-commit]	Commit edits as UPDATE
  \\[clutch-result-apply-filter]	Apply WHERE filter
  \\[clutch-result-sort-by-column]	Sort ascending (SQL ORDER BY)
  \\[clutch-result-sort-by-column-desc]	Sort descending (SQL ORDER BY)
  \\[clutch-result-widen-column]	Widen column
  \\[clutch-result-narrow-column]	Narrow column
  \\[clutch-result-pin-column]	Pin column
  \\[clutch-result-unpin-column]	Unpin column
  \\[clutch-result-rerun]	Re-execute the query"
  (setq truncate-lines t)
  (hl-line-mode 1)
  ;; Make tab-line use default background so footer renders cleanly
  (face-remap-add-relative 'tab-line :inherit 'default)
  (setq-local revert-buffer-function #'clutch-result--revert)
  (add-hook 'post-command-hook
            #'clutch--update-header-highlight nil t)
  (add-hook 'kill-buffer-hook #'clutch--result-buffer-cleanup nil t)
  (add-hook 'change-major-mode-hook #'clutch--result-buffer-cleanup nil t)
  (clutch--enable-window-size-hook))

(defun clutch-result-next-page ()
  "Go to the next data page."
  (interactive)
  (let ((rows-on-page (length clutch--result-rows)))
    (when (< rows-on-page clutch-result-max-rows)
      (user-error "Already on last page (fewer rows than page size)"))
    (clutch--execute-page (1+ clutch--page-current))))

(defun clutch-result-prev-page ()
  "Go to the previous data page."
  (interactive)
  (when (<= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch--execute-page (1- clutch--page-current)))

(defun clutch-result-first-page ()
  "Go to the first data page."
  (interactive)
  (when (= clutch--page-current 0)
    (user-error "Already on first page"))
  (clutch--execute-page 0))

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

(defun clutch--sql-find-top-level-clause (sql pattern &optional start)
  "Return the start position of PATTERN at parenthesis depth 0 in SQL.
PATTERN is matched case-insensitively with word boundaries.  START
defaults to 0.  Returns nil if not found."
  (clutch-db-sql-find-top-level-clause sql pattern start))

(defun clutch--sql-normalize-for-rewrite (sql)
  "Return SQL trimmed for rewrite operations."
  (string-trim-right
   (replace-regexp-in-string ";\\s-*\\'" ""
                             (clutch--strip-leading-comments sql))))

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
      ('where (format "SELECT * FROM (%s) AS _clutch_filter WHERE %s" trimmed arg))
      ('count (format "SELECT COUNT(*) FROM (%s) AS _clutch_count"
                      (clutch--sql-strip-top-level-tail trimmed)))
      (_ (error "Unsupported rewrite op: %s" op)))))

(defun clutch--sql-rewrite (sql op &optional arg)
  "Rewrite SQL for OP with optional ARG.
Current implementation uses top-level clause awareness and keeps a
fallback path; AST-based rewrite can replace this later."
  (condition-case nil
      (let ((normalized (clutch--sql-normalize-for-rewrite sql)))
        (pcase op
          ('where
           (format "SELECT * FROM (%s) AS _clutch_filter WHERE %s"
                   normalized arg))
          ('count
           (format "SELECT COUNT(*) FROM (%s) AS _clutch_count"
                   (clutch--sql-strip-top-level-tail normalized)))
          (_ (error "Unsupported rewrite op: %s" op))))
    (error
     (clutch--sql-rewrite-fallback sql op arg))))

(defun clutch--build-count-sql (sql)
  "Rewrite SQL as a COUNT(*) query.
Uses the rewrite layer so complex SQL is handled via derived-table count."
  (clutch--sql-rewrite sql 'count))

(defun clutch-result-count-total ()
  "Query the total row count for the current base query."
  (interactive)
  (let* ((conn clutch-connection)
         (base (or clutch--base-query clutch--last-query)))
    (unless (clutch--connection-alive-p conn)
      (user-error "Not connected"))
    (let* ((count-sql (clutch--build-count-sql base))
           (result (condition-case err
                       (clutch-db-query conn count-sql)
                     (clutch-db-error
                      (user-error "COUNT query error: %s"
                                  (error-message-string err)))))
           (count-val (caar (clutch-db-result-rows result))))
      (setq-local clutch--page-total-rows
                  (if (numberp count-val) count-val
                    (string-to-number (format "%s" count-val))))
      (clutch--refresh-display)
      (message "Total rows: %d" clutch--page-total-rows))))

(defun clutch-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql (or clutch--base-query clutch--last-query)))
      (clutch--execute sql clutch-connection)
    (user-error "No query to re-execute")))

(defun clutch-result--revert (_ignore-auto _noconfirm)
  "Revert function for result buffer — re-executes the query."
  (clutch-result-rerun))

;;;; Cell editing (C-c ')

(defun clutch-result--cell-at (pos)
  "Return (ROW-IDX COL-IDX FULL-VALUE) at buffer position POS, or nil."
  (when-let* ((ridx (get-text-property pos 'clutch-row-idx)))
    (list ridx
          (get-text-property pos 'clutch-col-idx)
          (get-text-property pos 'clutch-full-value))))

(defun clutch-result--cell-at-point ()
  "Return (ROW-IDX COL-IDX FULL-VALUE) for the cell at or near point.
If point is on a pipe separator or padding space, scans left then
right on the current line to find the nearest cell."
  (or (clutch-result--cell-at (point))
      (let ((bol (line-beginning-position))
            (eol (line-end-position)))
        (or (cl-loop for p downfrom (1- (point)) to bol
                     thereis (clutch-result--cell-at p))
            (cl-loop for p from (1+ (point)) to eol
                     thereis (clutch-result--cell-at p))))))

(defun clutch-result--row-idx-at-line ()
  "Return the row index for the current line, or nil.
Scans text properties across the line."
  (cl-loop for p from (line-beginning-position) to (line-end-position)
           thereis (get-text-property p 'clutch-row-idx)))

(defun clutch-result--rows-in-region (beg end)
  "Return sorted list of unique row indices in the region BEG..END."
  (save-excursion
    (goto-char beg)
    (sort (cl-loop while (< (point) end)
                   for ridx = (clutch-result--row-idx-at-line)
                   when ridx collect ridx into acc
                   do (forward-line 1)
                   finally return (cl-remove-duplicates acc))
          #'<)))

(defvar-local clutch-result--edit-callback nil
  "Callback for the cell edit buffer: (lambda (new-value) ...).")

(defvar-local clutch-result--edit-result-buffer nil
  "The result buffer to commit edits to after clutch-result-edit-finish.")

(defvar clutch-result-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'clutch-result-edit-finish)
    (define-key map (kbd "C-c C-k") #'clutch-result-edit-cancel)
    map)
  "Keymap for the cell edit buffer.")

(define-minor-mode clutch-result-edit-mode
  "Minor mode for editing a database cell value.
\\<clutch-result-edit-mode-map>
  \\[clutch-result-edit-finish]	Accept edit
  \\[clutch-result-edit-cancel]	Cancel"
  :lighter " DB-Edit"
  :keymap clutch-result-edit-mode-map)

(defun clutch-result-edit-cell ()
  "Edit the cell at point in a dedicated buffer (like `C-c \\='` in org)."
  (interactive)
  (pcase-let* ((`(,ridx ,cidx ,val) (or (clutch-result--cell-at-point)
                                         (user-error "No cell at point")))
               (col-name (nth cidx clutch--result-columns))
         (result-buf (current-buffer))
         (edit-buf (get-buffer-create
                    (format "*clutch-edit: [%d].%s*" ridx col-name))))
    (with-current-buffer edit-buf
      (erase-buffer)
      (insert (clutch--format-value val))
      (goto-char (point-min))
      (clutch-result-edit-mode 1)
      (setq-local header-line-format
                  (format " Editing row %d, column \"%s\"  |  C-c C-c: stage  C-c C-k: cancel"
                          ridx col-name))
      (setq-local clutch-result--edit-callback
                  (lambda (new-value)
                    (with-current-buffer result-buf
                      (clutch-result--apply-edit ridx cidx new-value))))
      (setq-local clutch-result--edit-result-buffer result-buf))
    (pop-to-buffer edit-buf)))

(defun clutch-result-edit-finish ()
  "Stage the edit and return to the result buffer.
Use C-c C-c in the result buffer to commit all staged edits."
  (interactive)
  (let ((new-value (string-trim-right (buffer-string)))
        (cb clutch-result--edit-callback))
    (quit-window 'kill)
    (when cb
      (funcall cb (if (string= new-value "NULL") nil new-value)))))

(defun clutch-result-edit-cancel ()
  "Cancel the edit and return to the result buffer."
  (interactive)
  (quit-window 'kill))

(defun clutch-result--apply-edit (ridx cidx new-value)
  "Record edit for row RIDX, column CIDX with NEW-VALUE and refresh display."
  (let ((key (cons ridx cidx))
        (original (nth cidx (nth ridx clutch--result-rows))))
    ;; If edited back to original, remove the pending edit
    (if (equal new-value original)
        (setq clutch--pending-edits
              (assoc-delete-all key clutch--pending-edits))
      (let ((existing (assoc key clutch--pending-edits)))
        (if existing
            (setcdr existing new-value)
          (push (cons key new-value) clutch--pending-edits)))))
  (clutch--refresh-display)
  (if clutch--pending-edits
      (message "%d pending edit%s — C-c C-c to commit"
               (length clutch--pending-edits)
               (if (= (length clutch--pending-edits) 1) "" "s"))
    (message "Edit reverted to original")))

;;;; Commit edits

(defun clutch-result--table-from-sql (sql)
  "Extract the first table name from the FROM clause of SQL.
Handles backtick, double-quote, and unquoted identifiers, with an
optional schema prefix (schema.table).  Returns a string or nil."
  (let ((case-fold-search t))
    (cond
     ;; backtick-quoted: FROM `schema`.`table`  or  FROM `table`
     ((string-match "\\bFROM\\s-+\\(?:`[^`]+`\\.\\)?`\\([^`]+\\)`" sql)
      (match-string 1 sql))
     ;; double-quoted: FROM "schema"."table"  or  FROM "table"
     ((string-match "\\bFROM\\s-+\\(?:\"[^\"]+\"\\.\\)?\"\\([^\"]+\\)\"" sql)
      (match-string 1 sql))
     ;; unquoted (including CJK): FROM schema.table  or  FROM table
     ((string-match "\\bFROM\\s-+\\(?:[^[:space:],();.]+\\.\\)?\\([^[:space:],();]+\\)" sql)
      (match-string 1 sql)))))

(defun clutch-result--detect-table ()
  "Try to detect the source table from the last query.
Returns table name string or nil."
  (when clutch--last-query
    (clutch-result--table-from-sql clutch--last-query)))

(defun clutch-result--detect-primary-key ()
  "Return a list of column indices that form the primary key, or nil."
  (when-let* ((conn clutch-connection)
              (table (clutch-result--detect-table)))
    (condition-case nil
        (let* ((pk-cols (clutch-db-primary-key-columns conn table))
               (col-names clutch--result-columns))
          (delq nil (mapcar (lambda (pk)
                              (cl-position pk col-names :test #'string=))
                            pk-cols)))
      (clutch-db-error nil))))

(defun clutch--load-fk-info ()
  "Load foreign key info for the current result's source table.
Populates `clutch--fk-info' with an alist mapping
column indices to their referenced table and column."
  (setq clutch--fk-info nil)
  (when-let* ((conn clutch-connection)
              (table (clutch-result--detect-table))
              (col-names clutch--result-columns))
    (condition-case nil
        (let ((fks (clutch-db-foreign-keys conn table)))
          (pcase-dolist (`(,col-name . ,ref-info) fks)
            (let ((idx (cl-position col-name col-names :test #'string=)))
              (when idx
                (push (cons idx ref-info) clutch--fk-info)))))
      (clutch-db-error nil))))

(defun clutch-result--group-edits-by-row (edits)
  "Group EDITS alist by row index into a hash-table.
Returns hash-table mapping ridx → list of (cidx . value)."
  (let ((ht (make-hash-table :test 'eql)))
    (pcase-dolist (`((,ridx . ,cidx) . ,val) edits)
      (push (cons cidx val) (gethash ridx ht)))
    ht))

(defun clutch--sql-has-top-level-where-p (sql)
  "Return non-nil if SQL has a top-level WHERE clause."
  (not (null (clutch--sql-find-top-level-clause
              (clutch--sql-normalize-for-rewrite sql) "WHERE"))))

(defun clutch-result--ensure-where-guard (statements op-name)
  "Ensure every statement in STATEMENTS has a top-level WHERE."
  (dolist (stmt statements)
    (unless (clutch--sql-has-top-level-where-p stmt)
      (user-error "%s blocked: statement without WHERE: %s"
                  op-name
                  (truncate-string-to-width (string-trim stmt) 120 nil nil "…")))))

(defun clutch-result--build-update-stmt (table row _ridx edits col-names pk-indices)
  "Build an UPDATE statement for TABLE.
ROW is the original row data at _RIDX, EDITS is a list of (cidx . value),
COL-NAMES are column names, PK-INDICES are primary key column indices."
  (let ((conn clutch-connection))
    (let ((set-parts
           (mapcar (lambda (e)
                     (format "%s = %s"
                             (clutch-db-escape-identifier
                              conn (nth (car e) col-names))
                             (clutch--value-to-literal (cdr e))))
                   edits))
          (where-parts
           (mapcar (lambda (pki)
                     (let ((v (nth pki row)))
                       (format "%s %s"
                               (clutch-db-escape-identifier
                                conn (nth pki col-names))
                               (if (null v) "IS NULL"
                                 (format "= %s"
                                         (clutch--value-to-literal v))))))
                   pk-indices)))
      (format "UPDATE %s SET %s WHERE %s"
              (clutch-db-escape-identifier conn table)
              (mapconcat #'identity set-parts ", ")
              (mapconcat #'identity where-parts " AND ")))))

(defun clutch-result--confirm-and-run-updates (statements)
  "Prompt for confirmation and execute UPDATE STATEMENTS.
Clear pending edits and re-run the last query if confirmed."
  (clutch-result--ensure-where-guard statements "UPDATE")
  (let ((sql-text (mapconcat (lambda (s) (concat s ";"))
                             (nreverse statements) "\n")))
    (when (yes-or-no-p
           (format "Execute %d UPDATE statement%s?\n\n%s\n\n"
                   (length statements)
                   (if (= (length statements) 1) "" "s")
                   sql-text))
      (dolist (stmt statements)
        (condition-case err
            (clutch-db-query clutch-connection stmt)
          (clutch-db-error
           (user-error "UPDATE failed: %s" (error-message-string err)))))
      (setq clutch--pending-edits nil)
      (message "%d row%s updated"
               (length statements)
               (if (= (length statements) 1) "" "s"))
      (clutch--execute clutch--last-query clutch-connection))))

(defun clutch-result--build-update-statements ()
  "Build UPDATE statements from `clutch--pending-edits'."
  (unless clutch--pending-edits
    (user-error "No pending edits"))
  (let* ((table (or (clutch-result--detect-table)
                    (user-error "Cannot detect source table (multi-table query?)")))
         (pk-indices (or (clutch-result--detect-primary-key)
                         (user-error "Cannot detect primary key for table %s" table)))
         (col-names clutch--result-columns)
         (rows clutch--result-rows)
         (by-row (clutch-result--group-edits-by-row clutch--pending-edits))
         statements)
    (maphash
     (lambda (ridx edits)
       (push (clutch-result--build-update-stmt
              table (nth ridx rows) ridx edits col-names pk-indices)
             statements))
     by-row)
    statements))

(defun clutch-result-commit ()
  "Generate and execute UPDATE statements for pending edits."
  (interactive)
  (let ((statements (clutch-result--build-update-statements)))
    (clutch-result--confirm-and-run-updates statements)))

;;;; Delete rows

(defun clutch-result--build-delete-stmt (table row col-names pk-indices)
  "Build a DELETE statement for TABLE.
ROW is the row data, COL-NAMES are column names,
PK-INDICES are primary key column indices."
  (let* ((conn clutch-connection)
         (where-parts
          (mapcar (lambda (pki)
                    (let ((v (nth pki row)))
                      (format "%s %s"
                              (clutch-db-escape-identifier
                               conn (nth pki col-names))
                              (if (null v) "IS NULL"
                                (format "= %s"
                                        (clutch--value-to-literal v))))))
                  pk-indices)))
    (format "DELETE FROM %s WHERE %s"
            (clutch-db-escape-identifier conn table)
            (mapconcat #'identity where-parts " AND "))))

(defun clutch-result--confirm-and-run-deletes (statements)
  "Prompt for confirmation, execute DELETE STATEMENTS, clear marks, refresh."
  (clutch-result--ensure-where-guard statements "DELETE")
  (let ((sql-text (mapconcat (lambda (s) (concat s ";")) statements "\n")))
    (when (yes-or-no-p
           (format "Execute %d DELETE statement%s?\n\n%s\n\n"
                   (length statements)
                   (if (= (length statements) 1) "" "s")
                   sql-text))
      (dolist (stmt statements)
        (condition-case err
            (clutch-db-query clutch-connection stmt)
          (clutch-db-error
           (user-error "DELETE failed: %s" (error-message-string err)))))
      (setq clutch--marked-rows nil)
      (message "%d row%s deleted"
               (length statements)
               (if (= (length statements) 1) "" "s"))
      (clutch--execute clutch--last-query clutch-connection))))

(defun clutch-result--build-delete-statements ()
  "Build DELETE statements from selected rows."
  (let* ((indices (or (clutch-result--selected-row-indices)
                      (user-error "No row at point")))
         (table (or (clutch-result--detect-table)
                    (user-error "Cannot detect source table")))
         (pk-indices (or (clutch-result--detect-primary-key)
                         (user-error "Cannot detect primary key for %s" table)))
         (col-names clutch--result-columns)
         (rows clutch--result-rows))
    (mapcar (lambda (ridx)
              (clutch-result--build-delete-stmt
               table (nth ridx rows) col-names pk-indices))
            indices)))

(defun clutch-result-delete-rows ()
  "Delete selected rows from the database.
Selection priority: region rows > current row.
Detects table and primary key, builds DELETE statements,
and prompts for confirmation before executing."
  (interactive)
  (let ((statements (clutch-result--build-delete-statements)))
    (clutch-result--confirm-and-run-deletes statements)))

;;;; Insert row

(defvar clutch-result-insert-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'clutch-result-insert-commit)
    (define-key map (kbd "C-c C-k") #'clutch-result-insert-cancel)
    map)
  "Keymap for the INSERT edit buffer.")

(define-minor-mode clutch-result-insert-mode
  "Minor mode for editing a new row to INSERT.
\\<clutch-result-insert-mode-map>
  \\[clutch-result-insert-commit]	Execute INSERT
  \\[clutch-result-insert-cancel]	Cancel"
  :lighter " DB-Insert"
  :keymap clutch-result-insert-mode-map)

(defvar-local clutch-result-insert--result-buffer nil
  "Reference to the parent result buffer (Insert buffer local).")

(defvar-local clutch-result-insert--table nil
  "Table name for the INSERT (Insert buffer local).")

(defun clutch-result-insert-row ()
  "Open an edit buffer to INSERT a new row into the current table."
  (interactive)
  (let* ((table (or (clutch-result--detect-table)
                    (user-error "Cannot detect source table")))
         (col-names clutch--result-columns)
         (result-buf (current-buffer))
         (buf (get-buffer-create (format "*clutch-insert: %s*" table))))
    (with-current-buffer buf
      (erase-buffer)
      (clutch-result-insert-mode 1)
      (setq-local clutch-result-insert--result-buffer result-buf
                  clutch-result-insert--table table
                  header-line-format
                  (format " INSERT into %s  |  C-c C-c: execute  C-c C-k: cancel"
                          table))
      (dolist (col col-names)
        (insert (propertize col 'face 'clutch-header-face)
                ": \n"))
      (goto-char (point-min))
      (end-of-line))
    (pop-to-buffer buf)))

(defun clutch-result-insert--parse-fields ()
  "Parse the insert buffer into an alist of (COLUMN . VALUE).
Skips columns with empty values."
  (let (fields)
    (save-excursion
      (goto-char (point-min))
      (while (not (eobp))
        (let ((line (buffer-substring-no-properties
                     (line-beginning-position) (line-end-position))))
          (when (string-match "\\`\\(.+?\\): \\(.*\\)\\'" line)
            (let ((col (match-string 1 line))
                  (val (match-string 2 line)))
              (unless (string-empty-p val)
                (push (cons col val) fields)))))
        (forward-line 1)))
    (nreverse fields)))

(defun clutch-result-insert--build-sql (conn table fields)
  "Build an INSERT SQL string for TABLE with FIELDS using CONN.
FIELDS is an alist of (column-name . value-string)."
  (let ((cols (mapconcat (lambda (f) (clutch-db-escape-identifier conn (car f)))
                         fields ", "))
        (vals (mapconcat (lambda (f)
                           (let ((v (cdr f)))
                             (if (string= (upcase v) "NULL") "NULL"
                               (clutch-db-escape-literal conn v))))
                         fields ", ")))
    (format "INSERT INTO %s (%s) VALUES (%s)"
            (clutch-db-escape-identifier conn table) cols vals)))

(defun clutch-result-insert-commit ()
  "Build and execute the INSERT statement from the edit buffer."
  (interactive)
  (let* ((fields     (clutch-result-insert--parse-fields))
         (table      clutch-result-insert--table)
         (result-buf clutch-result-insert--result-buffer))
    (unless fields      (user-error "No values entered"))
    (unless (buffer-live-p result-buf)
      (user-error "Result buffer no longer exists"))
    (let* ((conn (buffer-local-value 'clutch-connection result-buf))
           (sql  (clutch-result-insert--build-sql conn table fields)))
      (when (yes-or-no-p (format "Execute?\n\n%s\n\n" sql))
        (condition-case err
            (clutch-db-query conn sql)
          (clutch-db-error
           (user-error "INSERT failed: %s" (error-message-string err))))
        (quit-window 'kill)
        (when (buffer-live-p result-buf)
          (with-current-buffer result-buf
            (clutch--execute clutch--last-query conn)))
        (message "Row inserted into %s" table)))))

(defun clutch-result-insert-cancel ()
  "Cancel the INSERT and close the edit buffer."
  (interactive)
  (quit-window 'kill))

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

(defun clutch-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (clutch-result--sort (clutch-result--read-column) t))

;;;; WHERE filtering

(defun clutch--apply-where (sql filter)
  "Apply WHERE FILTER to SQL query string.
Wraps SQL as a derived table and applies FILTER in an outer WHERE.
This avoids brittle clause injection for CTE/UNION/subquery-heavy SQL."
  (clutch--sql-rewrite sql 'where filter))

(defun clutch-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
Prompts for a WHERE condition.  Enter empty string to clear."
  (interactive)
  (unless clutch--last-query
    (user-error "No query to filter"))
  (let* ((base (or clutch--base-query
                   clutch--last-query))
         (current clutch--where-filter)
         (input (string-trim
                 (read-string
                  (if current
                      (format "WHERE filter (current: %s, empty to clear): "
                              current)
                    "WHERE filter (e.g., age > 18): ")
                  nil nil current)))
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
      (clutch-refine-mode -1)
      (setq tab-line-format clutch--refine-saved-tab-line
            clutch--refine-rect nil
            clutch--refine-excluded-rows nil
            clutch--refine-excluded-cols nil
            clutch--refine-callback nil
            clutch--refine-saved-tab-line nil)
      (funcall cb final-rect))))

(defun clutch-refine-cancel ()
  "Cancel refine mode without executing the callback."
  (interactive)
  (clutch-refine-mode -1)
  (setq tab-line-format clutch--refine-saved-tab-line
        clutch--refine-rect nil
        clutch--refine-excluded-rows nil
        clutch--refine-excluded-cols nil
        clutch--refine-callback nil
        clutch--refine-saved-tab-line nil)
  (message "Refine cancelled"))

(defun clutch-result--start-refine (rect callback)
  "Enter refine mode for RECT with CALLBACK called with final rect on confirm.
RECT is (ROW-INDICES . COL-INDICES)."
  (deactivate-mark)
  (setq-local clutch--refine-rect rect
              clutch--refine-excluded-rows nil
              clutch--refine-excluded-cols nil
              clutch--refine-callback callback
              clutch--refine-saved-tab-line tab-line-format
              tab-line-format
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
FORMAT is one of symbols: `tsv', `csv', `insert'.
If region is active, copy rectangle bounds from region endpoints.
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
    (_
     (user-error "Unsupported copy format: %s" format))))

(defun clutch-result-copy-command (&optional refine)
  "Prompt for copy FORMAT and dispatch to `clutch-result-copy'.
With prefix arg REFINE and an active region, enter visual refine mode."
  (interactive "P")
  (let* ((choice (completing-read "Copy format: " '("tsv" "csv" "insert")
                                  nil t nil nil "tsv"))
         (fmt (pcase choice
                ("tsv" 'tsv)
                ("csv" 'csv)
                ("insert" 'insert)
                (_ 'tsv))))
    (if refine
        (progn
          (unless (use-region-p)
            (user-error "Set a region before using C-u"))
          (clutch-result--start-refine
           (clutch-result--region-rectangle-indices)
           (lambda (final-rect) (clutch-result-copy fmt final-rect))))
      (clutch-result-copy fmt))))


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
With region: use all selected columns.
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

(defun clutch--view-json-value (val)
  "Display VAL as formatted JSON in a pop-up buffer."
  (unless (and (stringp val) (not (string-empty-p val)))
    (user-error "No JSON value at point"))
  (let ((buf (get-buffer-create "*clutch-json*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert val)
        (condition-case nil (json-pretty-print-buffer) (error nil))
        (cond ((fboundp 'json-ts-mode) (json-ts-mode))
              ((fboundp 'json-mode)    (json-mode))
              (t                       (special-mode)))
        (goto-char (point-min))
        (setq buffer-read-only t)))
    (pop-to-buffer buf)))

(defun clutch-result-view-json ()
  "Display the JSON value of the cell at point in a formatted buffer."
  (interactive)
  (pcase-let ((`(,_ridx ,_cidx ,val) (or (clutch-result--cell-at-point)
                                          (user-error "No cell at point"))))
    (clutch--view-json-value val)))

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

(defun clutch-result--copy-rows-as-insert (&optional rect)
  "Copy row(s) as INSERT statement(s) to the kill ring.
Rows/columns: region rectangle > current cell."
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
         (table (or (clutch-result--detect-table) "TABLE"))
         (stmts (clutch-result--build-insert-statements indices col-indices table)))
    (kill-new (mapconcat #'identity stmts "\n"))
    (message "Copied %d INSERT statement%s (%d col%s)"
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
Rows/columns: region rectangle > current cell.
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
    (message "Copied %d row%s as CSV (%d col%s)"
             (length indices) (if (= (length indices) 1) "" "s")
             (length col-indices) (if (= (length col-indices) 1) "" "s"))))

(defun clutch-result--goto-col-idx (col-idx)
  "Move point to the first data cell matching COL-IDX in the buffer."
  (goto-char (point-min))
  (when-let* ((found (text-property-search-forward 'clutch-col-idx col-idx #'eq)))
    (goto-char (prop-match-beginning found))))

(defun clutch-result-goto-column ()
  "Jump to the column page containing a specific column."
  (interactive)
  (unless clutch--result-columns
    (user-error "No result columns"))
  (let* ((col-names clutch--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      (let ((target-page nil))
        (if (memq idx clutch--pinned-columns)
            (setq target-page clutch--current-col-page)
          (cl-loop for pi from 0 below (length clutch--column-pages)
                   when (cl-find idx (aref clutch--column-pages pi))
                   do (setq target-page pi)))
        (if (null target-page)
            (user-error "Column %s not found in pages" choice)
          (unless (= target-page clutch--current-col-page)
            (setq clutch--current-col-page target-page)
            (clutch--render-result))
          (clutch-result--goto-col-idx idx))))))

(defun clutch-result-export ()
  "Export the current result.
Prompts for format:
- copy: all rows to clipboard as CSV text
- file: all rows to CSV file."
  (interactive)
  (let ((fmt (completing-read "Export format: "
                              '("copy" "file")
                              nil t)))
    (pcase fmt
      ("copy" (clutch--export-csv-all-to-clipboard))
      ("file" (clutch--export-csv-all-file)))))

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

(defun clutch--export-csv-rows-to-file (rows)
  "Export ROWS as CSV to a file."
  (let* ((path (read-file-name "Export CSV to file: " nil nil nil "export.csv"))
         (coding (clutch--read-csv-export-coding-system))
         (coding-system-for-write coding))
    (with-temp-buffer
      (insert (clutch--csv-content rows))
      (write-region (point-min) (point-max) path nil 'silent))
    (message "Exported %d row%s to %s (%s)"
             (length rows) (if (= (length rows) 1) "" "s")
             path coding)))

(defun clutch--export-csv-rows-to-clipboard (rows)
  "Copy ROWS as CSV to the kill ring."
  (kill-new (clutch--csv-content rows))
  (message "Copied %d row%s as CSV"
           (length rows) (if (= (length rows) 1) "" "s")))

(defun clutch-result--effective-export-query ()
  "Return effective SQL used for exporting all rows."
  (let ((base (or clutch--base-query clutch--last-query)))
    (if (and base clutch--where-filter)
        (clutch--apply-where base clutch--where-filter)
      base)))

(defun clutch-result--collect-all-export-rows ()
  "Return all rows for current result by auto-paging when needed."
  (unless (clutch--connection-alive-p clutch-connection)
    (user-error "Not connected"))
  (let ((effective-sql (clutch-result--effective-export-query)))
    (cond
     ((null effective-sql)
      clutch--result-rows)
     ((or (null clutch--base-query)
          (clutch--sql-has-limit-p effective-sql))
      (clutch-db-result-rows
       (clutch-db-query clutch-connection effective-sql)))
     (t
      (let ((page-num 0)
            (page-size clutch-result-max-rows)
            (rows nil)
            done)
        (while (not done)
          (let* ((paged-sql (clutch--build-paged-sql
                             effective-sql page-num page-size clutch--order-by))
                 (result (clutch-db-query clutch-connection paged-sql))
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

;;;; Column page navigation and width adjustment

(defun clutch-result-next-col-page ()
  "Switch to the next column page."
  (interactive)
  (let ((max-page (1- (length clutch--column-pages))))
    (if (>= clutch--current-col-page max-page)
        (user-error "Already on last column page")
      (cl-incf clutch--current-col-page)
      (clutch--render-result))))

(defun clutch-result-prev-col-page ()
  "Switch to the previous column page."
  (interactive)
  (if (<= clutch--current-col-page 0)
      (user-error "Already on first column page")
    (cl-decf clutch--current-col-page)
    (clutch--render-result)))

(defun clutch-result-widen-column ()
  "Widen the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (cl-incf (aref clutch--column-widths cidx)
                 clutch-column-width-step)
        (clutch--refresh-display))
    (user-error "No column at point")))

(defun clutch-result-narrow-column ()
  "Narrow the column at point by `clutch-column-width-step'."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (let ((new-w (max 5 (- (aref clutch--column-widths cidx)
                              clutch-column-width-step))))
        (aset clutch--column-widths cidx new-w)
        (clutch--refresh-display))
    (user-error "No column at point")))

(defun clutch-result-pin-column ()
  "Pin the column at point so it appears on all column pages."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (cl-pushnew cidx clutch--pinned-columns)
        (clutch--refresh-display)
        (message "Pinned column %s" (nth cidx clutch--result-columns)))
    (user-error "No column at point")))

(defun clutch-result-unpin-column ()
  "Unpin the column at point."
  (interactive)
  (if-let* ((cidx (clutch--col-idx-at-point)))
      (progn
        (setq clutch--pinned-columns
              (delq cidx clutch--pinned-columns))
        (clutch--refresh-display)
        (message "Unpinned column %s" (nth cidx clutch--result-columns)))
    (user-error "No column at point")))

;;;; Fullscreen toggle

(defvar-local clutch--pre-fullscreen-config nil
  "Window configuration saved before entering fullscreen.")

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
    (define-key map "v" #'clutch-record-view-json)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'clutch-record-refresh)
    (define-key map (kbd "C-c ?") #'clutch-record-dispatch)
    map)
  "Keymap for `clutch-record-mode'.")

(define-derived-mode clutch-record-mode special-mode "Clutch-Record"
  "Mode for displaying a single database row in detail.

\\<clutch-record-mode-map>
  \\[clutch-record-toggle-expand]	Expand/collapse field or follow FK
  \\[clutch-record-next-row]	Next row
  \\[clutch-record-prev-row]	Previous row
  \\[clutch-record-refresh]	Refresh"
  (setq truncate-lines nil))

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

(defun clutch-record--render-field (name cidx val col-def ridx edits fk-info
                                        expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, RIDX the row index.
EDITS, FK-INFO, EXPANDED-FIELDS provide edit/FK/expand state.
MAX-NAME-W is the label column width."
  (let* ((edited (assoc (cons ridx cidx) edits))
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
         (fk-info (buffer-local-value 'clutch--fk-info result-buf))
         (edits (buffer-local-value 'clutch--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (user-error "Row %d no longer exists" ridx))
    (erase-buffer)
    (setq header-line-format
          (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                      'face 'clutch-header-face))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for cidx from 0
               do (clutch-record--render-field
                   name cidx (nth cidx row) (nth cidx col-defs)
                   ridx edits fk-info clutch-record--expanded-fields
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

(defun clutch-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= clutch-record--row-idx 0)
      (user-error "Already at first row")
    (cl-decf clutch-record--row-idx)
    (setq clutch-record--expanded-fields nil)
    (clutch-record--render)))

(defun clutch-record-view-json ()
  "Display the JSON field value at point in a formatted buffer."
  (interactive)
  (clutch--view-json-value
   (or (get-text-property (point) 'clutch-full-value)
       (user-error "No field at point"))))

(defun clutch-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (clutch-record--render))

;;;; REPL mode

(defvar clutch-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'clutch-connect)
    (define-key map (kbd "C-c C-t") #'clutch-list-tables)
    (define-key map (kbd "C-c C-d") #'clutch-describe-table-at-point)
    map)
  "Keymap for `clutch-repl-mode'.")

(defvar-local clutch-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")

(define-derived-mode clutch-repl-mode comint-mode "Clutch-REPL"
  "Major mode for database REPL.

\\<clutch-repl-mode-map>
  \\[clutch-connect]	Connect to server
  \\[clutch-list-tables]	List tables
  \\[clutch-describe-table-at-point]	Describe table at point"
  (setq comint-prompt-regexp "^db> \\|^    -> ")
  (setq comint-input-sender #'clutch-repl--input-sender)
  (add-hook 'completion-at-point-functions
            #'clutch-completion-at-point nil t)
  (add-hook 'completion-at-point-functions
            #'clutch-sql-keyword-completion-at-point nil t))

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
  (if (not (clutch--connection-alive-p clutch-connection))
      (clutch-repl--output "ERROR: Not connected.  Use C-c C-e to connect.\ndb> ")
    (setq clutch--last-query sql)
    (condition-case err
        (let* ((start (float-time))
               (result (clutch-db-query clutch-connection sql))
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
            (clutch-repl--output (clutch-repl--format-dml-result result elapsed))))
      (clutch-db-error
       (clutch-repl--output
        (format "\nERROR: %s\n\ndb> " (error-message-string err)))))))

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

;;;###autoload
(transient-define-prefix clutch-dispatch ()
  "Main dispatch menu for clutch."
  [["Connection"
    ("c" "Connect"    clutch-connect)
    ("d" "Disconnect" clutch-disconnect)
    ("R" "REPL"       clutch-repl)]
   ["Execute"
    ("x" "Query at point" clutch-execute-query-at-point)
    ("r" "Region"         clutch-execute-region)
    ("b" "Buffer"         clutch-execute-buffer)
   ("p" "Preview SQL"    clutch-preview-execution-sql)]
   ["Edit"
    ("'" "Indirect edit"  clutch-edit-indirect)]
   ["Schema"
    ("t" "List tables"    clutch-list-tables)
    ("D" "Describe table" clutch-describe-table-at-point)]])

(transient-define-prefix clutch-result-dispatch ()
  "Dispatch menu for clutch result buffer."
  [["Navigate"
    ("TAB" "Next cell"     clutch-result-next-cell)
    ("<backtab>" "Prev cell" clutch-result-prev-cell)
    ("n" "Down row"        clutch-result-down-cell)
    ("p" "Up row"          clutch-result-up-cell)
    ("RET" "Open record"   clutch-result-open-record)
    ("C" "Go to column"    clutch-result-goto-column)]
   ["Pages"
    ("N" "Next page"       clutch-result-next-page)
    ("P" "Prev page"       clutch-result-prev-page)
    ("M-<" "First page"    clutch-result-first-page)
    ("M->" "Last page"     clutch-result-last-page)
    ("#" "Count total"     clutch-result-count-total)
    ("]" "Next col page"   clutch-result-next-col-page)
    ("[" "Prev col page"   clutch-result-prev-col-page)]
   ["Filter / Sort"
    ("/" "Filter rows"     clutch-result-filter)
    ("W" "WHERE filter"    clutch-result-apply-filter)
    ("A" "Aggregate"       clutch-result-aggregate)
    ("s" "Sort ASC"        clutch-result-sort-by-column)
    ("S" "Sort DESC"       clutch-result-sort-by-column-desc)]]
  [["Edit"
    ("C-c '" "Edit cell"   clutch-result-edit-cell)
    ("C-c C-c" "Commit"    clutch-result-commit)
    ("i" "Insert row"      clutch-result-insert-row)
    ("d" "Delete row(s)"   clutch-result-delete-rows)]
   ["Copy (region/rect: C-x SPC)"
    ("c" "Copy..."          clutch-result-copy-command)
    ("e" "Export"           clutch-result-export)]
   ["Other"
    ("=" "Widen column"    clutch-result-widen-column)
   ("-" "Narrow column"   clutch-result-narrow-column)
   ("C-c p" "Pin column"  clutch-result-pin-column)
   ("C-c P" "Unpin column" clutch-result-unpin-column)
   ("g" "Re-execute"      clutch-result-rerun)
    ("x" "Preview SQL"     clutch-preview-execution-sql)
    ("f" "Fullscreen"      clutch-result-fullscreen-toggle)]])

(transient-define-prefix clutch-record-dispatch ()
  "Dispatch menu for clutch record buffer."
  [["Navigate"
    ("n" "Next row"     clutch-record-next-row)
    ("p" "Prev row"     clutch-record-prev-row)
    ("RET" "Expand/FK"  clutch-record-toggle-expand)]
   ["Other"
    ("v" "View JSON" clutch-record-view-json)
    ("g" "Refresh" clutch-record-refresh)
    ("q" "Quit"    quit-window)]])

(provide 'clutch)
;;; clutch.el ends here
