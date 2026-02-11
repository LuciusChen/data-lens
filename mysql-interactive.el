;;; mysql-interactive.el --- Interactive MySQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; This file is part of mysql.el.

;; mysql.el is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; mysql.el is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with mysql.el.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Interactive SQL client built on mysql.el.
;;
;; Provides:
;; - `mysql-mode': SQL editing major mode (derived from `sql-mode')
;; - `mysql-repl': REPL via `comint-mode'
;; - Query execution with org-table formatted results
;; - Schema browsing and completion
;;
;; Entry points:
;;   M-x mysql-mode      — open a SQL editing buffer
;;   M-x mysql-repl      — open a REPL
;;   Open a .mysql file   — activates mysql-mode automatically

;;; Code:

(require 'mysql)
(require 'sql)
(require 'comint)
(require 'org-table)
(require 'cl-lib)
(require 'ring)

;;;; Customization

(defgroup mysql-interactive nil
  "Interactive MySQL client."
  :group 'mysql
  :prefix "mysql-interactive-")

(defface mysql-header-face
  '((t :inherit bold))
  "Face for column headers in result tables."
  :group 'mysql-interactive)

(defface mysql-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'mysql-interactive)

(defface mysql-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'mysql-interactive)

(defface mysql-modified-face
  '((t :inherit warning))
  "Face for modified cell values."
  :group 'mysql-interactive)

(defcustom mysql-connection-alist nil
  "Alist of saved MySQL connections.
Each entry has the form:
  (NAME . (:host H :port P :user U :password P :database D))
NAME is a string used for `completing-read'."
  :type '(alist :key-type string
                :value-type (plist :options
                                   ((:host string)
                                    (:port integer)
                                    (:user string)
                                    (:password string)
                                    (:database string))))
  :group 'mysql-interactive)

(defcustom mysql-interactive-history-file
  (expand-file-name "mysql-history" user-emacs-directory)
  "File for persisting SQL query history."
  :type 'file
  :group 'mysql-interactive)

(defcustom mysql-interactive-history-length 500
  "Maximum number of history entries to keep."
  :type 'integer
  :group 'mysql-interactive)

(defcustom mysql-interactive-result-max-rows 1000
  "Maximum number of rows to display in result tables."
  :type 'integer
  :group 'mysql-interactive)

(defcustom mysql-interactive-result-max-column-width 50
  "Maximum column width before truncation."
  :type 'integer
  :group 'mysql-interactive)

;;;; Buffer-local variables

(defvar-local mysql-interactive-connection nil
  "Current `mysql-conn' for this buffer.")

(defvar-local mysql-interactive--last-query nil
  "Last executed SQL query string.")

(defvar-local mysql-interactive--result-columns nil
  "Column names from the last result, as a list of strings.")

(defvar-local mysql-interactive--result-rows nil
  "Row data from the last result, as a list of lists.")

(defvar-local mysql-interactive--result-elapsed nil
  "Elapsed time of the last query in seconds.")

(defvar-local mysql-interactive--vertical-view nil
  "Non-nil if the result buffer is in vertical (\\\\G) view.")

(defvar-local mysql-interactive--display-offset 0
  "Number of rows currently displayed (for load-more paging).")

(defvar-local mysql-interactive--result-column-defs nil
  "Full column definition plists from the last result.")

(defvar-local mysql-interactive--pending-edits nil
  "Alist of pending edits: ((ROW-IDX . COL-IDX) . NEW-VALUE).")

;;;; History

(defvar mysql-interactive--history (make-ring 500)
  "Ring buffer of executed SQL queries.")

(defvar mysql-interactive--history-loaded nil
  "Non-nil if history has been loaded from disk.")

(defun mysql-interactive--load-history ()
  "Load history from `mysql-interactive-history-file'."
  (unless mysql-interactive--history-loaded
    (setq mysql-interactive--history (make-ring mysql-interactive-history-length))
    (when (file-readable-p mysql-interactive-history-file)
      (let ((entries (split-string
                      (with-temp-buffer
                        (insert-file-contents mysql-interactive-history-file)
                        (buffer-string))
                      "\0" t)))
        (dolist (entry (nreverse entries))
          (ring-insert mysql-interactive--history entry))))
    (setq mysql-interactive--history-loaded t)))

(defun mysql-interactive--save-history ()
  "Save history to `mysql-interactive-history-file'."
  (let ((entries nil)
        (len (ring-length mysql-interactive--history)))
    (dotimes (i (min len mysql-interactive-history-length))
      (push (ring-ref mysql-interactive--history i) entries))
    (with-temp-file mysql-interactive-history-file
      (insert (mapconcat #'identity entries "\0")))))

(defun mysql-interactive--add-history (sql)
  "Add SQL to history ring, avoiding duplicates at head."
  (mysql-interactive--load-history)
  (let ((trimmed (string-trim sql)))
    (unless (string-empty-p trimmed)
      (when (or (ring-empty-p mysql-interactive--history)
                (not (string= trimmed (ring-ref mysql-interactive--history 0))))
        (ring-insert mysql-interactive--history trimmed))
      (mysql-interactive--save-history))))

(defun mysql-interactive-show-history ()
  "Select a query from history and insert it at point."
  (interactive)
  (mysql-interactive--load-history)
  (when (ring-empty-p mysql-interactive--history)
    (user-error "No history entries"))
  (let* ((entries (ring-elements mysql-interactive--history))
         (choice (completing-read "SQL history: " entries nil t)))
    (insert choice)))

;;;; Connection management

(defun mysql-interactive--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (format "%s@%s:%s/%s"
          (or (mysql-conn-user conn) "?")
          (or (mysql-conn-host conn) "?")
          (or (mysql-conn-port conn) 3306)
          (or (mysql-conn-database conn) "")))

(defun mysql-interactive--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn
       (mysql-conn-p conn)
       (process-live-p (mysql-conn-process conn))))

(defun mysql-interactive--ensure-connection ()
  "Ensure current buffer has a live connection.  Signal error if not."
  (unless (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (user-error "Not connected.  Use C-c C-e to connect")))

(defun mysql-interactive--update-mode-line ()
  "Update mode-line lighter with connection status."
  (setq mode-name
        (if (mysql-interactive--connection-alive-p mysql-interactive-connection)
            (format "MySQL[%s]"
                    (mysql-interactive--connection-key
                     mysql-interactive-connection))
          "MySQL[disconnected]"))
  (force-mode-line-update))

(defun mysql-connect-interactive ()
  "Connect to a MySQL server interactively.
If `mysql-connection-alist' is non-empty, offer saved connections via
`completing-read'.  Otherwise prompt for each parameter."
  (interactive)
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (mysql-disconnect mysql-interactive-connection)
    (setq mysql-interactive-connection nil))
  (let ((conn-params
         (if mysql-connection-alist
             (let* ((name (completing-read "Connection: "
                                           (mapcar #'car mysql-connection-alist)
                                           nil t))
                    (entry (cdr (assoc name mysql-connection-alist))))
               entry)
           (list :host (read-string "Host (127.0.0.1): " nil nil "127.0.0.1")
                 :port (read-number "Port (3306): " 3306)
                 :user (read-string "User: ")
                 :password (read-passwd "Password: ")
                 :database (let ((db (read-string "Database (optional): ")))
                             (unless (string-empty-p db) db))))))
    (condition-case err
        (let ((conn (apply #'mysql-connect conn-params)))
          (mysql-query conn "SET NAMES utf8mb4")
          (setq mysql-interactive-connection conn)
          (mysql-interactive--update-mode-line)
          (mysql-interactive--refresh-schema-cache conn)
          (message "Connected to %s" (mysql-interactive--connection-key conn)))
      (mysql-error
       (user-error "Connection failed: %s" (error-message-string err))))))

(defun mysql-interactive-disconnect ()
  "Disconnect from the current MySQL server."
  (interactive)
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (mysql-disconnect mysql-interactive-connection)
    (message "Disconnected"))
  (setq mysql-interactive-connection nil)
  (mysql-interactive--update-mode-line))

;;;; Value formatting

(defun mysql-interactive--format-value (val)
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

(defun mysql-interactive--truncate-cell (str max-width)
  "Truncate STR to MAX-WIDTH, replacing embedded pipes to protect org tables."
  (let ((clean (replace-regexp-in-string "|" "¦" (replace-regexp-in-string "\n" "↵" str))))
    (if (> (length clean) max-width)
        (concat (substring clean 0 (- max-width 1)) "…")
      clean)))

;;;; Result display

(defun mysql-interactive--result-buffer-name ()
  "Return the result buffer name based on current connection."
  (if (mysql-interactive--connection-alive-p mysql-interactive-connection)
      (format "*mysql: %s*"
              (or (mysql-conn-database mysql-interactive-connection) "results"))
    "*mysql: results*"))

(defun mysql-interactive--insert-org-table (col-names rows &optional row-offset)
  "Insert an org-table with COL-NAMES and ROWS.
ROW-OFFSET is the starting row index for text properties (default 0).
Each cell carries text properties `mysql-row-idx', `mysql-col-idx',
and `mysql-full-value' for edit support.
After `org-table-align', extracts the header row into
`header-line-format' so it stays fixed while scrolling."
  (let ((max-w mysql-interactive-result-max-column-width)
        (ridx (or row-offset 0))
        (table-start (point)))
    ;; header
    (insert "| "
            (mapconcat #'identity col-names " | ")
            " |\n")
    (insert "|-\n")
    ;; rows
    (dolist (row rows)
      (insert "| ")
      (cl-loop for val in row
               for cidx from 0
               for formatted = (mysql-interactive--format-value val)
               for display = (mysql-interactive--truncate-cell formatted max-w)
               for edited = (assoc (cons ridx cidx)
                                   mysql-interactive--pending-edits)
               do (let ((cell-text (if edited
                                       (mysql-interactive--truncate-cell
                                        (mysql-interactive--format-value (cdr edited))
                                        max-w)
                                     display)))
                    (insert (propertize
                             cell-text
                             'mysql-row-idx ridx
                             'mysql-col-idx cidx
                             'mysql-full-value (if edited (cdr edited) val)
                             'face (cond (edited 'mysql-modified-face)
                                         ((null val) 'mysql-null-face)
                                         (t nil)))))
               do (insert " | "))
      ;; fix trailing: replace last " | " with " |"
      (delete-char -2)
      (insert "|\n")
      (cl-incf ridx))
    (org-table-align)
    ;; Extract aligned header into header-line-format
    (goto-char table-start)
    (let* ((header-text (buffer-substring (line-beginning-position)
                                          (line-end-position)))
           (header-display (propertize header-text 'face 'mysql-header-face)))
      ;; Delete header row and separator from buffer
      (delete-region (line-beginning-position)
                     (progn (forward-line 2) (point)))
      (setq header-line-format (concat " " header-display)))))

(defun mysql-interactive--insert-vertical-view (col-names rows)
  "Insert results in vertical card format (like mysql \\\\G).
Each row is displayed as a block of column: value pairs."
  (let* ((max-col-width (apply #'max (mapcar #'string-width col-names)))
         (row-count 0))
    (catch 'truncated
      (dolist (row rows)
        (when (>= row-count mysql-interactive-result-max-rows)
          (insert (format "... truncated at %d rows\n"
                          mysql-interactive-result-max-rows))
          (throw 'truncated nil))
        (insert (propertize (format "*** row %d ***\n" (1+ row-count))
                            'face 'font-lock-comment-face))
        (cl-loop for name in col-names
                 for val in row
                 for formatted = (mysql-interactive--format-value val)
                 do (insert (propertize
                             (format "%s" (string-pad name max-col-width))
                             'face 'mysql-header-face)
                            (propertize ": " 'face 'mysql-border-face)
                            (if (string= formatted "NULL")
                                (propertize formatted 'face 'mysql-null-face)
                              formatted)
                            "\n"))
        (insert "\n")
        (cl-incf row-count)))))

(defun mysql-interactive--display-result (result sql elapsed)
  "Display RESULT in the result buffer.
SQL is the query text, ELAPSED the time in seconds."
  (let* ((buf-name (mysql-interactive--result-buffer-name))
         (buf (get-buffer-create buf-name))
         (columns (mysql-result-columns result))
         (rows (mysql-result-rows result))
         (col-names (mapcar (lambda (c) (plist-get c :name)) columns)))
    (with-current-buffer buf
      (mysql-result-mode)
      (setq-local mysql-interactive--last-query sql)
      (setq-local mysql-interactive-connection
                  (mysql-result-connection result))
      (setq-local mysql-interactive--result-columns col-names)
      (setq-local mysql-interactive--result-column-defs columns)
      (setq-local mysql-interactive--result-rows rows)
      (setq-local mysql-interactive--result-elapsed elapsed)
      (setq-local mysql-interactive--vertical-view nil)
      (setq-local mysql-interactive--display-offset 0)
      (setq-local mysql-interactive--pending-edits nil)
      (let ((inhibit-read-only t)
            (page-size mysql-interactive-result-max-rows)
            (total (length rows)))
        (erase-buffer)
        (if col-names
            ;; SELECT-like result with columns
            (let ((page (seq-take rows page-size)))
              (mysql-interactive--insert-org-table col-names page)
              (setq-local mysql-interactive--display-offset (length page))
              (when (> total page-size)
                (insert (propertize
                         (format "\n-- Showing %d/%d rows  [n: load more]\n"
                                 (length page) total)
                         'face 'font-lock-comment-face))))
          ;; DML result (INSERT/UPDATE/DELETE)
          (insert (propertize (format "-- %s\n" (string-trim sql))
                              'face 'font-lock-comment-face))
          (insert (format "Affected rows: %s\n"
                          (or (mysql-result-affected-rows result) 0)))
          (when-let* ((id (mysql-result-last-insert-id result)))
            (when (> id 0)
              (insert (format "Last insert ID: %s\n" id))))
          (when-let* ((w (mysql-result-warnings result)))
            (when (> w 0)
              (insert (format "Warnings: %s\n" w))))
          (insert (propertize (format "\nCompleted in %.3fs\n" elapsed)
                              'face 'font-lock-comment-face))))
      (goto-char (point-min)))
    (display-buffer buf '(display-buffer-at-bottom))))

;;;; Query execution engine

(defun mysql-interactive--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Records history, times execution, and displays results."
  (let ((connection (or conn mysql-interactive-connection)))
    (unless (mysql-interactive--connection-alive-p connection)
      (user-error "Not connected.  Use C-c C-e to connect"))
    (setq mysql-interactive--last-query sql)
    (mysql-interactive--add-history sql)
    (let* ((start (float-time))
           (result (condition-case err
                       (mysql-query connection sql)
                     (mysql-error
                      (user-error "Query error: %s" (error-message-string err)))))
           (elapsed (- (float-time) start)))
      (mysql-interactive--display-result result sql elapsed)
      result)))

;;;; Query-at-point detection

(defun mysql-interactive--query-at-point ()
  "Return the SQL query around point.
Queries are delimited by semicolons or blank lines."
  (save-excursion
    (let (beg end)
      ;; Find beginning: search backward for ; or blank line or BOB
      (save-excursion
        (if (re-search-backward "\\(;\\|^[[:space:]]*$\\)" nil t)
            (setq beg (match-end 0))
          (setq beg (point-min))))
      ;; Find end: search forward for ; or blank line or EOB
      (save-excursion
        (if (re-search-forward "\\(;\\|^[[:space:]]*$\\)" nil t)
            (setq end (match-beginning 0))
          (setq end (point-max))))
      (let ((query (string-trim (buffer-substring-no-properties beg end))))
        (when (string-empty-p query)
          (user-error "No query at point"))
        query))))

;;;; Interactive commands

(defun mysql-execute-query-at-point ()
  "Execute the SQL query at point."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute (mysql-interactive--query-at-point)))

(defun mysql-execute-region (beg end)
  "Execute SQL in the region from BEG to END."
  (interactive "r")
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute
   (string-trim (buffer-substring-no-properties beg end))))

(defun mysql-execute-buffer ()
  "Execute the entire buffer as a SQL query."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--execute
   (string-trim (buffer-substring-no-properties (point-min) (point-max)))))

;;;; Schema cache + completion

(defvar mysql-interactive--schema-cache (make-hash-table :test 'equal)
  "Global schema cache.  Keys are connection-key strings.
Values are hash-tables mapping table-name → list of column-name strings.")

(defun mysql-interactive--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (condition-case nil
      (let* ((key (mysql-interactive--connection-key conn))
             (table-result (mysql-query conn "SHOW TABLES"))
             (table-names (mapcar #'car (mysql-result-rows table-result)))
             (schema (make-hash-table :test 'equal)))
        (dolist (tbl table-names)
          (puthash tbl nil schema))
        (puthash key schema mysql-interactive--schema-cache)
        (message "Connected — %d tables" (hash-table-count schema)))
    (mysql-error nil)))

(defun mysql-interactive--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA.
Fetches via SHOW COLUMNS if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing)))
    (if (not (eq cols 'missing))
        (or cols
            ;; nil means not yet loaded
            (condition-case nil
                (let* ((result (mysql-query
                                conn
                                (format "SHOW COLUMNS FROM %s"
                                        (mysql-escape-identifier table))))
                       (col-names (mapcar #'car (mysql-result-rows result))))
                  (puthash table col-names schema)
                  col-names)
              (mysql-error nil)))
      nil)))

(defun mysql-interactive--schema-for-connection ()
  "Return the schema hash-table for the current connection, or nil."
  (when (mysql-interactive--connection-alive-p mysql-interactive-connection)
    (gethash (mysql-interactive--connection-key mysql-interactive-connection)
             mysql-interactive--schema-cache)))

(defun mysql-interactive-completion-at-point ()
  "Completion-at-point function for SQL identifiers."
  (when-let* ((schema (mysql-interactive--schema-for-connection))
              (conn mysql-interactive-connection)
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
           (candidates
            (if table-context-p
                (hash-table-keys schema)
              ;; Combine table names and lazily-loaded column names
              (let ((all (copy-sequence (hash-table-keys schema))))
                (dolist (tbl (hash-table-keys schema))
                  (when-let* ((cols (mysql-interactive--ensure-columns
                                     conn schema tbl)))
                    (setq all (nconc all (copy-sequence cols)))))
                (delete-dups all)))))
      (list beg end candidates :exclusive 'no))))

;;;; Schema browser

(defvar mysql-schema-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "g" #'mysql-schema-refresh)
    (define-key map (kbd "RET") #'mysql-schema-describe-at-point)
    map)
  "Keymap for `mysql-schema-mode'.")

(define-derived-mode mysql-schema-mode special-mode "MySQL-Schema"
  "Mode for browsing MySQL schema."
  (setq truncate-lines t)
  (setq-local revert-buffer-function #'mysql-schema--revert))

(defun mysql-schema--revert (_ignore-auto _noconfirm)
  "Revert function for schema browser."
  (mysql-schema-refresh))

(defun mysql-list-tables ()
  "Show a list of tables in the current database."
  (interactive)
  (mysql-interactive--ensure-connection)
  (let* ((conn mysql-interactive-connection)
         (result (mysql-query conn "SHOW TABLES"))
         (tables (mapcar #'car (mysql-result-rows result)))
         (buf (get-buffer-create
               (format "*mysql: %s tables*"
                       (or (mysql-conn-database conn) "?")))))
    (with-current-buffer buf
      (mysql-schema-mode)
      (setq-local mysql-interactive-connection conn)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize
                 (format "-- Tables in %s (%d)\n\n"
                         (or (mysql-conn-database conn) "?")
                         (length tables))
                 'face 'font-lock-comment-face))
        (dolist (tbl tables)
          (insert-text-button tbl
                              'action (lambda (btn)
                                        (mysql-describe-table
                                         (button-label btn)))
                              'follow-link t)
          (insert "\n"))
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun mysql-describe-table (table)
  "Show the structure of TABLE using DESCRIBE."
  (interactive
   (list (if-let* ((schema (mysql-interactive--schema-for-connection)))
             (completing-read "Table: " (hash-table-keys schema) nil t)
           (read-string "Table: "))))
  (mysql-interactive--ensure-connection)
  (let* ((conn mysql-interactive-connection)
         (result (mysql-query conn (format "DESCRIBE %s"
                                           (mysql-escape-identifier table))))
         (col-names (mapcar (lambda (c) (plist-get c :name))
                            (mysql-result-columns result)))
         (rows (mysql-result-rows result))
         (buf (get-buffer-create (format "*mysql: %s*" table))))
    (with-current-buffer buf
      (mysql-schema-mode)
      (setq-local mysql-interactive-connection conn)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "-- Structure of %s\n\n" table)
                            'face 'font-lock-comment-face))
        (mysql-interactive--insert-org-table col-names rows)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun mysql-describe-table-at-point ()
  "Describe the table name at point."
  (interactive)
  (if-let* ((table (thing-at-point 'symbol t)))
      (mysql-describe-table table)
    (call-interactively #'mysql-describe-table)))

(defun mysql-schema-describe-at-point ()
  "In schema browser, describe the table on the current line."
  (interactive)
  (if-let* ((btn (button-at (point))))
      (mysql-describe-table (button-label btn))
    (if-let* ((table (thing-at-point 'symbol t)))
        (mysql-describe-table table)
      (user-error "No table at point"))))

(defun mysql-schema-refresh ()
  "Refresh the schema browser and cache."
  (interactive)
  (mysql-interactive--ensure-connection)
  (mysql-interactive--refresh-schema-cache mysql-interactive-connection)
  (mysql-list-tables))

;;;; mysql-mode (SQL editing major mode)

(defvar mysql-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'mysql-execute-query-at-point)
    (define-key map (kbd "C-c C-r") #'mysql-execute-region)
    (define-key map (kbd "C-c C-b") #'mysql-execute-buffer)
    (define-key map (kbd "C-c C-e") #'mysql-connect-interactive)
    (define-key map (kbd "C-c C-t") #'mysql-list-tables)
    (define-key map (kbd "C-c C-d") #'mysql-describe-table-at-point)
    (define-key map (kbd "C-c C-l") #'mysql-interactive-show-history)
    map)
  "Keymap for `mysql-mode'.")

;;;###autoload
(define-derived-mode mysql-mode sql-mode "MySQL"
  "Major mode for editing and executing MySQL queries.

\\<mysql-mode-map>
Key bindings:
  \\[mysql-execute-query-at-point]	Execute query at point
  \\[mysql-execute-region]	Execute region
  \\[mysql-execute-buffer]	Execute buffer
  \\[mysql-connect-interactive]	Connect to server
  \\[mysql-list-tables]	List tables
  \\[mysql-describe-table-at-point]	Describe table at point
  \\[mysql-interactive-show-history]	Show query history"
  (add-hook 'completion-at-point-functions
            #'mysql-interactive-completion-at-point nil t)
  (mysql-interactive--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . mysql-mode))

;;;; mysql-result-mode

(defvar mysql-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "C-c '") #'mysql-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'mysql-result-commit)
    (define-key map "g" #'mysql-result-rerun)
    (define-key map "e" #'mysql-result-export)
    (define-key map "c" #'mysql-result-goto-column)
    (define-key map "v" #'mysql-result-toggle-vertical)
    (define-key map "n" #'mysql-result-load-more)
    (define-key map "s" #'mysql-result-sort-by-column)
    (define-key map "S" #'mysql-result-sort-by-column-desc)
    (define-key map "y" #'mysql-result-yank-cell)
    (define-key map "w" #'mysql-result-copy-row-as-insert)
    map)
  "Keymap for `mysql-result-mode'.")

(define-derived-mode mysql-result-mode special-mode "MySQL-Result"
  "Mode for displaying MySQL query results.

\\<mysql-result-mode-map>
  \\[mysql-result-edit-cell]	Edit cell value
  \\[mysql-result-commit]	Commit edits as UPDATE
  \\[mysql-result-goto-column]	Jump to column by name
  \\[mysql-result-toggle-vertical]	Toggle vertical/table view
  \\[mysql-result-sort-by-column]	Sort ascending
  \\[mysql-result-sort-by-column-desc]	Sort descending
  \\[mysql-result-yank-cell]	Copy cell value
  \\[mysql-result-copy-row-as-insert]	Copy row as INSERT
  \\[mysql-result-load-more]	Load more rows
  \\[mysql-result-rerun]	Re-execute the query
  \\[mysql-result-export]	Export results"
  (setq truncate-lines t)
  (hl-line-mode 1))

(defun mysql-result-load-more ()
  "Append the next page of rows to the result buffer."
  (interactive)
  (let* ((all-rows mysql-interactive--result-rows)
         (total (length all-rows))
         (offset mysql-interactive--display-offset)
         (page-size mysql-interactive-result-max-rows)
         (col-names mysql-interactive--result-columns))
    (when (>= offset total)
      (user-error "All %d rows already displayed" total))
    (let* ((next-page (seq-subseq all-rows offset
                                   (min (+ offset page-size) total)))
           (new-offset (+ offset (length next-page)))
           (inhibit-read-only t))
      ;; Remove the "Showing N/M" footer before appending
      (goto-char (point-max))
      (when (re-search-backward "^-- Showing" nil t)
        (delete-region (match-beginning 0) (point-max)))
      (if mysql-interactive--vertical-view
          (mysql-interactive--insert-vertical-view col-names next-page)
        (mysql-interactive--insert-org-table col-names next-page offset))
      (setq-local mysql-interactive--display-offset new-offset)
      (if (< new-offset total)
          (insert (propertize
                   (format "\n-- Showing %d/%d rows  [n: load more]\n"
                           new-offset total)
                   'face 'font-lock-comment-face))
        (insert (propertize
                 (format "\n-- All %d rows displayed\n" total)
                 'face 'font-lock-comment-face))))))

(defun mysql-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql mysql-interactive--last-query))
      (mysql-interactive--execute sql mysql-interactive-connection)
    (user-error "No query to re-execute")))

;;;; Cell editing (C-c ')

(defun mysql-result--cell-at-point ()
  "Return (ROW-IDX COL-IDX FULL-VALUE) for the cell at point, or nil."
  (let ((ridx (get-text-property (point) 'mysql-row-idx))
        (cidx (get-text-property (point) 'mysql-col-idx))
        (val  (get-text-property (point) 'mysql-full-value)))
    (when ridx
      (list ridx cidx val))))

(defvar mysql-result--edit-callback nil
  "Callback for the cell edit buffer: (lambda (new-value) ...).")

(defvar mysql-result-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'mysql-result-edit-finish)
    (define-key map (kbd "C-c C-k") #'mysql-result-edit-cancel)
    map)
  "Keymap for the cell edit buffer.")

(define-minor-mode mysql-result-edit-mode
  "Minor mode for editing a MySQL cell value.
\\<mysql-result-edit-mode-map>
  \\[mysql-result-edit-finish]	Accept edit
  \\[mysql-result-edit-cancel]	Cancel"
  :lighter " MySQL-Edit"
  :keymap mysql-result-edit-mode-map)

(defun mysql-result-edit-cell ()
  "Edit the cell at point in a dedicated buffer (like `C-c \\='` in org)."
  (interactive)
  (let ((cell (mysql-result--cell-at-point)))
    (unless cell
      (user-error "No cell at point"))
    (let* ((ridx (nth 0 cell))
           (cidx (nth 1 cell))
           (val  (nth 2 cell))
           (col-name (nth cidx mysql-interactive--result-columns))
           (result-buf (current-buffer))
           (edit-buf (get-buffer-create
                      (format "*mysql-edit: [%d].%s*" ridx col-name))))
      (with-current-buffer edit-buf
        (erase-buffer)
        (insert (mysql-interactive--format-value val))
        (goto-char (point-min))
        (mysql-result-edit-mode 1)
        (setq-local header-line-format
                    (format " Editing row %d, column \"%s\"  |  C-c C-c: accept  C-c C-k: cancel"
                            ridx col-name))
        (setq-local mysql-result--edit-callback
                    (lambda (new-value)
                      (with-current-buffer result-buf
                        (mysql-result--apply-edit ridx cidx new-value)))))
      (pop-to-buffer edit-buf))))

(defun mysql-result-edit-finish ()
  "Accept the edit and return to the result buffer."
  (interactive)
  (let ((new-value (string-trim-right (buffer-string)))
        (cb mysql-result--edit-callback))
    (quit-window 'kill)
    (when cb
      (funcall cb (if (string= new-value "NULL") nil new-value)))))

(defun mysql-result-edit-cancel ()
  "Cancel the edit and return to the result buffer."
  (interactive)
  (quit-window 'kill))

(defun mysql-result--apply-edit (ridx cidx new-value)
  "Record edit for row RIDX, column CIDX with NEW-VALUE and refresh display."
  (let ((key (cons ridx cidx))
        (original (nth cidx (nth ridx mysql-interactive--result-rows))))
    ;; If edited back to original, remove the pending edit
    (if (equal new-value original)
        (setq mysql-interactive--pending-edits
              (assoc-delete-all key mysql-interactive--pending-edits))
      (let ((existing (assoc key mysql-interactive--pending-edits)))
        (if existing
            (setcdr existing new-value)
          (push (cons key new-value) mysql-interactive--pending-edits)))))
  ;; Redraw
  (mysql-result--redraw))

(defun mysql-result--redraw ()
  "Redraw the result buffer preserving current state."
  (let* ((inhibit-read-only t)
         (col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (offset mysql-interactive--display-offset)
         (total (length rows))
         (page (seq-take rows offset)))
    (erase-buffer)
    (when mysql-interactive--pending-edits
      (insert (propertize
               (format "-- %d pending edit%s\n"
                       (length mysql-interactive--pending-edits)
                       (if (= (length mysql-interactive--pending-edits) 1) "" "s"))
               'face 'mysql-modified-face)))
    (if mysql-interactive--vertical-view
        (progn
          (setq header-line-format
                (propertize (format " %d row%s  (vertical view)"
                                    total (if (= total 1) "" "s"))
                            'face 'mysql-header-face))
          (mysql-interactive--insert-vertical-view col-names page))
      (mysql-interactive--insert-org-table col-names page))
    (when (< offset total)
      (insert (propertize
               (format "\n-- Showing %d/%d rows  [n: load more]\n" offset total)
               'face 'font-lock-comment-face)))
    (goto-char (point-min))))

;;;; Commit edits

(defun mysql-result--detect-table ()
  "Try to detect the source table from query or column metadata.
Returns table name string or nil."
  ;; Try column metadata first — :org-table is the physical table
  (when-let* ((defs mysql-interactive--result-column-defs)
              (tables (delete-dups
                       (delq nil (mapcar (lambda (c) (plist-get c :org-table))
                                         defs))))
              ;; single table only
              ((= (length tables) 1))
              ((not (string-empty-p (car tables)))))
    (car tables)))

(defun mysql-result--detect-primary-key ()
  "Return a list of column indices that form the primary key, or nil."
  (when-let* ((conn mysql-interactive-connection)
              (table (mysql-result--detect-table)))
    (condition-case nil
        (let* ((result (mysql-query
                        conn
                        (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                                (mysql-escape-identifier table))))
               (pk-cols (mapcar (lambda (row)
                                  ;; Column_name is index 4
                                  (nth 4 row))
                                (mysql-result-rows result)))
               (col-names mysql-interactive--result-columns))
          (delq nil (mapcar (lambda (pk)
                              (cl-position pk col-names :test #'string=))
                            pk-cols)))
      (mysql-error nil))))

(defun mysql-result-commit ()
  "Generate and execute UPDATE statements for pending edits."
  (interactive)
  (unless mysql-interactive--pending-edits
    (user-error "No pending edits"))
  (let* ((table (mysql-result--detect-table))
         (pk-indices (mysql-result--detect-primary-key))
         (col-names mysql-interactive--result-columns)
         (rows mysql-interactive--result-rows)
         (statements nil))
    (unless table
      (user-error "Cannot detect source table (multi-table query?)"))
    (unless pk-indices
      (user-error "Cannot detect primary key for table %s" table))
    ;; Group edits by row
    (let ((by-row (make-hash-table :test 'eql)))
      (pcase-dolist (`((,ridx . ,cidx) . ,val) mysql-interactive--pending-edits)
        (push (cons cidx val) (gethash ridx by-row)))
      (maphash
       (lambda (ridx edits)
         (let* ((row (nth ridx rows))
                ;; SET clause
                (set-parts
                 (mapcar (lambda (e)
                           (format "%s = %s"
                                   (mysql-escape-identifier (nth (car e) col-names))
                                   (if (null (cdr e))
                                       "NULL"
                                     (mysql-escape-literal
                                      (if (stringp (cdr e)) (cdr e)
                                        (mysql-interactive--format-value (cdr e)))))))
                         edits))
                ;; WHERE clause from PK
                (where-parts
                 (mapcar (lambda (pki)
                           (let ((v (nth pki row)))
                             (format "%s = %s"
                                     (mysql-escape-identifier (nth pki col-names))
                                     (if (null v) "IS NULL"
                                       (mysql-escape-literal
                                        (if (stringp v) v
                                          (mysql-interactive--format-value v)))))))
                         pk-indices)))
           (push (format "UPDATE %s SET %s WHERE %s"
                         (mysql-escape-identifier table)
                         (mapconcat #'identity set-parts ", ")
                         (mapconcat #'identity where-parts " AND "))
                 statements)))
       by-row))
    ;; Show statements and ask for confirmation
    (let ((sql-text (mapconcat (lambda (s) (concat s ";")) (nreverse statements) "\n")))
      (when (yes-or-no-p
             (format "Execute %d UPDATE statement%s?\n\n%s\n\n"
                     (length statements)
                     (if (= (length statements) 1) "" "s")
                     sql-text))
        (dolist (stmt statements)
          (condition-case err
              (mysql-query mysql-interactive-connection stmt)
            (mysql-error
             (user-error "UPDATE failed: %s" (error-message-string err)))))
        ;; Clear edits and re-run query to refresh
        (setq mysql-interactive--pending-edits nil)
        (message "%d row%s updated"
                 (length statements)
                 (if (= (length statements) 1) "" "s"))
        (mysql-interactive--execute mysql-interactive--last-query
                                    mysql-interactive-connection)))))

;;;; Sort

(defun mysql-result--sort-key (val)
  "Return a comparison key for VAL.
Numbers sort numerically, nil sorts last, everything else as string."
  (cond
   ((null val) nil)
   ((numberp val) val)
   ((stringp val) val)
   ;; date/time plists — format to string for comparison
   (t (mysql-interactive--format-value val))))

(defun mysql-result--compare (a b)
  "Compare two sort keys A and B.  nils sort last."
  (cond
   ((and (null a) (null b)) nil)
   ((null a) nil)  ; a (nil) goes after b
   ((null b) t)    ; b (nil) goes after a
   ((and (numberp a) (numberp b)) (< a b))
   (t (string< (format "%s" a) (format "%s" b)))))

(defun mysql-result--sort (col-name descending)
  "Sort result rows by COL-NAME.  If DESCENDING, reverse order."
  (unless mysql-interactive--result-columns
    (user-error "No result data"))
  (let* ((col-names mysql-interactive--result-columns)
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (user-error "Column %s not found" col-name))
    (setq mysql-interactive--result-rows
          (sort mysql-interactive--result-rows
                (lambda (a b)
                  (let ((va (mysql-result--sort-key (nth idx a)))
                        (vb (mysql-result--sort-key (nth idx b))))
                    (if descending
                        (mysql-result--compare vb va)
                      (mysql-result--compare va vb))))))
    ;; Reset offset to show from top and redraw
    (setq mysql-interactive--display-offset
          (min (length mysql-interactive--result-rows)
               mysql-interactive-result-max-rows))
    (mysql-result--redraw)
    (message "Sorted by %s %s" col-name (if descending "DESC" "ASC"))))

(defun mysql-result--read-column ()
  "Read a column name, defaulting to column at point."
  (let* ((col-names mysql-interactive--result-columns)
         (cidx (get-text-property (point) 'mysql-col-idx))
         (default (when cidx (nth cidx col-names))))
    (completing-read (if default
                         (format "Sort by column (default %s): " default)
                       "Sort by column: ")
                     col-names nil t nil nil default)))

(defun mysql-result-sort-by-column ()
  "Sort results ascending by a column."
  (interactive)
  (mysql-result--sort (mysql-result--read-column) nil))

(defun mysql-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (mysql-result--sort (mysql-result--read-column) t))

;;;; Yank cell / Copy row as INSERT

(defun mysql-result-yank-cell ()
  "Copy the full value of the cell at point to the kill ring."
  (interactive)
  (let ((cell (mysql-result--cell-at-point)))
    (unless cell
      (user-error "No cell at point"))
    (let ((text (mysql-interactive--format-value (nth 2 cell))))
      (kill-new text)
      (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…")))))

(defun mysql-result-copy-row-as-insert ()
  "Copy the current row as an INSERT statement to the kill ring."
  (interactive)
  (let ((ridx (get-text-property (point) 'mysql-row-idx)))
    (unless ridx
      (user-error "No row at point"))
    (let* ((table (or (mysql-result--detect-table) "TABLE"))
           (col-names mysql-interactive--result-columns)
           (row (nth ridx mysql-interactive--result-rows))
           (cols (mapconcat #'mysql-escape-identifier col-names ", "))
           (vals (mapconcat
                  (lambda (v)
                    (cond
                     ((null v) "NULL")
                     ((numberp v) (number-to-string v))
                     ((stringp v) (mysql-escape-literal v))
                     (t (mysql-escape-literal
                         (mysql-interactive--format-value v)))))
                  row ", "))
           (stmt (format "INSERT INTO %s (%s) VALUES (%s);"
                         (mysql-escape-identifier table) cols vals)))
      (kill-new stmt)
      (message "Copied INSERT for row %d" ridx))))

(defun mysql-result-goto-column ()
  "Jump to a column by name in the org-table result.
Uses `completing-read' to select, then scrolls horizontally."
  (interactive)
  (unless mysql-interactive--result-columns
    (user-error "No result columns"))
  (let* ((col-names mysql-interactive--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      ;; Find the org-table header line and locate the column
      (goto-char (point-min))
      (when (re-search-forward "^|[^-]" nil t)
        (beginning-of-line)
        (let ((col-count 0))
          (while (and (< col-count idx)
                      (search-forward "|" (line-end-position) t))
            (cl-incf col-count))
          ;; scroll so column is visible near left edge
          (set-window-hscroll (selected-window)
                              (max 0 (- (current-column) 4))))))))

(defun mysql-result-toggle-vertical ()
  "Toggle between horizontal org-table and vertical card view."
  (interactive)
  (unless mysql-interactive--result-columns
    (user-error "No result data"))
  (setq mysql-interactive--vertical-view
        (not mysql-interactive--vertical-view))
  (mysql-result--redraw)
  (set-window-hscroll (selected-window) 0))

(defun mysql-result-export ()
  "Export the current result.
Prompts for format: org (copy to kill ring) or csv (new buffer)."
  (interactive)
  (let ((fmt (completing-read "Export format: " '("org" "csv") nil t)))
    (pcase fmt
      ("org"
       (kill-ring-save (point-min) (point-max))
       (message "Org table copied to kill ring"))
      ("csv"
       (mysql-interactive--export-csv)))))

(defun mysql-interactive--export-csv ()
  "Export the current result buffer as CSV into a new buffer."
  (let ((lines (split-string (buffer-substring-no-properties
                               (point-min) (point-max))
                              "\n" t))
        (csv-buf (generate-new-buffer "*mysql: export.csv*")))
    (with-current-buffer csv-buf
      (dolist (line lines)
        ;; Skip org table separators and comment lines
        (unless (or (string-match-p "\\`|[-+]" line)
                    (string-match-p "\\`--" line)
                    (string-match-p "\\`[[:space:]]*$" line))
          (when (string-match-p "\\`|" line)
            (let* ((cells (split-string line "|" t))
                   (trimmed (mapcar #'string-trim cells)))
              (insert (mapconcat
                       (lambda (c)
                         (if (string-match-p "," c)
                             (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" c))
                           c))
                       trimmed ",")
                      "\n")))))
      (goto-char (point-min)))
    (pop-to-buffer csv-buf)))

;;;; REPL mode

(defvar mysql-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'mysql-connect-interactive)
    (define-key map (kbd "C-c C-t") #'mysql-list-tables)
    (define-key map (kbd "C-c C-d") #'mysql-describe-table-at-point)
    map)
  "Keymap for `mysql-repl-mode'.")

(defvar-local mysql-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")

(define-derived-mode mysql-repl-mode comint-mode "MySQL-REPL"
  "Major mode for MySQL REPL.

\\<mysql-repl-mode-map>
  \\[mysql-connect-interactive]	Connect to server
  \\[mysql-list-tables]	List tables
  \\[mysql-describe-table-at-point]	Describe table at point"
  (setq comint-prompt-regexp "^mysql> \\|^    -> ")
  (setq comint-input-sender #'mysql-repl--input-sender)
  (add-hook 'completion-at-point-functions
            #'mysql-interactive-completion-at-point nil t))

(defun mysql-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat mysql-repl--pending-input
                          (unless (string-empty-p mysql-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        ;; Complete statement — execute
        (progn
          (setq mysql-repl--pending-input "")
          (mysql-repl--execute-and-print (string-trim combined)))
      ;; Incomplete — accumulate and show continuation prompt
      (setq mysql-repl--pending-input combined)
      (mysql-repl--output "    -> "))))

(defun mysql-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((buf (current-buffer)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (goto-char (process-mark (get-buffer-process buf)))
        (insert text)
        (set-marker (process-mark (get-buffer-process buf)) (point))))))

(defun mysql-repl--execute-and-print (sql)
  "Execute SQL and print results inline in the REPL."
  (if (not (mysql-interactive--connection-alive-p mysql-interactive-connection))
      (mysql-repl--output "ERROR: Not connected.  Use C-c C-e to connect.\nmysql> ")
    (mysql-interactive--add-history sql)
    (setq mysql-interactive--last-query sql)
    (condition-case err
      (let* ((start (float-time))
             (result (mysql-query mysql-interactive-connection sql))
             (elapsed (- (float-time) start))
             (columns (mysql-result-columns result))
             (rows (mysql-result-rows result)))
        (if columns
            ;; SELECT-like
            (let* ((col-names (mapcar (lambda (c) (plist-get c :name)) columns))
                   (table-str
                    (with-temp-buffer
                      (mysql-interactive--insert-org-table col-names rows)
                      (buffer-string))))
              (mysql-repl--output
               (format "\n%s%d row%s in %.3fs\n\nmysql> "
                       table-str
                       (length rows)
                       (if (= (length rows) 1) "" "s")
                       elapsed)))
          ;; DML
          (let ((msg (format "\nAffected rows: %s"
                             (or (mysql-result-affected-rows result) 0))))
            (when-let* ((id (mysql-result-last-insert-id result)))
              (when (> id 0)
                (setq msg (concat msg (format ", Last insert ID: %s" id)))))
            (when-let* ((w (mysql-result-warnings result)))
              (when (> w 0)
                (setq msg (concat msg (format ", Warnings: %s" w)))))
            (mysql-repl--output
             (format "%s (%.3fs)\n\nmysql> " msg elapsed)))))
      (mysql-error
       (mysql-repl--output
        (format "\nERROR: %s\n\nmysql> " (error-message-string err)))))))

;;;###autoload
(defun mysql-repl ()
  "Start a MySQL REPL buffer."
  (interactive)
  (let* ((buf-name "*MySQL REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        ;; Start a dummy process for comint
        (let ((proc (start-process "mysql-repl" buf "cat")))
          (set-process-query-on-exit-flag proc nil)
          (mysql-repl-mode)
          (mysql-repl--output "mysql> "))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(provide 'mysql-interactive)
;;; mysql-interactive.el ends here
