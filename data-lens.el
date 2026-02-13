;;; data-lens.el --- Interactive database lens -*- lexical-binding: t; -*-

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
;; - `data-lens-mode': SQL editing major mode (derived from `sql-mode')
;; - `data-lens-repl': REPL via `comint-mode'
;; - Query execution with column-paged result tables
;; - Schema browsing and completion
;;
;; Entry points:
;;   M-x data-lens-mode      — open a SQL editing buffer
;;   M-x data-lens-repl      — open a REPL
;;   Open a .mysql file   — activates data-lens-mode automatically

;;; Code:

(require 'data-lens-db)
(require 'sql)
(require 'comint)
(require 'cl-lib)
(require 'ring)
(require 'transient)

(declare-function nerd-icons-mdicon "nerd-icons")
(declare-function nerd-icons-codicon "nerd-icons")

;;;; Customization

(defgroup data-lens nil
  "Interactive database lens."
  :group 'comm
  :prefix "data-lens-")

(defface data-lens-header-face
  '((t :inherit bold))
  "Face for column headers in result tables."
  :group 'data-lens)

(defface data-lens-pinned-header-face
  '((t :inherit data-lens-header-face))
  "Face for pinned column headers."
  :group 'data-lens)

(defface data-lens-header-active-face
  '((t :inherit hl-line :weight bold))
  "Face for the column header under the cursor."
  :group 'data-lens)

(defface data-lens-border-face
  '((t :inherit shadow))
  "Face for table borders (pipes and separators)."
  :group 'data-lens)

(defface data-lens-null-face
  '((t :inherit shadow :slant italic))
  "Face for NULL values."
  :group 'data-lens)

(defface data-lens-modified-face
  '((t :inherit warning))
  "Face for modified cell values."
  :group 'data-lens)

(defface data-lens-fk-face
  '((t :inherit font-lock-type-face :underline t))
  "Face for foreign key column values.
Underlined to indicate clickable (RET to follow)."
  :group 'data-lens)

(defface data-lens-marked-face
  '((t :inherit dired-marked))
  "Face for marked rows in result buffer."
  :group 'data-lens)

(defcustom data-lens-connection-alist nil
  "Alist of saved database connections.
Each entry has the form:
  (NAME . (:host H :port P :user U :password P :database D
           [:backend SYM] [:sql-product SYM]))
NAME is a string used for `completing-read'.
:backend is a symbol (\\='mysql or \\='pg, default \\='mysql).
:sql-product overrides `data-lens-sql-product' for this connection."
  :type '(alist :key-type string
                :value-type (plist :options
                                   ((:host string)
                                    (:port integer)
                                    (:user string)
                                    (:password string)
                                    (:database string)
                                    (:backend symbol)
                                    (:sql-product symbol))))
  :group 'data-lens)

(defcustom data-lens-history-file
  (expand-file-name "data-lens-history" user-emacs-directory)
  "File for persisting SQL query history."
  :type 'file
  :group 'data-lens)

(defcustom data-lens-history-length 500
  "Maximum number of history entries to keep."
  :type 'natnum
  :group 'data-lens)

(defcustom data-lens-result-max-rows 1000
  "Maximum number of rows to display in result tables."
  :type 'natnum
  :group 'data-lens)

(defcustom data-lens-column-width-max 30
  "Maximum display width for a single column in the result table."
  :type 'natnum
  :group 'data-lens)

(defcustom data-lens-column-width-step 5
  "Step size for widening/narrowing columns with +/-."
  :type 'natnum
  :group 'data-lens)

(defcustom data-lens-column-padding 1
  "Number of padding spaces on each side of a cell."
  :type 'natnum
  :group 'data-lens)

(defcustom data-lens-sql-product 'mysql
  "SQL product used for syntax highlighting.
Must be a symbol recognized by `sql-mode' (e.g. mysql, postgres)."
  :type '(choice (const :tag "MySQL" mysql)
                 (const :tag "PostgreSQL" postgres)
                 (const :tag "MariaDB" mariadb)
                 (symbol :tag "Other"))
  :group 'data-lens)

;;;; Buffer-local variables

(defvar-local data-lens-connection nil
  "Current database connection for this buffer.")

(defvar-local data-lens--conn-sql-product nil
  "SQL product for the current connection, or nil to use the default.")

(defvar-local data-lens--last-query nil
  "Last executed SQL query string.")

(defvar-local data-lens--result-columns nil
  "Column names from the last result, as a list of strings.")

(defvar-local data-lens--result-rows nil
  "Row data from the last result, as a list of lists.")

(defvar-local data-lens--column-widths nil
  "Vector of integers — display width for each column.")

(defvar-local data-lens--column-pages nil
  "Vector of vectors — each page contains non-pinned column indices.")

(defvar-local data-lens--current-col-page 0
  "Current column page index.")

(defvar-local data-lens--pinned-columns nil
  "List of column indices that are pinned (visible on all pages).")

(defvar-local data-lens--last-window-width nil
  "Last known window body width, to avoid redundant refreshes.")

(defvar-local data-lens--header-active-col nil
  "Col-idx currently highlighted in the header, or nil.")

(defvar-local data-lens--row-overlay nil
  "Overlay used to highlight the current row.")

(defvar-local data-lens--page-current 0
  "Current data page number (0-based).")

(defvar-local data-lens--page-total-rows nil
  "Total row count from COUNT(*), or nil if not yet queried.")

(defvar-local data-lens--order-by nil
  "Current ORDER BY state as (COL-NAME . DIRECTION) or nil.
DIRECTION is \"ASC\" or \"DESC\".")

(defvar-local data-lens--result-column-defs nil
  "Full column definition plists from the last result.")

(defvar-local data-lens--pending-edits nil
  "Alist of pending edits: ((ROW-IDX . COL-IDX) . NEW-VALUE).")

(defvar-local data-lens--fk-info nil
  "Foreign key info for the current result.
Alist of (COL-IDX . (:ref-table TABLE :ref-column COLUMN)).")

(defvar-local data-lens--sort-column nil
  "Column name currently sorted by, or nil.")

(defvar-local data-lens--sort-descending nil
  "Non-nil if the current sort is descending.")

(defvar-local data-lens--where-filter nil
  "Current WHERE filter string, or nil if no filter active.")

(defvar-local data-lens--base-query nil
  "The original unfiltered SQL query, used by WHERE filtering.")

(defvar-local data-lens--marked-rows nil
  "List of marked row indices (dired-style selection).")

(defvar-local data-lens-record--result-buffer nil
  "Reference to the parent result buffer (Record buffer local).")

(defvar-local data-lens-record--row-idx nil
  "Current row index being displayed (Record buffer local).")

(defvar-local data-lens-record--expanded-fields nil
  "List of column indices with expanded long fields (Record buffer local).")

;;;; History

(defvar data-lens--history (make-ring 500)
  "Ring buffer of executed SQL queries.")

(defvar data-lens--history-loaded nil
  "Non-nil if history has been loaded from disk.")

(defun data-lens--load-history ()
  "Load history from `data-lens-history-file'."
  (unless data-lens--history-loaded
    (setq data-lens--history (make-ring data-lens-history-length))
    (when (file-readable-p data-lens-history-file)
      (let ((entries (split-string
                      (with-temp-buffer
                        (insert-file-contents data-lens-history-file)
                        (buffer-string))
                      "\0" t)))
        (dolist (entry (nreverse entries))
          (ring-insert data-lens--history entry))))
    (setq data-lens--history-loaded t)))

(defun data-lens--save-history ()
  "Save history to `data-lens-history-file'."
  (let ((entries nil)
        (len (ring-length data-lens--history)))
    (dotimes (i (min len data-lens-history-length))
      (push (ring-ref data-lens--history i) entries))
    (with-temp-file data-lens-history-file
      (insert (mapconcat #'identity entries "\0")))))

(defun data-lens--add-history (sql)
  "Add SQL to history ring, avoiding duplicates at head."
  (data-lens--load-history)
  (let ((trimmed (string-trim sql)))
    (unless (string-empty-p trimmed)
      (when (or (ring-empty-p data-lens--history)
                (not (string= trimmed (ring-ref data-lens--history 0))))
        (ring-insert data-lens--history trimmed))
      (data-lens--save-history))))

(defun data-lens-show-history ()
  "Select a query from history and insert it at point."
  (interactive)
  (data-lens--load-history)
  (when (ring-empty-p data-lens--history)
    (user-error "No history entries"))
  (let* ((entries (ring-elements data-lens--history))
         (choice (completing-read "SQL history: " entries nil t)))
    (insert choice)))

;;;; Connection management

(defun data-lens--connection-key (conn)
  "Return a descriptive string for CONN like \"user@host:port/db\"."
  (format "%s@%s:%s/%s"
          (or (data-lens-db-user conn) "?")
          (or (data-lens-db-host conn) "?")
          (or (data-lens-db-port conn) "?")
          (or (data-lens-db-database conn) "")))

(defun data-lens--connection-alive-p (conn)
  "Return non-nil if CONN is live."
  (and conn (data-lens-db-live-p conn)))

(defun data-lens--ensure-connection ()
  "Ensure current buffer has a live connection.  Signal error if not."
  (unless (data-lens--connection-alive-p data-lens-connection)
    (user-error "Not connected.  Use C-c C-e to connect")))

(defun data-lens--update-mode-line ()
  "Update mode-line lighter with connection status."
  (setq mode-name
        (if (data-lens--connection-alive-p data-lens-connection)
            (format "%s[%s]"
                    (data-lens-db-display-name data-lens-connection)
                    (data-lens--connection-key data-lens-connection))
          "DB[disconnected]"))
  (force-mode-line-update))

(defun data-lens-connect ()
  "Connect to a database server interactively.
If `data-lens-connection-alist' is non-empty, offer saved connections via
`completing-read'.  Otherwise prompt for each parameter."
  (interactive)
  (when (data-lens--connection-alive-p data-lens-connection)
    (data-lens-db-disconnect data-lens-connection)
    (setq data-lens-connection nil))
  (let* ((conn-params
          (if data-lens-connection-alist
              (cdr (assoc (completing-read "Connection: "
                                          (mapcar #'car data-lens-connection-alist)
                                          nil t)
                          data-lens-connection-alist))
            (list :host (read-string "Host (127.0.0.1): " nil nil "127.0.0.1")
                  :port (read-number "Port (3306): " 3306)
                  :user (read-string "User: ")
                  :password (read-passwd "Password: ")
                  :database (let ((db (read-string "Database (optional): ")))
                              (unless (string-empty-p db) db)))))
         (backend (or (plist-get conn-params :backend) 'mysql))
         (product (plist-get conn-params :sql-product))
         (db-params (cl-loop for (k v) on conn-params by #'cddr
                             unless (memq k '(:sql-product :backend))
                             append (list k v)))
         (conn (condition-case err
                   (data-lens-db-connect backend db-params)
                 (data-lens-db-error
                  (user-error "Connection failed: %s"
                              (error-message-string err))))))
    (setq data-lens-connection conn)
    (setq data-lens--conn-sql-product product)
    (data-lens--update-mode-line)
    (data-lens--refresh-schema-cache conn)
    (message "Connected to %s" (data-lens--connection-key conn))))

(defun data-lens-disconnect ()
  "Disconnect from the current database server."
  (interactive)
  (when (data-lens--connection-alive-p data-lens-connection)
    (data-lens-db-disconnect data-lens-connection)
    (message "Disconnected"))
  (setq data-lens-connection nil)
  (data-lens--update-mode-line))

;;;; Value formatting

(defun data-lens--format-value (val)
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

(defun data-lens--truncate-cell (str max-width)
  "Truncate STR to MAX-WIDTH, replacing embedded pipes to protect org tables."
  (let ((clean (replace-regexp-in-string "|" "¦" (replace-regexp-in-string "\n" "↵" str))))
    (if (> (length clean) max-width)
        (concat (substring clean 0 (- max-width 1)) "…")
      clean)))

(defun data-lens--column-names (columns)
  "Extract column names from COLUMNS as a list of strings.
Handles the case where the driver returns non-string names
\(e.g., SELECT 1 produces an integer column name)."
  (mapcar (lambda (c)
            (let ((name (plist-get c :name)))
              (if (stringp name) name (format "%s" name))))
          columns))

(defun data-lens--value-to-literal (val)
  "Convert Elisp VAL to a SQL literal string.
nil → \"NULL\", numbers unquoted, strings escaped."
  (cond
   ((null val) "NULL")
   ((numberp val) (number-to-string val))
   ((stringp val) (data-lens-db-escape-literal data-lens-connection val))
   (t (data-lens-db-escape-literal data-lens-connection
                                   (data-lens--format-value val)))))

(defun data-lens--string-pad (str width &optional right-align)
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

;;;; Column width computation and paging

(defun data-lens--numeric-type-p (col-def)
  "Return non-nil if COL-DEF is a numeric column type."
  (eq (plist-get col-def :type-category) 'numeric))

(defun data-lens--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB)."
  (memq (plist-get col-def :type-category) '(json blob)))

(defun data-lens--long-field-placeholder (col-def)
  "Return a placeholder string for a long field type COL-DEF."
  (pcase (plist-get col-def :type-category)
    ('json "<JSON>")
    (_ "<BLOB>")))

(defun data-lens--compute-column-widths (col-names rows column-defs
                                                      &optional max-width)
  "Compute display width for each column.
COL-NAMES is a list of header strings, ROWS is the data,
COLUMN-DEFS is the column metadata list.
MAX-WIDTH caps individual column width (default `data-lens-column-width-max').
Pass a large value or nil to use the default.
Returns a vector of integers."
  (let* ((ncols (length col-names))
         (max-w (or max-width data-lens-column-width-max))
         (widths (make-vector ncols 0))
         (sample (seq-take rows 50)))
    (dotimes (i ncols)
      (if (and (data-lens--long-field-type-p (nth i column-defs))
               (<= max-w data-lens-column-width-max))
          (aset widths i 10)
        (let ((header-w (string-width (nth i col-names)))
              (data-w 0))
          (dolist (row sample)
            (let ((formatted (data-lens--format-value (nth i row))))
              (setq data-w (max data-w (string-width formatted)))))
          (aset widths i (max 5 (min max-w (max header-w data-w)))))))
    widths))

(defun data-lens--compute-column-pages (widths pinned window-width)
  "Compute column pages based on WIDTHS, PINNED columns, and WINDOW-WIDTH.
Each column occupies width + 2*padding + 1 (pipe separator).
PINNED columns are always shown and their width is deducted first.
Returns a vector of vectors, each containing non-pinned column indices."
  (let* ((padding data-lens-column-padding)
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

(defun data-lens--visible-columns ()
  "Return list of column indices visible on the current page.
Pinned columns come first, followed by the current page's columns."
  (let ((page-cols (when (and data-lens--column-pages
                              (< data-lens--current-col-page
                                 (length data-lens--column-pages)))
                     (append (aref data-lens--column-pages
                                   data-lens--current-col-page)
                             nil))))
    (append data-lens--pinned-columns page-cols)))

;;;; Result display

(defun data-lens--result-buffer-name ()
  "Return the result buffer name based on current connection."
  (if (data-lens--connection-alive-p data-lens-connection)
      (format "*data-lens: %s*"
              (or (data-lens-db-database data-lens-connection) "results"))
    "*data-lens: results*"))

(defun data-lens--render-static-table (col-names rows &optional column-defs)
  "Render a table string from COL-NAMES and ROWS.
Uses the same visual style as the column-paged result renderer.
COLUMN-DEFS, if provided, is used for long-field detection.
Returns a string (with text properties)."
  (let* ((data-lens--result-columns col-names)
         (data-lens--result-column-defs column-defs)
         (data-lens--pending-edits nil)
         (data-lens--fk-info nil)
         (ncols (length col-names))
         (all-cols (number-sequence 0 (1- ncols)))
         (widths (data-lens--compute-column-widths col-names rows column-defs 1000))
         (bface 'data-lens-border-face)
         (sep-top (propertize (data-lens--render-separator all-cols widths 'top)
                              'face bface))
         (sep-mid (propertize (data-lens--render-separator all-cols widths 'middle)
                              'face bface))
         (sep-bot (propertize (data-lens--render-separator all-cols widths 'bottom)
                              'face bface))
         (header (data-lens--render-header all-cols widths))
         (lines nil))
    (push sep-top lines)
    (push header lines)
    (push sep-mid lines)
    (let ((ridx 0))
      (dolist (row rows)
        (push (data-lens--render-row row ridx all-cols widths) lines)
        (cl-incf ridx)))
    (push sep-bot lines)
    (mapconcat #'identity (nreverse lines) "\n")))

;;;; Column-paged renderer

(defun data-lens--replace-edge-borders (str has-prev has-next)
  "Replace edge border characters in STR with page indicators.
When HAS-PREV is non-nil, replace the first border char with `◂'.
When HAS-NEXT is non-nil, replace the last border char with `▸'."
  (when has-prev
    (let ((c (aref str 0)))
      (when (memq c '(?│ ?┌ ?├ ?└ ?┬ ?┼ ?┴))
        (setq str (concat (propertize "◂" 'face 'data-lens-border-face)
                          (substring str 1))))))
  (when has-next
    (let* ((len (length str))
           (c (aref str (1- len))))
      (when (memq c '(?│ ?┐ ?┤ ?┘))
        (setq str (concat (substring str 0 (1- len))
                           (propertize "▸" 'face 'data-lens-border-face))))))
  str)

(defun data-lens--render-separator (visible-cols widths &optional position)
  "Render a separator line for VISIBLE-COLS with WIDTHS.
POSITION is `top', `middle', or `bottom' (default `middle')."
  (let* ((padding data-lens-column-padding)
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

(defun data-lens--icon (name &rest fallback)
  "Return a nerd-icons icon for NAME, or FALLBACK string.
NAME is a cons (FAMILY . ICON-NAME) where FAMILY is one of
`mdicon', `codicon', etc.  Falls back gracefully when
nerd-icons is not installed."
  (let ((family (car name))
        (icon-name (cdr name)))
    (or (and (require 'nerd-icons nil t)
             (pcase family
               ('mdicon (nerd-icons-mdicon icon-name))
               ('codicon (nerd-icons-codicon icon-name))))
        (car fallback)
        "")))

(defun data-lens--fixed-width-icon (spec fallback &optional face)
  "Return icon with `string-width' matching actual display width.
SPEC is (FAMILY . ICON-NAME) for `data-lens--icon'.
FALLBACK is the Unicode char when nerd-icons is unavailable.
Optional FACE is applied to the result.

When `string-pixel-width' is available, measures the icon glyph
pixel width and wraps it in a display property over the correct
number of space characters.  This ensures `string-width' matches
the real rendered width, preventing column misalignment."
  (let* ((raw (data-lens--icon spec fallback))
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

(defun data-lens--header-label (name cidx)
  "Build the display label for column NAME at index CIDX.
Prepends sort indicator and pin marker before the name."
  (let* ((hi 'font-lock-keyword-face)
         (sort (when (and data-lens--sort-column
                          (string= name data-lens--sort-column))
                 (data-lens--fixed-width-icon
                  (if data-lens--sort-descending
                      '(codicon . "nf-cod-arrow_down")
                    '(codicon . "nf-cod-arrow_up"))
                  (if data-lens--sort-descending "▼" "▲")
                  hi)))
         (pin (when (memq cidx data-lens--pinned-columns)
                (data-lens--fixed-width-icon
                 '(mdicon . "nf-md-pin") "∎" hi))))
    (if (or sort pin)
        (concat (or sort "") (or pin "") (if (or sort pin) " " "") name)
      name)))

(defun data-lens--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each column name carries a `data-lens-header-col' text property
so the active-column overlay can find it."
  (let ((padding data-lens-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((name (nth cidx data-lens--result-columns))
             (w (aref widths cidx))
             (label (data-lens--header-label name cidx))
             (padded (data-lens--string-pad
                      (if (> (string-width label) w)
                          (truncate-string-to-width label w)
                        label)
                      w))
             (face (if (memq cidx data-lens--pinned-columns)
                       'data-lens-pinned-header-face
                     'data-lens-header-face))
             (pad-str (make-string padding ?\s)))
        (push (concat (propertize "│" 'face 'data-lens-border-face)
                      pad-str
                      (propertize padded
                                  'face face
                                  'data-lens-header-col cidx)
                      pad-str)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "│" 'face 'data-lens-border-face))))

(defun data-lens--render-row (row ridx visible-cols widths)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector.
Returns a propertized string."
  (let ((padding data-lens-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((val (nth cidx row))
             (col-def (nth cidx data-lens--result-column-defs))
             (edited (assoc (cons ridx cidx) data-lens--pending-edits))
             (display-val (if edited (cdr edited) val))
             (w (aref widths cidx))
             (formatted (let ((s (replace-regexp-in-string
                               "\n" "↵"
                               (data-lens--format-value display-val))))
                          (if (and (not edited)
                                   (data-lens--long-field-type-p col-def)
                                   (> (length s) w)
                                   (not (stringp display-val)))
                              (data-lens--long-field-placeholder col-def)
                            s)))
             (truncated (if (> (string-width formatted) w)
                            (truncate-string-to-width formatted w)
                          formatted))
             (ralign (data-lens--numeric-type-p col-def))
             (padded (data-lens--string-pad truncated w ralign))
             (face (cond (edited 'data-lens-modified-face)
                         ((null val) 'data-lens-null-face)
                         ((assq cidx data-lens--fk-info) 'data-lens-fk-face)
                         (t nil)))
             (cell (propertize padded
                               'data-lens-row-idx ridx
                               'data-lens-col-idx cidx
                               'data-lens-full-value (if edited (cdr edited) val)
                               'face face))
             (pad-str (make-string padding ?\s)))
        (push (concat (propertize "│" 'face 'data-lens-border-face)
                      pad-str cell pad-str)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "│" 'face 'data-lens-border-face))))

(defun data-lens--footer-row-range (first-row last-row total-rows)
  "Return the row-range part of the footer.
FIRST-ROW and LAST-ROW are 1-based; TOTAL-ROWS is nil or a number."
  (let ((hi 'font-lock-keyword-face)
        (dim 'font-lock-comment-face))
    (concat (propertize "rows " 'face dim)
            (propertize (format "%d-%d" first-row last-row) 'face hi)
            (propertize " of " 'face dim)
            (propertize (if total-rows (format "%d" total-rows) "?")
                        'face hi))))

(defun data-lens--footer-page-indicator (page-num page-size total-rows)
  "Return the data-page part of the footer.
PAGE-NUM is 0-based, PAGE-SIZE is rows per page, TOTAL-ROWS may be nil."
  (let ((hi 'font-lock-keyword-face)
        (dim 'font-lock-comment-face))
    (if total-rows
        (let ((total-pages (max 1 (ceiling total-rows (float page-size)))))
          (concat (propertize " | Page " 'face dim)
                  (propertize (format "%d/%d" (1+ page-num)
                                      (truncate total-pages))
                              'face hi)))
      (concat (propertize " | Page " 'face dim)
              (propertize (format "%d" (1+ page-num)) 'face hi)))))

(defun data-lens--render-footer (row-count page-num page-size
                                           total-rows col-num-pages col-cur-page)
  "Return the footer string for pagination state.
ROW-COUNT is the number of rows on the current page.
PAGE-NUM is the current data page (0-based).
PAGE-SIZE is `data-lens-result-max-rows'.
TOTAL-ROWS is the known total or nil.
COL-NUM-PAGES and COL-CUR-PAGE are for column page display."
  (let* ((hi 'font-lock-keyword-face)
         (dim 'font-lock-comment-face)
         (first-row (1+ (* page-num page-size)))
         (last-row (+ (* page-num page-size) row-count))
         (parts (list (data-lens--footer-page-indicator page-num page-size total-rows)
                      (data-lens--footer-row-range first-row last-row total-rows))))
    (when (> col-num-pages 1)
      (push (concat (propertize " | Col page " 'face dim)
                    (propertize (format "%d/%d" col-cur-page col-num-pages)
                                'face hi)
                    (propertize " | " 'face dim)
                    (propertize "[" 'face hi)
                    (propertize "-prev/ " 'face dim)
                    (propertize "]" 'face hi)
                    (propertize "-next" 'face dim))
            parts))
    (when data-lens--where-filter
      (push (concat (propertize " | W: " 'face 'font-lock-warning-face)
                    (propertize data-lens--where-filter
                                'face 'font-lock-warning-face))
            parts))
    (apply #'concat (nreverse parts))))

(defun data-lens--effective-widths ()
  "Return column widths adjusted for header indicator icons.
Columns with sort or pin indicators get wider to fit the label."
  (let ((widths (copy-sequence data-lens--column-widths)))
    (dotimes (cidx (length widths))
      (let* ((name (nth cidx data-lens--result-columns))
             (label (data-lens--header-label name cidx))
             (label-w (string-width label)))
        (when (> label-w (aref widths cidx))
          (aset widths cidx label-w))))
    widths))

(defun data-lens--header-cell (cidx widths &optional active-cidx)
  "Build a single header cell string for column CIDX.
WIDTHS is the effective width vector.
ACTIVE-CIDX is the highlighted column index, if any."
  (let* ((name (nth cidx data-lens--result-columns))
         (w (aref widths cidx))
         (label (data-lens--header-label name cidx))
         (padded (data-lens--string-pad
                  (if (> (string-width label) w)
                      (truncate-string-to-width label w)
                    label)
                  w))
         (face (cond
                ((eql cidx active-cidx) 'data-lens-header-active-face)
                ((memq cidx data-lens--pinned-columns)
                 'data-lens-pinned-header-face)
                (t 'data-lens-header-face)))
         (pad-str (make-string data-lens-column-padding ?\s)))
    (concat (propertize "│" 'face 'data-lens-border-face)
            pad-str
            (propertize padded 'face face
                        'data-lens-header-col cidx)
            pad-str)))

(defun data-lens--build-header-line (visible-cols widths nw
                                                  has-prev has-next
                                                  &optional active-cidx)
  "Build the header-line-format string for the column header row.
VISIBLE-COLS, WIDTHS describe columns.
NW is the digit width for the row number column.
HAS-PREV/HAS-NEXT control edge border indicators.
ACTIVE-CIDX highlights that column when non-nil."
  (let* ((edge (lambda (s) (data-lens--replace-edge-borders s has-prev has-next)))
         (bface 'data-lens-border-face)
         (pad-str (make-string data-lens-column-padding ?\s))
         (cells (mapcar (lambda (cidx)
                          (data-lens--header-cell cidx widths active-cidx))
                        visible-cols))
         (data-header (funcall edge
                               (concat (apply #'concat cells)
                                       (propertize "│" 'face bface)))))
    ;; 1 char for mark column + nw for row number + padding
    (concat (propertize "│" 'face bface)
            " " (make-string nw ?\s) pad-str
            data-header)))

(defun data-lens--build-separator (visible-cols widths position
                                                   nw edge-fn)
  "Build a table separator line with row-number column.
VISIBLE-COLS and WIDTHS describe data columns.
POSITION is \\='top, \\='middle, or \\='bottom.
NW is the row-number digit width.
EDGE-FN applies column-page edge indicators."
  (let* ((bface 'data-lens-border-face)
         ;; +1 for the mark column char
         (rn-dash (+ 1 nw data-lens-column-padding))
         (raw (data-lens--render-separator visible-cols widths position))
         (cross (pcase position ('top "┬") ('bottom "┴") (_ "┼")))
         (left (pcase position ('top "┌") ('bottom "└") (_ "├")))
         (data-part (concat cross (substring raw 1)))
         (data-edged (funcall edge-fn (propertize data-part 'face bface)))
         (rn (propertize (concat left (make-string rn-dash ?─)) 'face bface)))
    (concat rn data-edged)))

(defun data-lens--insert-data-rows (rows visible-cols widths nw
                                         global-first-row edge-fn)
  "Insert data ROWS into the current buffer.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
GLOBAL-FIRST-ROW is the 0-based offset for numbering.
EDGE-FN applies column-page edge indicators."
  (let ((bface 'data-lens-border-face)
        (pad-str (make-string data-lens-column-padding ?\s))
        (marked data-lens--marked-rows))
    (cl-loop for row in rows
             for ridx from 0
             for data-row = (funcall edge-fn
                                     (data-lens--render-row
                                      row ridx visible-cols widths))
             for mark-char = (if (memq ridx marked) "*" " ")
             for num-label = (string-pad
                              (number-to-string
                               (1+ (+ global-first-row ridx)))
                              nw nil t)
             for num-face = (if (memq ridx marked)
                                'data-lens-marked-face 'shadow)
             do (insert (propertize "│" 'face bface)
                        (propertize mark-char 'face num-face)
                        (propertize num-label 'face num-face)
                        pad-str
                        data-row "\n"))))

(defun data-lens--render-result ()
  "Render the result buffer content using column paging."
  (let* ((inhibit-read-only t)
         (visible-cols (data-lens--visible-columns))
         (widths (data-lens--effective-widths))
         (rows data-lens--result-rows)
         (col-num-pages (length data-lens--column-pages))
         (cur-page data-lens--current-col-page)
         (has-prev (> cur-page 0))
         (has-next (< cur-page (1- col-num-pages)))
         (edge-fn (lambda (s) (data-lens--replace-edge-borders
                               s has-prev has-next)))
         (nw (data-lens--row-number-digits))
         (global-first-row (* data-lens--page-current
                              data-lens-result-max-rows)))
    (erase-buffer)
    (setq tab-line-format
          (concat (propertize " " 'display '(space :align-to 0))
                  (data-lens--build-separator
                   visible-cols widths 'top nw edge-fn)))
    (setq header-line-format
          (concat (propertize " " 'display '(space :align-to 0))
                  (data-lens--build-header-line visible-cols widths nw
                                                has-prev has-next
                                                data-lens--header-active-col)))
    (insert (data-lens--build-separator
             visible-cols widths 'middle nw edge-fn) "\n")
    (when data-lens--pending-edits
      (insert (propertize
               (format "-- %d pending edit%s\n"
                       (length data-lens--pending-edits)
                       (if (= (length data-lens--pending-edits) 1) "" "s"))
               'face 'data-lens-modified-face)))
    (data-lens--insert-data-rows rows visible-cols widths nw
                                 global-first-row edge-fn)
    (insert (data-lens--build-separator
             visible-cols widths 'bottom nw edge-fn) "\n")
    (insert (data-lens--render-footer
             (length rows) data-lens--page-current
             data-lens-result-max-rows data-lens--page-total-rows
             col-num-pages (1+ cur-page))
            "\n")
    (when data-lens--last-query
      (insert (propertize
               (truncate-string-to-width
                (replace-regexp-in-string
                 "[\n\r]+" " "
                 (string-trim data-lens--last-query))
                120 nil nil t)
               'face 'font-lock-comment-face)
              "\n"))
    (goto-char (point-min))))

(defun data-lens--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'data-lens-col-idx))

(defun data-lens--goto-cell (ridx cidx)
  "Move point to the cell at ROW-IDX RIDX and COL-IDX CIDX.
Falls back to the same row (any column), then point-min."
  (goto-char (point-min))
  (let ((found nil))
    (while (and (not found)
                (setq found (text-property-search-forward
                             'data-lens-row-idx ridx #'eq)))
      (let ((beg (prop-match-beginning found)))
        (if (eq (get-text-property beg 'data-lens-col-idx) cidx)
            (goto-char beg)
          (setq found nil))))
    (unless found
      ;; Fall back: find the same row, any column
      (goto-char (point-min))
      (if-let* ((m (text-property-search-forward 'data-lens-row-idx ridx #'eq)))
          (goto-char (prop-match-beginning m))
        (goto-char (point-min))))))

(defun data-lens--row-number-digits ()
  "Return the digit width needed for row numbers."
  (let* ((row-count (length data-lens--result-rows))
         (global-last (+ (* data-lens--page-current
                            data-lens-result-max-rows)
                         row-count)))
    (max 3 (length (number-to-string global-last)))))

(defun data-lens--refresh-display ()
  "Recompute column pages for current window width and re-render.
Preserves cursor position (row + column) across the refresh."
  (when data-lens--column-widths
    (let* ((save-ridx (get-text-property (point) 'data-lens-row-idx))
           (save-cidx (get-text-property (point) 'data-lens-col-idx))
           (win (get-buffer-window (current-buffer)))
           (win-width (if win (window-body-width win) 80))
           (nw (data-lens--row-number-digits))
           (width (- win-width 1 (* 2 data-lens-column-padding) nw)))
      (setq data-lens--column-pages
            (data-lens--compute-column-pages
             (data-lens--effective-widths)
             data-lens--pinned-columns
             width))
      (let ((max-page (1- (length data-lens--column-pages))))
        (setq data-lens--current-col-page
              (max 0 (min data-lens--current-col-page max-page))))
      (setq data-lens--last-window-width win-width)
      (setq data-lens--header-active-col nil)
      (when data-lens--row-overlay
        (delete-overlay data-lens--row-overlay)
        (setq data-lens--row-overlay nil))
      (data-lens--render-result)
      (when save-ridx
        (data-lens--goto-cell save-ridx save-cidx)))))

(defun data-lens--window-size-change (frame)
  "Handle window size changes for result buffers in FRAME."
  (dolist (win (window-list frame 'no-mini))
    (let ((buf (window-buffer win)))
      (when (buffer-local-value 'data-lens--column-widths buf)
        (let ((new-width (window-body-width win)))
          (unless (eq new-width
                      (buffer-local-value 'data-lens--last-window-width buf))
            (with-current-buffer buf
              (data-lens--refresh-display))))))))

(defun data-lens--display-select-result (col-names rows columns)
  "Render a SELECT result with COL-NAMES, ROWS, and COLUMNS metadata."
  (let ((inhibit-read-only t))
    (setq-local data-lens--column-widths
                (data-lens--compute-column-widths
                 col-names rows columns))
    (data-lens--refresh-display)
    (add-hook 'window-size-change-functions
              #'data-lens--window-size-change)))

(defun data-lens--display-dml-result (result sql elapsed)
  "Render a DML RESULT (INSERT/UPDATE/DELETE) with SQL and ELAPSED time."
  (let ((inhibit-read-only t))
    (erase-buffer)
    (setq-local data-lens--column-widths nil)
    (insert (propertize (format "-- %s\n" (string-trim sql))
                        'face 'font-lock-comment-face))
    (insert (format "Affected rows: %s\n"
                    (or (data-lens-db-result-affected-rows result) 0)))
    (when-let* ((id (data-lens-db-result-last-insert-id result))
                ((> id 0)))
      (insert (format "Last insert ID: %s\n" id)))
    (when-let* ((w (data-lens-db-result-warnings result))
                ((> w 0)))
      (insert (format "Warnings: %s\n" w)))
    (insert (propertize (format "\nCompleted in %.3fs\n" elapsed)
                        'face 'font-lock-comment-face))
    (goto-char (point-min))))

(defun data-lens--display-result (result sql elapsed)
  "Display RESULT in the result buffer.
SQL is the query text, ELAPSED the time in seconds.
If the result has columns, shows a table (without pagination).
Otherwise shows DML summary (affected rows, etc.)."
  (let* ((buf-name (data-lens--result-buffer-name))
         (buf (get-buffer-create buf-name))
         (columns (data-lens-db-result-columns result))
         (col-names (when columns (data-lens--column-names columns)))
         (rows (data-lens-db-result-rows result)))
    (with-current-buffer buf
      (data-lens-result-mode)
      (setq-local data-lens--last-query sql)
      (setq-local data-lens-connection
                  (data-lens-db-result-connection result))
      (if col-names
          ;; Tabular result (DESCRIBE, SHOW, EXPLAIN, etc.)
          (progn
            (setq-local data-lens--base-query nil)
            (setq-local data-lens--result-columns col-names)
            (setq-local data-lens--result-column-defs columns)
            (setq-local data-lens--result-rows rows)
            (setq-local data-lens--pending-edits nil)
            (setq-local data-lens--marked-rows nil)
            (setq-local data-lens--current-col-page 0)
            (setq-local data-lens--pinned-columns nil)
            (setq-local data-lens--sort-column nil)
            (setq-local data-lens--sort-descending nil)
            (setq-local data-lens--page-current 0)
            (setq-local data-lens--page-total-rows (length rows))
            (setq-local data-lens--order-by nil)
            (data-lens--display-select-result col-names rows columns))
        ;; DML result
        (data-lens--display-dml-result result sql elapsed)))
    (pop-to-buffer buf '(display-buffer-at-bottom))))

;;;; SQL pagination helpers

(defun data-lens--sql-has-limit-p (sql)
  "Return non-nil if SQL already contains a LIMIT clause."
  (let ((case-fold-search t))
    (string-match-p "\\bLIMIT\\b" sql)))

(defun data-lens--build-paged-sql (base-sql page-num page-size &optional order-by)
  "Build a paged SQL query wrapping BASE-SQL.
PAGE-NUM is 0-based, PAGE-SIZE is the row limit.
ORDER-BY is a cons (COL-NAME . DIRECTION) or nil.
If BASE-SQL already has LIMIT, return it unchanged.
Delegates to the backend for dialect-specific pagination."
  (if (data-lens--sql-has-limit-p base-sql)
      base-sql
    (data-lens-db-build-paged-sql data-lens-connection base-sql
                                  page-num page-size order-by)))

(defun data-lens--execute-page (page-num)
  "Execute the query for PAGE-NUM and refresh the result buffer display.
Uses `data-lens--base-query' as the base SQL.
Signals an error if pagination is not available."
  (unless data-lens--base-query
    (user-error "Pagination not available for this query"))
  (let* ((conn data-lens-connection)
         (base data-lens--base-query)
         (page-size data-lens-result-max-rows)
         (paged-sql (data-lens--build-paged-sql
                     base page-num page-size data-lens--order-by)))
    (unless (data-lens--connection-alive-p conn)
      (user-error "Not connected"))
    (let* ((start (float-time))
           (result (condition-case err
                       (data-lens-db-query conn paged-sql)
                     (data-lens-db-error
                      (user-error "Query error: %s"
                                  (error-message-string err)))))
           (elapsed (- (float-time) start))
           (columns (data-lens-db-result-columns result))
           (rows (data-lens-db-result-rows result))
           (col-names (data-lens--column-names columns)))
      (setq-local data-lens--result-columns col-names)
      (setq-local data-lens--result-column-defs columns)
      (setq-local data-lens--result-rows rows)
      (setq-local data-lens--page-current page-num)
      (setq-local data-lens--pending-edits nil)
      (setq-local data-lens--marked-rows nil)
      (setq-local data-lens--column-widths
                  (data-lens--compute-column-widths col-names rows columns))
      (data-lens--refresh-display)
      (message "Page %d loaded (%.3fs, %d row%s)"
               (1+ page-num) elapsed (length rows)
               (if (= (length rows) 1) "" "s")))))

;;;; Query execution engine

(defun data-lens--strip-leading-comments (sql)
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

(defun data-lens--destructive-query-p (sql)
  "Return non-nil if SQL is a destructive operation.
Leading SQL comments are stripped before checking."
  (let ((trimmed (data-lens--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:DELETE\\|DROP\\|TRUNCATE\\|ALTER\\)\\b"
                    (upcase trimmed))))

(defun data-lens--select-query-p (sql)
  "Return non-nil if SQL is a SELECT query.
Leading SQL comments are stripped before checking."
  (let ((trimmed (data-lens--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:SELECT\\|WITH\\)\\b"
                    (upcase trimmed))))

(defun data-lens--execute-select (sql connection)
  "Execute a SELECT SQL query with pagination on CONNECTION.
Returns the query result."
  (let* ((page-size data-lens-result-max-rows)
         (paged-sql (data-lens-db-build-paged-sql connection sql 0 page-size))
         (result (condition-case err
                     (data-lens-db-query connection paged-sql)
                   (data-lens-db-error
                    (user-error "Query error: %s"
                                (error-message-string err)))))
         (buf (get-buffer-create (data-lens--result-buffer-name)))
         (columns (data-lens-db-result-columns result))
         (rows (data-lens-db-result-rows result))
         (col-names (data-lens--column-names columns)))
    (with-current-buffer buf
      (data-lens-result-mode)
      (setq-local data-lens--last-query sql
                  data-lens--base-query sql
                  data-lens-connection connection
                  data-lens--result-columns col-names
                  data-lens--result-column-defs columns
                  data-lens--result-rows rows
                  data-lens--pending-edits nil
                  data-lens--marked-rows nil
                  data-lens--current-col-page 0
                  data-lens--pinned-columns nil
                  data-lens--sort-column nil
                  data-lens--sort-descending nil
                  data-lens--page-current 0
                  data-lens--page-total-rows nil
                  data-lens--order-by nil)
      (when col-names
        (data-lens--display-select-result col-names rows columns))
      (data-lens--load-fk-info))
    (pop-to-buffer buf '(display-buffer-at-bottom))
    result))

(defun data-lens--execute-dml (sql connection)
  "Execute a DML SQL query on CONNECTION and display results.
Returns the query result."
  (setq data-lens--last-query sql)
  (let* ((start (float-time))
         (result (condition-case err
                     (data-lens-db-query connection sql)
                   (data-lens-db-error
                    (user-error "Query error: %s"
                                (error-message-string err)))))
         (elapsed (- (float-time) start)))
    (data-lens--display-result result sql elapsed)
    result))

(defun data-lens--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Records history, times execution, and displays results.
For SELECT queries, applies pagination (LIMIT/OFFSET).
Prompts for confirmation on destructive operations."
  (let ((connection (or conn data-lens-connection)))
    (unless (data-lens--connection-alive-p connection)
      (user-error "Not connected.  Use C-c C-e to connect"))
    (when (data-lens--destructive-query-p sql)
      (unless (yes-or-no-p
               (format "Execute destructive query?\n  %s\n"
                       (truncate-string-to-width (string-trim sql) 80)))
        (user-error "Query cancelled")))
    (data-lens--add-history sql)
    (if (data-lens--select-query-p sql)
        (data-lens--execute-select sql connection)
      (data-lens--execute-dml sql connection))))

;;;; Query-at-point detection

(defun data-lens--query-at-point ()
  "Return the SQL query around point.
Queries are delimited by semicolons or blank lines."
  (let* ((delimiter "\\(;\\|^[[:space:]]*$\\)")
         (beg (save-excursion
                (if (re-search-backward delimiter nil t)
                    (match-end 0)
                  (point-min))))
         (end (save-excursion
                (if (re-search-forward delimiter nil t)
                    (match-beginning 0)
                  (point-max))))
         (query (string-trim (buffer-substring-no-properties beg end))))
    (when (string-empty-p query)
      (user-error "No query at point"))
    query))

;;;; Interactive commands

(defun data-lens-execute-query-at-point ()
  "Execute the SQL query at point."
  (interactive)
  (data-lens--ensure-connection)
  (data-lens--execute (data-lens--query-at-point)))

(defun data-lens-execute-region (beg end)
  "Execute SQL in the region from BEG to END."
  (interactive "r")
  (data-lens--ensure-connection)
  (data-lens--execute
   (string-trim (buffer-substring-no-properties beg end))))

(defun data-lens-execute-buffer ()
  "Execute the entire buffer as a SQL query."
  (interactive)
  (data-lens--ensure-connection)
  (data-lens--execute
   (string-trim (buffer-substring-no-properties (point-min) (point-max)))))

(defun data-lens--find-connection ()
  "Find a live database connection from any data-lens-mode buffer.
Returns the connection or nil."
  (cl-loop for buf in (buffer-list)
           for conn = (buffer-local-value 'data-lens-connection buf)
           when (data-lens--connection-alive-p conn)
           return conn))

;;;###autoload
(defun data-lens-execute (sql)
  "Execute SQL from any buffer.
With an active region, execute the region.  Otherwise execute the
current line.  Uses the connection from any data-lens-mode buffer."
  (interactive
   (list (string-trim
          (if (use-region-p)
              (buffer-substring-no-properties (region-beginning) (region-end))
            (buffer-substring-no-properties
             (line-beginning-position) (line-end-position))))))
  (when (string-empty-p sql)
    (user-error "No SQL to execute"))
  (let ((conn (or data-lens-connection
                  (data-lens--find-connection)
                  (user-error "No active connection.  Use M-x data-lens-mode then C-c C-e to connect"))))
    (data-lens--execute sql conn)))

;;;; Indirect edit buffer

(defun data-lens--string-at-point ()
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

(defvar data-lens-indirect-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c '") #'data-lens-indirect-execute)
    (define-key map (kbd "C-c C-k") #'data-lens-indirect-abort)
    map)
  "Keymap for `data-lens-indirect-mode'.")

(define-minor-mode data-lens-indirect-mode
  "Minor mode active in indirect SQL edit buffers.
\\<data-lens-indirect-mode-map>
Key bindings:
  \\[data-lens-indirect-execute]	Execute and close
  \\[data-lens-indirect-abort]	Abort and close"
  :lighter " Indirect")

(defun data-lens-indirect-execute ()
  "Execute the SQL in the indirect buffer, then close it."
  (interactive)
  (let ((sql (string-trim
              (buffer-substring-no-properties (point-min) (point-max))))
        (conn (or data-lens-connection
                  (data-lens--find-connection))))
    (when (string-empty-p sql)
      (user-error "No SQL to execute"))
    (unless conn
      (user-error "No active connection"))
    (quit-window 'kill)
    (data-lens--execute sql conn)))

(defun data-lens-indirect-abort ()
  "Abort the indirect edit buffer."
  (interactive)
  (quit-window 'kill))

;;;###autoload
(defun data-lens-edit-indirect ()
  "Open an indirect `data-lens-mode' buffer with SQL extracted from context.
With an active region, use the region.  When point is inside a
string literal (DAO code, etc.), extract the string content.
Otherwise use the current line.

The indirect buffer inherits the connection from any live
`data-lens-mode' buffer.  Edit the SQL freely, then press
\\<data-lens-indirect-mode-map>\\[data-lens-indirect-execute] \
to execute or \\[data-lens-indirect-abort] to abort."
  (interactive)
  (let* ((text (string-trim
                (cond
                 ((use-region-p)
                  (buffer-substring-no-properties
                   (region-beginning) (region-end)))
                 ((data-lens--string-at-point))
                 (t
                  (buffer-substring-no-properties
                   (line-beginning-position) (line-end-position))))))
         (conn (or (bound-and-true-p data-lens-connection)
                   (data-lens--find-connection)))
         (buf (generate-new-buffer "*data-lens: indirect*")))
    (pop-to-buffer buf)
    (data-lens-mode)
    (when conn
      (setq-local data-lens-connection conn)
      (data-lens--update-mode-line))
    (data-lens-indirect-mode 1)
    (insert text)
    (goto-char (point-min))
    (message "Edit SQL, then C-c ' to execute, C-c C-k to abort")))

;;;; Schema cache + completion

(defvar data-lens--schema-cache (make-hash-table :test 'equal)
  "Global schema cache.  Keys are connection-key strings.
Values are hash-tables mapping table-name → list of column-name strings.")

(defun data-lens--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (condition-case nil
      (let* ((key (data-lens--connection-key conn))
             (table-names (data-lens-db-list-tables conn))
             (schema (make-hash-table :test 'equal)))
        (dolist (tbl table-names)
          (puthash tbl nil schema))
        (puthash key schema data-lens--schema-cache)
        (message "Connected — %d tables" (hash-table-count schema)))
    (data-lens-db-error nil)))

(defun data-lens--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA.
Fetches from the backend if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing)))
    (unless (eq cols 'missing)
      (or cols
          (condition-case nil
              (let ((col-names (data-lens-db-list-columns conn table)))
                (puthash table col-names schema)
                col-names)
            (data-lens-db-error nil))))))

(defun data-lens--schema-for-connection ()
  "Return the schema hash-table for the current connection, or nil."
  (when (data-lens--connection-alive-p data-lens-connection)
    (gethash (data-lens--connection-key data-lens-connection)
             data-lens--schema-cache)))

(defun data-lens--tables-in-buffer (schema)
  "Return table names from SCHEMA that appear in the current buffer."
  (let ((text (buffer-substring-no-properties (point-min) (point-max)))
        (tables nil))
    (dolist (tbl (hash-table-keys schema))
      (when (string-match-p (regexp-quote tbl) text)
        (push tbl tables)))
    tables))

(defun data-lens-completion-at-point ()
  "Completion-at-point function for SQL identifiers.
Skips column loading if the connection is busy (prevents re-entrancy
when completion triggers during an in-flight query)."
  (when-let* ((schema (data-lens--schema-for-connection))
              (conn data-lens-connection)
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
           (busy (data-lens-db-busy-p conn))
           (candidates
            (if (or table-context-p busy)
                (hash-table-keys schema)
              ;; Load columns only for tables mentioned in the buffer
              (let ((all (copy-sequence (hash-table-keys schema))))
                (dolist (tbl (data-lens--tables-in-buffer schema))
                  (when-let* ((cols (data-lens--ensure-columns
                                     conn schema tbl)))
                    (setq all (nconc all (copy-sequence cols)))))
                (delete-dups all)))))
      (list beg end candidates :exclusive 'no))))

;;;; Schema browser

(defvar data-lens-schema-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map "g" #'data-lens-schema-refresh)
    (define-key map (kbd "RET") #'data-lens-schema-describe-at-point)
    map)
  "Keymap for `data-lens-schema-mode'.")

(define-derived-mode data-lens-schema-mode special-mode "DB-Schema"
  "Mode for browsing database schema."
  (setq truncate-lines t)
  (setq-local revert-buffer-function #'data-lens-schema--revert))

(defun data-lens-schema--revert (_ignore-auto _noconfirm)
  "Revert function for schema browser."
  (data-lens-schema-refresh))

(defun data-lens-list-tables ()
  "Show a list of tables in the current database."
  (interactive)
  (data-lens--ensure-connection)
  (let* ((conn data-lens-connection)
         (tables (data-lens-db-list-tables conn))
         (buf (get-buffer-create
               (format "*data-lens: %s tables*"
                       (or (data-lens-db-database conn) "?")))))
    (with-current-buffer buf
      (data-lens-schema-mode)
      (setq-local data-lens-connection conn)
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize
                 (format "-- Tables in %s (%d)\n\n"
                         (or (data-lens-db-database conn) "?")
                         (length tables))
                 'face 'font-lock-comment-face))
        (dolist (tbl tables)
          (insert-text-button tbl
                              'action (lambda (btn)
                                        (data-lens-describe-table
                                         (button-label btn)))
                              'follow-link t)
          (insert "\n"))
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun data-lens-describe-table (table)
  "Show the DDL of TABLE using SHOW CREATE TABLE."
  (interactive
   (list (if-let* ((schema (data-lens--schema-for-connection)))
             (completing-read "Table: " (hash-table-keys schema) nil t)
           (read-string "Table: "))))
  (data-lens--ensure-connection)
  (let* ((conn data-lens-connection)
         (product (or data-lens--conn-sql-product data-lens-sql-product))
         (ddl (data-lens-db-show-create-table conn table))
         (buf (get-buffer-create (format "*data-lens: %s*" table))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (sql-mode)
        (sql-set-product product)
        (setq-local data-lens-connection conn)
        (erase-buffer)
        (insert ddl)
        (insert "\n")
        (font-lock-ensure)
        (setq buffer-read-only t)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun data-lens-describe-table-at-point ()
  "Describe the table name at point."
  (interactive)
  (if-let* ((table (thing-at-point 'symbol t)))
      (data-lens-describe-table table)
    (call-interactively #'data-lens-describe-table)))

(defun data-lens-schema-describe-at-point ()
  "In schema browser, describe the table on the current line."
  (interactive)
  (if-let* ((btn (button-at (point))))
      (data-lens-describe-table (button-label btn))
    (if-let* ((table (thing-at-point 'symbol t)))
        (data-lens-describe-table table)
      (user-error "No table at point"))))

(defun data-lens-schema-refresh ()
  "Refresh the schema browser and cache."
  (interactive)
  (data-lens--ensure-connection)
  (data-lens--refresh-schema-cache data-lens-connection)
  (data-lens-list-tables))

;;;; data-lens-mode (SQL editing major mode)

(defvar data-lens-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-c") #'data-lens-execute-query-at-point)
    (define-key map (kbd "C-c C-r") #'data-lens-execute-region)
    (define-key map (kbd "C-c C-b") #'data-lens-execute-buffer)
    (define-key map (kbd "C-c C-e") #'data-lens-connect)
    (define-key map (kbd "C-c C-t") #'data-lens-list-tables)
    (define-key map (kbd "C-c C-d") #'data-lens-describe-table-at-point)
    (define-key map (kbd "C-c C-l") #'data-lens-show-history)
    (define-key map (kbd "C-c C-o") #'data-lens-dispatch)
    map)
  "Keymap for `data-lens-mode'.")

;;;###autoload
(define-derived-mode data-lens-mode sql-mode "DataLens"
  "Major mode for editing and executing SQL queries.

\\<data-lens-mode-map>
Key bindings:
  \\[data-lens-execute-query-at-point]	Execute query at point
  \\[data-lens-execute-region]	Execute region
  \\[data-lens-execute-buffer]	Execute buffer
  \\[data-lens-connect]	Connect to server
  \\[data-lens-list-tables]	List tables
  \\[data-lens-describe-table-at-point]	Describe table at point
  \\[data-lens-show-history]	Show query history"
  (add-hook 'completion-at-point-functions
            #'data-lens-completion-at-point nil t)
  (data-lens--update-mode-line))

;;;###autoload
(add-to-list 'auto-mode-alist '("\\.mysql\\'" . data-lens-mode))

;;;; Cell navigation

(defun data-lens-result-next-cell ()
  "Move point to the next cell (right, then wrap to next row)."
  (interactive)
  (let ((start (point)))
    (goto-char (next-single-property-change (point) 'data-lens-col-idx
                                            nil (point-max)))
    (if-let* ((m (text-property-search-forward 'data-lens-col-idx nil
                                               (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start))))

(defun data-lens-result-prev-cell ()
  "Move point to the previous cell (left, then wrap to prev row)."
  (interactive)
  (let ((start (point)))
    (when-let* ((beg (previous-single-property-change
                      (1+ (point)) 'data-lens-col-idx nil (point-min))))
      (goto-char beg))
    (if-let* ((m (text-property-search-backward 'data-lens-col-idx nil
                                                (lambda (_val cur) cur))))
        (goto-char (prop-match-beginning m))
      (goto-char start))))

(defun data-lens-result-down-cell ()
  "Move to the same column in the next row."
  (interactive)
  (when-let* ((cidx (data-lens--col-idx-at-point))
              (ridx (get-text-property (point) 'data-lens-row-idx)))
    (data-lens--goto-cell (1+ ridx) cidx)))

(defun data-lens-result-up-cell ()
  "Move to the same column in the previous row."
  (interactive)
  (when-let* ((cidx (data-lens--col-idx-at-point))
              (ridx (get-text-property (point) 'data-lens-row-idx))
              ((> ridx 0)))
    (data-lens--goto-cell (1- ridx) cidx)))

;;;; Row marking (dired-style)

(defun data-lens-result--marked-row-indices ()
  "Return the effective row indices for batch operations.
Priority: marked rows > current row."
  (or data-lens--marked-rows
      (when-let* ((ridx (data-lens-result--row-idx-at-line)))
        (list ridx))))

(defun data-lens-result--rerender-and-goto (ridx cidx)
  "Re-render the result buffer and move to cell at RIDX, CIDX.
Preserves the window scroll position relative to the target row."
  (let ((win (selected-window))
        (line-offset (count-lines (window-start) (point))))
    (data-lens--render-result)
    (data-lens--goto-cell ridx cidx)
    (set-window-start win (save-excursion
                            (forward-line (- line-offset))
                            (point))
                       t)))

(defun data-lens-result-toggle-mark ()
  "Toggle mark on the row at point and move to next row."
  (interactive)
  (when-let* ((ridx (data-lens-result--row-idx-at-line))
              (cidx (or (data-lens--col-idx-at-point) 0)))
    (if (memq ridx data-lens--marked-rows)
        (setq data-lens--marked-rows (delq ridx data-lens--marked-rows))
      (push ridx data-lens--marked-rows))
    (data-lens-result--rerender-and-goto (1+ ridx) cidx)))

(defun data-lens-result-unmark-row ()
  "Unmark the row at point and move to next row."
  (interactive)
  (when-let* ((ridx (data-lens-result--row-idx-at-line))
              (cidx (or (data-lens--col-idx-at-point) 0)))
    (setq data-lens--marked-rows (delq ridx data-lens--marked-rows))
    (data-lens-result--rerender-and-goto (1+ ridx) cidx)))

(defun data-lens-result-unmark-all ()
  "Remove all row marks."
  (interactive)
  (when data-lens--marked-rows
    (setq data-lens--marked-rows nil)
    (data-lens--render-result)))

;;;; data-lens-result-mode

(defvar data-lens-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "C-c '") #'data-lens-result-edit-cell)
    (define-key map (kbd "C-c C-c") #'data-lens-result-commit)
    (define-key map "g" #'data-lens-result-rerun)
    (define-key map "e" #'data-lens-result-export)
    (define-key map "c" #'data-lens-result-goto-column)
    (define-key map "n" #'data-lens-result-next-page)
    (define-key map "p" #'data-lens-result-prev-page)
    (define-key map (kbd "M->") #'data-lens-result-last-page)
    (define-key map (kbd "M-<") #'data-lens-result-first-page)
    (define-key map "#" #'data-lens-result-count-total)
    (define-key map "s" #'data-lens-result-sort-by-column)
    (define-key map "S" #'data-lens-result-sort-by-column-desc)
    (define-key map "y" #'data-lens-result-yank-cell)
    (define-key map "w" #'data-lens-result-copy-row-as-insert)
    (define-key map "Y" #'data-lens-result-copy-as-csv)
    (define-key map "W" #'data-lens-result-apply-filter)
    (define-key map (kbd "RET") #'data-lens-result-open-record)
    (define-key map "]" #'data-lens-result-next-col-page)
    (define-key map "[" #'data-lens-result-prev-col-page)
    (define-key map "=" #'data-lens-result-widen-column)
    (define-key map "-" #'data-lens-result-narrow-column)
    (define-key map (kbd "C-c p") #'data-lens-result-pin-column)
    (define-key map (kbd "C-c P") #'data-lens-result-unpin-column)
    (define-key map "F" #'data-lens-result-fullscreen-toggle)
    (define-key map "?" #'data-lens-result-dispatch)
    ;; Cell navigation
    (define-key map (kbd "TAB") #'data-lens-result-next-cell)
    (define-key map (kbd "<backtab>") #'data-lens-result-prev-cell)
    (define-key map (kbd "M-n") #'data-lens-result-down-cell)
    (define-key map (kbd "M-p") #'data-lens-result-up-cell)
    ;; Row marking
    (define-key map "m" #'data-lens-result-toggle-mark)
    (define-key map "u" #'data-lens-result-unmark-row)
    (define-key map "U" #'data-lens-result-unmark-all)
    map)
  "Keymap for `data-lens-result-mode'.")

(defun data-lens--update-position-indicator ()
  "Update mode-line with current cursor position in the result grid."
  (let ((cidx (data-lens--col-idx-at-point))
        (ridx (get-text-property (point) 'data-lens-row-idx)))
    (setq mode-line-position
          (when ridx
            (let* ((page-offset (* data-lens--page-current
                                   data-lens-result-max-rows))
                   (global-row (+ page-offset ridx))
                   (ncols (length data-lens--result-columns))
                   (col-name (when cidx
                               (nth cidx data-lens--result-columns))))
              (let ((mark-info (when data-lens--marked-rows
                                 (format " *%d" (length data-lens--marked-rows)))))
                (format " R%d/%s C%d/%d%s (pg %d)%s"
                        (1+ global-row)
                        (if data-lens--page-total-rows
                            (number-to-string data-lens--page-total-rows)
                          "?")
                        (if cidx (1+ cidx) 0) ncols
                        (if col-name (format " [%s]" col-name) "")
                        (1+ data-lens--page-current)
                        (or mark-info ""))))))))

(defun data-lens--update-row-highlight ()
  "Highlight the entire row under the cursor."
  (when data-lens--row-overlay
    (delete-overlay data-lens--row-overlay)
    (setq data-lens--row-overlay nil))
  (when (get-text-property (point) 'data-lens-row-idx)
    (let ((ov (make-overlay (line-beginning-position)
                            (line-end-position))))
      (overlay-put ov 'face 'hl-line)
      (overlay-put ov 'priority -1)
      (setq data-lens--row-overlay ov))))

(defun data-lens--update-header-highlight ()
  "Highlight the header cell for the column under the cursor.
Rebuilds `header-line-format' with the active column highlighted."
  (when data-lens--column-widths
    (data-lens--update-position-indicator)
    (data-lens--update-row-highlight)
    (let ((cidx (data-lens--col-idx-at-point)))
      (unless (eql cidx data-lens--header-active-col)
        (setq data-lens--header-active-col cidx)
        (let* ((visible-cols (data-lens--visible-columns))
               (widths (data-lens--effective-widths))
               (col-num-pages (length data-lens--column-pages))
               (cur-page data-lens--current-col-page)
               (has-prev (> cur-page 0))
               (has-next (< cur-page (1- col-num-pages)))
               (nw (data-lens--row-number-digits)))
          (setq header-line-format
                (concat (propertize " " 'display '(space :align-to 0))
                        (data-lens--build-header-line
                         visible-cols widths nw
                         has-prev has-next cidx))))))))

(define-derived-mode data-lens-result-mode special-mode "DB-Result"
  "Mode for displaying database query results with SQL pagination.

\\<data-lens-result-mode-map>
Navigate:
  \\[data-lens-result-next-cell]	Next cell (Tab)
  \\[data-lens-result-prev-cell]	Previous cell (S-Tab)
  \\[data-lens-result-down-cell]	Down in same column
  \\[data-lens-result-up-cell]	Up in same column
  \\[data-lens-result-open-record]	Open record view for row
  \\[data-lens-result-goto-column]	Jump to column by name
Mark:
  \\[data-lens-result-toggle-mark]	Toggle mark on row
  \\[data-lens-result-unmark-row]	Unmark row
  \\[data-lens-result-unmark-all]	Unmark all rows
Pages:
  \\[data-lens-result-next-page]	Next data page
  \\[data-lens-result-prev-page]	Previous data page
  \\[data-lens-result-first-page]	First data page
  \\[data-lens-result-last-page]	Last data page
  \\[data-lens-result-count-total]	Query total row count
  \\[data-lens-result-next-col-page]	Next column page
  \\[data-lens-result-prev-col-page]	Previous column page
Copy (C-u for column selection):
  \\[data-lens-result-yank-cell]	Copy cell value
  \\[data-lens-result-copy-row-as-insert]	Copy row(s) as INSERT
  \\[data-lens-result-copy-as-csv]	Copy row(s) as CSV
  \\[data-lens-result-export]	Export results
Edit:
  \\[data-lens-result-edit-cell]	Edit cell value
  \\[data-lens-result-commit]	Commit edits as UPDATE
  \\[data-lens-result-apply-filter]	Apply WHERE filter
  \\[data-lens-result-sort-by-column]	Sort ascending (SQL ORDER BY)
  \\[data-lens-result-sort-by-column-desc]	Sort descending (SQL ORDER BY)
  \\[data-lens-result-widen-column]	Widen column
  \\[data-lens-result-narrow-column]	Narrow column
  \\[data-lens-result-pin-column]	Pin column
  \\[data-lens-result-unpin-column]	Unpin column
  \\[data-lens-result-rerun]	Re-execute the query"
  (setq truncate-lines t)
  (hl-line-mode 1)
  ;; Make tab-line use default background so sep-top renders cleanly
  (face-remap-add-relative 'tab-line :inherit 'default)
  (add-hook 'post-command-hook
            #'data-lens--update-header-highlight nil t))

(defun data-lens-result-next-page ()
  "Go to the next data page."
  (interactive)
  (let ((rows-on-page (length data-lens--result-rows)))
    (when (< rows-on-page data-lens-result-max-rows)
      (user-error "Already on last page (fewer rows than page size)"))
    (data-lens--execute-page (1+ data-lens--page-current))))

(defun data-lens-result-prev-page ()
  "Go to the previous data page."
  (interactive)
  (when (<= data-lens--page-current 0)
    (user-error "Already on first page"))
  (data-lens--execute-page (1- data-lens--page-current)))

(defun data-lens-result-first-page ()
  "Go to the first data page."
  (interactive)
  (when (= data-lens--page-current 0)
    (user-error "Already on first page"))
  (data-lens--execute-page 0))

(defun data-lens-result-last-page ()
  "Go to the last data page.
Triggers a COUNT(*) query if total rows are not yet known."
  (interactive)
  (unless data-lens--page-total-rows
    (data-lens-result-count-total))
  (when data-lens--page-total-rows
    (let* ((page-size data-lens-result-max-rows)
           (last-page (max 0 (1- (ceiling data-lens--page-total-rows
                                           (float page-size))))))
      (if (= data-lens--page-current (truncate last-page))
          (user-error "Already on last page")
        (data-lens--execute-page (truncate last-page))))))

(defun data-lens-result-count-total ()
  "Query the total row count for the current base query."
  (interactive)
  (let* ((conn data-lens-connection)
         (base (or data-lens--base-query data-lens--last-query)))
    (unless (data-lens--connection-alive-p conn)
      (user-error "Not connected"))
    (let* ((count-sql (format "SELECT COUNT(*) FROM (%s) AS _dl_cnt"
                              (string-trim-right
                               (replace-regexp-in-string ";\\s-*\\'" "" base))))
           (result (condition-case err
                       (data-lens-db-query conn count-sql)
                     (data-lens-db-error
                      (user-error "COUNT query error: %s"
                                  (error-message-string err)))))
           (count-val (car (car (data-lens-db-result-rows result)))))
      (setq-local data-lens--page-total-rows
                  (if (numberp count-val) count-val
                    (string-to-number (format "%s" count-val))))
      (data-lens--refresh-display)
      (message "Total rows: %d" data-lens--page-total-rows))))

(defun data-lens-result-rerun ()
  "Re-execute the last query that produced this result buffer."
  (interactive)
  (if-let* ((sql (or data-lens--base-query data-lens--last-query)))
      (data-lens--execute sql data-lens-connection)
    (user-error "No query to re-execute")))

;;;; Cell editing (C-c ')

(defun data-lens-result--cell-at (pos)
  "Return (ROW-IDX COL-IDX FULL-VALUE) at buffer position POS, or nil."
  (when-let* ((ridx (get-text-property pos 'data-lens-row-idx)))
    (list ridx
          (get-text-property pos 'data-lens-col-idx)
          (get-text-property pos 'data-lens-full-value))))

(defun data-lens-result--cell-at-point ()
  "Return (ROW-IDX COL-IDX FULL-VALUE) for the cell at or near point.
If point is on a pipe separator or padding space, scans left then
right on the current line to find the nearest cell."
  (or (data-lens-result--cell-at (point))
      (let ((bol (line-beginning-position))
            (eol (line-end-position)))
        (or (cl-loop for p downfrom (1- (point)) to bol
                     thereis (data-lens-result--cell-at p))
            (cl-loop for p from (1+ (point)) to eol
                     thereis (data-lens-result--cell-at p))))))

(defun data-lens-result--row-idx-at-line ()
  "Return the row index for the current line, or nil.
Scans text properties across the line."
  (cl-loop for p from (line-beginning-position) to (line-end-position)
           thereis (get-text-property p 'data-lens-row-idx)))

(defun data-lens-result--rows-in-region (beg end)
  "Return sorted list of unique row indices in the region BEG..END."
  (let (indices)
    (save-excursion
      (goto-char beg)
      (while (< (point) end)
        (when-let* ((ridx (data-lens-result--row-idx-at-line)))
          (cl-pushnew ridx indices))
        (forward-line 1)))
    (sort indices #'<)))

(defvar-local data-lens-result--edit-callback nil
  "Callback for the cell edit buffer: (lambda (new-value) ...).")

(defvar data-lens-result-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-c") #'data-lens-result-edit-finish)
    (define-key map (kbd "C-c C-k") #'data-lens-result-edit-cancel)
    map)
  "Keymap for the cell edit buffer.")

(define-minor-mode data-lens-result-edit-mode
  "Minor mode for editing a database cell value.
\\<data-lens-result-edit-mode-map>
  \\[data-lens-result-edit-finish]	Accept edit
  \\[data-lens-result-edit-cancel]	Cancel"
  :lighter " DB-Edit"
  :keymap data-lens-result-edit-mode-map)

(defun data-lens-result-edit-cell ()
  "Edit the cell at point in a dedicated buffer (like `C-c \\='` in org)."
  (interactive)
  (let* ((cell (or (data-lens-result--cell-at-point)
                   (user-error "No cell at point")))
         (ridx (nth 0 cell))
         (cidx (nth 1 cell))
         (val  (nth 2 cell))
         (col-name (nth cidx data-lens--result-columns))
         (result-buf (current-buffer))
         (edit-buf (get-buffer-create
                    (format "*data-lens-edit: [%d].%s*" ridx col-name))))
    (with-current-buffer edit-buf
      (erase-buffer)
      (insert (data-lens--format-value val))
      (goto-char (point-min))
      (data-lens-result-edit-mode 1)
      (setq-local header-line-format
                  (format " Editing row %d, column \"%s\"  |  C-c C-c: accept  C-c C-k: cancel"
                          ridx col-name))
      (setq-local data-lens-result--edit-callback
                  (lambda (new-value)
                    (with-current-buffer result-buf
                      (data-lens-result--apply-edit ridx cidx new-value)))))
    (pop-to-buffer edit-buf)))

(defun data-lens-result-edit-finish ()
  "Accept the edit and return to the result buffer."
  (interactive)
  (let ((new-value (string-trim-right (buffer-string)))
        (cb data-lens-result--edit-callback))
    (quit-window 'kill)
    (when cb
      (funcall cb (if (string= new-value "NULL") nil new-value)))))

(defun data-lens-result-edit-cancel ()
  "Cancel the edit and return to the result buffer."
  (interactive)
  (quit-window 'kill))

(defun data-lens-result--apply-edit (ridx cidx new-value)
  "Record edit for row RIDX, column CIDX with NEW-VALUE and refresh display."
  (let ((key (cons ridx cidx))
        (original (nth cidx (nth ridx data-lens--result-rows))))
    ;; If edited back to original, remove the pending edit
    (if (equal new-value original)
        (setq data-lens--pending-edits
              (assoc-delete-all key data-lens--pending-edits))
      (let ((existing (assoc key data-lens--pending-edits)))
        (if existing
            (setcdr existing new-value)
          (push (cons key new-value) data-lens--pending-edits)))))
  (data-lens--refresh-display))

;;;; Commit edits

(defun data-lens-result--detect-table ()
  "Try to detect the source table from query or column metadata.
Returns table name string or nil."
  ;; Try column metadata first — :org-table is the physical table
  (when-let* ((defs data-lens--result-column-defs)
              (tables (delete-dups
                       (delq nil (mapcar (lambda (c) (plist-get c :org-table))
                                         defs))))
              ;; single table only
              ((= (length tables) 1))
              ((not (string-empty-p (car tables)))))
    (car tables)))

(defun data-lens-result--detect-primary-key ()
  "Return a list of column indices that form the primary key, or nil."
  (when-let* ((conn data-lens-connection)
              (table (data-lens-result--detect-table)))
    (condition-case nil
        (let* ((pk-cols (data-lens-db-primary-key-columns conn table))
               (col-names data-lens--result-columns))
          (delq nil (mapcar (lambda (pk)
                              (cl-position pk col-names :test #'string=))
                            pk-cols)))
      (data-lens-db-error nil))))

(defun data-lens--load-fk-info ()
  "Load foreign key info for the current result's source table.
Populates `data-lens--fk-info' with an alist mapping
column indices to their referenced table and column."
  (setq data-lens--fk-info nil)
  (when-let* ((conn data-lens-connection)
              (table (data-lens-result--detect-table))
              (col-names data-lens--result-columns))
    (condition-case nil
        (let ((fks (data-lens-db-foreign-keys conn table)))
          (dolist (fk fks)
            (let* ((col-name (car fk))
                   (ref-info (cdr fk))
                   (idx (cl-position col-name col-names :test #'string=)))
              (when idx
                (push (cons idx ref-info) data-lens--fk-info)))))
      (data-lens-db-error nil))))

(defun data-lens-result--group-edits-by-row (edits)
  "Group EDITS alist by row index into a hash-table.
Returns hash-table mapping ridx → list of (cidx . value)."
  (let ((ht (make-hash-table :test 'eql)))
    (pcase-dolist (`((,ridx . ,cidx) . ,val) edits)
      (push (cons cidx val) (gethash ridx ht)))
    ht))

(defun data-lens-result--build-update-stmt (table row _ridx edits col-names pk-indices)
  "Build an UPDATE statement for TABLE.
ROW is the original row data at _RIDX, EDITS is a list of (cidx . value),
COL-NAMES are column names, PK-INDICES are primary key column indices."
  (let ((conn data-lens-connection))
    (let ((set-parts
           (mapcar (lambda (e)
                     (format "%s = %s"
                             (data-lens-db-escape-identifier
                              conn (nth (car e) col-names))
                             (data-lens--value-to-literal (cdr e))))
                   edits))
          (where-parts
           (mapcar (lambda (pki)
                     (let ((v (nth pki row)))
                       (format "%s %s"
                               (data-lens-db-escape-identifier
                                conn (nth pki col-names))
                               (if (null v) "IS NULL"
                                 (format "= %s"
                                         (data-lens--value-to-literal v))))))
                   pk-indices)))
      (format "UPDATE %s SET %s WHERE %s"
              (data-lens-db-escape-identifier conn table)
              (mapconcat #'identity set-parts ", ")
              (mapconcat #'identity where-parts " AND ")))))

(defun data-lens-result-commit ()
  "Generate and execute UPDATE statements for pending edits."
  (interactive)
  (unless data-lens--pending-edits
    (user-error "No pending edits"))
  (let* ((table (or (data-lens-result--detect-table)
                    (user-error "Cannot detect source table (multi-table query?)")))
         (pk-indices (or (data-lens-result--detect-primary-key)
                         (user-error "Cannot detect primary key for table %s" table)))
         (col-names data-lens--result-columns)
         (rows data-lens--result-rows)
         (by-row (data-lens-result--group-edits-by-row data-lens--pending-edits))
         (statements nil))
    (maphash
     (lambda (ridx edits)
       (push (data-lens-result--build-update-stmt
              table (nth ridx rows) ridx edits col-names pk-indices)
             statements))
     by-row)
    (let ((sql-text (mapconcat (lambda (s) (concat s ";"))
                               (nreverse statements) "\n")))
      (when (yes-or-no-p
             (format "Execute %d UPDATE statement%s?\n\n%s\n\n"
                     (length statements)
                     (if (= (length statements) 1) "" "s")
                     sql-text))
        (dolist (stmt statements)
          (condition-case err
              (data-lens-db-query data-lens-connection stmt)
            (data-lens-db-error
             (user-error "UPDATE failed: %s" (error-message-string err)))))
        (setq data-lens--pending-edits nil)
        (message "%d row%s updated"
                 (length statements)
                 (if (= (length statements) 1) "" "s"))
        (data-lens--execute data-lens--last-query
                                    data-lens-connection)))))

;;;; Sort

(defun data-lens-result--sort (col-name descending)
  "Sort result rows by COL-NAME using SQL ORDER BY.
If DESCENDING, sort in descending order.
Re-executes from the first page."
  (unless data-lens--result-columns
    (user-error "No result data"))
  (let* ((col-names data-lens--result-columns)
         (idx (cl-position col-name col-names :test #'string=)))
    (unless idx
      (user-error "Column %s not found" col-name))
    (let ((direction (if descending "DESC" "ASC")))
      (setq data-lens--sort-column col-name)
      (setq data-lens--sort-descending descending)
      (setq data-lens--order-by (cons col-name direction))
      (setq data-lens--page-current 0)
      (data-lens--execute-page 0)
      (message "Sorted by %s %s" col-name direction))))

(defun data-lens-result--read-column ()
  "Read a column name, defaulting to column at point."
  (let* ((col-names data-lens--result-columns)
         (cidx (get-text-property (point) 'data-lens-col-idx))
         (default (when cidx (nth cidx col-names))))
    (completing-read (if default
                         (format "Sort by column (default %s): " default)
                       "Sort by column: ")
                     col-names nil t nil nil default)))

(defun data-lens-result-sort-by-column ()
  "Sort results by a column.
If the column is already sorted, toggle the direction."
  (interactive)
  (let* ((col-name (data-lens-result--read-column))
         (descending (if (and data-lens--sort-column
                              (string= col-name data-lens--sort-column))
                         (not data-lens--sort-descending)
                       nil)))
    (data-lens-result--sort col-name descending)))

(defun data-lens-result-sort-by-column-desc ()
  "Sort results descending by a column."
  (interactive)
  (data-lens-result--sort (data-lens-result--read-column) t))

;;;; WHERE filtering

(defun data-lens--apply-where (sql filter)
  "Apply WHERE FILTER to SQL query string.
If SQL already has a WHERE clause, appends FILTER with AND.
Otherwise inserts WHERE before ORDER BY/GROUP BY/HAVING/LIMIT or at end."
  (let* ((trimmed (string-trim-right
                   (replace-regexp-in-string ";\\s-*\\'" "" sql)))
         (case-fold-search t)
         (has-where (string-match-p "\\bWHERE\\b" trimmed))
         (clause (if has-where
                     (format "AND (%s) " filter)
                   (format "WHERE %s " filter)))
         (tail-re "\\b\\(ORDER[[:space:]]+BY\\|GROUP[[:space:]]+BY\\|HAVING\\|LIMIT\\)\\b"))
    (if (string-match tail-re trimmed)
        (let ((pos (match-beginning 0)))
          (concat (substring trimmed 0 pos) clause (substring trimmed pos)))
      (concat trimmed " " (string-trim-right clause)))))

(defun data-lens-result-apply-filter ()
  "Apply or clear a WHERE filter on the current result query.
Prompts for a WHERE condition.  Enter empty string to clear."
  (interactive)
  (unless data-lens--last-query
    (user-error "No query to filter"))
  (let* ((base (or data-lens--base-query
                   data-lens--last-query))
         (current data-lens--where-filter)
         (input (string-trim
                 (read-string
                  (if current
                      (format "WHERE filter (current: %s, empty to clear): "
                              current)
                    "WHERE filter (e.g., age > 18): ")
                  nil nil current)))
         (filtered-sql (unless (string-empty-p input)
                         (data-lens--apply-where base input))))
    (data-lens--execute (or filtered-sql base)
                                data-lens-connection)
    (setq data-lens--base-query (when filtered-sql base))
    (setq data-lens--where-filter (when filtered-sql input))
    (message (if filtered-sql
                 (format "Filter applied: WHERE %s" input)
               "Filter cleared"))))

;;;; Yank cell / Copy row as INSERT

(defun data-lens-result-yank-cell ()
  "Copy the full value of the cell at point to the kill ring."
  (interactive)
  (let* ((cell (or (data-lens-result--cell-at-point)
                   (user-error "No cell at point")))
         (text (data-lens--format-value (nth 2 cell))))
    (kill-new text)
    (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…"))))

(defun data-lens-result--select-columns ()
  "Prompt user to select columns via `completing-read-multiple'."
  (let* ((col-names data-lens--result-columns)
         (chosen (completing-read-multiple "Columns: " col-names nil t)))
    (or (cl-loop for name in chosen
                 for idx = (cl-position name col-names :test #'string=)
                 when idx collect idx)
        (user-error "No valid columns selected"))))

(defun data-lens-result-copy-row-as-insert (&optional select-cols)
  "Copy row(s) as INSERT statement(s) to the kill ring.
Rows: marked > region > current.
With prefix arg SELECT-COLS, prompt to choose columns."
  (interactive "P")
  (let* ((indices (or (data-lens-result--marked-row-indices)
                      (when (use-region-p)
                        (data-lens-result--rows-in-region
                         (region-beginning) (region-end)))
                      (user-error "No row at point")))
         (col-indices (if select-cols
                          (data-lens-result--select-columns)
                        (cl-loop for i below (length data-lens--result-columns)
                                 collect i)))
         (col-names (mapcar (lambda (i) (nth i data-lens--result-columns))
                            col-indices))
         (table (or (data-lens-result--detect-table) "TABLE"))
         (rows data-lens--result-rows)
         (conn data-lens-connection)
         (cols (mapconcat (lambda (c)
                            (data-lens-db-escape-identifier conn c))
                          col-names ", "))
         (stmts (cl-loop for ridx in indices
                         for row = (nth ridx rows)
                         for vals = (mapcar (lambda (i) (nth i row)) col-indices)
                         collect (format "INSERT INTO %s (%s) VALUES (%s);"
                                         (data-lens-db-escape-identifier conn table)
                                         cols
                                         (mapconcat #'data-lens--value-to-literal
                                                    vals ", ")))))
    (kill-new (mapconcat #'identity stmts "\n"))
    (message "Copied %d INSERT statement%s (%d col%s)"
             (length stmts) (if (= (length stmts) 1) "" "s")
             (length col-names) (if (= (length col-names) 1) "" "s"))))

(defun data-lens-result-copy-as-csv (&optional select-cols)
  "Copy row(s) as CSV to the kill ring.
Rows: marked > region > current.
With prefix arg SELECT-COLS, prompt to choose columns.
Includes a header row with column names."
  (interactive "P")
  (let* ((indices (or (data-lens-result--marked-row-indices)
                      (when (use-region-p)
                        (data-lens-result--rows-in-region
                         (region-beginning) (region-end)))
                      (user-error "No row at point")))
         (col-indices (if select-cols
                          (data-lens-result--select-columns)
                        (cl-loop for i below (length data-lens--result-columns)
                                 collect i)))
         (col-names (mapcar (lambda (i) (nth i data-lens--result-columns))
                            col-indices))
         (rows data-lens--result-rows)
         (csv-escape (lambda (val)
                       (let ((s (data-lens--format-value val)))
                         (if (string-match-p "[,\"\n]" s)
                             (format "\"%s\"" (replace-regexp-in-string
                                              "\"" "\"\"" s))
                           s))))
         (lines (cons (mapconcat #'identity col-names ",")
                      (cl-loop for ridx in indices
                               for row = (nth ridx rows)
                               for vals = (mapcar (lambda (i) (nth i row))
                                                  col-indices)
                               collect (mapconcat csv-escape vals ",")))))
    (kill-new (mapconcat #'identity lines "\n"))
    (message "Copied %d row%s as CSV (%d col%s)"
             (length indices) (if (= (length indices) 1) "" "s")
             (length col-names) (if (= (length col-names) 1) "" "s"))))

(defun data-lens-result--goto-col-idx (col-idx)
  "Move point to the first data cell matching COL-IDX in the buffer."
  (goto-char (point-min))
  (when-let* ((found (text-property-search-forward 'data-lens-col-idx col-idx #'eq)))
    (goto-char (prop-match-beginning found))))

(defun data-lens-result-goto-column ()
  "Jump to the column page containing a specific column."
  (interactive)
  (unless data-lens--result-columns
    (user-error "No result columns"))
  (let* ((col-names data-lens--result-columns)
         (choice (completing-read "Go to column: " col-names nil t))
         (idx (cl-position choice col-names :test #'string=)))
    (when idx
      (let ((target-page nil))
        (if (memq idx data-lens--pinned-columns)
            (setq target-page data-lens--current-col-page)
          (cl-loop for pi from 0 below (length data-lens--column-pages)
                   when (cl-find idx (aref data-lens--column-pages pi))
                   do (setq target-page pi)))
        (if (null target-page)
            (user-error "Column %s not found in pages" choice)
          (unless (= target-page data-lens--current-col-page)
            (setq data-lens--current-col-page target-page)
            (data-lens--render-result))
          (data-lens-result--goto-col-idx idx))))))

(defun data-lens-result-export ()
  "Export the current result.
Prompts for format: csv (new buffer) or copy (kill ring)."
  (interactive)
  (let ((fmt (completing-read "Export format: " '("csv" "copy") nil t)))
    (pcase fmt
      ("csv" (data-lens--export-csv))
      ("copy"
       (kill-ring-save (point-min) (point-max))
       (message "Buffer content copied to kill ring")))))

(defun data-lens--export-csv ()
  "Export the current result as CSV into a new buffer.
Generates CSV directly from cached data."
  (let* ((col-names data-lens--result-columns)
         (rows data-lens--result-rows)
         (csv-escape (lambda (val)
                       (let ((s (data-lens--format-value val)))
                         (if (string-match-p "[,\"\n]" s)
                             (format "\"%s\""
                                     (replace-regexp-in-string "\"" "\"\"" s))
                           s))))
         (csv-buf (generate-new-buffer "*data-lens: export.csv*")))
    (with-current-buffer csv-buf
      (insert (mapconcat #'identity col-names ",") "\n")
      (dolist (row rows)
        (insert (mapconcat csv-escape row ",") "\n"))
      (goto-char (point-min)))
    (pop-to-buffer csv-buf)))

;;;; Column page navigation and width adjustment

(defun data-lens-result-next-col-page ()
  "Switch to the next column page."
  (interactive)
  (let ((max-page (1- (length data-lens--column-pages))))
    (if (>= data-lens--current-col-page max-page)
        (user-error "Already on last column page")
      (cl-incf data-lens--current-col-page)
      (data-lens--render-result))))

(defun data-lens-result-prev-col-page ()
  "Switch to the previous column page."
  (interactive)
  (if (<= data-lens--current-col-page 0)
      (user-error "Already on first column page")
    (cl-decf data-lens--current-col-page)
    (data-lens--render-result)))

(defun data-lens-result-widen-column ()
  "Widen the column at point by `data-lens-column-width-step'."
  (interactive)
  (if-let* ((cidx (data-lens--col-idx-at-point)))
      (progn
        (cl-incf (aref data-lens--column-widths cidx)
                 data-lens-column-width-step)
        (data-lens--refresh-display))
    (user-error "No column at point")))

(defun data-lens-result-narrow-column ()
  "Narrow the column at point by `data-lens-column-width-step'."
  (interactive)
  (if-let* ((cidx (data-lens--col-idx-at-point)))
      (let ((new-w (max 5 (- (aref data-lens--column-widths cidx)
                              data-lens-column-width-step))))
        (aset data-lens--column-widths cidx new-w)
        (data-lens--refresh-display))
    (user-error "No column at point")))

(defun data-lens-result-pin-column ()
  "Pin the column at point so it appears on all column pages."
  (interactive)
  (if-let* ((cidx (data-lens--col-idx-at-point)))
      (progn
        (cl-pushnew cidx data-lens--pinned-columns)
        (data-lens--refresh-display)
        (message "Pinned column %s" (nth cidx data-lens--result-columns)))
    (user-error "No column at point")))

(defun data-lens-result-unpin-column ()
  "Unpin the column at point."
  (interactive)
  (if-let* ((cidx (data-lens--col-idx-at-point)))
      (progn
        (setq data-lens--pinned-columns
              (delq cidx data-lens--pinned-columns))
        (data-lens--refresh-display)
        (message "Unpinned column %s" (nth cidx data-lens--result-columns)))
    (user-error "No column at point")))

;;;; Fullscreen toggle

(defvar-local data-lens--pre-fullscreen-config nil
  "Window configuration saved before entering fullscreen.")

(defun data-lens-result-fullscreen-toggle ()
  "Toggle fullscreen display for the result buffer.
Expands the result buffer to fill the frame, or restores the
previous window layout."
  (interactive)
  (if data-lens--pre-fullscreen-config
      (progn
        (set-window-configuration data-lens--pre-fullscreen-config)
        (setq data-lens--pre-fullscreen-config nil)
        (message "Restored window layout"))
    (setq data-lens--pre-fullscreen-config
          (current-window-configuration))
    (delete-other-windows)
    (data-lens--refresh-display)
    (message "Fullscreen (press F again to restore)")))

;;;; Record buffer

(defvar data-lens-record-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'data-lens-record-toggle-expand)
    (define-key map (kbd "C-c '") #'data-lens-record-edit-field)
    (define-key map "n" #'data-lens-record-next-row)
    (define-key map "p" #'data-lens-record-prev-row)
    (define-key map "y" #'data-lens-record-yank-field)
    (define-key map "w" #'data-lens-record-copy-as-insert)
    (define-key map "q" #'quit-window)
    (define-key map "g" #'data-lens-record-refresh)
    (define-key map "?" #'data-lens-record-dispatch)
    map)
  "Keymap for `data-lens-record-mode'.")

(define-derived-mode data-lens-record-mode special-mode "DB-Record"
  "Mode for displaying a single database row in detail.

\\<data-lens-record-mode-map>
  \\[data-lens-record-toggle-expand]	Expand/collapse field or follow FK
  \\[data-lens-record-edit-field]	Edit field
  \\[data-lens-record-next-row]	Next row
  \\[data-lens-record-prev-row]	Previous row
  \\[data-lens-record-yank-field]	Copy field value
  \\[data-lens-record-copy-as-insert]	Copy row as INSERT
  \\[data-lens-record-refresh]	Refresh"
  (setq truncate-lines nil))

(defun data-lens-result-open-record ()
  "Open a Record buffer showing the row at point."
  (interactive)
  (let* ((ridx (or (data-lens-result--row-idx-at-line)
                    (user-error "No row at point")))
         (result-buf (current-buffer))
         (buf-name (format "*data-lens-record: row %d*" ridx))
         (buf (get-buffer-create buf-name)))
    (with-current-buffer buf
      (data-lens-record-mode)
      (setq-local data-lens-record--result-buffer result-buf)
      (setq-local data-lens-record--row-idx ridx)
      (setq-local data-lens-record--expanded-fields nil)
      (data-lens-record--render))
    (pop-to-buffer buf '(display-buffer-at-bottom))))

(defun data-lens-record--render-field (name cidx val col-def ridx edits fk-info
                                        expanded-fields max-name-w)
  "Insert one field line for column NAME at CIDX.
VAL is the cell value, COL-DEF the column metadata, RIDX the row index.
EDITS, FK-INFO, EXPANDED-FIELDS provide edit/FK/expand state.
MAX-NAME-W is the label column width."
  (let* ((edited (assoc (cons ridx cidx) edits))
         (display-val (if edited (cdr edited) val))
         (long-p (data-lens--long-field-type-p col-def))
         (expanded-p (memq cidx expanded-fields))
         (fk (cdr (assq cidx fk-info)))
         (formatted (data-lens--format-value display-val))
         (display (if (and long-p (not expanded-p) (> (length formatted) 80))
                      (concat (substring formatted 0 80) "…")
                    formatted))
         (face (cond (edited 'data-lens-modified-face)
                     ((null val) 'data-lens-null-face)
                     (fk 'data-lens-fk-face)
                     (t nil))))
    (insert (propertize (data-lens--string-pad name max-name-w)
                        'face 'data-lens-header-face)
            (propertize " : " 'face 'data-lens-border-face)
            (propertize display
                        'data-lens-row-idx ridx
                        'data-lens-col-idx cidx
                        'data-lens-full-value (if edited (cdr edited) val)
                        'face face)
            "\n")))

(defun data-lens-record--render ()
  "Render the current row in the Record buffer."
  (unless (buffer-live-p data-lens-record--result-buffer)
    (user-error "Result buffer no longer exists"))
  (let* ((result-buf data-lens-record--result-buffer)
         (ridx data-lens-record--row-idx)
         (col-names (buffer-local-value 'data-lens--result-columns result-buf))
         (col-defs (buffer-local-value 'data-lens--result-column-defs result-buf))
         (rows (buffer-local-value 'data-lens--result-rows result-buf))
         (fk-info (buffer-local-value 'data-lens--fk-info result-buf))
         (edits (buffer-local-value 'data-lens--pending-edits result-buf))
         (inhibit-read-only t))
    (unless (< ridx (length rows))
      (user-error "Row %d no longer exists" ridx))
    (erase-buffer)
    (setq header-line-format
          (propertize (format " Record: row %d/%d" (1+ ridx) (length rows))
                      'face 'data-lens-header-face))
    (let* ((row (nth ridx rows))
           (max-name-w (apply #'max (mapcar #'string-width col-names))))
      (cl-loop for name in col-names
               for cidx from 0
               do (data-lens-record--render-field
                   name cidx (nth cidx row) (nth cidx col-defs)
                   ridx edits fk-info data-lens-record--expanded-fields
                   max-name-w)))
    (goto-char (point-min))))

(defun data-lens-record-toggle-expand ()
  "Toggle expand/collapse for long fields, or follow FK."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'data-lens-col-idx))
            (ridx (get-text-property (point) 'data-lens-row-idx)))
      (let* ((result-buf data-lens-record--result-buffer)
             (fk-info (buffer-local-value 'data-lens--fk-info result-buf))
             (fk (cdr (assq cidx fk-info)))
             (col-defs (buffer-local-value 'data-lens--result-column-defs result-buf))
             (col-def (nth cidx col-defs))
             (val (get-text-property (point) 'data-lens-full-value)))
        (cond
         (fk
          (when (null val)
            (user-error "NULL value — cannot follow"))
          (with-current-buffer result-buf
            (data-lens--execute
             (let ((c (buffer-local-value 'data-lens-connection result-buf)))
               (format "SELECT * FROM %s WHERE %s = %s"
                       (data-lens-db-escape-identifier
                        c (plist-get fk :ref-table))
                       (data-lens-db-escape-identifier
                        c (plist-get fk :ref-column))
                       (data-lens--value-to-literal val)))
             data-lens-connection)))
         ((data-lens--long-field-type-p col-def)
          (if (memq cidx data-lens-record--expanded-fields)
              (setq data-lens-record--expanded-fields
                    (delq cidx data-lens-record--expanded-fields))
            (push cidx data-lens-record--expanded-fields))
          (data-lens-record--render))
         (t
          (message "%s" (data-lens--format-value val)))))
    (user-error "No field at point")))

(defun data-lens-record-edit-field ()
  "Edit the field at point in a dedicated buffer."
  (interactive)
  (if-let* ((cidx (get-text-property (point) 'data-lens-col-idx)))
      (let* ((ridx data-lens-record--row-idx)
             (result-buf data-lens-record--result-buffer)
             (col-names (buffer-local-value 'data-lens--result-columns result-buf))
             (col-name (nth cidx col-names))
             (val (get-text-property (point) 'data-lens-full-value))
             (record-buf (current-buffer))
             (edit-buf (get-buffer-create
                        (format "*data-lens-edit: [%d].%s*" ridx col-name))))
        (with-current-buffer edit-buf
          (erase-buffer)
          (insert (data-lens--format-value val))
          (goto-char (point-min))
          (data-lens-result-edit-mode 1)
          (setq-local header-line-format
                      (format " Editing row %d, column \"%s\"  |  C-c C-c: accept  C-c C-k: cancel"
                              ridx col-name))
          (setq-local data-lens-result--edit-callback
                      (lambda (new-value)
                        (with-current-buffer result-buf
                          (data-lens-result--apply-edit ridx cidx new-value))
                        (when (buffer-live-p record-buf)
                          (with-current-buffer record-buf
                            (data-lens-record--render))))))
        (pop-to-buffer edit-buf))
    (user-error "No field at point")))

(defun data-lens-record-next-row ()
  "Show the next row in the Record buffer."
  (interactive)
  (let ((total (with-current-buffer data-lens-record--result-buffer
                 (length data-lens--result-rows))))
    (if (>= (1+ data-lens-record--row-idx) total)
        (user-error "Already at last row")
      (cl-incf data-lens-record--row-idx)
      (setq data-lens-record--expanded-fields nil)
      (data-lens-record--render))))

(defun data-lens-record-prev-row ()
  "Show the previous row in the Record buffer."
  (interactive)
  (if (<= data-lens-record--row-idx 0)
      (user-error "Already at first row")
    (cl-decf data-lens-record--row-idx)
    (setq data-lens-record--expanded-fields nil)
    (data-lens-record--render)))

(defun data-lens-record-yank-field ()
  "Copy the field value at point to the kill ring."
  (interactive)
  (if-let* ((val (get-text-property (point) 'data-lens-full-value)))
      (let ((text (data-lens--format-value val)))
        (kill-new text)
        (message "Copied: %s" (truncate-string-to-width text 60 nil nil "…")))
    (user-error "No field at point")))

(defun data-lens-record-copy-as-insert ()
  "Copy the current record row as an INSERT statement."
  (interactive)
  (let ((ridx data-lens-record--row-idx)
        (result-buf data-lens-record--result-buffer))
    (with-current-buffer result-buf
      (let* ((table (or (data-lens-result--detect-table) "TABLE"))
             (col-names data-lens--result-columns)
             (row (nth ridx data-lens--result-rows))
             (conn (buffer-local-value 'data-lens-connection result-buf))
             (cols (mapconcat (lambda (c)
                                (data-lens-db-escape-identifier conn c))
                              col-names ", "))
             (vals (mapconcat #'data-lens--value-to-literal row ", ")))
        (kill-new (format "INSERT INTO %s (%s) VALUES (%s);"
                          (data-lens-db-escape-identifier conn table) cols vals))
        (message "Copied INSERT statement")))))

(defun data-lens-record-refresh ()
  "Refresh the Record buffer."
  (interactive)
  (data-lens-record--render))

;;;; REPL mode

(defvar data-lens-repl-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map comint-mode-map)
    (define-key map (kbd "C-c C-e") #'data-lens-connect)
    (define-key map (kbd "C-c C-t") #'data-lens-list-tables)
    (define-key map (kbd "C-c C-d") #'data-lens-describe-table-at-point)
    map)
  "Keymap for `data-lens-repl-mode'.")

(defvar-local data-lens-repl--pending-input ""
  "Accumulated partial SQL input waiting for a semicolon.")

(define-derived-mode data-lens-repl-mode comint-mode "DB-REPL"
  "Major mode for database REPL.

\\<data-lens-repl-mode-map>
  \\[data-lens-connect]	Connect to server
  \\[data-lens-list-tables]	List tables
  \\[data-lens-describe-table-at-point]	Describe table at point"
  (setq comint-prompt-regexp "^db> \\|^    -> ")
  (setq comint-input-sender #'data-lens-repl--input-sender)
  (add-hook 'completion-at-point-functions
            #'data-lens-completion-at-point nil t))

(defun data-lens-repl--input-sender (_proc input)
  "Process INPUT from comint.
Accumulates input until a semicolon is found, then executes."
  (let ((combined (concat data-lens-repl--pending-input
                          (unless (string-empty-p data-lens-repl--pending-input) "\n")
                          input)))
    (if (string-match-p ";\\s-*$" combined)
        ;; Complete statement — execute
        (progn
          (setq data-lens-repl--pending-input "")
          (data-lens-repl--execute-and-print (string-trim combined)))
      ;; Incomplete — accumulate and show continuation prompt
      (setq data-lens-repl--pending-input combined)
      (data-lens-repl--output "    -> "))))

(defun data-lens-repl--output (text)
  "Insert TEXT into the REPL buffer at the process mark."
  (let ((inhibit-read-only t)
        (proc (get-buffer-process (current-buffer))))
    (goto-char (process-mark proc))
    (insert text)
    (set-marker (process-mark proc) (point))))

(defun data-lens-repl--format-dml-result (result elapsed)
  "Format a DML RESULT with ELAPSED time as a string for the REPL."
  (let ((msg (format "\nAffected rows: %s"
                     (or (data-lens-db-result-affected-rows result) 0))))
    (when-let* ((id (data-lens-db-result-last-insert-id result))
                ((> id 0)))
      (setq msg (concat msg (format ", Last insert ID: %s" id))))
    (when-let* ((w (data-lens-db-result-warnings result))
                ((> w 0)))
      (setq msg (concat msg (format ", Warnings: %s" w))))
    (format "%s (%.3fs)\n\ndb> " msg elapsed)))

(defun data-lens-repl--execute-and-print (sql)
  "Execute SQL and print results inline in the REPL."
  (if (not (data-lens--connection-alive-p data-lens-connection))
      (data-lens-repl--output "ERROR: Not connected.  Use C-c C-e to connect.\ndb> ")
    (data-lens--add-history sql)
    (setq data-lens--last-query sql)
    (condition-case err
        (let* ((start (float-time))
               (result (data-lens-db-query data-lens-connection sql))
               (elapsed (- (float-time) start))
               (columns (data-lens-db-result-columns result))
               (rows (data-lens-db-result-rows result)))
          (if columns
              (let* ((col-names (data-lens--column-names columns))
                     (table-str (data-lens--render-static-table
                                 col-names rows columns)))
                (data-lens-repl--output
                 (format "\n%s\n%d row%s in %.3fs\n\ndb> "
                         table-str (length rows)
                         (if (= (length rows) 1) "" "s")
                         elapsed)))
            (data-lens-repl--output (data-lens-repl--format-dml-result result elapsed))))
      (data-lens-db-error
       (data-lens-repl--output
        (format "\nERROR: %s\n\ndb> " (error-message-string err)))))))

;;;###autoload
(defun data-lens-repl ()
  "Start a database REPL buffer."
  (interactive)
  (let* ((buf-name "*DataLens REPL*")
         (buf (get-buffer-create buf-name)))
    (unless (comint-check-proc buf)
      (with-current-buffer buf
        ;; Start a dummy process for comint
        (let ((proc (start-process "data-lens-repl" buf "cat")))
          (set-process-query-on-exit-flag proc nil)
          (data-lens-repl-mode)
          (data-lens-repl--output "db> "))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

;;;; Transient dispatch menus

;;;###autoload
(transient-define-prefix data-lens-dispatch ()
  "Main dispatch menu for data-lens."
  [["Connection"
    ("c" "Connect"    data-lens-connect)
    ("d" "Disconnect" data-lens-disconnect)
    ("R" "REPL"       data-lens-repl)]
   ["Execute"
    ("x" "Query at point" data-lens-execute-query-at-point)
    ("r" "Region"         data-lens-execute-region)
    ("b" "Buffer"         data-lens-execute-buffer)]
   ["Edit / History"
    ("'" "Indirect edit"  data-lens-edit-indirect)
    ("l" "History"        data-lens-show-history)]
   ["Schema"
    ("t" "List tables"    data-lens-list-tables)
    ("D" "Describe table" data-lens-describe-table-at-point)]])

(transient-define-prefix data-lens-result-dispatch ()
  "Dispatch menu for data-lens result buffer."
  [["Navigate"
    ("TAB" "Next cell"     data-lens-result-next-cell)
    ("<backtab>" "Prev cell" data-lens-result-prev-cell)
    ("M-n" "Down cell"     data-lens-result-down-cell)
    ("M-p" "Up cell"       data-lens-result-up-cell)
    ("RET" "Open record"   data-lens-result-open-record)
    ("c" "Go to column"    data-lens-result-goto-column)]
   ["Mark"
    ("m" "Toggle mark"     data-lens-result-toggle-mark)
    ("u" "Unmark row"      data-lens-result-unmark-row)
    ("U" "Unmark all"      data-lens-result-unmark-all)]
   ["Pages"
    ("n" "Next page"       data-lens-result-next-page)
    ("p" "Prev page"       data-lens-result-prev-page)
    ("M-<" "First page"    data-lens-result-first-page)
    ("M->" "Last page"     data-lens-result-last-page)
    ("#" "Count total"     data-lens-result-count-total)
    ("]" "Next col page"   data-lens-result-next-col-page)
    ("[" "Prev col page"   data-lens-result-prev-col-page)]
   ["Filter / Sort"
    ("W" "WHERE filter"    data-lens-result-apply-filter)
    ("s" "Sort ASC"        data-lens-result-sort-by-column)
    ("S" "Sort DESC"       data-lens-result-sort-by-column-desc)]]
  [["Edit"
    ("C-c '" "Edit cell"   data-lens-result-edit-cell)
    ("C-c C-c" "Commit"    data-lens-result-commit)]
   ["Copy (C-u = select cols)"
    ("y" "Yank cell"       data-lens-result-yank-cell)
    ("w" "Row(s) INSERT"   data-lens-result-copy-row-as-insert)
    ("Y" "Row(s) CSV"      data-lens-result-copy-as-csv)
    ("e" "Export"           data-lens-result-export)]
   ["Other"
    ("=" "Widen column"    data-lens-result-widen-column)
    ("-" "Narrow column"   data-lens-result-narrow-column)
    ("C-c p" "Pin column"  data-lens-result-pin-column)
    ("C-c P" "Unpin column" data-lens-result-unpin-column)
    ("g" "Re-execute"      data-lens-result-rerun)
    ("F" "Fullscreen"      data-lens-result-fullscreen-toggle)]])

(transient-define-prefix data-lens-record-dispatch ()
  "Dispatch menu for data-lens record buffer."
  [["Navigate"
    ("n" "Next row"     data-lens-record-next-row)
    ("p" "Prev row"     data-lens-record-prev-row)
    ("RET" "Expand/FK"  data-lens-record-toggle-expand)]
   ["Edit"
    ("C-c '" "Edit field" data-lens-record-edit-field)]
   ["Copy"
    ("y" "Yank field"      data-lens-record-yank-field)
    ("w" "Row as INSERT"   data-lens-record-copy-as-insert)]
   ["Other"
    ("g" "Refresh" data-lens-record-refresh)
    ("q" "Quit"    quit-window)]])

(provide 'data-lens)
;;; data-lens.el ends here
