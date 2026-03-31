;;; clutch-query.el --- Query execution and result display for clutch -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
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

;; Query console, value formatting, column-width computation, pagination,
;; query execution engine, error humanization, error details, and indirect
;; edit buffers for clutch.
;;
;; This module is required by `clutch.el' — do not require `clutch' here.

;;; Code:

(require 'clutch-db)
(require 'clutch-connection)
(require 'cl-lib)

;; Forward declarations — variables defined in clutch.el
(defvar clutch-connection)
(defvar clutch--buffer-error-details)
(defvar clutch--executing-p)
(defvar clutch--error-position-overlay)
(defvar clutch--error-banner-overlay)
(defvar clutch--conn-sql-product)
(defvar clutch--last-query)
(defvar clutch--base-query)
(defvar clutch--connection-params)
(defvar clutch--console-name)
(defvar clutch--source-window)
(defvar clutch--executing-sql-start)
(defvar clutch--executing-sql-end)
(defvar clutch-debug-mode nil)
(defvar clutch-debug-buffer-name "*clutch-debug*")
(defvar clutch-connection-alist nil)
(defvar clutch-result-window-height 0.33)
(defvar clutch-result-max-rows 500)
(defvar clutch-column-width-max 30)
(defvar clutch-column-padding 1)
(defvar clutch-console-yank-cleanup t)
(defvar clutch-console-directory
  (expand-file-name "clutch" user-emacs-directory))

;; Forward declarations — variables defined in clutch-ui / clutch-edit
(defvar clutch--aggregate-summary)
(defvar clutch--cached-pk-indices)
(defvar clutch--column-widths)
(defvar clutch--dml-result)
(defvar clutch--filter-pattern)
(defvar clutch--filtered-rows)
(defvar clutch--fk-info)
(defvar clutch--marked-rows)
(defvar clutch--order-by)
(defvar clutch--page-current)
(defvar clutch--page-total-rows)
(defvar clutch--pending-deletes)
(defvar clutch--pending-edits)
(defvar clutch--pending-inserts)
(defvar clutch--query-elapsed)
(defvar clutch--result-column-defs)
(defvar clutch--result-columns)
(defvar clutch--result-rows)
(defvar clutch--sort-column)
(defvar clutch--sort-descending)
(defvar clutch--where-filter)

;; Forward declarations — functions defined in clutch.el
(declare-function clutch--console-buffer-base-name "clutch" (name))
(declare-function clutch--debug-sql-preview "clutch" (sql))
(declare-function clutch--remember-debug-event "clutch" (&rest event))
(declare-function clutch--remember-problem-record "clutch" (&rest args))
(declare-function clutch--find-console-buffer "clutch" (name))
(declare-function clutch--update-console-buffer-name "clutch" ())
(declare-function clutch--console-file "clutch" (name))
(declare-function clutch--effective-sql-product "clutch" (params))
(declare-function clutch--set-schema-status "clutch" (conn state &optional table-count error-message))
(declare-function clutch--sql-rewrite "clutch" (sql op &optional arg))

;; Forward declarations — functions from clutch-connection.el
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--ensure-clutch-loaded "clutch-connection" ())
(declare-function clutch--connection-oracle-jdbc-p "clutch-connection" (conn))
(declare-function clutch--clear-tx-dirty "clutch-connection" (conn))
(declare-function clutch--run-db-query "clutch-connection" (conn sql))
(declare-function clutch--connection-alive-p "clutch-connection" (conn))
(declare-function clutch--bind-connection-context "clutch-connection" (conn &optional params product))
(declare-function clutch--activate-current-buffer-connection "clutch-connection" (conn params &optional product))
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--spinner-start "clutch-connection" ())
(declare-function clutch--update-mode-line "clutch-connection" ())
(declare-function clutch--build-conn "clutch-connection" (params))
(declare-function clutch--saved-connection-params "clutch-connection" (name))
;; clutch-result-mode and clutch-mode are defined in clutch.el which requires
;; clutch-query.el — we cannot require clutch here.
(declare-function clutch-result-mode "clutch" ())
(declare-function clutch-mode "clutch" ())

;; Forward declarations — functions from clutch-ui / clutch-edit
(declare-function clutch--mark-executed-sql-region "clutch-ui" (beg end))
(declare-function clutch--render-separator "clutch-ui" (visible-cols widths &optional position))
(declare-function clutch--render-header "clutch-ui" (visible-cols widths))
(declare-function clutch--build-render-state "clutch-ui" ())
(declare-function clutch--render-row "clutch-ui" (row ridx visible-cols widths render-state))
(declare-function clutch--refresh-display "clutch-ui" ())
(declare-function clutch--display-select-result "clutch-ui" (col-names rows columns))
(declare-function clutch--display-result "clutch-ui" (result sql elapsed))
(declare-function clutch-result--detect-primary-key "clutch-edit" ())
(declare-function clutch--load-fk-info "clutch-edit" ())
(declare-function clutch-result--build-pending-insert-statements "clutch-edit" ())
(declare-function clutch-result--build-update-statements "clutch-edit" ())
(declare-function clutch-result--build-pending-delete-statements "clutch-edit" ())
(declare-function clutch--refresh-schema-cache-async "clutch-schema" (conn))

;; Forward declarations — functions from clutch-db
(declare-function clutch-db-interrupt-query "clutch-db" (conn))

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

(defun clutch--read-query-console-name ()
  "Read a saved connection name for `clutch-query-console'."
  (clutch--ensure-clutch-loaded)
  (if clutch-connection-alist
      (completing-read "Console: "
                       (mapcar #'car clutch-connection-alist)
                       nil t)
    (user-error "No saved connections.  Populate `clutch-connection-alist' first")))

(defun clutch--console-yank-cleanup ()
  "Clean whitespace in the just-pasted region of a query console.
Only runs when `clutch-console-yank-cleanup' is non-nil, the current
buffer is a query console, and the last command was a yank variant."
  (when (and clutch-console-yank-cleanup
             clutch--console-name
             (memq this-command '(yank yank-pop clipboard-yank)))
    (let ((beg (region-beginning))
          (end (region-end)))
      (when (< beg end)
        (whitespace-cleanup-region beg end)))))

;;;###autoload
(defun clutch-query-console (name)
  "Open or switch to the query console for saved connection NAME.
Creates a dedicated buffer *clutch: NAME* with `clutch-mode' enabled
and connects automatically if not already connected.
Repeated calls with the same NAME switch to the existing buffer.
When called from outside a clutch buffer, reuses any visible clutch
window rather than replacing the current window."
  (interactive (list (clutch--read-query-console-name)))
  (let ((existing (clutch--find-console-buffer name)))
    (if (and existing
             (buffer-local-value 'clutch-connection existing)
             (clutch--connection-alive-p
              (buffer-local-value 'clutch-connection existing)))
        (progn
          (select-window
           (or (clutch--console-window-for existing) (selected-window)))
          (switch-to-buffer existing))
      (let* ((params (or (clutch--saved-connection-params name)
                         (user-error "No saved connection named %s" name)))
             (product (clutch--effective-sql-product params))
             (conn (if (and existing
                            (buffer-local-value 'clutch-connection existing)
                            (not (clutch--connection-alive-p
                                  (buffer-local-value 'clutch-connection existing))))
                       (with-current-buffer existing
                         (clutch--build-conn params))
                     (clutch--build-conn params)))
             (buf (or existing
                      (get-buffer-create (clutch--console-buffer-base-name name))))
             (is-new (zerop (buffer-size buf))))
        (select-window (or (clutch--console-window-for buf) (selected-window)))
        (switch-to-buffer buf)
        (unless (eq major-mode 'clutch-mode)
          (clutch-mode))
        (setq-local clutch--console-name name)
        (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
        (when is-new
          (let ((coding-system-for-read 'utf-8)
                (file (clutch--console-file name)))
            (when (file-readable-p file)
              (insert-file-contents file))))
        (clutch--activate-current-buffer-connection conn params product)
        (clutch--update-console-buffer-name)))))

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
nil → \"NULL\", :false → \"false\", plists → formatted date/time strings,
hash-tables and vectors (JSON from MySQL/PG) → JSON string."
  (cond
   ((null val) "NULL")
   ((eq val :false) "false")
   ((stringp val) val)
   ((numberp val) (number-to-string val))
   ((listp val) (or (clutch-db-format-temporal val) (format "%S" val)))
   ((or (hash-table-p val) (vectorp val))
    (condition-case nil (json-serialize val) (error (format "%S" val))))
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

(defun clutch--format-elapsed (seconds)
  "Format SECONDS as a human-readable duration."
  (if (< seconds 1.0)
      (format "%dms" (round (* seconds 1000)))
    (format "%.3fs" seconds)))

(defun clutch--json-false-value-p (val)
  "Return non-nil when VAL represents a parsed JSON false sentinel."
  (or (eq val :false)
      ;; JDBC uses a private false sentinel so it can distinguish JSON false
      ;; from SQL NULL when decoding metadata payloads.
      (and (symbolp val)
           (string= (symbol-name val) "clutch-jdbc-json-false"))))

(defun clutch--json-value-to-string (val)
  "Convert VAL to valid JSON text suitable for JSON editing and viewing."
  (cond
   ((null val)
    "null")
   ((and (stringp val)
         (fboundp 'json-serialize)
         (fboundp 'json-parse-string))
    (condition-case nil
        (json-serialize (json-parse-string val))
      (error (json-serialize val))))
   ((clutch--json-false-value-p val)
    "false")
   ((and (fboundp 'json-serialize)
         (or (numberp val)
             (eq val t)
             (hash-table-p val)
             (vectorp val)
             (listp val)))
    (condition-case nil
        (json-serialize (if (clutch--json-false-value-p val) :false val))
      (error (clutch--format-value val))))
   (t (clutch--format-value val))))

;;;; Column width computation and paging

(defun clutch--numeric-type-p (col-def)
  "Return non-nil if COL-DEF is a numeric column type."
  (eq (plist-get col-def :type-category) 'numeric))

(defun clutch--long-field-type-p (col-def)
  "Return non-nil if COL-DEF is a long field type (JSON/BLOB)."
  (memq (plist-get col-def :type-category) '(json blob)))

(defun clutch--json-like-string-p (val)
  "Return non-nil when string VAL appears to contain JSON text."
  (and (stringp val) (string-match-p "\\`\\s-*[{\\[]" val)))


(defun clutch--xml-like-string-p (val)
  "Return non-nil when string VAL appears to contain XML text.
Uses a stricter heuristic to avoid misclassifying plain \"<...\" text."
  (and (stringp val)
       (let* ((s (string-trim-left val))
              (body (if (string-match "\\`<\\?xml\\(?:.\\|\n\\)*?\\?>\\s-*\\(.*\\)\\'" s)
                        (match-string 1 s)
                      s))
              (open-re "\\`<\\([[:alpha:]_][[:alnum:]_.:-]*\\)\\(?:\\s-+[^>]*\\)?\\s-*\\(/>\\|>\\)"))
         (when (string-match open-re body)
           (let ((tag (match-string 1 body))
                 (close (match-string 2 body)))
             (if (equal close "/>")
                 (string-match-p "\\`<[^>]+/>\\s-*\\'" body)
               (string-match-p (format "</%s\\s-*>" (regexp-quote tag)) body)))))))

(defun clutch--value-placeholder (val col-def)
  "Return compact placeholder text for VAL/COL-DEF in result grid."
  (let ((cat (plist-get col-def :type-category)))
    (cond
     ((or (eq cat 'json) (clutch--json-like-string-p val))
      "<JSON>")
     ((clutch--xml-like-string-p val)
      "<XML>")
     ((eq cat 'blob)
      "<BLOB>")
     (t nil))))

(defun clutch--cell-placeholder-value (val)
  "Return display placeholder text for special cell VAL, or nil."
  (pcase val
    (:clutch-generated-placeholder "<generated>")
    (:clutch-default-placeholder "<default>")
    (_ nil)))

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

(defun clutch--visible-columns ()
  "Return the column indices rendered in the result buffer."
  (cl-loop for i below (length clutch--result-columns)
           collect i))

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
Uses the same visual style as the result renderer.
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
         (render-state (clutch--build-render-state))
         (lines nil))
    (push sep-top lines)
    (push header lines)
    (push sep-mid lines)
    (cl-loop for row in rows
             for ridx from 0
             do (push (clutch--render-row row ridx all-cols widths render-state) lines))
    (push sep-bot lines)
    (mapconcat #'identity (nreverse lines) "\n")))


;;;; SQL pagination helpers

(defun clutch--sql-has-limit-p (sql)
  "Return non-nil if SQL has a top-level LIMIT clause."
  (clutch-db-sql-has-top-level-limit-p sql))

(defun clutch--sql-has-page-tail-p (sql)
  "Return non-nil if SQL has a top-level LIMIT or OFFSET clause."
  (or (clutch-db-sql-has-top-level-limit-p sql)
      (clutch-db-sql-has-top-level-offset-p sql)))

(defun clutch--sql-derived-table-alias (alias &optional conn)
  "Return a derived-table alias clause for ALIAS on CONN.
Oracle does not accept AS before a subquery alias."
  (if (clutch--connection-oracle-jdbc-p (or conn clutch-connection))
      (replace-regexp-in-string "\\`_+" "" alias)
    (format "AS %s" alias)))

(defun clutch--build-paged-sql (base-sql page-num page-size &optional order-by)
  "Build a paged SQL query wrapping BASE-SQL.
PAGE-NUM is 0-based, PAGE-SIZE is the row limit.
ORDER-BY is a cons (COL-NAME . DIRECTION) or nil.
If BASE-SQL already has a top-level LIMIT/OFFSET, pagination and sorting
operate on that result set via an outer wrapper query."
  (let ((effective-base
         (if (clutch--sql-has-page-tail-p base-sql)
             (format "SELECT * FROM (%s) %s"
                     (string-trim-right
                      (replace-regexp-in-string ";\\s-*\\'" "" base-sql))
                     (clutch--sql-derived-table-alias "_clutch_page"))
           base-sql)))
    (clutch-db-build-paged-sql clutch-connection effective-base
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
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--query-elapsed elapsed
                clutch--filter-pattern nil
                clutch--filtered-rows nil
                clutch--column-widths
                (clutch--compute-column-widths col-names rows columns))))

(defun clutch--execute-page (page-num)
  "Execute the query for PAGE-NUM and refresh the result buffer display.
Uses the current effective result SQL, including active WHERE filters.
Signals an error if pagination is not available."
  (let* ((source-buffer (current-buffer))
         (effective-sql (clutch-result--effective-query)))
    (unless effective-sql
      (user-error "Pagination not available for this query"))
    (clutch--ensure-connection)
    (when (and (or clutch--pending-edits
                   clutch--pending-deletes
                   clutch--pending-inserts)
               (not (yes-or-no-p "Discard pending changes and change page? ")))
      (user-error "Page change cancelled"))
    (let* ((paged-sql (clutch--build-paged-sql
                       effective-sql page-num
                       clutch-result-max-rows clutch--order-by))
           (start (float-time))
           (result (condition-case err
                       (clutch--run-db-query clutch-connection paged-sql)
                     (clutch-db-error
                      (let* ((failure
                              (clutch--remember-execute-error
                               source-buffer
                               clutch-connection
                               effective-sql
                               err
                               (list :page-num page-num
                                     :paged-sql
                                     (clutch--debug-sql-preview paged-sql))))
                             (summary (cdr failure)))
                        (user-error "%s" (clutch--debug-workflow-message summary))))))
           (elapsed (- (float-time) start))
           (rows (clutch-db-result-rows result)))
      (clutch--update-page-state
       (clutch-db-result-columns result) rows elapsed page-num)
      (clutch--refresh-display)
      (message "Page %d loaded (%s, %d row%s)"
               (1+ page-num)
               (clutch--format-elapsed elapsed)
               (length rows)
               (if (= (length rows) 1) "" "s")))))

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

(defun clutch--schema-affecting-query-p (sql)
  "Return non-nil if SQL is likely to invalidate cached schema."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p "\\`\\(?:CREATE\\|ALTER\\|DROP\\|TRUNCATE\\|RENAME\\)\\b"
                    (upcase trimmed))))

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
  "Return non-nil when SQL is a top-level UPDATE/DELETE statement without WHERE."
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
  "Return non-nil if SQL returns a result set.
Matches SELECT, WITH, and introspection keywords (DESCRIBE, DESC,
SHOW, EXPLAIN) that also return tabular results.
Leading SQL comments are stripped before checking."
  (let ((trimmed (clutch--strip-leading-comments sql)))
    (string-match-p
     "\\`\\(?:SELECT\\|WITH\\|DESCRIBE\\|DESC\\|SHOW\\|EXPLAIN\\)\\b"
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
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
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

(defun clutch--query-debug-summary (result)
  "Return a compact summary string for RESULT."
  (if-let* ((rows (clutch-db-result-rows result)))
      (format "Returned %d row(s)" (length rows))
    (format "Affected %s row(s)"
            (or (clutch-db-result-affected-rows result) 0))))

(defun clutch--execute-select (sql connection)
  "Execute a SELECT SQL query with pagination on CONNECTION.
Returns the query result."
  (let* ((page-size clutch-result-max-rows)
         (paged-sql (clutch-db-build-paged-sql connection sql 0 page-size))
         (start (float-time))
         (source-buffer
          (if (window-live-p clutch--source-window)
              (window-buffer clutch--source-window)
            (current-buffer)))
         (_debug-start
          (when clutch-debug-mode
            (clutch--remember-debug-event
             :buffer source-buffer
             :connection connection
             :op "execute"
             :phase "start"
             :backend (clutch--backend-key-from-conn connection)
             :sql sql
             :context (list :paged-sql (clutch--debug-sql-preview paged-sql)))))
         (result (condition-case err
                     (clutch--run-db-query connection paged-sql)
                   (clutch-db-error
                    (let* ((failure (clutch--remember-execute-error
                                     source-buffer connection sql err))
                           (message (car failure))
                           (summary (cdr failure)))
                      (clutch--mark-sql-error source-buffer sql message)
                      (message "%s" (clutch--debug-workflow-message summary))
                      (throw 'clutch--execution-aborted nil)))))
         (elapsed (- (float-time) start))
         (buf (get-buffer-create (clutch--result-buffer-name)))
         (columns (clutch-db-result-columns result))
         (rows (clutch-db-result-rows result)))
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :buffer source-buffer
       :connection connection
       :op "execute"
       :phase "success"
       :backend (clutch--backend-key-from-conn connection)
       :summary (clutch--query-debug-summary result)
       :sql sql
       :elapsed elapsed))
    (with-current-buffer buf
      (clutch-result-mode)
      (let ((col-names (clutch--init-result-state
                        connection sql columns rows elapsed)))
        (setq clutch--cached-pk-indices (clutch-result--detect-primary-key))
        (clutch--load-fk-info)
        (when col-names
          (clutch--display-select-result col-names rows columns)))
      )
    (clutch--show-result-buffer buf)
    result))

(defun clutch--execute-dml (sql connection)
  "Execute a DML SQL query on CONNECTION and display results.
Returns the query result."
  (setq clutch--last-query sql)
  (let* ((start (float-time))
         (_debug-start
          (when clutch-debug-mode
            (clutch--remember-debug-event
             :connection connection
             :op "execute"
             :phase "start"
             :backend (clutch--backend-key-from-conn connection)
             :sql sql)))
         (result (condition-case err
                     (clutch--run-db-query connection sql)
                   (clutch-db-error
                    (let* ((source-buffer
                            (if (window-live-p clutch--source-window)
                                (window-buffer clutch--source-window)
                              (current-buffer)))
                           (failure (clutch--remember-execute-error
                                     source-buffer connection sql err))
                           (message (car failure))
                           (summary (cdr failure)))
                      (clutch--mark-sql-error source-buffer sql message)
                      (message "%s" (clutch--debug-workflow-message summary))
                      (throw 'clutch--execution-aborted nil)))))
         (elapsed (- (float-time) start)))
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :connection connection
       :op "execute"
       :phase "success"
       :backend (clutch--backend-key-from-conn connection)
       :summary (clutch--query-debug-summary result)
       :sql sql
       :elapsed elapsed))
    (when (clutch--schema-affecting-query-p sql)
      (if (clutch-db-eager-schema-refresh-p connection)
          (clutch--set-schema-status connection 'stale)
        (clutch--refresh-schema-cache-async connection)))
    (clutch--display-result result sql elapsed)
    result))

(defun clutch-result--effective-query ()
  "Return the effective SQL for the current result workflow.
Includes the active WHERE filter when one is applied."
  (let ((base (or clutch--base-query clutch--last-query)))
    (if (and base clutch--where-filter clutch--base-query)
        (clutch--apply-where base clutch--where-filter)
      base)))

(defun clutch--apply-where (sql filter)
  "Apply WHERE FILTER to SQL query string.
Wraps SQL as a derived table and applies FILTER in an outer WHERE.
This avoids brittle clause injection for CTE/UNION/subquery-heavy SQL."
  (clutch--sql-rewrite sql 'where filter))

(defun clutch--check-pending-changes ()
  "Prompt to discard pending changes in the result buffer, if any.
Signals `user-error' if the user declines."
  (when-let* ((result-buf (get-buffer (clutch--result-buffer-name))))
    (with-current-buffer result-buf
      (when (and (or clutch--pending-edits
                     clutch--pending-deletes
                     clutch--pending-inserts)
                 (not (yes-or-no-p "Discard pending changes and re-run query? ")))
        (user-error "Execution cancelled")))))

(defun clutch--abandon-query-connection (connection)
  "Drop CONNECTION after an unrecoverable query interruption."
  (when (clutch--connection-alive-p connection)
    (clutch-db-disconnect connection))
  (clutch--clear-tx-dirty connection)
  (when (eq connection clutch-connection)
    (setq clutch-connection nil)))

(defun clutch--handle-query-quit (connection)
  "Convert a raw quit on CONNECTION into an interrupt or disconnect."
  (let* ((source-buffer (current-buffer))
         (interrupted
          (condition-case err
              (and (clutch--connection-alive-p connection)
                   (clutch-db-interrupt-query connection))
            (clutch-db-error
             (let* ((msg (error-message-string err))
                    (summary (clutch--humanize-db-error msg)))
               (clutch--remember-buffer-query-error-details
                source-buffer connection nil err)
               (when clutch-debug-mode
                 (clutch--remember-debug-event
                  :buffer source-buffer
                  :connection connection
                  :op "cancel"
                  :phase "error"
                  :backend (clutch--backend-key-from-conn connection)
                  :summary summary))
               (message "Interrupt failed: %s"
                        (clutch--debug-workflow-message summary))
               nil)))))
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :connection connection
       :op "interrupt"
       :phase (if interrupted "success" "disconnect")
       :backend (clutch--backend-key-from-conn connection)
       :summary (if interrupted
                    "Interrupted running query without disconnecting"
                  "Interrupt recovery failed; connection abandoned")))
    (unless interrupted
      (clutch--abandon-query-connection connection))
    (signal 'clutch-query-interrupted nil)))

(defun clutch--execute (sql &optional conn)
  "Execute SQL on CONN (or current buffer connection).
Times execution and displays results.
For SELECT queries, applies pagination (LIMIT/OFFSET).
Prompts for confirmation on destructive operations."
  (clutch--ensure-connection)
  (clutch--clear-error-position-overlay)
  (setq-local clutch--buffer-error-details nil)
  (clutch--check-pending-changes)
  (let ((connection (or conn clutch-connection))
        (source-win (selected-window)))
    (when (clutch--destructive-query-p sql)
      (unless (yes-or-no-p
               (format "Execute destructive query?\n  %s\n\nProceed? "
                       (truncate-string-to-width (string-trim sql) 80)))
        (user-error "Query cancelled")))
    (clutch--require-risky-dml-confirmation sql)
    (setq clutch--executing-p t)
    (clutch--spinner-start)
    (clutch--update-mode-line)
    (redisplay t)
    (unwind-protect
        (condition-case nil
            (catch 'clutch--execution-aborted
              (let ((clutch--source-window source-win))
                (if (clutch--select-query-p sql)
                    (clutch--execute-select sql connection)
                  (clutch--execute-dml sql connection))))
          (quit
           (clutch--handle-query-quit connection)))
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

(defun clutch--sql-line-column-to-position (sql line column)
  "Return 1-based character position in SQL for LINE and COLUMN."
  (when (and (stringp sql)
             (integerp line) (> line 0)
             (integerp column) (> column 0))
    (with-temp-buffer
      (insert sql)
      (goto-char (point-min))
      (forward-line (1- line))
      (move-to-column (1- column))
      (1+ (- (point) (point-min))))))

;;;; Error humanization

(defconst clutch--db-error-hints
  '(;; ClickHouse
    ("Lightweight updates are not supported"
     . "enable lightweight update: ALTER TABLE ... MODIFY SETTING enable_block_number_column = 1")
    ("Lightweight deletes? \\(?:is\\|are\\) not supported"
     . "enable lightweight delete: SET allow_experimental_lightweight_delete = 1")
    ;; Oracle
    ("No suitable driver found for jdbc:oracle:"
     . "Oracle JDBC driver not installed; run M-x clutch-jdbc-install-driver RET oracle")
    ("ORA-00942" . "table or view does not exist; check name and privileges")
    ("ORA-01031" . "insufficient privileges")
    ("ORA-00904" . "invalid column name")
    ;; MySQL
    ("Access denied for user" . "wrong username or password")
    ("Unknown column" . "column does not exist; check spelling")
    ;; PostgreSQL
    ("relation .* does not exist" . "table does not exist; check schema and name")
    ("permission denied" . "insufficient privileges")
    ;; General
    ("Connection refused" . "cannot connect; check host and port")
    ("connect timed out\\|connection timeout" . "connection timed out; check network and firewall"))
  "Alist of (REGEX . HINT) for known database error patterns.")

(defun clutch--humanize-db-error (msg)
  "Return a user-friendly version of database error MSG.
Strips internal noise (queryId, version, stack traces) and appends
actionable hints for known error patterns."
  (let ((cleaned (or msg ""))
        (case-fold-search t))
    ;; Strip "Database error: " prefix
    (setq cleaned (replace-regexp-in-string
                   "\\`Database error: " "" cleaned))
    ;; Strip ClickHouse queryId suffix: (queryId= uuid-value)
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(queryId=[^)]*)" "" cleaned))
    ;; Strip ClickHouse version suffix: (version N.N.N (official build))
    (setq cleaned (replace-regexp-in-string
                   "[ \t]*(version [^)]*([^)]*)[^)]*)" "" cleaned))
    ;; Strip Java stack trace (from first "at " frame onward)
    (when (string-match "\n[ \t]*at " cleaned)
      (setq cleaned (substring cleaned 0 (match-beginning 0))))
    ;; Normalize whitespace
    (setq cleaned (string-trim
                   (replace-regexp-in-string "[[:space:]\n\r]+" " " cleaned)))
    ;; Look up hint
    (let ((hint (cl-loop for (pattern . h) in clutch--db-error-hints
                         when (string-match-p pattern cleaned)
                         return h)))
      (if hint
          (format "%s [%s]" cleaned hint)
        cleaned))))

(defun clutch--debug-workflow-message (message)
  "Return MESSAGE annotated with the single debug-buffer workflow."
  (if (or (not message)
          (string-match-p (regexp-quote clutch-debug-buffer-name) message)
          (string-match-p
           "Run M-x clutch-jdbc-\\(ensure-agent\\|install-driver\\)" message))
      message
    (if clutch-debug-mode
        (format "%s See %s for details." message clutch-debug-buffer-name)
      (format "%s Enable clutch-debug-mode, reproduce the failure, then inspect %s."
              message clutch-debug-buffer-name))))

(defun clutch--parse-error-position (msg &optional sql)
  "Extract a 1-based character position from error MSG for SQL, or nil.
Handles PG \\='(position N)\\=' suffix and Oracle-style \\='line N, column M\\='."
  (let ((case-fold-search t))
    (or
     (when (string-match "(position \\([0-9]+\\))" msg)
       (string-to-number (match-string 1 msg)))
     (when (and sql
                (string-match "\\bline \\([0-9]+\\), column \\([0-9]+\\)\\b" msg))
       (clutch--sql-line-column-to-position
        sql
        (string-to-number (match-string 1 msg))
        (string-to-number (match-string 2 msg)))))))

(defun clutch--clear-error-position-overlay ()
  "Remove the error overlays from the current buffer."
  (when (overlayp clutch--error-position-overlay)
    (delete-overlay clutch--error-position-overlay)
    (setq clutch--error-position-overlay nil))
  (when (overlayp clutch--error-banner-overlay)
    (delete-overlay clutch--error-banner-overlay)
    (setq clutch--error-banner-overlay nil)))

(defun clutch--make-buffer-query-error-details (connection sql err)
  "Return structured error details for a failed SQL execution on CONNECTION.
SQL is the user-visible statement.  ERR is the original signaled condition."
  (let* ((message (or (cadr err) (error-message-string err)))
         (details (copy-tree (nth 2 err)))
         (diag (copy-tree (plist-get details :diag)))
         (context (copy-tree (plist-get diag :context))))
    (unless details
      (setq details (list :summary (clutch--humanize-db-error message))))
    (when-let* ((backend (and connection
                              (condition-case nil
                                  (clutch--backend-key-from-conn connection)
                                (error nil)))))
      (unless (plist-member details :backend)
        (setq details (plist-put details :backend backend))))
    (unless (plist-get details :summary)
      (setq details (plist-put details :summary
                               (clutch--humanize-db-error message))))
    (unless diag
      (setq diag (list :raw-message message)))
    (unless (plist-get diag :raw-message)
      (setq diag (plist-put diag :raw-message message)))
    (unless (or (plist-get context :generated-sql)
                (plist-get context :sql))
      (setq context (plist-put context :sql sql)))
    (setq diag (plist-put diag :context context))
    (plist-put details :diag diag)))

(defun clutch--remember-buffer-query-error-details (buffer connection sql err)
  "Store the failed SQL execution details for BUFFER.
CONNECTION, SQL and ERR describe the failed query."
  (when (buffer-live-p buffer)
    (clutch--remember-problem-record
     :buffer buffer
     :connection connection
     :problem (clutch--make-buffer-query-error-details connection sql err))))

(defun clutch--remember-execute-error (buffer connection sql err &optional context)
  "Capture a failed execute path for BUFFER on CONNECTION.
SQL is the user-visible statement and ERR is the original condition.
Optional CONTEXT is merged into the debug event.  Return
`(MESSAGE . SUMMARY)' for the failure."
  (let* ((message (error-message-string err))
         (summary (clutch--humanize-db-error message)))
    (clutch--remember-buffer-query-error-details buffer connection sql err)
    (when clutch-debug-mode
      (clutch--remember-debug-event
       :buffer buffer
       :connection connection
       :op "execute"
       :phase "error"
       :backend (clutch--backend-key-from-conn connection)
       :summary summary
       :sql sql
       :context context))
    (cons message summary)))

;;;; Error overlays

(defun clutch--format-error-banner (msg)
  "Return a compact single-line banner string for error MSG."
  (let* ((humanized (clutch--humanize-db-error msg))
         (text (if (string-empty-p humanized)
                   "SQL execution failed"
                 humanized)))
    (truncate-string-to-width text 160 0 nil "...")))

(defun clutch--mark-error-banner (buf pos &optional msg)
  "Place an inline error banner overlay in BUF.
The banner appears above the line containing POS and uses MSG when
present."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (when (overlayp clutch--error-banner-overlay)
        (delete-overlay clutch--error-banner-overlay))
      (save-excursion
        (goto-char (max (point-min) (min pos (point-max))))
        (let* ((bol (line-beginning-position))
               (anchor-end (min (point-max) (1+ bol)))
               (banner (concat
                        (propertize (clutch--format-error-banner msg)
                                    'face 'clutch-error-banner-face)
                        "\n")))
          (setq clutch--error-banner-overlay
                (make-overlay bol anchor-end nil t nil))
          (overlay-put clutch--error-banner-overlay 'before-string banner)
          (overlay-put clutch--error-banner-overlay 'priority 1002)
          (overlay-put clutch--error-banner-overlay 'evaporate t)
          (when msg
            (overlay-put clutch--error-banner-overlay 'help-echo msg)))))))

(defun clutch--mark-error-region (buf beg end &optional msg)
  "Place an error overlay in BUF from BEG to END.
MSG, when non-nil, is attached as overlay help text."
  (when (buffer-live-p buf)
    (with-current-buffer buf
      (clutch--clear-error-position-overlay)
      (setq beg (max (point-min) beg))
      (setq end (min (1+ (point-max)) end))
      (when (< beg end)
        (setq clutch--error-position-overlay
              (make-overlay beg end))
        (overlay-put clutch--error-position-overlay 'face 'clutch-error-position-face)
        (overlay-put clutch--error-position-overlay 'priority 1001)
        (overlay-put clutch--error-position-overlay 'evaporate t)
        (when msg
          (overlay-put clutch--error-position-overlay 'help-echo msg))
        (clutch--mark-error-banner buf beg msg)))))

(defun clutch--mark-error-position (buf pos &optional msg)
  "Place an error overlay in BUF at POS.
POS is a 1-based buffer position, and MSG is used when present."
  (clutch--mark-error-region buf pos (1+ pos) msg))

(defun clutch--mark-sql-error (buf sql msg)
  "Mark SQL execution failure in BUF using MSG.
Prefers an exact error position; otherwise highlights the whole statement."
  (when (buffer-live-p buf)
    (if-let* ((sql-start clutch--executing-sql-start)
              (pos (clutch--parse-error-position msg sql)))
        (clutch--mark-error-position buf (+ sql-start (1- pos)) msg)
      (when-let* ((sql-start clutch--executing-sql-start)
                  (sql-end clutch--executing-sql-end))
        (clutch--mark-error-region buf sql-start sql-end msg)))))

(defun clutch--execute-and-mark (sql beg end &optional conn)
  "Execute SQL on CONN and mark BEG..END on success."
  (pcase-let* ((`(,trim-beg . ,trim-end)
                 (or (clutch--trim-sql-bounds beg end)
                     (cons beg end))))
    (when (let ((clutch--executing-sql-start trim-beg)
                (clutch--executing-sql-end trim-end))
            (clutch--execute sql conn))
      (clutch--mark-executed-sql-region beg end))))

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
  "Preview the execution payload for the current workflow."
  (interactive)
  (let ((sql
         (cond
          ((derived-mode-p 'clutch-result-mode)
           (if (or clutch--pending-inserts
                   clutch--pending-edits
                   clutch--pending-deletes)
               (mapconcat (lambda (s) (concat s ";"))
                          (append
                           (when clutch--pending-inserts
                             (clutch-result--build-pending-insert-statements))
                           (when clutch--pending-edits
                             (clutch-result--build-update-statements))
                           (when clutch--pending-deletes
                             (clutch-result--build-pending-delete-statements)))
                          "\n")
             (clutch-result--effective-query)))
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
         (done 0)
         (source-buffer (current-buffer)))
    (cl-labels
        ((signal-statement-error (err stmt)
           (let* ((failure
                   (clutch--remember-execute-error
                    source-buffer clutch-connection stmt err
                    (list :statement-index (1+ done))))
                  (summary (cdr failure)))
             (user-error "Statement %d failed: %s"
                         (1+ done)
                         (clutch--debug-workflow-message summary)))))
      (dolist (stmt before-last)
        (condition-case err
            (progn (clutch--run-db-query clutch-connection stmt) (cl-incf done))
          (quit
           (clutch--handle-query-quit clutch-connection))
          (clutch-db-error
           (signal-statement-error err stmt))))
      (if (clutch--select-query-p last)
          (progn
            (when (> done 0)
              (message "%d statement%s executed" done (if (= done 1) "" "s")))
            (clutch--execute last))
        (condition-case err
            (progn (clutch--run-db-query clutch-connection last) (cl-incf done)
                   (message "%d statement%s executed"
                            done (if (= done 1) "" "s")))
          (quit
           (clutch--handle-query-quit clutch-connection))
          (clutch-db-error
           (signal-statement-error err last)))))))

(defun clutch-execute-dwim (beg end)
  "Execute the region from BEG to END if active, otherwise execute query at point.
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
    ;; `quit-window' kills the indirect buffer, leaving the Lisp execution
    ;; context in a dead buffer.  Any subsequent `with-current-buffer' call
    ;; would fail when `save-current-buffer' tries to restore that dead buffer.
    ;; Explicitly switch to the live buffer now selected after the kill.
    (with-current-buffer (window-buffer (selected-window))
      (clutch--execute sql conn))))

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
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (buf  (generate-new-buffer "*clutch: indirect*")))
    (pop-to-buffer buf)
    (clutch-mode)
    (when conn
      (clutch--bind-connection-context conn params product)
      (clutch--update-mode-line))
    (clutch-indirect-mode 1)
    (insert text)
    (goto-char (point-min))
    (message "Edit SQL, then C-c ' to execute, C-c C-k to abort")))

(provide 'clutch-query)
;;; clutch-query.el ends here
