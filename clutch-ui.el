;;; clutch-ui.el --- Result rendering and icon helpers -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Internal result rendering and icon helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)

(defvar clutch--executing-p)
(declare-function clutch--spinner-string "clutch-connection" ())

(defvar-local clutch--aggregate-summary nil
  "Last aggregate summary plist for result footer, or nil.
Plist keys: :label, :rows, :cells, :skipped, :sum, :avg, :min, :max, :count.")
(defvar clutch--cached-pk-indices)
(defvar clutch--cell-default-placeholder)
(defvar clutch--cell-generated-placeholder)
(defvar-local clutch--column-widths nil
  "Vector of display widths for each result column.")
(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar-local clutch--dml-result nil
  "Non-nil when this result buffer shows a DML result.")
(defvar-local clutch--filter-pattern nil
  "Current client-side filter string, or nil.")
(defvar-local clutch--fk-info nil
  "Foreign key info for the current result.")
(defvar-local clutch--filtered-rows nil
  "Filtered subset of `clutch--result-rows', or nil when unfiltered.")
(defvar-local clutch--header-active-col nil
  "Column index currently highlighted in the header, or nil.")
(defvar-local clutch--footer-base-string nil
  "Static portion of mode-line footer (pagination, sort, etc.).")
(defvar-local clutch--footer-display-cache nil
  "Cached complete footer string returned by `:eval'.
Assembled from segment caches by `clutch--assemble-footer-display'.")
(defvar-local clutch--footer-timing-cache nil
  "Cached timing segment string for the footer.")
(defvar-local clutch--footer-cursor-cache nil
  "Cached cursor position segment string for the footer.")
(defvar-local clutch--footer-filters-cache nil
  "Cached filter/aggregate segment string for the footer.")
(defvar-local clutch--header-line-string nil
  "Full header-line string before hscroll adjustment.")
(defvar-local clutch--last-window-width nil
  "Last known window body width for the current result buffer.")
(defvar clutch--last-query)
(defvar-local clutch--marked-rows nil
  "List of marked row indices.")
(defvar-local clutch--order-by nil
  "Current ORDER BY state as (COL-NAME . DIRECTION) or nil.")
(defvar-local clutch--page-current 0
  "Current data page number (0-based).")
(defvar-local clutch--page-total-rows nil
  "Total row count from COUNT(*), or nil if not yet queried.")
(defvar-local clutch--pending-deletes nil
  "List of pk-value vectors staged for deletion.")
(defvar-local clutch--pending-edits nil
  "Alist of staged edits: ((PK-VEC . COL-IDX) . NEW-VALUE).")
(defvar-local clutch--pending-inserts nil
  "List of field alists staged for insertion.")
(defvar-local clutch--query-elapsed nil
  "Elapsed time in seconds for the last query execution.")
(defvar-local clutch--result-source-table nil
  "Detected source table name for the current result buffer, or nil.")
(defvar-local clutch--result-column-defs nil
  "Full column definition plists from the last result.")
(defvar-local clutch--result-columns nil
  "Column names from the last result.")
(defvar-local clutch--result-rows nil
  "Row data from the last result.")
(defvar-local clutch--executed-sql-overlay nil
  "Overlay marking the last successfully executed SQL statement.")
(defvar-local clutch--row-overlay nil
  "Overlay used to highlight the current row.")
(defvar clutch--row-start-positions)
(defvar-local clutch--sort-column nil
  "Column name currently sorted by, or nil.")
(defvar-local clutch--sort-descending nil
  "Non-nil if the current sort is descending.")
(defvar-local clutch--result-column-details nil
  "List of column detail plists aligned with `clutch--result-columns'.
Each element corresponds to the same-index column.  Nil when unavailable.")
(defvar-local clutch--where-filter nil
  "Current WHERE filter string, or nil if no filter is active.")
(defvar-local clutch--refine-rect nil
  "Rectangle (ROW-INDICES . COL-INDICES) being refined, or nil.")
(defvar-local clutch--refine-excluded-rows nil
  "Row indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-excluded-cols nil
  "Column indices (0-based) excluded during refine mode.")
(defvar-local clutch--refine-overlays nil
  "Overlays created during refine mode.")
(defvar-local clutch--refine-callback nil
  "Callback called with final rect when refine is confirmed.")
(defvar-local clutch--refine-saved-mode-line nil
  "Saved `mode-line-format' to restore after refine mode exits.")
(defvar-local clutch-record--result-buffer nil
  "Reference to the parent result buffer for record display.")
(defvar-local clutch-record--row-idx nil
  "Current row index being displayed in a record buffer.")
(defvar-local clutch-record--expanded-fields nil
  "List of expanded long field column indices in a record buffer.")
(defvar-local clutch-record--header-base nil
  "Cached record header string, set during render.")
(defvar clutch-column-padding)
(defvar clutch-connection)
(defvar clutch-describe--header-base)
(defvar clutch-record--header-base)
(defvar clutch-result-max-rows)

(declare-function clutch--bind-connection-context "clutch-connection" (conn &optional params product))
(declare-function clutch--cell-placeholder-value "clutch-query" (value))
(declare-function clutch--center-padding-widths "clutch-query" (content-width total-width))
(declare-function nerd-icons--function-name "nerd-icons" (name))
(declare-function clutch--column-names "clutch-query" (columns))
(declare-function clutch--compute-column-widths "clutch-query" (col-names rows columns))
(declare-function clutch--cached-column-details "clutch-schema" (conn table))
(declare-function clutch--ensure-column-details "clutch-schema" (conn table &optional strict))
(declare-function clutch--ensure-column-details-async "clutch-schema" (conn table))
(declare-function clutch--format-elapsed "clutch-query" (seconds))
(declare-function clutch--format-value "clutch-query" (value))
(declare-function clutch--numeric-type-p "clutch-query" (col-def))
(declare-function clutch--result-buffer-name "clutch-query" ())
(declare-function clutch--show-result-buffer "clutch-query" (buf))
(declare-function clutch--string-pad "clutch-query" (s width &optional pad-left numeric))
(declare-function clutch--tx-header-line-segment "clutch-connection" (conn))
(declare-function clutch--trim-sql-bounds "clutch-query" (beg end))
(declare-function clutch--value-placeholder "clutch-query" (value col-def))
(declare-function clutch--visible-columns "clutch-query" ())
(declare-function clutch-result--detect-table "clutch-edit" ())
(declare-function clutch-result--table-from-sql "clutch-edit" (sql))
(declare-function clutch-result--extract-pk-vec "clutch-edit" (row pk-indices))
(declare-function clutch-result--row-idx-at-line "clutch-edit" ())
(declare-function clutch-result-mode "clutch" (&optional arg))
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch-db-result-affected-rows "clutch-db" (result))
(declare-function clutch-db-result-columns "clutch-db" (result))
(declare-function clutch-db-result-connection "clutch-db" (result))
(declare-function clutch-db-result-last-insert-id "clutch-db" (result))
(declare-function clutch-db-result-rows "clutch-db" (result))
(declare-function clutch-db-result-warnings "clutch-db" (result))

(defcustom clutch-column-displayers nil
  "Per-table/per-column display functions for result cells.
Each entry is (TABLE-NAME . ((COLUMN-NAME . FUNCTION) ...)).
TABLE-NAME and COLUMN-NAME are matched case-insensitively.
FUNCTION receives the raw cell value and must return a string, which may
include text properties.  Return nil to fall back to default rendering."
  :type '(alist :key-type string
                :value-type (alist :key-type string :value-type function))
  :group 'clutch)

(defun clutch--case-insensitive-string= (left right)
  "Return non-nil when LEFT and RIGHT match case-insensitively."
  (and (stringp left)
       (stringp right)
       (string= (downcase left) (downcase right))))

(defun clutch--lookup-column-displayer (table column)
  "Return registered displayer for TABLE and COLUMN, or nil."
  (when-let* ((table-entry
               (cl-assoc table clutch-column-displayers
                         :test #'clutch--case-insensitive-string=))
              (column-entry
               (cl-assoc column (cdr table-entry)
                         :test #'clutch--case-insensitive-string=)))
    (cdr column-entry)))

(defun clutch-register-column-displayer (table column function)
  "Register FUNCTION as the renderer for COLUMN in TABLE.
FUNCTION receives the raw cell value and should return a string.  Return
nil from FUNCTION to fall back to default display.

Examples:

  ;; Show JSON column as a compact summary.
  (clutch-register-column-displayer \"orders\" \"metadata\"
    (lambda (value)
      (when (stringp value)
        (ignore-errors
          (format \"{%d keys}\"
                  (hash-table-count (json-parse-string value)))))))

  ;; Show a URL as a clickable button.
  (clutch-register-column-displayer \"bookmarks\" \"url\"
    (lambda (value)
      (when (stringp value)
        (propertize (truncate-string-to-width value 40 nil nil \"…\")
                    (quote face) (quote link)
                    (quote help-echo) value))))

  ;; Map numeric status codes to styled labels.
  (clutch-register-column-displayer \"tasks\" \"status\"
    (lambda (value)
      (pcase value
        (0 (propertize \"pending\" (quote face) (quote warning)))
        (1 (propertize \"active\" (quote face) (quote success)))
        (2 (propertize \"done\" (quote face) (quote shadow))))))"
  (unless (stringp table)
    (signal 'wrong-type-argument (list 'stringp table)))
  (unless (stringp column)
    (signal 'wrong-type-argument (list 'stringp column)))
  (unless (functionp function)
    (signal 'wrong-type-argument (list 'functionp function)))
  (if-let* ((table-entry
             (cl-assoc table clutch-column-displayers
                       :test #'clutch--case-insensitive-string=)))
      (progn
        (setcar table-entry table)
        (if-let* ((column-entry
                   (cl-assoc column (cdr table-entry)
                             :test #'clutch--case-insensitive-string=)))
            (progn
              (setcar column-entry column)
              (setcdr column-entry function))
          (setcdr table-entry (cons (cons column function) (cdr table-entry)))))
    (push (cons table (list (cons column function))) clutch-column-displayers))
  function)

(defun clutch-unregister-column-displayer (table column)
  "Remove any custom displayer for COLUMN in TABLE."
  (unless (stringp table)
    (signal 'wrong-type-argument (list 'stringp table)))
  (unless (stringp column)
    (signal 'wrong-type-argument (list 'stringp column)))
  (when-let* ((table-entry
               (cl-assoc table clutch-column-displayers
                         :test #'clutch--case-insensitive-string=)))
    (setcdr table-entry
            (cl-remove-if
             (lambda (column-entry)
               (clutch--case-insensitive-string=
                (car-safe column-entry) column))
             (cdr table-entry)))
    (when (null (cdr table-entry))
      (setq clutch-column-displayers
            (delq table-entry clutch-column-displayers))))
  nil)

(defun clutch--cell-custom-display (value col-def)
  "Return custom display string for VALUE in COL-DEF, or nil."
  (when-let* ((table clutch--result-source-table)
              (column (plist-get col-def :name))
              (displayer (clutch--lookup-column-displayer table column)))
    (condition-case err
        (let ((display (funcall displayer value)))
          (cond
           ((null display) nil)
           ((stringp display) display)
           (t
            (message "clutch column displayer %s.%s returned %S, falling back"
                     table column display)
            nil)))
      (error
       (message "clutch column displayer %s.%s failed: %s"
                table column (error-message-string err))
       nil))))

(defun clutch--nerd-icons-available-p ()
  "Return non-nil when nerd-icons is loaded and usable."
  (and (require 'nerd-icons nil t)
       (fboundp 'nerd-icons--function-name)))

(defun clutch--icon (name &optional fallback &rest icon-args)
  "Return a nerd-icons icon for NAME, or FALLBACK string.
NAME is a cons (FAMILY . ICON-NAME) where FAMILY is any nerd-icons
glyph-set symbol (e.g. `mdicon', `devicon', `codicon', `octicon').
ICON-ARGS are keyword arguments forwarded to the nerd-icons function
\(e.g. :height 1.2).  The icon function is resolved dynamically via
`nerd-icons--function-name', so new families require no changes here.
Falls back to FALLBACK (a Unicode symbol) when nerd-icons is not
installed or the icon is unknown."
  (pcase-let ((`(,family . ,icon-name) name))
    (or (and (clutch--nerd-icons-available-p)
             (let ((fn (nerd-icons--function-name family)))
               (and (fboundp fn)
                    (apply fn icon-name icon-args))))
        fallback
        "")))

(defun clutch--append-face (string face)
  "Return STRING with FACE appended, preserving existing properties."
  (let ((copy (copy-sequence string)))
    (unless (or (null face)
                (string-empty-p copy))
      (add-face-text-property 0 (length copy) face 'append copy))
    copy))

(defun clutch--icon-with-face (name fallback face &rest icon-args)
  "Return icon NAME/FALLBACK with FACE appended to its text properties.
Pass ICON-ARGS through to `clutch--icon'."
  (clutch--append-face (apply #'clutch--icon name fallback icon-args) face))

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
         (raw (clutch--append-face raw face))
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
    result))

(defun clutch--footer-icon (spec fallback face)
  "Return footer icon SPEC/FALLBACK with explicit FACE."
  (concat (clutch--icon-with-face spec fallback face)
          (propertize " " 'face face)))

(defun clutch--disconnected-badge ()
  "Return a disconnected indicator string with warning face, or nil if connected."
  (unless clutch-connection
    (concat (clutch--icon-with-face '(mdicon . "nf-md-database_off") "⨯" 'warning)
            (propertize " Disconnected" 'face 'warning))))

(defun clutch--header-with-disconnect-badge (base)
  "Prepend disconnected badge to BASE header string when not connected."
  (let ((badge (clutch--disconnected-badge)))
    (if badge
        (concat (propertize " " 'display '(space :align-to 0))
                badge
                (propertize "  •  " 'face 'font-lock-comment-face)
                base)
      (or base ""))))

(defun clutch--clear-executed-sql-overlay (&rest _)
  "Remove the last executed SQL overlay in the current buffer."
  (when (overlayp clutch--executed-sql-overlay)
    (let ((overlay clutch--executed-sql-overlay))
      (setq clutch--executed-sql-overlay nil)
      (delete-overlay overlay))))

(defun clutch--executed-sql-marker-before-string ()
  "Return the before-string used to mark executed SQL."
  (if (display-graphic-p)
      (propertize " "
                  'display
                  '(left-fringe clutch-executed-sql-dot clutch-executed-sql-marker-face))
    (propertize "✓ " 'face 'clutch-executed-sql-marker-face)))

(defun clutch--mark-executed-sql-region (beg end)
  "Mark the last successfully executed SQL region BEG..END."
  (when-let* ((trimmed (clutch--trim-sql-bounds beg end))
              (tbeg (car trimmed))
              (tend (cdr trimmed)))
    (clutch--clear-executed-sql-overlay)
    (setq clutch--executed-sql-overlay
          (make-overlay (save-excursion
                          (goto-char tbeg)
                          (line-beginning-position))
                        (save-excursion
                          (goto-char tbeg)
                          (line-beginning-position))
                        nil nil nil))
    (overlay-put clutch--executed-sql-overlay 'before-string
                 (clutch--executed-sql-marker-before-string))
    (overlay-put clutch--executed-sql-overlay 'help-echo
                 (format "Last executed SQL (%d chars)" (- tend tbeg)))))

(defun clutch--header-label (name)
  "Build the display label for column NAME.
Prepends the sort indicator when the column is active."
  (let* ((sort (when (and clutch--sort-column
                          (string= name clutch--sort-column))
                 (let ((s (clutch--fixed-width-icon
                           (if clutch--sort-descending
                               '(codicon . "nf-cod-arrow_down")
                             '(codicon . "nf-cod-arrow_up"))
                           (if clutch--sort-descending "▼" "▲"))))
                   (when s
                     (propertize s 'clutch-header-icon t))))))
    (if sort
        (concat sort " " name)
      name)))

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
    ;; Replace the leading cross of the first column with the left edge.
    (let ((line (concat (mapconcat #'identity (nreverse parts) "") right)))
      (concat left (substring line 1)))))

(defun clutch--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each header cell body carries a `clutch-header-col' text property so
column-local commands still work from padded whitespace."
  (let ((padding clutch-column-padding)
        (parts nil))
    (dolist (cidx visible-cols)
      (let* ((name (nth cidx clutch--result-columns))
             (w (aref widths cidx))
             (label (clutch--header-label name))
             (truncated (if (> (string-width label) w)
                            (truncate-string-to-width label w)
                          label))
             (pads (clutch--center-padding-widths (string-width truncated) w))
             (lead (make-string (car pads) ?\s))
             (trail (make-string (cdr pads) ?\s))
             (cell (concat lead truncated trail))
             (face 'clutch-field-name-face)
             (pad-str (make-string padding ?\s))
             (body nil))
        ;; Append base face so icon-specific face (e.g. pin color) is preserved.
        (add-face-text-property 0 (length cell) face 'append cell)
        (setq body (concat pad-str cell pad-str))
        (add-text-properties 0 (length body)
                             `(clutch-header-col ,cidx)
                             body)
        (push (concat (propertize "│" 'face 'clutch-border-face)
                      body)
              parts)))
    (concat (mapconcat #'identity (nreverse parts) "")
            (propertize "│" 'face 'clutch-border-face))))

(defun clutch--cell-face (val edited cidx)
  "Return the display face for a cell with VAL at CIDX, EDITED if modified."
  (cond (edited 'clutch-modified-face)
        ((clutch--cell-placeholder-value val) 'clutch-null-face)
        ((null val) 'clutch-null-face)
        ((assq cidx clutch--fk-info) 'clutch-fk-face)
        (t nil)))

(defun clutch--cell-display-content (val w col-def edited)
  "Return the unpadded display string for a cell value VAL in width W.
COL-DEF is the column definition plist, EDITED is a staged edit cons or nil."
  (let* ((display-val (if edited (cdr edited) val))
         (custom (clutch--cell-custom-display display-val col-def))
         (special-placeholder (and (not custom)
                                   (not edited)
                                   (clutch--cell-placeholder-value display-val)))
         (s (and (not custom)
                 (or special-placeholder
                     (replace-regexp-in-string "\n" "↵"
                                               (clutch--format-value display-val)))))
         (placeholder (and (not custom)
                           (not edited)
                           (not special-placeholder)
                           (> (string-width s) w)
                           (clutch--value-placeholder display-val col-def)))
         (formatted (or custom placeholder s)))
    (if (> (string-width formatted) w)
        (truncate-string-to-width formatted w)
      formatted)))

(defun clutch--pending-insert-placeholders ()
  "Return placeholder sentinels aligned with `clutch--result-columns'."
  (when-let* ((conn clutch-connection)
              (table (clutch-result--detect-table))
              (details (clutch--ensure-column-details conn table)))
    (mapcar (lambda (col-name)
              (when-let* ((detail (cl-find col-name details
                                           :key (lambda (d) (plist-get d :name))
                                           :test #'string=)))
                (cond
                 ((plist-get detail :generated) clutch--cell-generated-placeholder)
                 ((plist-get detail :default) clutch--cell-default-placeholder)
                 (t nil))))
            clutch--result-columns)))

(defun clutch--build-render-state ()
  "Return hash-table lookups for the current result render.
These tables avoid repeated linear scans through staged UI state while
rendering large result pages."
  (let ((edit-table (make-hash-table :test 'equal))
        (edit-row-table (make-hash-table :test 'equal))
        (marked-table (make-hash-table :test 'eql))
        (delete-table (make-hash-table :test 'equal)))
    (dolist (edit clutch--pending-edits)
      (puthash (car edit) edit edit-table)
      (puthash (car (car edit)) t edit-row-table))
    (dolist (ridx clutch--marked-rows)
      (puthash ridx t marked-table))
    (dolist (pk-vec clutch--pending-deletes)
      (puthash pk-vec t delete-table))
    (list :edits edit-table
          :edit-rows edit-row-table
          :marked marked-table
          :deletes delete-table
          :insert-placeholders (clutch--pending-insert-placeholders)
          :pk-indices clutch--cached-pk-indices)))

(defun clutch--render-edit-entry (row _ridx cidx render-state)
  "Return staged edit entry for ROW/CIDX from RENDER-STATE, or nil."
  (let* ((edits (plist-get render-state :edits))
         (pk-indices (plist-get render-state :pk-indices)))
    (and pk-indices
         (gethash (cons (clutch-result--extract-pk-vec row pk-indices) cidx)
                  edits))))

(defun clutch--row-pending-edit-p (row _ridx render-state)
  "Return non-nil when ROW has any staged edit in RENDER-STATE."
  (let* ((edit-rows (plist-get render-state :edit-rows))
         (pk-indices (plist-get render-state :pk-indices)))
    (and pk-indices
         (gethash (clutch-result--extract-pk-vec row pk-indices)
                  edit-rows))))

(defun clutch--render-cell (row ridx cidx widths render-state)
  "Render cell at column CIDX of ROW at row index RIDX.
WIDTHS is the width vector.  Returns a propertized string
including the leading border and padding."
  (let* ((val     (nth cidx row))
         (col-def (nth cidx clutch--result-column-defs))
         (edited  (clutch--render-edit-entry row ridx cidx render-state))
         (w       (aref widths cidx))
         (content (clutch--cell-display-content val w col-def edited))
         (padded  (clutch--string-pad content w (clutch--numeric-type-p col-def)))
         (face    (clutch--cell-face val edited cidx))
         (pad-str (make-string clutch-column-padding ?\s))
         (body nil))
    (when (and (eq face 'clutch-fk-face)
               (not (string-empty-p content)))
      (let ((start (if (clutch--numeric-type-p col-def)
                       (- (length padded) (length content))
                     0)))
        (add-face-text-property start (+ start (length content)) face 'append padded)
        (setq face nil)))
    (when face
      (add-face-text-property 0 (length padded) face 'append padded))
    (setq body (concat pad-str padded pad-str))
    (add-text-properties 0 (length body)
                         `(clutch-row-idx ,ridx
                           clutch-col-idx ,cidx
                           clutch-full-value ,(if edited (cdr edited) val))
                         body)
    (concat (propertize "│" 'face 'clutch-border-face)
            body)))

(defun clutch--render-row (row ridx visible-cols widths render-state)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector,
and RENDER-STATE carries cached render data.
Returns a propertized string."
  (concat (mapconcat (lambda (cidx)
                       (clutch--render-cell row ridx cidx widths render-state))
                     visible-cols "")
          (propertize "│" 'face 'clutch-border-face)))

(defun clutch--render-row-line (ridx render-state)
  "Return the rendered buffer line string for row RIDX.
RENDER-STATE carries cached lookup tables for staged row state."
  (let* ((rows (or clutch--filtered-rows clutch--result-rows))
         (row (nth ridx rows))
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (nw (clutch--row-number-digits))
         (global-first-row (* clutch--page-current clutch-result-max-rows))
         (bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (marked-table (plist-get render-state :marked))
         (delete-table (plist-get render-state :deletes))
         (pk-indices (plist-get render-state :pk-indices))
         (deletingp (and pk-indices
                         (gethash (clutch-result--extract-pk-vec row pk-indices)
                                  delete-table)))
         (editedp (clutch--row-pending-edit-p row ridx render-state))
         (data-row (let ((rendered (clutch--render-row
                                    row ridx visible-cols widths render-state)))
                     (if deletingp
                         (propertize rendered 'face 'clutch-pending-delete-face)
                       rendered)))
         (mark-char (cond (deletingp "D")
                          (editedp "E")
                          ((gethash ridx marked-table) "*")
                          (t " ")))
         (num-label (string-pad
                     (number-to-string
                      (1+ (+ global-first-row ridx)))
                     nw nil t))
         (num-face (cond (deletingp 'clutch-pending-delete-face)
                         (editedp 'clutch-modified-face)
                         ((gethash ridx marked-table) 'clutch-marked-face)
                         (t 'shadow))))
    (concat (propertize "│" 'face bface)
            (propertize mark-char 'face num-face)
            (propertize num-label 'face num-face)
            pad-str
            data-row
            "\n")))

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
           (clutch--footer-icon '(mdicon . "nf-md-calculator_variant") "∑"
                                'font-lock-keyword-face)
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
         (clutch--footer-icon '(mdicon . "nf-md-calculator_variant") "∑"
                              'font-lock-keyword-face)
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
           (let ((icon (clutch--footer-icon '(codicon . "nf-cod-filter") "W:"
                                            'font-lock-warning-face)))
             (concat icon
                     (propertize clutch--where-filter
                                 'face 'font-lock-warning-face))))
         (when clutch--filter-pattern
           (let ((icon (clutch--footer-icon '(codicon . "nf-cod-search") "/:"
                                            'font-lock-string-face)))
             (concat icon
                     (propertize clutch--filter-pattern
                                 'face 'font-lock-string-face)))))))

(defun clutch--footer-sort-part ()
  "Build footer part for active SQL ORDER BY state."
  (when-let* ((order clutch--order-by))
    (pcase-let ((`(,column . ,direction) order))
      (let ((icon (if (string-match-p "\\`desc\\'" direction)
                      '(octicon . "nf-oct-sort_desc")
                    '(octicon . "nf-oct-sort_asc")))
            (hi 'font-lock-keyword-face))
        (concat (clutch--footer-icon icon "↕" hi)
                (propertize (format "%s[%s]" (upcase direction) column)
                            'face hi))))))

(defun clutch--footer-pending-part ()
  "Build footer part for staged edits, deletions, or insertions."
  (let (parts)
    (when clutch--pending-edits
      (push (format "E-%d" (length clutch--pending-edits))
            parts))
    (when clutch--pending-deletes
      (push (format "D-%d" (length clutch--pending-deletes))
            parts))
    (when clutch--pending-inserts
      (push (format "I-%d" (length clutch--pending-inserts))
            parts))
    (when parts
      (let ((commit-icon (clutch--icon-with-face '(codicon . "nf-cod-check")
                                                 "✓" 'font-lock-comment-face))
            (discard-icon (clutch--icon-with-face '(codicon . "nf-cod-discard")
                                                  "✗" 'font-lock-comment-face)))
        (concat
         (clutch--footer-icon '(codicon . "nf-cod-diff_modified") "✎"
                              'clutch-modified-face)
         (propertize (mapconcat #'identity (nreverse parts) " ")
                     'face 'clutch-modified-face)
         (propertize "  " 'face 'font-lock-comment-face)
         commit-icon
         (propertize ":C-c C-c  " 'face 'font-lock-comment-face)
         discard-icon
         (propertize ":C-c C-k" 'face 'font-lock-comment-face))))))

(defun clutch--footer-mutation-capability-part ()
  "Build footer part describing update/delete capability for the result."
  (when (and clutch--result-columns
             (clutch-result--detect-table))
    (unless clutch--cached-pk-indices
      (let ((warn-icon 'font-lock-warning-face)
            (warn-text '(:inherit font-lock-warning-face :weight normal)))
        (concat (clutch--footer-icon '(codicon . "nf-cod-warning") "⚠" warn-icon)
                (propertize "PK missing" 'face warn-text)
                (propertize " E/D off" 'face warn-text))))))

(defun clutch--footer-main-parts (row-count page-num page-size total-rows)
  "Return list of main footer part strings for pagination state.
ROW-COUNT and PAGE-NUM describe the visible page, and PAGE-SIZE sets
the page length.  TOTAL-ROWS describes the full result size when known."
  (let* ((hi 'font-lock-keyword-face)
         (dim 'font-lock-comment-face)
         (total-pages (when total-rows
                        (max 1 (ceiling total-rows (float page-size))))))
    (delq nil
          (list
           (concat (clutch--footer-icon '(mdicon . "nf-md-sigma") "Σ" hi)
                   (clutch--footer-row-summary row-count total-rows))
           (concat (clutch--footer-icon '(codicon . "nf-cod-files") "⊞" hi)
                   (propertize (format "%d" (1+ page-num)) 'face hi)
                   (propertize "/" 'face dim)
                   (propertize (format "%d" (or total-pages 1)) 'face hi))
           (clutch--tx-header-line-segment clutch-connection)
           (clutch--footer-sort-part)
           (clutch--footer-mutation-capability-part)
           (clutch--footer-pending-part)))))

(defun clutch--footer-timing-part ()
  "Return the dynamic footer timing segment for the current result buffer."
  (let ((hi 'font-lock-keyword-face))
    (when-let* ((payload (cond
                          (clutch--executing-p
                           (and (clutch--spinner-string)
                                (propertize (clutch--spinner-string)
                                            'face 'success)))
                          (clutch--query-elapsed
                           (propertize
                            (clutch--format-elapsed clutch--query-elapsed)
                            'face hi)))))
      (concat (clutch--footer-icon '(mdicon . "nf-md-timer_outline") "⏱" hi)
              payload))))

(defun clutch--footer-cursor-part ()
  "Return a mode-line segment showing the current row and column at point."
  (let ((ridx (get-text-property (point) 'clutch-row-idx))
        (cidx (get-text-property (point) 'clutch-col-idx)))
    (when (and ridx cidx)
      (let* ((hi 'font-lock-keyword-face)
             (dim 'font-lock-comment-face)
             (total-cols (length clutch--result-columns))
             (raw-name (or (nth cidx clutch--result-columns) ""))
             (col-name (if (> (length raw-name) 20)
                           (concat (substring raw-name 0 17) "...")
                         raw-name)))
        (concat (clutch--footer-icon '(mdicon . "nf-md-cursor_default_click_outline") "⌖" hi)
                (propertize (format "R-%d" (1+ ridx)) 'face hi)
                (propertize ":" 'face dim)
                (propertize (format "%s-%d/%d" col-name (1+ cidx) total-cols)
                            'face hi))))))

(defun clutch--render-footer (row-count page-num page-size total-rows)
  "Return the static footer string for pagination state.
ROW-COUNT and PAGE-NUM describe the visible page, and PAGE-SIZE sets
the page length.  TOTAL-ROWS describes the full result size when known."
  (let ((sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (mapconcat #'identity
               (clutch--footer-main-parts row-count page-num page-size
                                          total-rows)
               sep)))

(defun clutch--footer-mode-line-display ()
  "Return the cached footer string for mode-line display.
The cache is rebuilt by `clutch--refresh-footer-display'."
  (or clutch--footer-display-cache ""))

(defun clutch--assemble-footer-display ()
  "Assemble `clutch--footer-display-cache' from cached segments."
  (let ((badge (clutch--disconnected-badge))
        (base (or clutch--footer-base-string ""))
        (timing clutch--footer-timing-cache)
        (filters clutch--footer-filters-cache)
        (cursor clutch--footer-cursor-cache)
        (sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (setq clutch--footer-display-cache
          (concat (propertize " " 'display '(space :align-to 0))
                  (when badge (concat badge sep))
                  base
                  (when timing (concat sep timing))
                  (when filters (concat sep (string-join filters sep)))
                  (when cursor (concat sep cursor))))))

(defun clutch--refresh-footer-display ()
  "Rebuild all footer segments and assemble the cached display string."
  (setq clutch--footer-timing-cache (clutch--footer-timing-part)
        clutch--footer-cursor-cache (clutch--footer-cursor-part)
        clutch--footer-filters-cache (clutch--footer-filter-parts))
  (clutch--assemble-footer-display))

(defun clutch--refresh-footer-timing ()
  "Rebuild only the timing segment and reassemble the footer."
  (setq clutch--footer-timing-cache (clutch--footer-timing-part))
  (clutch--assemble-footer-display))

(defun clutch--refresh-footer-cursor ()
  "Rebuild only the cursor segment and reassemble the footer."
  (setq clutch--footer-cursor-cache (clutch--footer-cursor-part))
  (clutch--assemble-footer-display))

(defun clutch--effective-widths ()
  "Return column widths adjusted for header indicator icons.
Columns with sort indicators get wider to fit the label."
  (let ((widths (copy-sequence clutch--column-widths)))
    (dotimes (cidx (length widths))
      (let* ((name (nth cidx clutch--result-columns))
             (label (clutch--header-label name))
             (label-w (string-width label)))
        (when (> label-w (aref widths cidx))
          (aset widths cidx label-w))))
    widths))

(defun clutch--column-info-string (cidx)
  "Build the display string for column at CIDX from cached details."
  (when-let* ((details clutch--result-column-details)
              (col (nth cidx details)))
    (let ((parts nil))
      (when-let* ((comment (plist-get col :comment)))
        (unless (string-empty-p comment)
          (push comment parts)))
      (when-let* ((default (plist-get col :default)))
        (push (format "Default: %s" default) parts))
      (push (format "Nullable: %s" (if (plist-get col :nullable) "YES" "NO")) parts)
      (when-let* ((type (plist-get col :type)))
        (push (format "Type: %s" type) parts))
      (when-let* ((name (plist-get col :name)))
        (push name parts))
      (string-join parts "\n"))))

(defun clutch--resolve-result-column-details (conn sql col-names)
  "Resolve column details for result columns COL-NAMES.
Uses CONN and SQL to detect the source table.  Checks the cache first;
if missing, loads details via `clutch--ensure-column-details'.
Returns a list of detail plists aligned with COL-NAMES, or nil."
  (when-let* ((table (when sql
                       (clutch-result--table-from-sql sql)))
              (details (or (clutch--cached-column-details conn table)
                           (clutch--ensure-column-details conn table))))
    (let ((by-name (make-hash-table :test 'equal)))
      (dolist (d details)
        (puthash (downcase (plist-get d :name)) d by-name))
      (mapcar (lambda (name)
                (gethash (downcase name) by-name))
              col-names))))

(defun clutch--cached-result-column-details (conn sql col-names)
  "Return cached result column details for COL-NAMES, or nil when unavailable.
Uses CONN and SQL to detect the source table, but does not trigger synchronous
metadata loading."
  (when-let* ((table (when sql
                       (clutch-result--table-from-sql sql)))
              (details (clutch--cached-column-details conn table)))
    (let ((by-name (make-hash-table :test 'equal)))
      (dolist (detail details)
        (puthash (downcase (plist-get detail :name)) detail by-name))
      (mapcar (lambda (name)
                (gethash (downcase name) by-name))
              col-names))))

(defun clutch--queue-result-column-details-enrichment (conn sql)
  "Start async result column-detail preheat for CONN and SQL when needed."
  (when-let* ((table (when sql
                       (clutch-result--table-from-sql sql)))
              ((not (clutch--cached-column-details conn table))))
    (clutch--ensure-column-details-async conn table)))

(defun clutch--refresh-result-metadata-buffers (conn table)
  "Refresh cached result column metadata for live result buffers on CONN/TABLE."
  (when-let* ((conn-key (and conn (clutch--connection-key conn))))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'clutch-result-mode)
                     clutch-connection
                     clutch--result-columns
                     clutch--last-query
                     (string= (clutch--connection-key clutch-connection) conn-key)
                     (equal (clutch-result--table-from-sql clutch--last-query) table))
            (setq-local clutch--result-column-details
                        (clutch--cached-result-column-details
                         clutch-connection clutch--last-query clutch--result-columns))))))))

(defun clutch--header-cell (cidx widths &optional active-cidx)
  "Build a single header cell string for column CIDX.
WIDTHS is the effective width vector.
ACTIVE-CIDX is the highlighted column index, if any."
  (let* ((name (nth cidx clutch--result-columns))
         (w (aref widths cidx))
         (label (clutch--header-label name))
         (truncated (if (> (string-width label) w)
                        (truncate-string-to-width label w)
                      label))
         (pads (clutch--center-padding-widths (string-width truncated) w))
         (lead (make-string (car pads) ?\s))
         (trail (make-string (cdr pads) ?\s))
         (label (copy-sequence truncated))
         (active-p (eql cidx active-cidx))
         (base-face 'clutch-field-name-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (body nil))
    ;; Append base/underline style without overwriting icon-specific face.
    (add-face-text-property 0 (length label) base-face 'append label)
    (when active-p
      (add-face-text-property 0 (length label) 'clutch-header-active-face
                              'append label))
    (add-face-text-property 0 (length label) '(:underline t) 'append label)
    ;; Keep sort/pin icons un-underlined for cleaner visual hierarchy.
    (dotimes (i (length label))
      (when (get-text-property i 'clutch-header-icon label)
        (let ((icon-face (or (get-text-property i 'face label) base-face)))
          (put-text-property i (1+ i) 'face
                             (list '(:underline nil) icon-face)
                             label))))
    (setq body (concat pad-str
                       (propertize lead 'face base-face)
                       label
                       (propertize trail 'face base-face)
                       pad-str))
    (add-text-properties 0 (length body)
                         `(clutch-header-col ,cidx)
                         body)
    (concat (propertize "│" 'face 'clutch-border-face)
            body)))

(defun clutch--build-header-line (visible-cols widths nw &optional active-cidx)
  "Build the `header-line-format' string for the column header row.
VISIBLE-COLS, WIDTHS describe columns.
NW is the digit width for the row number column.
ACTIVE-CIDX highlights that column when non-nil."
  (let* ((bface 'clutch-border-face)
         (pad-str (make-string clutch-column-padding ?\s))
         (cells (mapcar (lambda (cidx)
                          (clutch--header-cell cidx widths active-cidx))
                        visible-cols))
         (data-header (concat (apply #'concat cells)
                              (propertize "│" 'face bface))))
    ;; 1 char for mark column + nw for row number + padding
    (concat (propertize "│" 'face bface)
            " " (make-string nw ?\s) pad-str
            data-header)))

(defun clutch--header-line-with-hscroll ()
  "Return the header string shifted to match `window-hscroll'.
The header-line should track body hscroll exactly."
  (when clutch--header-line-string
    (let* ((hs (window-hscroll))
           (str clutch--header-line-string)
           (len (length str)))
      (if (>= hs len)
          ""
        (substring str hs)))))

(defun clutch--header-line-display ()
  "Return the display-ready header-line string for result buffers."
  (concat (propertize " " 'display '(space :align-to 0))
          (or (clutch--header-line-with-hscroll) "")))

(defun clutch--insert-data-rows (rows row-positions render-state)
  "Insert data ROWS into the current buffer.
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for staged UI state."
  (cl-loop for _row in rows
           for ridx from 0
           do (aset row-positions ridx (point))
           do (insert (clutch--render-row-line ridx render-state))))

(defun clutch--insert-pending-insert-rows (visible-cols widths nw nrows row-positions
                                                        render-state)
  "Append ghost rows for staged inserts below the real data rows.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
NROWS is the count of real rows (used to compute ghost row indices).
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for staged UI state."
  (let ((bface 'clutch-border-face)
        (pad-str (make-string clutch-column-padding ?\s))
        (insert-placeholders (plist-get render-state :insert-placeholders)))
    (cl-loop for fields in clutch--pending-inserts
             for iidx from 0
             for ridx = (+ nrows iidx)
             do (aset row-positions ridx (point))
             for row = (cl-mapcar (lambda (col placeholder)
                                    (if-let* ((entry (assoc col fields)))
                                        (cdr entry)
                                      placeholder))
                                  clutch--result-columns
                                  insert-placeholders)
             for data-row = (propertize
                             (clutch--render-row row ridx visible-cols widths render-state)
                             'face 'clutch-pending-insert-face)
             for num-label = (string-pad (format "I%d" (1+ iidx)) nw nil t)
             do (insert (propertize "│" 'face bface)
                        (propertize "I" 'face 'clutch-pending-insert-face)
                        (propertize num-label 'face 'clutch-pending-insert-face)
                        pad-str
                        data-row "\n"))))

(defun clutch--refresh-header-line ()
  "Rebuild the header-line format without touching the table body."
  (if clutch--column-widths
      (let* ((visible-cols (clutch--visible-columns))
             (widths (clutch--effective-widths))
             (nw (clutch--row-number-digits)))
        (setq clutch--header-line-string
              (clutch--build-header-line visible-cols widths nw
                                         clutch--header-active-col))
        (setq header-line-format '(:eval (clutch--header-line-display))))
    (setq clutch--header-line-string nil
          header-line-format nil)))

(defun clutch--refresh-footer-line ()
  "Rebuild the mode-line footer format without touching the table body."
  (let ((rows (or clutch--filtered-rows clutch--result-rows)))
    (setq clutch--footer-base-string
          (clutch--render-footer
           (length rows) clutch--page-current
           clutch-result-max-rows clutch--page-total-rows))
    (setq mode-line-format '(:eval (clutch--footer-mode-line-display)))
    (clutch--refresh-footer-display)))

(defun clutch--update-result-line-formats (_rows _visible-cols _widths _nw)
  "Set `mode-line-format' and `header-line-format' for the result buffer.
ROWS, VISIBLE-COLS, and WIDTHS define the rendered table, and NW is the
available window width."
  (clutch--refresh-footer-line)
  (clutch--refresh-header-line))

(defun clutch--render-result ()
  "Render the result buffer content as one horizontally scrollable table.
Preserves point position (row + column) across the render."
  (let* ((save-ridx (or (get-text-property (point) 'clutch-row-idx)
                        (clutch-result--row-idx-at-line)))
         (save-cidx (get-text-property (point) 'clutch-col-idx))
         (inhibit-read-only t)
         (visible-cols (clutch--visible-columns))
         (widths (clutch--effective-widths))
         (rows (or clutch--filtered-rows clutch--result-rows))
         (render-state (clutch--build-render-state))
         (nw (clutch--row-number-digits))
         (row-positions (make-vector (+ (length rows)
                                        (length clutch--pending-inserts))
                                     nil)))
    (erase-buffer)
    (setq clutch--row-start-positions row-positions)
    (clutch--update-result-line-formats rows visible-cols widths nw)
    (clutch--insert-data-rows rows row-positions render-state)
    (clutch--insert-pending-insert-rows visible-cols widths nw (length rows)
                                        row-positions render-state)
    (if save-ridx
        (clutch--goto-cell save-ridx save-cidx)
      (goto-char (point-min)))))

(defun clutch--reindex-row-starts-from (ridx)
  "Recompute `clutch--row-start-positions' from RIDX to the end of the buffer."
  (when (and (vectorp clutch--row-start-positions)
             (integerp ridx)
             (<= 0 ridx)
             (< ridx (length clutch--row-start-positions)))
    (let ((line-pos (aref clutch--row-start-positions ridx))
          (idx ridx)
          (len (length clutch--row-start-positions)))
      (when line-pos
        (save-excursion
          (goto-char line-pos)
          (while (and (< idx len)
                      (< (point) (point-max)))
            (aset clutch--row-start-positions idx (point))
            (setq idx (1+ idx))
            (forward-line 1))
          (while (< idx len)
            (aset clutch--row-start-positions idx nil)
            (setq idx (1+ idx))))))))

(defun clutch--replace-row-at-index (ridx)
  "Re-render row RIDX in place without a full body redraw.
Falls back to `clutch--refresh-display' when row-local replacement is unsafe."
  (let* ((rows (or clutch--filtered-rows clutch--result-rows))
         (nrows (length rows))
         (line-pos (and (vectorp clutch--row-start-positions)
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx (length clutch--row-start-positions))
                        (aref clutch--row-start-positions ridx))))
    (if (or (not line-pos)
            (not (integerp ridx))
            (< ridx 0)
            (>= ridx nrows))
        (clutch--refresh-display)
      (let* ((save-ridx (get-text-property (point) 'clutch-row-idx))
             (save-cidx (get-text-property (point) 'clutch-col-idx))
             (next-pos (and (< (1+ ridx) (length clutch--row-start-positions))
                            (aref clutch--row-start-positions (1+ ridx))))
             (end-pos (or next-pos
                          (save-excursion
                            (goto-char line-pos)
                            (forward-line 1)
                            (point))))
             (render-state (clutch--build-render-state))
             (line (clutch--render-row-line ridx render-state))
             (inhibit-read-only t))
        (save-excursion
          (goto-char line-pos)
          (delete-region line-pos end-pos)
          (insert line))
        (clutch--reindex-row-starts-from ridx)
        (when save-ridx
          (clutch--goto-cell save-ridx save-cidx))))))

(defun clutch--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'clutch-col-idx))

(defun clutch--column-border-position (cidx)
  "Return the buffer column of the left border `│' for column CIDX."
  (let* ((widths (clutch--effective-widths))
         (nw (clutch--row-number-digits))
         (pad clutch-column-padding)
         ;; │ + mark + row-num + padding
         (pos (+ 1 1 nw pad)))
    (dotimes (i cidx)
      (cl-incf pos (+ 1 (* 2 pad) (aref widths i))))
    pos))

(defun clutch--ensure-point-visible-horizontally ()
  "Scroll the result window so the cell at point is fully visible.
When the target column's border is outside the visible area, snap
hscroll to that column's left border so it appears at the window edge."
  (when-let* ((win (get-buffer-window (current-buffer)))
              (cidx (get-text-property (point) 'clutch-col-idx)))
    (let* ((hscroll (window-hscroll win))
           (width (max 1 (window-body-width win)))
           (widths (clutch--effective-widths))
           (pad clutch-column-padding)
           (border (clutch--column-border-position cidx))
           (col-end (+ border 1 (* 2 pad) (aref widths cidx))))
      (cond
       ;; Column border is left of visible area — page backward:
       ;; place target column at the right edge of the window.
       ((< border hscroll)
        (set-window-hscroll win (max 0 (- col-end width))))
       ;; Column right edge extends past visible area — page forward:
       ;; place target column at the left edge of the window.
       ((> col-end (+ hscroll width))
        (set-window-hscroll win border))))))

(defun clutch--goto-cell (ridx cidx)
  "Move point to the cell at ROW-IDX RIDX and COL-IDX CIDX.
Falls back to the same row (any column), then point-min."
  (let* ((line-pos (and (vectorp clutch--row-start-positions)
                        (integerp ridx)
                        (<= 0 ridx)
                        (< ridx (length clutch--row-start-positions))
                        (aref clutch--row-start-positions ridx)))
         found)
    (if line-pos
        (progn
          (goto-char line-pos)
          (let ((eol (line-end-position)))
            (setq found
                  (or (and cidx (text-property-any (point) eol 'clutch-col-idx cidx))
                      (text-property-not-all (point) eol 'clutch-col-idx nil)))
            (if found
                (goto-char found)
              (goto-char (point-min)))))
      (goto-char (point-min))
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
          (goto-char (point-min)))))
    (clutch--ensure-point-visible-horizontally)))

(defun clutch--row-number-digits ()
  "Return the digit width needed for row numbers."
  (let* ((row-count (length clutch--result-rows))
         (global-last (+ (* clutch--page-current
                            clutch-result-max-rows)
                         row-count)))
    (max 3 (length (number-to-string global-last)))))

(defun clutch--refresh-display ()
  "Re-render the current result table after width-affecting changes.
Preserves cursor position (row + column) and the top visible row."
  (when clutch--column-widths
    (let* ((save-ridx (or (get-text-property (point) 'clutch-row-idx)
                          (clutch-result--row-idx-at-line)))
           (save-cidx (get-text-property (point) 'clutch-col-idx))
           (win (get-buffer-window (current-buffer)))
           (win-width (if win (window-body-width win) 80))
           (save-top-ridx
            (when win
              (with-selected-window win
                (save-excursion
                  (goto-char (window-start win))
                  (clutch-result--row-idx-at-line))))))
      (setq clutch--last-window-width win-width)
      (setq clutch--header-active-col nil)
      (when clutch--row-overlay
        (delete-overlay clutch--row-overlay)
        (setq clutch--row-overlay nil))
      (clutch--render-result)
      (when save-ridx
        (clutch--goto-cell save-ridx save-cidx)
        (when (and win (integerp save-top-ridx))
          (with-selected-window win
            (when-let* ((top-pos (and (vectorp clutch--row-start-positions)
                                      (<= 0 save-top-ridx)
                                      (< save-top-ridx (length clutch--row-start-positions))
                                      (aref clutch--row-start-positions save-top-ridx))))
              (set-window-start win top-pos))))))))

(defun clutch--window-size-change (frame)
  "Handle window size changes for clutch display buffers in FRAME."
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
  "Remove the window-size hook when no result buffers remain.
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
    (setq-local clutch--result-source-table (clutch-result--detect-table))
    (setq-local clutch--column-widths
                (clutch--compute-column-widths
                 col-names rows columns))
    (clutch--refresh-display)))

(defun clutch--display-dml-result (result sql elapsed)
  "Render a DML RESULT (INSERT/UPDATE/DELETE) with SQL and ELAPSED time."
  (let ((inhibit-read-only t))
    (erase-buffer)
    (setq-local clutch--dml-result t)
    (setq-local clutch--result-source-table nil)
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
  "Initialize buffer-local state for a non-paginated SELECT result.
COL-NAMES and COLUMNS describe the result shape, and ROWS provides the
initial result data."
  (setq-local clutch--base-query nil)
  (setq-local clutch--result-columns col-names)
  (setq-local clutch--result-column-defs columns)
  (setq-local clutch--result-rows rows)
  (setq-local clutch--pending-edits nil)
  (setq-local clutch--pending-deletes nil)
  (setq-local clutch--pending-inserts nil)
  (setq-local clutch--marked-rows nil)
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
         (params clutch--connection-params)
         (product clutch--conn-sql-product)
         (columns  (clutch-db-result-columns result))
         (col-names (when columns (clutch--column-names columns)))
         (rows     (clutch-db-result-rows result)))
    (with-current-buffer buf
      (clutch-result-mode)
      (setq-local clutch--last-query sql)
      (clutch--bind-connection-context
       (clutch-db-result-connection result)
       params
       product)
      (if col-names
          (progn
            (clutch--init-select-result-state col-names columns rows)
            (setq-local clutch--result-column-details
                        (clutch--cached-result-column-details
                         (clutch-db-result-connection result) sql col-names))
            (clutch--queue-result-column-details-enrichment
             (clutch-db-result-connection result) sql)
            (clutch--display-select-result col-names rows columns))
        (clutch--display-dml-result result sql elapsed)))
    (clutch--show-result-buffer buf)))


(provide 'clutch-ui)
;;; clutch-ui.el ends here
