;;; clutch-ui.el --- Result UI helpers for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Internal result rendering and icon helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)

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
(defvar-local clutch--header-line-string nil
  "Full header-line string before hscroll adjustment.")
(defvar-local clutch--last-window-width nil
  "Last known window body width for the current result buffer.")
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
  "Alist of pending edits: ((PK-VEC . COL-IDX) . NEW-VALUE).")
(defvar-local clutch--pending-inserts nil
  "List of field alists staged for insertion.")
(defvar-local clutch--query-elapsed nil
  "Elapsed time in seconds for the last query execution.")
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
  "Saved mode-line-format to restore after refine mode exits.")
(defvar-local clutch-record--result-buffer nil
  "Reference to the parent result buffer for record display.")
(defvar-local clutch-record--row-idx nil
  "Current row index being displayed in a record buffer.")
(defvar-local clutch-record--expanded-fields nil
  "List of expanded long field column indices in a record buffer.")
(defvar clutch-column-padding)
(defvar clutch-connection)
(defvar clutch-result-max-rows)

(declare-function clutch--bind-connection-context "clutch" (conn &optional params product))
(declare-function clutch--cell-placeholder-value "clutch" (value))
(declare-function clutch--center-padding-widths "clutch" (content-width total-width))
(declare-function clutch--column-names "clutch" (columns))
(declare-function clutch--compute-column-widths "clutch" (col-names rows columns))
(declare-function clutch--ensure-column-details "clutch-schema" (conn table))
(declare-function clutch--format-elapsed "clutch" (seconds))
(declare-function clutch--format-value "clutch" (value))
(declare-function clutch--numeric-type-p "clutch" (col-def))
(declare-function clutch--render-separator "clutch"
                  (visible-cols widths position))
(declare-function clutch--result-buffer-name "clutch" ())
(declare-function clutch--show-result-buffer "clutch" (buf))
(declare-function clutch--string-pad "clutch" (s width &optional pad-left numeric))
(declare-function clutch--tx-header-line-segment "clutch" (conn))
(declare-function clutch--trim-sql-bounds "clutch" (beg end))
(declare-function clutch--value-placeholder "clutch" (value col-def))
(declare-function clutch--visible-columns "clutch" ())
(declare-function clutch-result--detect-table "clutch-edit" ())
(declare-function clutch-result--extract-pk-vec "clutch-edit" (row pk-indices))
(declare-function clutch-result--row-idx-at-line "clutch-edit" ())
(declare-function clutch-result-mode "clutch" (&optional arg))
(declare-function clutch-db-result-affected-rows "clutch-db" (result))
(declare-function clutch-db-result-columns "clutch-db" (result))
(declare-function clutch-db-result-connection "clutch-db" (result))
(declare-function clutch-db-result-last-insert-id "clutch-db" (result))
(declare-function clutch-db-result-rows "clutch-db" (result))
(declare-function clutch-db-result-warnings "clutch-db" (result))

(defun clutch--icon (name &optional fallback &rest icon-args)
  "Return a nerd-icons icon for NAME, or FALLBACK string.
NAME is a cons (FAMILY . ICON-NAME) where FAMILY is any nerd-icons
glyph-set symbol (e.g. `mdicon', `devicon', `codicon', `octicon').
ICON-ARGS are keyword arguments forwarded to the nerd-icons function
\(e.g. :height 1.2).  The icon function is resolved dynamically via
`nerd-icons--function-name', so new families require no changes here.
Falls back gracefully when nerd-icons is not installed or unknown."
  (pcase-let ((`(,family . ,icon-name) name))
    (or (and (require 'nerd-icons nil t)
             (fboundp 'nerd-icons--function-name)
             (let ((fn (nerd-icons--function-name family)))
               (and (fboundp fn)
                    (apply fn icon-name icon-args))))
        fallback
        "")))

(defun clutch--icon-with-face (name fallback face &rest icon-args)
  "Return icon NAME/FALLBACK with FACE appended to its text properties."
  (let ((icon (apply #'clutch--icon name fallback icon-args)))
    (unless (string-empty-p icon)
      (add-face-text-property 0 (length icon) face 'append icon))
    icon))

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

(defun clutch--footer-icon (spec fallback face)
  "Return footer icon SPEC/FALLBACK with explicit FACE."
  (propertize (concat (clutch--icon spec fallback) " ") 'face face))

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

(defun clutch--render-header (visible-cols widths)
  "Render the header row string for VISIBLE-COLS with WIDTHS.
Each column name carries a `clutch-header-col' text property
so the active-column overlay can find it."
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
             (face 'clutch-header-face)
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
        ((clutch--cell-placeholder-value val) 'clutch-null-face)
        ((null val) 'clutch-null-face)
        ((assq cidx clutch--fk-info) 'clutch-fk-face)
        (t nil)))

(defun clutch--cell-display-value (val w col-def edited)
  "Return the padded display string for a cell value VAL in column width W.
COL-DEF is the column definition plist, EDITED is the pending edit cons or nil."
  (let* ((display-val (if edited (cdr edited) val))
         (special-placeholder (and (not edited)
                                   (clutch--cell-placeholder-value display-val)))
         (s (or special-placeholder
                (replace-regexp-in-string "\n" "↵"
                                          (clutch--format-value display-val))))
         (placeholder (and (not edited)
                           (not special-placeholder)
                           (> (string-width s) w)
                           (clutch--value-placeholder display-val col-def)))
         (formatted (or placeholder s)))
    (clutch--string-pad
     (if (> (string-width formatted) w)
         (truncate-string-to-width formatted w)
       formatted)
     w
     (clutch--numeric-type-p col-def))))

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
These tables avoid repeated linear scans through pending UI state while
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
  "Return pending edit entry for ROW/CIDX from RENDER-STATE, or nil."
  (let* ((edits (plist-get render-state :edits))
         (pk-indices (plist-get render-state :pk-indices)))
    (and pk-indices
         (gethash (cons (clutch-result--extract-pk-vec row pk-indices) cidx)
                  edits))))

(defun clutch--row-pending-edit-p (row _ridx render-state)
  "Return non-nil when ROW has any pending edit in RENDER-STATE."
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

(defun clutch--render-row (row ridx visible-cols widths render-state)
  "Render a single data ROW at row index RIDX.
VISIBLE-COLS is a list of column indices, WIDTHS is the width vector.
Returns a propertized string."
  (concat (mapconcat (lambda (cidx)
                       (clutch--render-cell row ridx cidx widths render-state))
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
  "Return list of main footer part strings for pagination state."
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
           (clutch--footer-pending-part)
           (when clutch--query-elapsed
             (concat (clutch--footer-icon '(mdicon . "nf-md-timer_outline") "⏱" hi)
                     (propertize (clutch--format-elapsed clutch--query-elapsed)
                                 'face hi)))))))

(defun clutch--render-footer (row-count page-num page-size total-rows)
  "Return the mode-line footer string for pagination state."
  (let ((sep (propertize "  •  " 'face 'font-lock-comment-face)))
    (mapconcat #'identity
               (append (clutch--footer-main-parts row-count page-num page-size
                                                  total-rows)
                       (clutch--footer-filter-parts))
               sep)))

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
         (face (cond
                ((eql cidx active-cidx) 'clutch-header-active-face)
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

(defun clutch--build-header-line (visible-cols widths nw &optional active-cidx)
  "Build the header-line-format string for the column header row.
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

(defun clutch--build-separator (visible-cols widths position nw)
  "Build a table separator line with row-number column.
VISIBLE-COLS and WIDTHS describe data columns.
POSITION is \\='top, \\='middle, or \\='bottom.
NW is the row-number digit width."
  (let* ((bface 'clutch-border-face)
         ;; +1 for the mark column char
         (rn-dash (+ 1 nw clutch-column-padding))
         (raw (clutch--render-separator visible-cols widths position))
         (cross (pcase position ('top "┬") ('bottom "┴") (_ "┼")))
         (left (pcase position ('top "┌") ('bottom "└") (_ "├")))
         (data-part (concat cross (substring raw 1)))
         (rn (propertize (concat left (make-string rn-dash ?─)) 'face bface)))
    (concat rn (propertize data-part 'face bface))))

(defun clutch--insert-data-rows (rows visible-cols widths nw
                                      global-first-row row-positions render-state)
  "Insert data ROWS into the current buffer.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
GLOBAL-FIRST-ROW is the 0-based offset for numbering.
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for pending UI state."
  (let ((bface 'clutch-border-face)
        (pad-str (make-string clutch-column-padding ?\s))
        (marked-table (plist-get render-state :marked))
        (delete-table (plist-get render-state :deletes))
        (pk-indices (plist-get render-state :pk-indices)))
    (cl-loop for row in rows
             for ridx from 0
             do (aset row-positions ridx (point))
             for deletingp = (and pk-indices
                                  (gethash (clutch-result--extract-pk-vec row pk-indices)
                                           delete-table))
             for editedp = (clutch--row-pending-edit-p row ridx render-state)
             for data-row = (let ((r (clutch--render-row
                                      row ridx visible-cols widths render-state)))
                               (if deletingp
                                   (propertize r 'face 'clutch-pending-delete-face)
                                 r))
             for mark-char = (cond (deletingp "D")
                                   (editedp "E")
                                   ((gethash ridx marked-table) "*")
                                   (t " "))
             for num-label = (string-pad
                              (number-to-string
                               (1+ (+ global-first-row ridx)))
                              nw nil t)
             for num-face = (cond (deletingp 'clutch-pending-delete-face)
                                  (editedp 'clutch-modified-face)
                                  ((gethash ridx marked-table) 'clutch-marked-face)
                                  (t 'shadow))
             do (insert (propertize "│" 'face bface)
                        (propertize mark-char 'face num-face)
                        (propertize num-label 'face num-face)
                        pad-str
                        data-row "\n"))))

(defun clutch--insert-pending-insert-rows (visible-cols widths nw nrows row-positions
                                                        render-state)
  "Append ghost rows for pending inserts below the real data rows.
VISIBLE-COLS, WIDTHS describe columns.  NW is row-number digit width.
NROWS is the count of real rows (used to compute ghost row indices).
ROW-POSITIONS stores line starts keyed by rendered row index.
RENDER-STATE contains render lookup tables for pending UI state."
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

(defun clutch--update-result-line-formats (rows visible-cols widths nw)
  "Set mode-line-format and header-line-format for the result buffer."
  (setq mode-line-format
        (concat (propertize " " 'display '(space :align-to 0))
                (clutch--render-footer
                 (length rows) clutch--page-current
                 clutch-result-max-rows clutch--page-total-rows)))
  (setq clutch--header-line-string
        (clutch--build-header-line visible-cols widths nw
                                   clutch--header-active-col))
  (setq header-line-format '(:eval (clutch--header-line-display))))

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
                                     nil))
         (global-first-row (* clutch--page-current clutch-result-max-rows)))
    (erase-buffer)
    (setq clutch--row-start-positions row-positions)
    (clutch--update-result-line-formats rows visible-cols widths nw)
    (clutch--insert-data-rows rows visible-cols widths nw global-first-row
                              row-positions render-state)
    (clutch--insert-pending-insert-rows visible-cols widths nw (length rows)
                                        row-positions render-state)
    (if save-ridx
        (clutch--goto-cell save-ridx save-cidx)
      (goto-char (point-min)))))

(defun clutch--col-idx-at-point ()
  "Return the column index at point, from data cells."
  (get-text-property (point) 'clutch-col-idx))

(defun clutch--ensure-point-visible-horizontally ()
  "Scroll the selected result window horizontally when point leaves view."
  (when-let* ((win (get-buffer-window (current-buffer))))
    (let* ((margin 2)
           (col (current-column))
           (hscroll (window-hscroll win))
           (width (max 1 (window-body-width win))))
      (cond
       ((< col (+ hscroll margin))
        (set-window-hscroll win (max 0 (- col margin))))
       ((>= col (- (+ hscroll width) margin))
        (set-window-hscroll
         win
         (max 0 (- col width (1- margin)))))))))

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
    (setq-local clutch--dml-result t)
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
            (clutch--display-select-result col-names rows columns))
        (clutch--display-dml-result result sql elapsed)))
    (clutch--show-result-buffer buf)))


(provide 'clutch-ui)
;;; clutch-ui.el ends here
