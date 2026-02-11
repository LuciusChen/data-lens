;;; data-lens-transient.el --- Transient menus for data-lens -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; This file is part of data-lens.

;; data-lens is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; data-lens is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with data-lens.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Transient-based dispatch menus (Magit-style) for data-lens.
;; Provides `data-lens-dispatch' as the main entry point.
;;
;; Usage:
;;   (require 'data-lens-transient)
;;   ;; Then: M-x data-lens-dispatch
;;   ;; Or bind in data-lens-mode:
;;   ;;   (define-key data-lens-mode-map (kbd "C-c C-c") #'data-lens-dispatch)

;;; Code:

(require 'transient)
(require 'data-lens)

;;;###autoload (autoload 'data-lens-dispatch "data-lens-transient" nil t)
(transient-define-prefix data-lens-dispatch ()
  "Main dispatch menu for data-lens."
  ["Connection"
   ("c" "Connect"    data-lens-connect)
   ("d" "Disconnect" data-lens-disconnect)
   ("R" "REPL"       data-lens-repl)]
  ["Execute"
   ("x" "Query at point" data-lens-execute-query-at-point)
   ("r" "Region"         data-lens-execute-region)
   ("b" "Buffer"         data-lens-execute-buffer)
   ("l" "History"        data-lens-show-history)]
  ["Schema"
   ("t" "List tables"    data-lens-list-tables)
   ("D" "Describe table" data-lens-describe-table-at-point)])

;;;###autoload (autoload 'data-lens-result-dispatch "data-lens-transient" nil t)
(transient-define-prefix data-lens-result-dispatch ()
  "Dispatch menu for data-lens result buffer."
  ["Navigate"
   ("RET" "Open record" data-lens-result-open-record)
   ("c" "Go to column" data-lens-result-goto-column)
   ("n" "Load more"    data-lens-result-load-more)]
  ["Column Pages"
   ("]" "Next page"     data-lens-result-next-col-page)
   ("[" "Prev page"     data-lens-result-prev-col-page)
   ("+" "Widen column"  data-lens-result-widen-column)
   ("-" "Narrow column" data-lens-result-narrow-column)
   ("p" "Pin column"    data-lens-result-pin-column)
   ("P" "Unpin column"  data-lens-result-unpin-column)]
  ["Filter / Sort"
   ("W" "WHERE filter" data-lens-result-apply-filter)
   ("s" "Sort ASC"  data-lens-result-sort-by-column)
   ("S" "Sort DESC" data-lens-result-sort-by-column-desc)]
  ["Edit"
   ("e" "Edit cell"  data-lens-result-edit-cell)
   ("C" "Commit"     data-lens-result-commit)]
  ["Copy / Export"
   ("y" "Yank cell"       data-lens-result-yank-cell)
   ("w" "Row(s) as INSERT" data-lens-result-copy-row-as-insert)
   ("Y" "Row(s) as CSV"   data-lens-result-copy-as-csv)
   ("E" "Export"           data-lens-result-export)]
  ["Other"
   ("g" "Re-execute" data-lens-result-rerun)])

;;;###autoload (autoload 'data-lens-record-dispatch "data-lens-transient" nil t)
(transient-define-prefix data-lens-record-dispatch ()
  "Dispatch menu for data-lens record buffer."
  ["Navigate"
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
   ("q" "Quit"    quit-window)])

(provide 'data-lens-transient)
;;; data-lens-transient.el ends here
