;;; mysql-transient.el --- Transient menus for mysql-interactive -*- lexical-binding: t; -*-

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

;; Transient-based dispatch menus (Magit-style) for mysql-interactive.
;; Provides `mysql-dispatch' as the main entry point.
;;
;; Usage:
;;   (require 'mysql-transient)
;;   ;; Then: M-x mysql-dispatch
;;   ;; Or bind in mysql-mode:
;;   ;;   (define-key mysql-mode-map (kbd "C-c C-c") #'mysql-dispatch)

;;; Code:

(require 'transient)
(require 'mysql-interactive)

;;;###autoload (autoload 'mysql-dispatch "mysql-transient" nil t)
(transient-define-prefix mysql-dispatch ()
  "Main dispatch menu for MySQL."
  ["Connection"
   ("c" "Connect"    mysql-connect-interactive)
   ("d" "Disconnect" mysql-interactive-disconnect)
   ("R" "REPL"       mysql-repl)]
  ["Execute"
   ("x" "Query at point" mysql-execute-query-at-point)
   ("r" "Region"         mysql-execute-region)
   ("b" "Buffer"         mysql-execute-buffer)
   ("l" "History"        mysql-interactive-show-history)]
  ["Schema"
   ("t" "List tables"    mysql-list-tables)
   ("D" "Describe table" mysql-describe-table-at-point)])

;;;###autoload (autoload 'mysql-result-dispatch "mysql-transient" nil t)
(transient-define-prefix mysql-result-dispatch ()
  "Dispatch menu for MySQL result buffer."
  ["Navigate"
   ("c" "Go to column" mysql-result-goto-column)
   ("v" "Vertical view" mysql-result-toggle-vertical)
   ("n" "Load more"    mysql-result-load-more)]
  ["Sort"
   ("s" "Sort ASC"  mysql-result-sort-by-column)
   ("S" "Sort DESC" mysql-result-sort-by-column-desc)]
  ["Edit"
   ("e" "Edit cell"  mysql-result-edit-cell)
   ("C" "Commit"     mysql-result-commit)]
  ["Copy / Export"
   ("y" "Yank cell"      mysql-result-yank-cell)
   ("w" "Row as INSERT"  mysql-result-copy-row-as-insert)
   ("E" "Export"         mysql-result-export)]
  ["Other"
   ("g" "Re-execute" mysql-result-rerun)])

(provide 'mysql-transient)
;;; mysql-transient.el ends here
