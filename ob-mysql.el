;;; ob-mysql.el --- Org-Babel support for MySQL via mysql.el -*- lexical-binding: t; -*-

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

;; Org-Babel backend for MySQL using the pure Elisp driver (mysql.el).
;; No external mysql/mariadb CLI required.
;;
;; Usage in .org files:
;;
;;   #+begin_src mysql :connection dev-db
;;   SELECT * FROM users LIMIT 10;
;;   #+end_src
;;
;;   #+begin_src mysql :host 127.0.0.1 :port 3306 :user root :password secret :database mydb
;;   SHOW TABLES;
;;   #+end_src
;;
;; Header arguments:
;;   :connection  — name from `data-lens-connection-alist'
;;   :host :port :user :password :database — inline connection params
;;
;; Setup:
;;   (with-eval-after-load 'org
;;     (org-babel-do-load-languages
;;      'org-babel-load-languages
;;      '((mysql . t))))

;;; Code:

(require 'ob)
(require 'mysql)

(defvar data-lens-connection-alist)

(defvar org-babel-default-header-args:mysql '((:results . "table"))
  "Default header arguments for MySQL source blocks.")

(defvar ob-mysql--connection-cache (make-hash-table :test 'equal)
  "Cache of open MySQL connections keyed by connection parameters.")

(defun ob-mysql--connect (params)
  "Get or create a MySQL connection from PARAMS.
PARAMS is the Babel params alist."
  (let* ((conn-name (cdr (assq :connection params)))
         (conn-params
          (if conn-name
              (or (cdr (assoc conn-name data-lens-connection-alist))
                  (user-error "Unknown connection: %s" conn-name))
            (let ((host (or (cdr (assq :host params)) "127.0.0.1"))
                  (port (or (cdr (assq :port params)) 3306))
                  (user (cdr (assq :user params)))
                  (password (cdr (assq :password params)))
                  (database (cdr (assq :database params))))
              (unless user
                (user-error "Missing :user (or use :connection)"))
              (append (list :host host :port (if (stringp port)
                                                  (string-to-number port)
                                                port)
                            :user user)
                      (when password (list :password password))
                      (when database (list :database database))))))
         (key (format "%S" conn-params))
         (cached (gethash key ob-mysql--connection-cache)))
    (if (and cached
             (mysql-conn-p cached)
             (process-live-p (mysql-conn-process cached)))
        cached
      (let ((conn (apply #'mysql-connect conn-params)))
        (mysql-query conn "SET NAMES utf8mb4")
        (puthash key conn ob-mysql--connection-cache)
        conn))))

(defun org-babel-execute:mysql (body params)
  "Execute a MySQL BODY with Babel PARAMS."
  (let* ((conn (ob-mysql--connect params))
         (sql (org-babel-expand-body:generic body params))
         (result (mysql-query conn sql))
         (columns (mysql-result-columns result))
         (rows (mysql-result-rows result)))
    (if columns
        ;; SELECT-like: return table with header
        (let ((col-names (mapcar (lambda (c) (plist-get c :name)) columns))
              (data (mapcar (lambda (row)
                              (mapcar #'ob-mysql--format-value row))
                            rows)))
          (cons col-names (cons 'hline data)))
      ;; DML: return summary
      (format "Affected rows: %s"
              (or (mysql-result-affected-rows result) 0)))))

(defun ob-mysql--format-value (val)
  "Format VAL for Org-Babel table output."
  (cond
   ((null val) "NULL")
   ((numberp val) val)
   ((stringp val) val)
   ;; datetime plist
   ((and (listp val) (plist-get val :year) (plist-get val :hours))
    (format "%04d-%02d-%02d %02d:%02d:%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   ;; date plist
   ((and (listp val) (plist-get val :year))
    (format "%04d-%02d-%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)))
   ;; time plist
   ((and (listp val) (plist-get val :hours))
    (format "%s%02d:%02d:%02d"
            (if (plist-get val :negative) "-" "")
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   (t (format "%S" val))))

(provide 'ob-mysql)
;;; ob-mysql.el ends here
