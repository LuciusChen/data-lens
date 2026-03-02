;;; ob-clutch.el --- Org-Babel support via clutch-db -*- lexical-binding: t; -*-

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
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Org-Babel backend for MySQL/PostgreSQL/SQLite via clutch-db.
;;
;; Supported block types:
;;   #+begin_src mysql
;;   #+begin_src postgresql
;;   #+begin_src sqlite
;;
;; Optional generic block:
;;   #+begin_src clutch :backend pg
;;
;; Header arguments:
;;   :connection                name from `clutch-connection-alist'
;;   :backend                   mysql|pg|postgresql|sqlite (for src clutch)
;;   :host :port :user :password :database

;;; Code:

(require 'ob)
(require 'auth-source)
(require 'cl-lib)
(require 'clutch-db)

(defvar clutch-connection-alist nil)

(defvar org-babel-default-header-args:clutch '((:results . "table"))
  "Default header arguments for clutch source blocks.")

(defvar org-babel-default-header-args:mysql '((:results . "table"))
  "Default header arguments for mysql source blocks.")

(defvar org-babel-default-header-args:postgresql '((:results . "table"))
  "Default header arguments for postgresql source blocks.")

(defvar org-babel-default-header-args:sqlite '((:results . "table"))
  "Default header arguments for sqlite source blocks.")

(defvar ob-clutch--connection-cache (make-hash-table :test 'equal)
  "Cache of live DB connections keyed by backend+connection parameters.")

(defconst ob-clutch--meta-keys
  '(:backend :sql-product :pass-entry)
  "Connection plist keys not passed to backend connect functions.")

(defun ob-clutch--pass-secret-by-suffix (suffix)
  "Return pass secret from the first entry whose path ends with SUFFIX."
  (when (and (fboundp 'auth-source-pass-entries)
             (fboundp 'auth-source-pass-parse-entry))
    (let* ((re (format "\\(^\\|/\\)%s$" (regexp-quote suffix)))
           (entry (cl-find-if (lambda (e) (string-match-p re e))
                              (auth-source-pass-entries))))
      (when entry
        (cdr (assq 'secret (auth-source-pass-parse-entry entry)))))))

(defun ob-clutch--resolve-password (params)
  "Resolve password for PARAMS via :password, pass, then auth-source."
  (let ((pw (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (> (length pw) 0)) pw)
     (t
      (or (and entry (ob-clutch--pass-secret-by-suffix entry))
          (when-let* ((found (car (auth-source-search
                                   :host (plist-get params :host)
                                   :user (plist-get params :user)
                                   :port (plist-get params :port)
                                   :max 1)))
                      (secret (plist-get found :secret)))
            (if (functionp secret) (funcall secret) secret)))))))

(defun ob-clutch--inject-entry-name (params name)
  "Return PARAMS with :pass-entry defaulting to NAME when needed."
  (if (or (plist-get params :password) (plist-get params :pass-entry))
      params
    (append params (list :pass-entry name))))

(defun ob-clutch--normalize-backend (backend)
  "Normalize BACKEND to one of symbols: mysql, pg, sqlite."
  (let ((sym (if (stringp backend)
                 (intern (downcase backend))
               backend)))
    (pcase sym
      ((or 'mysql 'mariadb) 'mysql)
      ((or 'pg 'postgres 'postgresql) 'pg)
      ('sqlite 'sqlite)
      (_ (user-error "Unsupported backend: %s" backend)))))

(defun ob-clutch--plist-without-meta (plist)
  "Return copy of PLIST excluding keys in `ob-clutch--meta-keys'."
  (let (out)
    (while plist
      (let ((k (pop plist))
            (v (pop plist)))
        (unless (memq k ob-clutch--meta-keys)
          (setq out (append out (list k v))))))
    out))

(defun ob-clutch--inline-params (params backend)
  "Build inline connection params from Babel PARAMS for BACKEND."
  (pcase backend
    ('sqlite
     (let ((db (cdr (assq :database params))))
       (unless db
         (user-error "Missing :database for sqlite block"))
       (list :database db)))
    (_
     (let ((host (or (cdr (assq :host params)) "127.0.0.1"))
           (port (or (cdr (assq :port params))
                     (if (eq backend 'pg) 5432 3306)))
           (user (cdr (assq :user params)))
           (password (cdr (assq :password params)))
           (database (cdr (assq :database params))))
       (unless user
         (user-error "Missing :user (or use :connection)"))
       (append (list :host host
                     :port (if (stringp port) (string-to-number port) port)
                     :user user)
               (when password (list :password password))
               (when database (list :database database)))))))

(defun ob-clutch--resolve-connection (params default-backend)
  "Return (BACKEND . CONN-PARAMS) from Babel PARAMS.
DEFAULT-BACKEND is used by language-specific executors."
  (if-let* ((conn-name (cdr (assq :connection params))))
      (let* ((entry (or (assoc conn-name clutch-connection-alist)
                        (user-error "Unknown connection: %s" conn-name)))
             (plist (copy-sequence (cdr entry)))
             (plist (ob-clutch--inject-entry-name plist conn-name))
             (backend (ob-clutch--normalize-backend
                       (or (plist-get plist :backend) default-backend)))
             (conn-params (ob-clutch--plist-without-meta plist))
             (pw (and (not (eq backend 'sqlite))
                      (ob-clutch--resolve-password plist))))
        (cons backend (if pw (plist-put conn-params :password pw) conn-params)))
    (let* ((backend (ob-clutch--normalize-backend
                     (or (cdr (assq :backend params)) default-backend)))
           (conn-params (ob-clutch--inline-params params backend)))
      (if (eq backend 'sqlite)
          (cons backend conn-params)
        (let ((pw (ob-clutch--resolve-password conn-params)))
          (cons backend (if pw (plist-put conn-params :password pw) conn-params)))))))

(defun ob-clutch--connect (params default-backend)
  "Get or create a cached clutch-db connection for PARAMS."
  (pcase-let* ((`(,backend . ,conn-params)
                (ob-clutch--resolve-connection params default-backend))
               (key (format "%S:%S" backend conn-params))
               (cached (gethash key ob-clutch--connection-cache)))
    (if (and cached (clutch-db-live-p cached))
        cached
      (let ((conn (clutch-db-connect backend conn-params)))
        (puthash key conn ob-clutch--connection-cache)
        conn))))

(defun ob-clutch--format-value (val)
  "Format VAL for Org-Babel table output."
  (cond
   ((null val) "NULL")
   ((numberp val) val)
   ((stringp val) val)
   ((and (listp val) (plist-get val :year) (plist-get val :hours))
    (format "%04d-%02d-%02d %02d:%02d:%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   ((and (listp val) (plist-get val :year))
    (format "%04d-%02d-%02d"
            (plist-get val :year) (plist-get val :month) (plist-get val :day)))
   ((and (listp val) (plist-get val :hours))
    (format "%s%02d:%02d:%02d"
            (if (plist-get val :negative) "-" "")
            (plist-get val :hours) (plist-get val :minutes) (plist-get val :seconds)))
   (t (format "%S" val))))

(defun ob-clutch--execute (body params default-backend)
  "Execute BODY with Babel PARAMS using DEFAULT-BACKEND."
  (let* ((conn (ob-clutch--connect params default-backend))
         (sql (org-babel-expand-body:generic body params))
         (result (clutch-db-query conn sql))
         (columns (clutch-db-result-columns result))
         (rows (clutch-db-result-rows result)))
    (if columns
        (let ((col-names (mapcar (lambda (c)
                                   (let ((name (plist-get c :name)))
                                     (if (stringp name) name (format "%s" name))))
                                 columns))
              (data (mapcar (lambda (row)
                              (mapcar #'ob-clutch--format-value row))
                            rows)))
          (cons col-names (cons 'hline data)))
      (format "Affected rows: %s"
              (or (clutch-db-result-affected-rows result) 0)))))

(defun org-babel-execute:clutch (body params)
  "Execute a generic clutch BODY with Babel PARAMS."
  (ob-clutch--execute body params
                      (or (cdr (assq :backend params))
                          (user-error "Missing :backend for clutch block"))))

(defun org-babel-execute:mysql (body params)
  "Execute a MySQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'mysql))

(defun org-babel-execute:postgresql (body params)
  "Execute a PostgreSQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'pg))

(defun org-babel-execute:sqlite (body params)
  "Execute a SQLite BODY with Babel PARAMS."
  (ob-clutch--execute body params 'sqlite))

(provide 'ob-clutch)
;;; ob-clutch.el ends here
