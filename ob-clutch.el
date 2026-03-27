;;; ob-clutch.el --- Org-Babel support via clutch-db -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

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

;; Org-Babel backend for MySQL/PostgreSQL/SQLite/JDBC databases via clutch-db.
;;
;; Supported block types:
;;   #+begin_src mysql
;;   #+begin_src postgresql
;;   #+begin_src sqlite
;;
;; Optional generic block (supports all backends including JDBC):
;;   #+begin_src clutch :backend pg
;;   #+begin_src clutch :backend oracle
;;   #+begin_src clutch :backend sqlserver
;;
;; Header arguments:
;;   :connection                name from `clutch-connection-alist'
;;   :backend                   mysql|pg|postgresql|sqlite|oracle|sqlserver|...
;;   :host :port :user :password :database
;;
;; JDBC backends (oracle, sqlserver, db2, snowflake, redshift) require
;; clutch-db-jdbc.el to be loaded and clutch-jdbc-agent.jar to be available.
;; Use :connection to reference a pre-configured clutch-connection-alist entry,
;; or supply :host/:port/:user/:database inline.  Port defaults to the JDBC
;; driver default when omitted.

;;; Code:

(require 'ob)
(require 'auth-source)
(require 'cl-lib)
(require 'clutch-db)

(defvar clutch-connection-alist nil
  "Forward declaration; defined as `defcustom' in clutch.el.")

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

(defun ob-clutch--resolve-password (params)
  "Resolve password for PARAMS via :password, pass, then auth-source."
  (let ((pw (plist-get params :password))
        (entry (plist-get params :pass-entry)))
    (cond
     ((and (stringp pw) (> (length pw) 0)) pw)
     (t
      (or (and entry (clutch-db--pass-secret-by-suffix entry))
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
  "Normalize BACKEND string or symbol for clutch-db-connect.
Pure Elisp backends are canonicalized (mysql/pg/sqlite).
Any other symbol is passed through as-is; clutch-db-connect will signal
an error if the backend is truly unknown.  This allows JDBC driver symbols
(oracle, sqlserver, db2, snowflake, redshift, etc.) to work without
ob-clutch needing to know about clutch-db-jdbc."
  (let ((sym (if (stringp backend)
                 (intern (downcase backend))
               backend)))
    (pcase sym
      ((or 'mysql 'mariadb) 'mysql)
      ((or 'pg 'postgres 'postgresql) 'pg)
      ('sqlite 'sqlite)
      (_ sym))))

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
                     (pcase backend ('pg 5432) ('mysql 3306) (_ nil))))
           (user (cdr (assq :user params)))
           (password (cdr (assq :password params)))
           (database (cdr (assq :database params))))
       (unless user
         (user-error "Missing :user (or use :connection)"))
       (let ((port-num (and port (if (stringp port) (string-to-number port) port))))
         (append (list :host host :user user)
                 (when port-num (list :port port-num))
                 (when password (list :password password))
                 (when database (list :database database))))))))

(defun ob-clutch--maybe-inject-password (backend conn-params source-params)
  "Return CONN-PARAMS with password injected unless BACKEND is sqlite.
SOURCE-PARAMS is the plist used for password resolution."
  (if (eq backend 'sqlite)
      conn-params
    (if-let* ((pw (ob-clutch--resolve-password source-params)))
        (plist-put conn-params :password pw)
      conn-params)))

(defun ob-clutch--guard-jdbc-pass-entry (backend source-params conn-params)
  "Fail early when JDBC BACKEND has an unresolved explicit :pass-entry.
Return CONN-PARAMS unchanged otherwise."
  (when (and (memq backend '(oracle sqlserver db2 snowflake redshift))
             (plist-get source-params :pass-entry)
             (null (plist-get conn-params :password)))
    (user-error
     (concat "No password resolved for JDBC Org-Babel block %s (:pass-entry %s). "
             "Enable auth-source-pass/auth-source, or set :password explicitly")
     backend
     (plist-get source-params :pass-entry)))
  conn-params)

(defun ob-clutch--resolve-connection (params default-backend)
  "Return (BACKEND . CONN-PARAMS) from Babel PARAMS.
DEFAULT-BACKEND is used by language-specific executors."
    (if-let* ((conn-name (cdr (assq :connection params))))
      (let* ((entry (or (assoc conn-name clutch-connection-alist)
                        (user-error "Unknown connection: %s" conn-name)))
             (plist (copy-sequence (cdr entry)))
             (plist (ob-clutch--inject-entry-name plist conn-name))
             (backend (ob-clutch--normalize-backend
                       (or (plist-get plist :backend) default-backend 'mysql)))
             (conn-params (ob-clutch--plist-without-meta plist)))
        (cons backend
              (ob-clutch--guard-jdbc-pass-entry
               backend plist
               (ob-clutch--maybe-inject-password backend conn-params plist))))
    (let* ((backend-sym (or (cdr (assq :backend params)) default-backend))
           (backend (or (and backend-sym
                             (ob-clutch--normalize-backend backend-sym))
                        (user-error
                         "Missing :backend for clutch block (or use :connection to reference a saved connection)")))
           (conn-params (ob-clutch--inline-params params backend))
           (source-params (append (ob-clutch--plist-without-meta params)
                                  (when-let* ((entry (cdr (assq :pass-entry params))))
                                    (list :pass-entry entry)))))
      (cons backend
            (ob-clutch--guard-jdbc-pass-entry
             backend source-params
             (ob-clutch--maybe-inject-password backend conn-params source-params))))))

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
   ((numberp val) val)           ; raw number for Org column alignment
   ((stringp val) val)
   ((listp val) (or (clutch-db-format-temporal val) (format "%S" val)))
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
                      (cdr (assq :backend params))))

(defun org-babel-execute:mysql (body params)
  "Execute a MySQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'mysql))

(defun org-babel-execute:postgresql (body params)
  "Execute a PostgreSQL BODY with Babel PARAMS."
  (ob-clutch--execute body params 'pg))

(defun org-babel-execute:sqlite (body params)
  "Execute a SQLite BODY with Babel PARAMS."
  (ob-clutch--execute body params 'sqlite))

;;;; Cleanup on exit

(defun ob-clutch--disconnect-all ()
  "Disconnect all cached ob-clutch connections and clear the cache."
  (maphash (lambda (_key conn)
             (condition-case nil
                 (clutch-db-disconnect conn)
               (error nil)))
           ob-clutch--connection-cache)
  (clrhash ob-clutch--connection-cache))

(add-hook 'kill-emacs-hook #'ob-clutch--disconnect-all)

(provide 'ob-clutch)
;;; ob-clutch.el ends here
