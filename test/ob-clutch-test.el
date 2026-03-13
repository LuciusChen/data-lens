;;; ob-clutch-test.el --- Tests for ob-clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Unit tests for Org-Babel clutch backend.

;;; Code:

(require 'ert)
(require 'ob-clutch)

(ert-deftest ob-clutch-test-normalize-backend-aliases ()
  "Test backend alias normalization."
  (should (eq (ob-clutch--normalize-backend 'mysql) 'mysql))
  (should (eq (ob-clutch--normalize-backend 'mariadb) 'mysql))
  (should (eq (ob-clutch--normalize-backend 'postgresql) 'pg))
  (should (eq (ob-clutch--normalize-backend "POSTGRES") 'pg))
  (should (eq (ob-clutch--normalize-backend 'sqlite) 'sqlite)))

(ert-deftest ob-clutch-test-resolve-connection-unknown-name-errors ()
  "Test unknown :connection raises explicit user error."
  (let ((clutch-connection-alist nil))
    (should-error
     (ob-clutch--resolve-connection '((:connection . "missing")) 'mysql)
     :type 'user-error)))

(ert-deftest ob-clutch-test-resolve-connection-injects-pass-entry ()
  "Test :connection defaults :pass-entry to connection name."
  (let ((clutch-connection-alist
         '(("dev" . (:backend mysql :host "127.0.0.1" :port 3306 :user "root"))))
        resolved)
    (cl-letf (((symbol-function 'ob-clutch--resolve-password)
               (lambda (params)
                 (setq resolved params)
                 "secret")))
      (pcase-let ((`(,backend . ,conn-params)
                   (ob-clutch--resolve-connection '((:connection . "dev")) 'mysql)))
        (should (eq backend 'mysql))
        (should (equal (plist-get conn-params :password) "secret"))
        (should (equal (plist-get resolved :pass-entry) "dev"))))))

(ert-deftest ob-clutch-test-resolve-connection-inline-default-port ()
  "Test inline mysql connection defaults to port 3306."
  (pcase-let ((`(,backend . ,conn-params)
               (ob-clutch--resolve-connection
                '((:host . "127.0.0.1") (:user . "u")) 'mysql)))
    (should (eq backend 'mysql))
    (should (= (plist-get conn-params :port) 3306))))

(ert-deftest ob-clutch-test-resolve-connection-inline-pass-entry ()
  "Test inline params preserve :pass-entry for password resolution."
  (let (resolved)
    (cl-letf (((symbol-function 'ob-clutch--resolve-password)
               (lambda (params)
                 (setq resolved params)
                 "secret")))
      (pcase-let ((`(,backend . ,conn-params)
                   (ob-clutch--resolve-connection
                    '((:host . "127.0.0.1")
                      (:user . "u")
                      (:pass-entry . "db/dev"))
                    'mysql)))
        (should (eq backend 'mysql))
        (should (equal (plist-get conn-params :password) "secret"))
        (should (equal (plist-get resolved :pass-entry) "db/dev"))))))

(ert-deftest ob-clutch-test-resolve-connection-errors-early-for-unresolved-jdbc-pass-entry ()
  "JDBC Org-Babel params should fail fast when explicit :pass-entry resolves to no password."
  (cl-letf (((symbol-function 'ob-clutch--resolve-password)
             (lambda (_params) nil)))
    (should-error
     (ob-clutch--resolve-connection
      '((:backend . oracle)
        (:host . "db")
        (:port . 1521)
        (:user . "scott")
        (:sid . "ORCL")
        (:pass-entry . "prod-oracle"))
      'oracle)
     :type 'user-error)))

(ert-deftest ob-clutch-test-resolve-connection-inline-sqlite-requires-database ()
  "Test sqlite inline params require :database."
  (should-error
   (ob-clutch--resolve-connection '() 'sqlite)
   :type 'user-error))

(ert-deftest ob-clutch-test-resolve-password-priority ()
  "Test password resolution priority: explicit > pass > auth-source."
  ;; explicit
  (should (equal (ob-clutch--resolve-password '(:password "p1")) "p1"))
  ;; pass
  (cl-letf (((symbol-function 'clutch-db--pass-secret-by-suffix)
             (lambda (_suffix) "p2"))
            ((symbol-function 'auth-source-search)
             (lambda (&rest _args) nil)))
    (should (equal (ob-clutch--resolve-password '(:pass-entry "dev")) "p2")))
  ;; auth-source function secret
  (cl-letf (((symbol-function 'clutch-db--pass-secret-by-suffix)
             (lambda (_suffix) nil))
            ((symbol-function 'auth-source-search)
             (lambda (&rest _args)
               (list (list :secret (lambda () "p3"))))))
    (should (equal (ob-clutch--resolve-password
                    '(:host "h" :user "u" :port 3306))
                   "p3"))))

(ert-deftest ob-clutch-test-connect-caches-live-connection ()
  "Test `ob-clutch--connect' reuses live cached connections."
  (let ((clutch-connection-alist nil)
        (ob-clutch--connection-cache (make-hash-table :test 'equal))
        (created 0))
    (cl-letf (((symbol-function 'clutch-db-connect)
               (lambda (_backend _params)
                 (cl-incf created)
                 (list :conn-id created)))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t)))
      (let* ((params '((:host . "127.0.0.1") (:user . "u")))
             (c1 (ob-clutch--connect params 'mysql))
             (c2 (ob-clutch--connect params 'mysql)))
        (should (equal c1 c2))
        (should (= created 1))))))

(provide 'ob-clutch-test)
;;; ob-clutch-test.el ends here
