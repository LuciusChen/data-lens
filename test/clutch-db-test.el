;;; clutch-db-test.el --- ERT tests for database backends -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch-db generic database interface.
;;
;; Unit tests run without a database server.
;; Live tests require both MySQL and PostgreSQL:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;   docker run -d -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
;;
;; Note: MySQL 8 defaults to `caching_sha2_password'.  The native mysql
;; client retries with TLS when the server requires a secure channel; local
;; container certificates are typically self-signed, so the MySQL live helpers
;; bind `mysql-tls-verify-server' to nil unless the test environment installs a
;; trusted CA.
;;
;; Run unit tests:
;;   Emacs -batch -L .. -l ert -l clutch-db-test \
;;     -f ert-run-tests-batch-and-exit
;;
;; Run live tests:
;;   Emacs -batch -L .. -l ert -l clutch-db-test \
;;     --eval '(setq clutch-db-test-mysql-password "test")' \
;;     --eval '(setq clutch-db-test-pg-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'eieio)
(require 'clutch-db)
(require 'clutch-db-jdbc)

(eval-when-compile
  (require 'pg))

;; `clutch--schema-cache' lives in clutch.el (the UI layer), which is not
;; loaded in this test batch.  Declare it special here so that `let' bindings
;; in the cache-based completion tests create dynamic (not lexical) bindings
;; that the bytecode in clutch-db-jdbc.el can see.
(defvar clutch--schema-cache (make-hash-table :test 'equal))
(defvar clutch-debug-buffer-name "*clutch-debug*")
;; `mysql-tls-verify-server' is defined in mysql.el; declare it
;; special here so local test bindings remain dynamic even before the backend
;; requires mysql.el.
(defvar mysql-tls-verify-server)
(defvar clutch-connection)
(defvar clutch-jdbc--error-details-by-conn)
(defvar clutch-jdbc--busy-request-ids)
(defvar clutch-jdbc--ignored-response-ids)
(defvar mysql-type-long)
(defvar mysql-type-float)
(defvar mysql-type-double)
(defvar mysql-type-decimal)
(defvar mysql-type-longlong)
(defvar mysql-type-date)
(defvar mysql-type-time)
(defvar mysql-type-datetime)
(defvar mysql-type-timestamp)
(defvar mysql-type-blob)
(defvar mysql-type-json)
(declare-function clutch-db-mysql--type-category "clutch-db-mysql" (type character-set))
(declare-function clutch-db-mysql--convert-columns "clutch-db-mysql" (columns))
(declare-function clutch-db-mysql-connect "clutch-db-mysql" (params))
(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))
(declare-function clutch-db-pg--convert-columns "clutch-db-pg" (columns))
(declare-function clutch-db-pg--wrap-result "clutch-db-pg" (result))
(declare-function clutch-db-pg-connect "clutch-db-pg" (params))
(declare-function clutch-db-sqlite-connect "clutch-db-sqlite" (params))
(declare-function clutch--jdbc-backend-p "clutch" (backend))
(declare-function clutch--backend-display-name-from-params "clutch" (params))
(declare-function clutch--build-conn "clutch" (params))
(declare-function clutch--format-value "clutch" (value))
(declare-function clutch--value-to-literal "clutch" (value))
(declare-function make-mysql-conn "mysql" (&rest args))
(declare-function make-mysql-result "mysql" (&rest args))
(declare-function mysql-conn-database "mysql" (conn))
(declare-function make-pgcon "pg" (&rest args))
(declare-function make-pgresult "pg" (&rest args))
(declare-function pgcon-connect-plist "pg" (conn))
(declare-function pgcon-dbname "pg" (conn))
(declare-function sqlite-available-p "sqlite" ())

;;;; Test configuration

(defvar clutch-db-test-mysql-host "127.0.0.1")
(defvar clutch-db-test-mysql-port 3306)
(defvar clutch-db-test-mysql-user "root")
(defvar clutch-db-test-mysql-password nil)
(defvar clutch-db-test-mysql-database "mysql")

(defvar clutch-db-test-pg-host "127.0.0.1")
(defvar clutch-db-test-pg-port 5432)
(defvar clutch-db-test-pg-user "postgres")
(defvar clutch-db-test-pg-password nil)
(defvar clutch-db-test-pg-database "postgres")

(defconst clutch-db-test--pg-oid-bytea 17)
(defconst clutch-db-test--pg-oid-int8 20)
(defconst clutch-db-test--pg-oid-int4 23)
(defconst clutch-db-test--pg-oid-json 114)
(defconst clutch-db-test--pg-oid-float8 701)
(defconst clutch-db-test--pg-oid-date 1082)
(defconst clutch-db-test--pg-oid-time 1083)
(defconst clutch-db-test--pg-oid-timestamp 1114)
(defconst clutch-db-test--pg-oid-timestamptz 1184)
(defconst clutch-db-test--pg-oid-numeric 1700)
(defconst clutch-db-test--pg-oid-jsonb 3802)

(defun clutch-db-test--make-pgcon (&rest params)
  "Return a lightweight upstream `pgcon' built from PARAMS."
  (let* ((dbname (or (plist-get params :database)
                     (plist-get params :dbname)
                     "test"))
         (conn (make-pgcon :dbname dbname :process nil)))
    (oset conn connect-plist
          (list 'method :tcp
                'host (or (plist-get params :host) "localhost")
                'port (or (plist-get params :port) 5432)
                'dbname dbname
                'user (plist-get params :user)
                'password (plist-get params :password)))
    conn))

(defmacro clutch-db-test--with-local-mysql-tls (&rest body)
  "Run BODY with MySQL TLS verification disabled for local self-signed certs."
  (declare (indent 0))
  `(progn
     (require 'mysql)
     (cl-letf (((symbol-value 'mysql-tls-verify-server) nil))
       ,@body)))

(defmacro clutch-db-test--with-immediate-idle-metadata (scheduled-var &rest body)
  "Run BODY with idle metadata work executing immediately.
Capture the scheduled idle timer shape in SCHEDULED-VAR and treat the
connection as live and not busy."
  (declare (indent 1))
  `(cl-letf (((symbol-function 'run-with-idle-timer)
              (lambda (secs repeat fn &rest args)
                (setq ,scheduled-var (list secs repeat))
                (apply fn args)
                'fake-timer))
             ((symbol-function 'clutch-db-busy-p)
              (lambda (_conn) nil))
             ((symbol-function 'clutch-db-live-p)
              (lambda (_conn) t)))
     ,@body))

;;;; Unit tests — connection parameter normalization

(ert-deftest clutch-db-test-normalize-mysql-connect-params-canonicalizes-disabled ()
  "MySQL plaintext options should normalize to `:ssl-mode disabled'."
  (let ((params (clutch-db--normalize-connect-params
                 'mysql
                 '(:host "127.0.0.1" :tls nil :ssl-mode off))))
    (should (eq (plist-get params :clutch-tls-mode) 'disable))
    (should (eq (plist-get params :ssl-mode) 'disabled))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-mysql-connect-params-rejects-conflict ()
  "MySQL conflicting TLS options should fail early."
  (should-error
   (clutch-db--normalize-connect-params
    'mysql '(:host "127.0.0.1" :tls t :ssl-mode disabled))
   :type 'clutch-db-error))

(ert-deftest clutch-db-test-normalize-pg-connect-params-canonicalizes-require ()
  "PostgreSQL `:tls t' should normalize to `:sslmode require'."
  (let ((params (clutch-db--normalize-connect-params
                 'pg '(:host "127.0.0.1" :tls t))))
    (should (eq (plist-get params :clutch-tls-mode) 'require))
    (should (eq (plist-get params :sslmode) 'require))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-pg-connect-params-preserves-prefer ()
  "PostgreSQL `:sslmode prefer' should stay in official form."
  (let ((params (clutch-db--normalize-connect-params
                 'pg '(:host "127.0.0.1" :sslmode "prefer"))))
    (should (eq (plist-get params :clutch-tls-mode) 'prefer))
    (should (eq (plist-get params :sslmode) 'prefer))
    (should-not (plist-member params :tls))))

(ert-deftest clutch-db-test-normalize-pg-connect-params-rejects-unsupported-mode ()
  "Unsupported PostgreSQL sslmodes should fail early."
  (should-error
   (clutch-db--normalize-connect-params
    'pg '(:host "127.0.0.1" :sslmode verify-ca))
   :type 'clutch-db-error))

;;;; Unit tests — clutch-db-result struct

(ert-deftest clutch-db-test-result-struct ()
  "Test clutch-db-result struct creation and accessors."
  (let ((result (make-clutch-db-result
                 :connection 'fake-conn
                 :columns '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
                 :rows '((1 "alice") (2 "bob"))
                 :affected-rows 2
                 :last-insert-id 42
                 :warnings 0)))
    (should (clutch-db-result-p result))
    (should (eq (clutch-db-result-connection result) 'fake-conn))
    (should (= (length (clutch-db-result-columns result)) 2))
    (should (= (length (clutch-db-result-rows result)) 2))
    (should (= (clutch-db-result-affected-rows result) 2))
    (should (= (clutch-db-result-last-insert-id result) 42))
    (should (= (clutch-db-result-warnings result) 0))))

(ert-deftest clutch-db-test-result-empty ()
  "Test clutch-db-result with empty/nil values."
  (let ((result (make-clutch-db-result
                 :columns '((:name "v" :type-category numeric))
                 :rows nil
                 :affected-rows 0)))
    (should (clutch-db-result-p result))
    (should (null (clutch-db-result-connection result)))
    (should (null (clutch-db-result-rows result)))
    (should (= (clutch-db-result-affected-rows result) 0))
    (should (null (clutch-db-result-last-insert-id result)))))

(ert-deftest clutch-db-test-jdbc-fetch-all-preserves-row-order ()
  "JDBC fetch-all should preserve batch order while avoiding repeated tail scans."
  (let ((batches '((:rows (("alpha" 17) ("beta" 23)) :done nil)
                   (:rows (("omega" -9)) :done t)))
        (conn (make-clutch-jdbc-conn :params '(:rpc-timeout 9))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (should (equal op "fetch"))
                 (should (= (alist-get 'cursor-id params) 9))
                 (should (= timeout-seconds 9))
                 (pop batches))))
      (should (equal (clutch-jdbc--fetch-all conn 9)
                     '(("alpha" 17) ("beta" 23) ("omega" -9)))))))

(ert-deftest clutch-db-test-jdbc-fetch-all-maps-query-and-rpc-timeouts ()
  "JDBC fetch-all should pass the same effective query timeout as execute."
  (let ((conn (make-clutch-jdbc-conn :params '(:rpc-timeout 15
                                               :query-timeout 16)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:rows nil :done t))))
      (should (equal (clutch-jdbc--fetch-all conn 9) nil))
      (should (equal captured-op "fetch"))
      (should (= (alist-get 'cursor-id captured-params) 9))
      (should (= captured-timeout 15))
      ;; min(16, max(1, 15-5)) = min(16, 10) = 10
      (should (= (alist-get 'query-timeout-seconds captured-params) 10)))))

(ert-deftest clutch-db-test-jdbc-referencing-objects-maps-rpc-response ()
  "JDBC reverse-reference lookup should map RPC rows to object entries."
  (let (captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout-seconds)
                 (setq captured-op op
                       captured-params params)
                 '(:objects ((:name "SALES_ORDERS" :schema "APP"
                              :source-schema "APP_OWNER")
                             (:name "INVOICE_ITEMS" :schema "BILLING"))))))
      (should (equal
               (clutch-db-referencing-objects
                (make-clutch-jdbc-conn :conn-id 7 :params '(:backend oracle :schema "APP"))
                "CUSTOMERS")
               '((:name "SALES_ORDERS" :type "TABLE"
                  :schema "APP" :source-schema "APP_OWNER")
                 (:name "INVOICE_ITEMS" :type "TABLE"
                  :schema "BILLING" :source-schema "BILLING"))))
      (should (equal captured-op "get-referencing-objects"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should (equal (alist-get 'table captured-params) "CUSTOMERS"))
      (should (equal (alist-get 'schema captured-params) "APP")))))

(ert-deftest clutch-db-test-jdbc-connect-maps-timeouts ()
  "JDBC connect should map explicit timeout phases to the agent call."
  (let ((clutch-jdbc-oracle-manual-commit t)
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7))))
      (let ((conn (clutch-db-jdbc-connect
                   'oracle
                   '(:host "db"
                     :port 1521
                     :database "svc"
                     :user "scott"
                     :password "tiger"
                     :connect-timeout 7
                     :read-idle-timeout 23
                     :rpc-timeout 41))))
        (should (equal captured-op "connect"))
        (should (= captured-timeout 7))
        (should (eq (alist-get 'auto-commit captured-params) clutch-jdbc--json-false))
        (should (= (alist-get 'connect-timeout-seconds captured-params) 7))
        (should (= (alist-get 'network-timeout-seconds captured-params) 23))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 41))
        (should (= (clutch-jdbc-conn-conn-id conn) 7))))))

(ert-deftest clutch-db-test-jdbc-connect-non-oracle-sends-autocommit-true ()
  "Non-Oracle JDBC connect should keep auto-commit enabled by default."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 8))))
      (clutch-db-jdbc-connect
       'sqlserver
       '(:host "db" :port 1433 :database "app" :user "sa" :password "secret"))
      (should (eq (alist-get 'auto-commit captured-params) t)))))

(ert-deftest clutch-db-test-jdbc-connect-oracle-global-autocommit-override ()
  "Oracle connect should honor the global manual-commit default override."
  (let ((clutch-jdbc-oracle-manual-commit nil)
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 9))))
      (clutch-db-jdbc-connect
       'oracle
       '(:host "db" :port 1521 :database "svc" :user "scott" :password "tiger"))
      (should (eq (alist-get 'auto-commit captured-params) t)))))

(ert-deftest clutch-db-test-jdbc-connect-defaults-connect-timeout-separately-from-rpc ()
  "Direct JDBC connect should not inherit connect timeout from rpc timeout."
  (let ((clutch-connect-timeout-seconds 10)
        (clutch-read-idle-timeout-seconds 30)
        (clutch-query-timeout-seconds 20)
        (clutch-jdbc-rpc-timeout-seconds 30)
        captured-timeout captured-params conn)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional timeout-seconds)
                 (setq captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7))))
      (setq conn
            (clutch-db-jdbc-connect
             'oracle
             '(:host "db" :port 1521 :database "svc" :user "scott" :password "tiger")))
      (should (= captured-timeout 10))
      (should (= (alist-get 'connect-timeout-seconds captured-params) 10))
      (should (= (alist-get 'network-timeout-seconds captured-params) 30))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :connect-timeout) 10))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :read-idle-timeout) 30))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :query-timeout) 20))
      (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 30)))))

(ert-deftest clutch-db-test-jdbc-query-maps-query-and-rpc-timeouts ()
  "JDBC query clamps query-timeout to rpc-timeout - 5 when it exceeds the margin."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:rpc-timeout 15
                                               :query-timeout 16)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:type "dml" :affected-rows 1))))
      (let ((result (clutch-db-query conn "delete from t")))
        (should (equal captured-op "execute"))
        (should (= captured-timeout 15))
        ;; min(16, max(1, 15-5)) = min(16, 10) = 10
        (should (= (alist-get 'query-timeout-seconds captured-params) 10))
        (should (= (clutch-db-result-affected-rows result) 1))))))

(ert-deftest clutch-db-test-jdbc-query-does-not-clamp-when-within-margin ()
  "JDBC query should not clamp query-timeout when it fits within rpc-timeout - 5."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:rpc-timeout 15
                                               :query-timeout 8)))
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn _op params &optional _timeout)
                 (setq captured-params params)
                 '(:type "dml" :affected-rows 0))))
      (clutch-db-query conn "delete from t where 1=0")
      ;; min(8, max(1, 10)) = min(8, 10) = 8 — no clamping
      (should (= (alist-get 'query-timeout-seconds captured-params) 8)))))

(ert-deftest clutch-db-test-jdbc-query-clamps-query-timeout-to-rpc-minus-five ()
  "JDBC query should clamp query-timeout to rpc-timeout - 5 in the default case."
  (let ((conn (make-clutch-jdbc-conn :conn-id 6
                                     :params '(:rpc-timeout 30
                                               :query-timeout 30)))
        captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc-on-conn)
               (lambda (_conn _op params &optional _timeout)
                 (setq captured-params params)
                 '(:type "dml" :affected-rows 1))))
      (clutch-db-query conn "update t set x = 1")
      ;; min(30, max(1, 25)) = min(30, 25) = 25
      (should (= (alist-get 'query-timeout-seconds captured-params) 25)))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-oracle ()
  "Oracle JDBC connections should default to manual-commit mode."
  (let ((clutch-jdbc-oracle-manual-commit t)
        (conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott"))))
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-sqlserver ()
  "Non-Oracle JDBC connections should default to auto-commit mode."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver sqlserver :user "sa"))))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-manual-commit-p-oracle-global-override ()
  "Oracle JDBC connections should respect the global default override."
  (let ((clutch-jdbc-oracle-manual-commit nil)
        (conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott"))))
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-commit-fires-rpc ()
  "Clutch-db-commit should issue a commit RPC with the connection id."
  (let ((conn (make-clutch-jdbc-conn :conn-id 17
                                     :params '(:driver oracle :rpc-timeout 12)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 17))))
      (clutch-db-commit conn)
      (should (equal captured-op "commit"))
      (should (= (alist-get 'conn-id captured-params) 17))
      (should (= captured-timeout 12)))))

(ert-deftest clutch-db-test-jdbc-rollback-fires-rpc ()
  "Clutch-db-rollback should issue a rollback RPC with the connection id."
  (let ((conn (make-clutch-jdbc-conn :conn-id 18
                                     :params '(:driver oracle :rpc-timeout 13)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 18))))
      (clutch-db-rollback conn)
      (should (equal captured-op "rollback"))
      (should (= (alist-get 'conn-id captured-params) 18))
      (should (= captured-timeout 13)))))

(ert-deftest clutch-db-test-jdbc-set-auto-commit-fires-rpc ()
  "Clutch-db-set-auto-commit should issue set-auto-commit RPC with auto-commit value."
  (let ((conn (make-clutch-jdbc-conn :conn-id 19
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 19 :auto-commit t))))
      (clutch-db-set-auto-commit conn t)
      (should (equal captured-op "set-auto-commit"))
      (should (= (alist-get 'conn-id captured-params) 19))
      (should (eq (alist-get 'auto-commit captured-params) t))
      (should (= captured-timeout 12)))))

(ert-deftest clutch-db-test-jdbc-set-auto-commit-updates-params ()
  "Clutch-db-set-auto-commit should update :manual-commit in conn params."
  (let ((conn (make-clutch-jdbc-conn :conn-id 20
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc) (lambda (_op _params &optional _to) nil)))
      ;; Switch to auto-commit: manual-commit should become nil
      (clutch-db-set-auto-commit conn t)
      (should-not (plist-get (clutch-jdbc-conn-params conn) :manual-commit))
      ;; Switch back to manual-commit: manual-commit should become t
      (clutch-db-set-auto-commit conn nil)
      (should (plist-get (clutch-jdbc-conn-params conn) :manual-commit)))))

(ert-deftest clutch-db-test-native-mysql-manual-commit-follows-autocommit ()
  "Native MySQL manual-commit should mirror session autocommit state."
  (require 'clutch-db-mysql)
  (should-not
   (clutch-db-manual-commit-p
    (make-mysql-conn :status-flags #x0002)))
  (should
   (clutch-db-manual-commit-p
    (make-mysql-conn :status-flags 0))))

(ert-deftest clutch-db-test-native-mysql-transaction-methods-delegate ()
  "Native MySQL transaction methods should delegate to mysql.el helpers."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306))
        captured-auto-commit
        committed
        rolled-back)
    (cl-letf (((symbol-function 'mysql-set-autocommit)
               (lambda (_conn v)
                 (setq captured-auto-commit v)))
              ((symbol-function 'mysql-commit)
               (lambda (_conn)
                 (setq committed t)))
              ((symbol-function 'mysql-rollback)
               (lambda (_conn)
                 (setq rolled-back t))))
      (clutch-db-set-auto-commit conn nil)
      (should-not captured-auto-commit)
      (clutch-db-commit conn)
      (clutch-db-rollback conn)
      (should committed)
      (should rolled-back))))

(ert-deftest clutch-db-test-native-pg-toggle-enables-manual-mode ()
  "Native PostgreSQL should enter manual-commit mode after toggling autocommit off."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test")))
    (should-not (clutch-db-manual-commit-p conn))
    (clutch-db-set-auto-commit conn nil)
    (should (clutch-db-manual-commit-p conn))
    (clutch-db-set-auto-commit conn t)
    (should-not (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-native-pg-manual-mode-lazy-begin ()
  "Native PostgreSQL manual-commit should lazily BEGIN on the first foreground query."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (pcase sql
                   ("BEGIN"
                    (make-pgresult :connection conn :status "BEGIN"))
                   ("SELECT 1"
                    (make-pgresult :connection conn
                                   :status "SELECT 1"
                                   :attributes '(("n" 23 4))
                                   :tuples '((1))))
                   ("SELECT 2"
                    (make-pgresult :connection conn
                                   :status "SELECT 1"
                                   :attributes '(("n" 23 4))
                                   :tuples '((2))))
                   (_
                    (ert-fail (format "Unexpected SQL: %s" sql)))))))
      (let ((result1 (clutch-db-query conn "SELECT 1"))
            (result2 (clutch-db-query conn "SELECT 2")))
        (should (equal (nreverse calls)
                       '("BEGIN" "SELECT 1" "SELECT 2")))
        (should (= (caar (clutch-db-result-rows result1)) 1))
        (should (= (caar (clutch-db-result-rows result2)) 2))))))

(ert-deftest clutch-db-test-native-pg-toggle-auto-commit-rolls-back-failed-transaction ()
  "Native PostgreSQL should roll back an aborted manual transaction before enabling autocommit."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (pcase sql
                   ("BEGIN"
                    (make-pgresult :connection conn :status "BEGIN"))
                   ("UPDATE demo SET x = 1"
                    (signal 'pg-error '("statement failed")))
                   ("ROLLBACK"
                    (make-pgresult :connection conn :status "ROLLBACK"))
                   (_
                    (ert-fail (format "Unexpected SQL: %s" sql)))))))
      (should-error (clutch-db-query conn "UPDATE demo SET x = 1")
                    :type 'clutch-db-error)
      (clutch-db-set-auto-commit conn t)
      (should (equal (nreverse calls)
                     '("BEGIN" "UPDATE demo SET x = 1" "ROLLBACK")))
      (should-not (clutch-db-manual-commit-p conn)))))

(ert-deftest clutch-db-test-native-pg-transaction-control-allows-leading-comments ()
  "Native PostgreSQL should not inject lazy BEGIN before commented transaction control."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        calls)
    (clutch-db-set-auto-commit conn nil)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (push sql calls)
                 (make-pgresult :connection conn
                                :status
                                (cond
                                 ((string-match-p "COMMIT" sql) "COMMIT")
                                 ((string-match-p "BEGIN" sql) "BEGIN")
                                 (t "ROLLBACK"))))))
      (clutch-db-query conn "/* lead */ COMMIT")
      (clutch-db-query conn "-- lead\nBEGIN")
      (should (equal (nreverse calls)
                     '("/* lead */ COMMIT" "-- lead\nBEGIN"))))))

(ert-deftest clutch-db-test-native-pg-ctid-identity-reads-relkind-cell ()
  "PostgreSQL CTID identity should read the relkind cell, not its first char."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test")))
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn _sql)
                 (make-pgresult :connection conn
                                :status "SELECT 1"
                                :attributes '(("relkind" 25 -1))
                                :tuples '(("r"))))))
      (should (equal (clutch-db-pg--ctid-identity conn "demo")
                     '(:kind row-locator
                       :name "ctid"
                       :select-expressions ("ctid::text")
                       :where-sql "ctid = ?::tid"))))))

(ert-deftest clutch-db-test-sqlite-rowid-identity-in-memory ()
  "SQLite rowid tables should expose `rowid' as a row locator."
  (skip-unless (require 'clutch-db-sqlite nil t))
  (skip-unless (and (fboundp 'sqlite-available-p)
                    (sqlite-available-p)))
  (let* ((db-file (make-temp-file "clutch-sqlite-rowid-" nil ".db"))
         (conn (clutch-db-sqlite-connect (list :database db-file))))
    (unwind-protect
        (progn
          (clutch-db-query conn "CREATE TABLE demo (name TEXT)")
          (let ((candidate (car (clutch-db-row-identity-candidates
                                 conn "demo"))))
            (should (equal (plist-get candidate :kind) 'row-locator))
            (should (equal (plist-get candidate :name) "rowid"))
            (should (equal (plist-get candidate :select-expressions)
                           '("rowid")))
            (should (equal (plist-get candidate :where-sql) "rowid = ?"))))
      (clutch-db-disconnect conn)
      (ignore-errors (delete-file db-file)))))

(ert-deftest clutch-db-test-jdbc-show-create-table-uses-oracle-style-identifiers ()
  "Oracle synthesized JDBC DDL should quote only identifiers that need it."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:driver oracle :schema "CLUTCH"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout-seconds)
                 '(:columns ((:name "PK_MAIN" :type "CHAR" :nullable :json-false)
                             (:name "TYPE" :type "VARCHAR2" :nullable :json-false)
                             (:name "ACTION" :type "VARCHAR2" :nullable :json-false)
                             (:name "mixedCase" :type "VARCHAR2" :nullable :json-false))))))
      (let ((ddl (clutch-db-show-create-table conn "ZJ_NCBUSINESSDATA")))
        (should (string-match-p "CREATE TABLE ZJ_NCBUSINESSDATA" ddl))
        (should (string-match-p "PK_MAIN CHAR" ddl))
        (should (string-match-p "\"TYPE\" VARCHAR2" ddl))
        (should (string-match-p "\"ACTION\" VARCHAR2" ddl))
        (should (string-match-p "\"mixedCase\" VARCHAR2" ddl))
        (should-not (string-match-p "\"ZJ_NCBUSINESSDATA\"" ddl))
        (should-not (string-match-p "\"PK_MAIN\"" ddl))))))

(ert-deftest clutch-db-test-jdbc-oracle-row-identity-falls-back-to-rowid ()
  "Oracle JDBC should offer ROWID when no logical key is available."
  (let ((conn (make-clutch-jdbc-conn :conn-id 4
                                     :params '(:driver oracle
                                               :schema "CLUTCH"))))
    (cl-letf (((symbol-function 'clutch-db-primary-key-columns)
               (lambda (_conn _table) nil))
              ((symbol-function 'clutch-jdbc--unique-not-null-identities)
               (lambda (_conn _table) nil)))
      (should (equal (clutch-db-row-identity-candidates conn "DEMO")
                     '((:kind row-locator
                        :name "ROWID"
                        :select-expressions ("ROWID")
                        :where-sql "ROWID = ?")))))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-returns-table-names ()
  "Async JDBC schema refresh should return only table names to its callback."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params `(:driver oracle :user "scott"
                                               :rpc-timeout ,clutch-jdbc-rpc-timeout-seconds)))
        callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params callback &optional errback _timeout-seconds _conn)
                 (should-not errback)
                 (funcall callback '(:cursor-id nil
                                    :columns ("name" "type" "schema" "source_schema")
                                    :rows (("USERS" "TABLE" "SCOTT" "SCOTT")
                                           ("ORDERS_VIEW" "VIEW" "SCOTT" "SCOTT")
                                           ("PUBLIC_ORDERS" "SYNONYM" "APP" "PUBLIC")
                                           ("ORDERS" "TABLE" "SCOTT" "SCOTT"))
                                    :done t))
                 42)))
      (should (clutch-db-refresh-schema-async
               conn
               (lambda (tables)
                 (setq callback-result tables))))
      (should (equal callback-result '("USERS" "ORDERS"))))))

(ert-deftest clutch-db-test-jdbc-list-table-entries-preserves-source-schema ()
  "JDBC table entry listing should preserve schema/source metadata."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "app"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (&rest _args)
                 '(:cursor-id nil
                   :columns ("name" "type" "schema" "source_schema")
                   :rows (("ORDERS" "SYNONYM" "DATA_OWNER" "APP")
                          ("USER_TABLES" "PUBLIC SYNONYM" "SYS" "PUBLIC"))
                   :done t))))
      (let ((entries (clutch-db-list-table-entries conn)))
        (should (equal entries
                       '((:name "ORDERS" :type "SYNONYM" :schema "DATA_OWNER" :source-schema "APP")
                         (:name "USER_TABLES" :type "PUBLIC SYNONYM" :schema "SYS" :source-schema "PUBLIC"))))))))

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-uses-connection-rpc-timeout ()
  "Async JDBC schema refresh should respect per-connection rpc timeout."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "scott"
                                               :rpc-timeout 7)))
        captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params _callback &optional _errback timeout-seconds _conn)
                 (setq captured-timeout timeout-seconds)
                 42)))
      (should (clutch-db-refresh-schema-async conn #'ignore))
      (should (= captured-timeout 7)))))

(ert-deftest clutch-db-test-mysql-refresh-schema-async-schedules-idle-call ()
  "Native MySQL schema refresh should schedule idle work on the main thread."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                                    :user "root" :database "mysql"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-tables)
               (lambda (context)
                 (should (eq context conn))
                 '("users" "orders"))))
        (should (eq (clutch-db-refresh-schema-async
                     conn
                     (lambda (tables)
                       (setq callback-result tables)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result '("users" "orders")))))))

(ert-deftest clutch-db-test-pg-refresh-schema-async-schedules-idle-call ()
  "Native PostgreSQL schema refresh should schedule idle work on the main thread."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-tables)
               (lambda (context)
                 (should (eq context conn))
                 '("customers" "orders"))))
        (should (eq (clutch-db-refresh-schema-async
                     conn
                     (lambda (tables)
                       (setq callback-result tables)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result '("customers" "orders")))))))

(ert-deftest clutch-db-test-idle-metadata-call-reschedules-while-busy ()
  "Idle metadata calls should not run on a busy connection.
They should reschedule and only execute FN after `clutch-db-busy-p' becomes nil."
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        (scheduled 0)
        callback-result
        (busy-states '(t nil)))
    (cl-letf (((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn &rest args)
                 (setq scheduled (1+ scheduled))
                 (apply fn args)
                 'fake-timer))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn)
                 (prog1 (car busy-states)
                   (setq busy-states (or (cdr busy-states) '(nil))))))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '("id" "name"))))
      (should (eq (clutch-db--schedule-idle-metadata-call
                   conn
                   (lambda (columns)
                     (setq callback-result columns))
                   nil
                   #'clutch-db-list-columns
                   "users")
                  'fake-timer))
      (should (= scheduled 2))
      (should (equal callback-result '("id" "name"))))))

(ert-deftest clutch-db-test-mysql-list-columns-async-schedules-idle-call ()
  "Native MySQL column-name preheat should schedule idle work."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                                    :user "root" :database "mysql"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '("id" "name"))))
        (should (eq (clutch-db-list-columns-async
                     conn "users"
                     (lambda (columns)
                       (setq callback-result columns)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result '("id" "name")))))))

(ert-deftest clutch-db-test-mysql-column-details-async-schedules-idle-call ()
  "Native MySQL column-detail preheat should schedule idle work."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                                    :user "root" :database "mysql"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-column-details)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '((:name "id" :type "bigint")))))
        (should (eq (clutch-db-column-details-async
                     conn "users"
                     (lambda (details)
                       (setq callback-result details)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result
                       '((:name "id" :type "bigint"))))))))

(ert-deftest clutch-db-test-mysql-table-comment-async-schedules-idle-call ()
  "Native MySQL table-comment preheat should schedule idle work."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                                    :user "root" :database "mysql"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-table-comment)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 "Users table")))
        (should (eq (clutch-db-table-comment-async
                     conn "users"
                     (lambda (comment)
                       (setq callback-result comment)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result "Users table"))))))

(ert-deftest clutch-db-test-mysql-list-objects-async-schedules-idle-call ()
  "Native MySQL object warmup should schedule idle work."
  (require 'clutch-db-mysql)
  (let ((conn (make-mysql-conn :host "127.0.0.1" :port 3306
                                    :user "root" :database "mysql"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-objects)
               (lambda (context category)
                 (should (eq context conn))
                 (should (eq category 'indexes))
                 '((:name "users_pkey" :type "INDEX")))))
        (should (eq (clutch-db-list-objects-async
                     conn 'indexes
                     (lambda (entries)
                       (setq callback-result entries)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result
                       '((:name "users_pkey" :type "INDEX"))))))))

(ert-deftest clutch-db-test-pg-list-columns-async-schedules-idle-call ()
  "Native PostgreSQL column-name preheat should schedule idle work."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-columns)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '("id" "name"))))
        (should (eq (clutch-db-list-columns-async
                     conn "users"
                     (lambda (columns)
                       (setq callback-result columns)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result '("id" "name")))))))

(ert-deftest clutch-db-test-pg-column-details-async-schedules-idle-call ()
  "Native PostgreSQL column-detail preheat should schedule idle work."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-column-details)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 '((:name "id" :type "integer")))))
        (should (eq (clutch-db-column-details-async
                     conn "users"
                     (lambda (details)
                       (setq callback-result details)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result
                       '((:name "id" :type "integer"))))))))

(ert-deftest clutch-db-test-pg-table-comment-async-schedules-idle-call ()
  "Native PostgreSQL table-comment preheat should schedule idle work."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-table-comment)
               (lambda (context table)
                 (should (eq context conn))
                 (should (equal table "users"))
                 "Users table")))
        (should (eq (clutch-db-table-comment-async
                     conn "users"
                     (lambda (comment)
                       (setq callback-result comment)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result "Users table"))))))

(ert-deftest clutch-db-test-pg-list-objects-async-schedules-idle-call ()
  "Native PostgreSQL object warmup should schedule idle work."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                          :user "postgres" :database "test"))
        callback-result
        scheduled)
    (clutch-db-test--with-immediate-idle-metadata scheduled
      (cl-letf (((symbol-function 'clutch-db-list-objects)
               (lambda (context category)
                 (should (eq context conn))
                 (should (eq category 'indexes))
                 '((:name "users_pkey" :type "INDEX")))))
        (should (eq (clutch-db-list-objects-async
                     conn 'indexes
                     (lambda (entries)
                       (setq callback-result entries)))
                    'fake-timer))
        (should (equal scheduled '(0 nil)))
        (should (equal callback-result
                       '((:name "users_pkey" :type "INDEX"))))))))

(ert-deftest clutch-db-test-jdbc-list-columns-async-maps-column-names ()
  "JDBC async column-name preheat should normalize the returned names."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "scott")))
        callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (_op _params callback &optional _errback _timeout _conn)
                 (funcall callback
                          '(:columns ((:name "ID" :type "NUMBER")
                                      (:name "NAME" :type "VARCHAR2"))))
                 42)))
      (should (clutch-db-list-columns-async
               conn "USERS"
               (lambda (columns)
                 (setq callback-result columns))))
      (should (equal callback-result '("ID" "NAME"))))))

;;;; Unit tests — clutch-jdbc--collect-table-entries

(ert-deftest clutch-db-test-jdbc-collect-table-entries-direct ()
  "When :tables is present, collect-table-entries returns it directly."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott")))
        fetch-called)
    (cl-letf (((symbol-function 'clutch-jdbc--fetch-all)
               (lambda (_conn _cursor-id)
                 (setq fetch-called t)
                 '())))
      (let ((entries (clutch-jdbc--collect-table-entries
                      conn
                      '(:tables ((:name "USERS" :type "TABLE" :schema "SCOTT")
                                 (:name "ORDERS" :type "TABLE" :schema "SCOTT"))))))
        (should (equal entries '((:name "USERS" :type "TABLE" :schema "SCOTT")
                                 (:name "ORDERS" :type "TABLE" :schema "SCOTT"))))
        (should-not fetch-called)))))

(ert-deftest clutch-db-test-jdbc-collect-table-entries-legacy-cursor ()
  "Legacy cursor-format results are normalized to entry plists."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver oracle :user "scott")))
        fetch-cursor-id)
    (cl-letf (((symbol-function 'clutch-jdbc--fetch-all)
               (lambda (_conn cursor-id)
                 (setq fetch-cursor-id cursor-id)
                 '(("PRODUCTS" "TABLE" "SCOTT")))))
      (let ((entries (clutch-jdbc--collect-table-entries
                      conn
                      '(:rows (("USERS" "TABLE" "SCOTT"))
                        :cursor-id 42
                        :done nil))))
        (should (equal entries '((:name "USERS" :type "TABLE" :schema "SCOTT" :source-schema "SCOTT")
                                 (:name "PRODUCTS" :type "TABLE" :schema "SCOTT" :source-schema "SCOTT"))))
        (should (= fetch-cursor-id 42))))))

(ert-deftest clutch-db-test-jdbc-list-table-entries-keeps-object-types ()
  "JDBC list-table-entries should preserve view and synonym metadata."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver oracle :user "scott"))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout)
                 '(:tables ((:name "USERS" :type "TABLE" :schema "SCOTT")
                            (:name "USER_VIEW" :type "VIEW" :schema "SCOTT")
                            (:name "USER_SYM" :type "SYNONYM" :schema "SCOTT"
                                    :target-schema "APP" :target-name "USERS"))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "USERS" :type "TABLE" :schema "SCOTT")
                (:name "USER_VIEW" :type "VIEW" :schema "SCOTT")
                (:name "USER_SYM" :type "SYNONYM" :schema "SCOTT"
                        :target-schema "APP" :target-name "USERS")))))))

(ert-deftest clutch-db-test-jdbc-conn-catalog-clickhouse-defaults-to-database ()
  "ClickHouse JDBC metadata should use :database as the default catalog."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default"))))
    (should (equal (clutch-jdbc--conn-catalog conn) "default"))))

(ert-deftest clutch-db-test-jdbc-clickhouse-list-table-entries-omits-catalog ()
  "ClickHouse table discovery should omit catalog from JDBC metadata RPC."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op
                       captured-params params)
                 '(:tables ((:name "events" :type "TABLE" :schema "")
                            (:name "daily_mv" :type "VIEW" :schema ""))))))
      (should
       (equal (clutch-db-list-table-entries conn)
              '((:name "events" :type "TABLE" :schema "")
                (:name "daily_mv" :type "VIEW" :schema ""))))
      (should (equal captured-op "get-tables"))
      (should-not (alist-get 'catalog captured-params))
      (should-not (alist-get 'schema captured-params)))))

(ert-deftest clutch-db-test-jdbc-clickhouse-search-table-entries-omits-catalog ()
  "ClickHouse table search should omit catalog from JDBC metadata RPC."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op
                       captured-params params)
                 '(:tables ((:name "clutch_live_smoke" :type "TABLE"
                              :schema "" :source-schema ""))))))
      (should
       (equal (clutch-db-search-table-entries conn "clutch")
              '((:name "clutch_live_smoke" :type "TABLE"
                 :schema "" :source-schema ""))))
      (should (equal captured-op "search-tables"))
      (should-not (alist-get 'catalog captured-params))
      (should (equal (alist-get 'prefix captured-params) "clutch"))
      (should-not (alist-get 'schema captured-params)))))

(ert-deftest clutch-db-test-jdbc-clickhouse-column-details-omit-catalog ()
  "ClickHouse column metadata RPCs should omit catalog from metadata calls."
  (let ((conn (make-clutch-jdbc-conn :conn-id 5
                                     :params '(:driver clickhouse
                                               :database "default")))
        calls)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (push (cons op params) calls)
                 (pcase op
                   ("get-primary-keys" '(:primary-keys ("id")))
                   ("get-foreign-keys" '(:foreign-keys nil))
                   ("get-columns" `(:columns ((:name "id" :type "UInt64"
                                               :nullable ,clutch-jdbc--json-false)
                                              (:name "name" :type "String"
                                               :nullable t))))
                   (_ (ert-fail (format "unexpected op: %s" op)))))))
      (should
       (equal (clutch-db-column-details conn "events")
              '((:name "id" :type "UInt64" :nullable nil
                 :primary-key t :foreign-key nil :comment nil)
                (:name "name" :type "String" :nullable t
                 :primary-key nil :foreign-key nil :comment nil))))
      (dolist (call calls)
        (should-not (alist-get 'catalog (cdr call)))
        (should-not (alist-get 'schema (cdr call)))))))

;;;; Unit tests — clutch-db-complete-tables (Oracle, cache-first)

(ert-deftest clutch-db-test-jdbc-complete-tables-uses-cache ()
  "When schema cache is populated, complete-tables filters locally without RPC."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 5
                                      :params '(:driver oracle :user "scott")))
         (clutch--schema-cache (make-hash-table :test 'equal))
         (schema (make-hash-table :test 'equal))
         rpc-called)
    (puthash "USERS" nil schema)
    (puthash "ORDERS" nil schema)
    (puthash "USER_ROLES" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "test-key"))
              ((symbol-function 'clutch--schema-status-entry)
               (lambda (_conn) '(:state ready)))
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (&rest _) (setq rpc-called t) nil)))
      (puthash "test-key" schema clutch--schema-cache)
      (let ((result (clutch-db-complete-tables conn "US")))
        (should-not rpc-called)
        (should (equal (sort (copy-sequence result) #'string<)
                       '("USERS" "USER_ROLES")))))))

(ert-deftest clutch-db-test-jdbc-complete-tables-ready-empty-cache-fallback-rpc ()
  "Oracle completion should fall back to RPC when the ready cache has no matches."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 5
                                      :params '(:driver oracle :user "scott")))
         (clutch--schema-cache (make-hash-table :test 'equal))
         (schema (make-hash-table :test 'equal))
         captured-op)
    (puthash "AUDIT_LOG" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "test-key"))
              ((symbol-function 'clutch--schema-status-entry)
               (lambda (_conn) '(:state ready)))
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op)
                 (should (equal (alist-get 'prefix params) "OR"))
                 '(:tables ((:name "ORDERS"))))))
      (puthash "test-key" schema clutch--schema-cache)
      (let ((result (clutch-db-complete-tables conn "OR")))
        (should (equal captured-op "search-tables"))
        (should (equal result '("ORDERS")))))))

(ert-deftest clutch-db-test-jdbc-complete-tables-stale-cache-fallback-rpc ()
  "Oracle completion should ignore stale cache entries and fall back to RPC."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 5
                                      :params '(:driver oracle :user "scott")))
         (clutch--schema-cache (make-hash-table :test 'equal))
         (schema (make-hash-table :test 'equal))
         captured-op)
    (puthash "USERS" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "test-key"))
              ((symbol-function 'clutch--schema-status-entry)
               (lambda (_conn) '(:state stale)))
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op)
                 (should (equal (alist-get 'prefix params) "US"))
                 '(:tables ((:name "USERS_NEW"))))))
      (puthash "test-key" schema clutch--schema-cache)
      (let ((result (clutch-db-complete-tables conn "US")))
        (should (equal captured-op "search-tables"))
        (should (equal result '("USERS_NEW")))))))

(ert-deftest clutch-db-test-jdbc-complete-tables-fallback-rpc ()
  "When schema cache is nil for this connection, complete-tables fires search-tables RPC."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 5
                                      :params '(:driver oracle :user "scott")))
         (clutch--schema-cache (make-hash-table :test 'equal))
         captured-op)
    ;; Cache is empty for this connection key
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "test-key"))
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout)
                 (setq captured-op op)
                 (should (equal (alist-get 'prefix params) "US"))
                 '(:tables ((:name "USERS") (:name "USER_ROLES"))))))
      (let ((result (clutch-db-complete-tables conn "US")))
        (should (equal captured-op "search-tables"))
        (should (equal (sort (copy-sequence result) #'string<)
                       '("USERS" "USER_ROLES")))))))

(ert-deftest clutch-db-test-jdbc-rpc-async-times-out-and-cleans-up ()
  "Async JDBC RPC should call ERRBACK and clear state on timeout."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        (clutch-jdbc-rpc-timeout-seconds 1)
        timeout-message
        timer-fn)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params) 77))
              ((symbol-function 'run-at-time)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer)))
      (clutch-jdbc--rpc-async
       "get-tables" '((conn-id . 1))
       #'ignore
       (lambda (message)
         (setq timeout-message message)))
      (funcall timer-fn)
      (should (string-match-p "timeout waiting for async response" timeout-message))
      (should-not (gethash 77 clutch-jdbc--async-callbacks)))))

(ert-deftest clutch-db-test-jdbc-dispatch-async-response-remembers-conn-scoped-errors ()
  "Async JDBC failures should remember diagnostics and call ERRBACK."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        remembered
        errback-message)
    (puthash 77 (list :errback (lambda (message)
                                 (setq errback-message message))
                      :conn 'conn-77
                      :op 'get-tables)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'run-at-time)
               (lambda (_secs _repeat fn &rest args)
                 (apply fn args)
                 nil))
              ((symbol-function 'clutch-jdbc--remember-error-response)
               (lambda (conn op response)
                 (setq remembered (list conn op response))))
              ((symbol-function 'message)
               (lambda (&rest _args) nil)))
      (should (clutch-jdbc--dispatch-async-response
               '(:id 77 :ok :json-false :error "metadata blew up")))
      (should (equal remembered
                     '(conn-77 get-tables
                               (:id 77 :ok :json-false :error "metadata blew up"))))
      (should (equal errback-message "metadata blew up")))))

(ert-deftest clutch-db-test-jdbc-validate-agent-jar-rejects-mismatch ()
  "JDBC agent startup should reject a jar with the wrong checksum."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (clutch-jdbc-agent-sha256 "deadbeef"))
    (unwind-protect
        (progn
          (with-temp-file jar
            (insert "not a release jar"))
          (should-error (clutch-jdbc--validate-agent-jar jar) :type 'user-error))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-cleans-stale-jars ()
  "Ensuring the agent should keep only the current versioned jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (stale-a (expand-file-name "clutch-jdbc-agent-0.1.0.jar" tmpdir))
         (stale-b (expand-file-name "clutch-jdbc-agent-0.1.1.jar" tmpdir))
         (clutch-jdbc-agent-sha256 nil))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar
            (insert "current"))
          (with-temp-file stale-a
            (insert "old-a"))
          (with-temp-file stale-b
            (insert "old-b"))
          (clutch-jdbc-ensure-agent)
          (should (file-exists-p jar))
          (should-not (file-exists-p stale-a))
          (should-not (file-exists-p stale-b)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-ensure-agent-allows-custom-jar-when-checksum-disabled ()
  "Checksum verification can be disabled for a local custom jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (clutch-jdbc-agent-sha256 nil)
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir)))
    (unwind-protect
        (progn
          (with-temp-file jar
            (insert "custom build"))
          (should (clutch-jdbc--agent-jar-valid-p jar))
          (should (progn (clutch-jdbc--validate-agent-jar jar) t)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-requires-agent-command ()
  "Missing JDBC agent should instruct the user to run the explicit install command."
  (let ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
        (clutch-jdbc-agent-version "0.1.2")
        (clutch-jdbc-agent-dir nil))
    (setq clutch-jdbc-agent-dir tmpdir)
    (unwind-protect
        (let ((err (should-error (clutch-jdbc--setup-prerequisites 'oracle)
                                 :type 'user-error)))
          (should (string-match-p
                   "Run M-x clutch-jdbc-ensure-agent"
                   (cadr err))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-points-to-install-driver ()
  "Missing Maven drivers should point users at `clutch-jdbc-install-driver'."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2"))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar (insert "placeholder"))
          (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                     (lambda (_jar) t)))
            (let ((err (should-error
                        (clutch-jdbc--setup-prerequisites 'sqlserver)
                        :type 'user-error)))
              (should (string-match-p
                       "Run M-x clutch-jdbc-install-driver RET sqlserver"
                       (cadr err))))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-setup-prerequisites-reports-manual-driver-url ()
  "Missing manual JDBC drivers should report the download URL and destination."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-agent-" t))
         (jar (expand-file-name "clutch-jdbc-agent-0.1.2.jar" tmpdir))
         (clutch-jdbc-agent-dir tmpdir)
         (clutch-jdbc-agent-version "0.1.2")
         (dest (expand-file-name "db2jcc4.jar" (expand-file-name "drivers" tmpdir))))
    (unwind-protect
        (progn
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file jar (insert "placeholder"))
          (cl-letf (((symbol-function 'clutch-jdbc--agent-jar-valid-p)
                     (lambda (_jar) t)))
            (let ((err (should-error (clutch-jdbc--setup-prerequisites 'db2)
                                     :type 'user-error)))
              (should (string-match-p "requires manual download" (cadr err)))
              (should (string-match-p "ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads"
                                      (cadr err)))
              (should (string-match-p (regexp-quote dest) (cadr err))))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-oracle-i18n-companion ()
  "Installing Oracle JDBC should also install the orai18n companion jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         downloaded)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (_coords dest)
                     (push (file-name-nondirectory dest) downloaded)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'oracle)
          (should (member "ojdbc8.jar" downloaded))
          (should (member "orai18n.jar" downloaded)))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-removes-conflicting-oracle-jar ()
  "Installing an Oracle driver should remove the conflicting Oracle jar."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (_coords dest)
                     (with-temp-file dest (insert "jar")))))
          (make-directory (expand-file-name "drivers" tmpdir) t)
          (with-temp-file (expand-file-name "drivers/ojdbc11.jar" tmpdir)
            (insert "jar"))
          (clutch-jdbc-install-driver 'oracle)
          (should (file-exists-p (expand-file-name "drivers/ojdbc8.jar" tmpdir)))
          (should-not (file-exists-p (expand-file-name "drivers/ojdbc11.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-install-driver-uses-sqlserver-jre11-artifact ()
  "Installing SQL Server JDBC should use the classifier-based Maven artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'sqlserver)
          (should (equal requested-coords
                         "com.microsoft.sqlserver:mssql-jdbc:13.4.0.jre11"))
          (should (file-exists-p (expand-file-name "drivers/mssql-jdbc.jar" tmpdir))))
      (delete-directory tmpdir t))))

;;;; Unit tests — props normalization

(ert-deftest clutch-db-test-jdbc-normalize-props-converts-plist ()
  "A plist :props should be converted to an alist before JSON encoding."
  (should (equal (clutch-jdbc--normalize-props '(:role "reporting" :schema "HR"))
                 '(("role" . "reporting") ("schema" . "HR"))))
  (should (equal (clutch-jdbc--normalize-props '(:key "val"))
                 '(("key" . "val"))))
  (should (null  (clutch-jdbc--normalize-props nil))))

(ert-deftest clutch-db-test-jdbc-normalize-props-passes-alist-through ()
  "An alist :props should be passed through unchanged."
  (let ((alist '(("role" . "reporting") ("schema" . "HR"))))
    (should (equal (clutch-jdbc--normalize-props alist) alist))))

(ert-deftest clutch-db-test-jdbc-connect-normalizes-plist-props ()
  "JDBC connect should send props as an alist even when given a plist."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--setup-prerequisites) #'ignore)
              ((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout-seconds)
                 (setq captured-params params)
                 '(:conn-id 1))))
      (clutch-db-jdbc-connect
       'oracle
       '(:host "db" :port 1521 :database "svc"
         :user "scott" :password "tiger"
         :props (:role "reporting" :schema "HR"))))
    (should (equal (alist-get 'props captured-params)
                   '(("role" . "reporting") ("schema" . "HR"))))))

;;;; Unit tests — row normalization

(ert-deftest clutch-db-test-jdbc-normalize-row-clob ()
  "Clob plists should be replaced with their :preview string."
  (let ((row (list "text"
                   '(:__type "clob" :length 1000 :preview "hello clob")
                   42)))
    (should (equal (clutch-jdbc--normalize-row row)
                   '("text" "hello clob" 42)))))

(ert-deftest clutch-db-test-jdbc-normalize-row-clob-nil-preview ()
  "Clob plists with no preview should normalize to nil."
  (let ((row (list '(:__type "clob" :length 0))))
    (should (equal (clutch-jdbc--normalize-row row) '(nil)))))

(ert-deftest clutch-db-test-jdbc-normalize-row-blob-with-text ()
  "Blob plists with :text should still normalize to the text string."
  (let ((row (list '(:__type "blob" :length 5 :text "hello"))))
    (should (equal (clutch-jdbc--normalize-row row) '("hello")))))

(ert-deftest clutch-db-test-jdbc-normalize-row-plain-values ()
  "Plain values should pass through normalize-row unchanged."
  (let ((row '(1 "str" nil t)))
    (should (equal (clutch-jdbc--normalize-row row) row))))

;;;; Unit tests — Redshift driver support

(ert-deftest clutch-db-test-jdbc-build-url-redshift ()
  "Redshift URL builder should produce a jdbc:redshift URL with default port 5439."
  (should (equal (clutch-jdbc--build-url
                  'redshift
                  '(:host "cluster.us-east-1.redshift.amazonaws.com" :database "mydb"))
                 "jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/mydb"))
  (should (equal (clutch-jdbc--build-url
                  'redshift
                  '(:host "cluster.example.com" :port 5440 :database "analytics"))
                 "jdbc:redshift://cluster.example.com:5440/analytics")))

(ert-deftest clutch-db-test-jdbc-display-name-redshift ()
  "Redshift connections should display as \"Redshift\"."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver redshift))))
    (should (equal (clutch-db-display-name conn) "Redshift"))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-redshift ()
  "Installing Redshift JDBC should download the redshift-jdbc42 Maven artifact."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         requested-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (setq requested-coords coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'redshift)
          (should (string-prefix-p "com.amazon.redshift:redshift-jdbc42:" requested-coords))
          (should (file-exists-p (expand-file-name "drivers/redshift-jdbc42.jar" tmpdir))))
      (delete-directory tmpdir t))))

;;;; Unit tests — ClickHouse driver support

(ert-deftest clutch-db-test-jdbc-build-url-clickhouse ()
  "ClickHouse URL builder should produce a jdbc:clickhouse URL with default port 8123."
  (should (equal (clutch-jdbc--build-url
                  'clickhouse
                  '(:host "ch.corp.com" :database "default"))
                 "jdbc:clickhouse://ch.corp.com:8123/default"))
  (should (equal (clutch-jdbc--build-url
                  'clickhouse
                  '(:host "ch.corp.com" :port 8443 :database "analytics"))
                 "jdbc:clickhouse://ch.corp.com:8443/analytics")))

(ert-deftest clutch-db-test-jdbc-display-name-clickhouse ()
  "ClickHouse connections should display as \"ClickHouse\"."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (should (equal (clutch-db-display-name conn) "ClickHouse"))))

(ert-deftest clutch-db-test-jdbc-install-driver-installs-clickhouse ()
  "Installing ClickHouse JDBC should download the all-classifier artifact and companions."
  (let* ((tmpdir (make-temp-file "clutch-jdbc-driver-" t))
         (clutch-jdbc-agent-dir tmpdir)
         all-coords)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-jdbc--download-maven-driver)
                   (lambda (coords dest)
                     (push coords all-coords)
                     (with-temp-file dest (insert "jar")))))
          (clutch-jdbc-install-driver 'clickhouse)
          (should (cl-some (lambda (c) (string-match-p ":all\\'" c)) all-coords))
          (should (file-exists-p (expand-file-name "drivers/clickhouse-jdbc.jar" tmpdir)))
          (should (file-exists-p (expand-file-name "drivers/slf4j-api.jar" tmpdir)))
          (should (file-exists-p (expand-file-name "drivers/slf4j-nop.jar" tmpdir))))
      (delete-directory tmpdir t))))

(ert-deftest clutch-db-test-jdbc-download-maven-classifier ()
  "Maven downloader should handle 4-segment coords (with classifier)."
  (let (captured-url)
    (cl-letf (((symbol-function 'url-copy-file)
               (lambda (url _dest &rest _) (setq captured-url url))))
      ;; 4-segment: group:artifact:version:classifier
      (clutch-jdbc--download-maven-driver
       "com.clickhouse:clickhouse-jdbc:0.9.8:all" "/tmp/test.jar")
      (should (string-match-p "clickhouse-jdbc-0.9.8-all\\.jar" captured-url))
      ;; 3-segment: group:artifact:version (unchanged behavior)
      (clutch-jdbc--download-maven-driver
       "com.amazon.redshift:redshift-jdbc42:2.1.0.30" "/tmp/test2.jar")
      (should (string-match-p "redshift-jdbc42-2.1.0.30\\.jar" captured-url)))))

;;;; Unit tests — clutch-jdbc--conn-schema

(ert-deftest clutch-db-test-jdbc-conn-schema-oracle-defaults-to-user ()
  "Oracle with no explicit :schema returns the uppercased :user as the schema."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver oracle :user "zjsy"))))
    (should (equal (clutch-jdbc--conn-schema conn) "ZJSY"))))

(ert-deftest clutch-db-test-jdbc-conn-schema-explicit-overrides-default ()
  "An explicit :schema is returned as-is, even for Oracle."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver oracle :user "zjsy" :schema "REPORTING"))))
    (should (equal (clutch-jdbc--conn-schema conn) "REPORTING"))))

(ert-deftest clutch-db-test-jdbc-conn-schema-non-oracle-returns-nil ()
  "Non-Oracle drivers with no :schema return nil."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver sqlserver :user "sa"))))
    (should (null (clutch-jdbc--conn-schema conn)))))

(ert-deftest clutch-db-test-jdbc-list-schemas-filters-oracle-system-schemas ()
  "Oracle JDBC schema listing should filter common system schemas."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver oracle :user "zjsy" :rpc-timeout 9))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op _params &optional _timeout-seconds)
                 '(:schemas ("SYS" "SYSTEM" "ZJSY" "CJH_TEST" "ZJ_TEST")))))
      (should (equal (clutch-db-list-schemas conn)
                     '("ZJSY" "CJH_TEST" "ZJ_TEST"))))))

(ert-deftest clutch-db-test-jdbc-set-current-schema-updates-params ()
  "Oracle JDBC schema switching should update both JDBC sessions and persist :schema."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver oracle :user "zjsy" :rpc-timeout 9)))
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 '(:conn-id 7 :schema "CJH_TEST"))))
      (should (equal (clutch-db-set-current-schema conn "cjh_test") "CJH_TEST"))
      (should (equal captured-op "set-current-schema"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should (equal (alist-get 'schema captured-params) "CJH_TEST"))
      (should (= captured-timeout 9))
      (should (equal (plist-get (clutch-jdbc-conn-params conn) :schema)
                     "CJH_TEST")))))

(ert-deftest clutch-db-test-jdbc-set-current-schema-rejects-generic-driver ()
  "Generic JDBC connections should keep schema switching unsupported."
  (let ((conn (make-clutch-jdbc-conn
               :conn-id 7
               :params '(:driver jdbc :display-name "KingbaseES" :rpc-timeout 9))))
    (should-error
     (clutch-db-set-current-schema conn "public")
     :type 'user-error)))

(ert-deftest clutch-db-test-mysql-set-current-schema-updates-connection-database ()
  "MySQL schema switching should execute USE and update the connection database."
  (let ((conn (make-mysql-conn :database "zj_test"))
        executed-sql)
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-clutch-db-result :connection conn :affected-rows 0))))
      (should (equal (clutch-db-set-current-schema conn "cjh_test") "cjh_test"))
      (should (equal executed-sql "USE `cjh_test`"))
      (should (equal (mysql-conn-database conn) "cjh_test")))))

;;;; Unit tests — clutch-jdbc--apply-timeout-defaults

(ert-deftest clutch-db-test-jdbc-apply-timeout-defaults-fills-missing ()
  "Empty params get all four timeouts filled from the global defcustoms."
  (let* ((clutch-connect-timeout-seconds 10)
         (clutch-read-idle-timeout-seconds 20)
         (clutch-query-timeout-seconds 30)
         (clutch-jdbc-rpc-timeout-seconds 40)
         (result (clutch-jdbc--apply-timeout-defaults nil)))
    (should (= (plist-get result :connect-timeout) 10))
    (should (= (plist-get result :read-idle-timeout) 20))
    (should (= (plist-get result :query-timeout) 30))
    (should (= (plist-get result :rpc-timeout) 40))))

(ert-deftest clutch-db-test-jdbc-apply-timeout-defaults-preserves-existing ()
  "Timeouts already present in params are not overwritten by global defaults."
  (let* ((clutch-connect-timeout-seconds 10)
         (clutch-read-idle-timeout-seconds 20)
         (clutch-query-timeout-seconds 30)
         (clutch-jdbc-rpc-timeout-seconds 40)
         (params '(:connect-timeout 99 :query-timeout 88))
         (result (clutch-jdbc--apply-timeout-defaults params)))
    (should (= (plist-get result :connect-timeout) 99))
    (should (= (plist-get result :read-idle-timeout) 20))
    (should (= (plist-get result :query-timeout) 88))
    (should (= (plist-get result :rpc-timeout) 40))))

;;;; Unit tests — backend registry

(ert-deftest clutch-db-test-backend-features ()
  "Test that backend features are correctly registered."
  (let ((mysql-features (alist-get 'mysql clutch-db--backend-features))
        (pg-features (alist-get 'pg clutch-db--backend-features))
        (jdbc-features (alist-get 'jdbc clutch-db--backend-features)))
    ;; MySQL backend
    (should mysql-features)
    (should (eq (plist-get mysql-features :require) 'clutch-db-mysql))
    (should (eq (plist-get mysql-features :connect-fn) 'clutch-db-mysql-connect))
    ;; PostgreSQL backend
    (should pg-features)
    (should (eq (plist-get pg-features :require) 'clutch-db-pg))
    (should (eq (plist-get pg-features :connect-fn) 'clutch-db-pg-connect))
    ;; Generic JDBC backend
    (should jdbc-features)
    (should (eq (plist-get jdbc-features :require) 'clutch-db-jdbc))
    (should (functionp (plist-get jdbc-features :connect-fn)))))

(ert-deftest clutch-db-test-connect-requires-selected-backend-only ()
  "`clutch-db-connect' should require only the selected adapter."
  (let ((clutch-db--backend-features
         '((mysql . (:require clutch-db-mysql :connect-fn clutch-db-mysql-connect))
           (pg . (:require clutch-db-pg :connect-fn clutch-db-pg-connect))))
        required)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (push feature required)
                 t))
              ((symbol-function 'clutch-db-mysql-connect)
               (lambda (params)
                 (cons 'mysql-conn params)))
              ((symbol-function 'clutch-db-pg-connect)
               (lambda (params)
                 (cons 'pg-conn params)))
              ((symbol-function 'clutch-db-init-connection)
               #'ignore))
      (should (equal (clutch-db-connect 'mysql '(:database "app"))
                     '(mysql-conn :database "app")))
      (should (equal (nreverse required) '(clutch-db-mysql))))))

(ert-deftest clutch-db-test-generic-jdbc-display-name-prefers-custom-label ()
  "Generic JDBC connections should honor a user-facing display label."
  (require 'clutch)
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc :display-name "KingbaseES"))))
    (should (clutch--jdbc-backend-p 'jdbc))
    (should (equal (clutch--backend-display-name-from-params
                    '(:backend jdbc :display-name "KingbaseES"))
                   "KingbaseES"))
    (should (equal (clutch-db-display-name conn) "KingbaseES"))))

(ert-deftest clutch-db-test-build-conn-routes-generic-jdbc-through-jdbc-backend ()
  "The generic JDBC backend should pass :url through to `clutch-db-connect'."
  (require 'clutch)
  (let (captured-backend captured-params)
    (cl-letf (((symbol-function 'clutch--normalize-timeout-params)
               (lambda (_backend params) params))
              ((symbol-function 'clutch--resolve-password)
               (lambda (_params) "secret"))
              ((symbol-function 'clutch-db-connect)
               (lambda (backend params)
                 (setq captured-backend backend
                       captured-params params)
                 'fake-conn)))
      (should (eq (clutch--build-conn
                   '(:backend jdbc
                     :url "jdbc:kingbase8://127.0.0.1:54321/test"
                     :display-name "KingbaseES"
                     :user "system"))
                  'fake-conn))
      (should (eq captured-backend 'jdbc))
      (should (equal (plist-get captured-params :url)
                     "jdbc:kingbase8://127.0.0.1:54321/test"))
      (should (equal (plist-get captured-params :display-name) "KingbaseES"))
      (should (equal (plist-get captured-params :password) "secret"))
      (should-not (plist-member captured-params :backend)))))

(ert-deftest clutch-db-test-unknown-backend ()
  "Test that connecting with unknown backend signals error."
  (should-error
   (clutch-db-connect 'unknown '(:host "localhost"))
   :type 'user-error))

(ert-deftest clutch-db-test-native-backend-missing-package-errors-clearly ()
  "Missing native backend packages should raise a direct user error."
  (dolist (case '((mysql clutch-db-mysql "mysql package")
                  (pg clutch-db-pg "pg package")))
    (pcase-let ((`(,backend ,feature ,expected) case))
      (ert-info ((symbol-name backend))
        (let ((orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'require)
                     (lambda (requested &optional filename noerror)
                       (if (eq requested feature)
                           (signal 'file-missing
                                   (list "Cannot open load file"
                                         "No such file or directory"
                                         (symbol-name feature)))
                         (funcall orig-require requested filename noerror)))))
            (condition-case err
                (progn
                  (clutch-db-connect backend '(:host "localhost"))
                  (should nil))
              (user-error
               (should (string-match-p expected (cadr err)))))))))))

(ert-deftest clutch-db-test-jdbc-driver-backend-loads-jdbc-on-demand ()
  "Driver-style JDBC backends should load `clutch-db-jdbc' on demand."
  (let ((clutch-db--backend-features
         '((mysql . (:require clutch-db-mysql :connect-fn clutch-db-mysql-connect))
           (pg . (:require clutch-db-pg :connect-fn clutch-db-pg-connect))
           (sqlite . (:require clutch-db-sqlite :connect-fn clutch-db-sqlite-connect))))
        required
        captured-params)
    (cl-letf (((symbol-function 'require)
               (lambda (feature &optional _filename _noerror)
                 (pcase feature
                   ('clutch-db-jdbc
                    (setq required t)
                    (push (cons 'oracle
                                (list :require 'clutch-db-jdbc
                                      :connect-fn (lambda (params)
                                                    (setq captured-params params)
                                                    'fake-conn)))
                          clutch-db--backend-features)
                    t)
                   (_ t))))
              ((symbol-function 'clutch-db-init-connection)
               (lambda (_conn) nil)))
      (should (eq (clutch-db-connect 'oracle '(:user "scott" :database "orcl"))
                  'fake-conn))
      (should required)
      (should (equal captured-params '(:user "scott" :database "orcl"))))))

;;;; Unit tests — MySQL type category mapping

(ert-deftest clutch-db-test-mysql-type-categories ()
  "Test MySQL type to category mapping."
  (require 'clutch-db-mysql)
  (require 'mysql)
  ;; Numeric types
  (should (eq (clutch-db-mysql--type-category mysql-type-long 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-float 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-double 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-decimal 33) 'numeric))
  (should (eq (clutch-db-mysql--type-category mysql-type-longlong 33) 'numeric))
  ;; Date/time types
  (should (eq (clutch-db-mysql--type-category mysql-type-date 33) 'date))
  (should (eq (clutch-db-mysql--type-category mysql-type-time 33) 'time))
  (should (eq (clutch-db-mysql--type-category mysql-type-datetime 33) 'datetime))
  (should (eq (clutch-db-mysql--type-category mysql-type-timestamp 33) 'datetime))
  ;; BLOB/TEXT split by charset
  (should (eq (clutch-db-mysql--type-category mysql-type-blob 63) 'blob))
  (should (eq (clutch-db-mysql--type-category mysql-type-blob 33) 'text))
  ;; JSON
  (should (eq (clutch-db-mysql--type-category mysql-type-json 63) 'json))
  ;; Unknown type defaults to text
  (should (eq (clutch-db-mysql--type-category 9999 0) 'text)))

(ert-deftest clutch-db-test-mysql-convert-columns ()
  "Test MySQL column conversion."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let* ((mysql-cols (list (list :name "id" :type mysql-type-long :character-set 33)
                           (list :name "data" :type mysql-type-json :character-set 63)
                           (list :name "blob_bin" :type mysql-type-blob :character-set 63)
                           (list :name "blob_txt" :type mysql-type-blob :character-set 33)
                           (list :name "created" :type mysql-type-datetime :character-set 33)))
         (converted (clutch-db-mysql--convert-columns mysql-cols)))
    (should (= (length converted) 5))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "blob_bin"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'blob))
    (should (equal (plist-get (nth 3 converted) :name) "blob_txt"))
    (should (eq (plist-get (nth 3 converted) :type-category) 'text))
    (should (equal (plist-get (nth 4 converted) :name) "created"))
    (should (eq (plist-get (nth 4 converted) :type-category) 'datetime))))

;;;; Unit tests — PostgreSQL type category mapping

(ert-deftest clutch-db-test-pg-type-categories ()
  "Test PostgreSQL OID to category mapping."
  (require 'clutch-db-pg)
  ;; Numeric types
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-int4) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-int8) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-float8) 'numeric))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-numeric) 'numeric))
  ;; Date/time types
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-date) 'date))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-time) 'time))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-timestamp) 'datetime))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-timestamptz) 'datetime))
  ;; BLOB/JSON
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-bytea) 'blob))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-json) 'json))
  (should (eq (clutch-db-pg--type-category clutch-db-test--pg-oid-jsonb) 'json))
  ;; Unknown OID defaults to text
  (should (eq (clutch-db-pg--type-category 999999) 'text)))

(ert-deftest clutch-db-test-pg-convert-columns ()
  "Test PostgreSQL column conversion."
  (require 'clutch-db-pg)
  (let* ((pg-cols '(("id" 23 4)
                    ("data" 3802 -1)
                    ("created" 1114 8)))
         (converted (clutch-db-pg--convert-columns pg-cols)))
    (should (= (length converted) 3))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "created"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'datetime))))

(ert-deftest clutch-db-test-pg-wrap-result-normalizes-temporal-values ()
  "PostgreSQL results should normalize upstream pg-el temporal values."
  (require 'clutch-db-pg)
  (let* ((conn (clutch-db-test--make-pgcon :database "test"))
         (date (encode-time 0 0 0 1 6 2024))
         (timestamp (encode-time 30 45 13 15 1 2024))
         (timestamptz (encode-time 45 30 8 7 4 2026))
         (pg-result (make-pgresult
                     :connection conn
                     :attributes `(("due_on" ,clutch-db-test--pg-oid-date 4)
                                   ("starts_at" ,clutch-db-test--pg-oid-time 8)
                                   ("opened_at" ,clutch-db-test--pg-oid-timestamp 8)
                                   ("shipped_at" ,clutch-db-test--pg-oid-timestamptz 8))
                     :tuples `((,date "09:10:11.250" ,timestamp ,timestamptz))))
         (result (clutch-db-pg--wrap-result pg-result))
         (row (car (clutch-db-result-rows result))))
    (should (equal (nth 0 row)
                   '(:year 2024 :month 6 :day 1)))
    (should (equal (nth 1 row)
                   '(:hours 9 :minutes 10 :seconds 11 :negative nil)))
    (should (equal (nth 2 row)
                   '(:year 2024 :month 1 :day 15
                     :hours 13 :minutes 45 :seconds 30)))
    (should (equal (nth 3 row)
                   '(:year 2026 :month 4 :day 7
                     :hours 8 :minutes 30 :seconds 45)))))

;;;; Unit tests — SQL building (paged queries)

(ert-deftest clutch-db-test-mysql-build-paged-sql ()
  "Test MySQL paged SQL generation."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Basic pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 2
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 1 10)))
      (should (string-match-p "OFFSET 10" sql)))
    ;; With order
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("name" . "ASC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "ASC" sql)))
    ;; Replacing existing ORDER BY for result-driven sort
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM t ORDER BY created_at DESC"
                0 10 '("name" . "ASC"))))
      (should (string-match-p "ORDER BY `name` ASC" sql))
      (should-not (string-match-p "ORDER BY created_at DESC.*ORDER BY" sql)))
    ;; Already has LIMIT — no modification
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t LIMIT 5" 0 10)))
      (should (equal sql "SELECT * FROM t LIMIT 5")))
    ;; Nested LIMIT should not disable outer pagination
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM (SELECT * FROM t LIMIT 1) AS sub"
                0 10)))
      (should (string-match-p "FROM (SELECT \\* FROM t LIMIT 1) AS sub" sql))
      (should (string-match-p "LIMIT 10 OFFSET 0\\'" sql)))))

(ert-deftest clutch-db-test-pg-build-paged-sql ()
  "Test PostgreSQL paged SQL generation."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "localhost")))
    ;; Basic pagination
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 3, page-size 25
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 2 25)))
      (should (string-match-p "LIMIT 25" sql))
      (should (string-match-p "OFFSET 50" sql)))
    ;; With descending order
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("id" . "DESC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "DESC" sql)))
    ;; Replacing existing ORDER BY for result-driven sort
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM t ORDER BY created_at DESC"
                0 10 '("id" . "ASC"))))
      (should (string-match-p "ORDER BY \"id\" ASC" sql))
      (should-not (string-match-p "ORDER BY created_at DESC.*ORDER BY" sql)))
    ;; Query with trailing semicolon
    (let ((sql (clutch-db-build-paged-sql conn "SELECT * FROM t;" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should-not (string-match-p ";\\s*LIMIT" sql)))
    ;; Nested LIMIT should not disable outer pagination
    (let ((sql (clutch-db-build-paged-sql
                conn
                "SELECT * FROM (SELECT * FROM t LIMIT 1) AS sub"
                0 10)))
      (should (string-match-p "FROM (SELECT \\* FROM t LIMIT 1) AS sub" sql))
      (should (string-match-p "LIMIT 10 OFFSET 0\\'" sql)))))

;;;; Unit tests — SQL escaping

(ert-deftest clutch-db-test-mysql-escape ()
  "Test MySQL identifier and literal escaping via generic interface."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Identifier escaping
    (should (equal (clutch-db-escape-identifier conn "table")
                   "`table`"))
    (should (equal (clutch-db-escape-identifier conn "my`table")
                   "`my``table`"))
    ;; Literal escaping
    (should (equal (clutch-db-escape-literal conn "hello")
                   "'hello'"))
    (should (equal (clutch-db-escape-literal conn "it's")
                   "'it\\'s'"))))

(ert-deftest clutch-db-test-pg-escape ()
  "Test PostgreSQL identifier and literal escaping via generic interface."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "localhost")))
    ;; Identifier escaping
    (should (equal (clutch-db-escape-identifier conn "table")
                   "\"table\""))
    (should (equal (clutch-db-escape-identifier conn "my\"table")
                   "\"my\"\"table\""))
    ;; Literal escaping
    (should (equal (clutch-db-escape-literal conn "hello")
                   "E'hello'"))
    (should (equal (clutch-db-escape-literal conn "it's")
                   "E'it''s'"))))

(ert-deftest clutch-db-test-jdbc-clickhouse-escape ()
  "ClickHouse JDBC should leave simple identifiers bare and backtick-escape others."
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (should (equal (clutch-db-escape-identifier conn "background_schedule_pool_log")
                   "background_schedule_pool_log"))
    (should (equal (clutch-db-escape-identifier conn "order-items")
                   "`order-items`"))
    (should (equal (clutch-db-escape-identifier conn "my`table")
                   "`my``table`"))))

;;;; Unit tests — metadata accessors

(ert-deftest clutch-db-test-mysql-metadata ()
  "Test MySQL metadata accessors."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "example.com" :port 3307
                                :user "testuser" :database "testdb")))
    (should (equal (clutch-db-host conn) "example.com"))
    (should (= (clutch-db-port conn) 3307))
    (should (equal (clutch-db-user conn) "testuser"))
    (should (equal (clutch-db-database conn) "testdb"))
    (should (equal (clutch-db-display-name conn) "MySQL"))))

(ert-deftest clutch-db-test-pg-metadata ()
  "Test PostgreSQL metadata accessors."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "example.com" :port 5433
                                          :user "pguser" :database "pgdb")))
    (should (equal (clutch-db-host conn) "example.com"))
    (should (= (clutch-db-port conn) 5433))
    (should (equal (clutch-db-user conn) "pguser"))
    (should (equal (clutch-db-database conn) "pgdb"))
    (should (equal (clutch-db-display-name conn) "PostgreSQL"))))

(ert-deftest clutch-db-test-pg-list-schemas-filters-system-schemas ()
  "PostgreSQL schema listing should omit built-in system schemas."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        captured-sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 (make-pgresult :tuples '(("app") ("public"))))))
      (should (equal (clutch-db-list-schemas conn) '("app" "public")))
      (should (string-match-p "information_schema" captured-sql))
      (should (string-match-p "NOT LIKE 'pg" captured-sql)))))

(ert-deftest clutch-db-test-pg-current-schema-caches-result ()
  "PostgreSQL current schema lookup should cache the result on the connection."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        (calls 0))
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn _sql)
                 (setq calls (1+ calls))
                 (make-pgresult :tuples '(("public"))))))
      (should (equal (clutch-db-current-schema conn) "public"))
      (should (equal (clutch-db-current-schema conn) "public"))
      (should (= calls 1)))))

(ert-deftest clutch-db-test-pg-set-current-schema-updates-search-path-cache ()
  "PostgreSQL schema switching should issue SET search_path and update cache."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :database "test"))
        executed-sql)
    (cl-letf (((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-pgresult :tuples nil))))
      (should (equal (clutch-db-set-current-schema conn "app") "app"))
      (should (equal executed-sql "SET search_path TO \"app\""))
      (should (equal (clutch-db-current-schema conn) "app")))))

(ert-deftest clutch-db-test-pg-connect-applies-schema-via-search-path ()
  "PostgreSQL connect should restore a requested schema via search_path."
  (require 'clutch-db-pg)
  (let (captured-args executed-sql)
    (cl-letf (((symbol-function 'pg-connect-plist)
               (lambda (&rest args)
                 (setq captured-args args)
                 (clutch-db-test--make-pgcon :host "127.0.0.1" :port 54321
                                             :user "system" :database "test")))
              ((symbol-function 'pg-exec)
               (lambda (_conn sql)
                 (setq executed-sql sql)
                 (make-pgresult :tuples nil))))
      (let ((conn (clutch-db-pg-connect
                   '(:host "127.0.0.1"
                     :port 54321
                     :database "test"
                     :user "system"
                     :password "123456"
                     :schema "app"))))
        (should (equal (nth 0 captured-args) "test"))
        (should (equal (nth 1 captured-args) "system"))
        (should-not (plist-member (nthcdr 2 captured-args) :schema))
        (should (equal executed-sql "SET search_path TO \"app\""))
        (should (equal (clutch-db-current-schema conn) "app"))))))

(ert-deftest clutch-db-test-pg-connect-normalizes-tls-to-sslmode ()
  "PostgreSQL backend connect should map canonical SSLMODE to pg-el TLS args."
  (require 'clutch-db-pg)
  (let (captured-args)
    (cl-letf (((symbol-function 'pg-connect-plist)
               (lambda (&rest args)
                 (setq captured-args args)
                 (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432
                                             :user "postgres" :database "test"))))
      (clutch-db-pg-connect
       '(:host "127.0.0.1"
         :port 5432
         :database "test"
         :user "postgres"
         :password "secret"
         :tls t))
      (should (eq (plist-get (nthcdr 2 captured-args) :tls-options) t))
      (should-not (plist-member (nthcdr 2 captured-args) :tls)))))

(ert-deftest clutch-db-test-mysql-connect-normalizes-tls-nil-to-ssl-mode ()
  "MySQL backend connect should pass canonical `:ssl-mode' to `mysql-connect'."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let (captured-args)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-args args)
                 (make-mysql-conn :host "127.0.0.1" :port 3306
                                  :user "root" :database "mysql-wire"))))
      (clutch-db-mysql-connect
       '(:host "127.0.0.1"
         :port 3306
         :database "mysql"
         :user "root"
         :password "secret"
         :tls nil))
      (should-not (plist-member captured-args :clutch-tls-mode))
      (should (eq (plist-get captured-args :ssl-mode) 'disabled))
      (should-not (plist-member captured-args :tls)))))

(ert-deftest clutch-db-test-mysql-connect-strips-pass-entry-before-wire-connect ()
  "MySQL backend connect should not pass `:pass-entry' to `mysql-connect'."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let (captured-args)
    (cl-letf (((symbol-function 'mysql-connect)
               (lambda (&rest args)
                 (setq captured-args args)
                 (make-mysql-conn :host "127.0.0.1" :port 3306
                                       :user "root" :database "mysql-wire"))))
      (clutch-db-mysql-connect
       '(:host "127.0.0.1"
         :port 3306
         :database "mysql"
         :user "root"
         :password "secret"
         :pass-entry "prod-db"))
      (should-not (plist-member captured-args :pass-entry))
      (should (equal (plist-get captured-args :password) "secret")))))

(ert-deftest clutch-db-test-pg-interrupt-query-returns-t-after-cancel ()
  "PostgreSQL interrupt should report success when cancel completes."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432))
        called)
    (cl-letf (((symbol-function 'pg-cancel)
               (lambda (pg-conn)
                 (setq called pg-conn)
                 t)))
      (should (clutch-db-interrupt-query conn))
      (should (eq called conn)))))

(ert-deftest clutch-db-test-pg-interrupt-query-returns-nil-on-pg-error ()
  "PostgreSQL interrupt should degrade to nil when cancel errors."
  (require 'clutch-db-pg)
  (let ((conn (clutch-db-test--make-pgcon :host "127.0.0.1" :port 5432)))
    (cl-letf (((symbol-function 'pg-cancel)
               (lambda (_pg-conn)
                 (signal 'pg-connection-error '("cancel failed")))))
      (should-not (clutch-db-interrupt-query conn)))))

(ert-deftest clutch-db-test-jdbc-metadata-derived-from-url ()
  "Generic JDBC metadata accessors should derive host/port/database from :url."
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc
                         :url "jdbc:kingbase8://127.0.0.1:54321/test"
                         :display-name "KingbaseES"
                         :user "system"))))
    (should (equal (clutch-db-host conn) "127.0.0.1"))
    (should (= (clutch-db-port conn) 54321))
    (should (equal (clutch-db-user conn) "system"))
    (should (equal (clutch-db-database conn) "test"))
    (should (equal (clutch-db-display-name conn) "KingbaseES"))))

;;;; Live integration tests — MySQL

(defmacro clutch-db-test--with-mysql (var &rest body)
  "Execute BODY with VAR bound to a MySQL connection.
Skips if `clutch-db-test-mysql-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-mysql-password)
       (ert-skip "Set clutch-db-test-mysql-password to enable MySQL live tests")
     ;; Local MySQL 8 containers usually present self-signed certs.  The native
     ;; client auto-upgrades to TLS for `caching_sha2_password', so disable
     ;; certificate verification here unless the caller has installed a trust
     ;; chain explicitly.
     (clutch-db-test--with-local-mysql-tls
       (let ((,var (clutch-db-connect
                    'mysql
                    (list :host clutch-db-test-mysql-host
                          :port clutch-db-test-mysql-port
                          :user clutch-db-test-mysql-user
                          :password clutch-db-test-mysql-password
                          :database clutch-db-test-mysql-database))))
         (unwind-protect
             (progn ,@body)
           (clutch-db-disconnect ,var))))))

(defun clutch-db-test--assert-live-basic-query (conn)
  "Assert the standard live smoke query against CONN."
  (let ((result (clutch-db-query conn "SELECT 1 AS n, 'hello' AS s")))
    (should (clutch-db-result-p result))
    (should (= (length (clutch-db-result-columns result)) 2))
    (should (= (length (clutch-db-result-rows result)) 1))
    (let ((row (car (clutch-db-result-rows result))))
      (should (= (car row) 1))
      (should (equal (cadr row) "hello")))))

(defun clutch-db-test--assert-live-basic-dml (conn)
  "Assert the standard live DML round-trip against CONN."
  (clutch-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
  (let ((result (clutch-db-query conn
                 "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
    (should (= (clutch-db-result-affected-rows result) 2)))
  (let ((result (clutch-db-query conn "SELECT * FROM _db_test")))
    (should (= (length (clutch-db-result-rows result)) 2))))

(defmacro clutch-db-test--define-live-basic-tests (prefix with-macro tags display-name)
  "Define shared live tests for PREFIX using WITH-MACRO, TAGS, and DISPLAY-NAME."
  `(progn
     (ert-deftest ,(intern (format "%s-live-connect" prefix)) ()
       :tags ',tags
       ,(format "Test %s connection via clutch-db-connect." display-name)
       (,with-macro conn
         (should (clutch-db-live-p conn))
         (should (equal (clutch-db-display-name conn) ,display-name))))
     (ert-deftest ,(intern (format "%s-live-query" prefix)) ()
       :tags ',tags
       ,(format "Test %s query via clutch-db-query." display-name)
       (,with-macro conn
         (clutch-db-test--assert-live-basic-query conn)))
     (ert-deftest ,(intern (format "%s-live-dml" prefix)) ()
       :tags ',tags
       ,(format "Test %s DML operations." display-name)
       (,with-macro conn
         (clutch-db-test--assert-live-basic-dml conn)))
     (ert-deftest ,(intern (format "%s-live-error" prefix)) ()
       :tags ',tags
       ,(format "Test %s error handling." display-name)
       (,with-macro conn
         (should-error (clutch-db-query conn "SELEC BAD")
                       :type 'clutch-db-error)))))

(clutch-db-test--define-live-basic-tests
 clutch-db-test-mysql
 clutch-db-test--with-mysql
 (:db-live :mysql-live)
 "MySQL")

(ert-deftest clutch-db-test-mysql-live-schema ()
  :tags '(:db-live :mysql-live)
  "Test MySQL schema introspection."
  (clutch-db-test--with-mysql conn
    ;; list-tables
    (let ((tables (clutch-db-list-tables conn)))
      (should (listp tables))
      (should (> (length tables) 0)))
    ;; list-columns
    (let ((columns (clutch-db-list-columns conn "user")))
      (should (listp columns))
      (should (member "Host" columns)))
    ;; show-create-table
    (let ((ddl (clutch-db-show-create-table conn "user")))
      (should (stringp ddl))
      (should (string-match-p "CREATE\\( TABLE\\| .* VIEW\\)" ddl)))))

(ert-deftest clutch-db-test-mysql-live-row-identity-uses-unique-not-null ()
  :tags '(:db-live :mysql-live)
  "MySQL should use a non-null unique key when no primary key exists."
  (clutch-db-test--with-mysql conn
    (let* ((table (format "clutch_rowid_unique_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (format "CREATE TABLE %s (code VARCHAR(32) NOT NULL, name VARCHAR(64), UNIQUE KEY uq_code (code))"
                    table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'unique-key))
              (should (equal (plist-get candidate :name) "uq_code"))
              (should (equal (plist-get candidate :columns) '("code")))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-db-test-mysql-live-row-identity-sees-generated-invisible-pk ()
  :tags '(:db-live :mysql-live)
  "MySQL generated invisible primary keys should behave as primary keys."
  (clutch-db-test--with-mysql conn
    (let* ((table (format "clutch_rowid_gipk_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (condition-case nil
          (progn
            (ignore-errors
              (clutch-db-query
               conn
               "SET SESSION show_gipk_in_create_table_and_information_schema=ON"))
            (clutch-db-query
             conn "SET SESSION sql_generate_invisible_primary_key=ON"))
        (clutch-db-error
         (ert-skip "MySQL generated invisible primary keys are unavailable")))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (name VARCHAR(64))" table))
            (let ((show-keys
                   (clutch-db-query
                    conn
                    (format "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'"
                            table))))
              (unless (clutch-db-result-rows show-keys)
                (ert-skip "MySQL generated invisible primary key metadata is hidden")))
            (should (equal (clutch-db-primary-key-columns conn table)
                           '("my_row_id")))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'primary-key))
              (should (equal (plist-get candidate :columns) '("my_row_id")))))
        (ignore-errors (clutch-db-query conn drop-sql))
        (ignore-errors
          (clutch-db-query
           conn "SET SESSION sql_generate_invisible_primary_key=OFF"))))))

(ert-deftest clutch-db-test-mysql-show-create-table-empty-rows-errors-cleanly ()
  "MySQL show-create-table should signal `clutch-db-error' on empty row sets."
  (let ((conn (make-mysql-conn :host "localhost")))
    (cl-letf (((symbol-function 'mysql-query)
               (lambda (_conn _sql)
                 (make-mysql-result :rows nil))))
      (should-error (clutch-db-show-create-table conn "missing_table")
                    :type 'clutch-db-error))))

;;;; Live integration tests — PostgreSQL

(defmacro clutch-db-test--with-pg (var &rest body)
  "Execute BODY with VAR bound to a PostgreSQL connection.
Skips if `clutch-db-test-pg-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-pg-password)
       (ert-skip "Set clutch-db-test-pg-password to enable PostgreSQL live tests")
     (let ((,var (clutch-db-connect
                  'pg
                  (list :host clutch-db-test-pg-host
                        :port clutch-db-test-pg-port
                        :user clutch-db-test-pg-user
                        :password clutch-db-test-pg-password
                        :database clutch-db-test-pg-database))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(clutch-db-test--define-live-basic-tests
 clutch-db-test-pg
 clutch-db-test--with-pg
 (:db-live :pg-live)
 "PostgreSQL")

(ert-deftest clutch-db-test-pg-live-schema ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL schema introspection."
  (clutch-db-test--with-pg conn
    ;; Create a test table for schema tests
    (clutch-db-query conn
     "CREATE TEMPORARY TABLE _schema_test (id SERIAL PRIMARY KEY, name TEXT)")
    ;; list-tables (temporary tables not in pg_tables, so just check it runs)
    (let ((tables (clutch-db-list-tables conn)))
      (should (listp tables)))
    ;; Create a real table for column/DDL tests
    (clutch-db-query conn
     "CREATE TABLE IF NOT EXISTS _schema_real (id SERIAL PRIMARY KEY, name TEXT)")
    ;; list-columns
    (let ((columns (clutch-db-list-columns conn "_schema_real")))
      (should (listp columns))
      (should (member "id" columns))
      (should (member "name" columns)))
    ;; show-create-table (synthesized DDL)
    (let ((ddl (clutch-db-show-create-table conn "_schema_real")))
      (should (stringp ddl))
      (should (string-match-p "CREATE TABLE" ddl)))
    ;; Cleanup
    (clutch-db-query conn "DROP TABLE IF EXISTS _schema_real")))

(ert-deftest clutch-db-test-pg-live-row-identity-uses-ctid ()
  :tags '(:db-live :pg-live)
  "PostgreSQL should use CTID when no logical key exists."
  (clutch-db-test--with-pg conn
    (let* ((table (format "clutch_rowid_ctid_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn
                             (format "CREATE TABLE %s (name TEXT)" table))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn table))))
              (should (equal (plist-get candidate :kind) 'row-locator))
              (should (equal (plist-get candidate :name) "ctid"))
              (should (equal (plist-get candidate :select-expressions)
                             '("ctid::text")))
              (should (equal (plist-get candidate :where-sql)
                             "ctid = ?::tid"))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

;;;; Live integration tests — JDBC / Oracle
;;
;; Oracle is the primary JDBC target for clutch.  These tests verify:
;;   • agent start-up and basic query round-trip
;;   • manual-commit mode (Oracle default, matches DataGrip behaviour)
;;   • explicit COMMIT and ROLLBACK RPCs
;;   • schema introspection via DatabaseMetaData
;;
;; To enable: set `clutch-db-test-jdbc-oracle-password' and point
;; `clutch-jdbc-agent-dir' at a directory that contains the jar and a
;; drivers/ subdirectory with ojdbc11.jar (or equivalent).
;;
;; Quick local setup (OrbStack):
;;   docker run -d --name clutch-oracle -e ORACLE_PASSWORD=test \
;;     -p 1521:1521 gvenzl/oracle-free:slim-faststart

(defvar clutch-db-test-jdbc-oracle-host "127.0.0.1"
  "Host for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-port 1521
  "Port for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-user "system"
  "User for Oracle JDBC live tests.")
(defvar clutch-db-test-jdbc-oracle-password nil
  "Password for Oracle JDBC live tests.  Non-nil enables the :jdbc-live suite.")
(defvar clutch-db-test-jdbc-oracle-service "freepdb1"
  "Service name for Oracle JDBC live tests (gvenzl/oracle-free default).")

(defvar clutch-db-test-jdbc-mssql-host "127.0.0.1"
  "Host for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-port 1433
  "Port for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-user "sa"
  "User for SQL Server JDBC live tests.")
(defvar clutch-db-test-jdbc-mssql-password nil
  "Password for SQL Server JDBC live tests.  Non-nil enables the :mssql-live suite.")
(defvar clutch-db-test-jdbc-mssql-database "master"
  "Database name for SQL Server JDBC live tests.")

(defvar clutch-db-test-jdbc-clickhouse-host "127.0.0.1"
  "Host for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-port 8123
  "Port for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-user "default"
  "User for ClickHouse JDBC live tests.")
(defvar clutch-db-test-jdbc-clickhouse-password nil
  "Password for ClickHouse JDBC live tests.
Non-nil enables the :clickhouse-live suite.")
(defvar clutch-db-test-jdbc-clickhouse-database "default"
  "Database name for ClickHouse JDBC live tests.")

(defmacro clutch-db-test--with-oracle (var &rest body)
  "Execute BODY with VAR bound to a live Oracle JDBC connection.
Skips if `clutch-db-test-jdbc-oracle-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-oracle-password)
       (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'oracle
                  (list :host clutch-db-test-jdbc-oracle-host
                        :port clutch-db-test-jdbc-oracle-port
                        :user clutch-db-test-jdbc-oracle-user
                        :password clutch-db-test-jdbc-oracle-password
                        :database clutch-db-test-jdbc-oracle-service))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defmacro clutch-db-test--with-mssql (var &rest body)
  "Execute BODY with VAR bound to a live SQL Server JDBC connection.
Skips if `clutch-db-test-jdbc-mssql-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-mssql-password)
       (ert-skip "Set clutch-db-test-jdbc-mssql-password to enable SQL Server live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'sqlserver
                  (list :host clutch-db-test-jdbc-mssql-host
                        :port clutch-db-test-jdbc-mssql-port
                        :user clutch-db-test-jdbc-mssql-user
                        :password clutch-db-test-jdbc-mssql-password
                        :database clutch-db-test-jdbc-mssql-database
                        :props '(("trustServerCertificate" . "true"))))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(defmacro clutch-db-test--with-clickhouse (var &rest body)
  "Execute BODY with VAR bound to a live ClickHouse JDBC connection.
Skips if `clutch-db-test-jdbc-clickhouse-password' is nil."
  (declare (indent 1))
  `(if (null clutch-db-test-jdbc-clickhouse-password)
       (ert-skip "Set clutch-db-test-jdbc-clickhouse-password to enable ClickHouse live tests")
     (require 'clutch-db-jdbc)
     (let ((,var (clutch-db-connect
                  'clickhouse
                  (list :host clutch-db-test-jdbc-clickhouse-host
                        :port clutch-db-test-jdbc-clickhouse-port
                        :user clutch-db-test-jdbc-clickhouse-user
                        :password clutch-db-test-jdbc-clickhouse-password
                        :database clutch-db-test-jdbc-clickhouse-database))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-connect ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC connection should start the agent and return a live conn."
  (clutch-db-test--with-oracle conn
    (should (clutch-db-live-p conn))
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC query should return correct columns and rows."
  (clutch-db-test--with-oracle conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n FROM DUAL")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-rows result)) 1)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-manual-commit ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC commit RPC should persist DML."
  (if (null clutch-db-test-jdbc-oracle-password)
      (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
    (require 'clutch-db-jdbc)
    (let ((conn (clutch-db-connect
                 'oracle
                 (list :host clutch-db-test-jdbc-oracle-host
                       :port clutch-db-test-jdbc-oracle-port
                       :user clutch-db-test-jdbc-oracle-user
                       :password clutch-db-test-jdbc-oracle-password
                       :database clutch-db-test-jdbc-oracle-service)))
          (tbl (format "CC_TEST_%d" (abs (random 9999)))))
      (unwind-protect
          (progn
            (should (clutch-db-manual-commit-p conn))
            (clutch-db-query conn (format "CREATE TABLE %s (id NUMBER)" tbl))
            ;; DDL auto-commits in Oracle; subsequent DML starts a new tx.
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1)" tbl))
            (clutch-db-commit conn)
            (let ((result (clutch-db-query
                           conn (format "SELECT COUNT(*) FROM %s" tbl))))
              (should (equal (caar (clutch-db-result-rows result)) "1"))))
        (ignore-errors (clutch-db-query conn (format "DROP TABLE %s" tbl)))
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-rollback ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC rollback RPC should discard uncommitted DML."
  (if (null clutch-db-test-jdbc-oracle-password)
      (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
    (require 'clutch-db-jdbc)
    (let ((conn (clutch-db-connect
                 'oracle
                 (list :host clutch-db-test-jdbc-oracle-host
                       :port clutch-db-test-jdbc-oracle-port
                       :user clutch-db-test-jdbc-oracle-user
                       :password clutch-db-test-jdbc-oracle-password
                       :database clutch-db-test-jdbc-oracle-service)))
          (tbl (format "CC_RB_%d" (abs (random 9999)))))
      (unwind-protect
          (progn
            (clutch-db-query conn (format "CREATE TABLE %s (id NUMBER)" tbl))
            (clutch-db-query conn (format "INSERT INTO %s VALUES (1)" tbl))
            (clutch-db-rollback conn)
            (let ((result (clutch-db-query
                           conn (format "SELECT COUNT(*) FROM %s" tbl))))
              (should (equal (caar (clutch-db-result-rows result)) "0"))))
        (ignore-errors (clutch-db-query conn (format "DROP TABLE %s" tbl)))
        (clutch-db-disconnect conn)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-toggle-auto-commit ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC set-auto-commit RPC should toggle between manual and auto modes."
  (clutch-db-test--with-oracle conn
    ;; Oracle starts in manual-commit mode
    (should (clutch-db-manual-commit-p conn))
    ;; Toggle to auto-commit
    (clutch-db-set-auto-commit conn t)
    (should-not (clutch-db-manual-commit-p conn))
    ;; Toggle back to manual-commit
    (clutch-db-set-auto-commit conn nil)
    (should (clutch-db-manual-commit-p conn))))

(ert-deftest clutch-db-test-jdbc-oracle-live-schema ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC schema introspection should list tables and columns."
  (clutch-db-test--with-oracle conn
    (let ((tbl (format "CC_SCHEMA_%d" (abs (random 9999)))))
      (unwind-protect
          (progn
            (clutch-db-query conn
             (format "CREATE TABLE %s (id NUMBER PRIMARY KEY, name VARCHAR2(64))" tbl))
            (let ((tables (clutch-db-list-tables conn)))
              (should (member tbl tables)))
            (let ((cols (clutch-db-list-columns conn tbl)))
              (should (member "ID" cols))
              (should (member "NAME" cols))))
        (ignore-errors
          (clutch-db-query conn (format "DROP TABLE %s" tbl)))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-row-identity-uses-unique-not-null ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC should use a non-null unique key when no primary key exists."
  (clutch-db-test--with-oracle conn
    (let* ((suffix (abs (random 9999)))
           (tbl (format "CC_UID_%d" suffix))
           (idx (format "CC_UID_UQ_%d" suffix)))
      (unwind-protect
          (progn
            (clutch-db-query
             conn
             (format "CREATE TABLE %s (code VARCHAR2(32) NOT NULL, name VARCHAR2(64), CONSTRAINT %s UNIQUE (code))"
                     tbl idx))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn tbl))))
              (should (equal (plist-get candidate :kind) 'unique-key))
              (should (equal (plist-get candidate :name) idx))
              (should (equal (plist-get candidate :columns) '("CODE")))))
        (ignore-errors
          (clutch-db-query conn (format "DROP TABLE %s" tbl)))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-row-identity-falls-back-to-rowid ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC should use ROWID to update a table without a logical key."
  (clutch-db-test--with-oracle conn
    (let ((tbl (format "CC_ROWID_%d" (abs (random 9999)))))
      (unwind-protect
          (progn
            (clutch-db-query conn
                             (format "CREATE TABLE %s (name VARCHAR2(64))"
                                     tbl))
            (clutch-db-query conn
                             (format "INSERT INTO %s (name) VALUES ('before')"
                                     tbl))
            (let ((candidate (car (clutch-db-row-identity-candidates
                                   conn tbl))))
              (should (equal (plist-get candidate :kind) 'row-locator))
              (should (equal (plist-get candidate :name) "ROWID"))
              (should (equal (plist-get candidate :select-expressions)
                             '("ROWID")))
              (should (equal (plist-get candidate :where-sql) "ROWID = ?")))
            (let* ((rows (clutch-db-result-rows
                          (clutch-db-query
                           conn
                           (format "SELECT ROWID, name FROM %s" tbl))))
                   (rowid (caar rows))
                   (update-result
                    (clutch-db-execute-params
                     conn
                     (format "UPDATE %s SET name = ? WHERE ROWID = ?" tbl)
                     (list "after" rowid))))
              (should (equal (cadar rows) "before"))
              (should (equal (clutch-db-result-affected-rows update-result)
                             1))
              (should (equal (clutch-db-result-rows
                              (clutch-db-query
                               conn
                               (format "SELECT name FROM %s WHERE ROWID = '%s'"
                                       tbl rowid)))
                             '(("after"))))))
        (ignore-errors
          (clutch-db-query conn (format "DROP TABLE %s" tbl)))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-cancel-keeps-connection-usable ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC cancel should interrupt the query and keep the session usable."
  (clutch-db-test--with-oracle conn
    (let ((request-id
           (clutch-jdbc--send
            "execute"
            `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
              (sql . "SELECT COUNT(*) FROM ALL_OBJECTS a, ALL_OBJECTS b")
              (fetch-size . ,clutch-jdbc-fetch-size))))
          cancel-result)
      (puthash request-id t clutch-jdbc--ignored-response-ids)
      (with-timeout
          (5 (ert-fail "Oracle live execute did not become cancellable"))
        (while (not (eq t (plist-get cancel-result :cancelled)))
          (setq cancel-result
                (clutch-jdbc--rpc
                 "cancel"
                 `((conn-id . ,(clutch-jdbc-conn-conn-id conn)))
                 (clutch-jdbc--conn-rpc-timeout conn)))
          (unless (eq t (plist-get cancel-result :cancelled))
            (sleep-for 0.05))))
      (should (eq t (plist-get cancel-result :cancelled)))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42")))
      (should-not (gethash request-id clutch-jdbc--ignored-response-ids)))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-query-error-carries-diagnostics ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle JDBC errors should return structured diagnostics."
  (clutch-db-test--with-oracle conn
    (let* ((token (format "definitely_missing_%d" (abs (random 999999))))
           (request-id
            (clutch-jdbc--send
             "execute"
             `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
               (sql . ,(format "SELECT %s FROM DUAL" token))
               (fetch-size . ,clutch-jdbc-fetch-size))))
           (response (clutch-jdbc--recv-response
                      request-id
                      (clutch-jdbc--conn-rpc-timeout conn)
                      "execute")))
      (should-not (eq t (plist-get response :ok)))
      (let ((diag (plist-get response :diag)))
        (should (equal (plist-get diag :op) "execute"))
        (should (= (plist-get diag :request-id) request-id))
        (should (= (plist-get diag :conn-id) (clutch-jdbc-conn-conn-id conn)))
        (should (equal (plist-get diag :category) "query"))
        (should (string-match-p
                 (upcase token)
                 (upcase (plist-get diag :raw-message)))))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-query-error-carries-debug-payload ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle JDBC errors should carry opt-in backend debug payloads."
  (let ((clutch-debug-mode t))
    (clutch-db-test--with-oracle conn
      (let* ((token (format "definitely_missing_%d" (abs (random 999999))))
             (request-id
              (clutch-jdbc--send
               "execute"
               `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
                 (sql . ,(format "SELECT %s FROM DUAL" token))
                 (fetch-size . ,clutch-jdbc-fetch-size))))
             (response (clutch-jdbc--recv-response
                        request-id
                        (clutch-jdbc--conn-rpc-timeout conn)
                        "execute")))
        (should-not (eq t (plist-get response :ok)))
        (let ((debug (plist-get response :debug)))
          (should (stringp (plist-get debug :stack-trace)))
          (should (string-match-p "java\\.sql\\."
                                  (plist-get debug :stack-trace)))
          (should (= (plist-get (plist-get debug :request-context) :fetch-size)
                     clutch-jdbc-fetch-size)))
        (let ((result (clutch-db-query conn "SELECT 44 AS answer FROM DUAL")))
          (should (equal (caar (clutch-db-result-rows result)) "44")))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query-error-caches-diagnostics ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle JDBC query errors should stay on the current connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (format "definitely_missing_%d" (abs (random 999999)))))
        (condition-case err
            (progn
              (clutch-db-query conn (format "SELECT %s FROM DUAL" token))
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let ((details (clutch-db-error-details conn)))
             (should (equal (plist-get (plist-get details :diag) :op) "execute"))
             (should (= (plist-get (plist-get details :diag) :conn-id)
                        (clutch-jdbc-conn-conn-id conn)))
             (should (equal (plist-get (plist-get details :diag) :category) "query"))
             (should (string-match-p
                      (upcase token)
                      (upcase (plist-get (plist-get details :diag) :raw-message))))))))
      (let ((result (clutch-db-query conn "SELECT 43 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "43"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-query-error-caches-debug-payload ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle JDBC query errors should cache opt-in debug payloads."
  (let ((clutch-debug-mode t)
        (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (format "definitely_missing_%d" (abs (random 999999)))))
        (condition-case err
            (progn
              (clutch-db-query conn (format "SELECT %s FROM DUAL" token))
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let ((debug (plist-get (clutch-db-error-details conn) :debug)))
             (should (stringp (plist-get debug :stack-trace)))
             (should (string-match-p "java\\.sql\\."
                                     (plist-get debug :stack-trace)))))))
      (let ((result (clutch-db-query conn "SELECT 45 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "45"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-wire-schema-switch-error-carries-generated-sql ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Wire-level Oracle schema-switch failures should expose generated SQL."
  (clutch-db-test--with-oracle conn
    (let* ((token (format "MISSING_SCHEMA_%d" (abs (random 999999))))
           (request-id
            (clutch-jdbc--send
             "set-current-schema"
             `((conn-id . ,(clutch-jdbc-conn-conn-id conn))
               (schema . ,token))))
           (response (clutch-jdbc--recv-response
                      request-id
                      (clutch-jdbc--conn-rpc-timeout conn)
                      "set-current-schema")))
      (should-not (eq t (plist-get response :ok)))
      (let* ((diag (plist-get response :diag))
             (context (plist-get diag :context))
             (generated-sql (plist-get context :generated-sql)))
        (should (equal (plist-get diag :op) "set-current-schema"))
        (should (= (plist-get diag :request-id) request-id))
        (should (= (plist-get diag :conn-id) (clutch-jdbc-conn-conn-id conn)))
        (should (equal (plist-get diag :category) "metadata"))
        (should (equal (plist-get context :schema) token))
        (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" generated-sql))
        (should (string-match-p token generated-sql)))
      (let ((result (clutch-db-query conn "SELECT 42 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "42"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-schema-switch-error-caches-generated-sql ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "High-level Oracle schema-switch failures should stay on the current connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (clutch-db-test--with-oracle conn
      (let ((token (format "MISSING_SCHEMA_%d" (abs (random 999999)))))
        (condition-case err
            (progn
              (clutch-db-set-current-schema conn token)
              (should nil))
          (clutch-db-error
           (should (stringp (cadr err)))
           (let* ((details (clutch-db-error-details conn))
                  (diag (plist-get details :diag))
                  (context (plist-get diag :context))
                  (generated-sql (plist-get context :generated-sql)))
             (should (equal (plist-get diag :op) "set-current-schema"))
             (should (= (plist-get diag :conn-id)
                        (clutch-jdbc-conn-conn-id conn)))
             (should (equal (plist-get diag :category) "metadata"))
             (should (equal (plist-get context :schema) token))
             (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" generated-sql))
             (should (string-match-p token generated-sql))))))
      (let ((result (clutch-db-query conn "SELECT 43 AS answer FROM DUAL")))
        (should (equal (caar (clutch-db-result-rows result)) "43"))))))

(ert-deftest clutch-db-test-jdbc-oracle-live-low-priv-completion ()
  :tags '(:db-live :jdbc-live :oracle-live)
  "Oracle JDBC low-privilege users should still get table completion and discovery."
  (if (null clutch-db-test-jdbc-oracle-password)
      (ert-skip "Set clutch-db-test-jdbc-oracle-password to enable Oracle live tests")
    (require 'clutch-db-jdbc)
    (let* ((admin (clutch-db-connect
                   'oracle
                   (list :host clutch-db-test-jdbc-oracle-host
                         :port clutch-db-test-jdbc-oracle-port
                         :user clutch-db-test-jdbc-oracle-user
                         :password clutch-db-test-jdbc-oracle-password
                         :database clutch-db-test-jdbc-oracle-service)))
           (user (format "CCLP_%d" (abs (random 999999))))
           (password "CcLowpriv123")
           (table-name "CC_LOWPRIV_TABLE"))
      (unwind-protect
          (progn
            (clutch-db-query
             admin
             (format "CREATE USER %s IDENTIFIED BY %s" user password))
            (clutch-db-query
             admin
             (format "GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO %s"
                     user))
            (let ((conn (clutch-db-connect
                         'oracle
                         (list :host clutch-db-test-jdbc-oracle-host
                               :port clutch-db-test-jdbc-oracle-port
                               :user user
                               :password password
                               :database clutch-db-test-jdbc-oracle-service))))
              (unwind-protect
                  (progn
                    (clutch-db-query
                     conn
                     (format "CREATE TABLE %s (id NUMBER PRIMARY KEY)" table-name))
                    (let ((tables (clutch-db-complete-tables conn "CC_LOW")))
                      (should (member table-name tables)))
                    (let ((entries (clutch-db-search-table-entries conn "CC_LOW")))
                      (should
                       (seq-some
                        (lambda (entry)
                          (and (equal (plist-get entry :name) table-name)
                               (equal (plist-get entry :schema) user)))
                        entries))))
                (clutch-db-disconnect conn))))
        (ignore-errors
          (clutch-db-query admin (format "DROP USER %s CASCADE" user)))
        (clutch-db-disconnect admin)))))

(ert-deftest clutch-db-test-jdbc-mssql-live-connect ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC connection should return a live conn."
  (clutch-db-test--with-mssql conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "SQL Server"))))

(ert-deftest clutch-db-test-jdbc-mssql-live-query ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC query should return one row for SELECT 1."
  (clutch-db-test--with-mssql conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-rows result)) 1))
      (should (equal (format "%s" (caar (clutch-db-result-rows result))) "1")))))

(ert-deftest clutch-db-test-jdbc-mssql-live-dml ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC should handle temporary-table DML."
  (clutch-db-test--with-mssql conn
    (let ((tbl (format "#cc_test_%d" (abs (random 9999)))))
      (clutch-db-query conn
       (format "CREATE TABLE %s (id INT PRIMARY KEY, val NVARCHAR(32))" tbl))
      (let ((result (clutch-db-query conn
                     (format "INSERT INTO %s VALUES (1, 'a')" tbl))))
        (should (= (clutch-db-result-affected-rows result) 1)))
      (let ((result (clutch-db-query conn
                     (format "SELECT COUNT(*) AS n FROM %s" tbl))))
        (should (equal (format "%s" (caar (clutch-db-result-rows result))) "1"))))))

(ert-deftest clutch-db-test-jdbc-mssql-live-schema ()
  :tags '(:db-live :jdbc-live :mssql-live)
  "SQL Server JDBC schema introspection should return table entries."
  (clutch-db-test--with-mssql conn
    (let ((entries (clutch-db-list-table-entries conn)))
      (should (listp entries))
      (should (> (length entries) 0)))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-connect ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC connection should return a live conn."
  (clutch-db-test--with-clickhouse conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "ClickHouse"))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-query ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC query should return one row for SELECT 1."
  (clutch-db-test--with-clickhouse conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-rows result)) 1))
      (should (equal (format "%s" (caar (clutch-db-result-rows result))) "1")))))

(ert-deftest clutch-db-test-jdbc-clickhouse-live-schema ()
  :tags '(:db-live :jdbc-live :clickhouse-live)
  "ClickHouse JDBC schema introspection should return table entries."
  (clutch-db-test--with-clickhouse conn
    (let ((entries (clutch-db-list-table-entries conn)))
      (should (listp entries))
      (should (> (length entries) 0)))))

;;;; Unit tests — clutch--format-value and clutch--value-to-literal

(ert-deftest clutch-db-test-format-value-primitives ()
  "Format-value handles nil, :false, strings, and numbers correctly."
  (should (equal (clutch--format-value nil)    "NULL"))
  (should (equal (clutch--format-value :false) "false"))
  (should (equal (clutch--format-value "hi")   "hi"))
  (should (equal (clutch--format-value 42)     "42"))
  (should (equal (clutch--format-value 3.14)   "3.14")))

(ert-deftest clutch-db-test-format-value-json-hash-table ()
  "Format-value serializes a hash-table (MySQL/PG JSON object) to a JSON string."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (let ((result (clutch--format-value ht)))
      (should (stringp result))
      (should (string-match-p "\"key\"" result))
      (should (string-match-p "\"val\"" result)))))

(ert-deftest clutch-db-test-format-value-json-vector ()
  "Format-value serializes a vector (MySQL/PG JSON array) to a JSON string."
  (should (equal (clutch--format-value [1 2 3]) "[1,2,3]")))

(ert-deftest clutch-db-test-format-value-json-serialization-error-surfaces ()
  "Format-value should signal `clutch-db-error' when JSON serialization fails."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (cl-letf (((symbol-function 'json-serialize)
               (lambda (_value)
                 (signal 'wrong-type-argument '("json serialization failed")))))
      (let ((err (should-error (clutch--format-value ht)
                               :type 'clutch-db-error)))
        (should (string-match-p
                 "Cannot serialize query result value as JSON"
                 (cadr err)))))))

(ert-deftest clutch-db-test-value-to-literal-json-hash-table ()
  "Value-to-literal escapes a JSON hash-table as a quoted SQL string literal."
  (let* ((ht (make-hash-table :test 'equal))
         (_ (puthash "k" "v" ht))
         (conn (make-clutch-jdbc-conn
                :params '(:driver sqlserver :user "sa")))
         (clutch-connection conn)
         (result (clutch--value-to-literal ht)))
    (should (stringp result))
    ;; Result should be a quoted string containing the JSON
    (should (string-match-p "\"k\"" result))
    (should (string-match-p "\"v\"" result))))

(ert-deftest clutch-db-test-value-to-literal-json-vector ()
  "Value-to-literal escapes a JSON vector as a quoted SQL string literal."
  (let* ((conn (make-clutch-jdbc-conn
                :params '(:driver sqlserver :user "sa")))
         (clutch-connection conn)
         (result (clutch--value-to-literal [1 2 3])))
    (should (stringp result))
    (should (string-match-p "1" result))
    (should (string-match-p "2" result))))

;;;; Cross-backend consistency tests

(ert-deftest clutch-db-test-cross-type-categories ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends use consistent type categories."
  (when (and (null clutch-db-test-mysql-password)
             (null clutch-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
  ;; Test numeric
  (clutch-db-test--with-local-mysql-tls
    (let ((mysql-conn (when clutch-db-test-mysql-password
                        (clutch-db-connect
                         'mysql
                         (list :host clutch-db-test-mysql-host
                               :port clutch-db-test-mysql-port
                               :user clutch-db-test-mysql-user
                               :password clutch-db-test-mysql-password
                               :database clutch-db-test-mysql-database))))
          (pg-conn (when clutch-db-test-pg-password
                     (clutch-db-connect
                      'pg
                      (list :host clutch-db-test-pg-host
                            :port clutch-db-test-pg-port
                            :user clutch-db-test-pg-user
                            :password clutch-db-test-pg-password
                            :database clutch-db-test-pg-database)))))
      (unwind-protect
          (progn
            ;; Both should return numeric type-category for integers
            (when mysql-conn
              (let* ((result (clutch-db-query mysql-conn "SELECT 42 AS n"))
                     (cols (clutch-db-result-columns result)))
                (should (eq (plist-get (car cols) :type-category) 'numeric))))
            (when pg-conn
              (let* ((result (clutch-db-query pg-conn "SELECT 42 AS n"))
                     (cols (clutch-db-result-columns result)))
                (should (eq (plist-get (car cols) :type-category) 'numeric)))))
        (when mysql-conn (clutch-db-disconnect mysql-conn))
        (when pg-conn (clutch-db-disconnect pg-conn))))))

(ert-deftest clutch-db-test-cross-null-handling ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends handle NULL values consistently."
  (when (and (null clutch-db-test-mysql-password)
             (null clutch-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
  (clutch-db-test--with-local-mysql-tls
    (dolist (backend-spec (list (cons 'mysql
                                      (list :host clutch-db-test-mysql-host
                                            :port clutch-db-test-mysql-port
                                            :user clutch-db-test-mysql-user
                                            :password clutch-db-test-mysql-password
                                            :database clutch-db-test-mysql-database))
                                (cons 'pg
                                      (list :host clutch-db-test-pg-host
                                            :port clutch-db-test-pg-port
                                            :user clutch-db-test-pg-user
                                            :password clutch-db-test-pg-password
                                            :database clutch-db-test-pg-database))))
      (when (plist-get (cdr backend-spec) :password)
        (let ((conn (clutch-db-connect (car backend-spec) (cdr backend-spec))))
          (unwind-protect
              (let* ((result (clutch-db-query conn "SELECT NULL AS n"))
                     (row (car (clutch-db-result-rows result))))
                (should (null (car row))))
            (clutch-db-disconnect conn)))))))

;;;; Unit tests — clutch-db-live-p (JDBC identity check)

(ert-deftest clutch-db-test-jdbc-live-p-variants ()
  "JDBC liveness should depend on current-agent identity and process state."
  (dolist (case '(("matching-live-process" fake-proc fake-proc t t)
                  ("dead-process" dead-proc dead-proc nil nil)
                  ("nil-agent-process" old-proc nil t nil)
                  ("mismatched-process" old-proc new-proc t nil)))
    (pcase-let ((`(,label ,conn-proc ,agent-proc ,livep ,expected) case))
      (ert-info ((format "live-p case: %s" label))
        (cl-letf (((symbol-function 'process-live-p) (lambda (_p) livep)))
          (let ((clutch-jdbc--agent-process agent-proc))
            (if expected
                (should (clutch-db-live-p
                         (make-clutch-jdbc-conn :process conn-proc :conn-id 1
                                                :params nil :busy nil)))
              (should-not (clutch-db-live-p
                           (make-clutch-jdbc-conn :process conn-proc :conn-id 1
                                                  :params nil :busy nil))))))))))

;;;; Unit tests — clutch-jdbc--recv-response timeout behaviour

(ert-deftest clutch-db-test-jdbc-recv-response-returns-matching ()
  "When a matching response is already queued, recv-response returns it immediately.
It does so without touching the agent process."
  (let ((clutch-jdbc--agent-process 'live-proc)
        (clutch-jdbc--response-queue
         (list '(:id 42 :ok t :result (:conn-id 1)))))
    (let ((result (clutch-jdbc--recv-response 42 10.0)))
      ;; Agent must NOT be killed.
      (should (eq clutch-jdbc--agent-process 'live-proc))
      (should (equal (plist-get result :id) 42)))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-kills-agent ()
  "When the RPC timeout fires, the agent process is killed and state is reset."
  (let (deleted-proc)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
              ((symbol-function 'delete-process)  (lambda (p) (setq deleted-proc p)))
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
      (let ((clutch-jdbc--agent-process 'fake-proc)
            (clutch-jdbc--response-queue '(stale)))
        ;; Timeout of 0.0 expires immediately.
        (should-error (clutch-jdbc--recv-response 9999 0.0) :type 'clutch-db-error)
        (should (eq deleted-proc 'fake-proc))
        (should (null clutch-jdbc--agent-process))
        (should (null clutch-jdbc--response-queue))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-clears-async-callbacks ()
  "Sync timeout should clear pending async callbacks immediately."
  (let ((clutch-jdbc--async-callbacks (make-hash-table :test 'eql))
        (cancelled nil))
    (puthash 77 (list :callback #'ignore :errback #'ignore :timer 'fake-timer)
             clutch-jdbc--async-callbacks)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
              ((symbol-function 'delete-process) #'ignore)
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil))
              ((symbol-function 'cancel-timer)
               (lambda (timer)
                 (push timer cancelled))))
      (let ((clutch-jdbc--agent-process 'fake-proc)
            (clutch-jdbc--response-queue nil))
        (should-error (clutch-jdbc--recv-response 9999 0.0) :type 'clutch-db-error)
        (should-not (gethash 77 clutch-jdbc--async-callbacks))
        (should (equal cancelled '(fake-timer)))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-agent-already-dead ()
  "When the agent already died, recv-response reports agent exit clearly."
  (let (deleted-proc)
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) nil))
              ((symbol-function 'delete-process)  (lambda (p) (setq deleted-proc p)))
              ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
      (let ((clutch-jdbc--agent-process 'dead-proc)
            (clutch-jdbc--response-queue nil))
        (condition-case err
            (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
          (clutch-db-error
           (should (string-match-p "exited before replying" (cadr err)))))
        (should (null deleted-proc))
        (should (null clutch-jdbc--agent-process))))))

(ert-deftest clutch-db-test-jdbc-recv-response-timeout-error-contains-connection-lost ()
  "A live-but-stuck agent still reports 'Connection lost' on timeout."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
            ((symbol-function 'delete-process)  #'ignore)
            ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
    (let ((clutch-jdbc--agent-process 'fake-proc)
          (clutch-jdbc--response-queue nil))
      (condition-case err
          (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
        (clutch-db-error
         (should (string-match-p "Connection lost" (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-recv-response-connect-timeout-omits-reconnect-hint ()
  "Connect timeouts should not tell users to reconnect an unestablished session."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t))
            ((symbol-function 'delete-process)  #'ignore)
            ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
    (let ((clutch-jdbc--agent-process 'fake-proc)
          (clutch-jdbc--response-queue nil))
      (condition-case err
          (progn (clutch-jdbc--recv-response 9999 0.0 "connect") (should nil))
        (clutch-db-error
         (should (string-match-p "Connection attempt timed out" (cadr err)))
         (should-not (string-match-p "reconnect with C-c C-e" (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-recv-response-agent-exit-reports-java-version-mismatch ()
  "An early agent exit with UnsupportedClassVersionError should report Java mismatch."
  (let ((stderr (get-buffer-create "*clutch-jdbc-agent-stderr*")))
    (unwind-protect
        (progn
          (with-current-buffer stderr
            (erase-buffer)
            (insert "Exception in thread \"main\" java.lang.UnsupportedClassVersionError: clutch/jdbc/Agent has been compiled by a more recent version of the Java Runtime\n"))
          (cl-letf (((symbol-function 'process-live-p) (lambda (_p) nil))
                    ((symbol-function 'delete-process) #'ignore)
                    ((symbol-function 'accept-process-output) (lambda (_p _s) nil)))
            (let ((clutch-jdbc--agent-process 'dead-proc)
                  (clutch-jdbc--response-queue nil)
                  (clutch-jdbc-agent-java-executable "java"))
              (condition-case err
                  (progn (clutch-jdbc--recv-response 9999 0.0) (should nil))
                (clutch-db-error
                 (should (string-match-p "requires a newer Java runtime" (cadr err)))
                 (should (string-match-p "`java'" (cadr err))))))))
      (kill-buffer stderr))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-points-to-debug-workflow-when-disabled ()
  "Connect errors should point users at the debug workflow when capture is off."
  (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
            ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 7))
            ((symbol-function 'clutch-jdbc--recv-response)
             (lambda (&rest _args)
               '(:ok nil
                 :error "diag-token-2038"
                 :diag (:category "connect")))))
    (condition-case err
        (progn
          (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
          (should nil))
      (clutch-db-error
       (should (string-match-p "diag-token-2038" (cadr err)))
       (should (string-match-p "clutch-debug-mode" (cadr err)))
       (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                               (cadr err)))))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-points-to-debug-buffer-when-enabled ()
  "Connect errors should point directly at the debug buffer when capture is on."
  (let ((clutch-debug-mode t))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 7))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 '(:ok nil
                   :error "diag-token-2038"
                   :diag (:category "connect")))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "diag-token-2038" (cadr err)))
         (should-not (string-match-p "clutch-debug-mode" (cadr err)))
         (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                 (cadr err))))))))

(ert-deftest clutch-db-test-jdbc-send-adds-debug-flag-when-debug-mode-enabled ()
  "JDBC requests should opt into backend debug payloads only in debug mode."
  (let ((clutch-jdbc--next-request-id 0)
        (clutch-debug-mode t)
        sent)
    (cl-letf (((symbol-function 'process-send-string)
               (lambda (_proc msg)
                 (setq sent msg))))
      (let ((clutch-jdbc--agent-process 'fake-proc))
        (clutch-jdbc--send "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb"))))
      (should (string-match-p "\"debug\":true" sent)))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-carries-structured-details ()
  "Connect errors should carry structured details in the condition data."
  (let ((diag '(:category "connect"
                :op "connect"
                :request-id 71
                :exception-class "java.sql.SQLNonTransientConnectionException"
                :sql-state "08071"
                :context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>"
                          :generated-sql "ALTER SESSION SET CURRENT_SCHEMA = \"REPORTING\""
                          :property-keys ("http_header_COOKIE" "socket_timeout"))
                :cause-chain ((:exception-class "java.sql.SQLNonTransientConnectionException"
                               :message "reason-71")
                              (:exception-class "java.net.ConnectException"
                               :message "root-71")))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 71))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-71"
                   :diag ,diag))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "summary-71" (cadr err)))
         (let* ((details (nth 2 err))
                (diag (plist-get details :diag))
                (context (plist-get diag :context)))
           (should (equal (plist-get details :backend) 'jdbc))
           (should (equal (plist-get details :summary) "summary-71"))
           (should (equal (plist-get diag :category) "connect"))
           (should (= (plist-get diag :request-id) 71))
           (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA"
                                   (plist-get context :generated-sql)))
           (should (equal (plist-get context :property-keys)
                          '("http_header_COOKIE" "socket_timeout")))
           (should (string-match-p "<redacted>"
                                   (plist-get context :redacted-url))))
         (should-not (string-match-p "cookie-secret-71"
                                     (prin1-to-string (nth 2 err)))))))))

(ert-deftest clutch-db-test-jdbc-rpc-connect-error-carries-debug-payload ()
  "Structured JDBC details should preserve opt-in backend debug payloads."
  (let ((diag '(:category "connect"
                :op "connect"
                :request-id 72))
        (debug '(:thread "clutch-jdbc-request"
                 :request-context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>")
                 :stack-trace "java.sql.SQLNonTransientConnectionException: boom")))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 72))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-72"
                   :diag ,diag
                   :debug ,debug))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc "connect" '((url . "jdbc:clickhouse://127.0.0.1:8123/testdb")))
            (should nil))
        (clutch-db-error
         (let ((details (nth 2 err)))
           (should (equal (plist-get details :summary) "summary-72"))
           (should (equal (plist-get (plist-get details :debug) :thread)
                          "clutch-jdbc-request"))
           (should (string-match-p "SQLNonTransientConnectionException"
                                   (plist-get (plist-get details :debug) :stack-trace)))))))))

(ert-deftest clutch-db-test-jdbc-rpc-on-conn-stores-structured-diagnostics-on-connection ()
  "Connection-scoped JDBC errors should stay on that connection."
  (let ((clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
        (conn (make-clutch-jdbc-conn :process 'proc :conn-id 11 :params '(:driver oracle)))
        (diag '(:category "query"
                :op "execute"
                :request-id 88
                :conn-id 11
                :raw-message "reason-88"
                :context (:generated-sql "SELECT * FROM hidden_table"))))
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
              ((symbol-function 'clutch-jdbc--send) (lambda (&rest _args) 88))
              ((symbol-function 'clutch-jdbc--recv-response)
               (lambda (&rest _args)
                 `(:ok nil
                   :error "summary-88"
                   :diag ,diag))))
      (condition-case err
          (progn
            (clutch-jdbc--rpc-on-conn conn "execute" '((conn-id . 11) (sql . "SELECT 1")))
            (should nil))
        (clutch-db-error
         (should (string-match-p "summary-88" (cadr err)))
         (let* ((details (clutch-db-error-details conn))
                (stored-diag (plist-get details :diag))
                (context (plist-get stored-diag :context)))
           (should (equal (plist-get details :summary) "summary-88"))
           (should (equal (plist-get stored-diag :op) "execute"))
           (should (= (plist-get stored-diag :conn-id) 11))
           (should (string-match-p "hidden_table"
                                   (plist-get context :generated-sql)))))))))

(ert-deftest clutch-db-test-jdbc-interrupt-cancel-success-returns-t ()
  "JDBC interrupt should return t only after a confirmed cancel acknowledgement."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc--agent-process 'fake-proc)
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue '((:id 99 :ok t)))
        captured-op captured-params)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (op params)
                 (setq captured-op op
                       captured-params params)
                 99)))
      (should (clutch-db-interrupt-query conn))
      (should (equal captured-op "cancel"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should (gethash 41 clutch-jdbc--ignored-response-ids)))))

(ert-deftest clutch-db-test-jdbc-interrupt-cancel-timeout-does-not-kill-agent ()
  "A slow cancel should degrade to nil without killing the shared JDBC agent."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc-cancel-timeout-seconds 0.1)
        (clutch-jdbc--agent-process 'fake-proc)
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        (clutch-jdbc--response-queue nil)
        deleted-proc
        send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params)
                 (setq send-called t)
                 99))
              ((symbol-function 'process-live-p)
               (lambda (_proc) t))
              ((symbol-function 'accept-process-output)
               (lambda (_proc _secs) nil))
              ((symbol-function 'delete-process)
               (lambda (proc)
                 (setq deleted-proc proc))))
      (should-not (clutch-db-interrupt-query conn))
      (should send-called)
      (should (eq clutch-jdbc--agent-process 'fake-proc))
      (should-not deleted-proc))))

(ert-deftest clutch-db-test-jdbc-interrupt-no-busy-request-returns-nil ()
  "JDBC interrupt should return nil and avoid RPC traffic when idle."
  (let ((conn (make-clutch-jdbc-conn :conn-id 7
                                     :params '(:driver jdbc :rpc-timeout 12)))
        (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql))
        send-called)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (&rest _args)
                 (setq send-called t)
                 (error "Cancel should not be sent"))))
      (should-not (clutch-db-interrupt-query conn))
      (should-not send-called)
      (should (= (hash-table-count clutch-jdbc--ignored-response-ids) 0)))))

(ert-deftest clutch-db-test-jdbc-disconnect-timeout-does-not-kill-agent ()
  "A slow disconnect should clean local state without killing the shared agent."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc-disconnect-timeout-seconds 0.1)
         (clutch-jdbc--agent-process 'fake-proc)
         (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         (clutch-jdbc--response-queue nil)
         deleted-proc
         send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (_op _params)
                 (setq send-called t)
                 99))
              ((symbol-function 'process-live-p)
               (lambda (_proc) t))
              ((symbol-function 'accept-process-output)
               (lambda (_proc _secs) nil))
              ((symbol-function 'delete-process)
               (lambda (proc)
                 (setq deleted-proc proc))))
      (clutch-db-disconnect conn)
      (should send-called)
      (should (eq clutch-jdbc--agent-process 'fake-proc))
      (should-not deleted-proc)
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should-not (gethash conn clutch-jdbc--error-details-by-conn))
      (should-not (gethash 7 clutch-jdbc--connections-by-id)))))

(ert-deftest clutch-db-test-jdbc-clear-error-details-forgets-conn-cache ()
  "Clearing JDBC diagnostics should remove the connection-scoped cache entry."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq)))
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (clutch-db-clear-error-details conn)
    (should-not (gethash conn clutch-jdbc--error-details-by-conn))))

(ert-deftest clutch-db-test-jdbc-disconnect-skips-rpc-when-agent-dead ()
  "Disconnect should skip the RPC when the shared JDBC agent is already dead."
  (let* ((conn (make-clutch-jdbc-conn :conn-id 7
                                      :params '(:driver jdbc :rpc-timeout 12)))
         (clutch-jdbc--agent-process nil)
         (clutch-jdbc--busy-request-ids (make-hash-table :test 'eq))
         (clutch-jdbc--error-details-by-conn (make-hash-table :test 'eq))
         (clutch-jdbc--connections-by-id (make-hash-table :test 'eql))
         send-called)
    (puthash conn 41 clutch-jdbc--busy-request-ids)
    (puthash conn '(:summary "old error") clutch-jdbc--error-details-by-conn)
    (puthash 7 conn clutch-jdbc--connections-by-id)
    (cl-letf (((symbol-function 'clutch-jdbc--send)
               (lambda (&rest _args)
                 (setq send-called t)
                 (error "Disconnect RPC should not be sent"))))
      (clutch-db-disconnect conn)
      (should-not send-called)
      (should-not (gethash conn clutch-jdbc--busy-request-ids))
      (should-not (gethash conn clutch-jdbc--error-details-by-conn))
      (should-not (gethash 7 clutch-jdbc--connections-by-id)))))

(ert-deftest clutch-db-test-jdbc-agent-filter-drops-invalid-json-lines ()
  "Malformed agent output should be ignored instead of enqueuing nil."
  (let ((buf (generate-new-buffer " *clutch-jdbc-filter-test*"))
        (clutch-jdbc--response-queue nil))
    (unwind-protect
        (cl-letf (((symbol-function 'process-buffer) (lambda (_proc) buf))
                  ((symbol-function 'clutch-jdbc--dispatch-async-response)
                   (lambda (_parsed) nil)))
          (clutch-jdbc--agent-filter 'fake-proc
                                     "{\"id\":1,\"ok\":true}\nnot-json\n")
          (should (equal clutch-jdbc--response-queue
                         '((:id 1 :ok t)))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest clutch-db-test-jdbc-agent-filter-drops-ignored-response-ids ()
  "Interrupted JDBC request responses should be dropped instead of queued."
  (let ((buf (generate-new-buffer " *clutch-jdbc-filter-test*"))
        (clutch-jdbc--response-queue nil)
        (clutch-jdbc--ignored-response-ids (make-hash-table :test 'eql)))
    (puthash 41 t clutch-jdbc--ignored-response-ids)
    (unwind-protect
        (cl-letf (((symbol-function 'process-buffer) (lambda (_proc) buf))
                  ((symbol-function 'clutch-jdbc--dispatch-async-response)
                   (lambda (_parsed) nil)))
          (clutch-jdbc--agent-filter 'fake-proc
                                     "{\"id\":41,\"ok\":false,\"error\":\"Query cancelled\"}\n{\"id\":42,\"ok\":true}\n")
          (should-not (gethash 41 clutch-jdbc--ignored-response-ids))
          (should (equal clutch-jdbc--response-queue
                         '((:id 42 :ok t)))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))
;;; clutch-db-test.el ends here
