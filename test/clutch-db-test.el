;;; clutch-db-test.el --- Tests for clutch-db -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the clutch-db generic database interface.
;;
;; Unit tests run without a database server.
;; Live tests require both MySQL and PostgreSQL:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;   docker run -d -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
;;
;; Note: MySQL 8 defaults to `caching_sha2_password'.  The native mysql.el
;; client retries with TLS when the server requires a secure channel; local
;; container certificates are typically self-signed, so the MySQL live helpers
;; bind `mysql-tls-verify-server' to nil unless the test environment installs a
;; trusted CA.
;;
;; Run unit tests:
;;   emacs -batch -L .. -l ert -l clutch-db-test \
;;     -f ert-run-tests-batch-and-exit
;;
;; Run live tests:
;;   emacs -batch -L .. -l ert -l clutch-db-test \
;;     --eval '(setq clutch-db-test-mysql-password "test")' \
;;     --eval '(setq clutch-db-test-pg-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'clutch-db)
(require 'clutch-db-jdbc)

;; `clutch--schema-cache' lives in clutch.el (the UI layer), which is not
;; loaded in this test batch.  Declare it special here so that `let' bindings
;; in the cache-based completion tests create dynamic (not lexical) bindings
;; that the bytecode in clutch-db-jdbc.el can see.
(defvar clutch--schema-cache (make-hash-table :test 'equal))
;; `mysql-tls-verify-server' is defined in mysql.el; declare it special here so
;; local test bindings remain dynamic even before the backend requires mysql.el.
(defvar mysql-tls-verify-server)

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

(defmacro clutch-db-test--with-local-mysql-tls (&rest body)
  "Run BODY with MySQL TLS verification disabled for local self-signed certs."
  (declare (indent 0))
  `(progn
     (require 'mysql)
     (cl-letf (((symbol-value 'mysql-tls-verify-server) nil))
       ,@body)))

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
  (let ((batches '((:rows ((3) (4)) :done nil)
                   (:rows ((5)) :done t)))
        (conn (make-clutch-jdbc-conn :params '(:rpc-timeout 9))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
                 (should (equal op "fetch"))
                 (should (= (alist-get 'cursor-id params) 9))
                 (should (= timeout-seconds 9))
                 (pop batches))))
      (should (equal (clutch-jdbc--fetch-all conn 9)
                     '((3) (4) (5)))))))

(ert-deftest clutch-db-test-jdbc-referencing-objects-maps-rpc-response ()
  "JDBC reverse-reference lookup should map RPC rows to object entries."
  (let (captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout-seconds)
                 (setq captured-op op
                       captured-params params)
                 '(:objects ((:name "ORDERS" :schema "APP")
                             (:name "PAYMENTS" :schema "APP"))))))
      (should (equal
               (clutch-db-referencing-objects
                (make-clutch-jdbc-conn :conn-id 7 :params '(:backend oracle :schema "APP"))
                "CUSTOMERS")
               '((:name "ORDERS" :type "TABLE" :schema "APP" :source-schema "APP")
                 (:name "PAYMENTS" :type "TABLE" :schema "APP" :source-schema "APP"))))
      (should (equal captured-op "get-referencing-objects"))
      (should (= (alist-get 'conn-id captured-params) 7))
      (should (equal (alist-get 'table captured-params) "CUSTOMERS"))
      (should (equal (alist-get 'schema captured-params) "APP")))))

(ert-deftest clutch-db-test-jdbc-connect-maps-timeouts ()
  "JDBC connect should map explicit timeout phases to the agent call."
  (let ((clutch-jdbc-oracle-manual-commit t)
        captured-op captured-params captured-timeout)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
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
                     :connect-timeout 11
                     :read-idle-timeout 12
                     :rpc-timeout 13))))
        (should (equal captured-op "connect"))
        (should (= captured-timeout 11))
        (should (eq (alist-get 'auto-commit captured-params) clutch-jdbc--json-false))
        (should (= (alist-get 'connect-timeout-seconds captured-params) 11))
        (should (= (alist-get 'network-timeout-seconds captured-params) 12))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 13))
        (should (= (clutch-jdbc-conn-conn-id conn) 7))))))

(ert-deftest clutch-db-test-jdbc-connect-non-oracle-sends-autocommit-true ()
  "Non-Oracle JDBC connect should keep auto-commit enabled by default."
  (let (captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
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
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
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
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
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
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional timeout-seconds)
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
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout)
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
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (_op params &optional _timeout)
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
  "clutch-db-commit should issue a commit RPC with the connection id."
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
  "clutch-db-rollback should issue a rollback RPC with the connection id."
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
  "clutch-db-set-auto-commit should issue set-auto-commit RPC with auto-commit value."
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
  "clutch-db-set-auto-commit should update :manual-commit in conn params."
  (let ((conn (make-clutch-jdbc-conn :conn-id 20
                                     :params '(:driver oracle :rpc-timeout 12 :manual-commit t))))
    (cl-letf (((symbol-function 'clutch-jdbc--rpc) (lambda (_op _params &optional _to) nil)))
      ;; Switch to auto-commit: manual-commit should become nil
      (clutch-db-set-auto-commit conn t)
      (should-not (plist-get (clutch-jdbc-conn-params conn) :manual-commit))
      ;; Switch back to manual-commit: manual-commit should become t
      (clutch-db-set-auto-commit conn nil)
      (should (plist-get (clutch-jdbc-conn-params conn) :manual-commit)))))

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

(ert-deftest clutch-db-test-jdbc-refresh-schema-async-uses-get-tables ()
  "Async JDBC schema refresh should fetch table names via get-tables."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params `(:driver oracle :user "scott"
                                               :rpc-timeout ,clutch-jdbc-rpc-timeout-seconds)))
        captured-op captured-params captured-timeout callback-result)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc-async)
               (lambda (op params callback &optional errback timeout-seconds)
                 (setq captured-op op
                       captured-params params
                       captured-timeout timeout-seconds)
                 (should-not errback)
                 (funcall callback '(:cursor-id nil
                                    :columns ("name" "type" "schema" "source_schema")
                                    :rows (("USERS" "TABLE" "SCOTT" "SCOTT")
                                           ("ORDERS" "TABLE" "SCOTT" "SCOTT"))
                                    :done t))
                 42)))
      (should (clutch-db-refresh-schema-async
               conn
               (lambda (tables)
                 (setq callback-result tables))))
      (should (equal captured-op "get-tables"))
      (should (= (alist-get 'conn-id captured-params) 9))
      (should (equal (alist-get 'schema captured-params) "SCOTT"))
      (should (= captured-timeout clutch-jdbc-rpc-timeout-seconds))
      (should (equal callback-result '("USERS" "ORDERS"))))))

(ert-deftest clutch-db-test-jdbc-list-tables-oracle-uses-get-tables ()
  "Oracle list-tables should use get-tables so browse shares schema-refresh data."
  (let ((conn (make-clutch-jdbc-conn :conn-id 9
                                     :params '(:driver oracle :user "app")))
        captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout-seconds)
                 (setq captured-op op
                       captured-params params)
                 '(:cursor-id nil
                   :columns ("name" "type" "schema" "source_schema")
                   :rows (("CUSTOMERS" "SYNONYM" "DATA_OWNER" "APP")
                          ("ORDERS" "SYNONYM" "DATA_OWNER" "APP")
                          ("PAYMENTS" "TABLE" "DATA_OWNER" "DATA_OWNER"))
                   :done t))))
      (let ((tables (clutch-db-list-tables conn)))
        (should (equal captured-op "get-tables"))
        (should (= (alist-get 'conn-id captured-params) 9))
        (should (equal (alist-get 'schema captured-params) "APP"))
        (should (equal tables '("CUSTOMERS" "ORDERS" "PAYMENTS")))))))

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
               (lambda (_op _params _callback &optional _errback timeout-seconds)
                 (setq captured-timeout timeout-seconds)
                 42)))
      (should (clutch-db-refresh-schema-async conn #'ignore))
      (should (= captured-timeout 7)))))

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
    (cl-letf (((symbol-function 'clutch-jdbc--ensure-agent) #'ignore)
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
               :params '(:driver oracle :user "zjsy" :rpc-timeout 9)))
        captured-op captured-params)
    (cl-letf (((symbol-function 'clutch-jdbc--rpc)
               (lambda (op params &optional _timeout-seconds)
                 (setq captured-op op
                       captured-params params)
                 '(:schemas ("SYS" "SYSTEM" "ZJSY" "CJH_TEST" "ZJ_TEST")))))
      (should (equal (clutch-db-list-schemas conn)
                     '("ZJSY" "CJH_TEST" "ZJ_TEST")))
      (should (equal captured-op "get-schemas"))
      (should (= (alist-get 'conn-id captured-params) 7)))))

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
        (pg-features (alist-get 'pg clutch-db--backend-features)))
    ;; MySQL backend
    (should mysql-features)
    (should (eq (plist-get mysql-features :require) 'clutch-db-mysql))
    (should (eq (plist-get mysql-features :connect-fn) 'clutch-db-mysql-connect))
    ;; PostgreSQL backend
    (should pg-features)
    (should (eq (plist-get pg-features :require) 'clutch-db-pg))
    (should (eq (plist-get pg-features :connect-fn) 'clutch-db-pg-connect))))

(ert-deftest clutch-db-test-unknown-backend ()
  "Test that connecting with unknown backend signals error."
  (should-error
   (clutch-db-connect 'unknown '(:host "localhost"))
   :type 'user-error))

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
  (require 'pg)
  ;; Numeric types
  (should (eq (clutch-db-pg--type-category pg-oid-int4) 'numeric))
  (should (eq (clutch-db-pg--type-category pg-oid-int8) 'numeric))
  (should (eq (clutch-db-pg--type-category pg-oid-float8) 'numeric))
  (should (eq (clutch-db-pg--type-category pg-oid-numeric) 'numeric))
  ;; Date/time types
  (should (eq (clutch-db-pg--type-category pg-oid-date) 'date))
  (should (eq (clutch-db-pg--type-category pg-oid-time) 'time))
  (should (eq (clutch-db-pg--type-category pg-oid-timestamp) 'datetime))
  (should (eq (clutch-db-pg--type-category pg-oid-timestamptz) 'datetime))
  ;; BLOB/JSON
  (should (eq (clutch-db-pg--type-category pg-oid-bytea) 'blob))
  (should (eq (clutch-db-pg--type-category pg-oid-json) 'json))
  (should (eq (clutch-db-pg--type-category pg-oid-jsonb) 'json))
  ;; Unknown OID defaults to text
  (should (eq (clutch-db-pg--type-category 999999) 'text)))

(ert-deftest clutch-db-test-pg-convert-columns ()
  "Test PostgreSQL column conversion."
  (require 'clutch-db-pg)
  (require 'pg)
  (let* ((pg-cols (list (list :name "id" :type-oid pg-oid-int4)
                        (list :name "data" :type-oid pg-oid-jsonb)
                        (list :name "created" :type-oid pg-oid-timestamp)))
         (converted (clutch-db-pg--convert-columns pg-cols)))
    (should (= (length converted) 3))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "created"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'datetime))))

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
  (require 'pg)
  (let ((conn (make-pg-conn :host "localhost")))
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
  (require 'pg)
  (let ((conn (make-pg-conn :host "localhost")))
    ;; Identifier escaping
    (should (equal (clutch-db-escape-identifier conn "table")
                   "\"table\""))
    (should (equal (clutch-db-escape-identifier conn "my\"table")
                   "\"my\"\"table\""))
    ;; Literal escaping
    (should (equal (clutch-db-escape-literal conn "hello")
                   "'hello'"))
    (should (equal (clutch-db-escape-literal conn "it's")
                   "'it''s'"))))

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
  (require 'pg)
  (let ((conn (make-pg-conn :host "example.com" :port 5433
                             :user "pguser" :database "pgdb")))
    (should (equal (clutch-db-host conn) "example.com"))
    (should (= (clutch-db-port conn) 5433))
    (should (equal (clutch-db-user conn) "pguser"))
    (should (equal (clutch-db-database conn) "pgdb"))
    (should (equal (clutch-db-display-name conn) "PostgreSQL"))))

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
  "Define shared live tests for PREFIX using WITH-MACRO and TAGS."
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
      (should (string-match-p "CREATE TABLE" ddl)))))

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

;;;; Unit tests — clutch--format-value and clutch--value-to-literal

(ert-deftest clutch-db-test-format-value-primitives ()
  "format-value handles nil, :false, strings, and numbers correctly."
  (should (equal (clutch--format-value nil)    "NULL"))
  (should (equal (clutch--format-value :false) "false"))
  (should (equal (clutch--format-value "hi")   "hi"))
  (should (equal (clutch--format-value 42)     "42"))
  (should (equal (clutch--format-value 3.14)   "3.14")))

(ert-deftest clutch-db-test-format-value-json-hash-table ()
  "format-value serializes a hash-table (MySQL/PG JSON object) to a JSON string."
  (let ((ht (make-hash-table :test 'equal)))
    (puthash "key" "val" ht)
    (let ((result (clutch--format-value ht)))
      (should (stringp result))
      (should (string-match-p "\"key\"" result))
      (should (string-match-p "\"val\"" result)))))

(ert-deftest clutch-db-test-format-value-json-vector ()
  "format-value serializes a vector (MySQL/PG JSON array) to a JSON string."
  (should (equal (clutch--format-value [1 2 3]) "[1,2,3]")))

(ert-deftest clutch-db-test-value-to-literal-json-hash-table ()
  "value-to-literal escapes a JSON hash-table as a quoted SQL string literal."
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
  "value-to-literal escapes a JSON vector as a quoted SQL string literal."
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

(ert-deftest clutch-db-test-jdbc-live-p-matching-live-process ()
  "conn whose process is the current agent and alive → live."
  (let ((proc 'fake-proc))
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t)))
      (let ((clutch-jdbc--agent-process proc))
        (should (clutch-db-live-p
                 (make-clutch-jdbc-conn :process proc :conn-id 1
                                        :params nil :busy nil)))))))

(ert-deftest clutch-db-test-jdbc-live-p-dead-process ()
  "conn whose process has died → not live."
  (let ((proc 'dead-proc))
    (cl-letf (((symbol-function 'process-live-p) (lambda (_p) nil)))
      (let ((clutch-jdbc--agent-process proc))
        (should-not (clutch-db-live-p
                     (make-clutch-jdbc-conn :process proc :conn-id 1
                                            :params nil :busy nil)))))))

(ert-deftest clutch-db-test-jdbc-live-p-nil-agent-process ()
  "When clutch-jdbc--agent-process is nil (agent was killed), conn is not live.
This is the key guard: after a timeout kill the JVM may still be in its
shutdown sequence, so process-live-p on the old process object can return t
briefly.  The nil check on the current-agent variable closes that window."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t)))
    (let ((clutch-jdbc--agent-process nil))
      (should-not (clutch-db-live-p
                   (make-clutch-jdbc-conn :process 'old-proc :conn-id 1
                                          :params nil :busy nil))))))

(ert-deftest clutch-db-test-jdbc-live-p-mismatched-process ()
  "conn whose stored process is NOT the current agent (e.g. after kill+restart) → not live.
This catches the race where agent-process was set to nil, a new agent was
started (new process object), but the old conn's :process field still holds
the old process which may still pass process-live-p during JVM shutdown."
  (cl-letf (((symbol-function 'process-live-p) (lambda (_p) t)))
    (let ((clutch-jdbc--agent-process 'new-proc))
      (should-not (clutch-db-live-p
                   (make-clutch-jdbc-conn :process 'old-proc :conn-id 1
                                          :params nil :busy nil))))))

;;;; Unit tests — clutch-jdbc--recv-response timeout behaviour

(ert-deftest clutch-db-test-jdbc-recv-response-returns-matching ()
  "When a matching response is already queued, recv-response returns it
immediately without touching the agent process."
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

(provide 'clutch-db-test)
;;; clutch-db-test.el ends here
