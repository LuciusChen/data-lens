;;; clutch-db-test.el --- Tests for clutch-db -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the clutch-db generic database interface.
;;
;; Unit tests run without a database server.
;; Live tests require both MySQL and PostgreSQL:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;   docker run -d -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
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

(ert-deftest clutch-db-test-jdbc-connect-maps-timeouts ()
  "JDBC connect should map explicit timeout phases to the agent call."
  (let (captured-op captured-params captured-timeout)
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
        (should (= (alist-get 'connect-timeout-seconds captured-params) 11))
        (should (= (alist-get 'network-timeout-seconds captured-params) 12))
        (should (= (plist-get (clutch-jdbc-conn-params conn) :rpc-timeout) 13))
        (should (= (clutch-jdbc-conn-conn-id conn) 7))))))

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
  "JDBC query should send query timeout separately from RPC timeout."
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
        (should (= (alist-get 'query-timeout-seconds captured-params) 16))
        (should (= (clutch-db-result-affected-rows result) 1))))))

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
     (let ((,var (clutch-db-connect
                  'mysql
                  (list :host clutch-db-test-mysql-host
                        :port clutch-db-test-mysql-port
                        :user clutch-db-test-mysql-user
                        :password clutch-db-test-mysql-password
                        :database clutch-db-test-mysql-database))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(ert-deftest clutch-db-test-mysql-live-connect ()
  :tags '(:db-live :mysql-live)
  "Test MySQL connection via clutch-db-connect."
  (clutch-db-test--with-mysql conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "MySQL"))))

(ert-deftest clutch-db-test-mysql-live-query ()
  :tags '(:db-live :mysql-live)
  "Test MySQL query via clutch-db-query."
  (clutch-db-test--with-mysql conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n, 'hello' AS s")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-columns result)) 2))
      (should (= (length (clutch-db-result-rows result)) 1))
      (let ((row (car (clutch-db-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest clutch-db-test-mysql-live-dml ()
  :tags '(:db-live :mysql-live)
  "Test MySQL DML operations."
  (clutch-db-test--with-mysql conn
    (clutch-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
    (let ((result (clutch-db-query conn
                   "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
      (should (= (clutch-db-result-affected-rows result) 2)))
    (let ((result (clutch-db-query conn "SELECT * FROM _db_test")))
      (should (= (length (clutch-db-result-rows result)) 2)))))

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

(ert-deftest clutch-db-test-mysql-live-error ()
  :tags '(:db-live :mysql-live)
  "Test MySQL error handling."
  (clutch-db-test--with-mysql conn
    (should-error (clutch-db-query conn "SELEC BAD")
                  :type 'clutch-db-error)))

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

(ert-deftest clutch-db-test-pg-live-connect ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL connection via clutch-db-connect."
  (clutch-db-test--with-pg conn
    (should (clutch-db-live-p conn))
    (should (equal (clutch-db-display-name conn) "PostgreSQL"))))

(ert-deftest clutch-db-test-pg-live-query ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL query via clutch-db-query."
  (clutch-db-test--with-pg conn
    (let ((result (clutch-db-query conn "SELECT 1 AS n, 'hello' AS s")))
      (should (clutch-db-result-p result))
      (should (= (length (clutch-db-result-columns result)) 2))
      (should (= (length (clutch-db-result-rows result)) 1))
      (let ((row (car (clutch-db-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest clutch-db-test-pg-live-dml ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL DML operations."
  (clutch-db-test--with-pg conn
    (clutch-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
    (let ((result (clutch-db-query conn
                   "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
      (should (= (clutch-db-result-affected-rows result) 2)))
    (let ((result (clutch-db-query conn "SELECT * FROM _db_test")))
      (should (= (length (clutch-db-result-rows result)) 2)))))

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

(ert-deftest clutch-db-test-pg-live-error ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL error handling."
  (clutch-db-test--with-pg conn
    (should-error (clutch-db-query conn "SELEC BAD")
                  :type 'clutch-db-error)))

;;;; Cross-backend consistency tests

(ert-deftest clutch-db-test-cross-type-categories ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends use consistent type categories."
  (when (and (null clutch-db-test-mysql-password)
             (null clutch-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
  ;; Test numeric
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
      (when pg-conn (clutch-db-disconnect pg-conn)))))

(ert-deftest clutch-db-test-cross-null-handling ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends handle NULL values consistently."
  (when (and (null clutch-db-test-mysql-password)
             (null clutch-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
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
          (clutch-db-disconnect conn))))))

(provide 'clutch-db-test)
;;; clutch-db-test.el ends here
