;;; data-lens-db-test.el --- Tests for data-lens-db -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the data-lens-db generic database interface.
;;
;; Unit tests run without a database server.
;; Live tests require both MySQL and PostgreSQL:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;   docker run -d -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
;;
;; Run unit tests:
;;   emacs -batch -L .. -l ert -l data-lens-db-test \
;;     -f ert-run-tests-batch-and-exit
;;
;; Run live tests:
;;   emacs -batch -L .. -l ert -l data-lens-db-test \
;;     --eval '(setq data-lens-db-test-mysql-password "test")' \
;;     --eval '(setq data-lens-db-test-pg-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'data-lens-db)

;;;; Test configuration

(defvar data-lens-db-test-mysql-host "127.0.0.1")
(defvar data-lens-db-test-mysql-port 3306)
(defvar data-lens-db-test-mysql-user "root")
(defvar data-lens-db-test-mysql-password nil)
(defvar data-lens-db-test-mysql-database "mysql")

(defvar data-lens-db-test-pg-host "127.0.0.1")
(defvar data-lens-db-test-pg-port 5432)
(defvar data-lens-db-test-pg-user "postgres")
(defvar data-lens-db-test-pg-password nil)
(defvar data-lens-db-test-pg-database "postgres")

;;;; Unit tests — data-lens-db-result struct

(ert-deftest data-lens-db-test-result-struct ()
  "Test data-lens-db-result struct creation and accessors."
  (let ((result (make-data-lens-db-result
                 :connection 'fake-conn
                 :columns '((:name "id" :type-category numeric)
                            (:name "name" :type-category text))
                 :rows '((1 "alice") (2 "bob"))
                 :affected-rows 2
                 :last-insert-id 42
                 :warnings 0)))
    (should (data-lens-db-result-p result))
    (should (eq (data-lens-db-result-connection result) 'fake-conn))
    (should (= (length (data-lens-db-result-columns result)) 2))
    (should (= (length (data-lens-db-result-rows result)) 2))
    (should (= (data-lens-db-result-affected-rows result) 2))
    (should (= (data-lens-db-result-last-insert-id result) 42))
    (should (= (data-lens-db-result-warnings result) 0))))

(ert-deftest data-lens-db-test-result-empty ()
  "Test data-lens-db-result with empty/nil values."
  (let ((result (make-data-lens-db-result
                 :columns '((:name "v" :type-category numeric))
                 :rows nil
                 :affected-rows 0)))
    (should (data-lens-db-result-p result))
    (should (null (data-lens-db-result-connection result)))
    (should (null (data-lens-db-result-rows result)))
    (should (= (data-lens-db-result-affected-rows result) 0))
    (should (null (data-lens-db-result-last-insert-id result)))))

;;;; Unit tests — backend registry

(ert-deftest data-lens-db-test-backend-features ()
  "Test that backend features are correctly registered."
  (let ((mysql-features (alist-get 'mysql data-lens-db--backend-features))
        (pg-features (alist-get 'pg data-lens-db--backend-features)))
    ;; MySQL backend
    (should mysql-features)
    (should (eq (plist-get mysql-features :require) 'data-lens-db-mysql))
    (should (eq (plist-get mysql-features :connect-fn) 'data-lens-db-mysql-connect))
    ;; PostgreSQL backend
    (should pg-features)
    (should (eq (plist-get pg-features :require) 'data-lens-db-pg))
    (should (eq (plist-get pg-features :connect-fn) 'data-lens-db-pg-connect))))

(ert-deftest data-lens-db-test-unknown-backend ()
  "Test that connecting with unknown backend signals error."
  (should-error
   (data-lens-db-connect 'unknown '(:host "localhost"))
   :type 'data-lens-db-error))

;;;; Unit tests — MySQL type category mapping

(ert-deftest data-lens-db-test-mysql-type-categories ()
  "Test MySQL type to category mapping."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  ;; Numeric types
  (should (eq (data-lens-db-mysql-type-category mysql-type-long) 'numeric))
  (should (eq (data-lens-db-mysql-type-category mysql-type-float) 'numeric))
  (should (eq (data-lens-db-mysql-type-category mysql-type-double) 'numeric))
  (should (eq (data-lens-db-mysql-type-category mysql-type-decimal) 'numeric))
  (should (eq (data-lens-db-mysql-type-category mysql-type-longlong) 'numeric))
  ;; Date/time types
  (should (eq (data-lens-db-mysql-type-category mysql-type-date) 'date))
  (should (eq (data-lens-db-mysql-type-category mysql-type-time) 'time))
  (should (eq (data-lens-db-mysql-type-category mysql-type-datetime) 'datetime))
  (should (eq (data-lens-db-mysql-type-category mysql-type-timestamp) 'datetime))
  ;; BLOB/JSON
  (should (eq (data-lens-db-mysql-type-category mysql-type-blob) 'blob))
  (should (eq (data-lens-db-mysql-type-category mysql-type-json) 'json))
  ;; Unknown type defaults to text
  (should (eq (data-lens-db-mysql-type-category 9999) 'text)))

(ert-deftest data-lens-db-test-mysql-convert-columns ()
  "Test MySQL column conversion."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  (let* ((mysql-cols (list (list :name "id" :type mysql-type-long)
                           (list :name "data" :type mysql-type-json)
                           (list :name "created" :type mysql-type-datetime)))
         (converted (data-lens-db-mysql--convert-columns mysql-cols)))
    (should (= (length converted) 3))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "created"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'datetime))))

;;;; Unit tests — PostgreSQL type category mapping

(ert-deftest data-lens-db-test-pg-type-categories ()
  "Test PostgreSQL OID to category mapping."
  (require 'data-lens-db-pg)
  (require 'pg)
  ;; Numeric types
  (should (eq (data-lens-db-pg--type-category pg-oid-int4) 'numeric))
  (should (eq (data-lens-db-pg--type-category pg-oid-int8) 'numeric))
  (should (eq (data-lens-db-pg--type-category pg-oid-float8) 'numeric))
  (should (eq (data-lens-db-pg--type-category pg-oid-numeric) 'numeric))
  ;; Date/time types
  (should (eq (data-lens-db-pg--type-category pg-oid-date) 'date))
  (should (eq (data-lens-db-pg--type-category pg-oid-time) 'time))
  (should (eq (data-lens-db-pg--type-category pg-oid-timestamp) 'datetime))
  (should (eq (data-lens-db-pg--type-category pg-oid-timestamptz) 'datetime))
  ;; BLOB/JSON
  (should (eq (data-lens-db-pg--type-category pg-oid-bytea) 'blob))
  (should (eq (data-lens-db-pg--type-category pg-oid-json) 'json))
  (should (eq (data-lens-db-pg--type-category pg-oid-jsonb) 'json))
  ;; Unknown OID defaults to text
  (should (eq (data-lens-db-pg--type-category 999999) 'text)))

(ert-deftest data-lens-db-test-pg-convert-columns ()
  "Test PostgreSQL column conversion."
  (require 'data-lens-db-pg)
  (require 'pg)
  (let* ((pg-cols (list (list :name "id" :type-oid pg-oid-int4)
                        (list :name "data" :type-oid pg-oid-jsonb)
                        (list :name "created" :type-oid pg-oid-timestamp)))
         (converted (data-lens-db-pg--convert-columns pg-cols)))
    (should (= (length converted) 3))
    (should (equal (plist-get (nth 0 converted) :name) "id"))
    (should (eq (plist-get (nth 0 converted) :type-category) 'numeric))
    (should (equal (plist-get (nth 1 converted) :name) "data"))
    (should (eq (plist-get (nth 1 converted) :type-category) 'json))
    (should (equal (plist-get (nth 2 converted) :name) "created"))
    (should (eq (plist-get (nth 2 converted) :type-category) 'datetime))))

;;;; Unit tests — SQL building (paged queries)

(ert-deftest data-lens-db-test-mysql-build-paged-sql ()
  "Test MySQL paged SQL generation."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Basic pagination
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 2
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 1 10)))
      (should (string-match-p "OFFSET 10" sql)))
    ;; With order
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("name" . "ASC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "ASC" sql)))
    ;; Already has LIMIT — no modification
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t LIMIT 5" 0 10)))
      (should (equal sql "SELECT * FROM t LIMIT 5")))))

(ert-deftest data-lens-db-test-pg-build-paged-sql ()
  "Test PostgreSQL paged SQL generation."
  (require 'data-lens-db-pg)
  (require 'pg)
  (let ((conn (make-pg-conn :host "localhost")))
    ;; Basic pagination
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should (string-match-p "OFFSET 0" sql)))
    ;; Page 3, page-size 25
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 2 25)))
      (should (string-match-p "LIMIT 25" sql))
      (should (string-match-p "OFFSET 50" sql)))
    ;; With descending order
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t" 0 10
                                              '("id" . "DESC"))))
      (should (string-match-p "ORDER BY" sql))
      (should (string-match-p "DESC" sql)))
    ;; Query with trailing semicolon
    (let ((sql (data-lens-db-build-paged-sql conn "SELECT * FROM t;" 0 10)))
      (should (string-match-p "LIMIT 10" sql))
      (should-not (string-match-p ";\\s*LIMIT" sql)))))

;;;; Unit tests — SQL escaping

(ert-deftest data-lens-db-test-mysql-escape ()
  "Test MySQL identifier and literal escaping via generic interface."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost")))
    ;; Identifier escaping
    (should (equal (data-lens-db-escape-identifier conn "table")
                   "`table`"))
    (should (equal (data-lens-db-escape-identifier conn "my`table")
                   "`my``table`"))
    ;; Literal escaping
    (should (equal (data-lens-db-escape-literal conn "hello")
                   "'hello'"))
    (should (equal (data-lens-db-escape-literal conn "it's")
                   "'it\\'s'"))))

(ert-deftest data-lens-db-test-pg-escape ()
  "Test PostgreSQL identifier and literal escaping via generic interface."
  (require 'data-lens-db-pg)
  (require 'pg)
  (let ((conn (make-pg-conn :host "localhost")))
    ;; Identifier escaping
    (should (equal (data-lens-db-escape-identifier conn "table")
                   "\"table\""))
    (should (equal (data-lens-db-escape-identifier conn "my\"table")
                   "\"my\"\"table\""))
    ;; Literal escaping
    (should (equal (data-lens-db-escape-literal conn "hello")
                   "'hello'"))
    (should (equal (data-lens-db-escape-literal conn "it's")
                   "'it''s'"))))

;;;; Unit tests — metadata accessors

(ert-deftest data-lens-db-test-mysql-metadata ()
  "Test MySQL metadata accessors."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "example.com" :port 3307
                                :user "testuser" :database "testdb")))
    (should (equal (data-lens-db-host conn) "example.com"))
    (should (= (data-lens-db-port conn) 3307))
    (should (equal (data-lens-db-user conn) "testuser"))
    (should (equal (data-lens-db-database conn) "testdb"))
    (should (equal (data-lens-db-display-name conn) "MySQL"))))

(ert-deftest data-lens-db-test-pg-metadata ()
  "Test PostgreSQL metadata accessors."
  (require 'data-lens-db-pg)
  (require 'pg)
  (let ((conn (make-pg-conn :host "example.com" :port 5433
                             :user "pguser" :database "pgdb")))
    (should (equal (data-lens-db-host conn) "example.com"))
    (should (= (data-lens-db-port conn) 5433))
    (should (equal (data-lens-db-user conn) "pguser"))
    (should (equal (data-lens-db-database conn) "pgdb"))
    (should (equal (data-lens-db-display-name conn) "PostgreSQL"))))

;;;; Live integration tests — MySQL

(defmacro data-lens-db-test--with-mysql (var &rest body)
  "Execute BODY with VAR bound to a MySQL connection.
Skips if `data-lens-db-test-mysql-password' is nil."
  (declare (indent 1))
  `(if (null data-lens-db-test-mysql-password)
       (ert-skip "Set data-lens-db-test-mysql-password to enable MySQL live tests")
     (let ((,var (data-lens-db-connect
                  'mysql
                  (list :host data-lens-db-test-mysql-host
                        :port data-lens-db-test-mysql-port
                        :user data-lens-db-test-mysql-user
                        :password data-lens-db-test-mysql-password
                        :database data-lens-db-test-mysql-database))))
       (unwind-protect
           (progn ,@body)
         (data-lens-db-disconnect ,var)))))

(ert-deftest data-lens-db-test-mysql-live-connect ()
  :tags '(:db-live :mysql-live)
  "Test MySQL connection via data-lens-db-connect."
  (data-lens-db-test--with-mysql conn
    (should (data-lens-db-live-p conn))
    (should (equal (data-lens-db-display-name conn) "MySQL"))))

(ert-deftest data-lens-db-test-mysql-live-query ()
  :tags '(:db-live :mysql-live)
  "Test MySQL query via data-lens-db-query."
  (data-lens-db-test--with-mysql conn
    (let ((result (data-lens-db-query conn "SELECT 1 AS n, 'hello' AS s")))
      (should (data-lens-db-result-p result))
      (should (= (length (data-lens-db-result-columns result)) 2))
      (should (= (length (data-lens-db-result-rows result)) 1))
      (let ((row (car (data-lens-db-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest data-lens-db-test-mysql-live-dml ()
  :tags '(:db-live :mysql-live)
  "Test MySQL DML operations."
  (data-lens-db-test--with-mysql conn
    (data-lens-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
    (let ((result (data-lens-db-query conn
                   "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
      (should (= (data-lens-db-result-affected-rows result) 2)))
    (let ((result (data-lens-db-query conn "SELECT * FROM _db_test")))
      (should (= (length (data-lens-db-result-rows result)) 2)))))

(ert-deftest data-lens-db-test-mysql-live-schema ()
  :tags '(:db-live :mysql-live)
  "Test MySQL schema introspection."
  (data-lens-db-test--with-mysql conn
    ;; list-tables
    (let ((tables (data-lens-db-list-tables conn)))
      (should (listp tables))
      (should (> (length tables) 0)))
    ;; list-columns
    (let ((columns (data-lens-db-list-columns conn "user")))
      (should (listp columns))
      (should (member "Host" columns)))
    ;; show-create-table
    (let ((ddl (data-lens-db-show-create-table conn "user")))
      (should (stringp ddl))
      (should (string-match-p "CREATE TABLE" ddl)))))

(ert-deftest data-lens-db-test-mysql-live-error ()
  :tags '(:db-live :mysql-live)
  "Test MySQL error handling."
  (data-lens-db-test--with-mysql conn
    (should-error (data-lens-db-query conn "SELEC BAD")
                  :type 'data-lens-db-error)))

;;;; Live integration tests — PostgreSQL

(defmacro data-lens-db-test--with-pg (var &rest body)
  "Execute BODY with VAR bound to a PostgreSQL connection.
Skips if `data-lens-db-test-pg-password' is nil."
  (declare (indent 1))
  `(if (null data-lens-db-test-pg-password)
       (ert-skip "Set data-lens-db-test-pg-password to enable PostgreSQL live tests")
     (let ((,var (data-lens-db-connect
                  'pg
                  (list :host data-lens-db-test-pg-host
                        :port data-lens-db-test-pg-port
                        :user data-lens-db-test-pg-user
                        :password data-lens-db-test-pg-password
                        :database data-lens-db-test-pg-database))))
       (unwind-protect
           (progn ,@body)
         (data-lens-db-disconnect ,var)))))

(ert-deftest data-lens-db-test-pg-live-connect ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL connection via data-lens-db-connect."
  (data-lens-db-test--with-pg conn
    (should (data-lens-db-live-p conn))
    (should (equal (data-lens-db-display-name conn) "PostgreSQL"))))

(ert-deftest data-lens-db-test-pg-live-query ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL query via data-lens-db-query."
  (data-lens-db-test--with-pg conn
    (let ((result (data-lens-db-query conn "SELECT 1 AS n, 'hello' AS s")))
      (should (data-lens-db-result-p result))
      (should (= (length (data-lens-db-result-columns result)) 2))
      (should (= (length (data-lens-db-result-rows result)) 1))
      (let ((row (car (data-lens-db-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest data-lens-db-test-pg-live-dml ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL DML operations."
  (data-lens-db-test--with-pg conn
    (data-lens-db-query conn "CREATE TEMPORARY TABLE _db_test (id INT, val TEXT)")
    (let ((result (data-lens-db-query conn
                   "INSERT INTO _db_test VALUES (1, 'a'), (2, 'b')")))
      (should (= (data-lens-db-result-affected-rows result) 2)))
    (let ((result (data-lens-db-query conn "SELECT * FROM _db_test")))
      (should (= (length (data-lens-db-result-rows result)) 2)))))

(ert-deftest data-lens-db-test-pg-live-schema ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL schema introspection."
  (data-lens-db-test--with-pg conn
    ;; Create a test table for schema tests
    (data-lens-db-query conn
     "CREATE TEMPORARY TABLE _schema_test (id SERIAL PRIMARY KEY, name TEXT)")
    ;; list-tables (temporary tables not in pg_tables, so just check it runs)
    (let ((tables (data-lens-db-list-tables conn)))
      (should (listp tables)))
    ;; Create a real table for column/DDL tests
    (data-lens-db-query conn
     "CREATE TABLE IF NOT EXISTS _schema_real (id SERIAL PRIMARY KEY, name TEXT)")
    ;; list-columns
    (let ((columns (data-lens-db-list-columns conn "_schema_real")))
      (should (listp columns))
      (should (member "id" columns))
      (should (member "name" columns)))
    ;; show-create-table (synthesized DDL)
    (let ((ddl (data-lens-db-show-create-table conn "_schema_real")))
      (should (stringp ddl))
      (should (string-match-p "CREATE TABLE" ddl)))
    ;; Cleanup
    (data-lens-db-query conn "DROP TABLE IF EXISTS _schema_real")))

(ert-deftest data-lens-db-test-pg-live-error ()
  :tags '(:db-live :pg-live)
  "Test PostgreSQL error handling."
  (data-lens-db-test--with-pg conn
    (should-error (data-lens-db-query conn "SELEC BAD")
                  :type 'data-lens-db-error)))

;;;; Cross-backend consistency tests

(ert-deftest data-lens-db-test-cross-type-categories ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends use consistent type categories."
  (when (and (null data-lens-db-test-mysql-password)
             (null data-lens-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
  ;; Test numeric
  (let ((mysql-conn (when data-lens-db-test-mysql-password
                      (data-lens-db-connect
                       'mysql
                       (list :host data-lens-db-test-mysql-host
                             :port data-lens-db-test-mysql-port
                             :user data-lens-db-test-mysql-user
                             :password data-lens-db-test-mysql-password
                             :database data-lens-db-test-mysql-database))))
        (pg-conn (when data-lens-db-test-pg-password
                   (data-lens-db-connect
                    'pg
                    (list :host data-lens-db-test-pg-host
                          :port data-lens-db-test-pg-port
                          :user data-lens-db-test-pg-user
                          :password data-lens-db-test-pg-password
                          :database data-lens-db-test-pg-database)))))
    (unwind-protect
        (progn
          ;; Both should return numeric type-category for integers
          (when mysql-conn
            (let* ((result (data-lens-db-query mysql-conn "SELECT 42 AS n"))
                   (cols (data-lens-db-result-columns result)))
              (should (eq (plist-get (car cols) :type-category) 'numeric))))
          (when pg-conn
            (let* ((result (data-lens-db-query pg-conn "SELECT 42 AS n"))
                   (cols (data-lens-db-result-columns result)))
              (should (eq (plist-get (car cols) :type-category) 'numeric)))))
      (when mysql-conn (data-lens-db-disconnect mysql-conn))
      (when pg-conn (data-lens-db-disconnect pg-conn)))))

(ert-deftest data-lens-db-test-cross-null-handling ()
  :tags '(:db-live :mysql-live :pg-live)
  "Test that both backends handle NULL values consistently."
  (when (and (null data-lens-db-test-mysql-password)
             (null data-lens-db-test-pg-password))
    (ert-skip "Need both MySQL and PostgreSQL for cross-backend tests"))
  (dolist (backend-spec (list (cons 'mysql
                                    (list :host data-lens-db-test-mysql-host
                                          :port data-lens-db-test-mysql-port
                                          :user data-lens-db-test-mysql-user
                                          :password data-lens-db-test-mysql-password
                                          :database data-lens-db-test-mysql-database))
                              (cons 'pg
                                    (list :host data-lens-db-test-pg-host
                                          :port data-lens-db-test-pg-port
                                          :user data-lens-db-test-pg-user
                                          :password data-lens-db-test-pg-password
                                          :database data-lens-db-test-pg-database))))
    (when (plist-get (cdr backend-spec) :password)
      (let ((conn (data-lens-db-connect (car backend-spec) (cdr backend-spec))))
        (unwind-protect
            (let* ((result (data-lens-db-query conn "SELECT NULL AS n"))
                   (row (car (data-lens-db-result-rows result))))
              (should (null (car row))))
          (data-lens-db-disconnect conn))))))

(provide 'data-lens-db-test)
;;; data-lens-db-test.el ends here
