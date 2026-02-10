;;; mysql-test.el --- Tests for mysql.el -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the MySQL client.
;;
;; Tests marked with :mysql-live require a running MySQL instance:
;;   docker run -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;
;; Run all unit tests:
;;   emacs -batch -L .. -l ert -l mysql-test -f ert-run-tests-batch-and-exit
;;
;; Run live integration tests:
;;   emacs -batch -L .. -l ert -l mysql-test \
;;     --eval '(setq mysql-test-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'mysql)

;;;; Test configuration for live tests

(defvar mysql-test-host "127.0.0.1")
(defvar mysql-test-port 3306)
(defvar mysql-test-user "root")
(defvar mysql-test-password nil
  "Set this to enable live integration tests.")
(defvar mysql-test-database "mysql")

;;;; Unit tests — protocol helpers (no server needed)

(ert-deftest mysql-test-int-le-bytes ()
  "Test little-endian integer encoding."
  (should (equal (mysql--int-le-bytes 0 1) (unibyte-string 0)))
  (should (equal (mysql--int-le-bytes 255 1) (unibyte-string 255)))
  (should (equal (mysql--int-le-bytes #x0102 2) (unibyte-string #x02 #x01)))
  (should (equal (mysql--int-le-bytes #x010203 3) (unibyte-string #x03 #x02 #x01)))
  (should (equal (mysql--int-le-bytes #x01020304 4)
                 (unibyte-string #x04 #x03 #x02 #x01))))

(ert-deftest mysql-test-lenenc-int-bytes ()
  "Test length-encoded integer encoding."
  (should (equal (mysql--lenenc-int-bytes 0) (unibyte-string 0)))
  (should (equal (mysql--lenenc-int-bytes 250) (unibyte-string 250)))
  (should (equal (mysql--lenenc-int-bytes 251)
                 (concat (unibyte-string #xfc) (mysql--int-le-bytes 251 2))))
  (should (equal (mysql--lenenc-int-bytes #xffff)
                 (concat (unibyte-string #xfc) (mysql--int-le-bytes #xffff 2))))
  (should (equal (mysql--lenenc-int-bytes #x10000)
                 (concat (unibyte-string #xfd) (mysql--int-le-bytes #x10000 3)))))

(ert-deftest mysql-test-lenenc-int-from-string ()
  "Test reading length-encoded integers from a string."
  (should (equal (mysql--read-lenenc-int-from-string (unibyte-string 42) 0)
                 '(42 . 1)))
  (should (equal (mysql--read-lenenc-int-from-string
                  (unibyte-string #xfc #x01 #x00) 0)
                 '(1 . 3)))
  (should (equal (mysql--read-lenenc-int-from-string
                  (unibyte-string #xfd #x01 #x00 #x00) 0)
                 '(1 . 4))))

(ert-deftest mysql-test-read-lenenc-string-from-string ()
  "Test reading length-encoded strings."
  (should (equal (mysql--read-lenenc-string-from-string
                  (concat (unibyte-string 5) "hello") 0)
                 '("hello" . 6)))
  (should (equal (mysql--read-lenenc-string-from-string
                  (concat (unibyte-string 0)) 0)
                 '("" . 1))))

(ert-deftest mysql-test-auth-mysql-native-password ()
  "Test mysql_native_password computation."
  ;; Empty password should return empty string
  (should (equal (mysql--auth-mysql-native-password "" "12345678901234567890") ""))
  (should (equal (mysql--auth-mysql-native-password nil "12345678901234567890") ""))
  ;; Non-empty password returns 20 bytes
  (let ((result (mysql--auth-mysql-native-password "secret" "12345678901234567890")))
    (should (= (length result) 20))))

(ert-deftest mysql-test-auth-caching-sha2-password ()
  "Test caching_sha2_password computation."
  (should (equal (mysql--auth-caching-sha2-password "" "12345678901234567890") ""))
  (let ((result (mysql--auth-caching-sha2-password "secret" "12345678901234567890")))
    (should (= (length result) 32))))

(ert-deftest mysql-test-xor-strings ()
  "Test XOR of two strings."
  (should (equal (mysql--xor-strings (unibyte-string #xff #x00 #xaa)
                                     (unibyte-string #xff #xff #x55))
                 (unibyte-string #x00 #xff #xff))))

(ert-deftest mysql-test-parse-ok-packet ()
  "Test OK packet parsing."
  (let* ((packet (concat (unibyte-string #x00  ; OK marker
                                         #x01  ; affected_rows = 1
                                         #x00  ; last_insert_id = 0
                                         #x02 #x00  ; status_flags
                                         #x00 #x00)))  ; warnings
         (info (mysql--parse-ok-packet packet)))
    (should (= (plist-get info :affected-rows) 1))
    (should (= (plist-get info :last-insert-id) 0))
    (should (= (plist-get info :warnings) 0))))

(ert-deftest mysql-test-parse-err-packet ()
  "Test ERR packet parsing."
  (let* ((packet (concat (unibyte-string #xff          ; ERR marker
                                         #x15 #x04)   ; error code 1045
                         "#"                           ; SQL state marker
                         "28000"                       ; SQL state
                         "Access denied"))
         (info (mysql--parse-err-packet packet)))
    (should (= (plist-get info :code) 1045))
    (should (equal (plist-get info :state) "28000"))
    (should (equal (plist-get info :message) "Access denied"))))

(ert-deftest mysql-test-packet-type ()
  "Test packet type detection."
  (should (eq (mysql--packet-type (unibyte-string #x00)) 'ok))
  (should (eq (mysql--packet-type (unibyte-string #xff)) 'err))
  (should (eq (mysql--packet-type (unibyte-string #xfe)) 'eof))
  (should (eq (mysql--packet-type (unibyte-string #xfb)) 'local-infile))
  (should (eq (mysql--packet-type (unibyte-string #x03 #x01 #x02)) 'data)))

(ert-deftest mysql-test-parse-value ()
  "Test MySQL type conversion."
  (should (= (mysql--parse-value "42" mysql--type-long) 42))
  (should (= (mysql--parse-value "3.14" mysql--type-float) 3.14))
  (should (= (mysql--parse-value "2024" mysql--type-year) 2024))
  (should (equal (mysql--parse-value "hello" mysql--type-var-string) "hello"))
  (should (null (mysql--parse-value nil mysql--type-long))))

(ert-deftest mysql-test-parse-result-row ()
  "Test result row parsing."
  ;; Row with two string columns: "hello" and "world"
  (let* ((packet (concat (unibyte-string 5) "hello"
                         (unibyte-string 5) "world"))
         (row (mysql--parse-result-row packet 2)))
    (should (equal row '("hello" "world"))))
  ;; Row with NULL value
  (let* ((packet (concat (unibyte-string #xfb)
                         (unibyte-string 3) "foo"))
         (row (mysql--parse-result-row packet 2)))
    (should (equal row '(nil "foo")))))

(ert-deftest mysql-test-struct-creation ()
  "Test that structs can be created."
  (let ((conn (make-mysql-conn :host "localhost" :port 3306
                               :user "root" :database "test")))
    (should (equal (mysql-conn-host conn) "localhost"))
    (should (= (mysql-conn-port conn) 3306))
    (should (= (mysql-conn-read-timeout conn) 10))
    (should (= (mysql-conn-sequence-id conn) 0)))
  (let ((result (make-mysql-result :status "OK" :affected-rows 5)))
    (should (equal (mysql-result-status result) "OK"))
    (should (= (mysql-result-affected-rows result) 5))))

;;;; Live integration tests (require a running MySQL server)

(defmacro mysql-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live MySQL connection.
Skips if `mysql-test-password' is nil."
  (declare (indent 1))
  `(if (null mysql-test-password)
       (ert-skip "Set mysql-test-password to enable live tests")
     (let ((,var (mysql-connect :host mysql-test-host
                                :port mysql-test-port
                                :user mysql-test-user
                                :password mysql-test-password
                                :database mysql-test-database)))
       (unwind-protect
           (progn ,@body)
         (mysql-disconnect ,var)))))

(ert-deftest mysql-test-live-connect-disconnect ()
  :tags '(:mysql-live)
  "Test connecting and disconnecting."
  (mysql-test--with-conn conn
    (should (mysql-conn-p conn))
    (should (mysql-conn-server-version conn))
    (should (> (mysql-conn-connection-id conn) 0))))

(ert-deftest mysql-test-live-select ()
  :tags '(:mysql-live)
  "Test a simple SELECT query."
  (mysql-test--with-conn conn
    (let ((result (mysql-query conn "SELECT 1 AS num, 'hello' AS greeting")))
      (should (mysql-result-p result))
      (should (equal (mysql-result-status result) "OK"))
      (should (= (length (mysql-result-columns result)) 2))
      (should (= (length (mysql-result-rows result)) 1))
      (let ((row (car (mysql-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest mysql-test-live-multi-row ()
  :tags '(:mysql-live)
  "Test query returning multiple rows."
  (mysql-test--with-conn conn
    (let ((result (mysql-query conn "SELECT user, host FROM user LIMIT 5")))
      (should (mysql-result-p result))
      (should (>= (length (mysql-result-rows result)) 1)))))

(ert-deftest mysql-test-live-dml ()
  :tags '(:mysql-live)
  "Test INSERT/UPDATE/DELETE (DML) returning affected-rows."
  (mysql-test--with-conn conn
    ;; Create a temp table
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_test (id INT, val VARCHAR(50))")
    (let ((result (mysql-query conn "INSERT INTO _mysql_el_test VALUES (1, 'one'), (2, 'two')")))
      (should (= (mysql-result-affected-rows result) 2)))
    (let ((result (mysql-query conn "UPDATE _mysql_el_test SET val = 'updated' WHERE id = 1")))
      (should (= (mysql-result-affected-rows result) 1)))
    (let ((result (mysql-query conn "SELECT * FROM _mysql_el_test ORDER BY id")))
      (should (= (length (mysql-result-rows result)) 2))
      (should (equal (cadr (car (mysql-result-rows result))) "updated")))
    (let ((result (mysql-query conn "DELETE FROM _mysql_el_test")))
      (should (= (mysql-result-affected-rows result) 2)))))

(ert-deftest mysql-test-live-query-error ()
  :tags '(:mysql-live)
  "Test that a syntax error signals mysql-query-error."
  (mysql-test--with-conn conn
    (should-error (mysql-query conn "SELEC BAD SYNTAX")
                  :type 'mysql-query-error)))

(ert-deftest mysql-test-live-auth-failure ()
  :tags '(:mysql-live)
  "Test that wrong password signals mysql-auth-error."
  (if (null mysql-test-password)
      (ert-skip "Set mysql-test-password to enable live tests")
    (should-error (mysql-connect :host mysql-test-host
                                 :port mysql-test-port
                                 :user mysql-test-user
                                 :password "definitely-wrong-password"
                                 :database mysql-test-database)
                  :type 'mysql-auth-error)))

(ert-deftest mysql-test-live-null-values ()
  :tags '(:mysql-live)
  "Test that NULL values are returned as nil."
  (mysql-test--with-conn conn
    (let ((result (mysql-query conn "SELECT NULL AS n, 42 AS v")))
      (let ((row (car (mysql-result-rows result))))
        (should (null (car row)))
        (should (= (cadr row) 42))))))

(ert-deftest mysql-test-live-empty-result ()
  :tags '(:mysql-live)
  "Test a query that returns zero rows."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_empty (id INT)")
    (let ((result (mysql-query conn "SELECT * FROM _mysql_el_empty")))
      (should (= (length (mysql-result-rows result)) 0))
      (should (= (length (mysql-result-columns result)) 1)))))

(provide 'mysql-test)
;;; mysql-test.el ends here
