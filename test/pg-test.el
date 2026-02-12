;;; pg-test.el --- Tests for pg.el -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the PostgreSQL client.
;;
;; Tests marked with :pg-live require a running PostgreSQL instance:
;;   docker run -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16
;;
;; Run all unit tests:
;;   emacs -batch -L .. -l ert -l pg-test -f ert-run-tests-batch-and-exit
;;
;; Run live integration tests:
;;   emacs -batch -L .. -l ert -l pg-test \
;;     --eval '(setq pg-test-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'pg)

;;;; Test configuration for live tests

(defvar pg-test-host "127.0.0.1")
(defvar pg-test-port 5432)
(defvar pg-test-user "postgres")
(defvar pg-test-password nil
  "Set this to enable live integration tests.")
(defvar pg-test-database "postgres")

;;;; Unit tests — wire encoding helpers (no server needed)

(ert-deftest pg-test-int32-be-bytes ()
  "Test big-endian 32-bit integer encoding."
  (should (equal (pg--int32-be-bytes 0)
                 (unibyte-string 0 0 0 0)))
  (should (equal (pg--int32-be-bytes 1)
                 (unibyte-string 0 0 0 1)))
  (should (equal (pg--int32-be-bytes #x01020304)
                 (unibyte-string #x01 #x02 #x03 #x04)))
  (should (equal (pg--int32-be-bytes #xffffffff)
                 (unibyte-string #xff #xff #xff #xff))))

(ert-deftest pg-test-int16-be-bytes ()
  "Test big-endian 16-bit integer encoding."
  (should (equal (pg--int16-be-bytes 0)
                 (unibyte-string 0 0)))
  (should (equal (pg--int16-be-bytes #x0102)
                 (unibyte-string #x01 #x02))))

(ert-deftest pg-test-encode-string ()
  "Test NUL-terminated string encoding."
  (should (equal (pg--encode-string "hello")
                 (concat "hello" (unibyte-string 0))))
  (should (equal (pg--encode-string "")
                 (unibyte-string 0))))

(ert-deftest pg-test-read-be-int32-from-string ()
  "Test reading big-endian 32-bit integers from a string."
  (should (= (pg--read-be-int32-from-string
              (unibyte-string 0 0 0 42) 0)
             42))
  (should (= (pg--read-be-int32-from-string
              (unibyte-string #x01 #x02 #x03 #x04) 0)
             #x01020304)))

(ert-deftest pg-test-read-be-int16-from-string ()
  "Test reading big-endian 16-bit integers from a string."
  (should (= (pg--read-be-int16-from-string
              (unibyte-string 0 42) 0)
             42))
  (should (= (pg--read-be-int16-from-string
              (unibyte-string #x01 #x02) 0)
             #x0102)))

(ert-deftest pg-test-read-nul-string-from-string ()
  "Test reading NUL-terminated strings from a byte string."
  (should (equal (pg--read-nul-string-from-string
                  (concat "hello" (unibyte-string 0) "world") 0)
                 '("hello" . 6)))
  (should (equal (pg--read-nul-string-from-string
                  (concat (unibyte-string 0) "rest") 0)
                 '("" . 1))))

;;;; Unit tests — error parsing

(ert-deftest pg-test-parse-error-fields ()
  "Test ErrorResponse field parsing."
  (let* ((payload (concat (unibyte-string ?S) "ERROR" (unibyte-string 0)
                          (unibyte-string ?C) "42601" (unibyte-string 0)
                          (unibyte-string ?M) "syntax error" (unibyte-string 0)
                          (unibyte-string 0)))  ; terminator
         (fields (pg--parse-error-fields payload)))
    (should (equal (alist-get ?S fields) "ERROR"))
    (should (equal (alist-get ?C fields) "42601"))
    (should (equal (alist-get ?M fields) "syntax error"))))

(ert-deftest pg-test-error-fields-message ()
  "Test error message formatting."
  (let ((fields (list (cons ?S "ERROR")
                      (cons ?C "42601")
                      (cons ?M "syntax error at or near \"SELEC\""))))
    (should (equal (pg--error-fields-message fields)
                   "ERROR [42601]: syntax error at or near \"SELEC\""))))

;;;; Unit tests — type parsing

(ert-deftest pg-test-parse-value-int ()
  "Test integer type parsing."
  (should (= (pg--parse-value "42" pg--oid-int4) 42))
  (should (= (pg--parse-value "-1" pg--oid-int4) -1))
  (should (= (pg--parse-value "0" pg--oid-int2) 0))
  (should (= (pg--parse-value "9999999999" pg--oid-int8) 9999999999)))

(ert-deftest pg-test-parse-value-float ()
  "Test float type parsing."
  (should (< (abs (- (pg--parse-value "3.14" pg--oid-float4) 3.14)) 0.001))
  (should (< (abs (- (pg--parse-value "2.718" pg--oid-float8) 2.718)) 0.001)))

(ert-deftest pg-test-parse-value-numeric ()
  "Test numeric/decimal type parsing."
  (should (= (pg--parse-value "123.45" pg--oid-numeric) 123.45)))

(ert-deftest pg-test-parse-value-bool ()
  "Test boolean type parsing."
  (should (eq (pg--parse-value "t" pg--oid-bool) t))
  (should (eq (pg--parse-value "f" pg--oid-bool) nil)))

(ert-deftest pg-test-parse-value-text ()
  "Test text type parsing."
  (should (equal (pg--parse-value "hello" pg--oid-text) "hello"))
  (should (equal (pg--parse-value "world" pg--oid-varchar) "world")))

(ert-deftest pg-test-parse-value-date ()
  "Test date type parsing."
  (should (equal (pg--parse-value "2024-03-15" pg--oid-date)
                 '(:year 2024 :month 3 :day 15))))

(ert-deftest pg-test-parse-value-time ()
  "Test time type parsing."
  (should (equal (pg--parse-value "13:45:30" pg--oid-time)
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil))))

(ert-deftest pg-test-parse-value-timestamp ()
  "Test timestamp type parsing."
  (should (equal (pg--parse-value "2024-03-15 13:45:30" pg--oid-timestamp)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))

(ert-deftest pg-test-parse-value-timestamptz ()
  "Test timestamptz type parsing (strips timezone)."
  (let ((result (pg--parse-value "2024-03-15 13:45:30+00" pg--oid-timestamptz)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :hours) 13))))

(ert-deftest pg-test-parse-value-null ()
  "Test NULL value handling."
  (should (null (pg--parse-value nil pg--oid-int4)))
  (should (null (pg--parse-value nil pg--oid-text))))

;;;; Unit tests — RowDescription parsing

(ert-deftest pg-test-parse-row-description ()
  "Test RowDescription parsing."
  (let* ((col1-name (concat "id" (unibyte-string 0)))
         (col1-data (concat (pg--int32-be-bytes 0)    ; table OID
                            (pg--int16-be-bytes 1)    ; column attr
                            (pg--int32-be-bytes pg--oid-int4) ; type OID
                            (pg--int16-be-bytes 4)    ; type size
                            (pg--int32-be-bytes -1)   ; type mod
                            (pg--int16-be-bytes 0)))  ; format
         (col2-name (concat "name" (unibyte-string 0)))
         (col2-data (concat (pg--int32-be-bytes 0)
                            (pg--int16-be-bytes 2)
                            (pg--int32-be-bytes pg--oid-text)
                            (pg--int16-be-bytes -1)
                            (pg--int32-be-bytes -1)
                            (pg--int16-be-bytes 0)))
         (payload (concat (pg--int16-be-bytes 2)
                          col1-name col1-data
                          col2-name col2-data))
         (columns (pg--parse-row-description payload)))
    (should (= (length columns) 2))
    (should (equal (plist-get (nth 0 columns) :name) "id"))
    (should (= (plist-get (nth 0 columns) :type-oid) pg--oid-int4))
    (should (equal (plist-get (nth 1 columns) :name) "name"))
    (should (= (plist-get (nth 1 columns) :type-oid) pg--oid-text))))

;;;; Unit tests — CommandComplete parsing

(ert-deftest pg-test-parse-command-complete ()
  "Test CommandComplete parsing."
  (pcase-let ((`(,tag . ,rows)
               (pg--parse-command-complete
                (concat "SELECT 5" (unibyte-string 0)))))
    (should (equal tag "SELECT 5"))
    (should (= rows 5)))
  (pcase-let ((`(,tag . ,rows)
               (pg--parse-command-complete
                (concat "INSERT 0 3" (unibyte-string 0)))))
    (should (equal tag "INSERT 0 3"))
    (should (= rows 3)))
  (pcase-let ((`(,_tag . ,rows)
               (pg--parse-command-complete
                (concat "CREATE TABLE" (unibyte-string 0)))))
    (should (null rows))))

;;;; Unit tests — escape functions

(ert-deftest pg-test-escape-identifier ()
  "Test identifier escaping."
  (should (equal (pg-escape-identifier "table") "\"table\""))
  (should (equal (pg-escape-identifier "my\"table") "\"my\"\"table\""))
  (should (equal (pg-escape-identifier "normal_name") "\"normal_name\"")))

(ert-deftest pg-test-escape-literal ()
  "Test literal escaping."
  (should (equal (pg-escape-literal "hello") "'hello'"))
  (should (equal (pg-escape-literal "it's") "'it''s'"))
  (should (equal (pg-escape-literal "no special") "'no special'")))

;;;; Unit tests — HMAC-SHA-256

(ert-deftest pg-test-hmac-sha256 ()
  "Test HMAC-SHA-256 against known values."
  ;; RFC 4231 Test Case 2
  (let* ((key (encode-coding-string "Jefe" 'utf-8))
         (data (encode-coding-string "what do ya want for nothing?" 'utf-8))
         (result (pg--hmac-sha256 key data))
         (hex (mapconcat (lambda (b) (format "%02x" b)) result "")))
    (should (equal hex "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"))))

;;;; Unit tests — struct creation

(ert-deftest pg-test-struct-creation ()
  "Test that structs can be created."
  (let ((conn (make-pg-conn :host "localhost" :port 5432
                             :user "postgres" :database "test")))
    (should (equal (pg-conn-host conn) "localhost"))
    (should (= (pg-conn-port conn) 5432))
    (should (= (pg-conn-read-timeout conn) 10)))
  (let ((result (make-pg-result :status "SELECT 1" :affected-rows 5)))
    (should (equal (pg-result-status result) "SELECT 1"))
    (should (= (pg-result-affected-rows result) 5))))

;;;; Unit tests — data-lens-db adapter

(ert-deftest pg-test-db-type-category ()
  "Test OID to type-category mapping."
  (require 'data-lens-db-pg)
  (should (eq (data-lens-db-pg--type-category pg--oid-int4) 'numeric))
  (should (eq (data-lens-db-pg--type-category pg--oid-float8) 'numeric))
  (should (eq (data-lens-db-pg--type-category pg--oid-json) 'json))
  (should (eq (data-lens-db-pg--type-category pg--oid-jsonb) 'json))
  (should (eq (data-lens-db-pg--type-category pg--oid-bytea) 'blob))
  (should (eq (data-lens-db-pg--type-category pg--oid-date) 'date))
  (should (eq (data-lens-db-pg--type-category pg--oid-time) 'time))
  (should (eq (data-lens-db-pg--type-category pg--oid-timestamp) 'datetime))
  (should (eq (data-lens-db-pg--type-category pg--oid-text) 'text))
  (should (eq (data-lens-db-pg--type-category 9999) 'text)))

;;;; Live integration tests (require a running PostgreSQL server)

(defmacro pg-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live PostgreSQL connection.
Skips if `pg-test-password' is nil."
  (declare (indent 1))
  `(if (null pg-test-password)
       (ert-skip "Set pg-test-password to enable live tests")
     (let ((,var (pg-connect :host pg-test-host
                              :port pg-test-port
                              :user pg-test-user
                              :password pg-test-password
                              :database pg-test-database)))
       (unwind-protect
           (progn ,@body)
         (pg-disconnect ,var)))))

(ert-deftest pg-test-live-connect-disconnect ()
  :tags '(:pg-live)
  "Test connecting and disconnecting."
  (pg-test--with-conn conn
    (should (pg-conn-p conn))
    (should (pg-conn-server-version conn))
    (should (> (pg-conn-pid conn) 0))))

(ert-deftest pg-test-live-select ()
  :tags '(:pg-live)
  "Test a simple SELECT query."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT 1 AS num, 'hello' AS greeting")))
      (should (pg-result-p result))
      (should (= (length (pg-result-columns result)) 2))
      (should (= (length (pg-result-rows result)) 1))
      (let ((row (car (pg-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest pg-test-live-multi-row ()
  :tags '(:pg-live)
  "Test query returning multiple rows."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT generate_series(1,5) AS n")))
      (should (= (length (pg-result-rows result)) 5)))))

(ert-deftest pg-test-live-dml ()
  :tags '(:pg-live)
  "Test INSERT/UPDATE/DELETE (DML) returning affected-rows."
  (pg-test--with-conn conn
    (pg-query conn "CREATE TEMPORARY TABLE _pg_el_test (id INT, val TEXT)")
    (let ((result (pg-query conn "INSERT INTO _pg_el_test VALUES (1, 'one'), (2, 'two')")))
      (should (= (pg-result-affected-rows result) 2)))
    (let ((result (pg-query conn "UPDATE _pg_el_test SET val = 'updated' WHERE id = 1")))
      (should (= (pg-result-affected-rows result) 1)))
    (let ((result (pg-query conn "SELECT * FROM _pg_el_test ORDER BY id")))
      (should (= (length (pg-result-rows result)) 2))
      (should (equal (cadr (car (pg-result-rows result))) "updated")))
    (let ((result (pg-query conn "DELETE FROM _pg_el_test")))
      (should (= (pg-result-affected-rows result) 2)))))

(ert-deftest pg-test-live-query-error ()
  :tags '(:pg-live)
  "Test that a syntax error signals pg-query-error."
  (pg-test--with-conn conn
    (should-error (pg-query conn "SELEC BAD SYNTAX")
                  :type 'pg-query-error)))

(ert-deftest pg-test-live-null-values ()
  :tags '(:pg-live)
  "Test that NULL values are returned as nil."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT NULL AS n, 42 AS v")))
      (let ((row (car (pg-result-rows result))))
        (should (null (car row)))
        (should (= (cadr row) 42))))))

(ert-deftest pg-test-live-date-time-types ()
  :tags '(:pg-live)
  "Test DATE, TIME, TIMESTAMP column parsing."
  (pg-test--with-conn conn
    (let ((result (pg-query conn
                   "SELECT DATE '2024-03-15', TIME '13:45:30', TIMESTAMP '2024-03-15 13:45:30'")))
      (let ((row (car (pg-result-rows result))))
        (should (equal (nth 0 row) '(:year 2024 :month 3 :day 15)))
        (should (equal (nth 1 row) '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
        (should (= (plist-get (nth 2 row) :year) 2024))
        (should (= (plist-get (nth 2 row) :hours) 13))))))

(ert-deftest pg-test-live-ping ()
  :tags '(:pg-live)
  "Test pg-ping."
  (pg-test--with-conn conn
    (should (eq (pg-ping conn) t))))

(ert-deftest pg-test-live-transaction-commit ()
  :tags '(:pg-live)
  "Test with-pg-transaction commits on success."
  (pg-test--with-conn conn
    (pg-query conn "CREATE TEMPORARY TABLE _pg_el_tx (id INT)")
    (with-pg-transaction conn
      (pg-query conn "INSERT INTO _pg_el_tx VALUES (1)")
      (pg-query conn "INSERT INTO _pg_el_tx VALUES (2)"))
    (let ((result (pg-query conn "SELECT COUNT(*) FROM _pg_el_tx")))
      (should (= (car (car (pg-result-rows result))) 2)))))

(ert-deftest pg-test-live-transaction-rollback ()
  :tags '(:pg-live)
  "Test with-pg-transaction rolls back on error."
  (pg-test--with-conn conn
    (pg-query conn "CREATE TEMPORARY TABLE _pg_el_tx2 (id INT)")
    (ignore-errors
      (with-pg-transaction conn
        (pg-query conn "INSERT INTO _pg_el_tx2 VALUES (1)")
        (error "Intentional error")))
    (let ((result (pg-query conn "SELECT COUNT(*) FROM _pg_el_tx2")))
      (should (= (car (car (pg-result-rows result))) 0)))))

(provide 'pg-test)
;;; pg-test.el ends here
