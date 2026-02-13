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
  (should (= (pg--parse-value "42" pg-oid-int4) 42))
  (should (= (pg--parse-value "-1" pg-oid-int4) -1))
  (should (= (pg--parse-value "0" pg-oid-int2) 0))
  (should (= (pg--parse-value "9999999999" pg-oid-int8) 9999999999)))

(ert-deftest pg-test-parse-value-float ()
  "Test float type parsing."
  (should (< (abs (- (pg--parse-value "3.14" pg-oid-float4) 3.14)) 0.001))
  (should (< (abs (- (pg--parse-value "2.718" pg-oid-float8) 2.718)) 0.001)))

(ert-deftest pg-test-parse-value-numeric ()
  "Test numeric/decimal type parsing."
  (should (= (pg--parse-value "123.45" pg-oid-numeric) 123.45)))

(ert-deftest pg-test-parse-value-bool ()
  "Test boolean type parsing."
  (should (eq (pg--parse-value "t" pg-oid-bool) t))
  (should (eq (pg--parse-value "f" pg-oid-bool) nil)))

(ert-deftest pg-test-parse-value-text ()
  "Test text type parsing."
  (should (equal (pg--parse-value "hello" pg-oid-text) "hello"))
  (should (equal (pg--parse-value "world" pg-oid-varchar) "world")))

(ert-deftest pg-test-parse-value-date ()
  "Test date type parsing."
  (should (equal (pg--parse-value "2024-03-15" pg-oid-date)
                 '(:year 2024 :month 3 :day 15))))

(ert-deftest pg-test-parse-value-time ()
  "Test time type parsing."
  (should (equal (pg--parse-value "13:45:30" pg-oid-time)
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil))))

(ert-deftest pg-test-parse-value-timestamp ()
  "Test timestamp type parsing."
  (should (equal (pg--parse-value "2024-03-15 13:45:30" pg-oid-timestamp)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))

(ert-deftest pg-test-parse-value-timestamptz ()
  "Test timestamptz type parsing (strips timezone)."
  (let ((result (pg--parse-value "2024-03-15 13:45:30+00" pg-oid-timestamptz)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :hours) 13))))

(ert-deftest pg-test-parse-value-null ()
  "Test NULL value handling."
  (should (null (pg--parse-value nil pg-oid-int4)))
  (should (null (pg--parse-value nil pg-oid-text))))

(ert-deftest pg-test-parse-value-bigint ()
  "Test large integer parsing."
  ;; Large positive
  (should (= (pg--parse-value "9223372036854775807" pg-oid-int8)
             9223372036854775807))
  ;; Large negative
  (should (= (pg--parse-value "-9223372036854775808" pg-oid-int8)
             -9223372036854775808)))

(ert-deftest pg-test-parse-value-float-edge-cases ()
  "Test float parsing edge cases."
  (should (= (pg--parse-value "0.0" pg-oid-float8) 0.0))
  (should (= (pg--parse-value "-0.0" pg-oid-float8) -0.0))
  ;; Scientific notation
  (should (= (pg--parse-value "1.5e10" pg-oid-float8) 1.5e10))
  (should (= (pg--parse-value "-3.14e-5" pg-oid-float8) -3.14e-5)))

(ert-deftest pg-test-parse-value-empty-string ()
  "Test empty string handling."
  (should (equal (pg--parse-value "" pg-oid-text) ""))
  (should (equal (pg--parse-value "" pg-oid-varchar) "")))

(ert-deftest pg-test-parse-value-json ()
  "Test JSON/JSONB parsing."
  (when (fboundp 'json-parse-string)
    (let ((result (pg--parse-value "{\"a\":1,\"b\":\"hello\"}" pg-oid-json)))
      (should (hash-table-p result))
      (should (= (gethash "a" result) 1))
      (should (equal (gethash "b" result) "hello")))
    (let ((result (pg--parse-value "[1,2,3]" pg-oid-jsonb)))
      (should (vectorp result))
      (should (= (length result) 3)))))

(ert-deftest pg-test-parse-value-timestamp-with-microseconds ()
  "Test timestamp parsing with microseconds."
  (let ((result (pg--parse-value "2024-03-15 13:45:30.123456" pg-oid-timestamp)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :seconds) 30))))

(ert-deftest pg-test-parse-value-timestamptz-various ()
  "Test timestamptz parsing with various timezone formats."
  ;; +00 timezone
  (let ((result (pg--parse-value "2024-03-15 13:45:30+00" pg-oid-timestamptz)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :hours) 13)))
  ;; +05:30 timezone
  (let ((result (pg--parse-value "2024-03-15 13:45:30+05:30" pg-oid-timestamptz)))
    (should (= (plist-get result :hours) 13)))
  ;; -08 timezone
  (let ((result (pg--parse-value "2024-03-15 13:45:30-08" pg-oid-timestamptz)))
    (should (= (plist-get result :month) 3))))

(ert-deftest pg-test-parse-value-time-with-fractional ()
  "Test time parsing with fractional seconds."
  (let ((result (pg--parse-value "13:45:30.999" pg-oid-time)))
    (should (= (plist-get result :hours) 13))
    (should (= (plist-get result :minutes) 45))
    (should (= (plist-get result :seconds) 30))))

(ert-deftest pg-test-parse-value-custom-parser ()
  "Test custom type parser override."
  (let ((pg-type-parsers (list (cons pg-oid-int4
                                      (lambda (v) (concat "custom:" v))))))
    (should (equal (pg--parse-value "42" pg-oid-int4) "custom:42")))
  ;; Without override, original behavior
  (should (= (pg--parse-value "42" pg-oid-int4) 42)))

;;;; Unit tests — RowDescription parsing

(ert-deftest pg-test-parse-row-description ()
  "Test RowDescription parsing."
  (let* ((col1-name (concat "id" (unibyte-string 0)))
         (col1-data (concat (pg--int32-be-bytes 0)    ; table OID
                            (pg--int16-be-bytes 1)    ; column attr
                            (pg--int32-be-bytes pg-oid-int4) ; type OID
                            (pg--int16-be-bytes 4)    ; type size
                            (pg--int32-be-bytes -1)   ; type mod
                            (pg--int16-be-bytes 0)))  ; format
         (col2-name (concat "name" (unibyte-string 0)))
         (col2-data (concat (pg--int32-be-bytes 0)
                            (pg--int16-be-bytes 2)
                            (pg--int32-be-bytes pg-oid-text)
                            (pg--int16-be-bytes -1)
                            (pg--int32-be-bytes -1)
                            (pg--int16-be-bytes 0)))
         (payload (concat (pg--int16-be-bytes 2)
                          col1-name col1-data
                          col2-name col2-data))
         (columns (pg--parse-row-description payload)))
    (should (= (length columns) 2))
    (should (equal (plist-get (nth 0 columns) :name) "id"))
    (should (= (plist-get (nth 0 columns) :type-oid) pg-oid-int4))
    (should (equal (plist-get (nth 1 columns) :name) "name"))
    (should (= (plist-get (nth 1 columns) :type-oid) pg-oid-text))))

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

;;;; Unit tests — HMAC-SHA-256 and cryptography

(ert-deftest pg-test-hmac-sha256 ()
  "Test HMAC-SHA-256 against known values."
  ;; RFC 4231 Test Case 2
  (let* ((key (encode-coding-string "Jefe" 'utf-8))
         (data (encode-coding-string "what do ya want for nothing?" 'utf-8))
         (result (pg--hmac-sha256 key data))
         (hex (mapconcat (lambda (b) (format "%02x" b)) result "")))
    (should (equal hex "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"))))

(ert-deftest pg-test-hmac-sha256-empty ()
  "Test HMAC-SHA-256 with empty inputs."
  ;; Empty data, non-empty key
  (let* ((key (encode-coding-string "key" 'utf-8))
         (data "")
         (result (pg--hmac-sha256 key data)))
    (should (= (length result) 32)))
  ;; Empty key, non-empty data
  (let* ((key "")
         (data (encode-coding-string "data" 'utf-8))
         (result (pg--hmac-sha256 key data)))
    (should (= (length result) 32))))

(ert-deftest pg-test-xor-strings ()
  "Test XOR of two equal-length strings."
  (should (equal (pg--xor-strings (unibyte-string #xff #x00 #xaa)
                                   (unibyte-string #xff #xff #x55))
                 (unibyte-string #x00 #xff #xff)))
  ;; XOR with itself gives zeros
  (let ((s (unibyte-string #x12 #x34 #x56)))
    (should (equal (pg--xor-strings s s) (make-string 3 0))))
  ;; XOR with zeros is identity
  (let ((s (unibyte-string #xab #xcd)))
    (should (equal (pg--xor-strings s (make-string 2 0)) s))))

(ert-deftest pg-test-pbkdf2-sha256 ()
  "Test PBKDF2-SHA256 derivation (basic sanity checks)."
  ;; Derive a 32-byte key
  (let ((key (pg--pbkdf2-sha256 "password" "salt" 1 32)))
    (should (= (length key) 32))
    (should (stringp key)))
  ;; More iterations should produce different output
  (let ((key1 (pg--pbkdf2-sha256 "password" "salt" 1 32))
        (key2 (pg--pbkdf2-sha256 "password" "salt" 2 32)))
    (should-not (equal key1 key2)))
  ;; Different salt should produce different output
  (let ((key1 (pg--pbkdf2-sha256 "password" "salt1" 1 32))
        (key2 (pg--pbkdf2-sha256 "password" "salt2" 1 32)))
    (should-not (equal key1 key2))))

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

(ert-deftest pg-test-struct-defaults ()
  "Test struct default values."
  (let ((conn (make-pg-conn :host "h" :user "u")))
    (should (null (pg-conn-process conn)))
    (should (null (pg-conn-buf conn)))
    (should (null (pg-conn-server-version conn)))
    (should (null (pg-conn-pid conn)))
    (should (null (pg-conn-parameters conn)))
    (should (= (pg-conn-read-timeout conn) 10))
    (should (null (pg-conn-tls conn))))
  (let ((result (make-pg-result)))
    (should (null (pg-result-connection result)))
    (should (null (pg-result-status result)))
    (should (null (pg-result-columns result)))
    (should (null (pg-result-rows result)))
    (should (null (pg-result-affected-rows result)))))

;;;; Unit tests — SCRAM authentication helpers

(ert-deftest pg-test-scram-client-first ()
  "Test SCRAM client-first message generation."
  (let ((result (pg--scram-client-first "testuser")))
    ;; Returns (CLIENT-FIRST-MESSAGE CLIENT-NONCE . CLIENT-FIRST-BARE)
    (should (consp result))
    (should (stringp (car result)))      ; client-first-message
    (should (stringp (cadr result)))     ; client-nonce
    (should (stringp (cddr result)))     ; client-first-bare
    ;; client-first starts with "n,,"
    (should (string-prefix-p "n,," (car result)))
    ;; client-first-bare contains the nonce
    (should (string-match-p (regexp-quote (cadr result)) (cddr result)))))

(ert-deftest pg-test-scram-parse-server-first ()
  "Test SCRAM server-first message parsing."
  (let ((server-first "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"))
    (let ((parsed (pg--scram-parse-server-first server-first)))
      (should (string= (plist-get parsed :nonce)
                       "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"))
      (should (stringp (plist-get parsed :salt)))
      (should (= (plist-get parsed :iterations) 4096)))))

(ert-deftest pg-test-parse-sasl-mechanisms ()
  "Test SASL mechanism list parsing."
  ;; Payload: 4-byte auth type + mechanism names (NUL-terminated) + final NUL
  (let ((payload (concat (unibyte-string 0 0 0 10)  ; auth type 10
                         "SCRAM-SHA-256" (unibyte-string 0)
                         "SCRAM-SHA-256-PLUS" (unibyte-string 0)
                         (unibyte-string 0))))  ; terminator
    (let ((mechanisms (pg--parse-sasl-mechanisms payload)))
      (should (= (length mechanisms) 2))
      (should (member "SCRAM-SHA-256" mechanisms))
      (should (member "SCRAM-SHA-256-PLUS" mechanisms))))
  ;; Single mechanism
  (let ((payload (concat (unibyte-string 0 0 0 10)
                         "SCRAM-SHA-256" (unibyte-string 0)
                         (unibyte-string 0))))
    (should (= (length (pg--parse-sasl-mechanisms payload)) 1))))

;;;; Unit tests — MD5 password

(ert-deftest pg-test-md5-password ()
  "Test MD5 password hashing."
  ;; Result format: "md5" + md5(md5(password + user) + salt)
  (let ((result (pg--md5-password "user1" "pass123" "SALT")))
    (should (stringp result))
    (should (string-prefix-p "md5" result))
    (should (= (length result) 35))))  ; "md5" + 32 hex chars

(ert-deftest pg-test-md5-password-deterministic ()
  "Test that MD5 password hashing is deterministic."
  (let ((r1 (pg--md5-password "user" "password" "salt"))
        (r2 (pg--md5-password "user" "password" "salt")))
    (should (equal r1 r2)))
  ;; Different inputs produce different outputs
  (let ((r1 (pg--md5-password "user1" "pass" "salt"))
        (r2 (pg--md5-password "user2" "pass" "salt")))
    (should-not (equal r1 r2))))

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

(ert-deftest pg-test-live-empty-result ()
  :tags '(:pg-live)
  "Test a query that returns zero rows."
  (pg-test--with-conn conn
    (pg-query conn "CREATE TEMPORARY TABLE _pg_el_empty (id INT)")
    (let ((result (pg-query conn "SELECT * FROM _pg_el_empty")))
      (should (= (length (pg-result-rows result)) 0))
      (should (= (length (pg-result-columns result)) 1)))))

(ert-deftest pg-test-live-empty-string ()
  :tags '(:pg-live)
  "Test that empty strings are handled correctly."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT '' AS empty")))
      (should (equal (car (car (pg-result-rows result))) "")))))

(ert-deftest pg-test-live-special-chars ()
  :tags '(:pg-live)
  "Test strings with special characters."
  (pg-test--with-conn conn
    ;; Single quotes
    (let ((result (pg-query conn "SELECT 'it''s' AS s")))
      (should (equal (car (car (pg-result-rows result))) "it's")))
    ;; Backslash
    (let ((result (pg-query conn "SELECT E'back\\\\slash' AS s")))
      (should (equal (car (car (pg-result-rows result))) "back\\slash")))
    ;; Newline
    (let ((result (pg-query conn "SELECT E'line1\\nline2' AS s")))
      (should (string-match-p "\n" (car (car (pg-result-rows result))))))))

(ert-deftest pg-test-live-unicode ()
  :tags '(:pg-live)
  "Test Unicode string handling."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT '你好世界' AS greeting")))
      (should (equal (car (car (pg-result-rows result))) "你好世界")))
    (let ((result (pg-query conn "SELECT 'émoji: 🎉' AS s")))
      (should (string-match-p "🎉" (car (car (pg-result-rows result))))))))

(ert-deftest pg-test-live-json-types ()
  :tags '(:pg-live)
  "Test JSON and JSONB column handling."
  (when (fboundp 'json-parse-string)
    (pg-test--with-conn conn
      ;; JSON
      (let* ((result (pg-query conn "SELECT '{\"a\":1}'::json AS j"))
             (row (car (pg-result-rows result)))
             (val (car row)))
        (should (hash-table-p val))
        (should (= (gethash "a" val) 1)))
      ;; JSONB
      (let* ((result (pg-query conn "SELECT '[1,2,3]'::jsonb AS j"))
             (row (car (pg-result-rows result)))
             (val (car row)))
        (should (vectorp val))
        (should (= (length val) 3))))))

(ert-deftest pg-test-live-boolean ()
  :tags '(:pg-live)
  "Test boolean column handling."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT TRUE AS t, FALSE AS f")))
      (let ((row (car (pg-result-rows result))))
        (should (eq (nth 0 row) t))
        (should (eq (nth 1 row) nil))))))

(ert-deftest pg-test-live-numeric-types ()
  :tags '(:pg-live)
  "Test various numeric types."
  (pg-test--with-conn conn
    ;; SMALLINT
    (let ((result (pg-query conn "SELECT 32767::smallint AS s")))
      (should (= (car (car (pg-result-rows result))) 32767)))
    ;; BIGINT
    (let ((result (pg-query conn "SELECT 9223372036854775807::bigint AS b")))
      (should (= (car (car (pg-result-rows result))) 9223372036854775807)))
    ;; NUMERIC/DECIMAL
    (let ((result (pg-query conn "SELECT 123.456::numeric AS n")))
      (should (= (car (car (pg-result-rows result))) 123.456)))))

(ert-deftest pg-test-live-with-connection ()
  :tags '(:pg-live)
  "Test with-pg-connection auto-close."
  (if (null pg-test-password)
      (ert-skip "Set pg-test-password to enable live tests")
    (let (saved-conn)
      (with-pg-connection conn (:host pg-test-host :port pg-test-port
                                :user pg-test-user :password pg-test-password
                                :database pg-test-database)
        (setq saved-conn conn)
        (should (pg-conn-p conn))
        (should (process-live-p (pg-conn-process conn))))
      ;; After the macro, the connection should be closed
      (should-not (process-live-p (pg-conn-process saved-conn))))))

(ert-deftest pg-test-live-large-result ()
  :tags '(:pg-live)
  "Test handling of larger result sets."
  (pg-test--with-conn conn
    (let ((result (pg-query conn "SELECT generate_series(1,1000) AS n")))
      (should (= (length (pg-result-rows result)) 1000))
      (should (= (car (car (pg-result-rows result))) 1))
      (should (= (car (car (last (pg-result-rows result)))) 1000)))))

(ert-deftest pg-test-live-auth-failure ()
  :tags '(:pg-live)
  "Test that wrong password signals pg-auth-error."
  (if (null pg-test-password)
      (ert-skip "Set pg-test-password to enable live tests")
    (should-error (pg-connect :host pg-test-host
                               :port pg-test-port
                               :user pg-test-user
                               :password "definitely-wrong-password"
                               :database pg-test-database)
                  :type 'pg-auth-error)))

;;;; Live tests — TLS

(defvar pg-test-tls-enabled nil
  "Set this to enable TLS live tests.")

(defmacro pg-test--with-tls-conn (var &rest body)
  "Execute BODY with VAR bound to a TLS PostgreSQL connection."
  (declare (indent 1))
  `(if (or (null pg-test-password) (null pg-test-tls-enabled))
       (ert-skip "Set pg-test-password and pg-test-tls-enabled for TLS tests")
     (let ((pg-tls-verify-server nil))
       (let ((,var (pg-connect :host pg-test-host
                                :port pg-test-port
                                :user pg-test-user
                                :password pg-test-password
                                :database pg-test-database
                                :tls t)))
         (unwind-protect
             (progn ,@body)
           (pg-disconnect ,var))))))

(ert-deftest pg-test-live-tls-connect ()
  :tags '(:pg-live :pg-tls)
  "Test TLS connection."
  (pg-test--with-tls-conn conn
    (should (pg-conn-tls conn))
    (should (pg-conn-p conn))))

(ert-deftest pg-test-live-tls-query ()
  :tags '(:pg-live :pg-tls)
  "Test query execution over TLS."
  (pg-test--with-tls-conn conn
    (let ((result (pg-query conn "SELECT 42 AS v, 'tls-ok' AS msg")))
      (let ((row (car (pg-result-rows result))))
        (should (= (car row) 42))
        (should (equal (cadr row) "tls-ok"))))))

(ert-deftest pg-test-live-tls-verify-ssl-status ()
  :tags '(:pg-live :pg-tls)
  "Test that connection is actually using SSL."
  (pg-test--with-tls-conn conn
    (let* ((result (pg-query conn "SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
           (rows (pg-result-rows result)))
      (when (> (length rows) 0)
        (should (eq (car (car rows)) t))))))

(provide 'pg-test)
;;; pg-test.el ends here
