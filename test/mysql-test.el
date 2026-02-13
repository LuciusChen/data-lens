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
(defvar mysql-test-tls-enabled nil
  "Set this to enable TLS live tests.")

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
  (should (= (mysql--parse-value "42" mysql-type-long) 42))
  (should (= (mysql--parse-value "3.14" mysql-type-float) 3.14))
  (should (= (mysql--parse-value "2024" mysql-type-year) 2024))
  (should (equal (mysql--parse-value "hello" mysql-type-var-string) "hello"))
  (should (null (mysql--parse-value nil mysql-type-long))))

;;;; Extended type system tests

(ert-deftest mysql-test-parse-date ()
  "Test DATE string parsing."
  (should (equal (mysql--parse-date "2024-03-15")
                 '(:year 2024 :month 3 :day 15)))
  (should (null (mysql--parse-date "0000-00-00")))
  (should (null (mysql--parse-date ""))))

(ert-deftest mysql-test-parse-time ()
  "Test TIME string parsing."
  (should (equal (mysql--parse-time "13:45:30")
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
  (should (equal (mysql--parse-time "-02:30:00")
                 '(:hours 2 :minutes 30 :seconds 0 :negative t)))
  (should (null (mysql--parse-time ""))))

(ert-deftest mysql-test-parse-datetime ()
  "Test DATETIME/TIMESTAMP string parsing."
  (should (equal (mysql--parse-datetime "2024-03-15 13:45:30")
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30)))
  (should (equal (mysql--parse-datetime "2024-01-01 00:00:00.123456")
                 '(:year 2024 :month 1 :day 1
                   :hours 0 :minutes 0 :seconds 0)))
  (should (null (mysql--parse-datetime "0000-00-00 00:00:00")))
  (should (null (mysql--parse-datetime ""))))

(ert-deftest mysql-test-parse-bit ()
  "Test BIT binary string parsing."
  (should (= (mysql--parse-bit (unibyte-string #x01)) 1))
  (should (= (mysql--parse-bit (unibyte-string #x00 #xff)) 255))
  (should (= (mysql--parse-bit (unibyte-string #x01 #x00)) 256)))

(ert-deftest mysql-test-custom-type-parser ()
  "Test custom type parser override."
  (let ((mysql-type-parsers (list (cons mysql-type-long
                                       (lambda (v) (concat "custom:" v))))))
    (should (equal (mysql--parse-value "42" mysql-type-long) "custom:42")))
  ;; Without override, original behavior
  (should (= (mysql--parse-value "42" mysql-type-long) 42)))

(ert-deftest mysql-test-parse-value-date-types ()
  "Test that parse-value dispatches date/time types correctly."
  (should (equal (mysql--parse-value "2024-03-15" mysql-type-date)
                 '(:year 2024 :month 3 :day 15)))
  (should (equal (mysql--parse-value "13:45:30" mysql-type-time)
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
  (should (equal (mysql--parse-value "2024-03-15 13:45:30" mysql-type-datetime)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30)))
  (should (equal (mysql--parse-value "2024-03-15 13:45:30" mysql-type-timestamp)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))

;;;; Convenience API unit tests

(ert-deftest mysql-test-escape-identifier ()
  "Test identifier escaping."
  (should (equal (mysql-escape-identifier "table") "`table`"))
  (should (equal (mysql-escape-identifier "my`table") "`my``table`"))
  (should (equal (mysql-escape-identifier "normal_name") "`normal_name`")))

(ert-deftest mysql-test-escape-literal ()
  "Test literal escaping."
  (should (equal (mysql-escape-literal "hello") "'hello'"))
  (should (equal (mysql-escape-literal "it's") "'it\\'s'"))
  (should (equal (mysql-escape-literal "line\nbreak") "'line\\nbreak'"))
  (should (equal (mysql-escape-literal "back\\slash") "'back\\\\slash'")))

(ert-deftest mysql-test-uri-parsing ()
  "Test MySQL URI parsing via regex."
  ;; Test that the regex matches valid URIs
  (should (string-match
           "\\`mysql://\\([^:@]*\\)\\(?::\\([^@]*\\)\\)?@\\([^:/]*\\)\\(?::\\([0-9]+\\)\\)?\\(?:/\\(.*\\)\\)?\\'"
           "mysql://root:pass@localhost:3306/mydb"))
  (should (equal (match-string 1 "mysql://root:pass@localhost:3306/mydb") "root"))
  (should (equal (match-string 2 "mysql://root:pass@localhost:3306/mydb") "pass"))
  (should (equal (match-string 3 "mysql://root:pass@localhost:3306/mydb") "localhost"))
  (should (equal (match-string 4 "mysql://root:pass@localhost:3306/mydb") "3306"))
  (should (equal (match-string 5 "mysql://root:pass@localhost:3306/mydb") "mydb"))
  ;; Without port
  (should (string-match
           "\\`mysql://\\([^:@]*\\)\\(?::\\([^@]*\\)\\)?@\\([^:/]*\\)\\(?::\\([0-9]+\\)\\)?\\(?:/\\(.*\\)\\)?\\'"
           "mysql://root:pass@localhost/mydb"))
  (should (null (match-string 4 "mysql://root:pass@localhost/mydb"))))

;;;; TLS unit tests

(ert-deftest mysql-test-ssl-request-packet ()
  "Test SSL_REQUEST packet structure."
  (let* ((conn (make-mysql-conn :host "localhost" :port 3306
                                :user "root" :database "test"))
         (packet (mysql--build-ssl-request conn)))
    ;; SSL_REQUEST is exactly 32 bytes
    (should (= (length packet) 32))
    ;; Check client flags include SSL capability
    (let ((flags (logior (aref packet 0)
                         (ash (aref packet 1) 8)
                         (ash (aref packet 2) 16)
                         (ash (aref packet 3) 24))))
      (should (not (zerop (logand flags mysql--cap-ssl))))
      (should (not (zerop (logand flags mysql--cap-protocol-41)))))
    ;; Character set byte should be 45 (utf8mb4)
    (should (= (aref packet 8) 45))
    ;; Bytes 9-31 should be zero (filler)
    (let ((all-zero t))
      (dotimes (i 23)
        (unless (= (aref packet (+ 9 i)) 0) (setq all-zero nil)))
      (should all-zero))))

;;;; Prepared statement unit tests

(ert-deftest mysql-test-build-execute-packet ()
  "Test COM_STMT_EXECUTE packet construction."
  (let* ((stmt (make-mysql-stmt :id 1 :param-count 2 :column-count 1
                                :conn nil
                                :param-definitions nil
                                :column-definitions nil))
         (packet (mysql--build-execute-packet stmt '(42 "hello"))))
    ;; First byte: command 0x17
    (should (= (aref packet 0) #x17))
    ;; stmt_id: 4 bytes LE = 1
    (should (= (aref packet 1) 1))
    (should (= (aref packet 2) 0))
    ;; flags: 0x00
    (should (= (aref packet 5) #x00))
    ;; iteration_count: 1
    (should (= (aref packet 6) 1))))

(ert-deftest mysql-test-null-bitmap ()
  "Test NULL bitmap construction in execute packet."
  (let* ((stmt (make-mysql-stmt :id 1 :param-count 3 :column-count 0
                                :conn nil
                                :param-definitions nil
                                :column-definitions nil))
         (packet (mysql--build-execute-packet stmt '(nil 42 nil))))
    ;; NULL bitmap starts at offset 10 (1+4+1+4)
    ;; Params: nil=bit0, 42=bit1, nil=bit2 → bitmap = 0b101 = 5
    (should (= (aref packet 10) 5))))

(ert-deftest mysql-test-elisp-to-mysql-type ()
  "Test Elisp to MySQL type mapping."
  (should (= (car (mysql--elisp-to-mysql-type nil)) mysql-type-null))
  (should (= (car (mysql--elisp-to-mysql-type 42)) mysql-type-longlong))
  (should (= (car (mysql--elisp-to-mysql-type 3.14)) mysql-type-var-string))
  (should (= (car (mysql--elisp-to-mysql-type "hello")) mysql-type-var-string)))

(ert-deftest mysql-test-ieee754-double ()
  "Test IEEE 754 double decoding."
  ;; 3.14 = 0x40091EB851EB851F in big-endian
  ;; Little-endian: 1F 85 EB 51 B8 1E 09 40
  (let ((data (unibyte-string #x1f #x85 #xeb #x51 #xb8 #x1e #x09 #x40)))
    (should (< (abs (- (mysql--ieee754-double-to-float data 0) 3.14)) 0.0001)))
  ;; 0.0
  (let ((data (make-string 8 0)))
    (should (= (mysql--ieee754-double-to-float data 0) 0.0)))
  ;; 1.0 = 0x3FF0000000000000 → LE: 00 00 00 00 00 00 F0 3F
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xf0 #x3f)))
    (should (= (mysql--ieee754-double-to-float data 0) 1.0))))

(ert-deftest mysql-test-ieee754-single ()
  "Test IEEE 754 single-precision float decoding."
  ;; 1.0 = 0x3F800000 → LE: 00 00 80 3F
  (let ((data (unibyte-string #x00 #x00 #x80 #x3f)))
    (should (= (mysql--ieee754-single-to-float data 0) 1.0)))
  ;; 0.0
  (let ((data (make-string 4 0)))
    (should (= (mysql--ieee754-single-to-float data 0) 0.0))))

(ert-deftest mysql-test-binary-null-p ()
  "Test NULL bitmap bit checking for binary rows."
  ;; Bitmap with bit 2 set (col 0, offset=2): byte 0 = 0b00000100 = 4
  (should (mysql--binary-null-p (unibyte-string #x04) 0))
  (should-not (mysql--binary-null-p (unibyte-string #x04) 1))
  ;; Bit 3 = col 1: byte 0 = 0b00001000 = 8
  (should (mysql--binary-null-p (unibyte-string #x08) 1)))

(ert-deftest mysql-test-decode-binary-datetime ()
  "Test binary DATETIME decoding."
  ;; Length 0: nil
  (let ((data (unibyte-string 0)))
    (should (null (car (mysql--decode-binary-datetime data 0 mysql-type-datetime)))))
  ;; Length 4: date only
  (let ((data (unibyte-string 4 #xe8 #x07 3 15)))  ; 2024-03-15
    (let ((result (car (mysql--decode-binary-datetime data 0 mysql-type-datetime))))
      (should (= (plist-get result :year) 2024))
      (should (= (plist-get result :month) 3))
      (should (= (plist-get result :day) 15))))
  ;; Length 7: date + time
  (let ((data (unibyte-string 7 #xe8 #x07 3 15 13 45 30)))
    (let ((result (car (mysql--decode-binary-datetime data 0 mysql-type-datetime))))
      (should (= (plist-get result :year) 2024))
      (should (= (plist-get result :hours) 13))
      (should (= (plist-get result :seconds) 30)))))

(ert-deftest mysql-test-decode-binary-time ()
  "Test binary TIME decoding."
  ;; Length 0: zero time
  (let ((data (unibyte-string 0)))
    (let ((result (car (mysql--decode-binary-time data 0))))
      (should (= (plist-get result :hours) 0))
      (should (= (plist-get result :minutes) 0))))
  ;; Length 8: non-negative time, 0 days, 13:45:30
  (let ((data (unibyte-string 8 0 0 0 0 0 13 45 30)))
    (let ((result (car (mysql--decode-binary-time data 0))))
      (should (= (plist-get result :hours) 13))
      (should (= (plist-get result :minutes) 45))
      (should (= (plist-get result :seconds) 30))
      (should-not (plist-get result :negative))))
  ;; Length 8: negative time
  (let ((data (unibyte-string 8 1 0 0 0 0 2 30 0)))
    (let ((result (car (mysql--decode-binary-time data 0))))
      (should (plist-get result :negative)))))

(ert-deftest mysql-test-parse-binary-row ()
  "Test binary row parsing."
  ;; 2 columns, no NULLs: INT=42, STRING="hi"
  ;; Packet: 0x00 (header) + null_bitmap(1 byte) + values
  ;; null bitmap for 2 cols: (2+2+7)/8 = 1 byte, all zeros
  ;; INT (LONGLONG): 42 as 8-byte LE
  ;; STRING: lenenc "hi" = 0x02 "hi"
  (let* ((columns (list (list :type mysql-type-longlong :name "id")
                        (list :type mysql-type-var-string :name "name")))
         (packet (concat (unibyte-string #x00)          ; header
                         (unibyte-string #x00)          ; null bitmap
                         (mysql--int-le-bytes 42 8)     ; INT value
                         (unibyte-string 2) "hi"))      ; STRING value
         (row (mysql--parse-binary-row packet columns)))
    (should (= (nth 0 row) 42))
    (should (equal (nth 1 row) "hi"))))

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

;;;; Live tests — Extended type system

(ert-deftest mysql-test-live-date-time-types ()
  :tags '(:mysql-live)
  "Test DATE, TIME, DATETIME, TIMESTAMP column parsing."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_dt (
       d DATE, t TIME, dt DATETIME, ts TIMESTAMP NULL)")
    (mysql-query conn "INSERT INTO _mysql_el_dt VALUES
       ('2024-03-15', '13:45:30', '2024-03-15 13:45:30', '2024-03-15 13:45:30')")
    (let* ((result (mysql-query conn "SELECT * FROM _mysql_el_dt"))
           (row (car (mysql-result-rows result))))
      ;; DATE
      (should (equal (nth 0 row) '(:year 2024 :month 3 :day 15)))
      ;; TIME
      (should (equal (nth 1 row) '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
      ;; DATETIME
      (should (= (plist-get (nth 2 row) :year) 2024))
      (should (= (plist-get (nth 2 row) :hours) 13))
      ;; TIMESTAMP
      (should (= (plist-get (nth 3 row) :year) 2024)))))

(ert-deftest mysql-test-live-bit-enum-set ()
  :tags '(:mysql-live)
  "Test BIT, ENUM, SET column parsing."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_bes (
       b BIT(8), e ENUM('a','b','c'), s SET('x','y','z'))")
    (mysql-query conn "INSERT INTO _mysql_el_bes VALUES (b'11111111', 'b', 'x,z')")
    (let* ((result (mysql-query conn "SELECT * FROM _mysql_el_bes"))
           (row (car (mysql-result-rows result))))
      ;; BIT(8) with all bits set = 255
      (should (= (nth 0 row) 255))
      ;; ENUM and SET are returned as strings
      (should (equal (nth 1 row) "b"))
      (should (equal (nth 2 row) "x,z")))))

;;;; Live tests — Convenience APIs

(ert-deftest mysql-test-live-with-connection ()
  :tags '(:mysql-live)
  "Test with-mysql-connection auto-close."
  (if (null mysql-test-password)
      (ert-skip "Set mysql-test-password to enable live tests")
    (let (saved-conn)
      (with-mysql-connection conn (:host mysql-test-host :port mysql-test-port
                                   :user mysql-test-user :password mysql-test-password
                                   :database mysql-test-database)
        (setq saved-conn conn)
        (should (mysql-conn-p conn))
        (should (process-live-p (mysql-conn-process conn))))
      ;; After the macro, the connection should be closed
      (should-not (process-live-p (mysql-conn-process saved-conn))))))

(ert-deftest mysql-test-live-transaction-commit ()
  :tags '(:mysql-live)
  "Test with-mysql-transaction commits on success."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_tx (id INT)")
    (with-mysql-transaction conn
      (mysql-query conn "INSERT INTO _mysql_el_tx VALUES (1)")
      (mysql-query conn "INSERT INTO _mysql_el_tx VALUES (2)"))
    (let ((result (mysql-query conn "SELECT COUNT(*) FROM _mysql_el_tx")))
      (should (= (car (car (mysql-result-rows result))) 2)))))

(ert-deftest mysql-test-live-transaction-rollback ()
  :tags '(:mysql-live)
  "Test with-mysql-transaction rolls back on error."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_tx2 (id INT)")
    (ignore-errors
      (with-mysql-transaction conn
        (mysql-query conn "INSERT INTO _mysql_el_tx2 VALUES (1)")
        (error "Intentional error")))
    (let ((result (mysql-query conn "SELECT COUNT(*) FROM _mysql_el_tx2")))
      (should (= (car (car (mysql-result-rows result))) 0)))))

(ert-deftest mysql-test-live-ping ()
  :tags '(:mysql-live)
  "Test COM_PING."
  (mysql-test--with-conn conn
    (should (eq (mysql-ping conn) t))))

;;;; Live tests — Prepared statements

(ert-deftest mysql-test-live-prepare-select ()
  :tags '(:mysql-live)
  "Test prepared SELECT with parameters."
  (mysql-test--with-conn conn
    (let ((stmt (mysql-prepare conn "SELECT ? + ? AS sum")))
      (should (mysql-stmt-p stmt))
      (should (= (mysql-stmt-param-count stmt) 2))
      (let ((result (mysql-execute stmt 10 20)))
        (should (= (length (mysql-result-rows result)) 1))
        (should (= (car (car (mysql-result-rows result))) 30)))
      (mysql-stmt-close stmt))))

(ert-deftest mysql-test-live-prepare-insert ()
  :tags '(:mysql-live)
  "Test prepared INSERT."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_ps (id INT, name VARCHAR(50))")
    (let ((stmt (mysql-prepare conn "INSERT INTO _mysql_el_ps VALUES (?, ?)")))
      (let ((result (mysql-execute stmt 1 "alice")))
        (should (= (mysql-result-affected-rows result) 1)))
      (let ((result (mysql-execute stmt 2 "bob")))
        (should (= (mysql-result-affected-rows result) 1)))
      (mysql-stmt-close stmt))
    (let ((result (mysql-query conn "SELECT * FROM _mysql_el_ps ORDER BY id")))
      (should (= (length (mysql-result-rows result)) 2))
      (should (equal (cadr (car (mysql-result-rows result))) "alice")))))

(ert-deftest mysql-test-live-prepare-null-params ()
  :tags '(:mysql-live)
  "Test prepared statement with NULL parameters."
  (mysql-test--with-conn conn
    (let ((stmt (mysql-prepare conn "SELECT ? AS v")))
      (let ((result (mysql-execute stmt nil)))
        (should (null (car (car (mysql-result-rows result))))))
      (mysql-stmt-close stmt))))

(ert-deftest mysql-test-live-prepare-string-params ()
  :tags '(:mysql-live)
  "Test prepared statement with string parameters."
  (mysql-test--with-conn conn
    (let ((stmt (mysql-prepare conn "SELECT CONCAT(?, ?) AS s")))
      (let ((result (mysql-execute stmt "hello" " world")))
        (should (equal (car (car (mysql-result-rows result))) "hello world")))
      (mysql-stmt-close stmt))))

(ert-deftest mysql-test-live-prepare-multiple-executions ()
  :tags '(:mysql-live)
  "Test multiple executions of the same prepared statement."
  (mysql-test--with-conn conn
    (let ((stmt (mysql-prepare conn "SELECT ? * 2 AS doubled")))
      (dotimes (i 5)
        (let ((result (mysql-execute stmt (1+ i))))
          (should (= (car (car (mysql-result-rows result))) (* (1+ i) 2)))))
      (mysql-stmt-close stmt))))

(ert-deftest mysql-test-live-prepare-binary-types ()
  :tags '(:mysql-live)
  "Test binary protocol type round-trips."
  (mysql-test--with-conn conn
    (mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_bt (
       i INT, f DOUBLE, s VARCHAR(100), d DATE, dt DATETIME)")
    (let ((stmt (mysql-prepare conn
                  "INSERT INTO _mysql_el_bt VALUES (?, ?, ?, '2024-03-15', '2024-03-15 10:30:00')")))
      (mysql-execute stmt 42 3.14 "hello")
      (mysql-stmt-close stmt))
    (let ((result (mysql-query conn "SELECT * FROM _mysql_el_bt")))
      (let ((row (car (mysql-result-rows result))))
        (should (= (nth 0 row) 42))
        ;; Float comes back via text protocol
        (should (< (abs (- (nth 1 row) 3.14)) 0.001))
        (should (equal (nth 2 row) "hello"))))))

;;;; Live tests — TLS (require mysql-test-tls-enabled)

(defmacro mysql-test--with-tls-conn (var &rest body)
  "Execute BODY with VAR bound to a TLS MySQL connection.
Skips unless both `mysql-test-password' and `mysql-test-tls-enabled' are set."
  (declare (indent 1))
  `(if (or (null mysql-test-password) (null mysql-test-tls-enabled))
       (ert-skip "Set mysql-test-password and mysql-test-tls-enabled for TLS tests")
     (let ((mysql-tls-verify-server nil))
       (let ((,var (mysql-connect :host mysql-test-host
                                  :port mysql-test-port
                                  :user mysql-test-user
                                  :password mysql-test-password
                                  :database mysql-test-database
                                  :tls t)))
         (unwind-protect
             (progn ,@body)
           (mysql-disconnect ,var))))))

(ert-deftest mysql-test-live-tls-connect ()
  :tags '(:mysql-live :mysql-tls)
  "Test TLS connection and verify encryption is active."
  (mysql-test--with-tls-conn conn
    (should (mysql-conn-tls conn))
    (let* ((result (mysql-query conn "SHOW STATUS LIKE 'Ssl_cipher'"))
           (cipher (cadr (car (mysql-result-rows result)))))
      (should (stringp cipher))
      (should (not (string-empty-p cipher))))))

(ert-deftest mysql-test-live-tls-query ()
  :tags '(:mysql-live :mysql-tls)
  "Test query execution over TLS."
  (mysql-test--with-tls-conn conn
    (let ((result (mysql-query conn "SELECT 42 AS v, 'tls-ok' AS msg")))
      (let ((row (car (mysql-result-rows result))))
        (should (= (car row) 42))
        (should (equal (cadr row) "tls-ok"))))))

(ert-deftest mysql-test-live-tls-prepared-statement ()
  :tags '(:mysql-live :mysql-tls)
  "Test prepared statements over TLS."
  (mysql-test--with-tls-conn conn
    (let ((stmt (mysql-prepare conn "SELECT ? + 1 AS v")))
      (let ((result (mysql-execute stmt 99)))
        (should (= (car (car (mysql-result-rows result))) 100)))
      (mysql-stmt-close stmt))))

(ert-deftest mysql-test-live-tls-caching-sha2-full-auth ()
  :tags '(:mysql-live :mysql-tls)
  "Test caching_sha2_password full auth over TLS (auth switch path)."
  (if (or (null mysql-test-password) (null mysql-test-tls-enabled))
      (ert-skip "Set mysql-test-password and mysql-test-tls-enabled for TLS tests")
    (let ((mysql-tls-verify-server nil))
      ;; Create a caching_sha2_password user and flush to force full auth
      (let ((admin (mysql-connect :host mysql-test-host
                                  :port mysql-test-port
                                  :user mysql-test-user
                                  :password mysql-test-password
                                  :database mysql-test-database)))
        (unwind-protect
            (progn
              (condition-case nil
                  (mysql-query admin "DROP USER '_mysql_el_sha2test'@'%'")
                (mysql-query-error nil))
              (mysql-query admin
                "CREATE USER '_mysql_el_sha2test'@'%' IDENTIFIED WITH caching_sha2_password BY 'testpw'")
              (mysql-query admin "GRANT ALL ON *.* TO '_mysql_el_sha2test'@'%'")
              (mysql-query admin "FLUSH PRIVILEGES"))
          (mysql-disconnect admin)))
      ;; Connect as the new user over TLS (full auth required)
      (let ((conn (mysql-connect :host mysql-test-host
                                 :port mysql-test-port
                                 :user "_mysql_el_sha2test"
                                 :password "testpw"
                                 :database mysql-test-database
                                 :tls t)))
        (unwind-protect
            (progn
              (should (mysql-conn-tls conn))
              (let ((result (mysql-query conn "SELECT CURRENT_USER()")))
                (should (string-prefix-p "_mysql_el_sha2test"
                                         (car (car (mysql-result-rows result)))))))
          (mysql-disconnect conn)))
      ;; Cleanup
      (let ((admin (mysql-connect :host mysql-test-host
                                  :port mysql-test-port
                                  :user mysql-test-user
                                  :password mysql-test-password
                                  :database mysql-test-database)))
        (unwind-protect
            (condition-case nil
                (mysql-query admin "DROP USER '_mysql_el_sha2test'@'%'")
              (mysql-query-error nil))
          (mysql-disconnect admin))))))

(provide 'mysql-test)
;;; mysql-test.el ends here
