;;; clutch-mysql-test.el --- ERT tests for the MySQL wire protocol client -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the internal clutch MySQL protocol client.
;;
;; Tests marked with :clutch-mysql-live require a running MySQL instance:
;;   docker run -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;
;; Run all unit tests:
;;   emacs -batch -L .. -l ert -l clutch-mysql-test -f ert-run-tests-batch-and-exit
;;
;; Run live integration tests:
;;   emacs -batch -L .. -l ert -l clutch-mysql-test \
;;     --eval '(setq clutch-mysql-test-password "test")' \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'clutch-mysql)

;;;; Test configuration for live tests

(defvar clutch-mysql-test-host "127.0.0.1")
(defvar clutch-mysql-test-port 3306)
(defvar clutch-mysql-test-user "root")
(defvar clutch-mysql-test-password nil
  "Set this to enable live integration tests.")
(defvar clutch-mysql-test-database "mysql")
(defvar clutch-mysql-test-tls-enabled nil
  "Set this to enable TLS live tests.")

(defconst clutch-mysql-test--unsupported-caps
  '((compress . #x00000020)
    (local-files . #x00000080)
    (multi-statements . #x00010000)
    (multi-results . #x00020000)
    (ps-multi-results . #x00040000)
    (connect-attrs . #x00100000)
    (plugin-auth-lenenc-data . #x00200000)
    (session-track . #x00800000)
    (deprecate-eof . #x01000000)
    (optional-resultset-metadata . #x02000000)
    (zstd-compression . #x04000000)
    (query-attributes . #x08000000)
    (multi-factor-authentication . #x10000000))
  "MySQL capability flags that the protocol client does not implement.")

(defun clutch-mysql-test--packet-client-flags (packet)
  "Return the 4-byte client capability flags from PACKET."
  (logior (aref packet 0)
          (ash (aref packet 1) 8)
          (ash (aref packet 2) 16)
          (ash (aref packet 3) 24)))

(defun clutch-mysql-test--assert-no-unsupported-caps (flags)
  "Assert FLAGS do not advertise unsupported MySQL protocol features."
  (dolist (cap clutch-mysql-test--unsupported-caps)
    (ert-info ((format "capability: %s" (car cap)))
      (should (zerop (logand flags (cdr cap)))))))

;;;; Unit tests — protocol helpers (no server needed)

(ert-deftest clutch-mysql-test-int-le-bytes ()
  "Test little-endian integer encoding."
  (should (equal (clutch-mysql--int-le-bytes 0 1) (unibyte-string 0)))
  (should (equal (clutch-mysql--int-le-bytes 255 1) (unibyte-string 255)))
  (should (equal (clutch-mysql--int-le-bytes #x0102 2) (unibyte-string #x02 #x01)))
  (should (equal (clutch-mysql--int-le-bytes #x010203 3) (unibyte-string #x03 #x02 #x01)))
  (should (equal (clutch-mysql--int-le-bytes #x01020304 4)
                 (unibyte-string #x04 #x03 #x02 #x01))))

(ert-deftest clutch-mysql-test-lenenc-int-bytes ()
  "Test length-encoded integer encoding."
  (should (equal (clutch-mysql--lenenc-int-bytes 0) (unibyte-string 0)))
  (should (equal (clutch-mysql--lenenc-int-bytes 250) (unibyte-string 250)))
  (should (equal (clutch-mysql--lenenc-int-bytes 251)
                 (concat (unibyte-string #xfc) (clutch-mysql--int-le-bytes 251 2))))
  (should (equal (clutch-mysql--lenenc-int-bytes #xffff)
                 (concat (unibyte-string #xfc) (clutch-mysql--int-le-bytes #xffff 2))))
  (should (equal (clutch-mysql--lenenc-int-bytes #x10000)
                 (concat (unibyte-string #xfd) (clutch-mysql--int-le-bytes #x10000 3)))))

(ert-deftest clutch-mysql-test-lenenc-int-from-string ()
  "Test reading length-encoded integers from a string."
  (should (equal (clutch-mysql--read-lenenc-int-from-string (unibyte-string 42) 0)
                 '(42 . 1)))
  (should (equal (clutch-mysql--read-lenenc-int-from-string
                  (unibyte-string #xfc #x01 #x00) 0)
                 '(1 . 3)))
  (should (equal (clutch-mysql--read-lenenc-int-from-string
                  (unibyte-string #xfd #x01 #x00 #x00) 0)
                 '(1 . 4))))

(ert-deftest clutch-mysql-test-read-lenenc-string-from-string ()
  "Test reading length-encoded strings."
  (should (equal (clutch-mysql--read-lenenc-string-from-string
                  (concat (unibyte-string 5) "hello") 0)
                 '("hello" . 6)))
  (should (equal (clutch-mysql--read-lenenc-string-from-string
                  (concat (unibyte-string 0)) 0)
                 '("" . 1))))

(ert-deftest clutch-mysql-test-auth-native-password ()
  "Test mysql_native_password computation."
  ;; Empty password should return empty string
  (should (equal (clutch-mysql--auth-native-password "" "12345678901234567890") ""))
  (should (equal (clutch-mysql--auth-native-password nil "12345678901234567890") ""))
  ;; Non-empty password returns 20 bytes
  (let ((result (clutch-mysql--auth-native-password "secret" "12345678901234567890")))
    (should (= (length result) 20))))

(ert-deftest clutch-mysql-test-auth-caching-sha2-password ()
  "Test caching_sha2_password computation."
  (should (equal (clutch-mysql--auth-caching-sha2-password "" "12345678901234567890") ""))
  (let ((result (clutch-mysql--auth-caching-sha2-password "secret" "12345678901234567890")))
    (should (= (length result) 32))))

(ert-deftest clutch-mysql-test-xor-strings ()
  "Test XOR of two strings."
  (should (equal (clutch-mysql--xor-strings (unibyte-string #xff #x00 #xaa)
                                     (unibyte-string #xff #xff #x55))
                 (unibyte-string #x00 #xff #xff))))

(ert-deftest clutch-mysql-test-parse-ok-packet ()
  "Test OK packet parsing."
  (let* ((packet (concat (unibyte-string #x00  ; OK marker
                                         #x01  ; affected_rows = 1
                                         #x00  ; last_insert_id = 0
                                         #x02 #x00  ; status_flags
                                         #x00 #x00)))  ; warnings
         (info (clutch-mysql--parse-ok-packet packet)))
    (should (= (plist-get info :affected-rows) 1))
    (should (= (plist-get info :last-insert-id) 0))
    (should (= (plist-get info :warnings) 0))))

(ert-deftest clutch-mysql-test-parse-err-packet ()
  "Test ERR packet parsing."
  (let* ((packet (concat (unibyte-string #xff          ; ERR marker
                                         #x15 #x04)   ; error code 1045
                         "#"                           ; SQL state marker
                         "28000"                       ; SQL state
                         "Access denied"))
         (info (clutch-mysql--parse-err-packet packet)))
    (should (= (plist-get info :code) 1045))
    (should (equal (plist-get info :state) "28000"))
    (should (equal (plist-get info :message) "Access denied"))))

(ert-deftest clutch-mysql-test-packet-type ()
  "Test packet type detection."
  (should (eq (clutch-mysql--packet-type (unibyte-string #x00)) 'ok))
  (should (eq (clutch-mysql--packet-type (unibyte-string #xff)) 'err))
  (should (eq (clutch-mysql--packet-type (unibyte-string #xfe)) 'eof))
  (should (eq (clutch-mysql--packet-type (unibyte-string #xfb)) 'local-infile))
  (should (eq (clutch-mysql--packet-type (unibyte-string #x03 #x01 #x02)) 'data)))

(ert-deftest clutch-mysql-test-parse-value ()
  "Test MySQL type conversion."
  (should (= (clutch-mysql--parse-value "42" clutch-mysql-type-long) 42))
  (should (= (clutch-mysql--parse-value "3.14" clutch-mysql-type-float) 3.14))
  (should (= (clutch-mysql--parse-value "2024" clutch-mysql-type-year) 2024))
  (should (equal (clutch-mysql--parse-value "hello" clutch-mysql-type-var-string) "hello"))
  (should (null (clutch-mysql--parse-value nil clutch-mysql-type-long))))

;;;; Extended type system tests

(ert-deftest clutch-mysql-test-parse-date ()
  "Test DATE string parsing."
  (should (equal (clutch-mysql--parse-date "2024-03-15")
                 '(:year 2024 :month 3 :day 15)))
  (should (null (clutch-mysql--parse-date "0000-00-00")))
  (should (null (clutch-mysql--parse-date ""))))

(ert-deftest clutch-mysql-test-parse-time ()
  "Test TIME string parsing."
  (should (equal (clutch-mysql--parse-time "13:45:30")
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
  (should (equal (clutch-mysql--parse-time "-02:30:00")
                 '(:hours 2 :minutes 30 :seconds 0 :negative t)))
  (should (null (clutch-mysql--parse-time ""))))

(ert-deftest clutch-mysql-test-parse-datetime ()
  "Test DATETIME/TIMESTAMP string parsing."
  (should (equal (clutch-mysql--parse-datetime "2024-03-15 13:45:30")
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30)))
  (should (equal (clutch-mysql--parse-datetime "2024-01-01 00:00:00.123456")
                 '(:year 2024 :month 1 :day 1
                   :hours 0 :minutes 0 :seconds 0)))
  (should (null (clutch-mysql--parse-datetime "0000-00-00 00:00:00")))
  (should (null (clutch-mysql--parse-datetime ""))))

(ert-deftest clutch-mysql-test-parse-bit ()
  "Test BIT binary string parsing."
  (should (= (clutch-mysql--parse-bit (unibyte-string #x01)) 1))
  (should (= (clutch-mysql--parse-bit (unibyte-string #x00 #xff)) 255))
  (should (= (clutch-mysql--parse-bit (unibyte-string #x01 #x00)) 256)))

(ert-deftest clutch-mysql-test-custom-type-parser ()
  "Test custom type parser override."
  (let ((clutch-mysql-type-parsers (list (cons clutch-mysql-type-long
                                       (lambda (v) (concat "custom:" v))))))
    (should (equal (clutch-mysql--parse-value "42" clutch-mysql-type-long) "custom:42")))
  ;; Without override, original behavior
  (should (= (clutch-mysql--parse-value "42" clutch-mysql-type-long) 42)))

(ert-deftest clutch-mysql-test-parse-value-date-types ()
  "Test that parse-value dispatches date/time types correctly."
  (should (equal (clutch-mysql--parse-value "2024-03-15" clutch-mysql-type-date)
                 '(:year 2024 :month 3 :day 15)))
  (should (equal (clutch-mysql--parse-value "13:45:30" clutch-mysql-type-time)
                 '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
  (should (equal (clutch-mysql--parse-value "2024-03-15 13:45:30" clutch-mysql-type-datetime)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30)))
  (should (equal (clutch-mysql--parse-value "2024-03-15 13:45:30" clutch-mysql-type-timestamp)
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))

;;;; Convenience API unit tests

(ert-deftest clutch-mysql-test-escape-identifier ()
  "Test identifier escaping."
  (should (equal (clutch-mysql-escape-identifier "table") "`table`"))
  (should (equal (clutch-mysql-escape-identifier "my`table") "`my``table`"))
  (should (equal (clutch-mysql-escape-identifier "normal_name") "`normal_name`")))

(ert-deftest clutch-mysql-test-escape-literal ()
  "Test literal escaping."
  (should (equal (clutch-mysql-escape-literal "hello") "'hello'"))
  (should (equal (clutch-mysql-escape-literal "it's") "'it\\'s'"))
  (should (equal (clutch-mysql-escape-literal "line\nbreak") "'line\\nbreak'"))
  (should (equal (clutch-mysql-escape-literal "back\\slash") "'back\\\\slash'")))

(ert-deftest clutch-mysql-test-uri-parsing ()
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

(ert-deftest clutch-mysql-test-transaction-state-helpers ()
  "Test autocommit and transaction status helpers."
  (let ((conn (make-clutch-mysql-conn :status-flags
                               (logior clutch-mysql--server-status-autocommit
                                       clutch-mysql--server-status-in-transaction))))
    (should (clutch-mysql-autocommit-p conn))
    (should (clutch-mysql-in-transaction-p conn)))
  (let ((conn (make-clutch-mysql-conn :status-flags clutch-mysql--server-status-in-transaction)))
    (should-not (clutch-mysql-autocommit-p conn))
    (should (clutch-mysql-in-transaction-p conn)))
  (let ((conn (make-clutch-mysql-conn)))
    (should (clutch-mysql-autocommit-p conn))
    (should-not (clutch-mysql-in-transaction-p conn))))

(ert-deftest clutch-mysql-test-set-autocommit-issues-sql ()
  "Test that `clutch-mysql-set-autocommit' sends the expected SQL."
  (let ((conn (make-clutch-mysql-conn))
        (queries nil))
    (cl-letf (((symbol-function 'clutch-mysql-query)
               (lambda (_conn sql)
                 (push sql queries)
                 (make-clutch-mysql-result :status "OK"))))
      (clutch-mysql-set-autocommit conn nil)
      (clutch-mysql-set-autocommit conn t))
    (should (equal (nreverse queries)
                   '("SET autocommit = 0"
                     "SET autocommit = 1")))))

(ert-deftest clutch-mysql-test-commit-and-rollback-issue-sql ()
  "Test that `clutch-mysql-commit' and `clutch-mysql-rollback' send the expected SQL."
  (let ((conn (make-clutch-mysql-conn))
        (queries nil))
    (cl-letf (((symbol-function 'clutch-mysql-query)
               (lambda (_conn sql)
                 (push sql queries)
                 (make-clutch-mysql-result :status "OK"))))
      (clutch-mysql-commit conn)
      (clutch-mysql-rollback conn))
    (should (equal (nreverse queries)
                   '("COMMIT" "ROLLBACK")))))

(ert-deftest clutch-mysql-test-client-capability-flags ()
  "Client flags should match implemented protocol paths."
  (let* ((conn (make-clutch-mysql-conn :host "localhost"
                                       :port 3306
                                       :user "root"
                                       :database "test"
                                       :tls t))
         (flags (clutch-mysql--client-capabilities conn)))
    (should (not (zerop (logand flags clutch-mysql--cap-long-password))))
    (should (not (zerop (logand flags clutch-mysql--cap-protocol-41))))
    (should (not (zerop (logand flags clutch-mysql--cap-transactions))))
    (should (not (zerop (logand flags clutch-mysql--cap-secure-connection))))
    (should (not (zerop (logand flags clutch-mysql--cap-plugin-auth))))
    (should (not (zerop (logand flags clutch-mysql--cap-ssl))))
    (should (not (zerop (logand flags clutch-mysql--cap-connect-with-db))))
    (clutch-mysql-test--assert-no-unsupported-caps flags)))

;;;; TLS unit tests

(ert-deftest clutch-mysql-test-ssl-request-packet ()
  "Test SSL_REQUEST packet structure."
  (let* ((conn (make-clutch-mysql-conn :host "localhost" :port 3306
                                :user "root" :database "test"))
         (packet (clutch-mysql--build-ssl-request conn)))
    ;; SSL_REQUEST is exactly 32 bytes
    (should (= (length packet) 32))
    ;; Check client flags include SSL capability
    (let ((flags (clutch-mysql-test--packet-client-flags packet)))
      (should (not (zerop (logand flags clutch-mysql--cap-ssl))))
      (should (not (zerop (logand flags clutch-mysql--cap-protocol-41))))
      (clutch-mysql-test--assert-no-unsupported-caps flags))
    ;; Character set byte should be 45 (utf8mb4)
    (should (= (aref packet 8) 45))
    ;; Bytes 9-31 should be zero (filler)
    (let ((all-zero t))
      (dotimes (i 23)
        (unless (= (aref packet (+ 9 i)) 0) (setq all-zero nil)))
      (should all-zero))))

;;;; Prepared statement unit tests

(ert-deftest clutch-mysql-test-build-execute-packet ()
  "Test COM_STMT_EXECUTE packet construction."
  (let* ((stmt (make-clutch-mysql-stmt :id 1 :param-count 2 :column-count 1
                                :conn nil
                                :param-definitions nil
                                :column-definitions nil))
         (packet (clutch-mysql--build-execute-packet stmt '(42 "hello"))))
    ;; First byte: command 0x17
    (should (= (aref packet 0) #x17))
    ;; stmt_id: 4 bytes LE = 1
    (should (= (aref packet 1) 1))
    (should (= (aref packet 2) 0))
    ;; flags: 0x00
    (should (= (aref packet 5) #x00))
    ;; iteration_count: 1
    (should (= (aref packet 6) 1))))

(ert-deftest clutch-mysql-test-null-bitmap ()
  "Test NULL bitmap construction in execute packet."
  (let* ((stmt (make-clutch-mysql-stmt :id 1 :param-count 3 :column-count 0
                                :conn nil
                                :param-definitions nil
                                :column-definitions nil))
         (packet (clutch-mysql--build-execute-packet stmt '(nil 42 nil))))
    ;; NULL bitmap starts at offset 10 (1+4+1+4)
    ;; Params: nil=bit0, 42=bit1, nil=bit2 → bitmap = 0b101 = 5
    (should (= (aref packet 10) 5))))

(ert-deftest clutch-mysql-test-elisp-to-wire-type ()
  "Test Elisp to MySQL type mapping."
  (should (= (car (clutch-mysql--elisp-to-wire-type nil)) clutch-mysql-type-null))
  (should (= (car (clutch-mysql--elisp-to-wire-type 42)) clutch-mysql-type-longlong))
  (should (= (car (clutch-mysql--elisp-to-wire-type 3.14)) clutch-mysql-type-var-string))
  (should (= (car (clutch-mysql--elisp-to-wire-type "hello")) clutch-mysql-type-var-string)))

(ert-deftest clutch-mysql-test-ieee754-double ()
  "Test IEEE 754 double decoding."
  ;; 3.14 = 0x40091EB851EB851F in big-endian
  ;; Little-endian: 1F 85 EB 51 B8 1E 09 40
  (let ((data (unibyte-string #x1f #x85 #xeb #x51 #xb8 #x1e #x09 #x40)))
    (should (< (abs (- (clutch-mysql--ieee754-double-to-float data 0) 3.14)) 0.0001)))
  ;; 0.0
  (let ((data (make-string 8 0)))
    (should (= (clutch-mysql--ieee754-double-to-float data 0) 0.0)))
  ;; 1.0 = 0x3FF0000000000000 → LE: 00 00 00 00 00 00 F0 3F
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xf0 #x3f)))
    (should (= (clutch-mysql--ieee754-double-to-float data 0) 1.0))))

(ert-deftest clutch-mysql-test-ieee754-single ()
  "Test IEEE 754 single-precision float decoding."
  ;; 1.0 = 0x3F800000 → LE: 00 00 80 3F
  (let ((data (unibyte-string #x00 #x00 #x80 #x3f)))
    (should (= (clutch-mysql--ieee754-single-to-float data 0) 1.0)))
  ;; 0.0
  (let ((data (make-string 4 0)))
    (should (= (clutch-mysql--ieee754-single-to-float data 0) 0.0))))

(ert-deftest clutch-mysql-test-binary-null-p ()
  "Test NULL bitmap bit checking for binary rows."
  ;; Bitmap with bit 2 set (col 0, offset=2): byte 0 = 0b00000100 = 4
  (should (clutch-mysql--binary-null-p (unibyte-string #x04) 0))
  (should-not (clutch-mysql--binary-null-p (unibyte-string #x04) 1))
  ;; Bit 3 = col 1: byte 0 = 0b00001000 = 8
  (should (clutch-mysql--binary-null-p (unibyte-string #x08) 1)))

(ert-deftest clutch-mysql-test-decode-binary-datetime ()
  "Test binary DATETIME decoding."
  ;; Length 0: nil
  (let ((data (unibyte-string 0)))
    (should (null (car (clutch-mysql--decode-binary-datetime data 0 clutch-mysql-type-datetime)))))
  ;; Length 4: date only
  (let ((data (unibyte-string 4 #xe8 #x07 3 15)))  ; 2024-03-15
    (let ((result (car (clutch-mysql--decode-binary-datetime data 0 clutch-mysql-type-datetime))))
      (should (= (plist-get result :year) 2024))
      (should (= (plist-get result :month) 3))
      (should (= (plist-get result :day) 15))))
  ;; Length 7: date + time
  (let ((data (unibyte-string 7 #xe8 #x07 3 15 13 45 30)))
    (let ((result (car (clutch-mysql--decode-binary-datetime data 0 clutch-mysql-type-datetime))))
      (should (= (plist-get result :year) 2024))
      (should (= (plist-get result :hours) 13))
      (should (= (plist-get result :seconds) 30)))))

(ert-deftest clutch-mysql-test-decode-binary-time ()
  "Test binary TIME decoding."
  ;; Length 0: zero time
  (let ((data (unibyte-string 0)))
    (let ((result (car (clutch-mysql--decode-binary-time data 0))))
      (should (= (plist-get result :hours) 0))
      (should (= (plist-get result :minutes) 0))))
  ;; Length 8: non-negative time, 0 days, 13:45:30
  (let ((data (unibyte-string 8 0 0 0 0 0 13 45 30)))
    (let ((result (car (clutch-mysql--decode-binary-time data 0))))
      (should (= (plist-get result :hours) 13))
      (should (= (plist-get result :minutes) 45))
      (should (= (plist-get result :seconds) 30))
      (should-not (plist-get result :negative))))
  ;; Length 8: negative time
  (let ((data (unibyte-string 8 1 0 0 0 0 2 30 0)))
    (let ((result (car (clutch-mysql--decode-binary-time data 0))))
      (should (plist-get result :negative)))))

(ert-deftest clutch-mysql-test-parse-binary-row ()
  "Test binary row parsing."
  ;; 2 columns, no NULLs: INT=42, STRING="hi"
  ;; Packet: 0x00 (header) + null_bitmap(1 byte) + values
  ;; null bitmap for 2 cols: (2+2+7)/8 = 1 byte, all zeros
  ;; INT (LONGLONG): 42 as 8-byte LE
  ;; STRING: lenenc "hi" = 0x02 "hi"
  (let* ((columns (list (list :type clutch-mysql-type-longlong :name "id")
                        (list :type clutch-mysql-type-var-string :name "name")))
         (packet (concat (unibyte-string #x00)          ; header
                         (unibyte-string #x00)          ; null bitmap
                         (clutch-mysql--int-le-bytes 42 8)     ; INT value
                         (unibyte-string 2) "hi"))      ; STRING value
         (row (clutch-mysql--parse-binary-row packet columns)))
    (should (= (nth 0 row) 42))
    (should (equal (nth 1 row) "hi"))))

(ert-deftest clutch-mysql-test-parse-result-row ()
  "Test result row parsing."
  ;; Row with two string columns: "hello" and "world"
  (let* ((packet (concat (unibyte-string 5) "hello"
                         (unibyte-string 5) "world"))
         (row (clutch-mysql--parse-result-row packet 2)))
    (should (equal row '("hello" "world"))))
  ;; Row with NULL value
  (let* ((packet (concat (unibyte-string #xfb)
                         (unibyte-string 3) "foo"))
         (row (clutch-mysql--parse-result-row packet 2)))
    (should (equal row '(nil "foo")))))

(ert-deftest clutch-mysql-test-struct-creation ()
  "Test that structs can be created."
  (let ((conn (make-clutch-mysql-conn :host "localhost" :port 3306
                               :user "root" :database "test")))
    (should (equal (clutch-mysql-conn-host conn) "localhost"))
    (should (= (clutch-mysql-conn-port conn) 3306))
    (should (= (clutch-mysql-conn-read-idle-timeout conn) 30))
    (should (= (clutch-mysql-conn-sequence-id conn) 0)))
  (let ((result (make-clutch-mysql-result :status "OK" :affected-rows 5)))
    (should (equal (clutch-mysql-result-status result) "OK"))
    (should (= (clutch-mysql-result-affected-rows result) 5))))

(ert-deftest clutch-mysql-test-open-connection-does-not-force-plain-type ()
  "Opening a MySQL socket should not force an unsupported process type."
  (let (captured-args)
    (cl-letf (((symbol-function 'make-network-process)
               (lambda (&rest args)
                 (setq captured-args args)
                 'fake-proc))
              ((symbol-function 'set-process-coding-system) #'ignore)
              ((symbol-function 'set-process-filter) #'ignore)
              ((symbol-function 'clutch-mysql--wait-for-connect) #'ignore))
      (pcase-let ((`(,proc . ,buf) (clutch-mysql--open-connection "127.0.0.1" 3306 10)))
        (unwind-protect
            (progn
              (should (eq proc 'fake-proc))
              (should-not (plist-member captured-args :type)))
          (kill-buffer buf))))))

(ert-deftest clutch-mysql-test-connect-retries-caching-sha2-full-auth-with-tls ()
  "A non-TLS caching_sha2 full-auth failure should reconnect with TLS."
  (let ((auth-tls-flags nil)
        (buffers nil))
    (cl-letf (((symbol-function 'clutch-mysql--tls-available-p) (lambda () t))
              ((symbol-function 'clutch-mysql--open-connection)
               (lambda (_host _port _timeout)
                 (let ((buf (generate-new-buffer " *clutch-mysql-test-auto-tls*")))
                   (push buf buffers)
                   (cons (gensym "proc") buf))))
              ((symbol-function 'clutch-mysql--authenticate)
               (lambda (conn _password tls)
                 (push tls auth-tls-flags)
                 (if tls
                     (setf (clutch-mysql-conn-tls conn) t)
                   (signal 'clutch-mysql-auth-error
                           '("caching_sha2_password full authentication requires TLS")))))
              ((symbol-function 'process-live-p) (lambda (_proc) t))
              ((symbol-function 'delete-process) (lambda (_proc) nil)))
      (unwind-protect
          (let ((conn (clutch-mysql-connect :host "127.0.0.1" :port 3306
                                     :user "root" :password "pw"
                                     :database "mysql")))
            (should (equal (nreverse auth-tls-flags) '(nil t)))
            (should (clutch-mysql-conn-tls conn)))
        (mapc (lambda (buf)
                (when (buffer-live-p buf)
                  (kill-buffer buf)))
              buffers)))))

(ert-deftest clutch-mysql-test-connect-ssl-mode-disabled-disables-auto-tls-retry ()
  "ssl-mode disabled should keep MySQL 8 auth on plaintext and fail explicitly."
  (let ((auth-tls-flags nil)
        (buffers nil))
    (cl-letf (((symbol-function 'clutch-mysql--tls-available-p) (lambda () t))
              ((symbol-function 'clutch-mysql--open-connection)
               (lambda (_host _port _timeout)
                 (let ((buf (generate-new-buffer " *clutch-mysql-test-ssl-off*")))
                   (push buf buffers)
                   (cons (gensym "proc") buf))))
              ((symbol-function 'clutch-mysql--authenticate)
               (lambda (_conn _password tls)
                 (push tls auth-tls-flags)
                 (signal 'clutch-mysql-auth-error
                         '("caching_sha2_password full authentication requires TLS"))))
              ((symbol-function 'process-live-p) (lambda (_proc) t))
              ((symbol-function 'delete-process) (lambda (_proc) nil)))
      (unwind-protect
          (should-error
           (clutch-mysql-connect :host "127.0.0.1" :port 3306
                          :user "root" :password "pw"
                          :database "mysql" :ssl-mode 'disabled)
           :type 'clutch-mysql-auth-error)
        (should (equal auth-tls-flags '(nil)))
        (mapc (lambda (buf)
                (when (buffer-live-p buf)
                  (kill-buffer buf)))
              buffers)))))

(ert-deftest clutch-mysql-test-connect-ssl-mode-off-alias-disables-auto-tls-retry ()
  "ssl-mode off should remain accepted as an alias for disabled."
  (let ((auth-tls-flags nil)
        (buffers nil))
    (cl-letf (((symbol-function 'clutch-mysql--tls-available-p) (lambda () t))
              ((symbol-function 'clutch-mysql--open-connection)
               (lambda (_host _port _timeout)
                 (let ((buf (generate-new-buffer " *clutch-mysql-test-ssl-off*")))
                   (push buf buffers)
                   (cons (gensym "proc") buf))))
              ((symbol-function 'clutch-mysql--authenticate)
               (lambda (_conn _password tls)
                 (push tls auth-tls-flags)
                 (signal 'clutch-mysql-auth-error
                         '("caching_sha2_password full authentication requires TLS"))))
              ((symbol-function 'process-live-p) (lambda (_proc) t))
              ((symbol-function 'delete-process) (lambda (_proc) nil)))
      (unwind-protect
          (should-error
           (clutch-mysql-connect :host "127.0.0.1" :port 3306
                          :user "root" :password "pw"
                          :database "mysql" :ssl-mode 'off)
           :type 'clutch-mysql-auth-error)
        (should (equal auth-tls-flags '(nil)))
        (mapc (lambda (buf)
                (when (buffer-live-p buf)
                  (kill-buffer buf)))
              buffers)))))

(ert-deftest clutch-mysql-test-connect-explicit-tls-nil-disables-auto-tls-retry ()
  "Explicit :tls nil should force plaintext and fail without auto-retry."
  (let ((auth-tls-flags nil)
        (buffers nil))
    (cl-letf (((symbol-function 'clutch-mysql--tls-available-p) (lambda () t))
              ((symbol-function 'clutch-mysql--open-connection)
               (lambda (_host _port _timeout)
                 (let ((buf (generate-new-buffer " *clutch-mysql-test-tls-nil*")))
                   (push buf buffers)
                   (cons (gensym "proc") buf))))
              ((symbol-function 'clutch-mysql--authenticate)
               (lambda (_conn _password tls)
                 (push tls auth-tls-flags)
                 (signal 'clutch-mysql-auth-error
                         '("caching_sha2_password full authentication requires TLS"))))
              ((symbol-function 'process-live-p) (lambda (_proc) t))
              ((symbol-function 'delete-process) (lambda (_proc) nil)))
      (unwind-protect
          (should-error
           (clutch-mysql-connect :host "127.0.0.1" :port 3306
                          :user "root" :password "pw"
                          :database "mysql" :tls nil)
           :type 'clutch-mysql-auth-error)
        (should (equal auth-tls-flags '(nil)))
        (mapc (lambda (buf)
                (when (buffer-live-p buf)
                  (kill-buffer buf)))
              buffers)))))

(ert-deftest clutch-mysql-test-connect-rejects-conflicting-tls-and-ssl-mode ()
  "Explicit TLS should conflict with ssl-mode disabled."
  (should-error (clutch-mysql-connect :host "127.0.0.1" :port 3306
                               :user "root" :password "pw"
                               :database "mysql"
                               :tls t :ssl-mode 'disabled)
                :type 'clutch-mysql-connection-error))

(ert-deftest clutch-mysql-test-connect-rejects-unknown-ssl-mode ()
  "Unknown ssl-mode values should fail early."
  (should-error (clutch-mysql-connect :host "127.0.0.1" :port 3306
                               :user "root" :password "pw"
                               :database "mysql" :ssl-mode 'required)
                :type 'clutch-mysql-connection-error))

;;;; Live integration tests (require a running MySQL server)

(defmacro clutch-mysql-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live MySQL connection.
Skips if `clutch-mysql-test-password' is nil."
  (declare (indent 1))
  `(if (null clutch-mysql-test-password)
       (ert-skip "Set clutch-mysql-test-password to enable live tests")
     (let ((clutch-mysql-tls-verify-server nil))
       (let ((,var (clutch-mysql-connect :host clutch-mysql-test-host
                                  :port clutch-mysql-test-port
                                  :user clutch-mysql-test-user
                                  :password clutch-mysql-test-password
                                  :database clutch-mysql-test-database)))
         (unwind-protect
             (progn ,@body)
           (clutch-mysql-disconnect ,var))))))

(ert-deftest clutch-mysql-test-live-connect-disconnect ()
  :tags '(:clutch-mysql-live)
  "Test connecting and disconnecting."
  (clutch-mysql-test--with-conn conn
    (should (clutch-mysql-conn-p conn))
    (should (clutch-mysql-conn-server-version conn))
    (should (> (clutch-mysql-conn-connection-id conn) 0))))

(ert-deftest clutch-mysql-test-live-select ()
  :tags '(:clutch-mysql-live)
  "Test a simple SELECT query."
  (clutch-mysql-test--with-conn conn
    (let ((result (clutch-mysql-query conn "SELECT 1 AS num, 'hello' AS greeting")))
      (should (clutch-mysql-result-p result))
      (should (equal (clutch-mysql-result-status result) "OK"))
      (should (= (length (clutch-mysql-result-columns result)) 2))
      (should (= (length (clutch-mysql-result-rows result)) 1))
      (let ((row (car (clutch-mysql-result-rows result))))
        (should (= (car row) 1))
        (should (equal (cadr row) "hello"))))))

(ert-deftest clutch-mysql-test-live-multi-row ()
  :tags '(:clutch-mysql-live)
  "Test query returning multiple rows."
  (clutch-mysql-test--with-conn conn
    (let ((result (clutch-mysql-query conn "SELECT user, host FROM user LIMIT 5")))
      (should (clutch-mysql-result-p result))
      (should (>= (length (clutch-mysql-result-rows result)) 1)))))

(ert-deftest clutch-mysql-test-live-dml ()
  :tags '(:clutch-mysql-live)
  "Test INSERT/UPDATE/DELETE (DML) returning affected-rows."
  (clutch-mysql-test--with-conn conn
    ;; Create a temp table
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_test (id INT, val VARCHAR(50))")
    (let ((result (clutch-mysql-query conn "INSERT INTO _mysql_el_test VALUES (1, 'one'), (2, 'two')")))
      (should (= (clutch-mysql-result-affected-rows result) 2)))
    (let ((result (clutch-mysql-query conn "UPDATE _mysql_el_test SET val = 'updated' WHERE id = 1")))
      (should (= (clutch-mysql-result-affected-rows result) 1)))
    (let ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_test ORDER BY id")))
      (should (= (length (clutch-mysql-result-rows result)) 2))
      (should (equal (cadr (car (clutch-mysql-result-rows result))) "updated")))
    (let ((result (clutch-mysql-query conn "DELETE FROM _mysql_el_test")))
      (should (= (clutch-mysql-result-affected-rows result) 2)))))

(ert-deftest clutch-mysql-test-live-query-error ()
  :tags '(:clutch-mysql-live)
  "Test that a syntax error signals clutch-mysql-query-error."
  (clutch-mysql-test--with-conn conn
    (should-error (clutch-mysql-query conn "SELEC BAD SYNTAX")
                  :type 'clutch-mysql-query-error)))

(ert-deftest clutch-mysql-test-live-auth-failure ()
  :tags '(:clutch-mysql-live)
  "Test that wrong password signals clutch-mysql-auth-error."
  (if (null clutch-mysql-test-password)
      (ert-skip "Set clutch-mysql-test-password to enable live tests")
    (let ((clutch-mysql-tls-verify-server nil))
      (should-error (clutch-mysql-connect :host clutch-mysql-test-host
                                   :port clutch-mysql-test-port
                                   :user clutch-mysql-test-user
                                   :password "definitely-wrong-password"
                                   :database clutch-mysql-test-database)
                    :type 'clutch-mysql-auth-error))))

(ert-deftest clutch-mysql-test-live-null-values ()
  :tags '(:clutch-mysql-live)
  "Test that NULL values are returned as nil."
  (clutch-mysql-test--with-conn conn
    (let ((result (clutch-mysql-query conn "SELECT NULL AS n, 42 AS v")))
      (let ((row (car (clutch-mysql-result-rows result))))
        (should (null (car row)))
        (should (= (cadr row) 42))))))

(ert-deftest clutch-mysql-test-live-empty-result ()
  :tags '(:clutch-mysql-live)
  "Test a query that returns zero rows."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_empty (id INT)")
    (let ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_empty")))
      (should (= (length (clutch-mysql-result-rows result)) 0))
      (should (= (length (clutch-mysql-result-columns result)) 1)))))

;;;; Live tests — Extended type system

(ert-deftest clutch-mysql-test-live-date-time-types ()
  :tags '(:clutch-mysql-live)
  "Test DATE, TIME, DATETIME, TIMESTAMP column parsing."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_dt (
       d DATE, t TIME, dt DATETIME, ts TIMESTAMP NULL)")
    (clutch-mysql-query conn "INSERT INTO _mysql_el_dt VALUES
       ('2024-03-15', '13:45:30', '2024-03-15 13:45:30', '2024-03-15 13:45:30')")
    (let* ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_dt"))
           (row (car (clutch-mysql-result-rows result))))
      ;; DATE
      (should (equal (nth 0 row) '(:year 2024 :month 3 :day 15)))
      ;; TIME
      (should (equal (nth 1 row) '(:hours 13 :minutes 45 :seconds 30 :negative nil)))
      ;; DATETIME
      (should (= (plist-get (nth 2 row) :year) 2024))
      (should (= (plist-get (nth 2 row) :hours) 13))
      ;; TIMESTAMP
      (should (= (plist-get (nth 3 row) :year) 2024)))))

(ert-deftest clutch-mysql-test-live-bit-enum-set ()
  :tags '(:clutch-mysql-live)
  "Test BIT, ENUM, SET column parsing."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_bes (
       b BIT(8), e ENUM('a','b','c'), s SET('x','y','z'))")
    (clutch-mysql-query conn "INSERT INTO _mysql_el_bes VALUES (b'11111111', 'b', 'x,z')")
    (let* ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_bes"))
           (row (car (clutch-mysql-result-rows result))))
      ;; BIT(8) with all bits set = 255
      (should (= (nth 0 row) 255))
      ;; ENUM and SET are returned as strings
      (should (equal (nth 1 row) "b"))
      (should (equal (nth 2 row) "x,z")))))

;;;; Live tests — Convenience APIs

(ert-deftest clutch-mysql-test-live-with-connection ()
  :tags '(:clutch-mysql-live)
  "Test with-clutch-mysql-connection auto-close."
  (if (null clutch-mysql-test-password)
      (ert-skip "Set clutch-mysql-test-password to enable live tests")
    (let (saved-conn)
      (with-clutch-mysql-connection conn (:host clutch-mysql-test-host :port clutch-mysql-test-port
                                   :user clutch-mysql-test-user :password clutch-mysql-test-password
                                   :database clutch-mysql-test-database)
        (setq saved-conn conn)
        (should (clutch-mysql-conn-p conn))
        (should (process-live-p (clutch-mysql-conn-process conn))))
      ;; After the macro, the connection should be closed
      (should-not (process-live-p (clutch-mysql-conn-process saved-conn))))))

(ert-deftest clutch-mysql-test-live-transaction-commit ()
  :tags '(:clutch-mysql-live)
  "Test with-clutch-mysql-transaction commits on success."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_tx (id INT)")
    (with-clutch-mysql-transaction conn
      (clutch-mysql-query conn "INSERT INTO _mysql_el_tx VALUES (1)")
      (clutch-mysql-query conn "INSERT INTO _mysql_el_tx VALUES (2)"))
    (let ((result (clutch-mysql-query conn "SELECT COUNT(*) FROM _mysql_el_tx")))
      (should (= (car (car (clutch-mysql-result-rows result))) 2)))))

(ert-deftest clutch-mysql-test-live-transaction-rollback ()
  :tags '(:clutch-mysql-live)
  "Test with-clutch-mysql-transaction rolls back on error."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_tx2 (id INT)")
    (ignore-errors
      (with-clutch-mysql-transaction conn
        (clutch-mysql-query conn "INSERT INTO _mysql_el_tx2 VALUES (1)")
        (error "Intentional error")))
    (let ((result (clutch-mysql-query conn "SELECT COUNT(*) FROM _mysql_el_tx2")))
      (should (= (car (car (clutch-mysql-result-rows result))) 0)))))

(ert-deftest clutch-mysql-test-live-autocommit-toggle ()
  :tags '(:clutch-mysql-live)
  "Test session autocommit toggling and transaction state helpers."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_toggle (id INT)")
    (should (clutch-mysql-autocommit-p conn))
    (clutch-mysql-set-autocommit conn nil)
    (should-not (clutch-mysql-autocommit-p conn))
    (should-not (clutch-mysql-in-transaction-p conn))
    (clutch-mysql-query conn "INSERT INTO _mysql_el_toggle VALUES (1)")
    (should (clutch-mysql-in-transaction-p conn))
    (clutch-mysql-rollback conn)
    (should-not (clutch-mysql-in-transaction-p conn))
    (should-not (clutch-mysql-autocommit-p conn))
    (let ((result (clutch-mysql-query conn "SELECT COUNT(*) FROM _mysql_el_toggle")))
      (should (= (car (car (clutch-mysql-result-rows result))) 0)))
    (clutch-mysql-set-autocommit conn t)
    (should (clutch-mysql-autocommit-p conn))
    (should-not (clutch-mysql-in-transaction-p conn))))

(ert-deftest clutch-mysql-test-live-ping ()
  :tags '(:clutch-mysql-live)
  "Test COM_PING."
  (clutch-mysql-test--with-conn conn
    (should (eq (clutch-mysql-ping conn) t))))

;;;; Live tests — Prepared statements

(ert-deftest clutch-mysql-test-live-prepare-select ()
  :tags '(:clutch-mysql-live)
  "Test prepared SELECT with parameters."
  (clutch-mysql-test--with-conn conn
    (let ((stmt (clutch-mysql-prepare conn "SELECT ? + ? AS sum")))
      (should (clutch-mysql-stmt-p stmt))
      (should (= (clutch-mysql-stmt-param-count stmt) 2))
      (let ((result (clutch-mysql-execute stmt 10 20)))
        (should (= (length (clutch-mysql-result-rows result)) 1))
        (should (= (car (car (clutch-mysql-result-rows result))) 30)))
      (clutch-mysql-stmt-close stmt))))

(ert-deftest clutch-mysql-test-live-prepare-insert ()
  :tags '(:clutch-mysql-live)
  "Test prepared INSERT."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_ps (id INT, name VARCHAR(50))")
    (let ((stmt (clutch-mysql-prepare conn "INSERT INTO _mysql_el_ps VALUES (?, ?)")))
      (let ((result (clutch-mysql-execute stmt 1 "alice")))
        (should (= (clutch-mysql-result-affected-rows result) 1)))
      (let ((result (clutch-mysql-execute stmt 2 "bob")))
        (should (= (clutch-mysql-result-affected-rows result) 1)))
      (clutch-mysql-stmt-close stmt))
    (let ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_ps ORDER BY id")))
      (should (= (length (clutch-mysql-result-rows result)) 2))
      (should (equal (cadr (car (clutch-mysql-result-rows result))) "alice")))))

(ert-deftest clutch-mysql-test-live-prepare-null-params ()
  :tags '(:clutch-mysql-live)
  "Test prepared statement with NULL parameters."
  (clutch-mysql-test--with-conn conn
    (let ((stmt (clutch-mysql-prepare conn "SELECT ? AS v")))
      (let ((result (clutch-mysql-execute stmt nil)))
        (should (null (car (car (clutch-mysql-result-rows result))))))
      (clutch-mysql-stmt-close stmt))))

(ert-deftest clutch-mysql-test-live-prepare-string-params ()
  :tags '(:clutch-mysql-live)
  "Test prepared statement with string parameters."
  (clutch-mysql-test--with-conn conn
    (let ((stmt (clutch-mysql-prepare conn "SELECT CONCAT(?, ?) AS s")))
      (let ((result (clutch-mysql-execute stmt "hello" " world")))
        (should (equal (car (car (clutch-mysql-result-rows result))) "hello world")))
      (clutch-mysql-stmt-close stmt))))

(ert-deftest clutch-mysql-test-live-prepare-multiple-executions ()
  :tags '(:clutch-mysql-live)
  "Test multiple executions of the same prepared statement."
  (clutch-mysql-test--with-conn conn
    (let ((stmt (clutch-mysql-prepare conn "SELECT ? * 2 AS doubled")))
      (dotimes (i 5)
        (let ((result (clutch-mysql-execute stmt (1+ i))))
          (should (= (car (car (clutch-mysql-result-rows result))) (* (1+ i) 2)))))
      (clutch-mysql-stmt-close stmt))))

(ert-deftest clutch-mysql-test-live-prepare-binary-types ()
  :tags '(:clutch-mysql-live)
  "Test binary protocol type round-trips."
  (clutch-mysql-test--with-conn conn
    (clutch-mysql-query conn "CREATE TEMPORARY TABLE _mysql_el_bt (
       i INT, f DOUBLE, s VARCHAR(100), d DATE, dt DATETIME)")
    (let ((stmt (clutch-mysql-prepare conn
                  "INSERT INTO _mysql_el_bt VALUES (?, ?, ?, '2024-03-15', '2024-03-15 10:30:00')")))
      (clutch-mysql-execute stmt 42 3.14 "hello")
      (clutch-mysql-stmt-close stmt))
    (let ((result (clutch-mysql-query conn "SELECT * FROM _mysql_el_bt")))
      (let ((row (car (clutch-mysql-result-rows result))))
        (should (= (nth 0 row) 42))
        ;; Float comes back via text protocol
        (should (< (abs (- (nth 1 row) 3.14)) 0.001))
        (should (equal (nth 2 row) "hello"))))))

;;;; Live tests — TLS (require clutch-mysql-test-tls-enabled)

(defmacro clutch-mysql-test--with-tls-conn (var &rest body)
  "Execute BODY with VAR bound to a TLS MySQL connection.
Skips unless both `clutch-mysql-test-password' and
`clutch-mysql-test-tls-enabled' are set."
  (declare (indent 1))
  `(if (or (null clutch-mysql-test-password) (null clutch-mysql-test-tls-enabled))
       (ert-skip "Set clutch-mysql-test-password and clutch-mysql-test-tls-enabled for TLS tests")
     (let ((clutch-mysql-tls-verify-server nil))
       (let ((,var (clutch-mysql-connect :host clutch-mysql-test-host
                                  :port clutch-mysql-test-port
                                  :user clutch-mysql-test-user
                                  :password clutch-mysql-test-password
                                  :database clutch-mysql-test-database
                                  :tls t)))
         (unwind-protect
             (progn ,@body)
           (clutch-mysql-disconnect ,var))))))

(ert-deftest clutch-mysql-test-live-tls-connect ()
  :tags '(:clutch-mysql-live :clutch-mysql-tls)
  "Test TLS connection and verify encryption is active."
  (clutch-mysql-test--with-tls-conn conn
    (should (clutch-mysql-conn-tls conn))
    (let* ((result (clutch-mysql-query conn "SHOW STATUS LIKE 'Ssl_cipher'"))
           (cipher (cadr (car (clutch-mysql-result-rows result)))))
      (should (stringp cipher))
      (should (not (string-empty-p cipher))))))

(ert-deftest clutch-mysql-test-live-tls-query ()
  :tags '(:clutch-mysql-live :clutch-mysql-tls)
  "Test query execution over TLS."
  (clutch-mysql-test--with-tls-conn conn
    (let ((result (clutch-mysql-query conn "SELECT 42 AS v, 'tls-ok' AS msg")))
      (let ((row (car (clutch-mysql-result-rows result))))
        (should (= (car row) 42))
        (should (equal (cadr row) "tls-ok"))))))

(ert-deftest clutch-mysql-test-live-tls-prepared-statement ()
  :tags '(:clutch-mysql-live :clutch-mysql-tls)
  "Test prepared statements over TLS."
  (clutch-mysql-test--with-tls-conn conn
    (let ((stmt (clutch-mysql-prepare conn "SELECT ? + 1 AS v")))
      (let ((result (clutch-mysql-execute stmt 99)))
        (should (= (car (car (clutch-mysql-result-rows result))) 100)))
      (clutch-mysql-stmt-close stmt))))

(ert-deftest clutch-mysql-test-live-tls-caching-sha2-full-auth ()
  :tags '(:clutch-mysql-live :clutch-mysql-tls)
  "Test caching_sha2_password full auth over TLS (auth switch path)."
  (if (or (null clutch-mysql-test-password) (null clutch-mysql-test-tls-enabled))
      (ert-skip "Set clutch-mysql-test-password and clutch-mysql-test-tls-enabled for TLS tests")
    (let ((clutch-mysql-tls-verify-server nil))
      ;; Create a caching_sha2_password user and flush to force full auth
      (let ((admin (clutch-mysql-connect :host clutch-mysql-test-host
                                  :port clutch-mysql-test-port
                                  :user clutch-mysql-test-user
                                  :password clutch-mysql-test-password
                                  :database clutch-mysql-test-database
                                  :tls t)))
        (unwind-protect
            (progn
              (condition-case nil
                  (clutch-mysql-query admin "DROP USER '_mysql_el_sha2test'@'%'")
                (clutch-mysql-query-error nil))
              (condition-case err
                  (clutch-mysql-query admin
                    "CREATE USER '_mysql_el_sha2test'@'%' IDENTIFIED WITH caching_sha2_password BY 'testpw'")
                (clutch-mysql-query-error
                 (ert-skip (format "Server does not support caching_sha2_password: %s"
                                   (cadr err)))))
              (clutch-mysql-query admin "GRANT ALL ON *.* TO '_mysql_el_sha2test'@'%'")
              (clutch-mysql-query admin "FLUSH PRIVILEGES"))
          (clutch-mysql-disconnect admin)))
      ;; Connect as the new user over TLS (full auth required)
      (let ((conn (clutch-mysql-connect :host clutch-mysql-test-host
                                 :port clutch-mysql-test-port
                                 :user "_mysql_el_sha2test"
                                 :password "testpw"
                                 :database clutch-mysql-test-database
                                 :tls t)))
        (unwind-protect
            (progn
              (should (clutch-mysql-conn-tls conn))
              (let ((result (clutch-mysql-query conn "SELECT CURRENT_USER()")))
                (should (string-prefix-p "_mysql_el_sha2test"
                                         (car (car (clutch-mysql-result-rows result)))))))
          (clutch-mysql-disconnect conn)))
      ;; Cleanup
      (let ((admin (clutch-mysql-connect :host clutch-mysql-test-host
                                  :port clutch-mysql-test-port
                                  :user clutch-mysql-test-user
                                  :password clutch-mysql-test-password
                                  :database clutch-mysql-test-database
                                  :tls t)))
        (unwind-protect
            (condition-case nil
                (clutch-mysql-query admin "DROP USER '_mysql_el_sha2test'@'%'")
              (clutch-mysql-query-error nil))
          (clutch-mysql-disconnect admin))))))

;;; clutch-mysql-test.el ends here
