;;; data-lens-test.el --- Tests for data-lens UI layer -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the data-lens user interface layer.
;;
;; Unit tests run without a database server.
;; Live tests require a running database:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;
;; Run unit tests:
;;   emacs -batch -L .. -l ert -l data-lens-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'data-lens-db)
(require 'data-lens)

;;;; Test configuration

(defvar data-lens-test-backend 'mysql)
(defvar data-lens-test-host "127.0.0.1")
(defvar data-lens-test-port 3306)
(defvar data-lens-test-user "root")
(defvar data-lens-test-password nil)
(defvar data-lens-test-database "mysql")

;;;; Unit tests — value formatting

(ert-deftest data-lens-test-format-value-nil ()
  "Test formatting of NULL values."
  (should (equal (data-lens--format-value nil) "NULL")))

(ert-deftest data-lens-test-format-value-string ()
  "Test formatting of string values."
  (should (equal (data-lens--format-value "hello") "hello"))
  (should (equal (data-lens--format-value "") "")))

(ert-deftest data-lens-test-format-value-number ()
  "Test formatting of numeric values."
  (should (equal (data-lens--format-value 42) "42"))
  (should (equal (data-lens--format-value -1) "-1"))
  (should (equal (data-lens--format-value 3.14) "3.14")))

(ert-deftest data-lens-test-format-value-date ()
  "Test formatting of date plist values."
  (let ((result (data-lens--format-value '(:year 2024 :month 3 :day 15))))
    (should (stringp result))
    (should (string-match-p "2024" result))
    (should (string-match-p "3" result))
    (should (string-match-p "15" result))))

(ert-deftest data-lens-test-format-value-time ()
  "Test formatting of time plist values."
  (let ((result (data-lens--format-value '(:hours 13 :minutes 45 :seconds 30 :negative nil))))
    (should (stringp result))
    (should (string-match-p "13" result))
    (should (string-match-p "45" result))))

(ert-deftest data-lens-test-format-value-datetime ()
  "Test formatting of datetime plist values."
  (let ((result (data-lens--format-value
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))
    (should (stringp result))
    (should (string-match-p "2024" result))
    (should (string-match-p "13" result))))

;;;; Unit tests — cell truncation

(ert-deftest data-lens-test-truncate-cell ()
  "Test cell value truncation."
  ;; Short string — no truncation
  (should (equal (data-lens--truncate-cell "hello" 10) "hello"))
  ;; Exact length — no truncation
  (should (equal (data-lens--truncate-cell "hello" 5) "hello"))
  ;; Long string — truncated with ellipsis
  (let ((result (data-lens--truncate-cell "hello world" 8)))
    (should (= (length result) 8))
    (should (string-suffix-p "…" result))))

;;;; Unit tests — string padding

(ert-deftest data-lens-test-string-pad ()
  "Test string padding."
  ;; Left-align (default)
  (should (equal (data-lens--string-pad "hi" 5) "hi   "))
  ;; Right-align
  (should (equal (data-lens--string-pad "hi" 5 t) "   hi"))
  ;; String longer than width — no padding
  (should (equal (data-lens--string-pad "hello" 3) "hello")))

;;;; Unit tests — column type detection

(ert-deftest data-lens-test-numeric-type-p ()
  "Test numeric column type detection."
  (should (data-lens--numeric-type-p '(:name "id" :type-category numeric)))
  (should-not (data-lens--numeric-type-p '(:name "name" :type-category text)))
  (should-not (data-lens--numeric-type-p '(:name "data" :type-category json))))

(ert-deftest data-lens-test-long-field-type-p ()
  "Test long field type detection."
  (should (data-lens--long-field-type-p '(:name "content" :type-category blob)))
  (should (data-lens--long-field-type-p '(:name "data" :type-category json)))
  (should-not (data-lens--long-field-type-p '(:name "id" :type-category numeric)))
  (should-not (data-lens--long-field-type-p '(:name "name" :type-category text))))

;;;; Unit tests — column name extraction

(ert-deftest data-lens-test-column-names ()
  "Test column name extraction from column definitions."
  (let ((columns (list '(:name "id" :type-category numeric)
                       '(:name "name" :type-category text)
                       '(:name "data" :type-category json))))
    (let ((names (data-lens--column-names columns)))
      (should (equal names '("id" "name" "data"))))))

;;;; Unit tests — column width computation

(ert-deftest data-lens-test-compute-column-widths ()
  "Test column width computation."
  (let* ((col-names '("id" "name" "email"))
         (rows '((1 "alice" "alice@example.com")
                 (2 "bob" "bob@example.com")))
         (columns '((:name "id" :type-category numeric)
                    (:name "name" :type-category text)
                    (:name "email" :type-category text)))
         (widths (data-lens--compute-column-widths col-names rows columns)))
    (should (vectorp widths))
    (should (= (length widths) 3))
    ;; id: max(2, 1) = 2
    (should (>= (aref widths 0) 2))
    ;; name: max(4, 5) = 5 (alice)
    (should (>= (aref widths 1) 5))
    ;; email: max(5, 17) = 17 (alice@example.com)
    (should (>= (aref widths 2) 5))))

(ert-deftest data-lens-test-compute-column-widths-with-max ()
  "Test column width computation respects max width."
  (let* ((data-lens-column-width-max 10)
         (col-names '("description"))
         (rows '(("this is a very long description that exceeds the maximum width")))
         (columns '((:name "description" :type-category text)))
         (widths (data-lens--compute-column-widths col-names rows columns)))
    ;; Should be capped at max width
    (should (<= (aref widths 0) data-lens-column-width-max))))

;;;; Unit tests — column page computation

(ert-deftest data-lens-test-compute-column-pages-single ()
  "Test column pages when all columns fit on one page."
  (let ((widths [5 10 8]))  ; Total ~30 chars
    (let ((pages (data-lens--compute-column-pages widths nil 80)))
      ;; All columns should fit on page 1
      (should (= (length pages) 1))
      (should (equal (append (aref pages 0) nil) '(0 1 2))))))

(ert-deftest data-lens-test-compute-column-pages-multiple ()
  "Test column pages when columns span multiple pages."
  (let ((widths [30 30 30 30]))  ; Each col + borders > 30
    (let ((pages (data-lens--compute-column-pages widths nil 70)))
      ;; Should create multiple pages
      (should (> (length pages) 1)))))

(ert-deftest data-lens-test-compute-column-pages-with-pinned ()
  "Test column pages with pinned columns."
  (let ((widths [5 30 30 30]))
    (let ((pages (data-lens--compute-column-pages widths '(0) 80)))
      ;; Pinned column (0) should not appear in pages
      (dolist (page (append pages nil))
        (should-not (member 0 (append page nil))))
      ;; Other columns should be distributed
      (should (> (length pages) 0)))))

;;;; Unit tests — SQL query detection

(ert-deftest data-lens-test-sql-has-limit-p ()
  "Test LIMIT clause detection."
  (should (data-lens--sql-has-limit-p "SELECT * FROM t LIMIT 10"))
  (should (data-lens--sql-has-limit-p "select * from t limit 10"))
  (should (data-lens--sql-has-limit-p "SELECT * FROM t WHERE x=1 LIMIT 5 OFFSET 10"))
  (should-not (data-lens--sql-has-limit-p "SELECT * FROM t"))
  (should-not (data-lens--sql-has-limit-p "SELECT * FROM t WHERE limitation = 1")))

(ert-deftest data-lens-test-strip-leading-comments ()
  "Test stripping leading SQL comments."
  (should (equal (data-lens--strip-leading-comments "SELECT 1") "SELECT 1"))
  (should (equal (data-lens--strip-leading-comments "  SELECT 1") "SELECT 1"))
  ;; Single-line comment
  (should (equal (data-lens--strip-leading-comments "-- hello\nSELECT 1")
                 "SELECT 1"))
  ;; Multiple single-line comments
  (should (equal (data-lens--strip-leading-comments "-- a\n-- b\nSELECT 1")
                 "SELECT 1"))
  ;; Multi-line comment
  (should (equal (data-lens--strip-leading-comments "/* foo */SELECT 1")
                 "SELECT 1"))
  ;; Mixed
  (should (equal (data-lens--strip-leading-comments "/* foo */\n-- bar\nSELECT 1")
                 "SELECT 1"))
  ;; Only comments
  (should (equal (data-lens--strip-leading-comments "-- nothing") "")))

(ert-deftest data-lens-test-destructive-query-p ()
  "Test destructive query detection."
  (should (data-lens--destructive-query-p "DROP TABLE users"))
  (should (data-lens--destructive-query-p "TRUNCATE users"))
  (should (data-lens--destructive-query-p "DELETE FROM users"))
  (should (data-lens--destructive-query-p "delete from users where id=1"))
  ;; With leading comment
  (should (data-lens--destructive-query-p "-- cleanup\nDROP TABLE users"))
  (should-not (data-lens--destructive-query-p "SELECT * FROM users"))
  (should-not (data-lens--destructive-query-p "UPDATE users SET name='x'")))

(ert-deftest data-lens-test-select-query-p ()
  "Test SELECT query detection."
  (should (data-lens--select-query-p "SELECT * FROM users"))
  (should (data-lens--select-query-p "select id from users"))
  (should (data-lens--select-query-p "  SELECT * FROM t"))
  (should (data-lens--select-query-p "WITH cte AS (SELECT 1) SELECT * FROM cte"))
  ;; With leading comments — previously broke SELECT detection
  (should (data-lens--select-query-p "-- get users\nSELECT * FROM users"))
  (should (data-lens--select-query-p "/* all */\nSELECT * FROM users"))
  (should (data-lens--select-query-p "-- a\n-- b\nSELECT 1"))
  ;; Note: SHOW/DESCRIBE/EXPLAIN are not recognized as SELECT
  (should-not (data-lens--select-query-p "SHOW TABLES"))
  (should-not (data-lens--select-query-p "DESCRIBE users"))
  (should-not (data-lens--select-query-p "EXPLAIN SELECT * FROM t"))
  (should-not (data-lens--select-query-p "INSERT INTO users VALUES (1)"))
  (should-not (data-lens--select-query-p "UPDATE users SET name='x'")))

;;;; Unit tests — value to literal conversion

(ert-deftest data-lens-test-value-to-literal-nil ()
  "Test NULL literal conversion."
  (should (equal (data-lens--value-to-literal nil) "NULL")))

(ert-deftest data-lens-test-value-to-literal-number ()
  "Test numeric literal conversion."
  (should (equal (data-lens--value-to-literal 42) "42"))
  (should (string-match-p "3\\.14" (data-lens--value-to-literal 3.14)))
  (should (equal (data-lens--value-to-literal -1) "-1")))

(ert-deftest data-lens-test-value-to-literal-string ()
  "Test string literal conversion (requires connection)."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  ;; String escaping requires a connection
  (let ((data-lens-connection (make-mysql-conn :host "localhost")))
    (let ((result (data-lens--value-to-literal "hello")))
      (should (stringp result))
      (should (string-prefix-p "'" result)))
    (let ((result (data-lens--value-to-literal "it's")))
      (should (string-match-p "\\\\'" result)))))

;;;; Unit tests — connection key

(ert-deftest data-lens-test-connection-key ()
  "Test connection key generation."
  (require 'data-lens-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost" :port 3306
                                :user "root" :database "test")))
    (let ((key (data-lens--connection-key conn)))
      (should (stringp key))
      (should (string-match-p "localhost" key))
      (should (string-match-p "3306" key))
      (should (string-match-p "root" key))
      (should (string-match-p "test" key)))))

;;;; Unit tests — WHERE filter application

(ert-deftest data-lens-test-apply-where ()
  "Test WHERE filter application to SQL."
  ;; Simple case
  (should (string-match-p "WHERE.*id = 1"
                          (data-lens--apply-where "SELECT * FROM t" "id = 1")))
  ;; Query already has WHERE
  (let ((result (data-lens--apply-where "SELECT * FROM t WHERE x > 0" "id = 1")))
    (should (string-match-p "WHERE" result))
    (should (string-match-p "id = 1" result))))

;;;; Unit tests — separator rendering

(ert-deftest data-lens-test-render-separator ()
  "Test table separator line rendering."
  (let ((visible-cols '(0 1 2))
        (widths [5 10 8]))
    (let ((sep (data-lens--render-separator visible-cols widths 'top)))
      (should (stringp sep))
      (should (> (length sep) 0)))))

;;;; Unit tests — row range formatting

(ert-deftest data-lens-test-footer-row-range ()
  "Test row range formatting for footer."
  ;; Normal range — includes "rows" prefix and styling
  (let ((result (data-lens--footer-row-range 1 10 100)))
    (should (stringp result))
    (should (string-match-p "1-10" result))
    (should (string-match-p "100" result))
    (should (string-match-p "rows" result)))
  ;; Last page
  (let ((result (data-lens--footer-row-range 91 100 100)))
    (should (string-match-p "91-100" result)))
  ;; Unknown total — shows "?"
  (let ((result (data-lens--footer-row-range 1 10 nil)))
    (should (string-match-p "1-10" result))
    (should (string-match-p "\\?" result))))

;;;; Unit tests — page indicator

(ert-deftest data-lens-test-footer-page-indicator ()
  "Test page indicator formatting."
  ;; Page 0, 10 rows per page, 100 total = page 1 of 10
  (let ((result (data-lens--footer-page-indicator 0 10 100)))
    (should (stringp result))
    (should (string-match-p "1" result)))
  ;; Unknown total
  (let ((result (data-lens--footer-page-indicator 0 10 nil)))
    (should (stringp result))))

;;;; Live integration tests

(defmacro data-lens-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live connection.
Skips if `data-lens-test-password' is nil."
  (declare (indent 1))
  `(if (null data-lens-test-password)
       (ert-skip "Set data-lens-test-password to enable live tests")
     (let ((,var (data-lens-db-connect
                  data-lens-test-backend
                  (list :host data-lens-test-host
                        :port data-lens-test-port
                        :user data-lens-test-user
                        :password data-lens-test-password
                        :database data-lens-test-database))))
       (unwind-protect
           (progn ,@body)
         (data-lens-db-disconnect ,var)))))

(ert-deftest data-lens-test-live-display-select-result ()
  :tags '(:data-lens-live)
  "Test displaying a SELECT result."
  (data-lens-test--with-conn conn
    (let* ((result (data-lens-db-query conn "SELECT 1 AS id, 'test' AS name"))
           (columns (data-lens-db-result-columns result))
           (rows (data-lens-db-result-rows result))
           (col-names (data-lens--column-names columns)))
      ;; Setup buffer state
      (with-temp-buffer
        (setq-local data-lens-connection conn)
        (setq-local data-lens--result-columns col-names)
        (setq-local data-lens--result-column-defs columns)
        (setq-local data-lens--result-rows rows)
        (setq-local data-lens--display-offset (length rows))
        (setq-local data-lens--pending-edits nil)
        (setq-local data-lens--fk-info nil)
        (setq-local data-lens--where-filter nil)
        (setq-local data-lens--current-col-page 0)
        (setq-local data-lens--pinned-columns nil)
        (let ((widths (data-lens--compute-column-widths col-names rows columns)))
          (setq-local data-lens--column-widths widths)
          (setq-local data-lens--column-pages
                      (data-lens--compute-column-pages widths nil 80)))
        ;; Render
        (data-lens--render-result)
        ;; Verify buffer has content
        (should (> (buffer-size) 0))
        ;; Column names are in header-line/tab-line, not buffer text.
        ;; Verify data values appear in the rendered table.
        (should (string-match-p "test" (buffer-string)))))))

(ert-deftest data-lens-test-live-schema-introspection ()
  :tags '(:data-lens-live)
  "Test schema introspection functions."
  (data-lens-test--with-conn conn
    ;; List tables
    (let ((tables (data-lens-db-list-tables conn)))
      (should (listp tables))
      (should (> (length tables) 0)))
    ;; List columns
    (let ((columns (data-lens-db-list-columns conn "user")))
      (should (listp columns))
      (should (> (length columns) 0)))
    ;; Primary keys
    (let ((pk-cols (data-lens-db-primary-key-columns conn "user")))
      (should (listp pk-cols)))))

(ert-deftest data-lens-test-live-paged-sql-building ()
  :tags '(:data-lens-live)
  "Test paged SQL query building."
  (data-lens-test--with-conn conn
    (let ((data-lens-connection conn)
          (base-sql "SELECT * FROM user"))
      ;; Build paged SQL
      (let ((paged (data-lens-db-build-paged-sql conn base-sql 0 10)))
        (should (stringp paged))
        (should (string-match-p "LIMIT" paged)))
      ;; With order
      (let ((paged (data-lens-db-build-paged-sql conn base-sql 0 10 '("Host" . "ASC"))))
        (should (string-match-p "ORDER BY" paged))))))

(provide 'data-lens-test)
;;; data-lens-test.el ends here
