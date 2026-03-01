;;; clutch-test.el --- Tests for clutch UI layer -*- lexical-binding: t; -*-

;;; Commentary:

;; ERT tests for the clutch user interface layer.
;;
;; Unit tests run without a database server.
;; Live tests require a running database:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;
;; Run unit tests:
;;   emacs -batch -L .. -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

(require 'ert)
(require 'clutch-db)
(require 'clutch)

;;;; Test configuration

(defvar clutch-test-backend 'mysql)
(defvar clutch-test-host "127.0.0.1")
(defvar clutch-test-port 3306)
(defvar clutch-test-user "root")
(defvar clutch-test-password nil)
(defvar clutch-test-database "mysql")

;;;; Unit tests — value formatting

(ert-deftest clutch-test-format-value-nil ()
  "Test formatting of NULL values."
  (should (equal (clutch--format-value nil) "NULL")))

(ert-deftest clutch-test-format-value-string ()
  "Test formatting of string values."
  (should (equal (clutch--format-value "hello") "hello"))
  (should (equal (clutch--format-value "") "")))

(ert-deftest clutch-test-format-value-number ()
  "Test formatting of numeric values."
  (should (equal (clutch--format-value 42) "42"))
  (should (equal (clutch--format-value -1) "-1"))
  (should (equal (clutch--format-value 3.14) "3.14")))

(ert-deftest clutch-test-format-value-date ()
  "Test formatting of date plist values."
  (let ((result (clutch--format-value '(:year 2024 :month 3 :day 15))))
    (should (stringp result))
    (should (string-match-p "2024" result))
    (should (string-match-p "3" result))
    (should (string-match-p "15" result))))

(ert-deftest clutch-test-format-value-time ()
  "Test formatting of time plist values."
  (let ((result (clutch--format-value '(:hours 13 :minutes 45 :seconds 30 :negative nil))))
    (should (stringp result))
    (should (string-match-p "13" result))
    (should (string-match-p "45" result))))

(ert-deftest clutch-test-format-value-datetime ()
  "Test formatting of datetime plist values."
  (let ((result (clutch--format-value
                 '(:year 2024 :month 3 :day 15
                   :hours 13 :minutes 45 :seconds 30))))
    (should (stringp result))
    (should (string-match-p "2024" result))
    (should (string-match-p "13" result))))

;;;; Unit tests — cell truncation

(ert-deftest clutch-test-truncate-cell ()
  "Test cell value truncation."
  ;; Short string — no truncation
  (should (equal (clutch--truncate-cell "hello" 10) "hello"))
  ;; Exact length — no truncation
  (should (equal (clutch--truncate-cell "hello" 5) "hello"))
  ;; Long string — truncated with ellipsis
  (let ((result (clutch--truncate-cell "hello world" 8)))
    (should (= (length result) 8))
    (should (string-suffix-p "…" result))))

;;;; Unit tests — string padding

(ert-deftest clutch-test-string-pad ()
  "Test string padding."
  ;; Left-align (default)
  (should (equal (clutch--string-pad "hi" 5) "hi   "))
  ;; Right-align
  (should (equal (clutch--string-pad "hi" 5 t) "   hi"))
  ;; String longer than width — no padding
  (should (equal (clutch--string-pad "hello" 3) "hello")))

;;;; Unit tests — column type detection

(ert-deftest clutch-test-numeric-type-p ()
  "Test numeric column type detection."
  (should (clutch--numeric-type-p '(:name "id" :type-category numeric)))
  (should-not (clutch--numeric-type-p '(:name "name" :type-category text)))
  (should-not (clutch--numeric-type-p '(:name "data" :type-category json))))

(ert-deftest clutch-test-long-field-type-p ()
  "Test long field type detection."
  (should (clutch--long-field-type-p '(:name "content" :type-category blob)))
  (should (clutch--long-field-type-p '(:name "data" :type-category json)))
  (should-not (clutch--long-field-type-p '(:name "id" :type-category numeric)))
  (should-not (clutch--long-field-type-p '(:name "name" :type-category text))))

;;;; Unit tests — column name extraction

(ert-deftest clutch-test-column-names ()
  "Test column name extraction from column definitions."
  (let ((columns (list '(:name "id" :type-category numeric)
                       '(:name "name" :type-category text)
                       '(:name "data" :type-category json))))
    (let ((names (clutch--column-names columns)))
      (should (equal names '("id" "name" "data"))))))

;;;; Unit tests — column width computation

(ert-deftest clutch-test-compute-column-widths ()
  "Test column width computation."
  (let* ((col-names '("id" "name" "email"))
         (rows '((1 "alice" "alice@example.com")
                 (2 "bob" "bob@example.com")))
         (columns '((:name "id" :type-category numeric)
                    (:name "name" :type-category text)
                    (:name "email" :type-category text)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    (should (vectorp widths))
    (should (= (length widths) 3))
    ;; id: max(2, 1) = 2
    (should (>= (aref widths 0) 2))
    ;; name: max(4, 5) = 5 (alice)
    (should (>= (aref widths 1) 5))
    ;; email: max(5, 17) = 17 (alice@example.com)
    (should (>= (aref widths 2) 5))))

(ert-deftest clutch-test-compute-column-widths-with-max ()
  "Test column width computation respects max width."
  (let* ((clutch-column-width-max 10)
         (col-names '("description"))
         (rows '(("this is a very long description that exceeds the maximum width")))
         (columns '((:name "description" :type-category text)))
         (widths (clutch--compute-column-widths col-names rows columns)))
    ;; Should be capped at max width
    (should (<= (aref widths 0) clutch-column-width-max))))

;;;; Unit tests — column page computation

(ert-deftest clutch-test-compute-column-pages-single ()
  "Test column pages when all columns fit on one page."
  (let ((widths [5 10 8]))  ; Total ~30 chars
    (let ((pages (clutch--compute-column-pages widths nil 80)))
      ;; All columns should fit on page 1
      (should (= (length pages) 1))
      (should (equal (append (aref pages 0) nil) '(0 1 2))))))

(ert-deftest clutch-test-compute-column-pages-multiple ()
  "Test column pages when columns span multiple pages."
  (let ((widths [30 30 30 30]))  ; Each col + borders > 30
    (let ((pages (clutch--compute-column-pages widths nil 70)))
      ;; Should create multiple pages
      (should (> (length pages) 1)))))

(ert-deftest clutch-test-compute-column-pages-with-pinned ()
  "Test column pages with pinned columns."
  (let ((widths [5 30 30 30]))
    (let ((pages (clutch--compute-column-pages widths '(0) 80)))
      ;; Pinned column (0) should not appear in pages
      (dolist (page (append pages nil))
        (should-not (member 0 (append page nil))))
      ;; Other columns should be distributed
      (should (> (length pages) 0)))))

;;;; Unit tests — SQL query detection

(ert-deftest clutch-test-sql-has-limit-p ()
  "Test LIMIT clause detection."
  (should (clutch--sql-has-limit-p "SELECT * FROM t LIMIT 10"))
  (should (clutch--sql-has-limit-p "select * from t limit 10"))
  (should (clutch--sql-has-limit-p "SELECT * FROM t WHERE x=1 LIMIT 5 OFFSET 10"))
  (should (clutch--sql-has-limit-p
           "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 20"))
  (should-not (clutch--sql-has-limit-p
               "SELECT * FROM (SELECT * FROM t LIMIT 5) AS s"))
  (should-not (clutch--sql-has-limit-p
               "(SELECT id FROM a LIMIT 1) UNION ALL (SELECT id FROM b)"))
  (should-not (clutch--sql-has-limit-p
               "WITH x AS (SELECT * FROM t LIMIT 3) SELECT * FROM x"))
  (should-not (clutch--sql-has-limit-p "SELECT * FROM t"))
  (should-not (clutch--sql-has-limit-p "SELECT * FROM t WHERE limitation = 1")))

(ert-deftest clutch-test-collect-all-export-rows-paged ()
  "Collect all export rows by paging when base query has no top-level LIMIT."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--base-query "SELECT id FROM t")
    (setq-local clutch--last-query "SELECT id FROM t")
    (setq-local clutch--where-filter nil)
    (setq-local clutch--order-by nil)
    (let ((clutch-result-max-rows 2))
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--build-paged-sql)
                 (lambda (_sql page-num _page-size _order-by)
                   (format "SELECT id FROM t -- page:%d" page-num)))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn sql)
                   (let ((rows (cond ((string-match-p "page:0\\'" sql) '((1) (2)))
                                     ((string-match-p "page:1\\'" sql) '((3)))
                                     (t nil))))
                     (make-clutch-db-result :rows rows)))))
        (should (equal (clutch-result--collect-all-export-rows)
                       '((1) (2) (3))))))))

(ert-deftest clutch-test-collect-all-export-rows-with-top-level-limit ()
  "Collect export rows with top-level LIMIT via single query."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--base-query "SELECT id FROM t LIMIT 2")
    (setq-local clutch--last-query "SELECT id FROM t LIMIT 2")
    (setq-local clutch--where-filter nil)
    (let ((calls 0))
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--build-paged-sql)
                 (lambda (&rest _args)
                   (error "should not paginate")))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (cl-incf calls)
                   (make-clutch-db-result :rows '((1) (2))))))
        (should (equal (clutch-result--collect-all-export-rows) '((1) (2))))
        (should (= calls 1))))))

(ert-deftest clutch-test-export-command-dispatches-copy ()
  "Export command should dispatch to all-rows clipboard export."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "copy"))
                ((symbol-function 'clutch--export-csv-all-to-clipboard)
                 (lambda () (setq called 'copy)))
                ((symbol-function 'clutch--export-csv-all-file)
                 (lambda () (setq called 'file))))
        (clutch-result-export)
        (should (eq called 'copy))))))

(ert-deftest clutch-test-export-command-dispatches-file ()
  "Export command should dispatch to all-rows file export."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "file"))
                ((symbol-function 'clutch--export-csv-all-to-clipboard)
                 (lambda () (setq called 'copy)))
                ((symbol-function 'clutch--export-csv-all-file)
                 (lambda () (setq called 'file))))
        (clutch-result-export)
        (should (eq called 'file))))))

(ert-deftest clutch-test-csv-content-escaping ()
  "CSV content should include header and escaped values."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (let ((csv (clutch--csv-content '((1 "a,b") (2 "x\"y")))))
      (should (string-match-p "^id,name\n" csv))
      (should (string-match-p "1,\"a,b\"" csv))
      (should (string-match-p "2,\"x\"\"y\"" csv)))))

(ert-deftest clutch-test-strip-leading-comments ()
  "Test stripping leading SQL comments."
  (should (equal (clutch--strip-leading-comments "SELECT 1") "SELECT 1"))
  (should (equal (clutch--strip-leading-comments "  SELECT 1") "SELECT 1"))
  ;; Single-line comment
  (should (equal (clutch--strip-leading-comments "-- hello\nSELECT 1")
                 "SELECT 1"))
  ;; Multiple single-line comments
  (should (equal (clutch--strip-leading-comments "-- a\n-- b\nSELECT 1")
                 "SELECT 1"))
  ;; Multi-line comment
  (should (equal (clutch--strip-leading-comments "/* foo */SELECT 1")
                 "SELECT 1"))
  ;; Mixed
  (should (equal (clutch--strip-leading-comments "/* foo */\n-- bar\nSELECT 1")
                 "SELECT 1"))
  ;; Only comments
  (should (equal (clutch--strip-leading-comments "-- nothing") "")))

(ert-deftest clutch-test-selected-row-indices-priority ()
  "Selection priority should be region > current row."
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () t))
              ((symbol-function 'clutch-result--rows-in-region)
               (lambda (_beg _end) '(2 3)))
              ((symbol-function 'clutch-result--row-idx-at-line)
               (lambda () 1))
              ((symbol-function 'region-beginning) (lambda () 10))
              ((symbol-function 'region-end) (lambda () 20)))
      (should (equal (clutch-result--selected-row-indices) '(2 3)))))
  (with-temp-buffer
    (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
              ((symbol-function 'clutch-result--row-idx-at-line)
               (lambda () 4)))
      (should (equal (clutch-result--selected-row-indices) '(4))))))

(ert-deftest clutch-test-down-cell-keeps-region-active ()
  "Row navigation should keep region active for selection workflows."
  (with-temp-buffer
    (let ((deactivate-mark t))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch--col-idx-at-point) (lambda () 1))
                ((symbol-function 'get-text-property)
                 (lambda (_pos prop) (when (eq prop 'clutch-row-idx) 2)))
                ((symbol-function 'clutch--goto-cell) (lambda (&rest _args) nil)))
        (clutch-result-down-cell)
        (should-not deactivate-mark)))))

(ert-deftest clutch-test-region-cells-rectangle ()
  "Region cell extraction should use rectangular cell bounds."
  (with-temp-buffer
    (setq-local clutch--result-rows
                '((r0c0 r0c1 r0c2)
                  (r1c0 r1c1 r1c2)
                  (r2c0 r2c1 r2c2)))
    (cl-letf (((symbol-function 'region-beginning) (lambda () 10))
              ((symbol-function 'region-end) (lambda () 20))
              ((symbol-function 'clutch-result--cell-at-or-near)
               (lambda (pos)
                 (if (= pos 10) '(0 1 nil) '(2 1 nil)))))
      (should (equal (clutch-result--region-cells)
                     '((0 1 r0c1)
                       (1 1 r1c1)
                       (2 1 r2c1)))))))

(ert-deftest clutch-test-yank-cell-default ()
  "Yank cell should copy current cell value."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 "hello"))))
        (clutch-result-yank-cell)
        (should (equal (current-kill 0) "hello"))))))

(ert-deftest clutch-test-yank-cell-with-region-copies-region-cells ()
  "Yank cell should copy region cells as TSV when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p)
                 (lambda () t))
                ((symbol-function 'region-beginning)
                 (lambda () 10))
                ((symbol-function 'region-end)
                 (lambda () 20))
                ((symbol-function 'clutch-result--region-cells)
                 (lambda ()
                   '((0 0 1) (0 2 "shanghai") (1 1 "bob")))))
        (clutch-result-yank-cell)
        (should (equal (current-kill 0) "1\tshanghai\nbob"))))))

(ert-deftest clutch-test-yank-cell-without-region-copies-point-cell ()
  "Yank cell should ignore region logic when region is not active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p)
                 (lambda () nil))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(2 3 "alice"))))
        (clutch-result-yank-cell)
        (should (equal (current-kill 0) "alice"))))))

(ert-deftest clutch-test-copy-command-dispatches-to-csv ()
  "Copy command should dispatch to CSV copier when csv is selected."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "csv"))
                ((symbol-function 'clutch-result-copy)
                 (lambda (fmt)
                   (setq called fmt))))
        (clutch-result-copy-command)
        (should (equal called 'csv))))))

(ert-deftest clutch-test-copy-command-defaults-to-tsv ()
  "Copy command should default to TSV format."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) "tsv"))
                ((symbol-function 'clutch-result-copy)
                 (lambda (fmt)
                   (setq called fmt))))
        (clutch-result-copy-command)
        (should (equal called 'tsv))))))

(ert-deftest clutch-test-copy-csv-via-unified-entry-uses-region-rectangle ()
  "Unified CSV copy should use rectangle row/column bounds when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("c0" "c1" "c2"))
      (setq-local clutch--result-rows '((a0 a1 a2) (b0 b1 b2) (c0 c1 c2)))
        (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 1 2))))
        (clutch-result-copy 'csv)
        (should (equal (current-kill 0) "c1,c2\na1,a2\nb1,b2"))))))

(ert-deftest clutch-test-copy-csv-without-region-copies-current-cell ()
  "Unified CSV copy should use current cell when region is inactive."
  (with-temp-buffer
    (setq-local clutch--result-columns '("c0" "c1"))
    (setq-local clutch--result-rows '((a0 a1)))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 a1))))
        (clutch-result-copy 'csv)
        (should (equal (current-kill 0) "c1\na1"))))))

(ert-deftest clutch-test-copy-insert-via-unified-entry-uses-region-rectangle ()
  "Unified INSERT copy should use rectangle row/column bounds when region is active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch-connection 'fake-conn)
      (setq-local clutch--result-columns '("id" "name" "age"))
      (setq-local clutch--result-rows '((1 "a" 10) (2 "b" 20)))
        (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 0 1)))
                ((symbol-function 'clutch-result--detect-table)
                 (lambda () "t"))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch--value-to-literal)
                 (lambda (v) (format "'%s'" v))))
        (clutch-result-copy 'insert)
        (should (string-match-p "INSERT INTO \"t\" (\"id\", \"name\") VALUES ('1', 'a');"
                                (current-kill 0)))
        (should (string-match-p "INSERT INTO \"t\" (\"id\", \"name\") VALUES ('2', 'b');"
                                (current-kill 0)))))))

(ert-deftest clutch-test-copy-insert-without-region-copies-current-cell ()
  "Unified INSERT copy should use current cell when region is inactive."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows '((1 "a")))
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 "a")))
                ((symbol-function 'clutch-result--detect-table)
                 (lambda () "t"))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch--value-to-literal)
                 (lambda (v) (format "'%s'" v))))
        (clutch-result-copy 'insert)
        (should (equal (current-kill 0)
                       "INSERT INTO \"t\" (\"name\") VALUES ('a');"))))))

(ert-deftest clutch-test-destructive-query-p ()
  "Test destructive query detection."
  (should (clutch--destructive-query-p "DROP TABLE users"))
  (should (clutch--destructive-query-p "TRUNCATE users"))
  (should (clutch--destructive-query-p "DELETE FROM users"))
  (should (clutch--destructive-query-p "delete from users where id=1"))
  ;; With leading comment
  (should (clutch--destructive-query-p "-- cleanup\nDROP TABLE users"))
  (should-not (clutch--destructive-query-p "SELECT * FROM users"))
  (should-not (clutch--destructive-query-p "UPDATE users SET name='x'")))

(ert-deftest clutch-test-select-query-p ()
  "Test SELECT query detection."
  (should (clutch--select-query-p "SELECT * FROM users"))
  (should (clutch--select-query-p "select id from users"))
  (should (clutch--select-query-p "  SELECT * FROM t"))
  (should (clutch--select-query-p "WITH cte AS (SELECT 1) SELECT * FROM cte"))
  ;; With leading comments — previously broke SELECT detection
  (should (clutch--select-query-p "-- get users\nSELECT * FROM users"))
  (should (clutch--select-query-p "/* all */\nSELECT * FROM users"))
  (should (clutch--select-query-p "-- a\n-- b\nSELECT 1"))
  ;; Note: SHOW/DESCRIBE/EXPLAIN are not recognized as SELECT
  (should-not (clutch--select-query-p "SHOW TABLES"))
  (should-not (clutch--select-query-p "DESCRIBE users"))
  (should-not (clutch--select-query-p "EXPLAIN SELECT * FROM t"))
  (should-not (clutch--select-query-p "INSERT INTO users VALUES (1)"))
  (should-not (clutch--select-query-p "UPDATE users SET name='x'")))

;;;; Unit tests — value to literal conversion

(ert-deftest clutch-test-value-to-literal-nil ()
  "Test NULL literal conversion."
  (should (equal (clutch--value-to-literal nil) "NULL")))

(ert-deftest clutch-test-value-to-literal-number ()
  "Test numeric literal conversion."
  (should (equal (clutch--value-to-literal 42) "42"))
  (should (string-match-p "3\\.14" (clutch--value-to-literal 3.14)))
  (should (equal (clutch--value-to-literal -1) "-1")))

(ert-deftest clutch-test-value-to-literal-string ()
  "Test string literal conversion (requires connection)."
  (require 'clutch-db-mysql)
  (require 'mysql)
  ;; String escaping requires a connection
  (let ((clutch-connection (make-mysql-conn :host "localhost")))
    (let ((result (clutch--value-to-literal "hello")))
      (should (stringp result))
      (should (string-prefix-p "'" result)))
    (let ((result (clutch--value-to-literal "it's")))
      (should (string-match-p "\\\\'" result)))))

;;;; Unit tests — connection key

(ert-deftest clutch-test-connection-key ()
  "Test connection key generation."
  (require 'clutch-db-mysql)
  (require 'mysql)
  (let ((conn (make-mysql-conn :host "localhost" :port 3306
                                :user "root" :database "test")))
    (let ((key (clutch--connection-key conn)))
      (should (stringp key))
      (should (string-match-p "localhost" key))
      (should (string-match-p "3306" key))
      (should (string-match-p "root" key))
      (should (string-match-p "test" key)))))

;;;; Unit tests — WHERE filter application

(ert-deftest clutch-test-apply-where ()
  "Test WHERE filter application to SQL."
  ;; Simple case wraps query and applies outer WHERE
  (should (string-match-p
           "SELECT \\* FROM (SELECT \\* FROM t) AS _clutch_filter WHERE id = 1"
           (clutch--apply-where "SELECT * FROM t" "id = 1")))
  ;; Query with existing WHERE is wrapped safely
  (let ((result (clutch--apply-where "SELECT * FROM t WHERE x > 0" "id = 1")))
    (should (string-match-p "FROM (SELECT \\* FROM t WHERE x > 0)" result))
    (should (string-match-p "WHERE id = 1\\'" result))))

(ert-deftest clutch-test-apply-where-with-cte ()
  "Test WHERE filter wrapping for CTE SQL."
  (let* ((sql "WITH x AS (SELECT id FROM t) SELECT * FROM x")
         (result (clutch--apply-where sql "id > 10")))
    (should (string-match-p "^SELECT \\* FROM (WITH x AS" result))
    (should (string-match-p "WHERE id > 10\\'" result))))

(ert-deftest clutch-test-apply-where-with-union ()
  "Test WHERE filter wrapping for UNION SQL."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b)")
         (result (clutch--apply-where sql "id > 10")))
    (should (string-match-p "^SELECT \\* FROM (.*UNION ALL.*) AS _clutch_filter" result))
    (should (string-match-p "WHERE id > 10\\'" result))))

(ert-deftest clutch-test-apply-where-normalizes-comments-and-semicolon ()
  "WHERE rewrite should strip leading comments and trailing semicolons."
  (let* ((sql "-- head comment\n/* block */\nSELECT id FROM t;")
         (result (clutch--apply-where sql "id > 10")))
    (should (string-prefix-p
             "SELECT * FROM (SELECT id FROM t) AS _clutch_filter WHERE id > 10"
             result))
    (should-not (string-match-p ";\\s-*) AS _clutch_filter" result))))

(ert-deftest clutch-test-build-count-sql-strips-order-and-limit ()
  "Count SQL should strip top-level ORDER BY / LIMIT / OFFSET clauses."
  (let ((result (clutch--build-count-sql
                 "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20")))
    (should (string-match-p
             "^SELECT COUNT(\\*) FROM (SELECT id, name FROM users) AS _clutch_count\\'"
             result))))

(ert-deftest clutch-test-build-count-sql-with-cte ()
  "Count SQL should wrap CTE query safely."
  (let* ((sql "WITH x AS (SELECT id FROM t ORDER BY id) SELECT * FROM x ORDER BY id")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "^SELECT COUNT(\\*) FROM (WITH x AS" result))
    (should (string-match-p ") AS _clutch_count\\'" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-distinct ()
  "Count SQL should preserve DISTINCT semantics via derived-table wrapping."
  (let* ((sql "SELECT DISTINCT user_id FROM visits ORDER BY user_id")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p
             "^SELECT COUNT(\\*) FROM (SELECT DISTINCT user_id FROM visits) AS _clutch_count\\'"
             result))))

(ert-deftest clutch-test-build-count-sql-keeps-inner-order-by ()
  "Count SQL should not remove ORDER BY inside nested subqueries."
  (let* ((sql "SELECT * FROM (SELECT id FROM t ORDER BY created_at DESC) s ORDER BY id")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "SELECT id FROM t ORDER BY created_at DESC" result))
    (should (string-match-p "AS _clutch_count\\'" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-union-top-order ()
  "Count SQL should drop only top-level ORDER BY for UNION queries."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) ORDER BY id")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "UNION ALL" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-with-union-limit-offset ()
  "Count SQL should strip top-level LIMIT/OFFSET on UNION queries."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 50 OFFSET 100")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "UNION ALL" result))
    (should-not (string-match-p "LIMIT 50\\s-+OFFSET 100\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-keeps-window-order-by ()
  "Count SQL should keep ORDER BY inside window OVER clauses."
  (let* ((sql "SELECT row_number() OVER (ORDER BY created_at DESC) AS rn FROM t ORDER BY rn LIMIT 5")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "OVER (ORDER BY created_at DESC)" result))
    (should-not (string-match-p "ORDER BY rn\\s-+LIMIT 5\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-strips-trailing-semicolon ()
  "Count SQL should normalize trailing semicolons."
  (let ((result (clutch--build-count-sql "SELECT * FROM users;")))
    (should-not (string-match-p ";\\s-*) AS _clutch_count\\'" result))
    (should (string-match-p "SELECT \\* FROM users" result))))

(ert-deftest clutch-test-build-count-sql-strips-leading-comments ()
  "Count SQL should ignore leading SQL comments."
  (let* ((sql "-- comment\n/* block */\nSELECT id FROM t ORDER BY id")
         (result (clutch--build-count-sql sql)))
    (should (string-prefix-p "SELECT COUNT(*) FROM (SELECT id FROM t)" result))
    (should-not (string-match-p "ORDER BY id\\s-*) AS _clutch_count\\'" result))))

;;;; Unit tests — separator rendering

(ert-deftest clutch-test-render-separator ()
  "Test table separator line rendering."
  (let ((visible-cols '(0 1 2))
        (widths [5 10 8]))
    (let ((sep (clutch--render-separator visible-cols widths 'top)))
      (should (stringp sep))
      (should (> (length sep) 0)))))

;;;; Unit tests — connection timeout / interruption handling

(ert-deftest clutch-test-build-conn-includes-read-timeout-for-network-backends ()
  "Test that `clutch--build-conn' passes :read-timeout to mysql/pg."
  (let ((clutch-query-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306 :user "u"))
      (should (equal (plist-get captured :read-timeout) 42))
      (clutch--build-conn '(:backend pg :host "127.0.0.1" :port 5432 :user "u"))
      (should (equal (plist-get captured :read-timeout) 42)))))

(ert-deftest clutch-test-build-conn-skips-read-timeout-for-sqlite ()
  "Test that `clutch--build-conn' does not pass :read-timeout to sqlite."
  (let ((clutch-query-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend sqlite :database ":memory:"))
      (should-not (plist-member captured :read-timeout)))))

(ert-deftest clutch-test-execute-quit-disconnects-and-clears-connection ()
  "Test `clutch--execute' converts quit into recoverable interruption."
  (let ((disconnected nil)
        (clutch-connection 'fake-conn)
        (clutch--executing-p nil))
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
              ((symbol-function 'clutch--add-history) (lambda (_sql) nil))
              ((symbol-function 'clutch--update-mode-line) (lambda () nil))
              ((symbol-function 'clutch--select-query-p) (lambda (_sql) t))
              ((symbol-function 'clutch--execute-select) (lambda (&rest _args) (signal 'quit nil)))
              ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t))))
      (should-error (clutch--execute "SELECT 1" clutch-connection)
                    :type 'user-error)
      (should disconnected)
      (should-not clutch-connection)
      (should-not clutch--executing-p))))


;;;; Unit tests — SQL keyword completion

(ert-deftest clutch-test-sql-keyword-completion-matching ()
  "Test that keyword capf returns candidates matching a prefix."
  (with-temp-buffer
    (insert "SEL")
    (let ((result (clutch-sql-keyword-completion-at-point)))
      (should result)
      (should (member "SELECT" (nth 2 result))))))

(ert-deftest clutch-test-sql-keyword-completion-case-insensitive ()
  "Test case-insensitive matching (input \"sel\" matches \"SELECT\")."
  (with-temp-buffer
    (insert "sel")
    (let* ((result (clutch-sql-keyword-completion-at-point))
           (candidates (nth 2 result)))
      (should result)
      ;; The candidate list includes all keywords; completion framework
      ;; handles case-insensitive filtering.  Verify SELECT is present.
      (should (member "SELECT" candidates)))))

(ert-deftest clutch-test-sql-keyword-completion-no-prefix ()
  "Test that keyword capf returns nil with no word at point."
  (with-temp-buffer
    (insert " ")
    (let ((result (clutch-sql-keyword-completion-at-point)))
      (should-not result))))

;;;; Live integration tests

(defmacro clutch-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live connection.
Skips if `clutch-test-password' is nil."
  (declare (indent 1))
  `(if (null clutch-test-password)
       (ert-skip "Set clutch-test-password to enable live tests")
     (let ((,var (clutch-db-connect
                  clutch-test-backend
                  (list :host clutch-test-host
                        :port clutch-test-port
                        :user clutch-test-user
                        :password clutch-test-password
                        :database clutch-test-database))))
       (unwind-protect
           (progn ,@body)
         (clutch-db-disconnect ,var)))))

(ert-deftest clutch-test-live-display-select-result ()
  :tags '(:clutch-live)
  "Test displaying a SELECT result."
  (clutch-test--with-conn conn
    (let* ((result (clutch-db-query conn "SELECT 1 AS id, 'test' AS name"))
           (columns (clutch-db-result-columns result))
           (rows (clutch-db-result-rows result))
           (col-names (clutch--column-names columns)))
      ;; Setup buffer state
      (with-temp-buffer
        (setq-local clutch-connection conn)
        (setq-local clutch--result-columns col-names)
        (setq-local clutch--result-column-defs columns)
        (setq-local clutch--result-rows rows)
        (setq-local clutch--display-offset (length rows))
        (setq-local clutch--pending-edits nil)
        (setq-local clutch--fk-info nil)
        (setq-local clutch--where-filter nil)
        (setq-local clutch--current-col-page 0)
        (setq-local clutch--pinned-columns nil)
        (let ((widths (clutch--compute-column-widths col-names rows columns)))
          (setq-local clutch--column-widths widths)
          (setq-local clutch--column-pages
                      (clutch--compute-column-pages widths nil 80)))
        ;; Render
        (clutch--render-result)
        ;; Verify buffer has content
        (should (> (buffer-size) 0))
        ;; Column names are in header-line/tab-line, not buffer text.
        ;; Verify data values appear in the rendered table.
        (should (string-match-p "test" (buffer-string)))))))

(ert-deftest clutch-test-live-schema-introspection ()
  :tags '(:clutch-live)
  "Test schema introspection functions."
  (clutch-test--with-conn conn
    ;; List tables
    (let ((tables (clutch-db-list-tables conn)))
      (should (listp tables))
      (should (> (length tables) 0)))
    ;; List columns
    (let ((columns (clutch-db-list-columns conn "user")))
      (should (listp columns))
      (should (> (length columns) 0)))
    ;; Primary keys
    (let ((pk-cols (clutch-db-primary-key-columns conn "user")))
      (should (listp pk-cols)))))

(ert-deftest clutch-test-live-paged-sql-building ()
  :tags '(:clutch-live)
  "Test paged SQL query building."
  (clutch-test--with-conn conn
    (let ((clutch-connection conn)
          (base-sql "SELECT * FROM user"))
      ;; Build paged SQL
      (let ((paged (clutch-db-build-paged-sql conn base-sql 0 10)))
        (should (stringp paged))
        (should (string-match-p "LIMIT" paged)))
      ;; With order
      (let ((paged (clutch-db-build-paged-sql conn base-sql 0 10 '("Host" . "ASC"))))
        (should (string-match-p "ORDER BY" paged))))))

(provide 'clutch-test)
;;; clutch-test.el ends here
