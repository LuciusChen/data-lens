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
(require 'ob-clutch)

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

(ert-deftest clutch-test-json-value-to-string-hash-table ()
  "JSON viewer should accept parsed JSON objects."
  (skip-unless (fboundp 'json-serialize))
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "a" 1 obj)
    (should (equal (clutch--json-value-to-string obj) "{\"a\":1}"))))

(ert-deftest clutch-test-dispatch-view-json-category-serializes-non-string ()
  "JSON category should route non-string values to JSON viewer."
  (let ((called nil)
        (seen nil))
    (cl-letf (((symbol-function 'clutch--view-json-value)
               (lambda (s) (setq called t
                                 seen s)))
              ((symbol-function 'clutch--json-value-to-string)
               (lambda (_val) "{\"ok\":true}")))
      (clutch--dispatch-view (vector 1 2) '(:type-category json))
      (should called)
      (should (equal seen "{\"ok\":true}")))))

(ert-deftest clutch-test-dispatch-view-fallback-to-plain ()
  "Unknown values should open plain viewer rather than JSON viewer."
  (let ((plain-called nil)
        (json-called nil))
    (cl-letf (((symbol-function 'clutch--view-plain-value)
               (lambda (_v) (setq plain-called t)))
              ((symbol-function 'clutch--view-json-value)
               (lambda (_v) (setq json-called t))))
      (clutch--dispatch-view "hello" '(:type-category text))
      (should plain-called)
      (should-not json-called))))

(ert-deftest clutch-test-dispatch-view-xml-content-overrides-blob ()
  "XML-like content should use XML viewer even when column type is blob."
  (let ((xml-called nil)
        (blob-called nil))
    (cl-letf (((symbol-function 'clutch--view-xml-value)
               (lambda (_v) (setq xml-called t)))
              ((symbol-function 'clutch--view-binary-as-string)
               (lambda (_v) (setq blob-called t))))
      (clutch--dispatch-view "<rss><item>1</item></rss>" '(:type-category blob))
      (should xml-called)
      (should-not blob-called))))

(ert-deftest clutch-test-dispatch-view-invalid-angle-text-not-xml ()
  "Invalid XML-like text should not be forced into XML viewer."
  (let ((xml-called nil)
        (plain-called nil))
    (cl-letf (((symbol-function 'clutch--view-xml-value)
               (lambda (_v) (setq xml-called t)))
              ((symbol-function 'clutch--view-plain-value)
               (lambda (_v) (setq plain-called t))))
      (clutch--dispatch-view "<abc" '(:type-category text))
      (should-not xml-called)
      (should plain-called))))

(ert-deftest clutch-test-blob-view-string-has-size-and-hex ()
  "Blob preview should include size and hex output."
  (let ((s (clutch--blob-view-string (unibyte-string #x00 #xff #x41 #x7f))))
    (should (string-match-p "BLOB size: 4 bytes" s))
    (should (string-match-p "Hex preview:" s))
    (should (string-match-p "00 ff 41 7f" s))))

(ert-deftest clutch-test-blob-view-string-text-preview ()
  "Text-like blobs should use concise text preview."
  (let ((s (clutch--blob-view-string "hello world")))
    (should (string-match-p "BLOB size: 11 bytes" s))
    (should (string-match-p "Text preview:" s))
    (should-not (string-match-p "Hex preview:" s))))

(ert-deftest clutch-test-value-placeholder-detects-xml-and-blob ()
  "Grid placeholders should compactly mark XML and BLOB values."
  (should (equal (clutch--value-placeholder "<root/>" '(:type-category text))
                 "<XML>"))
  (should (equal (clutch--value-placeholder (unibyte-string #x00 #x01)
                                            '(:type-category blob))
                 "<BLOB>")))

(ert-deftest clutch-test-xml-like-string-p-strict ()
  "XML detection should avoid false positives for plain angle-bracket text."
  (should (clutch--xml-like-string-p "<rss><item>1</item></rss>"))
  (should (clutch--xml-like-string-p "<?xml version=\"1.0\"?><rss/>"))
  (should-not (clutch--xml-like-string-p "<abc"))
  (should-not (clutch--xml-like-string-p "just <text> marker")))

(ert-deftest clutch-test-view-xml-value-enables-fontification ()
  "XML viewer should invoke fontification and show byte size in header."
  (let ((fontified nil)
        (buf nil))
    (cl-letf (((symbol-function 'executable-find) (lambda (_cmd) nil))
              ((symbol-function 'nxml-mode) (lambda () nil))
              ((symbol-function 'font-lock-ensure)
               (lambda (&rest _args) (setq fontified t)))
              ((symbol-function 'jit-lock-fontify-now)
               (lambda (&rest _args) nil))
              ((symbol-function 'pop-to-buffer)
               (lambda (b &rest _args)
                 (setq buf b)
                 b)))
      (clutch--view-xml-value "<root><a>1</a></root>")
      (should fontified)
      (with-current-buffer buf
        (should (string-match-p "XML" (format "%s" header-line-format)))
        (should (string-match-p "bytes" (format "%s" header-line-format)))))))

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

(ert-deftest clutch-test-footer-filter-parts-omits-sql-preview ()
  "Footer filter parts should no longer include last SQL preview text."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT id FROM t")
    (should (equal (clutch--footer-filter-parts) nil))))

(ert-deftest clutch-test-footer-filter-parts-includes-aggregate-summary ()
  "Footer filter parts should include aggregate summary segment."
  (with-temp-buffer
    (setq-local clutch--aggregate-summary
                '(:label "selection" :rows 2 :cells 4 :skipped 0
                         :sum 62 :avg 15.5 :min 10 :max 21 :count 4))
    (let ((parts (clutch--footer-filter-parts)))
      (should (= (length parts) 1))
      (should-not (string-match-p "selection" (car parts)))
      (should (string-match-p "sum=62" (car parts)))
      (should (string-match-p "\\[r2 c4 s0\\]" (car parts))))))

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

(ert-deftest clutch-test-aggregate-current-column-without-region ()
  "Aggregate should use current cell when region is inactive."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1.5") (2 "2.5") (3 "x") (4 4)))
      (cl-letf (((symbol-function 'use-region-p) (lambda () nil))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(1 1 "2.5"))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[score\\]" summary))
          (should (string-match-p "sum=2.5" summary))
          (should (string-match-p "avg=2.5" summary))
          (should (string-match-p "\\[rows=1 cells=1 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-region-multi-column-aggregates-all-columns ()
  "Aggregate should summarize all selected cells as one result."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "a" "b"))
      (setq-local clutch--result-rows '((1 10 20) (2 11 21)))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1) 1 2))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[selection\\]" summary))
          (should (string-match-p "sum=62" summary))
          (should (string-match-p "avg=15.5" summary))
          (should (string-match-p "\\[rows=2 cells=4 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-region-single-column ()
  "Aggregate should support rectangular region for one selected column."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1") (2 "2") (3 "3")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 2) 1)))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 "1"))))
        (clutch-result-aggregate)
        (let ((summary (current-kill 0)))
          (should (string-match-p "Aggregate \\[score\\]" summary))
          (should (string-match-p "sum=4" summary))
          (should (string-match-p "avg=2" summary))
          (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary)))))))

(ert-deftest clutch-test-aggregate-with-prefix-refines-region ()
  "C-u aggregate should use refined rectangle selection."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (setq-local clutch--result-columns '("id" "score"))
      (setq-local clutch--result-rows '((1 "1") (2 "2") (3 "3")))
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1 2) . (1))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (_rect callback)
                   (funcall callback '((0 2) . (1)))))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 "1"))))
        (clutch-result-aggregate t)
        (let ((summary (current-kill 0)))
          (should (string-match-p "sum=4" summary))
          (should (string-match-p "\\[rows=2 cells=2 skipped=0\\]" summary)))))))

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
        (clutch-result-copy 'tsv)
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
        (clutch-result-copy 'tsv)
        (should (equal (current-kill 0) "1\tshanghai\nbob"))))))

(ert-deftest clutch-test-yank-cell-without-region-copies-point-cell ()
  "Yank cell should ignore region logic when region is not active."
  (with-temp-buffer
    (let (kill-ring kill-ring-yank-pointer)
      (cl-letf (((symbol-function 'use-region-p)
                 (lambda () nil))
                ((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(2 3 "alice"))))
        (clutch-result-copy 'tsv)
        (should (equal (current-kill 0) "alice"))))))

(ert-deftest clutch-test-copy-csv-command-dispatches-to-csv ()
  "CSV copy command should dispatch to the unified copy entry."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'clutch-result-copy)
                 (lambda (fmt &optional rect)
                   (setq called (list fmt rect)))))
        (clutch-result-copy-csv)
        (should (equal called '(csv nil)))))))

(ert-deftest clutch-test-copy-tsv-command-dispatches-to-tsv ()
  "TSV copy command should dispatch to the unified copy entry."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'clutch-result-copy)
                 (lambda (fmt &optional rect)
                   (setq called (list fmt rect)))))
        (clutch-result-copy-tsv)
        (should (equal called '(tsv nil)))))))

(ert-deftest clutch-test-copy-fmt-with-refine-uses-refined-rectangle ()
  "Refined copy should pass the final rectangle into the unified copy entry."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'use-region-p) (lambda () t))
                ((symbol-function 'transient-args)
                 (lambda (_prefix) '("--refine")))
                ((symbol-function 'transient-arg-value)
                 (lambda (flag args)
                   (and (equal flag "--refine")
                        (member "--refine" args))))
                ((symbol-function 'clutch-result--region-rectangle-indices)
                 (lambda () '((0 1 2) . (1 2))))
                ((symbol-function 'clutch-result--start-refine)
                 (lambda (_rect callback)
                   (funcall callback '((0 2) . (2)))))
                ((symbol-function 'clutch-result-copy)
                 (lambda (fmt &optional rect)
                   (setq called (list fmt rect)))))
        (clutch-result--copy-fmt 'csv)
        (should (equal called '(csv ((0 2) . (2)))))))))

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

(ert-deftest clutch-test-risky-dml-p ()
  "Risky DML should detect UPDATE/DELETE without top-level WHERE."
  (should (clutch--risky-dml-p "UPDATE users SET name='x'"))
  (should (clutch--risky-dml-p "DELETE FROM users"))
  (should (clutch--risky-dml-p "WITH x AS (SELECT 1) UPDATE users SET name='x'"))
  (should-not (clutch--risky-dml-p "UPDATE users SET name='x' WHERE id=1"))
  (should-not (clutch--risky-dml-p "DELETE FROM users WHERE id=1"))
  (should-not (clutch--risky-dml-p "WITH x AS (SELECT 1) UPDATE users SET name='x' WHERE id=1"))
  (should-not (clutch--risky-dml-p "WITH x AS (SELECT 1) SELECT * FROM x"))
  (should-not (clutch--risky-dml-p "SELECT * FROM users")))

(ert-deftest clutch-test-require-risky-dml-confirmation-cancels ()
  "Risky DML should be cancelled unless user types YES."
  (cl-letf (((symbol-function 'clutch--risky-dml-p) (lambda (_sql) t))
            ((symbol-function 'read-string) (lambda (&rest _args) "NO")))
    (should-error (clutch--require-risky-dml-confirmation "UPDATE users SET x=1")
                  :type 'user-error)))

(ert-deftest clutch-test-require-risky-dml-confirmation-accepts-yes ()
  "Risky DML should proceed when user types YES."
  (cl-letf (((symbol-function 'clutch--risky-dml-p) (lambda (_sql) t))
            ((symbol-function 'read-string) (lambda (&rest _args) "YES")))
    (should (null (clutch--require-risky-dml-confirmation "UPDATE users SET x=1")))))

(ert-deftest clutch-test-ensure-where-guard-blocks-missing-where ()
  "Generated DML statements must contain top-level WHERE."
  (should-error
   (clutch-result--ensure-where-guard '("UPDATE t SET x=1") "UPDATE")
   :type 'user-error)
  (should-error
   (clutch-result--ensure-where-guard '("DELETE FROM t") "DELETE")
   :type 'user-error)
  (should (null (clutch-result--ensure-where-guard
                 '("UPDATE t SET x=1 WHERE id=1" "DELETE FROM t WHERE id=1")
                 "UPDATE"))))

(ert-deftest clutch-test-preview-execution-sql-prefers-pending-edits-in-result-mode ()
  "Preview in result mode should show generated UPDATE SQL when edits exist."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch--pending-edits '(((0 . 1) . "v")))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '("UPDATE t SET name='v' WHERE id=1")))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (string-match-p "UPDATE t SET name='v' WHERE id=1;" captured))))))

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

(ert-deftest clutch-test-build-conn-includes-native-timeouts-for-network-backends ()
  "Test that `clutch--build-conn' passes timeout defaults to mysql/pg."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306 :user "u"))
      (should (equal (plist-get captured :connect-timeout) 11))
      (should (equal (plist-get captured :read-idle-timeout) 42))
      (clutch--build-conn '(:backend pg :host "127.0.0.1" :port 5432 :user "u"))
      (should (equal (plist-get captured :connect-timeout) 11))
      (should (equal (plist-get captured :read-idle-timeout) 42)))))

(ert-deftest clutch-test-build-conn-includes-jdbc-timeouts ()
  "Test that `clutch--build-conn' passes timeout defaults to JDBC backends."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 12)
        (clutch-query-timeout-seconds 13)
        (clutch-jdbc-rpc-timeout-seconds 14)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend oracle :host "db" :port 1521 :user "u"))
      (should (equal (plist-get captured :connect-timeout) 11))
      (should (equal (plist-get captured :read-idle-timeout) 12))
      (should (equal (plist-get captured :query-timeout) 13))
      (should (equal (plist-get captured :rpc-timeout) 14)))))

(ert-deftest clutch-test-build-conn-skips-timeouts-for-sqlite ()
  "Test that `clutch--build-conn' does not pass network timeout keys to sqlite."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42)
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (clutch--build-conn '(:backend sqlite :database ":memory:"))
      (should-not (plist-member captured :connect-timeout))
      (should-not (plist-member captured :read-idle-timeout))
      (should-not (plist-member captured :query-timeout))
      (should-not (plist-member captured :rpc-timeout)))))

(ert-deftest clutch-test-build-conn-rejects-removed-read-timeout ()
  "Test that removed timeout keys fail fast with a clear error."
  (should-error
   (clutch--build-conn '(:backend mysql :host "127.0.0.1" :port 3306
                         :user "u" :read-timeout 5))
   :type 'user-error))

(ert-deftest clutch-test-tables-in-buffer-caches-until-buffer-changes ()
  "Table lookup in the buffer should reuse cached results until text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users")
    (let ((schema (make-hash-table :test 'equal))
          (calls 0)
          (orig-string-match-p (symbol-function 'string-match-p)))
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (cl-letf (((symbol-function 'string-match-p)
                 (lambda (regexp string &optional start)
                   (cl-incf calls)
                   (funcall orig-string-match-p regexp string start))))
        (should (equal (clutch--tables-in-buffer schema) '("users")))
        (should (= calls 2))
        (should (equal (clutch--tables-in-buffer schema) '("users")))
        (should (= calls 2))
        (goto-char (point-max))
        (insert " JOIN posts")
        (should (equal (clutch--tables-in-buffer schema) '("users" "posts")))
        (should (= calls 4))))))

(ert-deftest clutch-test-tables-in-query-caches-within-statement ()
  "Statement table lookup should reuse cached results until statement or text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users JOIN posts ON users.id = posts.user_id;\n\nSELECT * FROM logs")
    (let ((schema (make-hash-table :test 'equal))
          (calls 0)
          (orig-string-match-p (symbol-function 'string-match-p)))
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (puthash "logs" t schema)
      (cl-letf (((symbol-function 'string-match-p)
                 (lambda (regexp string &optional start)
                   (cl-incf calls)
                   (funcall orig-string-match-p regexp string start))))
        (goto-char (point-min))
        (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                       '("posts" "users")))
        (let ((first calls))
          (search-forward "users.id")
          (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                         '("posts" "users")))
          (should (= calls first))
          (goto-char (point-max))
          (should (equal (clutch--tables-in-query schema) '("logs")))
          (should (> calls first))
          (let ((second calls))
            (goto-char (point-max))
            (insert " WHERE level = 'error'")
            (should (equal (clutch--tables-in-query schema) '("logs")))
            (should (> calls second))))))))

(ert-deftest clutch-test-goto-cell-uses-row-start-positions ()
  "Cell navigation should use cached row starts when available."
  (with-temp-buffer
    (insert "row0\nrow1\n")
    (let* ((row0 (point-min))
           (row1 (save-excursion
                   (goto-char (point-min))
                   (forward-line 1)
                   (point)))
           (clutch--row-start-positions (vector row0 row1)))
      (add-text-properties (+ row0 1) (+ row0 2)
                           '(clutch-row-idx 0 clutch-col-idx 0))
      (add-text-properties (+ row1 2) (+ row1 3)
                           '(clutch-row-idx 1 clutch-col-idx 7))
      (clutch--goto-cell 1 7)
      (should (= (point) (+ row1 2))))))

(ert-deftest clutch-test-goto-cell-falls-back-to-first-cell-in-row ()
  "Cell navigation should fall back to the first cell on the target row."
  (with-temp-buffer
    (insert "row0\nrow1\n")
    (let* ((row0 (point-min))
           (row1 (save-excursion
                   (goto-char (point-min))
                   (forward-line 1)
                   (point)))
           (clutch--row-start-positions (vector row0 row1)))
      (add-text-properties (+ row1 3) (+ row1 4)
                           '(clutch-row-idx 1 clutch-col-idx 2))
      (clutch--goto-cell 1 99)
      (should (= (point) (+ row1 3))))))

(ert-deftest clutch-test-parse-error-position-supports-pg-and-oracle ()
  "Error position parsing should handle PG and Oracle/JDBC formats."
  (should (= 17 (clutch--parse-error-position "syntax error (position 17)")))
  (should (= 12 (clutch--parse-error-position
                 "ORA-06550: line 2, column 3:"
                 "SELECT 1\nFROM dual"))))

(ert-deftest clutch-test-mark-sql-error-falls-back-to-statement-region ()
  "Errors without a character position should still mark the statement."
  (with-temp-buffer
    (insert "SELECT missing_col FROM dual")
    (let ((clutch--executing-sql-start (point-min))
          (clutch--executing-sql-end (point-max)))
      (clutch--mark-sql-error
       (current-buffer)
       (buffer-string)
       "ORA-00904: \"MISSING_COL\": invalid identifier")
      (should (overlayp clutch--error-position-overlay))
      (should (= (overlay-start clutch--error-position-overlay) (point-min)))
      (should (= (overlay-end clutch--error-position-overlay) (point-max))))))

(ert-deftest clutch-test-mark-sql-error-uses-oracle-line-column ()
  "Oracle line/column errors should mark the reported character."
  (with-temp-buffer
    (insert "SELECT 1\nFROM dual")
    (let ((clutch--executing-sql-start (point-min))
          (clutch--executing-sql-end (point-max)))
      (clutch--mark-sql-error
       (current-buffer)
       (buffer-string)
       "ORA-06550: line 2, column 3:")
      (should (overlayp clutch--error-position-overlay))
      (should (= (overlay-start clutch--error-position-overlay) 12))
      (should (= (overlay-end clutch--error-position-overlay) 13)))))

(ert-deftest clutch-test-mark-sql-error-banner-works-on-first-line ()
  "The error banner should render above SQL that starts on the first line."
  (with-temp-buffer
    (insert "SELECT missing_col FROM dual")
    (let ((clutch--executing-sql-start (point-min))
          (clutch--executing-sql-end (point-max)))
      (clutch--mark-sql-error
       (current-buffer)
       (buffer-string)
       "ORA-00904: invalid identifier")
      (should (overlayp clutch--error-banner-overlay))
      (should (= (overlay-start clutch--error-banner-overlay) (point-min)))
      (should (string-match-p
               "SQL error: ORA-00904: invalid identifier"
               (overlay-get clutch--error-banner-overlay 'before-string))))))

(ert-deftest clutch-test-mark-sql-error-banner-anchors-to-statement-line ()
  "The error banner should appear above the statement line, not mid-line."
  (with-temp-buffer
    (insert "-- heading\nSELECT 1; SELECT bad_col FROM dual")
    (let ((clutch--executing-sql-start 20)
          (clutch--executing-sql-end (point-max)))
      (clutch--mark-sql-error
       (current-buffer)
       (buffer-substring-no-properties clutch--executing-sql-start clutch--executing-sql-end)
       "ORA-00904: invalid identifier")
      (should (overlayp clutch--error-banner-overlay))
      (should (= (overlay-start clutch--error-banner-overlay) 12)))))

(ert-deftest clutch-test-execute-and-mark-skips-success-overlay-on-error ()
  "Failed execution should not mark SQL as successfully executed."
  (with-temp-buffer
    (insert "SELECT bad_col FROM dual")
    (let ((marked nil))
      (cl-letf (((symbol-function 'clutch--execute) (lambda (&rest _) nil))
                ((symbol-function 'clutch--mark-executed-sql-region)
                 (lambda (&rest _) (setq marked t))))
        (clutch--execute-and-mark (buffer-string) (point-min) (point-max))
        (should-not marked)))))

(ert-deftest clutch-test-execute-quit-disconnects-and-clears-connection ()
  "Test `clutch--execute' converts quit into recoverable interruption."
  (let ((disconnected nil)
        (clutch-connection 'fake-conn)
        (clutch--executing-p nil))
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--check-pending-changes) #'ignore)
              ((symbol-function 'clutch--clear-error-position-overlay) #'ignore)
              ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
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

(ert-deftest clutch-test-execute-runs-risky-dml-confirmation ()
  "Execute should run risky DML confirmation before dispatch."
  (let ((called nil)
        (clutch-connection 'fake-conn))
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--check-pending-changes) #'ignore)
              ((symbol-function 'clutch--clear-error-position-overlay) #'ignore)
              ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
              ((symbol-function 'clutch--require-risky-dml-confirmation)
               (lambda (_sql) (setq called t)))
              ((symbol-function 'clutch--update-mode-line) (lambda () nil))
              ((symbol-function 'clutch--select-query-p) (lambda (_sql) t))
              ((symbol-function 'clutch--execute-select) (lambda (&rest _args) 'ok)))
      (clutch--execute "UPDATE users SET x=1" clutch-connection)
      (should called))))


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


;;;; Unit tests — REPL

(ert-deftest clutch-test-repl-input-sender-accumulates-until-semicolon ()
  "REPL input sender should accumulate partial SQL and show continuation prompt."
  (with-temp-buffer
    (let ((clutch-repl--pending-input "")
          output)
      (cl-letf (((symbol-function 'clutch-repl--output)
                 (lambda (text) (push text output)))
                ((symbol-function 'clutch-repl--execute-and-print)
                 (lambda (_sql) (error "should not execute"))))
        (clutch-repl--input-sender nil "SELECT 1")
        (should (equal clutch-repl--pending-input "SELECT 1"))
        (should (equal (car output) "    -> "))))))

(ert-deftest clutch-test-repl-input-sender-executes-on-semicolon ()
  "REPL input sender should execute when statement ends with semicolon."
  (with-temp-buffer
    (let ((clutch-repl--pending-input "SELECT")
          sent)
      (cl-letf (((symbol-function 'clutch-repl--execute-and-print)
                 (lambda (sql) (setq sent sql)))
                ((symbol-function 'clutch-repl--output)
                 (lambda (_text) (error "should not output continuation"))))
        (clutch-repl--input-sender nil " 1;")
        (should (equal sent "SELECT\n 1;"))
        (should (equal clutch-repl--pending-input ""))))))

(ert-deftest clutch-test-repl-execute-and-print-not-connected ()
  "REPL should print not-connected message when no live connection."
  (with-temp-buffer
    (let (captured)
      (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq captured text))))
        (clutch-repl--execute-and-print "SELECT 1")
        (should (string-match-p "Not connected" captured))
        (should (string-match-p "db> $" captured))))))

(ert-deftest clutch-test-repl-execute-and-print-select-result ()
  "REPL should print table summary for SELECT results."
  (with-temp-buffer
    (let ((clutch-connection 'fake-conn)
          output)
      (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result
                    :columns '((:name "id"))
                    :rows '((1)))))
                ((symbol-function 'clutch--column-names)
                 (lambda (_columns) '("id")))
                ((symbol-function 'clutch--render-static-table)
                 (lambda (_col-names _rows _columns) "| id |\n| 1 |"))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq output text))))
        (clutch-repl--execute-and-print "SELECT 1;")
        (should (string-match-p "| id |" output))
        (should (string-match-p "1 row" output))
        (should (string-match-p "db> $" output))))))

;;;; Unit tests - schema/table browse helpers

(ert-deftest clutch-test-schema-browse-at-point-errors-without-console ()
  "Schema browse should error when no matching query console is open."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-schema--table-at-point)
               (lambda () "users"))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn tbl) tbl))
              ((symbol-function 'clutch--find-console-for-conn)
               (lambda (_conn) nil)))
      (should-error (clutch-schema-browse-at-point) :type 'user-error))))

(ert-deftest clutch-test-schema-browse-at-point-inserts-sql-into-console ()
  "Schema browse should insert escaped SELECT in the target console buffer."
  (let ((console (generate-new-buffer " *clutch-test-console*")))
    (unwind-protect
        (with-current-buffer console
          (insert "SELECT 1;")
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'clutch-schema--table-at-point)
                     (lambda () "order-items"))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn tbl) (format "\"%s\"" tbl)))
                    ((symbol-function 'clutch--find-console-for-conn)
                     (lambda (_conn) console))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (set-buffer buf)
                       buf)))
            (with-temp-buffer
              (setq-local clutch-connection 'fake-conn)
              (clutch-schema-browse-at-point)))
          (should (string-suffix-p
                   "\n\nSELECT * FROM \"order-items\";"
                   (buffer-string))))
      (kill-buffer console))))

(ert-deftest clutch-test-schema-toggle-expand-keeps-point-on-table-header ()
  "Toggling expand from a column line should keep point on that table header."
  (with-temp-buffer
    (clutch-schema-mode)
    (setq-local clutch-connection 'fake-conn
                clutch-schema--tables '("a" "b")
                clutch-schema--expanded-tables '("a"))
    (cl-letf (((symbol-function 'clutch-db-database)
               (lambda (_conn) "db"))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn tbl)
                 (if (equal tbl "a")
                     (list (list :name "c1" :type "int")
                           (list :name "c2" :type "int")
                           (list :name "c3" :type "int"))
                   nil))))
      (clutch-schema--render)
      (goto-char (point-min))
      (search-forward "c2")
      (beginning-of-line)
      (should (equal (clutch-schema--table-at-point) "a"))
      (clutch-schema-toggle-expand)
      (should (equal (clutch-schema--table-at-point) "a"))
      (should (looking-at-p "▸ a")))))

(ert-deftest clutch-test-browse-table-inserts-escaped-select ()
  "Browse table command should escape identifier before inserting SQL."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (insert "-- tail")
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn tbl) (format "\"%s\"" tbl))))
      (clutch-browse-table "order-items"))
    (should (string-suffix-p
             "\n\nSELECT * FROM \"order-items\";"
             (buffer-string)))))

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

(ert-deftest clutch-test-live-edit-field-and-commit-persists ()
  :tags '(:clutch-live)
  "Edit one field and commit; the persisted row value should change."
  (clutch-test--with-conn conn
    (let* ((table (format "clutch_edit_commit_%d" (emacs-pid)))
           (drop-sql (format "DROP TABLE IF EXISTS %s" table))
           (create-sql
            (format "CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(64))" table))
           (insert-sql
            (format "INSERT INTO %s (id, name) VALUES (1, 'before')" table))
           (select-sql
            (format "SELECT id, name FROM %s ORDER BY id" table)))
      (unwind-protect
          (progn
            (clutch-db-query conn drop-sql)
            (clutch-db-query conn create-sql)
            (clutch-db-query conn insert-sql)
            (with-temp-buffer
              (setq-local clutch-connection conn)
              (setq-local clutch--last-query select-sql)
              (setq-local clutch--result-columns '("id" "name"))
              (setq-local clutch--result-rows '((1 "before")))
              (setq-local clutch--pending-edits nil)
              (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
                        ((symbol-function 'clutch--execute) #'ignore)
                        ((symbol-function 'yes-or-no-p) (lambda (&rest _) t)))
                (clutch-result--apply-edit 0 1 "after")
                (should clutch--pending-edits)
                (clutch-result-commit)
                (should-not clutch--pending-edits)))
            (let* ((res (clutch-db-query conn select-sql))
                   (rows (clutch-db-result-rows res)))
              (should (equal rows '((1 "after"))))))
        (ignore-errors (clutch-db-query conn drop-sql))))))

(ert-deftest clutch-test-completion-finished-status-p ()
  "Keyword spacing should trigger for accepted completion statuses."
  (should (clutch--completion-finished-status-p 'finished))
  (should (clutch--completion-finished-status-p 'exact))
  (should (clutch--completion-finished-status-p 'sole))
  (should-not (clutch--completion-finished-status-p 'unknown)))

(ert-deftest clutch-test-keyword-capf-exit-function-inserts-space-on-exact ()
  "Keyword CAPF exit-function should insert a trailing space for status `exact'."
  (with-temp-buffer
    (insert "FROM")
    (let* ((capf (clutch-sql-keyword-completion-at-point))
           (exit-fn (plist-get (cdddr capf) :exit-function)))
      (funcall exit-fn "FROM" 'exact)
      (should (equal (buffer-string) "FROM ")))))

;;;; Staged-commit PK-vec tests

(ert-deftest clutch-test-stage-delete-stores-pk-vec ()
  "Staging a delete stores a PK vector, not a ridx integer."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes nil)
    (cl-letf (((symbol-function 'clutch-result--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch-result--detect-primary-key) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-delete-rows)
      (should (equal clutch--pending-deletes (list (vector 42)))))))

(ert-deftest clutch-test-commit-delete-uses-pk-vec ()
  "DELETE statement uses PK values from stored vector, not ridx."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--pending-deletes (list (vector 42)))
    (cl-letf (((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch-result--detect-primary-key) (lambda () '(0)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name)))
              ((symbol-function 'clutch--value-to-literal) (lambda (v) (format "%s" v))))
      (let ((stmts (clutch-result--build-pending-delete-statements)))
        (should (= (length stmts) 1))
        (should (string-match-p "WHERE" (car stmts)))
        (should (string-match-p "42" (car stmts)))))))

(ert-deftest clutch-test-stage-edit-stores-pk-vec ()
  "Staging an edit stores (pk-vec . cidx) key, not (ridx . cidx)."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 7 "bob")))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result--apply-edit 0 1 "carol")
      (should (= (length clutch--pending-edits) 1))
      (let ((key (caar clutch--pending-edits)))
        (should (vectorp (car key)))
        (should (equal (car key) (vector 7)))
        (should (= (cdr key) 1))))))

(ert-deftest clutch-test-commit-edit-generates-update-with-pk-where ()
  "UPDATE statement uses PK values in WHERE clause."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 7) 1) "carol")))
    (cl-letf (((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch-result--detect-primary-key) (lambda () '(0)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name)))
              ((symbol-function 'clutch--value-to-literal)
               (lambda (v) (if (stringp v) (format "'%s'" v) (format "%s" v)))))
      (let ((stmts (clutch-result--build-update-statements)))
        (should (= (length stmts) 1))
        (should (string-match-p "carol" (car stmts)))
        (should (string-match-p "WHERE" (car stmts)))
        (should (string-match-p "7" (car stmts)))))))

(ert-deftest clutch-test-discard-delete-removes-pk-entry ()
  "C-c C-k removes the matching pk-vec from pending-deletes."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes (list (vector 42)))
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch-result--row-idx-at-line) (lambda () 0))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-deletes)))))

(ert-deftest clutch-test-discard-insert-removes-entry ()
  "C-c C-k on a ghost insert row removes it from pending-inserts."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 1 "x")))
    (setq-local clutch--pending-inserts (list '(("id" . "99") ("name" . "new"))))
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch-result--row-idx-at-line)
               (lambda () 1))  ; ridx=1 >= nrows=1 → insert slot 0
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-inserts)))))

(ert-deftest clutch-test-check-pending-changes-blocks-when-deletes-pending ()
  "clutch--check-pending-changes signals user-error when user declines to discard."
  (let ((buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer buf
          (setq-local clutch--pending-deletes (list (vector 1)))
          (setq-local clutch--pending-edits nil)
          (setq-local clutch--pending-inserts nil)
          (cl-letf (((symbol-function 'get-buffer)
                     (lambda (_name) buf))
                    ((symbol-function 'yes-or-no-p) (lambda (_) nil)))
            (should-error (clutch--check-pending-changes) :type 'user-error)))
      (kill-buffer buf))))

(ert-deftest clutch-test-commit-ordering-insert-update-delete ()
  "Commit executes INSERT before UPDATE before DELETE."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 1 "a") (list 2 "b")))
    (setq-local clutch--cached-pk-indices '(0))
    (setq-local clutch--pending-inserts '((("id" . "3") ("name" . "c"))))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 1) 1) "a2")))
    (setq-local clutch--pending-deletes (list (vector 2)))
    (let (executed)
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '("INSERT INTO users (id, name) VALUES (3, 'c')")))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '("UPDATE users SET name = 'a2' WHERE id = 1")))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '("DELETE FROM users WHERE id = 2")))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn stmt) (push stmt executed)))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-commit)
        (should (= (length executed) 3))
        ;; executed is in reverse push order: last executed is at (nth 0 executed)
        (should (string-prefix-p "INSERT" (nth 2 executed)))
        (should (string-prefix-p "UPDATE" (nth 1 executed)))
        (should (string-prefix-p "DELETE" (nth 0 executed)))))))

;;;; Protocol Layer Benchmarks and Correctness Tests

(require 'mysql)
(require 'pg)

;;; Opt 1: IEEE 754 float/double decoding correctness

(ert-deftest clutch-test-ieee754-double-1.0 ()
  "Double-precision: [00 00 00 00 00 00 F0 3F] -> 1.0."
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xF0 #x3F)))
    (should (= (mysql--ieee754-double-to-float data 0) 1.0))))

(ert-deftest clutch-test-ieee754-double-2.0 ()
  "Double-precision: [00 00 00 00 00 00 00 40] -> 2.0."
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x40)))
    (should (= (mysql--ieee754-double-to-float data 0) 2.0))))

(ert-deftest clutch-test-ieee754-double-neg-1.5 ()
  "Double-precision: -1.5 round-trip via known LE bytes."
  ;; -1.5: sign=1, exp=1023, mantissa=2^51; LE: 00 00 00 00 00 00 F8 BF
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xF8 #xBF)))
    (should (= (mysql--ieee754-double-to-float data 0) -1.5))))

(ert-deftest clutch-test-ieee754-double-pos-inf ()
  "Double-precision: [00 00 00 00 00 00 F0 7F] -> +Inf."
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xF0 #x7F)))
    (should (= (mysql--ieee754-double-to-float data 0) 1.0e+INF))))

(ert-deftest clutch-test-ieee754-double-neg-inf ()
  "Double-precision: [00 00 00 00 00 00 F0 FF] -> -Inf."
  (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xF0 #xFF)))
    (should (= (mysql--ieee754-double-to-float data 0) -1.0e+INF))))

(ert-deftest clutch-test-ieee754-double-nan ()
  "Double-precision: exponent=0x7FF, mantissa!=0 -> NaN."
  (let ((data (unibyte-string #x01 #x00 #x00 #x00 #x00 #x00 #xF0 #x7F)))
    (should (isnan (mysql--ieee754-double-to-float data 0)))))

(ert-deftest clutch-test-ieee754-double-subnormal ()
  "Double-precision subnormal: exponent=0, mantissa!=0 -> tiny positive float."
  (let* ((data (unibyte-string #x01 #x00 #x00 #x00 #x00 #x00 #x00 #x00))
         (result (mysql--ieee754-double-to-float data 0)))
    (should (floatp result))
    (should (> result 0.0))
    (should (< result 2.3e-308))))

(ert-deftest clutch-test-ieee754-double-offset ()
  "Double-precision decoder respects non-zero offset within data string."
  ;; Pad with one garbage byte at offset 0; double starts at offset 1.
  (let ((data (unibyte-string #xFF #x00 #x00 #x00 #x00 #x00 #x00 #xF0 #x3F)))
    (should (= (mysql--ieee754-double-to-float data 1) 1.0))))

(ert-deftest clutch-test-ieee754-single-1.0 ()
  "Single-precision: [00 00 80 3F] -> 1.0."
  (let ((data (unibyte-string #x00 #x00 #x80 #x3F)))
    (should (= (mysql--ieee754-single-to-float data 0) 1.0))))

(ert-deftest clutch-test-ieee754-single-2.0 ()
  "Single-precision: [00 00 00 40] -> 2.0."
  (let ((data (unibyte-string #x00 #x00 #x00 #x40)))
    (should (= (mysql--ieee754-single-to-float data 0) 2.0))))

(ert-deftest clutch-test-ieee754-single-pos-inf ()
  "Single-precision: [00 00 80 7F] -> +Inf."
  (let ((data (unibyte-string #x00 #x00 #x80 #x7F)))
    (should (= (mysql--ieee754-single-to-float data 0) 1.0e+INF))))

(ert-deftest clutch-test-ieee754-single-neg-inf ()
  "Single-precision: [00 00 80 FF] -> -Inf.
Note: [00 00 C0 FF] has mantissa bit set -> NaN, not -Inf."
  (let ((data (unibyte-string #x00 #x00 #x80 #xFF)))
    (should (= (mysql--ieee754-single-to-float data 0) -1.0e+INF))))

(ert-deftest clutch-test-ieee754-single-nan ()
  "Single-precision: exponent=0xFF, mantissa!=0 -> NaN."
  (let ((data (unibyte-string #x01 #x00 #x80 #x7F)))
    (should (isnan (mysql--ieee754-single-to-float data 0)))))

(ert-deftest clutch-test-ieee754-single-subnormal ()
  "Single-precision subnormal: exponent=0, mantissa!=0 -> tiny positive float."
  (let* ((data (unibyte-string #x01 #x00 #x00 #x00))
         (result (mysql--ieee754-single-to-float data 0)))
    (should (floatp result))
    (should (> result 0.0))
    (should (< result 1.2e-38))))

;; Benchmark (uncomment and eval manually; not run by default):
;; (ert-deftest clutch-bench-ieee754-double () :tags '(benchmark)
;;   (let ((data (unibyte-string #x00 #x00 #x00 #x00 #x00 #x00 #xF0 #x3F)))
;;     (message "IEEE754 double x10000: %.4fs"
;;              (car (benchmark-run 10000
;;                     (mysql--ieee754-double-to-float data 0))))))

;;; Opt 2: PG type dispatch hash table

(ert-deftest clutch-test-pg-parse-value-null ()
  "nil input returns nil for any OID."
  (should (null (pg--parse-value nil pg-oid-int4)))
  (should (null (pg--parse-value nil pg-oid-bool)))
  (should (null (pg--parse-value nil 99999))))

(ert-deftest clutch-test-pg-parse-value-bool ()
  "Bool OID: \"t\"->t, \"f\"->nil, other->raw string."
  (should (eq (pg--parse-value "t" pg-oid-bool) t))
  (should (null (pg--parse-value "f" pg-oid-bool)))
  (should (equal (pg--parse-value "unknown" pg-oid-bool) "unknown")))

(ert-deftest clutch-test-pg-parse-value-integers ()
  "Integer OIDs: string -> number."
  (should (= (pg--parse-value "42" pg-oid-int2) 42))
  (should (= (pg--parse-value "42" pg-oid-int4) 42))
  (should (= (pg--parse-value "42" pg-oid-int8) 42)))

(ert-deftest clutch-test-pg-parse-value-floats ()
  "Float/numeric OIDs: string -> number."
  (should (= (pg--parse-value "3.14" pg-oid-float4) 3.14))
  (should (= (pg--parse-value "3.14" pg-oid-float8) 3.14))
  (should (= (pg--parse-value "1.5" pg-oid-numeric) 1.5)))

(ert-deftest clutch-test-pg-parse-value-date ()
  "Date OID: string -> plist with :year/:month/:day."
  (let ((result (pg--parse-value "2024-01-15" pg-oid-date)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :month) 1))
    (should (= (plist-get result :day) 15))))

(ert-deftest clutch-test-pg-parse-value-time ()
  "Time OID: string -> plist with :hours/:minutes/:seconds."
  (let ((result (pg--parse-value "13:45:30" pg-oid-time)))
    (should (= (plist-get result :hours) 13))
    (should (= (plist-get result :minutes) 45))
    (should (= (plist-get result :seconds) 30))))

(ert-deftest clutch-test-pg-parse-value-timestamp ()
  "Timestamp OID: string -> plist with :year/:hours."
  (let ((result (pg--parse-value "2024-01-15 13:45:30" pg-oid-timestamp)))
    (should (= (plist-get result :year) 2024))
    (should (= (plist-get result :hours) 13))))

(ert-deftest clutch-test-pg-parse-value-timestamptz ()
  "Timestamptz OID: uses same parser as timestamp."
  (let ((result (pg--parse-value "2024-01-15 13:45:30+00" pg-oid-timestamptz)))
    (should (= (plist-get result :year) 2024))))

(ert-deftest clutch-test-pg-parse-value-unknown-oid ()
  "Unknown OID: raw string passthrough."
  (should (equal (pg--parse-value "foo" 99999) "foo"))
  (should (equal (pg--parse-value "" 0) "")))

(ert-deftest clutch-test-pg-parse-value-user-parser-takes-precedence ()
  "pg-type-parsers entries override the dispatch table."
  (let ((pg-type-parsers (list (cons pg-oid-int4 (lambda (_v) :custom-result)))))
    (should (eq (pg--parse-value "42" pg-oid-int4) :custom-result))))

(ert-deftest clutch-test-pg-parse-value-user-parser-unknown-oid ()
  "pg-type-parsers can add parsers for OIDs not in the dispatch table."
  (let ((pg-type-parsers (list (cons 99999 (lambda (v) (concat "custom:" v))))))
    (should (equal (pg--parse-value "test" 99999) "custom:test"))))

;; Benchmark (uncomment and eval manually; not run by default):
;; (ert-deftest clutch-bench-pg-parse-value () :tags '(benchmark)
;;   (let ((oids (list pg-oid-int4 pg-oid-float8 pg-oid-bool pg-oid-date
;;                     pg-oid-timestamp pg-oid-int8 pg-oid-numeric
;;                     pg-oid-int2 pg-oid-float4 pg-oid-time))
;;         (vals (list "42" "3.14" "t" "2024-01-15"
;;                     "2024-01-15 00:00:00" "999" "1.5" "7" "2.71" "13:00:00")))
;;     (message "pg--parse-value 10-col x10000: %.4fs"
;;              (car (benchmark-run 10000
;;                     (cl-mapcar #'pg--parse-value vals oids))))))

;;; Opt 3: PBKDF2-SHA256 loop condition

(defun clutch-test--bytes-to-hex (bytes)
  "Convert unibyte string BYTES to a lowercase hex string."
  (mapconcat (lambda (b) (format "%02x" b)) bytes ""))

(ert-deftest clutch-test-pbkdf2-sha256-c1-dklen32 ()
  "PBKDF2-SHA256 c=1, dkLen=32: verified with Python hashlib.pbkdf2_hmac."
  (skip-unless (fboundp 'secure-hash))
  (let ((result (pg--pbkdf2-sha256 "password" "salt" 1 32)))
    (should (equal (clutch-test--bytes-to-hex result)
                   "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"))))

(ert-deftest clutch-test-pbkdf2-sha256-c4096-dklen32 ()
  "PBKDF2-SHA256 c=4096, dkLen=32 matches RFC 7914 vector."
  (skip-unless (fboundp 'secure-hash))
  (let ((result (pg--pbkdf2-sha256 "password" "salt" 4096 32)))
    (should (equal (clutch-test--bytes-to-hex result)
                   "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"))))

(ert-deftest clutch-test-pbkdf2-sha256-multi-block ()
  "PBKDF2-SHA256 dkLen=64 exercises two-block outer loop.
Expected value verified with Python hashlib.pbkdf2_hmac."
  (skip-unless (fboundp 'secure-hash))
  (let ((result (pg--pbkdf2-sha256 "password" "salt" 1 64)))
    (should (equal (clutch-test--bytes-to-hex result)
                   (concat "120fb6cffcf8b32c43e7225256c4f837"
                           "a86548c92ccc35480805987cb70be17b"
                           "4dbf3a2f3dad3377264bb7b8e8330d4e"
                           "fc7451418617dabef683735361cdc18c")))))

(ert-deftest clutch-test-pbkdf2-sha256-output-length ()
  "PBKDF2-SHA256 returns exactly key-length bytes for various lengths."
  (skip-unless (fboundp 'secure-hash))
  (dolist (dklen '(1 16 32 33 64))
    (should (= (length (pg--pbkdf2-sha256 "pw" "salt" 1 dklen)) dklen))))

;; Benchmark (uncomment and eval manually; not run by default):
;; (ert-deftest clutch-bench-pbkdf2 () :tags '(benchmark)
;;   (skip-unless (fboundp 'secure-hash))
;;   (message "pbkdf2-sha256 c=4096: %.4fs"
;;            (car (benchmark-run 1
;;                   (pg--pbkdf2-sha256 "password" "salt" 4096 32)))))

;;; Opt 4: Buffer read-offset tracking

(defun clutch-test--make-mysql-conn-with-data (data)
  "Create a minimal mysql-conn with DATA pre-loaded in its buffer.
Use `cl-letf' to bypass `mysql--ensure-data' in tests."
  (let* ((buf (generate-new-buffer " *clutch-test-mysql*"))
         (conn (make-mysql-conn :buf buf :host "test" :read-idle-timeout 1)))
    (with-current-buffer buf
      (set-buffer-multibyte nil)
      (insert data))
    conn))

(defun clutch-test--mysql-conn-cleanup (conn)
  "Kill the synthetic buffer for a test conn."
  (when (buffer-live-p (mysql-conn-buf conn))
    (kill-buffer (mysql-conn-buf conn))))

(ert-deftest clutch-test-mysql-read-bytes-offset-advances ()
  "mysql--read-bytes advances read-offset without deleting buffer content."
  (let* ((data (unibyte-string #x01 #x02 #x03 #x04 #x05))
         (conn (clutch-test--make-mysql-conn-with-data data)))
    (unwind-protect
        (cl-letf (((symbol-function 'mysql--ensure-data) #'ignore))
          (let ((r1 (mysql--read-bytes conn 2)))
            (should (equal r1 (unibyte-string #x01 #x02)))
            (should (= (mysql-conn-read-offset conn) 2)))
          ;; Buffer content intact — not deleted yet
          (with-current-buffer (mysql-conn-buf conn)
            (should (= (- (point-max) (point-min)) 5)))
          (let ((r2 (mysql--read-bytes conn 2)))
            (should (equal r2 (unibyte-string #x03 #x04)))
            (should (= (mysql-conn-read-offset conn) 4))))
      (clutch-test--mysql-conn-cleanup conn))))

(ert-deftest clutch-test-mysql-read-packet-flushes-buffer ()
  "mysql--read-packet flushes consumed bytes in bulk and resets offset."
  ;; Packet: len=3 (LE: 03 00 00), seq=01, payload=AA BB CC
  (let* ((pkt (unibyte-string #x03 #x00 #x00 #x01 #xAA #xBB #xCC))
         (conn (clutch-test--make-mysql-conn-with-data pkt)))
    (unwind-protect
        (cl-letf (((symbol-function 'mysql--ensure-data) #'ignore))
          (let ((result (mysql--read-packet conn)))
            (should (equal result (unibyte-string #xAA #xBB #xCC)))
            (with-current-buffer (mysql-conn-buf conn)
              (should (= (- (point-max) (point-min)) 0)))
            (should (= (mysql-conn-read-offset conn) 0))))
      (clutch-test--mysql-conn-cleanup conn))))

(ert-deftest clutch-test-mysql-read-packet-two-sequential ()
  "Two sequential mysql--read-packet calls parse correctly."
  (let* ((data (concat (unibyte-string #x03 #x00 #x00 #x01 #xAA #xBB #xCC)
                       (unibyte-string #x02 #x00 #x00 #x02 #xDD #xEE)))
         (conn (clutch-test--make-mysql-conn-with-data data)))
    (unwind-protect
        (cl-letf (((symbol-function 'mysql--ensure-data) #'ignore))
          (should (equal (mysql--read-packet conn)
                         (unibyte-string #xAA #xBB #xCC)))
          (should (equal (mysql--read-packet conn)
                         (unibyte-string #xDD #xEE)))
          (with-current-buffer (mysql-conn-buf conn)
            (should (= (- (point-max) (point-min)) 0))))
      (clutch-test--mysql-conn-cleanup conn))))

(ert-deftest clutch-test-mysql-read-offset-reset-on-erase ()
  "erase-buffer + reset-offset restores a clean state."
  (let* ((data (unibyte-string #x01 #x02 #x03))
         (conn (clutch-test--make-mysql-conn-with-data data)))
    (unwind-protect
        (cl-letf (((symbol-function 'mysql--ensure-data) #'ignore))
          (mysql--read-bytes conn 2)
          (should (= (mysql-conn-read-offset conn) 2))
          (with-current-buffer (mysql-conn-buf conn)
            (erase-buffer)
            (setf (mysql-conn-read-offset conn) 0))
          (should (= (mysql-conn-read-offset conn) 0)))
      (clutch-test--mysql-conn-cleanup conn))))

(defun clutch-test--make-pg-conn-with-data (data)
  "Create a minimal pg-conn with DATA pre-loaded in its buffer."
  (let* ((buf (generate-new-buffer " *clutch-test-pg*"))
         (conn (make-pg-conn :buf buf :host "test" :read-idle-timeout 1)))
    (with-current-buffer buf
      (set-buffer-multibyte nil)
      (insert data))
    conn))

(defun clutch-test--pg-conn-cleanup (conn)
  "Kill the synthetic buffer for a test pg-conn."
  (when (buffer-live-p (pg-conn-buf conn))
    (kill-buffer (pg-conn-buf conn))))

(ert-deftest clutch-test-pg-read-bytes-offset-advances ()
  "pg--read-bytes advances read-offset without deleting buffer content."
  (let* ((data (unibyte-string #x01 #x02 #x03 #x04))
         (conn (clutch-test--make-pg-conn-with-data data)))
    (unwind-protect
        (cl-letf (((symbol-function 'pg--ensure-data) #'ignore))
          (let ((r1 (pg--read-bytes conn 2)))
            (should (equal r1 (unibyte-string #x01 #x02)))
            (should (= (pg-conn-read-offset conn) 2)))
          (with-current-buffer (pg-conn-buf conn)
            (should (= (- (point-max) (point-min)) 4))))
      (clutch-test--pg-conn-cleanup conn))))

(ert-deftest clutch-test-pg-read-message-flushes-buffer ()
  "pg--read-message flushes consumed bytes and resets offset."
  ;; PG message: type=?T(84), len=4+3=7 (BE: 00 00 00 07), payload=AA BB CC
  (let* ((msg (concat (unibyte-string ?T)
                      (unibyte-string #x00 #x00 #x00 #x07)
                      (unibyte-string #xAA #xBB #xCC)))
         (conn (clutch-test--make-pg-conn-with-data msg)))
    (unwind-protect
        (cl-letf (((symbol-function 'pg--ensure-data) #'ignore))
          (let ((result (pg--read-message conn)))
            (should (= (car result) ?T))
            (should (equal (cdr result) (unibyte-string #xAA #xBB #xCC)))
            (with-current-buffer (pg-conn-buf conn)
              (should (= (- (point-max) (point-min)) 0)))
            (should (= (pg-conn-read-offset conn) 0))))
      (clutch-test--pg-conn-cleanup conn))))

;; Benchmark (uncomment and eval manually; not run by default):
;; (ert-deftest clutch-bench-buffer-read () :tags '(benchmark)
;;   (let* ((row (make-string (* 20 8) #x41))
;;          (buf (generate-new-buffer " *bench-buf*"))
;;          (conn (make-mysql-conn :buf buf :host "bench" :read-idle-timeout 1)))
;;     (with-current-buffer buf
;;       (set-buffer-multibyte nil)
;;       (dotimes (_ 1000) (insert row)))
;;     (unwind-protect
;;         (cl-letf (((symbol-function 'mysql--ensure-data) #'ignore))
;;           (message "read-bytes 1000x20x8: %.4fs"
;;                    (car (benchmark-run 1
;;                           (dotimes (_ (* 1000 20))
;;                             (mysql--read-bytes conn 8))))))
;;       (kill-buffer buf))))

;;;; Quality fix tests

;;; Fix 1 — pg-oid-bool type-category

(ert-deftest clutch-test-pg-bool-type-category ()
  "pg-oid-bool should map to text, not numeric."
  (require 'clutch-db-pg)
  (let ((entry (assoc pg-oid-bool clutch-db-pg--type-category-alist)))
    (should entry)
    (should (eq (cdr entry) 'text))))

;;; Fix 3 — clutch-db-format-temporal

(ert-deftest clutch-test-format-temporal-datetime ()
  "clutch-db-format-temporal formats datetime plists."
  (should (equal (clutch-db-format-temporal
                  '(:year 2024 :month 1 :day 15 :hours 13 :minutes 45 :seconds 30))
                 "2024-01-15 13:45:30")))

(ert-deftest clutch-test-format-temporal-date-only ()
  "clutch-db-format-temporal formats date-only plists."
  (should (equal (clutch-db-format-temporal '(:year 2024 :month 6 :day 1))
                 "2024-06-01")))

(ert-deftest clutch-test-format-temporal-time-only ()
  "clutch-db-format-temporal formats time-only plists."
  (should (equal (clutch-db-format-temporal
                  '(:hours 13 :minutes 5 :seconds 0 :negative nil))
                 "13:05:00")))

(ert-deftest clutch-test-format-temporal-time-negative ()
  "clutch-db-format-temporal formats negative time plists."
  (should (equal (clutch-db-format-temporal
                  '(:hours 1 :minutes 0 :seconds 0 :negative t))
                 "-01:00:00")))

(ert-deftest clutch-test-format-temporal-non-temporal ()
  "clutch-db-format-temporal returns nil for non-temporal plists."
  (should (null (clutch-db-format-temporal '(:foo 1 :bar 2)))))

(ert-deftest clutch-test-ob-format-value-number-raw ()
  "ob-clutch--format-value returns raw number for Org column alignment."
  (should (= (ob-clutch--format-value 42) 42)))

;;; Fix 2 — reconnect clears pending state

(ert-deftest clutch-test-reconnect-clears-pending ()
  "Reconnect discards pending state in result buffers."
  (let ((result-buf (generate-new-buffer "*clutch-test-result*"))
        (clutch-buf (generate-new-buffer "*clutch-test*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch--pending-deletes (list (vector 1)))
            (setq-local clutch--pending-edits nil)
            (setq-local clutch--pending-inserts nil))
          (with-current-buffer clutch-buf
            (cl-letf (((symbol-function 'clutch--build-conn)
                       (lambda (_) 'fake-conn))
                      ((symbol-function 'clutch-db-live-p)
                       (lambda (_) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (_) "fake"))
                      ((symbol-function 'clutch--update-mode-line) #'ignore)
                      ((symbol-function 'clutch--refresh-display) #'ignore))
              (setq-local clutch--connection-params '(:backend mysql))
              (clutch--try-reconnect)))
          (with-current-buffer result-buf
            (should (null clutch--pending-deletes))))
      (kill-buffer result-buf)
      (kill-buffer clutch-buf))))

;;; Fix 4 — clutch-refresh-schema

(ert-deftest clutch-test-refresh-schema-calls-refresh ()
  "clutch-refresh-schema calls clutch--refresh-schema-cache."
  (let (refresh-called)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn) (setq refresh-called t))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch-refresh-schema)
        (should refresh-called)))))

;;; Fix 5 — ob-clutch--disconnect-all

(ert-deftest clutch-test-ob-clutch-disconnect-all ()
  "ob-clutch--disconnect-all disconnects and clears all cached connections."
  (let ((disconnected nil)
        (ob-clutch--connection-cache (make-hash-table :test 'equal)))
    (puthash "key1" 'conn1 ob-clutch--connection-cache)
    (puthash "key2" 'conn2 ob-clutch--connection-cache)
    (cl-letf (((symbol-function 'clutch-db-disconnect)
               (lambda (c) (push c disconnected))))
      (ob-clutch--disconnect-all)
      (should (= (length disconnected) 2))
      (should (= (hash-table-count ob-clutch--connection-cache) 0)))))

(provide 'clutch-test)
;;; clutch-test.el ends here
