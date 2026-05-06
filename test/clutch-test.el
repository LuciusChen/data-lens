;;; clutch-test.el --- ERT tests for database workflows -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; ERT tests for the clutch user interface layer.
;;
;; Unit tests run without a database server.
;; Live tests require a running database:
;;   docker run -d -e MYSQL_ROOT_PASSWORD=test -p 3306:3306 mysql:8
;;
;; Run unit tests:
;;   Emacs -batch -L .. -l ert -l clutch-test \
;;     -f ert-run-tests-batch-and-exit

;;; Code:

;;;; Test configuration

(require 'cl-lib)

(require 'ert)

(require 'clutch-db)

(require 'clutch-db-jdbc)

(require 'clutch)

(defvar mysql-tls-verify-server)

(defvar clutch-column-displayers)

(defvar clutch--result-source-table)

(declare-function make-clutch-jdbc-conn "clutch-db-jdbc" (&rest slot-value-pairs))

(declare-function make-mysql-conn "mysql" (&rest args))

(declare-function clutch-db-pg--type-category "clutch-db-pg" (oid))

(defvar clutch-test-backend 'mysql)

(defvar clutch-test-host "127.0.0.1")

(defvar clutch-test-port 3306)

(defvar clutch-test-user "root")

(defvar clutch-test-password nil)

(defvar clutch-test-database "mysql")

;;;; Test helpers

(defun clutch-test--debug-buffer-string ()
  "Return the current dedicated clutch debug buffer contents."
  (let ((buf (get-buffer clutch-debug-buffer-name)))
    (should (buffer-live-p buf))
    (with-current-buffer buf
      (buffer-string))))

(defun clutch-test--primary-row-identity (&optional table columns indices)
  "Return primary-key row identity metadata for tests."
  (let ((indices (or indices '(0))))
    (list :kind 'primary-key
          :name "PRIMARY"
          :table (or table "users")
          :columns (or columns '("id"))
          :indices indices
          :source-indices indices)))

;;;; Rendering — value formatting

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
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "a" 1 obj)
    (should (equal (clutch--json-value-to-string obj) "{\"a\":1}"))))

(ert-deftest clutch-test-json-value-to-string-hash-table-preserves-unicode ()
  "Parsed JSON objects should keep readable Unicode text."
  (let ((obj (make-hash-table :test 'equal)))
    (puthash "quote" "记忆碎片已封存" obj)
    (puthash "operator" "Saito" obj)
    (puthash "thermoptic" :false obj)
    (should (equal (clutch--json-value-to-string obj)
                   "{\"quote\":\"记忆碎片已封存\",\"operator\":\"Saito\",\"thermoptic\":false}"))))

(ert-deftest clutch-test-json-value-to-string-scalars ()
  "JSON scalar values should remain valid JSON when edited or viewed."
  (should (equal (clutch--json-value-to-string "hello") "\"hello\""))
  (should (equal (clutch--json-value-to-string nil) "null"))
  (should (equal (clutch--json-value-to-string t) "true"))
  (should (equal (clutch--json-value-to-string :false) "false"))
  (should (equal (clutch--json-value-to-string 42) "42")))

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

(ert-deftest clutch-test-dispatch-view-json-string-bypasses-serialize ()
  "String vals should be passed directly to the JSON viewer without re-serialization.
This avoids `json-serialize' escaping non-ASCII characters (e.g. CJK) as \\uXXXX."
  (let ((seen nil)
        (serialize-called nil))
    (cl-letf (((symbol-function 'clutch--view-json-value)
               (lambda (s) (setq seen s)))
              ((symbol-function 'clutch--json-value-to-string)
               (lambda (_v) (setq serialize-called t) "{}")))
      (clutch--dispatch-view "{\"name\":\"张三\"}" '(:type-category json))
      (should (equal seen "{\"name\":\"张三\"}"))
      (should-not serialize-called))))

(ert-deftest clutch-test-json-view-mode-falls-back-when-json-ts-errors ()
  "JSON viewers should tolerate missing tree-sitter grammars."
  (let (selected-mode)
    (with-temp-buffer
      (insert "{\"ok\":true}")
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (error "missing JSON grammar")))
                ((symbol-function 'json-mode)
                 (lambda () (setq selected-mode 'json-mode)))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch--setup-json-view-buffer)))
    (should (eq selected-mode 'json-mode))))

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

(ert-deftest clutch-test-decode-xml-char-refs-string ()
  "XML viewer helper should decode numeric character references."
  (should (equal
           (clutch--decode-xml-char-refs-string
            "<zone>&#x6E7E;&#x5CB8;</zone><operator>&#25998;&#34276;</operator>")
           "<zone>湾岸</zone><operator>斎藤</operator>")))

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

(ert-deftest clutch-test-view-xml-value-decodes-numeric-char-refs-for-display ()
  "XML viewer should display numeric character references as UTF-8 text."
  (let ((buf nil))
    (cl-letf (((symbol-function 'executable-find) (lambda (_cmd) nil))
              ((symbol-function 'nxml-mode) (lambda () nil))
              ((symbol-function 'font-lock-ensure) (lambda (&rest _args) nil))
              ((symbol-function 'jit-lock-fontify-now) (lambda (&rest _args) nil))
              ((symbol-function 'pop-to-buffer)
               (lambda (b &rest _args)
                 (setq buf b)
                 b)))
      (clutch--view-xml-value
       "<?xml version=\"1.0\"?><overlay><zone>&#x6E7E;&#x5CB8;&#x30B1;&#x30FC;&#x30D6;&#x30EB;&#x7DB2;</zone><operator>&#x658E;&#x85E4;</operator></overlay>")
      (with-current-buffer buf
        (let ((text (buffer-string)))
          (should (string-match-p "湾岸ケーブル網" text))
          (should (string-match-p "斎藤" text))
          (should-not (string-match-p "&#x6E7E;" text)))))))

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

;;;; Rendering — cell truncation and padding

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

(ert-deftest clutch-test-string-pad ()
  "Test string padding."
  ;; Left-align (default)
  (should (equal (clutch--string-pad "hi" 5) "hi   "))
  ;; Right-align
  (should (equal (clutch--string-pad "hi" 5 t) "   hi"))
  ;; String longer than width — no padding
  (should (equal (clutch--string-pad "hello" 3) "hello")))

;;;; Rendering — column layout and widths

(ert-deftest clutch-test-column-names ()
  "Test column name extraction from column definitions."
  (let ((columns (list '(:name "id" :type-category numeric)
                       '(:name "name" :type-category text)
                       '(:name "data" :type-category json))))
    (let ((names (clutch--column-names columns)))
      (should (equal names '("id" "name" "data"))))))

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

(ert-deftest clutch-test-visible-columns-renders-all-result-columns ()
  "The result buffer should render every result column into one table."
  (with-temp-buffer
    (setq-local clutch--result-columns '("c1" "c2" "c3" "c4"))
    (should (equal (clutch--visible-columns) '(0 1 2 3)))))

(ert-deftest clutch-test-visible-columns-skips-hidden-row-identity-columns ()
  "Hidden row identity columns should not be rendered as user data."
  (with-temp-buffer
    (setq-local clutch--result-columns '("clutch__rid_0" "id" "name")
                clutch--result-column-defs
                '((:name "clutch__rid_0" :hidden t)
                  (:name "id")
                  (:name "name")))
    (should (equal (clutch--visible-columns) '(1 2)))))

(ert-deftest clutch-test-row-identity-prep-injects-hidden-primary-key ()
  "Simple table SELECTs should receive hidden identity expressions."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name FROM users WHERE active = 1")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :hidden-aliases) '("clutch__rid_0")))
      (should (equal (plist-get prep :sql)
                     "SELECT name, \"id\" AS \"clutch__rid_0\" FROM users WHERE active = 1")))))

(ert-deftest clutch-test-row-identity-prep-preserves-ordinal-order-by ()
  "Hidden identity expressions should not change ORDER BY ordinals."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let ((prep (clutch--prepare-row-identity-query
                 'fake-conn "SELECT name, status FROM users ORDER BY 1")))
      (should (plist-get prep :augmented))
      (should (equal (plist-get prep :sql)
                     "SELECT name, status, \"id\" AS \"clutch__rid_0\" FROM users ORDER BY 1")))))

(ert-deftest clutch-test-row-identity-prep-skips-joined-selects ()
  "Joined SELECTs should not receive table-local hidden identity columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (let* ((sql "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id")
           (prep (clutch--prepare-row-identity-query 'fake-conn sql)))
      (should-not (plist-get prep :augmented))
      (should (equal (plist-get prep :sql) sql)))))

(ert-deftest clutch-test-row-identity-prep-skips-comma-joins-and-derived-selects ()
  "Ambiguous or derived SELECTs should not receive hidden identity columns."
  (cl-letf (((symbol-function 'clutch-db-row-identity-candidates)
             (lambda (_conn _table)
               (list (list :kind 'primary-key
                           :name "PRIMARY"
                           :columns '("id")))))
            ((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    (dolist (sql '("SELECT * FROM users, orders"
                   "WITH x AS (SELECT * FROM users) SELECT * FROM x"
                   "SELECT * FROM (SELECT * FROM users) u"))
      (let ((prep (clutch--prepare-row-identity-query 'fake-conn sql)))
        (should-not (plist-get prep :augmented))
        (should (equal (plist-get prep :sql) sql))))))

(ert-deftest clutch-test-row-identity-finalize-separates-hidden-and-source-pk ()
  "Hidden locator indices and visible source PK indices should stay distinct."
  (let* ((prep (list :table "users"
                     :candidate (list :kind 'primary-key
                                      :name "PRIMARY"
                                      :columns '("id"))
                     :hidden-aliases '("clutch__rid_0")
                     :augmented t))
         (columns (clutch--apply-row-identity-column-metadata
                   '((:name "id") (:name "name") (:name "clutch__rid_0"))
                   prep))
         (row-identity (clutch--finalize-row-identity prep columns)))
    (should (plist-get (nth 2 columns) :hidden))
    (should (equal (plist-get row-identity :indices) '(2)))
    (should (equal (plist-get row-identity :source-indices) '(0)))))

(ert-deftest clutch-test-render-result-includes-all-columns ()
  "Wide tables should keep later columns searchable and reachable by TAB."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("c1" "c2" "c3" "c4"))
    (setq-local clutch--result-column-defs '(nil nil nil nil))
    (setq-local clutch--result-rows '(("a" "b" "c" "needle")))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-inserts nil)
    (setq-local clutch--marked-rows nil)
    (setq-local clutch--sort-column nil)
    (setq-local clutch--sort-descending nil)
    (setq-local clutch--page-current 0)
    (setq-local clutch--page-total-rows 1)
    (setq-local clutch--column-widths [5 5 5 6])
    (clutch--refresh-display)
    (should (string-match-p "needle" (buffer-string)))
    (goto-char (point-min))
    (let ((first (text-property-search-forward 'clutch-col-idx 0 #'eq)))
      (should first)
      (goto-char (prop-match-beginning first)))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 1))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 2))
    (clutch-result-next-cell)
    (should (= (get-text-property (point) 'clutch-col-idx) 3))))

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

;;;; Rendering — row and separator rendering

(ert-deftest clutch-test-refresh-display-preserves-visible-row-position ()
  "Refreshing the result view should not drift point downward on screen."
  (save-window-excursion
    (let ((buf (get-buffer-create " *clutch-refresh-display*")))
      (unwind-protect
          (progn
            (switch-to-buffer buf)
            (with-current-buffer buf
              (clutch-result-mode)
              (setq-local clutch--result-columns '("c1" "c2" "c3"))
              (setq-local clutch--result-column-defs '(nil nil nil))
              (setq-local clutch--result-rows
                          (cl-loop for i from 1 to 40
                                   collect (list (format "row%02d-a" i)
                                                 (format "row%02d-b" i)
                                                 (format "row%02d-c" i))))
              (setq-local clutch--filtered-rows nil)
              (setq-local clutch--pending-edits nil)
              (setq-local clutch--pending-deletes nil)
              (setq-local clutch--pending-inserts nil)
              (setq-local clutch--marked-rows nil)
              (setq-local clutch--sort-column nil)
              (setq-local clutch--sort-descending nil)
              (setq-local clutch--page-current 0)
              (setq-local clutch--page-total-rows 40)
              (setq-local clutch--column-widths [8 8 8])
              (clutch--refresh-display)
              (let* ((win (selected-window))
                     (top-ridx 10)
                     (point-ridx 15))
                (set-window-start win (aref clutch--row-start-positions top-ridx))
                (goto-char (aref clutch--row-start-positions point-ridx))
                (forward-char 2)
                (let ((before-top-ridx
                       (save-excursion
                         (goto-char (window-start win))
                         (clutch-result--row-idx-at-line)))
                      (before-line
                       (count-screen-lines (window-start win)
                                           (line-beginning-position))))
                  (clutch--refresh-display)
                  (should (= (save-excursion
                               (goto-char (window-start win))
                               (clutch-result--row-idx-at-line))
                             before-top-ridx))
                  (should (= (count-screen-lines (window-start win)
                                                 (line-beginning-position))
                             before-line))
                  (clutch--refresh-display)
                  (should (= (save-excursion
                               (goto-char (window-start win))
                               (clutch-result--row-idx-at-line))
                             before-top-ridx))
                  (should (= (count-screen-lines (window-start win)
                                                 (line-beginning-position))
                             before-line))))))
        (when (buffer-live-p buf)
          (kill-buffer buf))))))

(defun clutch-test--setup-rendered-result (&optional rows)
  "Populate the current buffer with a rendered three-column result table.
ROWS defaults to a small three-row sample."
  (let ((rows (or rows '((1 "alpha" "oslo")
                         (2 "bravo" "rome")
                         (3 "charlie" "paris"))))
        (columns '((:name "id" :type-category numeric)
                   (:name "name" :type-category text)
                   (:name "city" :type-category text))))
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name" "city")
                clutch--result-column-defs columns
                clutch--result-rows rows
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--row-identity (clutch-test--primary-row-identity
                                       "users" '("id") '(0))
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows (length rows)
                clutch--query-elapsed nil
                clutch-result-max-rows 100
                clutch--column-widths [3 8 8])
    (clutch--render-result)))

(defun clutch-test--rendered-line-at (ridx)
  "Return rendered line RIDX from the current result buffer."
  (let ((start (aref clutch--row-start-positions ridx))
        (end (or (and (< (1+ ridx) (length clutch--row-start-positions))
                      (aref clutch--row-start-positions (1+ ridx)))
                 (point-max))))
    (buffer-substring start end)))

(ert-deftest clutch-test-replace-row-at-index-changes-only-target-row ()
  "Row replacement should update only the targeted rendered line."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((before0 (substring-no-properties (clutch-test--rendered-line-at 0)))
          (before1 (substring-no-properties (clutch-test--rendered-line-at 1)))
          (before2 (substring-no-properties (clutch-test--rendered-line-at 2))))
      (setq-local clutch--pending-deletes (list (vector 2)))
      (clutch--replace-row-at-index 1)
      (let ((after0 (substring-no-properties (clutch-test--rendered-line-at 0)))
            (after1 (substring-no-properties (clutch-test--rendered-line-at 1)))
            (after2 (substring-no-properties (clutch-test--rendered-line-at 2))))
        (should (equal before0 after0))
        (should (equal before2 after2))
        (should-not (equal before1 after1))
        (should (string-match-p "^│D" after1))))))

(ert-deftest clutch-test-replace-row-at-index-preserves-point-cell ()
  "Row replacement should keep point on the same logical cell."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 2) 2) "表表表")))
    (clutch--goto-cell 1 2)
    (clutch--replace-row-at-index 1)
    (should (= (get-text-property (point) 'clutch-row-idx) 1))
    (should (= (get-text-property (point) 'clutch-col-idx) 2))))

(ert-deftest clutch-test-replace-row-at-index-falls-back-without-row-starts ()
  "Row replacement should fall back to a full redraw without cached row starts."
  (with-temp-buffer
    (let (refreshed)
      (setq-local clutch--result-rows '((1 "alpha" "oslo")))
      (setq-local clutch--filtered-rows nil)
      (setq-local clutch--column-widths [3 8 8])
      (cl-letf (((symbol-function 'clutch--refresh-display)
                 (lambda ()
                   (setq refreshed t))))
        (clutch--replace-row-at-index 0)
        (should refreshed)))))

(ert-deftest clutch-test-reindex-row-starts-from-tracks-buffer-positions ()
  "Row-start reindexing should match actual buffer positions after replacement."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((old-third-start (aref clutch--row-start-positions 2)))
      (setq-local clutch--pending-edits
                  (list (cons (cons (vector 2) 2) "表表表")))
      (clutch--replace-row-at-index 1)
      (let ((actual-third-start (save-excursion
                                  (goto-char (point-min))
                                  (forward-line 2)
                                  (point))))
        (should (= (aref clutch--row-start-positions 2) actual-third-start))
        (should (/= old-third-start actual-third-start))))))

(ert-deftest clutch-test-render-row-line-matches-inserted-output ()
  "Row-line rendering should match the line inserted into the result buffer."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let* ((render-state (clutch--build-render-state))
           (expected (clutch-test--rendered-line-at 1))
           (actual (clutch--render-row-line 1 render-state)))
      (should (equal-including-properties actual expected)))))

(ert-deftest clutch-test-render-cell-uses-render-state-edits ()
  "Cell rendering should use render-state lookups instead of alist scans."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (let* ((clutch--pending-edits '((([1] . 1) . "edited")))
           (render-state (clutch--build-render-state))
           (cell (clutch--render-cell '(1 "before") 0 1 [4 8] render-state)))
      (should (string-match-p "edited" cell))
      (should (eq (get-text-property 3 'clutch-col-idx cell) 1))
      (should (equal (get-text-property 3 'clutch-full-value cell) "edited")))))

(ert-deftest clutch-test-render-cell-fk-face-only-covers-displayed-value ()
  "Foreign-key face should not underline trailing cell padding."
  (with-temp-buffer
    (setq-local clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "customer_id" :type-category text))
                clutch--fk-info '((1 :ref-table "customers" :ref-column "id")))
    (let* ((clutch-column-padding 1)
           (cell (clutch--render-cell '(1 "abc") 0 1 [4 6] nil)))
      (should (eq (get-text-property 2 'face cell) 'clutch-fk-face))
      (should (eq (get-text-property 4 'face cell) 'clutch-fk-face))
      (should-not (get-text-property 5 'face cell)))))

(ert-deftest clutch-test-display-select-result-records-source-table ()
  "SELECT result rendering should cache the detected source table name."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT * FROM orders")
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch--display-select-result '("id") '((1))
                                     '((:name "id" :type-category numeric)))
      (should (equal clutch--result-source-table "orders")))))

(ert-deftest clutch-test-insert-data-rows-marks-pending-edits ()
  "Edited rows should show an E marker in the left prefix."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil
                clutch-result-max-rows 100
                clutch--page-current 0
                clutch--column-widths [4 8]
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--pending-edits '((([1] . 1) . "edited"))
                clutch--pending-deletes nil
                clutch--marked-rows nil)
    (let ((row-positions (make-vector 1 nil)))
      (clutch--insert-data-rows '((1 "before"))
                                row-positions
                                (clutch--build-render-state))
      (should (string-prefix-p "│E  1 " (buffer-string))))))

(ert-deftest clutch-test-record-render-uses-row-identity-keyed-pending-edits ()
  "Record view should render staged edits keyed by row identity."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("id" "name")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "name" :type-category text))
                        clutch--result-rows '((1 "before"))
                        clutch--row-identity (clutch-test--primary-row-identity
                                              "users" '("id") '(0))
                        clutch--pending-edits '((([1] . 1) . "edited"))
                        clutch--fk-info nil))
          (with-temp-buffer
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (let ((rendered (buffer-string)))
              (should (string-match-p "edited" rendered))
              (should-not (string-match-p "before" rendered)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-render-inherits-live-connection-context ()
  "Record view should reuse the parent result buffer connection context."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--connection-params '(:backend mysql :host "db")
                        clutch--conn-sql-product 'mysql
                        clutch--result-columns '("id" "name")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "name" :type-category text))
                        clutch--result-rows '((1 "before"))
                        clutch--pending-edits nil
                        clutch--fk-info nil))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (clutch-record--render)
            (should (eq clutch-connection 'fake-conn))
            (should (equal clutch--connection-params '(:backend mysql :host "db")))
            (should (eq clutch--conn-sql-product 'mysql))
            (let ((line (clutch--header-with-disconnect-badge
                         clutch-record--header-base)))
              (should-not (string-match-p "Disconnected" line)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-open-renders-current-row ()
  "Opening the record view should render the row at point."
  (let (record-buf)
    (unwind-protect
        (with-temp-buffer
          (clutch-result-mode)
          (setq-local clutch-connection nil
                      clutch--connection-params nil
                      clutch--conn-sql-product nil
                      clutch--result-columns '("id" "name")
                      clutch--result-column-defs '((:name "id" :type-category numeric)
                                                   (:name "name" :type-category text))
                      clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                      clutch--filtered-rows nil
                      clutch--pending-edits nil
                      clutch--pending-deletes nil
                      clutch--pending-inserts nil
                      clutch--marked-rows nil
                      clutch--sort-column nil
                      clutch--sort-descending nil
                      clutch--page-current 0
                      clutch--page-total-rows 3
                      clutch--fk-info nil
                      clutch--column-widths [2 5])
          (clutch--refresh-display)
          (goto-char (point-min))
          (let ((match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
            (should match)
            (goto-char (prop-match-beginning match)))
          (cl-letf (((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq record-buf buf)
                       buf)))
            (clutch-result-open-record))
          (should (buffer-live-p record-buf))
          (with-current-buffer record-buf
            (let ((rendered (buffer-string)))
              (should (string-match-p "id" rendered))
              (should (string-match-p "name" rendered))
              (should (string-match-p "id\\s-*:\\s-*2" rendered))
              (should (string-match-p "name\\s-*:\\s-*bob" rendered)))))
      (when (buffer-live-p record-buf)
        (kill-buffer record-buf)))))

(ert-deftest clutch-test-record-next-row-advances ()
  "Record view should advance to the next row."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0
                        clutch-record--expanded-fields nil)
            (cl-letf (((symbol-function 'clutch-record--render) #'ignore))
              (clutch-record-next-row)
              (should (= clutch-record--row-idx 1)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-next-row-errors-at-last ()
  "Record view should error when already at the last row."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))))
          (with-temp-buffer
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 2
                        clutch-record--expanded-fields nil)
            (should-error (clutch-record-next-row) :type 'user-error)))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-record-prev-row-decrements ()
  "Record view should move back to the previous row."
  (with-temp-buffer
    (clutch-record-mode)
    (setq-local clutch-record--row-idx 2
                clutch-record--expanded-fields nil)
    (cl-letf (((symbol-function 'clutch-record--render) #'ignore))
      (clutch-record-prev-row)
      (should (= clutch-record--row-idx 1)))))

(ert-deftest clutch-test-record-prev-row-errors-at-first ()
  "Record view should error when already at the first row."
  (with-temp-buffer
    (clutch-record-mode)
    (setq-local clutch-record--row-idx 0
                clutch-record--expanded-fields nil)
    (should-error (clutch-record-prev-row) :type 'user-error)))

(ert-deftest clutch-test-record-render-errors-when-result-buffer-dead ()
  "Record render should error when its source result buffer is dead."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (kill-buffer result-buf)
    (with-temp-buffer
      (clutch-record-mode)
      (setq-local clutch-record--result-buffer result-buf
                  clutch-record--row-idx 0
                  clutch-record--expanded-fields nil)
      (should-error (clutch-record--render) :type 'user-error))))

(ert-deftest clutch-test-render-separator ()
  "Test table separator line rendering."
  (let ((visible-cols '(0 1 2))
        (widths [5 10 8]))
    (let ((sep (clutch--render-separator visible-cols widths 'top)))
      (should (stringp sep))
      (should (> (length sep) 0)))))

;;;; Rendering — header-line and footer

(ert-deftest clutch-test-header-line-with-hscroll-matches-body-offset ()
  "Header-line hscroll should track body hscroll exactly."
  (with-temp-buffer
    (setq-local clutch--header-line-string "0123456789")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 3)))
      (should (equal (clutch--header-line-with-hscroll) "3456789")))))

(ert-deftest clutch-test-header-line-display-prefixes-align-to-zero ()
  "Header-line display should remain aligned to the window's left edge."
  (with-temp-buffer
    (setq-local clutch--header-line-string "abc")
    (cl-letf (((symbol-function 'window-hscroll) (lambda (&optional _window) 0)))
      (let ((rendered (clutch--header-line-display)))
        (should (equal (substring rendered 1) "abc"))
        (should (equal (get-text-property 0 'display rendered)
                       '(space :align-to 0)))))))

(ert-deftest clutch-test-refresh-footer-line-updates-without-changing-body ()
  "Footer refresh should update mode-line state without touching buffer text."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((body (buffer-string))
          (before (substring-no-properties clutch--footer-base-string)))
      (setq-local clutch--page-total-rows 9)
      (clutch--refresh-footer-line)
      (should (equal body (buffer-string)))
      (should-not (equal before
                         (substring-no-properties clutch--footer-base-string)))
      (should (string-match-p "9"
                              (substring-no-properties clutch--footer-base-string))))))

(ert-deftest clutch-test-refresh-header-line-updates-without-changing-body ()
  "Header refresh should update header state without touching buffer text."
  (with-temp-buffer
    (clutch-test--setup-rendered-result)
    (let ((body (buffer-string))
          (before (substring-no-properties clutch--header-line-string)))
      (setq-local clutch--sort-column "name"
                  clutch--sort-descending t)
      (clutch--refresh-header-line)
      (should (equal body (buffer-string)))
      (should-not (equal before
                         (substring-no-properties clutch--header-line-string))))))

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

(ert-deftest clutch-test-render-footer-includes-sort-and-pending-summary ()
  "Footer should aggregate sort state and staged changes."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--order-by '("created_at" . "desc")
                clutch--pending-edits '(a)
                clutch--pending-deletes '(b)
                clutch--pending-inserts '(c))
    (cl-letf (((symbol-function 'clutch--tx-header-line-segment)
               (lambda (_conn) "Tx: Manual*")))
      (let ((footer (substring-no-properties
                     (clutch--render-footer 10 0 500 100))))
        (should (string-match-p "Tx: Manual\\*" footer))
        (should (string-match-p "DESC\\[created_at\\]" footer))
        (should (string-match-p "E-1 D-1 I-1" footer))
        (should (string-match-p "C-c C-c" footer))
        (should (string-match-p "C-c C-k" footer))
        (should-not (string-match-p "commit:" footer))
        (should-not (string-match-p "discard:" footer))))))

(ert-deftest clutch-test-footer-icon-preserves-icon-family ()
  "Footer icons should keep the nerd-icons font family when tinted."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &rest _fallback)
               (propertize "[files]"
                           'face '(:family "Symbols Nerd Font Mono")))))
    (let* ((icon (clutch--footer-icon '(codicon . "nf-cod-files")
                                      "⊞"
                                      'font-lock-keyword-face))
           (face (get-text-property 0 'face icon)))
      (should (equal face
                     '((:family "Symbols Nerd Font Mono")
                       font-lock-keyword-face))))))

(ert-deftest clutch-test-footer-cursor-part-shows-column-position ()
  "Footer cursor segment should include the current column index and total."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "amount" "created_at"))
    (insert "amount")
    (put-text-property (point-min) (point-max) 'clutch-row-idx 11)
    (put-text-property (point-min) (point-max) 'clutch-col-idx 1)
    (goto-char (point-min))
    (should (string-match-p "R-12:amount-2/3"
                            (substring-no-properties
                             (clutch--footer-cursor-part))))))

(ert-deftest clutch-test-render-footer-warns-when-row-identity-missing ()
  "Footer should explain when edit/delete are disabled due to missing identity."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--last-query "SELECT * FROM users")
    (let* ((footer-prop (clutch--render-footer 10 0 500 100))
           (footer (substring-no-properties footer-prop))
           (identity-start (string-match "row identity missing" footer)))
      (should (string-match-p "row identity missing" footer))
      (should-not (string-match-p "users" footer))
      (should (string-match-p "E/D off" footer))
      (should identity-start)
      (should (equal (get-text-property identity-start 'face footer-prop)
                     '(:inherit font-lock-warning-face :weight normal))))))

(ert-deftest clutch-test-result-mode-does-not-override-mouse-wheel ()
  "Result mode should leave mouse-wheel scrolling to Emacs defaults."
  (should-not (lookup-key clutch-result-mode-map [wheel-up]))
  (should-not (lookup-key clutch-result-mode-map [wheel-down])))

;;;; Filter

(ert-deftest clutch-test-filter-matches-substring-case-insensitive ()
  "Client-side filter should match substrings case-insensitively."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ALI")
      (should (equal clutch--filtered-rows '((1 "alice"))))
      (should (equal clutch--filter-pattern "ALI")))))

(ert-deftest clutch-test-filter-matches-formatted-values ()
  "Client-side filter should match non-string values via their display text."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "value")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "value" :type-category numeric))
                clutch--result-rows '((1 nil) (2 42) (3 "hello"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "42")
      (should (equal clutch--filtered-rows '((2 42))))
      (should (equal clutch--filter-pattern "42")))))

(ert-deftest clutch-test-filter-clear-restores-all-rows ()
  "Clearing the client-side filter should restore the full result set."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--filter-pattern nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) "")))
        (clutch-result-filter))
      (should-not clutch--filtered-rows)
      (should-not clutch--filter-pattern))))

(ert-deftest clutch-test-filter-clears-marked-rows ()
  "Applying the client-side filter should clear marked rows."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows '(0 2)
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (cl-letf (((symbol-function 'clutch--render-result) #'ignore))
      (clutch-result--apply-filter "ali")
      (should-not clutch--marked-rows))))

(ert-deftest clutch-test-apply-filter-errors-without-query ()
  "WHERE filtering should error when there is no last query."
  (with-temp-buffer
    (should-error (clutch-result-apply-filter) :type 'user-error)))

(ert-deftest clutch-test-apply-filter-builds-where-and-executes ()
  "WHERE filtering should build a clause and execute the filtered query."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--result-columns '("id" "name")
                clutch--where-filter nil)
    (let (captured)
      (cl-letf (((symbol-function 'completing-read) (lambda (&rest _args) "id"))
                ((symbol-function 'read-string) (lambda (&rest _args) "> 5"))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn)
                   (setq captured (list sql conn)))))
        (clutch-result-apply-filter)
        (should (equal captured
                       (list (clutch--apply-where "SELECT * FROM t" "id > 5")
                             'fake-conn)))
        (should (equal clutch--where-filter "id > 5"))
        (should (equal clutch--base-query "SELECT * FROM t"))))))

(ert-deftest clutch-test-apply-filter-empty-condition-clears-filter ()
  "Clearing a WHERE filter should reset the stored clause."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--last-query "SELECT * FROM t"
                clutch--base-query "SELECT * FROM t"
                clutch--where-filter "id > 5")
    (let (captured)
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _args) ""))
                ((symbol-function 'clutch--execute)
                 (lambda (sql conn)
                   (setq captured (list sql conn)))))
        (clutch-result-apply-filter)
        (should (equal captured '("SELECT * FROM t" fake-conn)))
        (should-not clutch--where-filter)
        (should-not clutch--base-query)))))

;;;; Rendering — custom column displayers

(ert-deftest clutch-test-register-column-displayer-replaces-and-unregisters ()
  "Column displayer registration should replace existing entries and unregister cleanly."
  (let ((clutch-column-displayers nil)
        (first (lambda (_value) "first"))
        (second (lambda (_value) "second")))
    (clutch-register-column-displayer "Orders" "Status" first)
    (should (eq (clutch--lookup-column-displayer "orders" "status") first))
    (clutch-register-column-displayer "orders" "status" second)
    (should (= (length clutch-column-displayers) 1))
    (should (= (length (cdar clutch-column-displayers)) 1))
    (should (eq (clutch--lookup-column-displayer "ORDERS" "STATUS") second))
    (clutch-unregister-column-displayer "ORDERS" "STATUS")
    (should-not clutch-column-displayers)))

(ert-deftest clutch-test-cell-display-content-uses-case-insensitive-column-displayer ()
  "Custom column displayers should match table and column names case-insensitively."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "Orders" "Status"
     (lambda (value)
       (format "state:%s" value)))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (should (equal
               (clutch--cell-display-content
                7 12 '(:name "STATUS" :type-category numeric) nil)
               "state:7")))))

(ert-deftest clutch-test-cell-display-content-falls-back-when-displayer-returns-nil ()
  "Nil from a column displayer should fall back to the default renderer."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "orders" "status"
     (lambda (_value) nil))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (should (equal
               (clutch--cell-display-content
                "queued" 12 '(:name "status" :type-category text) nil)
               "queued")))))

(ert-deftest clutch-test-cell-display-content-falls-back-when-displayer-errors ()
  "Errors from a column displayer should not escape the result renderer."
  (let ((clutch-column-displayers nil)
        logged)
    (clutch-register-column-displayer
     "orders" "status"
     (lambda (_value)
       (error "Boom")))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (cl-letf (((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq logged (apply #'format fmt args)))))
        (should (equal
                 (clutch--cell-display-content
                  "queued" 12 '(:name "status" :type-category text) nil)
                 "queued"))
        (should (string-match-p "failed: boom" logged))))))

(ert-deftest clutch-test-cell-display-content-truncates-custom-displayer-output ()
  "Custom column displayer output should still be truncated to column width."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "orders" "status"
     (lambda (_value)
       "abcdef"))
    (with-temp-buffer
      (setq-local clutch--result-source-table "orders")
      (should (equal
               (clutch--cell-display-content
                "queued" 4 '(:name "status" :type-category text) nil)
               "abcd")))))

(ert-deftest clutch-test-render-cell-custom-displayer-keeps-full-value-raw ()
  "Custom cell display should keep `clutch-full-value' on the raw value."
  (let ((clutch-column-displayers nil))
    (clutch-register-column-displayer
     "tasks" "status"
     (lambda (_value)
       "done"))
    (with-temp-buffer
      (setq-local clutch--result-source-table "tasks"
                  clutch--result-column-defs
                  '((:name "status" :type-category numeric)))
      (let ((cell (clutch--render-cell '(2) 0 0 [6] nil)))
        (should (string-match-p "done" cell))
        (should (= (get-text-property 2 'clutch-full-value cell) 2))))))

;;;; Rendering — live value viewer

(defun clutch-test--prepare-live-view-source (buffer)
  "Populate BUFFER as a simple result grid for live-view tests."
  (with-current-buffer buffer
    (clutch-result-mode)
    (setq-local clutch--result-source-table "cases")
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-defs
                '((:name "id" :type-category numeric)
                  (:name "name" :type-category text)))
    (setq-local clutch--result-rows '((1 "alice") (2 "bob")))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-deletes nil)
    (setq-local clutch--pending-inserts nil)
    (setq-local clutch--marked-rows nil)
    (setq-local clutch--sort-column nil)
    (setq-local clutch--sort-descending nil)
    (setq-local clutch--page-current 0)
    (setq-local clutch--page-total-rows 2)
    (setq-local clutch--column-widths [2 5])
    (clutch--refresh-display)
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward 'clutch-col-idx 1 #'eq)))
      (goto-char (prop-match-beginning match)))))

(ert-deftest clutch-test-live-view-follows-result-point ()
  "Live viewer should refresh as point moves across result cells."
  (let ((source (generate-new-buffer " *clutch-live-source*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)
              (should (buffer-live-p viewer))
              (with-current-buffer viewer
                (should (string-match-p "alice" (buffer-string))))
              (clutch-result-down-cell)
              (run-hooks 'post-command-hook)
              (with-current-buffer viewer
                (should (string-match-p "bob" (buffer-string)))))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-live-view-freeze-stops-following ()
  "Frozen live viewer should ignore subsequent source point changes."
  (let ((source (generate-new-buffer " *clutch-live-freeze*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)))
          (with-current-buffer viewer
            (clutch--live-view-toggle-freeze))
          (with-current-buffer source
            (clutch-result-down-cell)
            (run-hooks 'post-command-hook))
          (with-current-buffer viewer
            (should (string-match-p "alice" (buffer-string)))
            (clutch--live-view-toggle-freeze)
            (should (string-match-p "bob" (buffer-string)))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

(ert-deftest clutch-test-live-view-quit-detaches-source-hooks ()
  "Closing the live viewer should detach it from the source buffer."
  (let ((source (generate-new-buffer " *clutch-live-quit*"))
        viewer)
    (unwind-protect
        (progn
          (clutch-test--prepare-live-view-source source)
          (with-current-buffer source
            (cl-letf (((symbol-function 'display-buffer)
                       (lambda (buf &rest _args)
                         (setq viewer buf)
                         buf)))
              (clutch-result-live-view-value)
              (should (eq clutch--live-view-buffer viewer))
              (should (memq #'clutch--live-view-source-post-command
                            post-command-hook))))
          (with-current-buffer viewer
            (clutch--live-view-quit))
          (should-not (buffer-live-p viewer))
          (with-current-buffer source
            (should-not clutch--live-view-buffer)
            (should-not (memq #'clutch--live-view-source-post-command
                              post-command-hook))))
      (when (buffer-live-p viewer)
        (kill-buffer viewer))
      (when (buffer-live-p source)
        (kill-buffer source)))))

;;;; Shell command on cell

(ert-deftest clutch-test-shell-command-on-cell-pipes-value ()
  "Shell commands should receive the current cell value as stdin."
  (with-temp-buffer
    (let (captured)
      (cl-letf (((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 "hello world")))
                ((symbol-function 'clutch--view-in-buffer)
                 (lambda (val &rest _args)
                   (setq captured val))))
        (clutch-result-shell-command-on-cell "cat")
        (should (string-match-p "hello world" captured))))))

(ert-deftest clutch-test-shell-command-on-cell-formats-non-string ()
  "Shell commands should receive formatted text for non-string cell values."
  (with-temp-buffer
    (let (captured)
      (cl-letf (((symbol-function 'clutch-result--cell-at-point)
                 (lambda () '(0 1 42)))
                ((symbol-function 'clutch--view-in-buffer)
                 (lambda (val &rest _args)
                   (setq captured val))))
        (clutch-result-shell-command-on-cell "cat")
        (should (string-match-p "42" captured))))))

(ert-deftest clutch-test-shell-command-on-cell-errors-without-cell ()
  "Shell commands should error when point is not on a result cell."
  (with-temp-buffer
    (should-error (clutch-result-shell-command-on-cell "cat") :type 'user-error)))

;;;; SQL parsing — query classification

(ert-deftest clutch-test-schema-affecting-query-p ()
  "DDL-like statements should mark schema stale."
  (should (clutch--schema-affecting-query-p "CREATE TABLE t (id INT)"))
  (should (clutch--schema-affecting-query-p "alter table t add column x int"))
  (should (clutch--schema-affecting-query-p "DROP VIEW v"))
  (should-not (clutch--schema-affecting-query-p "DELETE FROM t"))
  (should-not (clutch--schema-affecting-query-p "SELECT * FROM t")))

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
  ;; Result-set introspection commands also route through the SELECT/result path.
  (should (clutch--select-query-p "SHOW TABLES"))
  (should (clutch--select-query-p "DESCRIBE users"))
  (should (clutch--select-query-p "EXPLAIN SELECT * FROM t"))
  (should-not (clutch--select-query-p "INSERT INTO users VALUES (1)"))
  (should-not (clutch--select-query-p "UPDATE users SET name='x'")))

;;;; SQL parsing — statement bounds

(ert-deftest clutch-test-statement-bounds-ignores-blank-lines ()
  "Statement bounds use only semicolons, ignoring blank lines."
  (with-temp-buffer
    (insert "SELECT *\nFROM users\n\nWHERE id = 1")
    (goto-char 20) ;; inside the statement
    (let ((bounds (clutch--statement-bounds-at-point)))
      (should (equal (string-trim
                      (buffer-substring-no-properties (car bounds) (cdr bounds)))
                     "SELECT *\nFROM users\n\nWHERE id = 1")))))

(ert-deftest clutch-test-statement-bounds-semicolon-delimited ()
  "Statement bounds stop at semicolons."
  (with-temp-buffer
    (insert "SELECT 1;\nSELECT 2;\nSELECT 3")
    (goto-char 15) ;; inside SELECT 2
    (let ((bounds (clutch--statement-bounds-at-point)))
      (should (equal (string-trim
                      (buffer-substring-no-properties (car bounds) (cdr bounds)))
                     "SELECT 2")))))

(ert-deftest clutch-test-statement-bounds-first-statement ()
  "Statement bounds work for the first statement (no leading semicolon)."
  (with-temp-buffer
    (insert "SELECT 1;\nSELECT 2")
    (goto-char 5) ;; inside SELECT 1
    (let ((bounds (clutch--statement-bounds-at-point)))
      (should (equal (string-trim
                      (buffer-substring-no-properties (car bounds) (cdr bounds)))
                     "SELECT 1")))))

(ert-deftest clutch-test-statement-bounds-skips-semicolon-in-string ()
  "Statement bounds skip semicolons inside string literals."
  (with-temp-buffer
    (insert "SELECT 'a;b' AS v;")
    (goto-char 5) ;; inside the SELECT
    (let ((bounds (clutch--statement-bounds-at-point)))
      (should (equal (string-trim
                      (buffer-substring-no-properties (car bounds) (cdr bounds)))
                     "SELECT 'a;b' AS v")))))

(ert-deftest clutch-test-statement-bounds-skips-semicolon-in-comment ()
  "Statement bounds skip semicolons inside line comments."
  (with-temp-buffer
    (insert "SELECT 1 -- foo;\nFROM t;")
    (goto-char 5) ;; inside SELECT 1
    (let ((bounds (clutch--statement-bounds-at-point)))
      (should (equal (string-trim
                      (buffer-substring-no-properties (car bounds) (cdr bounds)))
                     "SELECT 1 -- foo;\nFROM t")))))

(ert-deftest clutch-test-execute-dwim-prefers-semicolon-statement-bounds ()
  "DWIM execution should prefer semicolon-delimited statement bounds."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (clutch-execute-dwim (point) (point))
        (should (equal captured
                       '("INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"
                         "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')")))))))

(ert-deftest clutch-test-execute-dwim-falls-back-to-query-bounds-without-semicolons ()
  "DWIM execution should keep blank-line query parsing when no top-level semicolon exists."
  (with-temp-buffer
    (insert "SELECT 1\n\nSELECT 2")
    (goto-char (point-max))
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (sql beg end &optional _conn)
                   (setq captured
                         (list sql
                               (string-trim
                                (buffer-substring-no-properties beg end)))))))
        (clutch-execute-dwim (point) (point))
        (should (equal captured '("SELECT 2" "SELECT 2")))))))

(ert-deftest clutch-test-execute-region-splits-multiple-statements ()
  "Region execution should split semicolon-delimited statements."
  (with-temp-buffer
    (let ((sql "INSERT INTO demo VALUES (1);\nINSERT INTO demo VALUES (2);")
          executed ensured single-call)
      (insert sql)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)))
                ((symbol-function 'clutch--execute-statements)
                 (lambda (stmts)
                   (setq executed stmts)))
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (&rest _args)
                   (setq single-call t))))
        (clutch-execute-region (point-min) (point-max))
        (should ensured)
        (should (equal executed
                       '("INSERT INTO demo VALUES (1)"
                         "INSERT INTO demo VALUES (2)")))
        (should-not single-call)))))

(ert-deftest clutch-test-execute-buffer-splits-multiple-statements ()
  "Buffer execution should split semicolon-delimited statements."
  (with-temp-buffer
    (let ((sql "INSERT INTO demo VALUES (1);\nINSERT INTO demo VALUES (2);")
          executed ensured single-call)
      (insert sql)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)))
                ((symbol-function 'clutch--execute-statements)
                 (lambda (stmts)
                   (setq executed stmts)))
                ((symbol-function 'clutch--execute-and-mark)
                 (lambda (&rest _args)
                   (setq single-call t))))
        (clutch-execute-buffer)
        (should ensured)
        (should (equal executed
                       '("INSERT INTO demo VALUES (1)"
                         "INSERT INTO demo VALUES (2)")))
        (should-not single-call)))))

;;;; SQL parsing — table and alias extraction

(ert-deftest clutch-test-tables-in-buffer-caches-until-buffer-changes ()
  "Table lookup in the buffer should reuse cached results until text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users")
    (let ((schema (make-hash-table :test 'equal))
          first-cache)
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (should (equal (clutch--tables-in-buffer schema) '("users")))
      (setq first-cache clutch--tables-in-buffer-cache)
      (should (equal (clutch--tables-in-buffer schema) '("users")))
      (should (eq clutch--tables-in-buffer-cache first-cache))
      (goto-char (point-max))
      (insert " JOIN posts")
      (should (equal (clutch--tables-in-buffer schema) '("users" "posts")))
      (should-not (eq clutch--tables-in-buffer-cache first-cache)))))

(ert-deftest clutch-test-tables-in-query-caches-within-statement ()
  "Statement table lookup should reuse cached results until statement or text changes."
  (with-temp-buffer
    (insert "SELECT * FROM users JOIN posts ON users.id = posts.user_id;\n\nSELECT * FROM logs")
    (let ((schema (make-hash-table :test 'equal))
          first-cache second-cache)
      (puthash "users" t schema)
      (puthash "posts" t schema)
      (puthash "logs" t schema)
      (goto-char (point-min))
      (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                     '("posts" "users")))
      (setq first-cache clutch--tables-in-query-cache)
      (search-forward "users.id")
      (should (equal (sort (copy-sequence (clutch--tables-in-query schema)) #'string<)
                     '("posts" "users")))
      (should (eq clutch--tables-in-query-cache first-cache))
      (goto-char (point-max))
      (should (equal (clutch--tables-in-query schema) '("logs")))
      (setq second-cache clutch--tables-in-query-cache)
      (should-not (eq second-cache first-cache))
      (goto-char (point-max))
      (insert " WHERE level = 'error'")
      (should (equal (clutch--tables-in-query schema) '("logs")))
      (should-not (eq clutch--tables-in-query-cache second-cache)))))

(ert-deftest clutch-test-table-scan-does-not-consume-join-as-previous-table-alias ()
  "JOIN should not be consumed as the previous table's optional alias.
Otherwise the scanner skips past the JOIN token and misses the joined table."
  (with-temp-buffer
    (insert "select p.title from users join posts p on users.id = p.user_id")
    (let ((entry (clutch--compute-tables-in-query-cache nil)))
      (should (equal (plist-get entry :statement-tables)
                     '("users" "posts")))
      (should (equal (plist-get entry :statement-aliases)
                     '(("p" . "posts")))))))

(ert-deftest clutch-test-union-all-alias-scoped-to-branch ()
  "The same alias in different UNION ALL branches should resolve branch-locally.
It should not always resolve to the first occurrence."
  ;; Cursor in the second branch: t. should complete with posts columns.
  (with-temp-buffer
    (insert "select t.id from users t\nunion all\nselect t.ti from posts t")
    (goto-char (point-min))
    (search-forward "t.ti")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" '("id" "name") schema)
      (puthash "posts" '("title" "body") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "should not queue async loads when branch columns are cached"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "title" candidates))
          (should-not (member "name" candidates)))))))

(ert-deftest clutch-test-union-all-alias-scoped-inside-subquery ()
  "UNION ALL inside a subquery should still scope aliases per branch."
  ;; Outer: select ... from (...) data_view
  ;; Inner: two branches with same alias t for different tables.
  (with-temp-buffer
    (insert (concat "select data_view.id from (\n"
                    "  select t.id from users t\n"
                    "  union all\n"
                    "  select t.ti from posts t\n"
                    ") data_view"))
    (goto-char (point-min))
    (search-forward "t.ti")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" '("id" "name") schema)
      (puthash "posts" '("title" "body") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "should not queue async loads when branch columns are cached"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "title" candidates))
          (should-not (member "name" candidates)))))))

(ert-deftest clutch-test-extract-tables-and-aliases ()
  "Clutch--extract-tables-and-aliases should preserve table and alias order.
It should retain duplicates for callers that deduplicate later."
  (let* ((sql "SELECT a.id FROM users a JOIN orders b ON a.id = b.uid JOIN users c")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql)))
         (tables (car result))
         (aliases (cdr result)))
    ;; Tables in order, including duplicate "users".
    (should (equal tables '("users" "orders" "users")))
    ;; Aliases in order.
    (should (equal aliases '(("a" . "users") ("b" . "orders") ("c" . "users"))))
    ;; delete-dups (as used by the cache) removes the second "users".
    (should (equal (delete-dups (copy-sequence tables)) '("users" "orders")))))

(ert-deftest clutch-test-extract-tables-and-aliases-handles-schema-qualified-table-without-alias ()
  "Schema-qualified tables without aliases should still parse cleanly."
  (let* ((sql "SELECT * FROM test.users")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
    (should (equal (car result) '("users")))
    (should-not (cdr result))))

(ert-deftest clutch-test-extract-tables-and-aliases-handles-multiline-from-clause ()
  "Table extraction should survive multiline FROM clauses with indentation."
  (with-temp-buffer
    (clutch-mode)
    (let* ((sql "SELECT
    case_code
FROM
    section9_cases_wide
ORDER BY id")
           (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
      (should (equal (car result) '("section9_cases_wide")))
      (should-not (cdr result)))))

(ert-deftest clutch-test-statement-table-identifiers-handle-multiline-from-clause ()
  "Statement table scanning should survive multiline FROM clauses."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT
    case_code
FROM
    section9_cases_wide
ORDER BY id")
    (should (equal (clutch--statement-table-identifiers)
                   '("section9_cases_wide")))))

(ert-deftest clutch-test-normalize-table-token-strips-quotes-from-schema-qualified ()
  "Quoted schema-qualified names like \"HR\".\"EMPLOYEES\" should normalize cleanly.
They should become a bare table name."
  (should (equal (clutch--normalize-statement-table-token "\"HR\".\"EMPLOYEES\"")
                 "EMPLOYEES"))
  (should (equal (clutch--normalize-statement-table-token "`db`.`table`")
                 "table"))
  (should (equal (clutch--normalize-statement-table-token "schema.table")
                 "table"))
  (should (equal (clutch--normalize-statement-table-token "\"EMPLOYEES\"")
                 "EMPLOYEES"))
  (should (equal (clutch--normalize-statement-table-token "users")
                 "users")))

;;;; SQL parsing — string and comment awareness

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

(ert-deftest clutch-test-skip-literal-or-comment-single-quote ()
  "Skip a single-quoted string literal."
  (should (= (clutch-db-sql-skip-literal-or-comment "'hello'" 0) 7))
  ;; Escaped quote via ''
  (should (= (clutch-db-sql-skip-literal-or-comment "'it''s'" 0) 7))
  ;; Unterminated — return length.
  (should (= (clutch-db-sql-skip-literal-or-comment "'hello" 0) 6))
  ;; Not at a literal — return nil.
  (should-not (clutch-db-sql-skip-literal-or-comment "SELECT" 0)))

(ert-deftest clutch-test-skip-literal-or-comment-line-comment ()
  "Skip a -- line comment."
  (should (= (clutch-db-sql-skip-literal-or-comment "-- comment\ncode" 0) 11))
  ;; Comment at end of string (no newline).
  (should (= (clutch-db-sql-skip-literal-or-comment "-- comment" 0) 10)))

(ert-deftest clutch-test-skip-literal-or-comment-block-comment ()
  "Skip a /* block comment */."
  (should (= (clutch-db-sql-skip-literal-or-comment "/* block */" 0) 11))
  ;; Unterminated.
  (should (= (clutch-db-sql-skip-literal-or-comment "/* open" 0) 7)))

(ert-deftest clutch-test-skip-literal-or-comment-ignores-double-quote ()
  "Double-quoted identifiers are NOT literals — should return nil."
  (should-not (clutch-db-sql-skip-literal-or-comment "\"User Table\"" 0)))

(ert-deftest clutch-test-skip-literal-or-comment-ignores-backtick ()
  "Backtick identifiers are NOT literals — should return nil."
  (should-not (clutch-db-sql-skip-literal-or-comment "`user_table`" 0)))

(ert-deftest clutch-test-mask-literal-or-comment ()
  "Mask string literals and comments but preserve identifiers."
  (let ((masked (clutch-db-sql-mask-literal-or-comment
                 "select 'union all' from t -- limit")))
    (should (= (length masked) (length "select 'union all' from t -- limit")))
    ;; String content masked.
    (should (string-match-p "select '         ' from t" masked))
    ;; Comment masked.
    (should-not (string-match-p "limit" masked))
    ;; Delimiters preserved.
    (should (string-match-p "from t" masked)))
  ;; Double-quoted identifiers are NOT masked.
  (let ((masked (clutch-db-sql-mask-literal-or-comment
                 "select * from \"User Table\" u")))
    (should (string-match-p "\"User Table\"" masked)))
  ;; Multibyte string content must not trigger aset errors.
  (let* ((sql "select case when '全提' then '即存' end from t")
         (masked (clutch-db-sql-mask-literal-or-comment sql)))
    (should (= (length masked) (length sql)))
    (should (string-match-p "from t" masked))
    (should-not (string-match-p "全提" masked))
    (should-not (string-match-p "即存" masked))))

(ert-deftest clutch-test-find-top-level-clause-skips-string ()
  "LIMIT inside a string literal should not be found."
  (should-not (clutch-db-sql-find-top-level-clause
               "select 'limit 10' from t" "LIMIT")))

(ert-deftest clutch-test-find-top-level-clause-skips-comment ()
  "LIMIT inside a comment should not be found, but a real one should."
  (should (clutch-db-sql-find-top-level-clause
           "select /* limit */ from t limit 5" "LIMIT"))
  (should (clutch-db-sql-find-top-level-clause
           "select -- limit\nfrom t limit 5" "LIMIT")))

(ert-deftest clutch-test-extract-tables-skips-string-literal ()
  "FROM inside a string literal should not produce a table."
  (let* ((sql "select 'from users' from orders o")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql))))
    (should (equal (car result) '("orders")))
    (should (equal (cdr result) '(("o" . "orders"))))))

(ert-deftest clutch-test-extract-tables-preserves-quoted-identifier ()
  "Backtick-quoted identifiers should still be extracted.
Double-quoted multi-word identifiers are a pre-existing regex limitation."
  (let* ((sql "select * from `order_items` oi join \"users\" u")
         (result (clutch--extract-tables-and-aliases sql 0 (length sql)))
         (tables (car result))
         (aliases (cdr result)))
    (should (member "order_items" tables))
    (should (member "users" tables))
    (should (assoc "oi" aliases))
    (should (assoc "u" aliases))))

(ert-deftest clutch-test-union-branch-skips-string-union ()
  "UNION inside a string should not split branches."
  (let* ((sql "select 'union all' from a union all select * from b")
         ;; Point in the first branch (before the real UNION).
         (range (clutch--union-branch-range sql 5)))
    ;; The range should NOT start after the string — only split at real UNION.
    (should (= (car range) 0))
    (should (< (car range) 26))))

(ert-deftest clutch-test-innermost-paren-skips-string-parens ()
  "Parentheses inside string literals should not affect depth tracking."
  (let* ((sql "select (a, ')', b) from t")
         (range (clutch--innermost-paren-range sql 9)))
    ;; Point at 'a' inside the real parens — range should be (8 . 17).
    ;; ( is at 7, ) is at 17; result is (1+ open) . close-pos.
    (should (= (car range) 8))
    (should (= (cdr range) 17))))

;;;; SQL parsing — WHERE and COUNT rewriting

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

(ert-deftest clutch-test-build-count-sql-preserves-top-level-limit-offset ()
  "Count SQL should count the current limited result set."
  (let ((result (clutch--build-count-sql
                 "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20")))
    (should (string-match-p
             "^SELECT COUNT(\\*) FROM (SELECT id, name FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20) AS _clutch_count\\'"
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
  "Count SQL should preserve top-level LIMIT/OFFSET on UNION queries."
  (let* ((sql "(SELECT id FROM a) UNION ALL (SELECT id FROM b) LIMIT 50 OFFSET 100")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "UNION ALL" result))
    (should (string-match-p "LIMIT 50\\s-+OFFSET 100\\s-*) AS _clutch_count\\'" result))))

(ert-deftest clutch-test-build-count-sql-keeps-window-order-by ()
  "Count SQL should keep ORDER BY inside window OVER clauses."
  (let* ((sql "SELECT row_number() OVER (ORDER BY created_at DESC) AS rn FROM t ORDER BY rn LIMIT 5")
         (result (clutch--build-count-sql sql)))
    (should (string-match-p "OVER (ORDER BY created_at DESC)" result))
    (should (string-match-p "ORDER BY rn\\s-+LIMIT 5\\s-*) AS _clutch_count\\'" result))))

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

(ert-deftest clutch-test-build-count-sql-omits-as-for-oracle-derived-table ()
  "Oracle count rewrite should not use AS before a derived-table alias."
  (cl-letf (((symbol-function 'clutch--connection-oracle-jdbc-p)
             (lambda (&rest _) t)))
    (should (equal (clutch--build-count-sql
                    "SELECT id FROM t FETCH FIRST 50 ROWS ONLY")
                   "SELECT COUNT(*) FROM (SELECT id FROM t FETCH FIRST 50 ROWS ONLY) clutch_count"))))

(ert-deftest clutch-test-apply-where-omits-as-for-oracle-derived-table ()
  "Oracle WHERE rewrite should not use AS before a derived-table alias."
  (cl-letf (((symbol-function 'clutch--connection-oracle-jdbc-p)
             (lambda (&rest _) t)))
    (should (equal (clutch--apply-where "SELECT * FROM t" "id = 1")
                   "SELECT * FROM (SELECT * FROM t) clutch_filter WHERE id = 1"))))

;;;; SQL parsing — LIMIT detection and paging SQL

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

(ert-deftest clutch-test-build-paged-sql-wraps-limited-query-result-set ()
  "Paging should wrap queries with top-level LIMIT so paging stays correct."
  (let ((clutch-connection 'fake-conn)
        captured)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql page-num page-size &optional order-by)
                 (setq captured (list sql page-num page-size order-by))
                 "SELECT * FROM page")))
      (clutch--build-paged-sql
       "SELECT * FROM users ORDER BY created_at DESC LIMIT 1000" 1 500)
      (should (equal captured
                     '("SELECT * FROM (SELECT * FROM users ORDER BY created_at DESC LIMIT 1000) AS _clutch_page"
                       1 500 nil))))))

(ert-deftest clutch-test-build-paged-sql-wraps-limited-query-before-resort ()
  "Resorting a limited query should sort the limited result set, not append to it."
  (let ((clutch-connection 'fake-conn)
        captured)
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql page-num page-size &optional order-by)
                 (setq captured (list sql page-num page-size order-by))
                 "SELECT * FROM page")))
      (clutch--build-paged-sql
       "SELECT * FROM users ORDER BY created_at DESC LIMIT 1000" 0 500 '("name" . "ASC"))
      (should (equal captured
                     '("SELECT * FROM (SELECT * FROM users ORDER BY created_at DESC LIMIT 1000) AS _clutch_page"
                       0 500 ("name" . "ASC")))))))

;;;; Schema cache — refresh and status

(ert-deftest clutch-test-refresh-schema-cache-records-ready-status ()
  "Schema refresh should record ready state and table count."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch-db-list-tables)
               (lambda (_conn) '("users" "orders")))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--refresh-schema-cache 'fake-conn))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready))
      (should (= (plist-get (gethash "fake" clutch--schema-status-cache) :tables)
                 2)))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-ready-status ()
  "Async schema refresh should record ready state and table count."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch-db-refresh-schema-async)
               (lambda (_conn callback &optional _errback)
                 (funcall callback '("users" "orders"))
                 t)))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready))
      (should (= (plist-get (gethash "fake" clutch--schema-status-cache) :tables)
                 2)))))

(ert-deftest clutch-test-refresh-schema-cache-async-ignores-stale-callbacks ()
  "Old async schema refresh callbacks should not overwrite newer state."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0)
        first-callback second-callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch-db-refresh-schema-async)
               (lambda (_conn callback &optional _errback)
                 (if first-callback
                     (setq second-callback callback)
                   (setq first-callback callback))
                 t)))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (should (clutch--refresh-schema-cache-async 'fake-conn))
      (funcall first-callback '("stale_users"))
      (should-not (gethash "fake" clutch--schema-cache))
      (funcall second-callback '("users" "orders"))
      (should (= (hash-table-count (gethash "fake" clutch--schema-cache)) 2))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state)
                  'ready)))))

(ert-deftest clutch-test-refresh-schema-cache-async-records-debug-submit-success-and-stale ()
  "Async schema refresh should record submit, success, and stale-drop events."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-refresh-ticket-counter 0)
        first-callback second-callback)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "fake"))
                  ((symbol-function 'clutch-db-live-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'mysql))
                  ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                  ((symbol-function 'clutch--invalidate-object-warmup) #'ignore)
                  ((symbol-function 'clutch--schedule-object-warmup) #'ignore)
                  ((symbol-function 'clutch-db-refresh-schema-async)
                   (lambda (_conn callback &optional _errback)
                     (if first-callback
                         (setq second-callback callback)
                       (setq first-callback callback))
                     t))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (should (clutch--refresh-schema-cache-async 'fake-conn))
          (should (clutch--refresh-schema-cache-async 'fake-conn))
          (funcall first-callback '("stale_users"))
          (funcall second-callback '("users" "orders"))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: schema-refresh" text))
            (should (string-match-p "Phase: submit" text))
            (should (string-match-p "Phase: success" text))
            (should (string-match-p "Phase: stale-drop" text))))))))

(ert-deftest clutch-test-install-schema-cache-batches-large-refreshes ()
  "Large schema installs should be split across idle slices."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-install-timers (make-hash-table :test 'equal))
        (clutch-schema-cache-install-batch-size 2)
        callbacks
        warmup-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake"))
              ((symbol-function 'clutch-db-live-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
              ((symbol-function 'clutch--schedule-object-warmup)
               (lambda (_conn) (setq warmup-called t)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (push fn callbacks)
                 (intern (format "fake-timer-%s" (length callbacks))))))
      (puthash "fake" 1 clutch--schema-refresh-tickets)
      (puthash "fake" '(:state refreshing) clutch--schema-status-cache)
      (should (clutch--install-schema-cache 'fake-conn '("a" "b" "c") 1))
      (should-not (gethash "fake" clutch--schema-cache))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state) 'refreshing))
      (funcall (car (last callbacks)))
      (should-not (gethash "fake" clutch--schema-cache))
      (funcall (car callbacks))
      (should (= (hash-table-count (gethash "fake" clutch--schema-cache)) 3))
      (should (eq (plist-get (gethash "fake" clutch--schema-status-cache) :state) 'ready))
      (should warmup-called))))

(ert-deftest clutch-test-console-buffer-name-reflects-schema-status ()
  "Console buffer names should expose schema status."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "dev"
                  clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "dev-key")))
        (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema~]"))
        (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema...]"))
        (puthash "dev-key" '(:state ready :tables 42) clutch--schema-status-cache)
        (clutch--update-console-buffer-name)
        (should (equal (buffer-name) "*clutch: dev* [schema 42t]"))))))

(ert-deftest clutch-test-schema-status-header-line-segment ()
  "Schema states should produce the correct header-line segment text."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key")))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema~" 'face 'warning)))
      (puthash "dev-key" '(:state failed) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema!" 'face 'error)))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (should (equal (clutch--schema-status-header-line-segment 'fake-conn)
                     (propertize "schema…" 'face 'shadow))))))

(ert-deftest clutch-test-schema-cache-guidance-reflects-recovery-state ()
  "Schema cache guidance should explain how to recover from non-ready states."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal)))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key")))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (should (string-match-p "C-c C-s"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state failed) clutch--schema-status-cache)
      (should (string-match-p "retry"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (should (string-match-p "in progress"
                              (clutch--schema-cache-guidance 'fake-conn)))
      (puthash "dev-key" '(:state ready :tables 2) clutch--schema-status-cache)
      (should-not (clutch--schema-cache-guidance 'fake-conn)))))

(ert-deftest clutch-test-refresh-current-schema-avoids-duplicate-refresh ()
  "Refreshing state should block duplicate schema refresh attempts."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        refresh-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn) (setq refresh-called t) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (puthash "dev-key" '(:state refreshing) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should-not (clutch--refresh-current-schema))
        (should-not refresh-called)
        (should (string-match-p "already in progress" seen-message))))))

(ert-deftest clutch-test-refresh-current-schema-starts-background-refresh-for-lazy-backends ()
  "Lazy backends should start schema refresh in the background."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn) (setq sync-called t) t))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn) (setq async-called t) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch--refresh-current-schema))
        (should async-called)
        (should-not sync-called)
        (should (string-match-p "started in background" seen-message))))))

(ert-deftest clutch-test-prime-schema-cache-falls-back-to-sync-when-async-unavailable ()
  "Schema priming should fall back to sync refresh when async is unavailable."
  (let (sync-called async-called)
    (cl-letf (((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 t)))
      (clutch--prime-schema-cache 'fake-conn)
      (should async-called)
      (should sync-called))))

(ert-deftest clutch-test-refresh-current-schema-falls-back-to-sync-when-background-refresh-is-unavailable ()
  "Manual schema refresh should fall back to sync when async setup is unavailable."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 nil))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 (puthash "dev-key" '(:state ready :tables 2)
                          clutch--schema-status-cache)
                 t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch--refresh-current-schema))
        (should async-called)
        (should sync-called)
        (should (string-match-p (regexp-quote "Schema refreshed (2 tables)")
                                seen-message))))))

(ert-deftest clutch-test-refresh-schema-command-forces-sync-refresh-on-lazy-backends ()
  "Explicit schema refresh should bypass background refresh for lazy backends."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        seen-message
        sync-called
        async-called)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-eager-schema-refresh-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--refresh-schema-cache-async)
               (lambda (_conn)
                 (setq async-called t)
                 t))
              ((symbol-function 'clutch--refresh-schema-cache)
               (lambda (_conn)
                 (setq sync-called t)
                 (puthash "dev-key" '(:state ready :tables 2)
                          clutch--schema-status-cache)
                 t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq seen-message (apply #'format fmt args)))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (clutch-refresh-schema))
        (should sync-called)
        (should-not async-called)
        (should (string-match-p (regexp-quote "Schema refreshed (2 tables)")
                                seen-message))))))

(ert-deftest clutch-test-clear-connection-metadata-caches-cancels-explicit-key-timers ()
  "Clearing metadata for an explicit key should cancel timers for that key."
  (let ((clutch--schema-cache (make-hash-table :test 'equal))
        (clutch--columns-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--help-doc-cache (make-hash-table :test 'equal))
        (clutch--object-cache (make-hash-table :test 'equal))
        (clutch--schema-status-cache (make-hash-table :test 'equal))
        (clutch--schema-refresh-tickets (make-hash-table :test 'equal))
        (clutch--schema-install-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        canceled)
    (puthash "old-key" 'schema-timer clutch--schema-install-timers)
    (puthash "old-key" 'warmup-timer clutch--object-warmup-timers)
    (cl-letf (((symbol-function 'cancel-timer)
               (lambda (timer)
                 (push timer canceled)))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "new-key"))
              ((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "new-key")))
      (clutch--clear-connection-metadata-caches 'fake-conn "old-key")
      (should (member 'schema-timer canceled))
      (should (member 'warmup-timer canceled))
      (should-not (gethash "old-key" clutch--schema-install-timers))
      (should-not (gethash "old-key" clutch--object-warmup-timers)))))

(ert-deftest clutch-test-describe-table-warns-when-schema-cache-is-stale ()
  "Cache-backed table prompts should surface stale-schema recovery hints."
  (let ((clutch--schema-status-cache (make-hash-table :test 'equal))
        hinted
        described)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--schema-for-connection)
               (lambda ()
                 (let ((h (make-hash-table :test 'equal)))
                   (puthash "users" nil h)
                   h)))
              ((symbol-function 'clutch--read-table-name)
               (lambda (_prompt _tables) "users"))
              ((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-object-describe)
               (lambda (entry)
                 (setq described entry)))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq hinted (apply #'format fmt args)))))
      (puthash "dev-key" '(:state stale) clutch--schema-status-cache)
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (call-interactively #'clutch-describe-table))
      (should (equal described '(:name "users" :type "TABLE")))
      (should (string-match-p "Schema cache is stale" hinted)))))

;;;; Schema cache — column details and metadata

(ert-deftest clutch-test-edit-column-detail-propagates-metadata-errors ()
  "Edit metadata lookup should not silently hide column-detail failures."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-result--detect-table)
               (lambda () "shipping_incidents"))
              ((symbol-function 'clutch--ensure-column-details)
               (lambda (&rest _args)
                 (signal 'clutch-db-error '("column detail boom")))))
      (should-error (clutch-result--column-detail (current-buffer) "severity")
                    :type 'clutch-db-error))))

(ert-deftest clutch-test-result-column-info-works-on-cell-padding ()
  "Column info should resolve from padded whitespace inside a data cell."
  (with-temp-buffer
    (setq-local clutch-column-padding 1
                clutch--result-columns '("name")
                clutch--result-column-defs '((:name "name" :type-category text))
                clutch--result-column-details
                (list (list :name "name" :type "VARCHAR(255)" :nullable t)))
    (insert (clutch--render-cell '("alice") 0 0 [8] nil))
    (goto-char 2)
    (let (seen)
      (cl-letf (((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq seen (apply #'format fmt args)))))
        (clutch-result-column-info)
        (should (string-match-p "name" seen))
        (should (string-match-p "Type: VARCHAR(255)" seen))))))

(ert-deftest clutch-test-ensure-column-details-failure-is-memoized ()
  "Repeated sync detail loads should not reissue the same failing RPC."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (signal 'clutch-db-error '("detail load failed")))))
      (should-not (clutch--ensure-column-details 'fake-conn "users"))
      (should-not (clutch--ensure-column-details 'fake-conn "users"))
      (should (= calls 1))
      (should (eq (plist-get (clutch--column-details-status 'fake-conn "users")
                             :state)
                  'failed)))))

(ert-deftest clutch-test-ensure-table-comment-does-not-cache-db-errors ()
  "Transient table-comment failures should not be memoized as missing comments."
  (let ((clutch--table-comment-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (if (= calls 1)
                     (signal 'clutch-db-error '("comment boom"))
                   "Orders table"))))
      (should-not (clutch--ensure-table-comment 'fake-conn "orders"))
      (should (equal (clutch--ensure-table-comment 'fake-conn "orders")
                     "Orders table"))
      (should (= calls 2)))))

(ert-deftest clutch-test-ensure-help-doc-does-not-cache-transient-query-errors ()
  "Transient HELP lookup failures should not be memoized as missing docs."
  (let ((clutch--help-doc-cache (make-hash-table :test 'equal))
        (calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--run-db-query)
               (lambda (_conn _sql)
                 (cl-incf calls)
                 (if (= calls 1)
                     (signal 'clutch-db-error '("help boom"))
                   (make-clutch-db-result
                    :columns '("name" "description" "example")
                    :rows '(("ABS"
                             "Syntax:\n\nABS(X)\n\nReturns absolute value."
                             "")))))))
      (should-not (clutch--ensure-help-doc 'fake-conn "abs"))
      (should (string-match-p "ABS(X)"
                              (clutch--ensure-help-doc 'fake-conn "abs")))
      (should (= calls 2)))))

(ert-deftest clutch-test-ensure-columns-failure-is-memoized ()
  "Repeated sync column-name loads should not reissue the same failing RPC."
  (let ((clutch--columns-status-cache (make-hash-table :test 'equal))
        (schema (make-hash-table :test 'equal))
        (calls 0))
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (_conn _table)
                 (cl-incf calls)
                 (signal 'clutch-db-error '("column load failed")))))
      (should-not (clutch--ensure-columns 'fake-conn schema "users"))
      (should-not (clutch--ensure-columns 'fake-conn schema "users"))
      (should (= calls 1))
      (should (eq (plist-get (clutch--columns-status 'fake-conn "users")
                             :state)
                  'failed)))))

(ert-deftest clutch-test-ensure-columns-async-falls-back-to-sync-when-unavailable ()
  "Async column preheat should fall back to sync loading when unavailable."
  (let ((clutch--columns-status-cache (make-hash-table :test 'equal))
        (schema (make-hash-table :test 'equal))
        calls)
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-list-columns-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 '("id" "name")))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-columns-async 'fake-conn schema "users"))
      (should (= calls 1))
      (should (equal (gethash "users" schema) '("id" "name")))
      (should-not (plist-get (clutch--columns-status 'fake-conn "users")
                             :state)))))

(ert-deftest clutch-test-column-details-async-ignores-stale-callbacks ()
  "Old async detail callbacks should not overwrite newer table state."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--column-details-ticket-counter 0)
        callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (_conn _table cb &optional _errback)
                 (setq callback cb)
                 t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (clutch--ensure-column-details-async 'fake-conn "users")
      (clutch--set-column-details-status
       'fake-conn "users" 'loading nil (clutch--begin-column-details-ticket))
      (funcall callback (list (list :name "stale_col" :type "int")))
      (should-not (clutch--cached-column-details 'fake-conn "users")))))

(ert-deftest clutch-test-column-details-async-ignores-dead-connection-callbacks ()
  "Async detail callbacks should be ignored after the connection dies."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--column-details-ticket-counter 0)
        (alive t)
        callback)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) alive))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (_conn _table cb &optional _errback)
                 (setq callback cb)
                 t))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (clutch--ensure-column-details-async 'fake-conn "users")
      (setq alive nil)
      (funcall callback (list (list :name "dead_col" :type "int")))
      (should-not (clutch--cached-column-details 'fake-conn "users")))))

(ert-deftest clutch-test-column-details-async-falls-back-to-sync-when-unavailable ()
  "Async column-detail preheat should fall back to sync loading when unavailable."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (clutch--column-details-queue-cache (make-hash-table :test 'equal))
        (clutch--column-details-active-cache (make-hash-table :test 'equal))
        (clutch--column-details-ticket-counter 0)
        refreshed
        calls)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-column-details-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 (list (list :name "id" :type "int"))))
              ((symbol-function 'clutch--refresh-result-metadata-buffers)
               (lambda (_conn table)
                 (setq refreshed table)))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-column-details-async 'fake-conn "users"))
      (should (= calls 1))
      (should (equal refreshed "users"))
      (should (equal (clutch--cached-column-details 'fake-conn "users")
                     '((:name "id" :type "int"))))
      (should-not (clutch--column-details-active 'fake-conn)))))

(ert-deftest clutch-test-ensure-table-comment-async-falls-back-to-sync-when-unavailable ()
  "Async table-comment preheat should fall back to sync loading when unavailable."
  (let ((clutch--table-comment-cache (make-hash-table :test 'equal))
        (clutch--table-comment-status-cache (make-hash-table :test 'equal))
        calls)
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-table-comment-async)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch-db-table-comment)
               (lambda (_conn _table)
                 (setq calls (1+ (or calls 0)))
                 "Users table"))
              ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore))
      (should (clutch--ensure-table-comment-async 'fake-conn "users"))
      (should (= calls 1))
      (should (equal (clutch--cached-table-comment 'fake-conn "users")
                     "Users table"))
      (should-not (plist-get (clutch--table-comment-status 'fake-conn "users")
                             :state)))))

(ert-deftest clutch-test-refresh-result-metadata-buffers-updates-only-matching-results ()
  "Result metadata refresh should only touch matching live result buffers."
  (let ((buf-a (generate-new-buffer " *clutch-result-a*"))
        (buf-b (generate-new-buffer " *clutch-result-b*"))
        (details '((:name "id" :type "int"))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (conn)
                     (pcase conn
                       ('conn-a "conn-a")
                       ('conn-b "conn-b")
                       (_ "other"))))
                  ((symbol-function 'clutch--cached-result-column-details)
                   (lambda (_conn _sql _col-names)
                     details)))
          (with-current-buffer buf-a
            (clutch-result-mode)
            (setq-local clutch-connection 'conn-a)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--last-query "select * from users"))
          (with-current-buffer buf-b
            (clutch-result-mode)
            (setq-local clutch-connection 'conn-b)
            (setq-local clutch--result-columns '("id"))
            (setq-local clutch--last-query "select * from orders"))
          (clutch--refresh-result-metadata-buffers 'conn-a "users")
          (with-current-buffer buf-a
            (should (equal clutch--result-column-details details)))
          (with-current-buffer buf-b
            (should-not clutch--result-column-details)))
      (when (buffer-live-p buf-a)
        (kill-buffer buf-a))
      (when (buffer-live-p buf-b)
        (kill-buffer buf-b)))))

(ert-deftest clutch-test-column-info-string-with-details ()
  "Column info string formats type and nullable info."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-column-details
                (list (list :name "id" :type "INT" :nullable nil)
                      (list :name "name" :type "VARCHAR(255)" :nullable t
                            :default "unnamed")))
    (let ((info0 (clutch--column-info-string 0))
          (info1 (clutch--column-info-string 1)))
      (should (string-match-p "Type: INT" info0))
      (should (string-match-p "Nullable: NO" info0))
      (should (string-match-p "Type: VARCHAR(255)" info1))
      (should (string-match-p "Nullable: YES" info1))
      (should (string-match-p "Default: unnamed" info1)))))

(ert-deftest clutch-test-column-info-string-nil-when-no-details ()
  "Column info string returns nil when details are unavailable."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id"))
    (setq-local clutch--result-column-details nil)
    (should-not (clutch--column-info-string 0))))

(ert-deftest clutch-test-resolve-column-details-maps-by-name ()
  "Detail resolution matches result columns to cached details by name."
  (cl-letf (((symbol-function 'clutch-result--table-from-sql)
             (lambda (_) "users"))
            ((symbol-function 'clutch--cached-column-details)
             (lambda (_conn _table)
               (list (list :name "ID" :type "INT" :nullable nil)
                     (list :name "NAME" :type "VARCHAR" :nullable t)))))
    (let ((result (clutch--resolve-result-column-details
                   'dummy-conn "SELECT id, name FROM users" '("id" "name"))))
      (should (= (length result) 2))
      (should (equal (plist-get (nth 0 result) :type) "INT"))
      (should (equal (plist-get (nth 1 result) :type) "VARCHAR")))))

(ert-deftest clutch-test-resolve-column-details-nil-for-no-table ()
  "Detail resolution returns nil when no source table is detected."
  (cl-letf (((symbol-function 'clutch-result--table-from-sql)
             (lambda (_) nil)))
    (should-not (clutch--resolve-result-column-details
                 'dummy "SELECT 1+1" '("col1")))))

;;;; Completion — SQL keywords

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

(ert-deftest clutch-test-sql-keyword-completion-honors-lowercase-style ()
  "Keyword completion should honor `clutch-sql-completion-case-style'."
  (let ((clutch-sql-completion-case-style 'lower))
    (with-temp-buffer
      (insert "sel")
      (let* ((result (clutch-sql-keyword-completion-at-point))
             (candidates (nth 2 result)))
        (should result)
        (should (member "select" candidates))
        (should-not (member "SELECT" candidates))))))

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

;;;; Completion — identifiers and columns

(ert-deftest clutch-test-insert-completion-at-point-uses-enum-candidates ()
  "Insert buffer completion should return enum candidates for the current field."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "is_ship_blocked" "owner")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "owner" :type-category text))))
          (with-temp-buffer
            (insert "severity: \nis_ship_blocked: \nowner: alice\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity" :type "enum('low','medium','high','critical')")
                               (list :name "is_ship_blocked" :type "tinyint(1)")
                               (list :name "owner" :type "varchar(255)")))))
              (goto-char (point-min))
              (goto-char (clutch-result-insert--current-field-value-position))
              (pcase-let ((`(,beg ,end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (= beg end))
                (should (equal candidates '("low" "medium" "high" "critical"))))
              (forward-line 1)
              (goto-char (clutch-result-insert--current-field-value-position))
              (pcase-let ((`(,_beg ,_end ,candidates . ,_)
                           (clutch-result-insert-completion-at-point)))
                (should (equal candidates '("0" "1"))))
              (forward-line 1)
              (goto-char (clutch-result-insert--current-field-value-position))
              (should-not (clutch-result-insert-completion-at-point)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-complete-field-falls-back-to-completing-read ()
  "Insert completion command should fall back when CAPF does not handle it."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (insert-buf (generate-new-buffer "*clutch-insert-temp*"))
        completion-called)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity")
                        clutch--result-column-defs '((:name "severity" :type-category text))))
          (with-current-buffer insert-buf
            (erase-buffer)
            (insert "severity: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'completion-at-point) (lambda () nil))
                      ((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity"
                                     :type "enum('low','medium','high','critical')"))))
                      ((symbol-function 'completing-read)
                       (lambda (&rest _args)
                         (setq completion-called t)
                         "critical")))
              (goto-char (point-min))
              (goto-char (clutch-result-insert--current-field-value-start))
              (clutch-result-insert-complete-field)
              (should completion-called)
              (should (equal (buffer-string) "severity: critical\n")))))
      (kill-buffer result-buf)
      (kill-buffer insert-buf))))

(ert-deftest clutch-test-edit-complete-field-propagates-capf-errors ()
  "Edit completion should not swallow `completion-at-point' failures."
  (with-temp-buffer
    (setq-local clutch-result-edit--column-name "severity")
    (clutch--result-edit-mode 1)
    (cl-letf (((symbol-function 'completion-at-point)
               (lambda ()
                 (error "CAPF boom"))))
      (should-error (clutch-result-edit-complete-field) :type 'error))))

(ert-deftest clutch-test-insert-complete-field-propagates-capf-errors ()
  "Insert completion should not swallow `completion-at-point' failures."
  (with-temp-buffer
    (insert "severity: \n")
    (clutch-result-insert-mode 1)
    (cl-letf (((symbol-function 'completion-at-point)
               (lambda ()
                 (error "CAPF boom"))))
      (goto-char (point-min))
      (goto-char (clutch-result-insert--current-field-value-start))
      (should-error (clutch-result-insert-complete-field) :type 'error))))

(ert-deftest clutch-test-completion-at-point-keeps-short-prefix-table-only ()
  "Non-keyword short prefixes should not load columns eagerly."
  (with-temp-buffer
    (insert "x")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          called)
      (puthash "xaccounts" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (setq called t)
                   '("id"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "xaccounts" candidates))
          (should-not called))))))

(ert-deftest clutch-test-completion-at-point-uses-cached-columns-for-small-statement-scope ()
  "Identifier completion should reuse cached columns and sync-load cache misses."
  (with-temp-buffer
    (insert "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          loaded)
      (puthash "users" '("id" "name") schema)
      (puthash "orders" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users" "orders")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq loaded table)
                   '("order_id" "user_id")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "sync completion should not queue async column loads"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (equal loaded "orders"))
          (should (member "id" candidates))
          (should (member "order_id" candidates))
          (should (member "users" candidates)))))))

(ert-deftest clutch-test-completion-at-point-keeps-statement-scope-candidates-tight ()
  "Statement-scoped column completion should not leak unrelated schema tables."
  (with-temp-buffer
    (insert "select us from users join posts on users.id = posts.user_id")
    (goto-char (point-min))
    (search-forward "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" nil schema)
      (puthash "posts" nil schema)
      (puthash "logs" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users" "posts")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "should not queue async loads when statement columns are cached"))))
        (puthash "users" '("user_id" "user_name") schema)
        (puthash "posts" '("post_id" "user_id") schema)
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "users" candidates))
          (should (member "posts" candidates))
          (should (member "user_id" candidates))
          (should-not (member "logs" candidates)))))))

(ert-deftest clutch-test-completion-at-point-sync-loads-columns-for-small-statement-scope ()
  "Native statement-scoped completion should sync-load columns on cache miss."
  (with-temp-buffer
    (insert "us")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          loaded)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq loaded table)
                   '("id" "name")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "sync completion should not queue async column loads"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (equal loaded "users"))
          (should (member "id" candidates))
          (should (member "users" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-alias-qualified-table-for-cached-column-loading ()
  "Alias-qualified completion should only use cached columns for the referenced table."
  (with-temp-buffer
    (insert "select u.na from users u join posts p on u.id = p.user_id")
    (goto-char (point-min))
    (search-forward "na")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "users" '("name" "nickname") schema)
      (puthash "posts" '("title" "body") schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "should not queue async loads when alias target is cached"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "name" candidates))
          (should (member "nickname" candidates))
          (should-not (member "title" candidates))
          (should-not (member "users" candidates))
          (should-not (member "posts" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-direct-table-candidates-without-schema-cache ()
  "Direct backend table completion should work without a schema cache."
  (with-temp-buffer
    (insert "select * from zj_")
    (goto-char (point-max))
    (let ((clutch-connection 'fake))
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () nil))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-complete-tables)
                 (lambda (_conn prefix)
                   (should (equal prefix "zj_"))
                   '("ZJ_NCBUSINESSDATA" "ZJ_SYS_PARA"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "ZJ_NCBUSINESSDATA" candidates))
          (should (member "ZJ_SYS_PARA" candidates)))))))

(ert-deftest clutch-test-install-completion-capfs-keeps-identifier-before-keyword ()
  "Identifier completion should precede keyword completion in buffer-local CAPFs."
  (with-temp-buffer
    (setq-local completion-at-point-functions nil)
    (clutch--install-completion-capfs)
    (should (equal completion-at-point-functions
                   '(clutch-completion-at-point
                     clutch-sql-keyword-completion-at-point)))))

(ert-deftest clutch-test-completion-at-point-prefers-table-candidates-in-from-context ()
  "FROM/JOIN table completion should not be shadowed by SQL keyword completion."
  (with-temp-buffer
    (insert "SELECT * FROM OR")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          captured)
      (puthash "ORDERS" nil schema)
      (setq-local completion-at-point-functions nil)
      (clutch--install-completion-capfs)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                (completion-in-region-function
                 (lambda (start end collection &optional predicate)
                   (setq captured
                         (list :input (buffer-substring-no-properties start end)
                               :candidates (all-completions
                                            (buffer-substring-no-properties start end)
                                            collection predicate)))
                   t)))
        (should (completion-at-point))
        (should (equal (plist-get captured :input) "OR"))
        (should (equal (plist-get captured :candidates) '("ORDERS")))))))

(ert-deftest clutch-test-completion-at-point-defers-to-keywords-for-keyword-prefixes ()
  "Statement-start keyword prefixes should not be shadowed by schema tables."
  (with-temp-buffer
    (insert "sele")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "SELECT_LOG" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil)))
        (should-not (clutch-completion-at-point))
        (let* ((capf (clutch-sql-keyword-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "SELECT" candidates))
          (should-not (member "SELECT_LOG" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-direct-column-candidates-when-sync-loads-disabled ()
  "Direct backend column completion should avoid synchronous ensure-columns."
  (with-temp-buffer
    (insert "select pa from ZJ_SYS_PARA")
    (goto-char (point-min))
    (search-forward "pa")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake))
      (puthash "ZJ_SYS_PARA" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (error "Should not synchronously load columns")))
                ((symbol-function 'clutch-db-complete-columns)
                 (lambda (_conn table prefix)
                   (should (equal table "ZJ_SYS_PARA"))
                   (should (equal prefix "pa"))
                   '("PARA_ID" "PARA_NAME"))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (member "PARA_ID" candidates))
          (should (member "PARA_NAME" candidates)))))))

(ert-deftest clutch-test-completion-at-point-uses-alias-qualified-table-for-direct-column-loading ()
  "Alias-qualified completion should only query direct columns for the referenced table."
  (with-temp-buffer
    (insert "select u.pa from ZJ_SYS_PARA u join ZJ_LOG p on u.id = p.para_id")
    (goto-char (point-min))
    (search-forward "pa")
    (let ((schema (make-hash-table :test 'equal))
          (clutch-connection 'fake)
          seen)
      (puthash "ZJ_SYS_PARA" nil schema)
      (puthash "ZJ_LOG" nil schema)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda () schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (error "Should not synchronously load columns")))
                ((symbol-function 'clutch-db-complete-columns)
                 (lambda (_conn table prefix)
                   (push table seen)
                   (should (equal prefix "pa"))
                   (pcase table
                     ("ZJ_SYS_PARA" '("PARA_ID" "PARA_NAME"))
                     ("ZJ_LOG" '("PAYLOAD"))
                     (_ nil)))))
        (let* ((capf (clutch-completion-at-point))
               (candidates (nth 2 capf)))
          (should capf)
          (should (equal seen '("ZJ_SYS_PARA")))
          (should (member "PARA_ID" candidates))
          (should (member "PARA_NAME" candidates))
          (should-not (member "PAYLOAD" candidates))
          (should-not (member "ZJ_SYS_PARA" candidates))
          (should-not (member "ZJ_LOG" candidates)))))))

(ert-deftest clutch-test-completion-at-point-lowercases-identifiers-when-configured ()
  "Identifier completion should honor lowercase case style."
  (let ((clutch-sql-completion-case-style 'lower))
    (with-temp-buffer
      (insert "select pa from ZJ_SYS_PARA")
      (goto-char (point-min))
      (search-forward "pa")
      (let ((schema (make-hash-table :test 'equal))
            (clutch-connection 'fake))
        (puthash "ZJ_SYS_PARA" nil schema)
        (cl-letf (((symbol-function 'clutch--schema-for-connection)
                   (lambda () schema))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--tables-in-current-statement)
                   (lambda (_schema) '("ZJ_SYS_PARA")))
                  ((symbol-function 'clutch-db-completion-sync-columns-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-complete-columns)
                   (lambda (_conn table prefix)
                     (should (equal table "ZJ_SYS_PARA"))
                     (should (equal prefix "pa"))
                     '("PARA_ID" "PARA_NAME"))))
          (let* ((capf (clutch-completion-at-point))
                 (candidates (nth 2 capf)))
            (should capf)
            (should (member "zj_sys_para" candidates))
            (should (member "para_id" candidates))
            (should-not (member "ZJ_SYS_PARA" candidates))))))))

(ert-deftest clutch-test-completion-at-point-swallows-oracle-i18n-completion-errors ()
  "Oracle completion should fail soft when orai18n.jar is missing."
  (let ((clutch--oracle-i18n-warning-shown nil)
        warned)
    (with-temp-buffer
      (insert "select * from zj_")
      (goto-char (point-max))
      (let ((clutch-connection 'fake))
        (cl-letf (((symbol-function 'clutch--schema-for-connection)
                   (lambda () nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-complete-tables)
                   (lambda (_conn _prefix)
                     (signal 'clutch-db-error
                             '("Non supported character set (add orai18n.jar in your classpath): ZHS16GBK"))))
                  ((symbol-function 'message)
                   (lambda (fmt &rest args)
                     (setq warned (apply #'format fmt args)))))
          (should-not (clutch-completion-at-point))
          (should (string-match-p "orai18n.jar" warned))
          (setq warned nil)
          (should-not (clutch-completion-at-point))
          (should-not warned))))))

;;;; Eldoc — schema and column info

(ert-deftest clutch-test-eldoc-schema-string-skips-large-statement-scope ()
  "Eldoc should not synchronously load columns across too many tables."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("a" "b" "c" "d")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (setq called t)
                   nil)))
        (should-not (clutch--eldoc-schema-string 'fake schema "id"))
        (should-not called)))))

(ert-deftest clutch-test-eldoc-schema-string-uses-current-statement-columns ()
  "Eldoc should resolve column info from cached current-statement tables."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id" "name") schema)
      (cl-letf (((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-schema-string 'fake schema "id")
                       "users.id bigint"))))))

(ert-deftest clutch-test-eldoc-schema-string-sync-loads-current-statement-columns ()
  "Native backends should sync-load current statement columns for eldoc."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   '("id" "name")))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (&rest _args)
                   (ert-fail "eldoc should not fall back to async-only column loading here")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-schema-string 'fake schema "id")
                       "users.id bigint"))
        (should (equal called "users"))))))

(ert-deftest clutch-test-eldoc-schema-string-skips-sync-load-when-connection-busy ()
  "Eldoc should not synchronously load columns while CONN is busy.
Instead it should queue async warmup and return nil until metadata is ready."
  (with-temp-buffer
    (let ((schema (make-hash-table :test 'equal))
          async-called)
      (puthash "users" nil schema)
      (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema) '("users")))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error
                           '("sync column load should not run while busy"))))
                ((symbol-function 'clutch--ensure-columns-async)
                 (lambda (_conn _schema table)
                   (setq async-called table)
                   t)))
        (should-not (clutch--eldoc-schema-string 'fake schema "id"))
        (should (equal async-called "users"))))))

(ert-deftest clutch-test-eldoc-schema-string-uses-cached-columns-when-sync-loads-disabled ()
  "Eldoc should not synchronously load columns when backend disables it."
  (let ((schema (make-hash-table :test 'equal)))
    (puthash "ZJ_SYS_PARA" '("PARA_ID" "PARA_NAME") schema)
    (cl-letf (((symbol-function 'clutch-db-completion-sync-columns-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _args) t))
              ((symbol-function 'clutch--ensure-table-comment-async)
               #'ignore)
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "ORCL")))
      (should (string-match-p "ZJ_SYS_PARA"
                              (clutch--eldoc-schema-string 'fake schema "ZJ_SYS_PARA")))
      (should (string-match-p "2 cols"
                              (clutch--eldoc-schema-string 'fake schema "ZJ_SYS_PARA"))))))

(ert-deftest clutch-test-eldoc-on-schema-qualifier-resolves-table-name ()
  "Eldoc on the schema part of schema.table should resolve to the table."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT * FROM test.users;")
    (goto-char (point-min))
    (search-forward "test.users")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id") schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--cached-columns)
                 (lambda (_schema _table) '("id")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (let ((eldoc (clutch--eldoc-function)))
          (should (stringp eldoc))
          (should (string-match-p "users" eldoc))
          (should (string-match-p "1 col" eldoc)))))))

(ert-deftest clutch-test-eldoc-on-uppercase-column-uses-cached-lowercase-metadata ()
  "Uppercase SQL identifiers should still match lowercase cached columns."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT ID FROM users;")
    (goto-char (point-min))
    (search-forward "ID")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal)))
      (puthash "users" '("id") schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "users.id bigint"))))))

(ert-deftest clutch-test-eldoc-qualified-column-bypasses-statement-table-limit ()
  "Alias-qualified field eldoc should not be blocked by large join scopes."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT d.id
FROM accounts a
JOIN customers c ON c.account_id = a.id
JOIN invoices i ON i.customer_id = c.id
JOIN orders_large d ON d.invoice_id = i.id;")
    (goto-char (point-min))
    (search-forward "d.id")
    (goto-char (+ (match-beginning 0) 2))
    (let ((schema (make-hash-table :test 'equal))
          called)
      (dolist (table '("accounts" "customers" "invoices" "orders_large"))
        (puthash table nil schema))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--tables-in-current-statement)
                 (lambda (_schema)
                   '("accounts" "customers" "invoices" "orders_large")))
                ((symbol-function 'clutch--table-aliases-in-current-statement)
                 (lambda (_schema)
                   '(("a" . "accounts")
                     ("c" . "customers")
                     ("i" . "invoices")
                     ("d" . "orders_large"))))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   (should (equal table "orders_large"))
                   '("id" "invoice_id")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s bigint" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "orders_large.id bigint"))
        (should (equal called "orders_large"))))))

(ert-deftest clutch-test-eldoc-field-resolves-multiline-from-table ()
  "Field eldoc should work when FROM and the table name are on separate lines."
  (with-temp-buffer
    (clutch-mode)
    (insert "SELECT
    case_code, operative_name
FROM
    section9_cases_wide
ORDER BY id")
    (goto-char (point-min))
    (search-forward "case_code")
    (goto-char (match-beginning 0))
    (let ((schema (make-hash-table :test 'equal))
          called)
      (puthash "section9_cases_wide" nil schema)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--schema-for-connection)
                 (lambda (&optional _conn) schema))
                ((symbol-function 'clutch-db-busy-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch-db-completion-sync-columns-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--ensure-columns)
                 (lambda (_conn _schema table)
                   (setq called table)
                   '("case_code" "operative_name")))
                ((symbol-function 'clutch--cached-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--table-comment-cached-p)
                 (lambda (&rest _args) t))
                ((symbol-function 'clutch--ensure-table-comment-async)
                 #'ignore)
                ((symbol-function 'clutch-db-database)
                 (lambda (_conn) "testdb"))
                ((symbol-function 'clutch--eldoc-column-string)
                 (lambda (_conn table col-name)
                   (format "%s.%s text" table col-name))))
        (should (equal (clutch--eldoc-function)
                       "section9_cases_wide.case_code text"))
        (should (equal called "section9_cases_wide"))))))

(ert-deftest clutch-test-eldoc-schema-string-queues-background-metadata-on-cache-miss ()
  "Eldoc should queue background metadata when a table cache entry is empty."
  (let ((schema (make-hash-table :test 'equal))
        queued-columns queued-comments)
    (puthash "users" nil schema)
    (cl-letf (((symbol-function 'clutch--cached-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--table-comment-cached-p)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--ensure-columns-async)
               (lambda (_conn _schema table)
                 (push table queued-columns)
                 t))
              ((symbol-function 'clutch--ensure-table-comment-async)
               (lambda (_conn table)
                 (push table queued-comments)
                 t))
              ((symbol-function 'clutch-db-database)
               (lambda (_conn) "appdb")))
      (let ((doc (clutch--eldoc-schema-string 'fake schema "users")))
        (should (stringp doc))
        (should (equal queued-columns '("users")))
        (should (equal queued-comments '("users")))))))

;;;; Xref — alias jump

(ert-deftest clutch-test-xref-alias-jump-basic ()
  "Alias `u' in a WHERE clause should jump to its FROM-clause definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.* FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to the `u' in `u.id'
      (goto-char (point-min))
      (search-forward "u.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          ;; Verify the character at pos is `u' after `users '
          (should (eq (char-after pos) ?u))
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-jump-qualified ()
  "Qualified reference `u.name' should resolve qualifier `u' as the alias."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.name FROM users u")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to `name' in `u.name'
      (goto-char (point-min))
      (search-forward "u.name")
      (goto-char (+ (match-beginning 0) 2)) ;; on `name'
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?u))
          ;; `u' after `users ' in FROM
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-join ()
  "Alias `o' should jump to the JOIN definition, not the FROM alias."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT o.total FROM users u JOIN orders o ON o.uid = u.id")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users") ("o" . "orders"))
                       :statement-tables '("users" "orders")))))
      ;; Move to `o' in `o.total'
      (goto-char (point-min))
      (search-forward "o.total")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "o"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?o))
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-non-alias-returns-no-definitions ()
  "A non-alias symbol should return the symbol but no definitions."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT name FROM users u")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("u" . "users"))
                       :statement-tables '("users")))))
      ;; Move to `name' (not an alias, not qualified)
      (goto-char (point-min))
      (search-forward "name")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "name"))
        (should-not (xref-backend-definitions 'clutch id))))))

(ert-deftest clutch-test-xref-alias-union-scoped ()
  "In a UNION query, alias jump should target the current branch's definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) 'fake-schema))
              ((symbol-function 'clutch--tables-in-query-cache-entry)
               (lambda (_schema)
                 (list :beg (point-min) :end (point-max)
                       :statement-aliases '(("a" . "users") ("a" . "orders"))
                       :statement-tables '("users" "orders")))))
      ;; Move to `a' in second branch (after UNION ALL)
      (goto-char (point-min))
      (search-forward "UNION ALL\n")
      (search-forward "a.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "a"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?a))
          ;; Should be in the second branch (after "orders ")
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-inside-parens ()
  "An alias inside SUM(...) or CASE should still find the FROM definition."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT SUM(fopd.goods_num) FROM\n    ffp_order_plan_detail fopd WHERE fopd.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; cursor on fopd inside SUM(...)
      (goto-char (point-min))
      (search-forward "SUM(fopd")
      (goto-char (- (point) 4)) ;; on `fopd' inside parens
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "fopd"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (equal (buffer-substring-no-properties pos (+ pos 4)) "fopd"))
          (goto-char pos)
          (should (looking-back "ffp_order_plan_detail " (- pos 30))))))))

(ert-deftest clutch-test-xref-alias-multiline-from ()
  "Alias lookup should work when FROM and table name are on separate lines."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT\n    fopd.goods_num\nFROM\n    ffp_order_plan_detail fopd\n    LEFT JOIN ffp_order_plan fop ON fop.plan_id = fopd.plan_id\nWHERE\n    fopd.plan_id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; Move to `fopd' in WHERE clause
      (goto-char (point-min))
      (search-forward "fopd.plan_id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "fopd"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?f))
          (should (equal (buffer-substring-no-properties pos (+ pos 4)) "fopd"))
          (goto-char pos)
          (should (looking-back "ffp_order_plan_detail " (- pos 30))))))))

(ert-deftest clutch-test-xref-alias-no-schema-cache ()
  "Alias lookup should work even when the schema cache is not warmed."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT u.name FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?u))
          (goto-char pos)
          (should (looking-back "users " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-no-schema-union-scoped ()
  "Without schema cache, alias resolution should still scope to UNION branch."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT a.id FROM users a\nUNION ALL\nSELECT a.id FROM orders a")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      ;; cursor in second branch
      (goto-char (point-min))
      (search-forward "UNION ALL\n")
      (search-forward "a.id")
      (goto-char (match-beginning 0))
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "a"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?a))
          (goto-char pos)
          (should (looking-back "orders " (- pos 10))))))))

(ert-deftest clutch-test-xref-alias-quoted ()
  "Alias lookup should handle quoted alias identifiers like `\"u\"'."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT \"u\".name FROM users \"u\" WHERE \"u\".id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "\"u\".id")
      (goto-char (+ (match-beginning 0) 1)) ;; on `u' inside quotes
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "u"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          ;; Should land on the `"' of `"u"' in FROM clause
          (should (eq (char-after pos) ?\")))))))

(ert-deftest clutch-test-xref-ignores-alias-in-comment ()
  "Alias lookup should not jump on alias-like text inside a comment."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT * FROM users u -- use u.id here\nWHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id here")
      (goto-char (match-beginning 0))
      (should-not
       (xref-backend-definitions
        'clutch
        (xref-backend-identifier-at-point 'clutch))))))

(ert-deftest clutch-test-xref-ignores-alias-in-string ()
  "Alias lookup should not jump on alias-like text inside a string."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT 'u.id' AS note FROM users u WHERE u.id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward "u.id'")
      (goto-char (match-beginning 0))
      (should-not
       (xref-backend-definitions
        'clutch
        (xref-backend-identifier-at-point 'clutch))))))

(ert-deftest clutch-test-xref-alias-quoted-multiword ()
  "Alias lookup should handle double-quoted multi-word identifiers."
  (with-temp-buffer
    (clutch-mode)
    (setq-local clutch-connection 'fake-conn)
    (insert "SELECT \"User Name\".id FROM users \"User Name\" WHERE \"User Name\".id = 1")
    (cl-letf (((symbol-function 'clutch--schema-for-connection)
               (lambda (&optional _conn) nil)))
      (goto-char (point-min))
      (search-forward ".id")
      (backward-char 2)
      (let ((id (xref-backend-identifier-at-point 'clutch)))
        (should (equal id "User Name"))
        (let* ((defs (xref-backend-definitions 'clutch id))
               (loc (xref-item-location (car defs)))
               (pos (xref-buffer-location-position loc)))
          (should (eq (char-after pos) ?\")))))))

;;;; Edit — cell editing

(defun clutch-test--make-edit-cell-result-buffer (columns column-defs rows
                                                          &rest locals)
  "Return a result buffer prepared for edit-cell tests.
COLUMNS, COLUMN-DEFS, and ROWS initialize the result grid.  LOCALS is a
plist of additional buffer-local variables to set."
  (let ((buf (generate-new-buffer "*clutch-result*")))
    (with-current-buffer buf
      (setq-local clutch-connection 'fake-conn
                  clutch--result-columns columns
                  clutch--result-column-defs column-defs
                  clutch--result-rows rows)
      (while locals
        (set (make-local-variable (pop locals))
             (pop locals))))
    buf))

(defun clutch-test--open-edit-cell (result-buf cell table &optional details)
  "Open an edit buffer from RESULT-BUF for CELL on TABLE.
DETAILS, when non-nil, is returned by `clutch--ensure-column-details'."
  (cl-letf (((symbol-function 'clutch-result--cell-at-point)
             (lambda () cell))
            ((symbol-function 'clutch-result--detect-table)
             (lambda () table))
            ((symbol-function 'clutch--ensure-column-details)
             (lambda (_conn _table &optional _strict)
               details))
            ((symbol-function 'pop-to-buffer)
             (lambda (buf &rest _args) buf)))
    (with-current-buffer result-buf
      (clutch-result-edit-cell))))

(ert-deftest clutch-test-edit-pending-insert-reopens-prefilled-insert-buffer ()
  "Editing a ghost insert row should reopen the staged insert with its values."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer result-buf
          (setq-local clutch--result-columns '("id" "severity" "owner")
                      clutch--result-rows '((1 "low" "alice"))
                      clutch--pending-inserts '((("severity" . "high")
                                                 ("owner" . "bob"))))
          (cl-letf (((symbol-function 'clutch-result--cell-at-point)
                     (lambda () (list 1 1 "high")))
                    ((symbol-function 'clutch-result--detect-table)
                     (lambda () "shipping_incidents"))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (let ((buf (clutch-result-edit-cell)))
              (with-current-buffer buf
                (should (equal clutch-result-insert--pending-index 0))
                (should (equal clutch-result-insert--table "shipping_incidents"))
                (should (string-match-p "^severity[ ]*: high$" (buffer-string)))
                (should (string-match-p "^owner[ ]*: bob$" (buffer-string)))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-shows-metadata-and-completion-hints ()
  "Edit buffer should expose enum metadata and completion affordances."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("severity")
                     '((:name "severity" :type-category text))
                     '(("low"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("severity") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "low")
                    "shipping_incidents"
                    (list (list :name "severity"
                                :type "enum('low','medium','high')")))))
          (with-current-buffer buf
            (should (string-match-p "\\[enum\\]" (format "%s" header-line-format)))
            (should (string-match-p "M-TAB: complete" (format "%s" header-line-format)))
            (pcase-let ((`(,beg ,end ,candidates . ,_)
                         (clutch-result-edit-completion-at-point)))
              (should (= beg (point-min)))
              (should (= end (point-max)))
              (should (equal candidates '("low" "medium" "high"))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-errors-clearly-without-row-identity ()
  "Edit entry should fail early when the result is not updateable."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("id" "name")
                     '((:name "id" :type-category numeric)
                       (:name "name" :type-category text))
                     '((1 "alice"))
                     'clutch--last-query "SELECT * FROM users")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-result--cell-at-point)
                   (lambda () '(0 1 "alice"))))
          (with-current-buffer result-buf
            (let ((err (should-error (clutch-result-edit-cell)
                                     :type 'user-error)))
              (should (string-match-p
                       "Cannot edit cell: no primary, unique, or row locator identity available for table users"
                       (error-message-string err))))
            (should-not (get-buffer "*clutch-edit: [0].name*"))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-shows-temporal-now-hint ()
  "Temporal edit buffers should advertise the shared now shortcut."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("opened_at")
                     '((:name "opened_at" :type-category datetime))
                     '(("2026-03-10 10:00:00"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("opened_at") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "2026-03-10 10:00:00")
                    "shipping_incidents"
                    (list (list :name "opened_at" :type "datetime")))))
          (with-current-buffer buf
            (should (string-match-p "\\[datetime\\]" (format "%s" header-line-format)))
            (should (string-match-p (regexp-quote "C-c .: now")
                                    (format "%s" header-line-format)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-opens-json-sub-editor-directly ()
  "JSON cells should jump straight into the JSON sub-editor."
  (let ((result-buf (clutch-test--make-edit-cell-result-buffer
                     '("payload")
                     '((:name "payload" :type-category json))
                     '(("{\"a\":1}"))
                     'clutch--row-identity
                     (clutch-test--primary-row-identity
                      "shipping_incidents" '("payload") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    '(0 0 "{\"a\":1}")
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (should (string-match-p "\\*clutch-edit-json: payload\\*" (buffer-name buf)))
          (with-current-buffer buf
            (should (equal clutch-result-edit-json--field-name "payload"))
            (should (string-match-p "JSON field payload"
                                    (format "%s" header-line-format)))
            (should (equal (buffer-substring-no-properties (point-min) (point-max))
                           "{\n  \"a\": 1\n}"))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-json-object-opens-sub-editor-with-json-text ()
  "Parsed JSON objects should reach the JSON sub-editor as JSON text."
  (skip-unless (fboundp 'json-serialize))
  (let* ((payload (make-hash-table :test 'equal))
         (result-buf (clutch-test--make-edit-cell-result-buffer
                      '("payload")
                      '((:name "payload" :type-category json))
                      (list (list payload))
                      'clutch--row-identity
                      (clutch-test--primary-row-identity
                       "shipping_incidents" '("payload") '(0)))))
    (puthash "test" t payload)
    (puthash "data" (vector 1 2) payload)
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    (list 0 0 payload)
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (with-current-buffer buf
            (should-not (string-match-p "#s(hash-table"
                                        (buffer-substring-no-properties
                                         (point-min) (point-max))))
            (should (string-match-p "\"test\": true"
                                    (buffer-substring-no-properties
                                     (point-min) (point-max))))
            (should (string-match-p "\"data\": \\["
                                    (buffer-substring-no-properties
                                     (point-min) (point-max))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-cell-json-string-opens-sub-editor-with-json-text ()
  "Parsed JSON string scalars should stay valid JSON in the sub-editor."
  (skip-unless (fboundp 'json-serialize))
  (let* ((payload "hello")
         (result-buf (clutch-test--make-edit-cell-result-buffer
                      '("payload")
                      '((:name "payload" :type-category json))
                      (list (list payload))
                      'clutch--row-identity
                      (clutch-test--primary-row-identity
                       "shipping_incidents" '("payload") '(0)))))
    (unwind-protect
        (let ((buf (clutch-test--open-edit-cell
                    result-buf
                    (list 0 0 payload)
                    "shipping_incidents"
                    (list (list :name "payload" :type "json")))))
          (with-current-buffer buf
            (should (equal (buffer-substring-no-properties (point-min) (point-max))
                           "\"hello\""))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-set-current-time-replaces-existing-value ()
  "The edit-buffer current-time helper should replace the current value with now."
  (with-temp-buffer
    (insert "2020-01-01 00:00:00")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--column-name "opened_at"
                clutch-result-edit--column-def '(:name "opened_at" :type-category datetime)
                clutch-result-edit--column-detail '(:name "opened_at" :type "datetime"))
    (cl-letf (((symbol-function 'current-time)
               (lambda () (encode-time 30 45 13 12 3 2026))))
      (clutch-result-edit-set-current-time)
      (should (equal (buffer-string) "2026-03-12 13:45:30")))))

;;;; Edit — insert buffer

(ert-deftest clutch-test-insert-buffer-tab-navigation ()
  "Insert buffer TAB navigation should jump between field value positions."
  (with-temp-buffer
    (insert "id: \nname: alice\ncreated_at: \n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (goto-char (clutch-result-insert--current-field-value-position))
    (should (= (current-column) (length "id: ")))
    (clutch-result-insert-next-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "name: alice"))
    (should (= (point) (line-end-position)))
    (clutch-result-insert-next-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "created_at: "))
    (should (= (current-column) (length "created_at: ")))
    (clutch-result-insert-prev-field)
    (should (equal (buffer-substring-no-properties
                    (line-beginning-position) (line-end-position))
                   "name: alice"))
    (should (= (point) (line-end-position)))))

(ert-deftest clutch-test-insert-mode-annotates-existing-buffer-lines ()
  "Insert mode should annotate hand-written form text with field properties."
  (with-temp-buffer
    (insert "severity: high\nowner: bob\n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (should (equal (get-text-property (point) 'clutch-insert-field-name)
                   "severity"))
    (search-forward "bob")
    (backward-char 1)
    (should (equal (get-text-property (point) 'clutch-insert-field-name)
                   "owner"))
    (should (equal (clutch-result-insert--current-field-name) "owner"))))

(ert-deftest clutch-test-insert-return-key-navigates-like-tab ()
  "RET should advance to the next insert field without replacing TAB."
  (with-temp-buffer
    (insert "id: \nname: alice\ncreated_at: \n")
    (clutch-result-insert-mode 1)
    (goto-char (point-min))
    (goto-char (clutch-result-insert--current-field-value-start))
    (call-interactively (key-binding (kbd "RET")))
    (should (equal (clutch-result-insert--current-field-name) "name"))
    (call-interactively (key-binding (kbd "TAB")))
    (should (equal (clutch-result-insert--current-field-name) "created_at"))))

(ert-deftest clutch-test-insert-buffer-renders-tight-tags-and-highlights-current-line ()
  "Insert buffer should keep tags tight to the colon and highlight the active field line."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "is_ship_blocked")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents"
                        clutch-result-insert--show-all-fields t)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "id" :type "int" :generated t :nullable nil)
                               (list :name "severity" :type "enum('low','medium')" :nullable nil)
                               (list :name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)))))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("id" "severity" "is_ship_blocked"))
              (let (prefixes)
                (goto-char (point-min))
                (while (not (eobp))
                  (let* ((field (clutch-result-insert--current-field-or-error))
                         (bounds (clutch-result-insert--field-value-bounds field)))
                    (push (buffer-substring-no-properties (line-beginning-position)
                                                          (car bounds))
                          prefixes))
                  (forward-line 1))
                (setq prefixes (nreverse prefixes))
                (should (string-match-p "^id[ ]+\\[generated\\]: $"
                                        (nth 0 prefixes)))
                (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                        (nth 1 prefixes)))
                (should (string-match-p "^is_ship_blocked \\[default=0 bool\\]: $"
                                        (nth 2 prefixes)))))
              (goto-char (point-min))
              (clutch-result-insert-next-field)
              (let ((ov clutch-result-insert--active-field-overlay))
                (should (overlayp ov))
                (should (eq (overlay-get ov 'face) 'clutch-insert-active-field-face))
                (should (string-match-p "^severity" (buffer-substring-no-properties
                                                     (overlay-start ov)
                                                     (overlay-end ov))))
              (let ((prefix-ov clutch-result-insert--active-prefix-overlay))
                (should (overlayp prefix-ov))
                (should (eq (overlay-get prefix-ov 'face)
                            'clutch-insert-active-field-name-face))
                (should (string-match-p "^severity[ ]+\\[enum required\\]$"
                                        (buffer-substring-no-properties
                                         (overlay-start prefix-ov)
                                         (overlay-end prefix-ov))))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-pending-insert-renders-generated-and-default-placeholders ()
  "Staged insert rows should show generated/default placeholders when known."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name" "created_at" "notes")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)
                                             (:name "created_at" :type-category datetime)
                                             (:name "notes" :type-category text))
                clutch--pending-inserts '((("name" . "alice"))))
    (let ((row-positions (make-vector 1 nil))
          render-state)
      (cl-letf (((symbol-function 'clutch-result--detect-table) (lambda () "users"))
                ((symbol-function 'clutch--ensure-column-details)
                 (lambda (_conn _table)
                   (list (list :name "id" :generated t)
                         (list :name "name")
                         (list :name "created_at" :default "CURRENT_TIMESTAMP")
                         (list :name "notes")))))
        (setq render-state (clutch--build-render-state))
        (clutch--insert-pending-insert-rows '(0 1 2 3) [12 12 12 12] 3 0 row-positions
                                            render-state)
        (let ((rendered (buffer-string)))
          (should (string-match-p "<generated>" rendered))
          (should (string-match-p "<default>" rendered))
          (should (string-match-p "alice" rendered)))))))

(ert-deftest clutch-test-pending-insert-uses-insert-markers ()
  "Staged insert rows should use insert semantics in the left prefix."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--pending-inserts '((("id" . "99") ("name" . "new"))))
    (let ((row-positions (make-vector 1 nil)))
      (clutch--insert-pending-insert-rows '(0 1) [8 8] 3 0 row-positions
                                          (clutch--build-render-state))
      (should (string-prefix-p "│I I1 " (buffer-string))))))

(ert-deftest clutch-test-insert-fill-current-time-respects-column-type ()
  "The insert buffer time-filling helper should use result column metadata."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch--result-columns '("due_on" "created_at" "name")
                        clutch--result-column-defs '((:name "due_on" :type-category date)
                                                     (:name "created_at" :type-category datetime)
                                                     (:name "name" :type-category text))))
          (with-temp-buffer
            (insert "due_on: 2024-01-01\ncreated_at: 2024-01-01 00:00:00\nname: alice\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf)
            (cl-letf (((symbol-function 'current-time)
                       (lambda () (encode-time 30 45 13 12 3 2026))))
              (goto-char (point-min))
              (clutch-result-insert-fill-current-time)
              (should (equal (buffer-substring-no-properties
                              (line-beginning-position) (line-end-position))
                             "due_on: 2026-03-12"))
              (forward-line 1)
              (clutch-result-insert-fill-current-time)
              (should (equal (buffer-substring-no-properties
                              (line-beginning-position) (line-end-position))
                             "created_at: 2026-03-12 13:45:30"))
              (forward-line 1)
              (should-error (clutch-result-insert-fill-current-time)
                            :type 'user-error))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-buffer-labels-show_field_metadata ()
  "Insert buffer labels should show field metadata without changing parsed names."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "postmortem" "is_ship_blocked" "opened_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "postmortem" :type-category json)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "opened_at" :type-category datetime))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents"
                        clutch-result-insert--show-all-fields t)
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "id" :type "int" :generated t :nullable nil)
                               (list :name "severity" :type "enum('low','medium')" :nullable nil)
                               (list :name "postmortem" :type "json" :nullable t)
                               (list :name "is_ship_blocked" :type "tinyint(1)" :default "0" :nullable nil)
                               (list :name "opened_at" :type "datetime" :nullable nil)))))
              (clutch-result-insert--populate-buffer "shipping_incidents"
                                                    '("id" "severity" "postmortem" "is_ship_blocked" "opened_at"))
              (let ((rendered (buffer-string)))
                (should (string-match-p "^id[ ]+\\[generated\\]: $" rendered))
                (should (string-match-p "^severity[ ]+\\[enum required\\]: $" rendered))
                (should (string-match-p "^postmortem[ ]+\\[json\\]: $" rendered))
                (should (string-match-p "^is_ship_blocked \\[default=0 bool\\]: $" rendered))
                (should (string-match-p "^opened_at[ ]+\\[datetime required\\]: $" rendered)))
              (goto-char (point-min))
              (should (get-text-property (point) 'read-only))
              (should (eq (get-text-property (point) 'face)
                          'clutch-field-name-face))
              (search-forward "[generated]")
              (should (eq (get-text-property (1- (point)) 'face)
                          'clutch-insert-field-tag-face))
              (goto-char (point-min))
              (search-forward "severity")
              (goto-char (clutch-result-insert--current-field-value-position))
              (insert "low")
              (goto-char (point-min))
              (let ((fields (clutch-result-insert--parse-fields)))
                (should (equal fields '(("severity" . "low"))))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-populate-buffer-reuses-read-only-buffer ()
  "Repopulating an insert buffer should work when prefixes are already read-only."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "owner")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "owner" :type-category text))))
          (with-temp-buffer
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity"
                                     :type "enum('low','medium','high')")
                               (list :name "owner" :type "varchar(64)")))))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("severity" "owner"))
              (should (get-text-property (point-min) 'read-only))
              (clutch-result-insert--populate-buffer
               "shipping_incidents" '("severity" "owner")
               '(("severity" . "high")))
              (should (string-match-p "^severity \\[enum required\\]: high$"
                                      (buffer-string))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-sparse-layout-toggle-preserves-hidden-values ()
  "Sparse insert layout should hide defaulted fields without dropping their values."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("id" "severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :default "system" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $" (buffer-string)))
            (should-not (string-match-p "^owner" (buffer-string)))
            (should-not (string-match-p "^created_at" (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (goto-char (point-min))
            (re-search-forward "^owner.*: " nil t)
            (insert "bob")
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                    (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^owner[ ]+\\[default=system\\]: bob$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-prefills-effective-result-values ()
  "Cloning a result row should reuse visible row values and staged edits."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM shipping_incidents"
                        clutch--result-columns '("id" "severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))
                        clutch--result-rows '((1 "low" "alice" "2026-03-01 10:00:00"))
                        clutch--filtered-rows nil
                        clutch--row-identity (clutch-test--primary-row-identity
                                              "shipping_incidents" '("id") '(0))
                        clutch--pending-edits '((([1] . 1) . "high"))))
          (cl-letf (((symbol-function 'clutch-result--row-idx-at-line)
                     (lambda () 0))
                    ((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "severity" :type "enum('low','medium','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer result-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                    (buffer-string)))
            (should (string-match-p "^owner[ ]*: alice$" (buffer-string)))
            (should (string-match-p "^created_at .*2026-03-01 10:00:00$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-works-from-record-buffer ()
  "Cloning from a record buffer should prefill from the current record row."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        (record-buf (generate-new-buffer "*clutch-record*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM shipping_incidents"
                        clutch--result-columns '("id" "owner")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "owner" :type-category text))
                        clutch--result-rows '((7 "carol"))))
          (with-current-buffer record-buf
            (clutch-record-mode)
            (setq-local clutch-record--result-buffer result-buf
                        clutch-record--row-idx 0))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :generated t :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer record-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^owner[ ]*: carol$" (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (when (buffer-live-p record-buf)
        (kill-buffer record-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-clone-row-to-insert-hides-primary-key-by-default ()
  "Clone-to-insert should not prefill or sparsely render primary-key fields."
  (let ((result-buf (generate-new-buffer "*clutch-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (clutch-result-mode)
            (setq-local clutch-connection 'fake-conn
                        clutch--last-query "SELECT * FROM incident_codes"
                        clutch--result-columns '("id" "label")
                        clutch--result-column-defs '((:name "id" :type-category numeric)
                                                     (:name "label" :type-category text))
                        clutch--result-rows '((42 "duplicate me"))
                        clutch--filtered-rows nil))
          (cl-letf (((symbol-function 'clutch-result--row-idx-at-line)
                     (lambda () 0))
                    ((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "id" :type "int" :primary-key t :nullable nil)
                             (list :name "label" :type "varchar(64)" :nullable nil))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (with-current-buffer result-buf
              (clutch-clone-row-to-insert)))
          (with-current-buffer insert-buf
            (should-not (string-match-p "^id" (buffer-string)))
            (should (string-match-p "^label[ ]+\\[required\\]: duplicate me$"
                                    (buffer-string)))
            (clutch-result-insert-toggle-field-layout)
            (should (string-match-p "^id[ ]+\\[required\\]: $" (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-import-delimited-parses-quoted-csv-row ()
  "Single-row CSV import should prefill the current insert form."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("owner" "severity")
                        clutch--result-column-defs '((:name "owner" :type-category text)
                                                     (:name "severity" :type-category text))))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "severity" :type "enum('low','high')" :nullable nil))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (clutch-result-insert-import-delimited
             "owner,severity\n\"Bob, Jr.\",high\n")
            (should (string-match-p "^owner[ ]*: Bob, Jr\\.$" (buffer-string)))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: high$"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-import-delimited-stages-multi-row-header-mapping ()
  "Multi-row delimited import should stage inserts by header names."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        insert-buf)
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "owner" "created_at")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "owner" :type-category text)
                                                     (:name "created_at" :type-category datetime))
                        clutch--pending-inserts nil))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (_conn _table)
                       (list (list :name "severity" :type "enum('low','high')" :nullable nil)
                             (list :name "owner" :type "varchar(64)" :nullable t)
                             (list :name "created_at" :type "datetime"
                                   :default "CURRENT_TIMESTAMP" :nullable t))))
                    ((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq insert-buf buf)
                       buf)))
            (clutch-result-insert--open-buffer "shipping_incidents" result-buf))
          (with-current-buffer insert-buf
            (clutch-result-insert-import-delimited
             "owner\tseverity\nbob\thigh\nann\tlow\n")
            (should (equal (with-current-buffer result-buf
                             clutch--pending-inserts)
                           '((("owner" . "bob") ("severity" . "high"))
                              (("owner" . "ann") ("severity" . "low")))))
            (should (string-match-p "^severity[ ]+\\[enum required\\]: $"
                                    (buffer-string)))))
      (when (buffer-live-p insert-buf)
        (kill-buffer insert-buf))
      (kill-buffer result-buf))))

;;;; Edit — staged mutations (row identity)

(ert-deftest clutch-test-insert-commit-replaces-existing-pending-insert ()
  "Committing a re-edited insert should replace the staged entry in place."
  (let ((result-buf (generate-new-buffer "*clutch-result*")))
    (unwind-protect
        (with-current-buffer result-buf
          (setq-local clutch--pending-inserts '((("severity" . "low"))))
          (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
                    ((symbol-function 'quit-window) #'ignore))
            (with-temp-buffer
              (insert "severity: high\n")
              (clutch-result-insert-mode 1)
              (setq-local clutch-result-insert--result-buffer result-buf
                          clutch-result-insert--pending-index 0)
              (clutch-result-insert-commit))
            (should (equal clutch--pending-inserts
                           '((("severity" . "high")))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-build-render-state-creates-fast-lookups ()
  "Render-state tables should preserve staged edit/delete/mark semantics."
  (with-temp-buffer
    (setq-local clutch--pending-edits '((([1] . 1) . "edited")))
    (setq-local clutch--pending-deletes '([1]))
    (setq-local clutch--marked-rows '(0 2))
    (let* ((state (clutch--build-render-state))
           (edits (plist-get state :edits))
           (edit-rows (plist-get state :edit-rows))
           (marked (plist-get state :marked))
           (deletes (plist-get state :deletes)))
      (should (equal (gethash '([1] . 1) edits)
                     '(([1] . 1) . "edited")))
      (should (gethash [1] edit-rows))
      (should (gethash 0 marked))
      (should (gethash 2 marked))
      (should (gethash [1] deletes)))))

(ert-deftest clutch-test-apply-edit-errors-clearly-without-row-identity ()
  "Edit staging should explain why update/delete are disabled."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT * FROM users"
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil)
    (condition-case err
        (progn
          (clutch-result--apply-edit 0 1 "after")
          (should nil))
      (user-error
       (should (string-match-p
                "no primary, unique, or row locator identity available for table users"
                (error-message-string err)))))))

(ert-deftest clutch-test-delete-rows-errors-clearly-without-row-identity ()
  "Delete staging should explain why update/delete are disabled."
  (with-temp-buffer
    (setq-local clutch--last-query "SELECT * FROM users"
                clutch--result-rows '((1 "before"))
                clutch--filtered-rows nil)
    (cl-letf (((symbol-function 'clutch-result--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (condition-case err
          (progn
            (clutch-result-delete-rows)
            (should nil))
        (user-error
         (should (string-match-p
                  "no primary, unique, or row locator identity available for table users"
                  (error-message-string err))))))))

(ert-deftest clutch-test-build-delete-stmt-variants ()
  "DELETE builder should handle primary-key variants correctly."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (dolist (case '(("single"
                       "orders"
                       (42 "alice" "2024-01-15")
                       ("id" "customer" "created_at")
                       (0)
                       "DELETE FROM \"orders\" WHERE \"id\" = ?"
                       (42))
                      ("compound"
                       "order_items"
                       (7 99 3)
                       ("order_id" "item_id" "qty")
                       (0 1)
                       "DELETE FROM \"order_items\" WHERE \"order_id\" = ? AND \"item_id\" = ?"
                       (7 99))
                      ("null"
                       "events"
                       (nil "orphan")
                       ("parent_id" "name")
                       (0)
                       "DELETE FROM \"events\" WHERE \"parent_id\" IS NULL"
                       ())))
        (pcase-let ((`(,label ,table ,row ,columns ,pk-indices ,sql ,params) case))
          (ert-info ((format "delete case: %s" label))
            (let* ((row-identity
                    (list :kind 'primary-key
                          :name "PRIMARY"
                          :table table
                          :columns (mapcar (lambda (i) (nth i columns))
                                           pk-indices)
                          :indices pk-indices))
                   (stmt (clutch-result--build-delete-stmt
                          table row columns row-identity)))
              (should (equal (car stmt) sql))
              (should (equal (cdr stmt) params))
              (should-not (member nil (cdr stmt))))))))))

(ert-deftest clutch-test-build-insert-sql-returns-template-and-params ()
  "INSERT builder should return a template plus parameter values."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id))))
      (let ((stmt (clutch-result-insert--build-sql
                   'fake-conn
                   "users"
                   '(("id" . "7")
                     ("name" . "alice")
                     ("note" . "NULL")))))
        (should (equal (car stmt)
                       "INSERT INTO \"users\" (\"id\", \"name\", \"note\") VALUES (?, ?, ?)"))
        (should (equal (cdr stmt) '("7" "alice" nil)))))))

(ert-deftest clutch-test-stage-delete-stores-row-identity-vec ()
  "Staging a delete stores an identity vector, not a ridx integer."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes nil)
    (cl-letf (((symbol-function 'clutch-result--selected-row-indices) (lambda () '(0)))
              ((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-delete-rows)
      (should (equal clutch--pending-deletes (list (vector 42)))))))

(ert-deftest clutch-test-commit-delete-uses-row-identity-vec ()
  "DELETE statement uses identity values from stored vector, not ridx."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--pending-deletes (list (vector 42)))
    (cl-letf (((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name))))
      (let ((stmts (clutch-result--build-pending-delete-statements)))
        (should (= (length stmts) 1))
        (should (equal (caar stmts) "DELETE FROM `users` WHERE `id` = ?"))
        (should (equal (cdar stmts) '(42)))))))

(ert-deftest clutch-test-stage-edit-stores-row-identity-vec ()
  "Staging an edit stores (identity-vec . cidx) key, not (ridx . cidx)."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 7 "bob")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-edits nil)
    (cl-letf (((symbol-function 'clutch--refresh-display) #'ignore)
              ((symbol-function 'clutch-result--detect-table) (lambda () "users")))
      (clutch-result--apply-edit 0 1 "carol")
      (should (= (length clutch--pending-edits) 1))
      (let ((key (caar clutch--pending-edits)))
        (should (vectorp (car key)))
        (should (equal (car key) (vector 7)))
        (should (= (cdr key) 1))))))

(ert-deftest clutch-test-commit-edit-generates-update-with-primary-key-identity-where ()
  "UPDATE statement uses primary-key identity values in WHERE clause."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 7) 1) "carol")))
    (cl-letf (((symbol-function 'clutch-result--detect-table) (lambda () "users"))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "`%s`" name))))
      (let ((stmts (clutch-result--build-update-statements)))
        (should (= (length stmts) 1))
        (should (equal (caar stmts)
                       "UPDATE `users` SET `name` = ? WHERE `id` = ?"))
        (should (equal (cdar stmts) '("carol" 7)))))))

(ert-deftest clutch-test-build-update-stmt-uses-row-locator-where ()
  "UPDATE builder should use backend row locator predicates when available."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "\"%s\"" name))))
      (let* ((row-identity (list :kind 'row-locator
                                 :table "users"
                                 :indices '(0)
                                 :hidden-aliases '("clutch__rid_0")
                                 :where-sql "ctid = ?::tid"))
             (stmt (clutch-result--build-update-stmt
                    "users" (vector "(0,1)") '((1 . "carol"))
                    '("clutch__rid_0" "name") row-identity)))
        (should (equal (car stmt)
                       "UPDATE \"users\" SET \"name\" = ? WHERE ctid = ?::tid"))
        (should (equal (cdr stmt) '("carol" "(0,1)")))))))

(ert-deftest clutch-test-discard-delete-removes-row-identity-entry ()
  "Discarding a delete should remove the matching staged delete."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (setq-local clutch--result-rows (list (list 42 "alice")))
    (setq-local clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (setq-local clutch--filtered-rows nil)
    (setq-local clutch--pending-deletes (list (vector 42)))
    (setq-local clutch--pending-edits nil)
    (setq-local clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch-result--row-idx-at-line) (lambda () 0))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-deletes)))))

(ert-deftest clutch-test-discard-insert-removes-entry ()
  "Discarding a ghost insert row should remove the staged insert."
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

(ert-deftest clutch-test-discard-edit-removes-cell-entry ()
  "Discarding an edited cell should remove the matching staged edit."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-rows (list (list 42 "alice"))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0))
                clutch--filtered-rows nil
                clutch--pending-edits (list (cons (cons (vector 42) 1) "carol"))
                clutch--pending-deletes nil
                clutch--pending-inserts nil)
    (cl-letf (((symbol-function 'clutch-result--row-idx-at-line) (lambda () 0))
              ((symbol-function 'clutch--col-idx-at-point) (lambda () 1))
              ((symbol-function 'clutch--refresh-display) #'ignore))
      (clutch-result-discard-pending-at-point)
      (should (null clutch--pending-edits)))))

(ert-deftest clutch-test-check-pending-changes-blocks-when-deletes-pending ()
  "`clutch--check-pending-changes' should signal when discard is declined."
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
    (setq-local clutch--pending-inserts '((("id" . "3") ("name" . "c"))))
    (setq-local clutch--pending-edits
                (list (cons (cons (vector 1) 1) "a2")))
    (setq-local clutch--pending-deletes (list (vector 2)))
    (let (executed)
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO users (id, name) VALUES (?, ?)" . ("3" "c")))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE users SET name = ? WHERE id = ?" . ("a2" 1)))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM users WHERE id = ?" . (2)))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'yes-or-no-p) (lambda (_) t))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql &optional params)
                   (push (cons sql params) executed)))
                ((symbol-function 'clutch--execute) #'ignore))
        (clutch-result-commit)
        (should (= (length executed) 3))
        ;; executed is in reverse push order: last executed is at (nth 0 executed)
        (should (string-prefix-p "INSERT" (car (nth 2 executed))))
        (should (equal (cdr (nth 2 executed)) '("3" "c")))
        (should (string-prefix-p "UPDATE" (car (nth 1 executed))))
        (should (equal (cdr (nth 1 executed)) '("a2" 1)))
        (should (string-prefix-p "DELETE" (car (nth 0 executed))))
        (should (equal (cdr (nth 0 executed)) '(2)))))))

;;;; Edit — validation

(ert-deftest clutch-test-insert-local-validation-shows-inline-error ()
  "Editing an invalid value should mark the current insert field inline."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))))
          (with-temp-buffer
            (insert "impact_score: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (goto-char (clutch-result-insert--current-field-value-start))
              (insert "x")
              (let* ((field (clutch-result-insert--field-state "impact_score"))
                     (after (overlay-get (plist-get field :error-overlay)
                                         'after-string)))
                (should (equal (plist-get field :error-message)
                               "Field impact_score expects a numeric value"))
                (should (overlayp (plist-get field :error-overlay)))
                (should (string-match-p "\\[invalid numeric\\]" after))
                (should-not (string-prefix-p "\n" after))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-local-validation-clears-inline-error ()
  "Fixing a field should clear its inline validation message."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))))
          (with-temp-buffer
            (insert "impact_score: x\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (let ((field (clutch-result-insert--field-state "impact_score")))
                (clutch-result-insert--validate-field-live field)
                (setq field (clutch-result-insert--field-state "impact_score"))
                (should (plist-get field :error-message))
                (goto-char (clutch-result-insert--current-field-value-start))
                (delete-region (point) (line-end-position))
                (insert "1.5")
                (setq field (clutch-result-insert--field-state "impact_score"))
                (should-not (plist-get field :error-message))
                (should-not (plist-get field :error-overlay))))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-json-validation-is-scheduled-on-idle ()
  "JSON fields should defer local validation until the user goes idle."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (scheduled nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'run-with-idle-timer)
                       (lambda (secs _repeat fn &rest args)
                         (setq scheduled (list secs fn args))
                         'fake-timer)))
              (goto-char (clutch-result-insert--current-field-value-start))
              (insert "{")
              (should scheduled)
              (should (= (car scheduled) clutch-insert-validation-idle-delay))
              (should (eq (cadr scheduled)
                          #'clutch-result-insert--run-idle-validation)))))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-edit-live-validation-shows-short-token ()
  "Edit buffers should surface a compact live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--row-idx 0
                clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (should (equal clutch-result-edit--error-message
                   "Field impact_score expects a numeric value"))
    (should (string-match-p "\\[invalid numeric\\]"
                            (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-live-validation-clears-when-fixed ()
  "Fixing an invalid edit value should clear the live-validation token."
  (with-temp-buffer
    (insert "xx")
    (clutch--result-edit-mode 1)
    (setq-local clutch-result-edit--row-idx 0
                clutch-result-edit--column-name "impact_score"
                clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)"))
    (clutch-result-edit--refresh-header-line)
    (clutch-result-edit--validate-live)
    (erase-buffer)
    (insert "1.5")
    (clutch-result-edit--validate-live)
    (should-not clutch-result-edit--error-message)
    (should-not (string-match-p "\\[invalid numeric\\]"
                                (format "%s" header-line-format)))))

(ert-deftest clutch-test-edit-json-live-validation-is-scheduled-on-idle ()
  "JSON edit buffers should defer live validation until the user goes idle."
  (with-temp-buffer
    (let (scheduled)
      (clutch--result-edit-mode 1)
      (setq-local clutch-result-edit--row-idx 0
                  clutch-result-edit--column-name "payload"
                  clutch-result-edit--column-def '(:name "payload" :type-category json)
                  clutch-result-edit--column-detail '(:name "payload" :type "json"))
      (cl-letf (((symbol-function 'run-with-idle-timer)
                 (lambda (delay _repeat fn &rest args)
                   (setq scheduled (list delay fn args))
                   :fake-timer)))
        (clutch-result-edit--schedule-validation)
        (should (= (car scheduled) clutch-insert-validation-idle-delay))
        (should (eq (cadr scheduled) #'clutch-result-edit--run-idle-validation))
        (should (equal (caddr scheduled) (list (current-buffer))))))))

(ert-deftest clutch-test-edit-finish-validates-numeric-before-stage ()
  "Edit staging should reject invalid numeric values and keep the edit buffer open."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        quit-called
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "xx")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "impact_score"
                      clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                      clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _args) (setq quit-called t))))
            (setq err
                  (should-error (clutch-result-edit-finish) :type 'user-error))
            (should (string-match-p "Field impact_score expects a numeric value"
                                    (error-message-string err)))
            (should-not quit-called)
            (should-not staged-value)
            (should (buffer-live-p edit-buf))))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-validates-enum-before-stage ()
  "Edit staging should reject invalid enum values locally."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "urgent")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "severity"
                      clutch-result-edit--column-def '(:name "severity" :type-category text)
                      clutch-result-edit--column-detail
                      '(:name "severity" :type "enum('low','medium','high')")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (setq err
                (should-error (clutch-result-edit-finish) :type 'user-error))
          (should (string-match-p "Field severity must be one of: low, medium, high"
                                  (error-message-string err)))
          (should-not staged-value))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-validates-json-before-stage ()
  "Inline JSON edits should validate before staging."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        err)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "{oops}")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "payload"
                      clutch-result-edit--column-def '(:name "payload" :type-category json)
                      clutch-result-edit--column-detail '(:name "payload" :type "json")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (setq err
                (should-error (clutch-result-edit-finish) :type 'user-error))
          (should (string-match-p "Field payload expects valid JSON"
                                  (error-message-string err)))
          (should-not staged-value))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-allows-null-sentinel ()
  "Typing NULL in edit buffers should still stage a nil value."
  (let ((edit-buf (generate-new-buffer "*clutch-edit-test*"))
        staged-value
        quit-called)
    (unwind-protect
        (with-current-buffer edit-buf
          (insert "NULL")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "impact_score"
                      clutch-result-edit--column-def '(:name "impact_score" :type-category numeric)
                      clutch-result-edit--column-detail '(:name "impact_score" :type "decimal(5,1)")
                      clutch-result--edit-callback (lambda (value) (setq staged-value value)))
          (cl-letf (((symbol-function 'quit-window)
                     (lambda (&rest _args) (setq quit-called t))))
            (clutch-result-edit-finish)
            (should quit-called)
            (should-not staged-value)))
      (kill-buffer edit-buf))))

(ert-deftest clutch-test-edit-finish-errors-when-result-buffer-is-dead ()
  "Finishing an edit should fail cleanly when the parent result buffer is gone."
  (let ((result-buf (generate-new-buffer "*clutch-result-test*"))
        edit-buf)
    (unwind-protect
        (with-current-buffer result-buf
          (clutch-result-mode)
          (setq-local clutch--result-columns '("name")
                      clutch--result-column-defs '((:name "name" :type-category text))
                      clutch--result-rows '(("before"))
                      clutch--last-query "SELECT * FROM users"
                      clutch--row-identity (clutch-test--primary-row-identity
                                            "users" '("name") '(0)))
          (let ((inhibit-read-only t))
            (insert "before")
            (add-text-properties (point-min) (point-max)
                                 '(clutch-row-idx 0 clutch-col-idx 0
                                   clutch-full-value "before")))
          (goto-char (point-min))
          (cl-letf (((symbol-function 'clutch--ensure-column-details)
                     (lambda (&rest _) nil))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (setq edit-buf buf)
                       buf)))
            (clutch-result-edit-cell))
          (kill-buffer result-buf)
          (with-current-buffer edit-buf
            (erase-buffer)
            (insert "after")
            (cl-letf (((symbol-function 'quit-window) #'ignore))
              (should-error (clutch-result-edit-finish) :type 'user-error))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf))
      (when (buffer-live-p edit-buf)
        (kill-buffer edit-buf)))))

(ert-deftest clutch-test-insert-commit-validates-enum-bool-and-json-before-stage ()
  "Insert staging should reject invalid enum/bool/json values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("severity" "is_ship_blocked" "postmortem")
                        clutch--result-column-defs '((:name "severity" :type-category text)
                                                     (:name "is_ship_blocked" :type-category numeric)
                                                     (:name "postmortem" :type-category json))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "severity: nope\nis_ship_blocked: 7\npostmortem: not-json\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "severity" :type "enum('low','medium')")
                               (list :name "is_ship_blocked" :type "tinyint(1)")
                               (list :name "postmortem" :type "json")))))
              (should-error (clutch-result-insert-commit) :type 'user-error)))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should (buffer-live-p result-buf))
          (should-not fields-after))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-commit-validates-temporal-before-stage ()
  "Insert staging should reject invalid date/time/datetime values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after err-msg)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("opened_at" "due_on" "starts_at")
                        clutch--result-column-defs '((:name "opened_at" :type-category datetime)
                                                     (:name "due_on" :type-category date)
                                                     (:name "starts_at" :type-category time))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "opened_at: ss\ndue_on: 2026-02-30\nstarts_at: 25:61\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "opened_at" :type "datetime")
                               (list :name "due_on" :type "date")
                               (list :name "starts_at" :type "time")))))
              (setq err-msg
                    (should-error (clutch-result-insert-commit)
                                  :type 'user-error))
              (should (string-match-p "Field opened_at expects YYYY-MM-DD HH:MM\\[:SS\\]"
                                      (error-message-string err-msg)))))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should-not fields-after))
      (kill-buffer result-buf))))

(ert-deftest clutch-test-insert-commit-validates-numeric-before-stage ()
  "Insert staging should reject invalid numeric values locally."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*")))
    (unwind-protect
        (let (fields-after err-msg)
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("impact_score")
                        clutch--result-column-defs '((:name "impact_score" :type-category numeric))
                        clutch--pending-inserts nil))
          (with-temp-buffer
            (insert "impact_score: xx\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "impact_score" :type "decimal(5,1)")))))
              (setq err-msg
                    (should-error (clutch-result-insert-commit)
                                  :type 'user-error))
              (should (string-match-p "Field impact_score expects a numeric value"
                                      (error-message-string err-msg)))))
          (setq fields-after
                (with-current-buffer result-buf clutch--pending-inserts))
          (should-not fields-after))
      (kill-buffer result-buf))))

;;;; Edit — JSON sub-editor

(ert-deftest clutch-test-json-editor-mode-falls-back-when-json-ts-errors ()
  "JSON editors should tolerate missing tree-sitter grammars."
  (let (selected-mode)
    (with-temp-buffer
      (cl-letf (((symbol-function 'json-ts-mode)
                 (lambda () (error "missing JSON grammar")))
                ((symbol-function 'js-mode)
                 (lambda () (setq selected-mode 'js-mode))))
        (clutch-result-insert--json-editor-mode)))
    (should (eq selected-mode 'js-mode))))

(ert-deftest clutch-test-insert-json-editor-save-roundtrip ()
  "Saving a JSON child editor should write compact JSON back to the insert field."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (editor-buf nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: \n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq editor-buf buf)
                         buf)))
              (clutch-result-insert-edit-json-field))
            (with-current-buffer editor-buf
              (erase-buffer)
              (insert "{\n  \"severity\": \"high\",\n  \"ship_blocked\": true\n}")
              (cl-letf (((symbol-function 'clutch--ensure-column-details)
                         (lambda (_conn _table)
                           (list (list :name "postmortem" :type "json"))))
                        ((symbol-function 'quit-window) (lambda (&rest _args) nil))
                        ((symbol-function 'pop-to-buffer) (lambda (buf &rest _args) buf)))
                (clutch-result-insert-json-finish)))
            (should (equal (buffer-string)
                           "postmortem: {\"severity\":\"high\",\"ship_blocked\":true}\n"))))
      (kill-buffer result-buf)
      (when (buffer-live-p editor-buf)
        (kill-buffer editor-buf)))))

(ert-deftest clutch-test-insert-json-editor-cancel-keeps-parent-value ()
  "Cancelling the JSON child editor should leave the parent field unchanged."
  (let ((result-buf (generate-new-buffer "*clutch-insert-result*"))
        (editor-buf nil))
    (unwind-protect
        (progn
          (with-current-buffer result-buf
            (setq-local clutch-connection 'fake-conn
                        clutch--result-columns '("postmortem")
                        clutch--result-column-defs '((:name "postmortem" :type-category json))))
          (with-temp-buffer
            (insert "postmortem: {\"ok\":true}\n")
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--result-buffer result-buf
                        clutch-result-insert--table "shipping_incidents")
            (cl-letf (((symbol-function 'clutch--ensure-column-details)
                       (lambda (_conn _table)
                         (list (list :name "postmortem" :type "json"))))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq editor-buf buf)
                         buf)))
              (clutch-result-insert-edit-json-field))
            (with-current-buffer editor-buf
              (erase-buffer)
              (insert "{\"ok\":false}")
              (cl-letf (((symbol-function 'quit-window) (lambda (&rest _args) nil))
                        ((symbol-function 'pop-to-buffer) (lambda (buf &rest _args) buf)))
                (clutch-result-insert-json-cancel)))
            (should (equal (buffer-string)
                           "postmortem: {\"ok\":true}\n"))))
      (kill-buffer result-buf)
      (when (buffer-live-p editor-buf)
        (kill-buffer editor-buf)))))

(ert-deftest clutch-test-edit-json-field-roundtrip ()
  "JSON edit sub-buffer should save normalized contents back to the parent edit buffer."
  (let ((parent-buf (generate-new-buffer "*clutch-edit-parent*")))
    (unwind-protect
        (with-current-buffer parent-buf
          (insert "{\"a\":1}")
          (clutch--result-edit-mode 1)
          (setq-local clutch-result-edit--column-name "payload"
                      clutch-result-edit--column-def '(:name "payload" :type-category json)
                      clutch-result-edit--column-detail '(:name "payload" :type "json"))
          (cl-letf (((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (let ((json-buf (clutch-result-edit-json-field)))
              (with-current-buffer json-buf
                (erase-buffer)
                (insert "{\"a\":2}")
                (clutch-result-edit-json-finish))
              (should (equal (with-current-buffer parent-buf (buffer-string))
                             "{\"a\":2}")))))
      (kill-buffer parent-buf))))

(ert-deftest clutch-test-json-sub-editor-open-path-is-shared ()
  "Insert and edit JSON editors should both reuse the shared open helper."
  (let ((insert-parent (generate-new-buffer "*clutch-insert-parent*"))
        (edit-parent (generate-new-buffer "*clutch-edit-parent*"))
        opened
        spawned)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--open-json-sub-editor)
                   (lambda (buffer-name initial-text field-name finish-fn cancel-fn)
                     (let ((buf (generate-new-buffer buffer-name)))
                       (push (list buffer-name initial-text field-name finish-fn cancel-fn) opened)
                       (push buf spawned)
                       buf)))
                  ((symbol-function 'clutch-result-insert--current-field-or-error)
                   (lambda () '(:name "payload" :value "{\"a\":1}")))
                  ((symbol-function 'clutch-result-insert--json-field-p)
                   (lambda (_field) t))
                  ((symbol-function 'clutch-result-insert--sync-field-value) #'ignore))
          (with-current-buffer insert-parent
            (clutch-result-insert-mode 1)
            (setq-local clutch-result-insert--table "events")
            (clutch-result-insert-edit-json-field))
          (with-current-buffer edit-parent
            (insert "{\"b\":2}")
            (clutch--result-edit-mode 1)
            (setq-local clutch-result-edit--column-name "payload"
                        clutch-result-edit--column-def '(:name "payload" :type-category json)
                        clutch-result-edit--column-detail '(:name "payload" :type "json"))
            (clutch-result-edit-json-field))
          (should (= (length opened) 2))
          (should (equal (mapcar (lambda (entry) (nth 2 entry)) opened)
                         '("payload" "payload"))))
      (mapc (lambda (buf)
              (when (buffer-live-p buf)
                (kill-buffer buf)))
            spawned)
      (kill-buffer insert-parent)
      (kill-buffer edit-parent))))

;;;; Export — dispatch and content

(defun clutch-test--assert-export-dispatch (choice expected)
  "Assert `clutch-result-export' dispatches CHOICE to EXPECTED."
  (with-temp-buffer
    (let (called)
      (cl-letf (((symbol-function 'completing-read)
                 (lambda (&rest _args) choice))
                ((symbol-function 'clutch--export-csv-all-to-clipboard)
                 (lambda () (setq called 'copy)))
                ((symbol-function 'clutch--export-csv-all-file)
                 (lambda () (setq called 'file)))
                ((symbol-function 'clutch--export-insert-all-to-clipboard)
                 (lambda () (setq called 'insert-copy)))
                ((symbol-function 'clutch--export-insert-all-file)
                 (lambda () (setq called 'insert-file)))
                ((symbol-function 'clutch--export-update-all-to-clipboard)
                 (lambda () (setq called 'update-copy)))
                ((symbol-function 'clutch--export-update-all-file)
                 (lambda () (setq called 'update-file))))
        (clutch-result-export)
        (should (eq called expected))))))

(ert-deftest clutch-test-export-command-dispatches-copy ()
  "Export command should dispatch to all-rows clipboard export."
  (clutch-test--assert-export-dispatch "csv-copy" 'copy))

(ert-deftest clutch-test-export-command-dispatches-file ()
  "Export command should dispatch to all-rows file export."
  (clutch-test--assert-export-dispatch "csv-file" 'file))

(ert-deftest clutch-test-export-command-dispatches-insert-copy ()
  "Export command should dispatch to all-rows INSERT clipboard export."
  (clutch-test--assert-export-dispatch "insert-copy" 'insert-copy))

(ert-deftest clutch-test-export-command-dispatches-insert-file ()
  "Export command should dispatch to all-rows INSERT file export."
  (clutch-test--assert-export-dispatch "insert-file" 'insert-file))

(ert-deftest clutch-test-export-command-dispatches-update-copy ()
  "Export command should dispatch to all-rows UPDATE clipboard export."
  (clutch-test--assert-export-dispatch "update-copy" 'update-copy))

(ert-deftest clutch-test-export-command-dispatches-update-file ()
  "Export command should dispatch to all-rows UPDATE file export."
  (clutch-test--assert-export-dispatch "update-file" 'update-file))

(ert-deftest clutch-test-csv-content-escaping ()
  "CSV content should include header and escaped values."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name"))
    (let ((csv (clutch--csv-content '((1 "a,b") (2 "x\"y")))))
      (should (string-match-p "^id,name\n" csv))
      (should (string-match-p "1,\"a,b\"" csv))
      (should (string-match-p "2,\"x\"\"y\"" csv)))))

(ert-deftest clutch-test-insert-content-builds-full-row-sql ()
  "INSERT export content should reuse the existing INSERT builder for all rows."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--last-query "SELECT id, name FROM users")
    (cl-letf (((symbol-function 'clutch--simple-insert-source-table)
               (lambda () "users"))
              ((symbol-function 'clutch-result--build-insert-statements)
               (lambda (indices col-indices table)
                 (should (equal indices '(0 1)))
                 (should (equal col-indices '(0 1)))
                 (should (equal table "users"))
                 '("INSERT INTO users (id, name) VALUES (1, 'a');"
                   "INSERT INTO users (id, name) VALUES (2, 'b');"))))
      (should (equal (clutch--insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO users (id, name) VALUES (1, 'a');\n"
                      "INSERT INTO users (id, name) VALUES (2, 'b');\n"))))))

(ert-deftest clutch-test-update-content-builds-full-row-sql ()
  "UPDATE export content should reuse the shared UPDATE builder for all rows."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name"))
    (cl-letf (((symbol-function 'clutch-result--build-update-statements-for-rows)
               (lambda (rows col-indices op)
                 (should (equal rows '((1 "a") (2 "b"))))
                 (should (equal col-indices '(0 1)))
                 (should (equal op "export UPDATE SQL"))
                 '("UPDATE users SET name = 'a' WHERE id = 1"
                   "UPDATE users SET name = 'b' WHERE id = 2"))))
      (should (equal (clutch--update-content '((1 "a") (2 "b")))
                     (concat
                      "UPDATE users SET name = 'a' WHERE id = 1\n"
                      "UPDATE users SET name = 'b' WHERE id = 2\n"))))))

(ert-deftest clutch-test-simple-insert-source-table-rejects-joined-query ()
  "Joined result queries should not pretend one table is the INSERT target."
  (with-temp-buffer
    (setq-local clutch--last-query
                "SELECT u.id, p.title FROM users u JOIN posts p ON p.user_id = u.id")
    (should-not (clutch--simple-insert-source-table))
    (should (equal (clutch--insert-target-table) "MY_TABLE"))))

(ert-deftest clutch-test-copy-rows-as-insert-uses-placeholder-for-ambiguous-source ()
  "INSERT copy should use a placeholder table for ambiguous result queries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id")
                clutch--result-rows '((1))
                clutch--last-query
                "SELECT u.id FROM users u JOIN posts p ON p.user_id = u.id")
    (cl-letf (((symbol-function 'clutch-result--cell-at-point)
               (lambda () (list 0 0 1)))
              ((symbol-function 'clutch-result--build-insert-statements)
               (lambda (indices col-indices table)
                 (should (equal indices '(0)))
                 (should (equal col-indices '(0)))
                 (should (equal table "MY_TABLE"))
                 '("INSERT INTO MY_TABLE (id) VALUES (1);"))))
      (clutch-result--copy-rows-as-insert)
      (should (equal (current-kill 0) "INSERT INTO MY_TABLE (id) VALUES (1);")))))

(ert-deftest clutch-test-insert-content-uses-placeholder-for-ambiguous-source ()
  "INSERT export content should use `MY_TABLE' for ambiguous result queries."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name")
                clutch--last-query
                "SELECT u.id, p.name FROM users u JOIN posts p ON p.user_id = u.id")
    (cl-letf (((symbol-function 'clutch-result--build-insert-statements)
               (lambda (indices col-indices table)
                 (should (equal indices '(0 1)))
                 (should (equal col-indices '(0 1)))
                 (should (equal table "MY_TABLE"))
                 '("INSERT INTO MY_TABLE (id, name) VALUES (1, 'a');"
                   "INSERT INTO MY_TABLE (id, name) VALUES (2, 'b');"))))
      (should (equal (clutch--insert-content '((1 "a") (2 "b")))
                     (concat
                      "INSERT INTO MY_TABLE (id, name) VALUES (1, 'a');\n"
                      "INSERT INTO MY_TABLE (id, name) VALUES (2, 'b');\n"))))))

(ert-deftest clutch-test-copy-update-with-region-uses-region-rectangle ()
  "UPDATE copy should use rectangle row/column selection when a region is active."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name" "status")
                clutch--result-rows '((1 "a" "new")
                                      (2 "b" "done")))
    (cl-letf (((symbol-function 'use-region-p) (lambda () t))
              ((symbol-function 'clutch-result--region-rectangle-indices)
               (lambda () '((0 1) . (1 2))))
              ((symbol-function 'clutch-result--build-update-statements-for-rows)
               (lambda (rows col-indices op)
                 (should (equal rows '((1 "a" "new") (2 "b" "done"))))
                 (should (equal col-indices '(1 2)))
                 (should (equal op "copy UPDATE SQL"))
                 '("UPDATE users SET name='a' WHERE id=1"
                   "UPDATE users SET name='b' WHERE id=2"))))
      (clutch-result--copy-rows-as-update))))

(ert-deftest clutch-test-copy-update-without-region-copies-current-cell ()
  "UPDATE copy without region should target the current cell."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-rows '((1 "a")))
    (cl-letf (((symbol-function 'clutch-result--cell-at-point)
               (lambda () (list 0 1 "a")))
              ((symbol-function 'clutch-result--build-update-statements-for-rows)
               (lambda (rows col-indices op)
                 (should (equal rows '((1 "a"))))
                 (should (equal col-indices '(1)))
                 (should (equal op "copy UPDATE SQL"))
                 '("UPDATE users SET name = 'a' WHERE id = 1"))))
      (clutch-result--copy-rows-as-update))))

(ert-deftest clutch-test-copy-update-errors-when-only-pk-column-is-selected ()
  "UPDATE copy should reject selections that contain only primary key columns."
  (with-temp-buffer
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--result-source-table-or-user-error)
               (lambda (_op) "users")))
      (let ((err (should-error
                  (clutch-result--build-update-statements-for-rows
                   '((1 "a")) '(0) "copy UPDATE SQL")
                  :type 'user-error)))
        (should (string-match-p
                 "Cannot copy UPDATE SQL: no writable source columns selected"
                 (error-message-string err)))))))

(ert-deftest clutch-test-copy-update-errors-on-non-source-columns ()
  "UPDATE copy should reject alias or computed columns."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "name" "computed_total")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text)
                                             (:name "computed_total" :type-category numeric))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--result-source-table-or-user-error)
               (lambda (_op) "users"))
              ((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (list (list :name "id")
                       (list :name "name")))))
      (let ((err (should-error
                  (clutch-result--build-update-statements-for-rows
                   '((1 "alice" 42)) '(0 1 2) "copy UPDATE SQL")
                  :type 'user-error)))
        (should (string-match-p
                 "Cannot copy UPDATE SQL: selected columns are not writable source columns: computed_total"
                 (error-message-string err)))))))

(ert-deftest clutch-test-copy-update-errors-on-generated-source-columns ()
  "UPDATE copy should reject generated source columns."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--result-columns '("id" "generated_name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "generated_name" :type-category text))
                clutch--row-identity (clutch-test--primary-row-identity
                                      "users" '("id") '(0)))
    (cl-letf (((symbol-function 'clutch--result-source-table-or-user-error)
               (lambda (_op) "users"))
              ((symbol-function 'clutch--ensure-column-details)
               (lambda (_conn _table &optional _strict)
                 (list (list :name "id")
                       (list :name "generated_name" :generated t)))))
      (let ((err (should-error
                  (clutch-result--build-update-statements-for-rows
                   '((1 "alice")) '(0 1) "copy UPDATE SQL")
                  :type 'user-error)))
        (should (string-match-p
                 "Cannot copy UPDATE SQL: selected columns are not writable source columns: generated_name"
                 (error-message-string err)))))))

(ert-deftest clutch-test-copy-pending-sql-copies-current-batch ()
  "Staged SQL copy should mirror the staged commit batch."
  (with-temp-buffer
    (let (copied)
      (setq-local clutch--pending-inserts '(a)
                  clutch--pending-edits '(b)
                  clutch--pending-deletes '(c))
      (cl-letf (((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO t VALUES (1)" . nil))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM t WHERE id=1" . nil))))
                ((symbol-function 'kill-new)
                 (lambda (text) (setq copied text))))
        (clutch-result-copy-pending-sql)
        (should (equal copied
                       "INSERT INTO t VALUES (1);\nUPDATE t SET name='' WHERE id=1;\nDELETE FROM t WHERE id=1;\n"))))))

(ert-deftest clutch-test-save-pending-sql-writes-current-batch ()
  "Staged SQL save should write the staged commit batch to disk."
  (let ((path (make-temp-file "clutch-pending-" nil ".sql")))
    (unwind-protect
        (with-temp-buffer
          (setq-local clutch--pending-edits '(b))
          (cl-letf (((symbol-function 'clutch-result--build-update-statements)
                     (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                    ((symbol-function 'read-file-name)
                     (lambda (&rest _args) path)))
            (clutch-result-save-pending-sql)
            (should (equal (with-temp-buffer
                             (insert-file-contents path)
                             (buffer-string))
                           "UPDATE t SET name='' WHERE id=1;\n"))))
      (delete-file path))))

;;;; Aggregate and copy

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
  "Prefix-arg aggregate should use refined rectangle selection."
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
                 (lambda (_pos prop &optional _object)
                   (when (eq prop 'clutch-row-idx) 2)))
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

(ert-deftest clutch-test-copy-format-commands-dispatch-to-unified-copy ()
  "CSV and TSV copy commands should dispatch to `clutch-result-copy'."
  (dolist (case '((clutch-result-copy-csv csv)
                  (clutch-result-copy-tsv tsv)))
    (pcase-let ((`(,command ,expected-format) case))
      (ert-info ((symbol-name command))
        (with-temp-buffer
          (let (called)
            (cl-letf (((symbol-function 'clutch-result-copy)
                       (lambda (fmt &optional rect)
                         (setq called (list fmt rect)))))
              (funcall command)
              (should (equal called (list expected-format nil))))))))))

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
                ((symbol-function 'clutch--simple-insert-source-table)
                 (lambda (&optional _sql) "t"))
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
                ((symbol-function 'clutch--simple-insert-source-table)
                 (lambda (&optional _sql) "t"))
                ((symbol-function 'clutch-db-escape-identifier)
                 (lambda (_conn s) (format "\"%s\"" s)))
                ((symbol-function 'clutch--value-to-literal)
                 (lambda (v) (format "'%s'" v))))
        (clutch-result-copy 'insert)
        (should (equal (current-kill 0)
                       "INSERT INTO \"t\" (\"name\") VALUES ('a');"))))))

;;;; Refine

(ert-deftest clutch-test-refine-start-activates-mode-and-overlays ()
  "Starting refine mode should enable the mode and create selection overlays."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (let ((callback (lambda (_rect) nil)))
      (clutch-result--start-refine '((0 1 2) . (0 1)) callback)
      (should clutch-refine-mode)
      (should (equal clutch--refine-rect '((0 1 2) . (0 1))))
      (should clutch--refine-overlays)
      (should (eq clutch--refine-callback callback)))))

(ert-deftest clutch-test-refine-toggle-row-excludes-and-includes ()
  "Refine mode should toggle row exclusion at point."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-row)
    (should (equal clutch--refine-excluded-rows '(1)))
    (clutch-refine-toggle-row)
    (should-not clutch--refine-excluded-rows)))

(ert-deftest clutch-test-refine-toggle-col-excludes-and-includes ()
  "Refine mode should toggle column exclusion at point."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (clutch-result--start-refine '((0 1 2) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-col-idx 1 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-col)
    (should (equal clutch--refine-excluded-cols '(1)))
    (clutch-refine-toggle-col)
    (should-not clutch--refine-excluded-cols)))

(ert-deftest clutch-test-refine-confirm-calls-callback-with-filtered-rect ()
  "Refine confirm should pass the remaining rectangle to the callback."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (let (seen)
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (rect) (setq seen rect)))
      (goto-char (point-min))
      (let ((row-match (text-property-search-forward 'clutch-row-idx 1 #'eq)))
        (should row-match)
        (goto-char (prop-match-beginning row-match)))
      (clutch-refine-toggle-row)
      (goto-char (point-min))
      (let ((col-match (text-property-search-forward 'clutch-col-idx 0 #'eq)))
        (should col-match)
        (goto-char (prop-match-beginning col-match)))
      (clutch-refine-toggle-col)
      (clutch-refine-confirm)
      (should (equal seen '((0 2) . (1))))
      (should-not clutch-refine-mode))))

(ert-deftest clutch-test-refine-confirm-errors-when-all-rows-excluded ()
  "Refine confirm should error when no rows remain."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (clutch-result--start-refine '((0) . (0 1)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-row-idx 0 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-row)
    (should-error (clutch-refine-confirm) :type 'user-error)))

(ert-deftest clutch-test-refine-confirm-errors-when-all-cols-excluded ()
  "Refine confirm should error when no columns remain."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (clutch-result--start-refine '((0 1) . (0)) #'ignore)
    (goto-char (point-min))
    (let ((match (text-property-search-forward 'clutch-col-idx 0 #'eq)))
      (should match)
      (goto-char (prop-match-beginning match)))
    (clutch-refine-toggle-col)
    (should-error (clutch-refine-confirm) :type 'user-error)))

(ert-deftest clutch-test-refine-cancel-does-not-call-callback ()
  "Refine cancel should exit without invoking the callback."
  (with-temp-buffer
    (clutch-result-mode)
    (setq-local clutch--result-columns '("id" "name")
                clutch--result-column-defs '((:name "id" :type-category numeric)
                                             (:name "name" :type-category text))
                clutch--result-rows '((1 "alice") (2 "bob") (3 "carol"))
                clutch--filtered-rows nil
                clutch--pending-edits nil
                clutch--pending-deletes nil
                clutch--pending-inserts nil
                clutch--marked-rows nil
                clutch--sort-column nil
                clutch--sort-descending nil
                clutch--page-current 0
                clutch--page-total-rows 3
                clutch--column-widths [2 5])
    (clutch--refresh-display)
    (let ((called nil))
      (clutch-result--start-refine '((0 1 2) . (0 1))
                                   (lambda (_rect) (setq called t)))
      (clutch-refine-cancel)
      (should-not called)
      (should-not clutch-refine-mode))))

;;;; Page navigation and sorting

(ert-deftest clutch-test-next-page-errors-on-last-page ()
  "Next page should error when current page has fewer rows than page size."
  (with-temp-buffer
    (setq-local clutch--result-rows '((1) (2))
                clutch-result-max-rows 10)
    (should-error (clutch-result-next-page) :type 'user-error)))

(ert-deftest clutch-test-next-page-advances-when-full ()
  "Next page should advance when the page is full."
  (with-temp-buffer
    (setq-local clutch--result-rows (make-list 50 '(1))
                clutch-result-max-rows 50
                clutch--page-current 0)
    (let (executed-page)
      (cl-letf (((symbol-function 'clutch--execute-page)
                 (lambda (p) (setq executed-page p))))
        (clutch-result-next-page)
        (should (= executed-page 1))))))

(ert-deftest clutch-test-prev-page-errors-on-first-page ()
  "Previous page should error when already on page 0."
  (with-temp-buffer
    (setq-local clutch--page-current 0)
    (should-error (clutch-result-prev-page) :type 'user-error)))

(ert-deftest clutch-test-prev-page-goes-back ()
  "Previous page should decrement the page number."
  (with-temp-buffer
    (setq-local clutch--page-current 3)
    (let (executed-page)
      (cl-letf (((symbol-function 'clutch--execute-page)
                 (lambda (p) (setq executed-page p))))
        (clutch-result-prev-page)
        (should (= executed-page 2))))))

(ert-deftest clutch-test-first-page-errors-when-already-first ()
  "First page should error when already on page 0."
  (with-temp-buffer
    (setq-local clutch--page-current 0)
    (should-error (clutch-result-first-page) :type 'user-error)))

(ert-deftest clutch-test-first-page-jumps-to-zero ()
  "First page should go to page 0."
  (with-temp-buffer
    (setq-local clutch--page-current 5)
    (let (executed-page)
      (cl-letf (((symbol-function 'clutch--execute-page)
                 (lambda (p) (setq executed-page p))))
        (clutch-result-first-page)
        (should (= executed-page 0))))))

(ert-deftest clutch-test-last-page-calculates-correct-page ()
  "Last page should calculate page number from total rows and page size."
  (with-temp-buffer
    (setq-local clutch--page-current 0
                clutch--page-total-rows 237
                clutch-result-max-rows 50)
    (let (executed-page)
      (cl-letf (((symbol-function 'clutch--execute-page)
                 (lambda (p) (setq executed-page p))))
        (clutch-result-last-page)
        ;; 237 rows / 50 per page = 5 pages (0..4), last page = 4
        (should (= executed-page 4))))))

(ert-deftest clutch-test-last-page-errors-when-already-last ()
  "Last page should error when already on the last page."
  (with-temp-buffer
    (setq-local clutch--page-current 4
                clutch--page-total-rows 237
                clutch-result-max-rows 50)
    (should-error (clutch-result-last-page) :type 'user-error)))

(ert-deftest clutch-test-last-page-single-page-result ()
  "Last page should error when total rows fit in one page."
  (with-temp-buffer
    (setq-local clutch--page-current 0
                clutch--page-total-rows 30
                clutch-result-max-rows 50)
    (should-error (clutch-result-last-page) :type 'user-error)))

(ert-deftest clutch-test-sort-by-column-toggles-direction ()
  "Sorting the same column twice should toggle ascending/descending."
  (with-temp-buffer
    (setq-local clutch--sort-column "name"
                clutch--sort-descending nil)
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "name"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("name" t)))))))

(ert-deftest clutch-test-sort-by-column-new-column-defaults-ascending ()
  "Sorting a different column should default to ascending."
  (with-temp-buffer
    (setq-local clutch--sort-column "name"
                clutch--sort-descending t)
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "age"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column)
        (should (equal sort-args '("age" nil)))))))

(ert-deftest clutch-test-sort-by-column-desc-always-descending ()
  "Sort descending command should always pass descending=t."
  (with-temp-buffer
    (let (sort-args)
      (cl-letf (((symbol-function 'clutch-result--read-column)
                 (lambda () "created_at"))
                ((symbol-function 'clutch-result--sort)
                 (lambda (col desc) (setq sort-args (list col desc)))))
        (clutch-result-sort-by-column-desc)
        (should (equal sort-args '("created_at" t)))))))

;;;; Connection — build, timeout, and lifecycle

(ert-deftest clutch-test-connection-oracle-jdbc-p-returns-nil-for-non-jdbc-connections ()
  "Oracle JDBC detection should return nil for non-JDBC connections."
  (should-not (clutch--connection-oracle-jdbc-p 'fake-conn)))

(ert-deftest clutch-test-connection-oracle-jdbc-p-propagates-jdbc-param-errors ()
  "Oracle JDBC detection should not hide JDBC plist access failures."
  (let* ((sentinel (list :driver 'oracle))
         (conn (make-clutch-jdbc-conn :params sentinel)))
    (cl-letf (((symbol-function 'plist-get)
               (lambda (plist prop &optional _default)
                 (if (eq plist sentinel)
                     (signal 'wrong-type-argument '(listp sentinel))
                   (let ((tail plist)
                         found)
                     (while tail
                       (when (eq (car tail) prop)
                         (setq found (cadr tail)
                               tail nil))
                       (setq tail (cddr tail)))
                     found)))))
      (should-error (clutch--connection-oracle-jdbc-p conn) :type 'wrong-type-argument))))

(ert-deftest clutch-test-backend-key-from-conn-returns-nil-for-unknown-non-jdbc-backends ()
  "Backend key detection should return nil when the connection is not JDBC."
  (cl-letf (((symbol-function 'clutch-db-display-name)
             (lambda (_conn) "DuckDB")))
    (should-not (clutch--backend-key-from-conn 'fake-conn))))

(ert-deftest clutch-test-backend-key-from-conn-returns-nil-for-opaque-connections ()
  "Backend key detection should tolerate opaque test connection objects."
  (cl-letf (((symbol-function 'clutch-db-display-name)
             (lambda (_conn)
               (signal 'cl-no-applicable-method
                       '(clutch-db-display-name fake-conn)))))
    (should-not (clutch--backend-key-from-conn 'fake-conn))))

(ert-deftest clutch-test-backend-key-from-conn-swallows-jdbc-param-errors ()
  "Backend key detection should return nil when JDBC plist access fails.
The function catches `clutch-db-error' and `wrong-type-argument' to avoid
crashing the UI layer."
  (let* ((sentinel (list :driver 'oracle))
         (conn (make-clutch-jdbc-conn :params sentinel)))
    (cl-letf (((symbol-function 'plist-get)
               (lambda (plist prop)
                 (if (eq plist sentinel)
                     (signal 'clutch-db-error '("backend key boom"))
                   (let ((tail plist)
                         found)
                     (while tail
                       (when (eq (car tail) prop)
                       (setq found (cadr tail)
                             tail nil))
                       (setq tail (cddr tail)))
                     found))))
              ((symbol-function 'clutch-db-display-name)
               (lambda (_conn) nil)))
      (should-not (clutch--backend-key-from-conn conn)))))

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

(ert-deftest clutch-test-connection-display-key-prefers-remote-ssh-endpoint ()
  "Connection labels should keep the remote endpoint visible when SSH is used."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (puthash conn '(:host "db.internal" :port 5432 :ssh-host "bastion-prod")
             clutch--connection-remote-params-cache)
    (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "alice"))
              ((symbol-function 'clutch-db-host) (lambda (_conn) "127.0.0.1"))
              ((symbol-function 'clutch-db-port) (lambda (_conn) 40123))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "appdb"))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "PostgreSQL")))
      (should (equal (clutch--connection-key conn)
                     "alice@db.internal:5432/appdb"))
      (should (equal (clutch--connection-display-key conn)
                     "alice@db.internal via bastion-prod")))))

(ert-deftest clutch-test-build-conn-includes-native-timeouts-for-network-backends ()
  "Test that `clutch--build-conn' passes timeout defaults to mysql/pg."
  (let ((clutch-connect-timeout-seconds 11)
        (clutch-read-idle-timeout-seconds 42)
        (clutch-query-timeout-seconds 13)
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
      (should-not (plist-member captured :query-timeout))
      (clutch--build-conn '(:backend pg :host "127.0.0.1" :port 5432 :user "u"))
      (should (equal (plist-get captured :connect-timeout) 11))
      (should (equal (plist-get captured :read-idle-timeout) 42))
      (should (equal (plist-get captured :query-timeout) 13)))))

(ert-deftest clutch-test-build-conn-rewrites-network-endpoint-through-ssh-tunnel ()
  "SSH-backed connections should target the local forwarded port."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-ssh-tunnel-cache (make-hash-table :test 'eq))
        captured)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (params)
                 (should (equal (plist-get params :ssh-host) "bastion-prod"))
                 '(:process fake-proc :local-port 40123 :ssh-host "bastion-prod")))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend params)
                 (setq captured params)
                 'fake-conn)))
      (should (eq (clutch--build-conn
                   '(:backend pg
                     :host "db.internal"
                     :port 5432
                     :user "alice"
                     :database "appdb"
                     :ssh-host "bastion-prod"))
                  'fake-conn))
      (should (equal (plist-get captured :host) "127.0.0.1"))
      (should (= (plist-get captured :port) 40123))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-remote-params-cache)
                                :host)
                     "db.internal"))
      (should (equal (plist-get (gethash 'fake-conn clutch--connection-ssh-tunnel-cache)
                                :ssh-host)
                     "bastion-prod")))))

(ert-deftest clutch-test-build-conn-stops-ssh-tunnel-when-db-connect-fails ()
  "Failed DB connect should tear down the SSH tunnel it just opened."
  (let (stopped)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch--start-ssh-tunnel)
               (lambda (_params)
                 '(:process fake-proc :local-port 40123 :ssh-host "bastion-prod")))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc)))
              ((symbol-function 'clutch-db-connect)
               (lambda (_backend _params)
                 (signal 'clutch-db-error '("db connect failed")))))
      (should-error
       (clutch--build-conn
        '(:backend pg
          :host "db.internal"
          :port 5432
          :user "alice"
          :database "appdb"
          :ssh-host "bastion-prod"))
       :type 'user-error)
      (should (eq stopped 'fake-proc)))))

(ert-deftest clutch-test-start-ssh-tunnel-rejects-jdbc-url ()
  "SSH tunneling should fail fast when the profile only provides a raw JDBC URL."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'unexpected)))
      (should-error
       (clutch--start-ssh-tunnel
        '(:backend oracle
          :url "jdbc:oracle:thin:@//db.example.com:1521/ORCL"
          :user "scott"
          :ssh-host "bastion-prod"))
       :type 'user-error)
      (should-not connect-called))))

(ert-deftest clutch-test-start-ssh-tunnel-starts-batch-tunnel-directly ()
  "SSH tunnel startup should rely on the real batch tunnel command."
  (let (process-file-called make-process-args)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'process-file)
               (lambda (&rest _args)
                 (setq process-file-called t)
                 0))
              ((symbol-function 'clutch--allocate-local-port)
               (lambda () 40123))
              ((symbol-function 'make-process)
                (lambda (&rest args)
                 (setq make-process-args args)
                 'fake-proc))
              ((symbol-function 'set-process-query-on-exit-flag) #'ignore)
              ((symbol-function 'clutch--wait-for-ssh-tunnel)
               (lambda (&rest _args) t)))
      (let ((tunnel (clutch--start-ssh-tunnel
                     '(:backend pg
                       :host "db.internal"
                       :port 5432
                       :ssh-host "bastion-prod"))))
        (should-not process-file-called)
        (should (eq (plist-get tunnel :process) 'fake-proc))
        (should (equal (plist-get make-process-args :command)
                       '("ssh"
                         "-N"
                         "-o" "BatchMode=yes"
                         "-o" "ExitOnForwardFailure=yes"
                         "-L" "127.0.0.1:40123:db.internal:5432"
                         "bastion-prod")))))))

(ert-deftest clutch-test-ssh-diagnose-output-hints-locked-key ()
  "SSH diagnosis should point users at an interactive unlock step."
  (let ((message (clutch--ssh-diagnose-output
                  "bastion-prod"
                  "sign_and_send_pubkey: signing failed for ED25519 \"~/.ssh/id_ed25519\" from agent: agent refused operation\n")))
    (should (string-match-p "locked" message))
    (should (string-match-p "clutch-prepare-ssh-host" message))))

(ert-deftest clutch-test-ssh-diagnose-output-hints-authorized-keys ()
  "SSH diagnosis should surface remote auth problems clearly."
  (let ((message (clutch--ssh-diagnose-output
                  "bastion-prod"
                  "lucius@db: Permission denied (publickey,password).\n")))
    (should (string-match-p "clutch-prepare-ssh-host" message))
    (should (string-match-p "authorized_keys" message))))

(ert-deftest clutch-test-prepare-ssh-host-starts-interactive-session ()
  "Interactive SSH prepare should open a comint session for the alias."
  (let (started puts sentinel-set query-flag popped message-text proc-lookups)
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_name) "/usr/bin/ssh"))
              ((symbol-function 'make-comint-in-buffer)
               (lambda (name buffer program startfile &rest switches)
                 (setq started (list name buffer program startfile switches))
                 (get-buffer-create buffer)))
              ((symbol-function 'get-buffer-process)
               (lambda (_buffer)
                 (prog1 (if proc-lookups 'fake-proc nil)
                   (setq proc-lookups t))))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'process-put)
               (lambda (proc key value)
                 (push (list proc key value) puts)))
              ((symbol-function 'set-process-query-on-exit-flag)
               (lambda (proc flag)
                 (setq query-flag (list proc flag))))
              ((symbol-function 'set-process-sentinel)
               (lambda (proc fn)
                 (setq sentinel-set (list proc fn))))
              ((symbol-function 'pop-to-buffer)
               (lambda (buffer &rest _args)
                 (setq popped buffer)
                 buffer))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (setq message-text (apply #'format fmt args)))))
      (clutch-prepare-ssh-host "bastion-prod")
      (should (equal (car started) "clutch-ssh-prepare-bastion-prod"))
      (should (equal (buffer-name (cadr started))
                     "*clutch-ssh-prepare bastion-prod*"))
      (should (equal (cddr started)
                     '("ssh"
                       nil
                       ("bastion-prod" "exit"))))
      (should (equal query-flag '(fake-proc nil)))
      (should (equal (car puts) '(fake-proc :clutch-ssh-host "bastion-prod")))
      (should (eq (car sentinel-set) 'fake-proc))
      (should (eq (cadr sentinel-set) #'clutch--ssh-prepare-sentinel))
      (should (equal (buffer-name popped) "*clutch-ssh-prepare bastion-prod*"))
      (should (string-match-p "Complete any SSH prompts" message-text)))))

(ert-deftest clutch-test-ssh-prepare-sentinel-nonzero-suggests-retry ()
  "Non-zero SSH prepare exit should not imply preparation was useless."
  (let ((buffer (get-buffer-create "*clutch-ssh-prepare bastion-prod*"))
        message-text)
    (unwind-protect
        (cl-letf (((symbol-function 'process-status)
                   (lambda (_proc) 'exit))
                  ((symbol-function 'process-exit-status)
                   (lambda (_proc) 255))
                  ((symbol-function 'process-get)
                   (lambda (_proc key)
                     (when (eq key :clutch-ssh-host) "bastion-prod")))
                  ((symbol-function 'process-buffer)
                   (lambda (_proc) buffer))
                  ((symbol-function 'message)
                   (lambda (fmt &rest args)
                     (setq message-text (apply #'format fmt args)))))
          (clutch--ssh-prepare-sentinel 'fake-proc "exited")
          (should (string-match-p "retry clutch-connect" message-text))
          (should (string-match-p "inspect" message-text)))
      (kill-buffer buffer))))

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

(ert-deftest clutch-test-build-conn-errors-early-when-jdbc-pass-entry-is-unresolved ()
  "JDBC connections should fail fast when :pass-entry resolves to no password."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (should-error
       (clutch--build-conn
       '(:backend oracle :host "db" :port 1521 :user "u" :sid "orcl"
          :pass-entry "prod-oracle"))
       :type 'user-error)
      (should-not connect-called))))

(ert-deftest clutch-test-resolve-password-errors-when-pass-entry-cannot-be-read ()
  "Unreadable pass entries should fail before the backend sees auth."
  (let (connect-called)
    (cl-letf (((symbol-function 'clutch-db--pass-entry-by-suffix)
               (lambda (_suffix) "mysql/zj_online"))
              ((symbol-function 'auth-source-pass-parse-entry)
               (lambda (_entry) nil))
              ((symbol-function 'clutch-db-connect)
               (lambda (&rest _args)
                 (setq connect-called t)
                 'fake-conn)))
      (condition-case err
          (clutch--build-conn
           '(:backend mysql :host "db" :port 3306 :user "u"
             :database "app" :pass-entry "zj_online"))
        (user-error
         (should (equal
                  (cadr err)
                  "Database password lookup failed for pass entry mysql/zj_online. Unlock pass/auth-source-pass and retry"))))
      (should-not connect-called))))

(ert-deftest clutch-test-resolve-password-errors-when-auth-source-secret-fails ()
  "auth-source secret retrieval failures should surface directly."
  (condition-case err
      (cl-letf (((symbol-function 'clutch-db--pass-entry-by-suffix)
                 (lambda (_suffix) nil))
                ((symbol-function 'auth-source-search)
                 (lambda (&rest _args)
                   (list (list :secret (lambda ()
                                         (error "bad decrypt")))))))
        (clutch--resolve-password
         '(:host "db.example.com" :port 3306 :user "scott")))
    (user-error
     (should (equal
              (cadr err)
              "Database password lookup failed via auth-source for scott@db.example.com:3306: bad decrypt")))))

(ert-deftest clutch-test-build-conn-relays-db-errors-without-duplicate-prefixes ()
  "User-facing connect errors should not duplicate nested connection-failed wrappers."
  (cl-letf (((symbol-function 'clutch--resolve-password)
             (lambda (_params) nil))
            ((symbol-function 'clutch-db-connect)
             (lambda (&rest _args)
               (signal 'clutch-db-error
                       '("Connection failed (oracle): Connection attempt timed out")))))
    (should-error
     (clutch--build-conn '(:backend oracle :host "db" :port 1521 :user "u"))
     :type 'user-error)
    (condition-case err
        (clutch--build-conn '(:backend oracle :host "db" :port 1521 :user "u"))
      (user-error
       (should (string-match-p
                "Connection failed [(]oracle[)]: Connection attempt timed out"
                (cadr err)))
       (should (string-match-p "clutch-debug-mode" (cadr err)))
       (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                               (cadr err)))))))

(ert-deftest clutch-test-build-conn-wraps-ssh-setup-errors-before-user-display ()
  "SSH setup errors should be normalized before they reach the minibuffer."
  (cl-letf (((symbol-function 'clutch--materialize-connection-params)
             (lambda (params) params))
            ((symbol-function 'clutch--prepare-connect-params)
             (lambda (_params)
               (signal 'clutch-db-error
                       '("SSH tunnel to arch failed: SSH authentication to arch was rejected")))))
    (condition-case err
        (clutch--build-conn '(:backend mysql :host "db" :port 3306 :ssh-host "arch"))
      (user-error
       (should (string-match-p
                "SSH tunnel to arch failed: SSH authentication to arch was rejected"
                (cadr err)))
       (should-not (string-match-p "^let\\*:" (cadr err)))
       (should-not (string-match-p "^if:" (cadr err)))
       (should-not (string-match-p "^when-let\\*:" (cadr err)))))))

(ert-deftest clutch-test-build-conn-enriches-buffer-error-details ()
  "Connect failures should keep enriched `current-buffer' error details."
  (with-temp-buffer
    (let ((raw-message "Connection refused (host=db.example.com, port=3306)"))
      (cl-letf (((symbol-function 'clutch--resolve-password)
                 (lambda (_params) nil))
                ((symbol-function 'clutch-db-connect)
                 (lambda (&rest _args)
                   (signal 'clutch-db-error (list raw-message)))))
        (let (signaled)
          (condition-case err
              (clutch--build-conn
               '(:backend oracle :host "db.example.com" :port 1521
                 :user "scott" :database "orcl"))
            (user-error
             (setq signaled err)))
          (should signaled)
          (should (equal (cadr signaled)
                         (clutch--debug-workflow-message
                          (clutch--humanize-db-error raw-message))))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (context (plist-get diag :context)))
            (should details)
            (should (eq (plist-get details :backend) 'oracle))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get context :host) "db.example.com"))
            (should (equal (plist-get context :port) 1521))
            (should (equal (plist-get context :database) "orcl"))
            (should (eq (plist-get context :backend) 'oracle))))))))

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

(ert-deftest clutch-test-effective-sql-product-returns-nil-without-backend ()
  "Connections without :backend should not infer a SQL product."
  (should-not (clutch--effective-sql-product '(:host "127.0.0.1"
                                               :port 3306
                                               :user "u"
                                               :database "db"))))

(ert-deftest clutch-test-build-conn-errors-without-backend ()
  "Building a connection should fail fast when :backend is missing."
  (should-error
   (clutch--build-conn '(:host "127.0.0.1" :port 3306 :user "u" :database "db"))
   :type 'user-error)
  (condition-case err
      (clutch--build-conn '(:host "127.0.0.1" :port 3306 :user "u" :database "db"))
    (user-error
     (should (string-match-p ":backend" (cadr err))))))

(ert-deftest clutch-test-default-connect-timeout-is-10-seconds ()
  "Project default connect timeout should stay at 10 seconds."
  (should (= clutch-connect-timeout-seconds 10)))

(ert-deftest clutch-test-build-conn-failure-points-to-debug-workflow-when-disabled ()
  "Connect failures should tell users how to capture a debug trace."
  (cl-letf (((symbol-function 'clutch-db-connect)
             (lambda (_backend _params)
               (signal 'clutch-db-error '("Connection refused (host=db.example.com, port=3306)")))))
    (condition-case err
        (progn
          (clutch--build-conn '(:backend mysql :host "db.example.com" :port 3306))
          (should nil))
      (user-error
       (should (string-match-p "check host and port" (cadr err)))
       (should (string-match-p "clutch-debug-mode" (cadr err)))
       (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                               (cadr err)))))))

(ert-deftest clutch-test-build-conn-failure-points-to-debug-buffer-when-enabled ()
  "Connect failures should point directly at the debug buffer when capture is on."
  (let ((clutch-debug-mode t))
    (cl-letf (((symbol-function 'clutch-db-connect)
               (lambda (_backend _params)
                 (signal 'clutch-db-error '("Connection refused (host=db.example.com, port=3306)")))))
      (condition-case err
          (progn
            (clutch--build-conn '(:backend mysql :host "db.example.com" :port 3306))
            (should nil))
        (user-error
         (should (string-match-p "check host and port" (cadr err)))
         (should-not (string-match-p "clutch-debug-mode" (cadr err)))
         (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                 (cadr err))))))))

(ert-deftest clutch-test-build-conn-jdbc-driver-missing-points-to-install-command ()
  "Missing JDBC driver should point to install-driver, not the debug workflow."
  (cl-letf (((symbol-function 'clutch-db-connect)
             (lambda (_backend _params)
               (signal 'clutch-db-error
                       '("SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL")))))
    (condition-case err
        (progn
          (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
          (should nil))
      (user-error
       (should (string-match-p "clutch-jdbc-install-driver RET oracle" (cadr err)))
       (should-not (string-match-p "clutch-debug-mode" (cadr err)))
       (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                   (cadr err)))))))

(ert-deftest clutch-test-build-conn-agent-missing-points-to-ensure-agent ()
  "Missing JDBC agent should point to ensure-agent, not the debug workflow."
  (cl-letf (((symbol-function 'clutch-db-connect)
             (lambda (_backend _params)
               (signal 'clutch-db-error
                       '("JDBC agent jar not found: /tmp/clutch-jdbc-agent.jar\nRun M-x clutch-jdbc-ensure-agent")))))
    (condition-case err
        (progn
          (clutch--build-conn '(:backend oracle :driver jdbc :host "db" :port 1521))
          (should nil))
      (user-error
       (should (string-match-p "Run M-x clutch-jdbc-ensure-agent" (cadr err)))
       (should-not (string-match-p "clutch-debug-mode" (cadr err)))
       (should-not (string-match-p (regexp-quote clutch-debug-buffer-name)
                                   (cadr err)))))))

(ert-deftest clutch-test-readers-load-clutch-entrypoint ()
  "Saved-connection readers should load `clutch' before checking profiles."
  (dolist (case `((clutch--read-connection-params
                   (:backend mysql :database "app_a" :pass-entry "alpha"))
                  (clutch--read-query-console-name
                   "alpha")))
    (pcase-let ((`(,reader ,expected) case))
      (ert-info ((symbol-name reader))
        (let ((clutch-connection-alist nil)
              required
              (orig-featurep (symbol-function 'featurep))
              (orig-require (symbol-function 'require)))
          (cl-letf (((symbol-function 'featurep)
                     (lambda (feature)
                       (if (eq feature 'clutch)
                           nil
                         (funcall orig-featurep feature))))
                    ((symbol-function 'require)
                     (lambda (feature &optional filename noerror)
                       (if (eq feature 'clutch)
                           (progn
                             (setq required t
                                   clutch-connection-alist
                                   '(("alpha" . (:backend mysql :database "app_a"))))
                             t)
                         (funcall orig-require feature filename noerror))))
                    ((symbol-function 'completing-read)
                     (lambda (&rest _args) "alpha")))
            (should (equal (funcall reader) expected))
            (should required)))))))

(ert-deftest clutch-test-read-connection-params-prompts-for-backend-when-unsaved ()
  "Manual connection prompts should collect an explicit backend first."
  (let ((clutch-connection-alist nil)
        backend-prompt
        port-default)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (prompt _collection &rest _args)
                 (setq backend-prompt prompt)
                 "pg"))
              ((symbol-function 'read-string)
               (lambda (prompt &optional _initial _history default-value _inherit)
                 (pcase prompt
                   ("Host (127.0.0.1): " "db.example.com")
                   ("User: " "alice")
                   ("SSH host from ~/.ssh/config (optional): " "bastion-prod")
                   ("Database (optional): " "app_db")
                   (_ (or default-value "")))))
              ((symbol-function 'read-number)
               (lambda (_prompt default)
                 (setq port-default default)
                 5544))
              ((symbol-function 'clutch--resolve-password)
               (lambda (_params) nil))
              ((symbol-function 'read-passwd)
               (lambda (&rest _args) "secret")))
      (should (equal (clutch--read-connection-params)
                     '(:backend pg
                       :host "db.example.com"
                       :port 5544
                       :user "alice"
                       :ssh-host "bastion-prod"
                       :password "secret"
                       :database "app_db")))
      (should (string-match-p "Backend" backend-prompt))
      (should (= port-default 5432)))))

;;;; Connection — transaction and auto-commit

(ert-deftest clutch-test-tx-header-line-and-update ()
  "Tx header-line uses semantic colors and dirty adds *."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq)))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch-db-display-name)
                 (lambda (_conn) "Oracle"))
                ((symbol-function 'clutch--connection-display-key)
                 (lambda (_conn) "scott@db"))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-manual-commit-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--schema-status-header-line-segment)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--icon)
                 (lambda (_spec &rest _fb) "[lock]")))
        ;; Mode-line is now just the mode name.
        (clutch--update-mode-line)
        (should (equal mode-name "clutch"))
        (should (equal header-line-format
                       '((:eval (clutch--build-connection-header-line)))))
        ;; Auto-commit: shows Tx: Auto in success face.
        (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
                   (lambda (_conn) nil)))
          (let ((seg (clutch--tx-header-line-segment clutch-connection)))
            (should (string-match-p "Tx: Auto" seg))
            (should (eq (get-text-property 0 'face seg) 'success))))
        ;; header-line-format is an (:eval ...) form; evaluate it to get content.
        (let ((hl (clutch--build-connection-header-line)))
          ;; Clean manual-commit: shows Tx: Manual (no asterisk).
          (should (string-match-p "Tx: Manual" hl))
          (should-not (string-match-p "Tx: Manual\\*" hl)))
        (let ((seg (clutch--tx-header-line-segment clutch-connection)))
          (should (eq (get-text-property 0 'face seg) 'warning)))
        ;; Dirty: header-line shows Tx: Manual*.
        (puthash clutch-connection t clutch--tx-dirty-cache)
        (let ((hl (clutch--build-connection-header-line))
              (seg (clutch--tx-header-line-segment clutch-connection)))
          (should (string-match-p "Tx: Manual\\*" hl))
          (should (eq (get-text-property 0 'face seg) 'error)))))))

(ert-deftest clutch-test-update-mode-line-shows-spinner-when-executing ()
  "Busy buffers should show the current spinner frame in `mode-name'."
  (with-temp-buffer
    (clutch-mode)
    (let ((clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (clutch--update-mode-line)
      (should (equal mode-name "clutch ⠹"))
      (should-not (string-match-p "\\[\\.\\.\\.\\]" mode-name)))))

(ert-deftest clutch-test-result-footer-shows-spinner-when-executing ()
  "Busy result buffers should show the current spinner frame in the footer."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (clutch--refresh-footer-display)
      (should (string-match-p "⠹"
                              (clutch--footer-mode-line-display))))))

(ert-deftest clutch-test-result-footer-spinner-replaces-elapsed-time ()
  "Busy result buffers should use the elapsed-time slot for the spinner."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--query-elapsed 0.042)
          (clutch--executing-p t)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (cl-letf (((symbol-function 'clutch--format-elapsed)
                 (lambda (_seconds) "42ms")))
        (clutch--refresh-footer-display)
        (let ((footer (substring-no-properties
                       (clutch--footer-mode-line-display))))
          (should (string-match-p "⏱ +⠹" footer))
          (should-not (string-match-p "42ms" footer)))))))

(ert-deftest clutch-test-result-footer-restores-elapsed-time-when-idle ()
  "Idle result buffers should show elapsed time in the timing slot."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch--footer-base-string "Σ 1 of ? rows")
          (clutch--query-elapsed 0.042)
          (clutch--executing-p nil)
          (clutch--spinner-timer t)
          (clutch--spinner-index 2))
      (cl-letf (((symbol-function 'clutch--format-elapsed)
                 (lambda (_seconds) "42ms")))
        (clutch--refresh-footer-display)
        (let ((footer (substring-no-properties
                       (clutch--footer-mode-line-display))))
          (should (string-match-p "⏱ +42ms" footer))
          (should-not (string-match-p "⠹" footer)))))))

(ert-deftest clutch-test-spinner-tick-stops-when-no-busy-buffers ()
  "Spinner timer should stop itself when no buffers are busy."
  (with-temp-buffer
    (let ((clutch--spinner-timer 'fake-timer)
          (clutch--spinner-index 0)
          cancelled)
      (cl-letf (((symbol-function 'buffer-list)
                 (lambda (&optional _frame) (list (current-buffer))))
                ((symbol-function 'cancel-timer)
                 (lambda (_timer) (setq cancelled t))))
        (clutch--spinner-tick)
        (should cancelled)
        (should-not clutch--spinner-timer)
        (should (zerop clutch--spinner-index))))))

(ert-deftest clutch-test-run-db-query-marks-manual-commit-dirty ()
  "Successful DML should mark manual-commit connections dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn _sql) '(:ok t)))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch--run-db-query conn "UPDATE demo SET x = 1")
      (should (clutch--tx-dirty-p conn)))))

(ert-deftest clutch-test-run-db-query-marks-native-pg-ddl-dirty ()
  "Transactional PostgreSQL DDL should mark manual-commit connections dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn _sql) '(:ok t)))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--backend-key-from-conn)
               (lambda (_conn) 'pg))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch--run-db-query conn "CREATE TABLE demo (id int)")
      (should (clutch--tx-dirty-p conn)))))

(ert-deftest clutch-test-run-db-query-keeps-native-mysql-ddl-clean ()
  "MySQL DDL should not mark manual-commit connections dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn _sql) '(:ok t)))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--backend-key-from-conn)
               (lambda (_conn) 'mysql))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch--run-db-query conn "CREATE TABLE demo (id int)")
      (should-not (clutch--tx-dirty-p conn)))))

(ert-deftest clutch-test-run-db-query-clears-dirty-on-commit ()
  "Successful COMMIT should clear dirty manual-commit state."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (puthash conn t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn _sql) '(:ok t)))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch--run-db-query conn "COMMIT")
      (should-not (clutch--tx-dirty-p conn)))))

(ert-deftest clutch-test-run-db-query-clears-dirty-on-oracle-ddl ()
  "Oracle schema-affecting DDL should clear [TX*] after auto-commit."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (conn 'fake-conn))
    (puthash conn t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch-db-query)
               (lambda (_conn _sql) '(:ok t)))
              ((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--connection-oracle-jdbc-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch--run-db-query conn "ALTER TABLE demo ADD x NUMBER")
      (should-not (clutch--tx-dirty-p conn)))))

(ert-deftest clutch-test-commit-errors-in-autocommit-mode ()
  "Clutch-commit should signal user-error when the connection is not in manual-commit mode."
  (let ((clutch-connection 'fake-conn))
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) nil)))
      (should-error (clutch-commit) :type 'user-error))))

(ert-deftest clutch-test-commit-calls-rpc-and-clears-dirty ()
  "Clutch-commit should fire the RPC and clear the dirty cache."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        committed)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-commit)
               (lambda (_conn) (setq committed t)))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch-commit)
      (should committed)
      (should-not (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-rollback-errors-in-autocommit-mode ()
  "Clutch-rollback should signal user-error when the connection is not in manual-commit mode."
  (let ((clutch-connection 'fake-conn))
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) nil)))
      (should-error (clutch-rollback) :type 'user-error))))

(ert-deftest clutch-test-rollback-calls-rpc-and-clears-dirty ()
  "Clutch-rollback should fire the RPC and clear the dirty cache."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        rolled-back)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-rollback)
               (lambda (_conn) (setq rolled-back t)))
              ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
      (clutch-rollback)
      (should rolled-back)
      (should-not (clutch--tx-dirty-p clutch-connection)))))

(defun clutch-test--make-dml-result-buf (conn)
  "Create a temporary DML result buffer associated with CONN for testing."
  (let ((buf (generate-new-buffer " *clutch-dml-test*")))
    (with-current-buffer buf
      (clutch-result-mode)
      (setq-local clutch-connection conn)
      (setq-local clutch--dml-result t)
      (setq-local header-line-format nil))
    buf))

(ert-deftest clutch-test-rollback-marks-dml-result-buffers ()
  "Clutch-rollback should add a warning banner to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
                  ((symbol-function 'clutch-db-rollback) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (clutch-rollback)
          (should (with-current-buffer buf header-line-format))
          (should (string-match-p "rolled back"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-commit-marks-dml-result-buffers ()
  "Clutch-commit should add a committed banner to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                  ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
                  ((symbol-function 'clutch-db-commit) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore))
          (clutch-commit)
          (should (string-match-p "committed"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-disconnect-marks-dml-result-buffers ()
  "Clutch-disconnect should add a connection-closed notice to open DML result buffers."
  (let* ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
         (clutch-connection 'fake-conn)
         (buf (clutch-test--make-dml-result-buf 'fake-conn)))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch-db-disconnect) #'ignore)
                  ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                  ((symbol-function 'clutch--update-console-buffer-name) #'ignore)
                  ((symbol-function 'clutch--update-mode-line) #'ignore))
          (clutch-disconnect)
          (should (string-match-p "closed"
                                  (with-current-buffer buf
                                    (substring-no-properties header-line-format)))))
      (kill-buffer buf))))

(ert-deftest clutch-test-toggle-auto-commit-manual-to-auto ()
  "Toggling from manual→auto (clean) calls set-auto-commit(t) and clears dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        captured-auto-commit
        mode-line-updated)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn v) (setq captured-auto-commit v)))
              ((symbol-function 'clutch--update-mode-line)
               (lambda () (setq mode-line-updated t))))
      (clutch-toggle-auto-commit)
      (should (eq t captured-auto-commit))
      (should mode-line-updated))))

(ert-deftest clutch-test-toggle-auto-commit-auto-to-manual ()
  "Toggling from auto→manual calls set-auto-commit(nil), does not clear dirty."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        captured-auto-commit)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) nil))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn v) (setq captured-auto-commit v)))
              ((symbol-function 'clutch--update-mode-line) #'ignore))
      (clutch-toggle-auto-commit)
      (should (eq nil captured-auto-commit)))))

(ert-deftest clutch-test-toggle-auto-commit-blocked-when-dirty ()
  "When dirty, the toggle must not proceed (no confirmation, immediate error)."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        toggle-called)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch-db-set-auto-commit)
               (lambda (_conn _v) (setq toggle-called t))))
      (should-error (clutch-toggle-auto-commit) :type 'user-error)
      (should-not toggle-called)
      (should (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-dwim-keybinding-does-not-collide-with-commit ()
  "DWIM execution should keep the primary SQL key distinct from commit."
  (should (eq (lookup-key clutch-mode-map (kbd "C-c C-c"))
              #'clutch-execute-dwim))
  (should-not (lookup-key clutch-mode-map (kbd "C-c ;")))
  (should (eq (lookup-key clutch-mode-map (kbd "C-c C-m"))
              #'clutch-commit))
  (should-not (equal (kbd "C-c C-c") (kbd "C-c C-m"))))

;;;; Connection — display key and icons

(ert-deftest clutch-test-tx-header-line-segment-preserves-icon-family ()
  "Transaction header icons should keep the nerd-icons font family."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch-db-manual-commit-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--tx-dirty-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--icon)
               (lambda (_spec &rest _fallback)
                 (propertize "[lock]"
                             'face '(:family "Symbols Nerd Font Mono")))))
      (let* ((seg (clutch--tx-header-line-segment clutch-connection))
             (face (get-text-property 0 'face seg)))
        (should (equal face '((:family "Symbols Nerd Font Mono") warning)))))))

(ert-deftest clutch-test-icon-with-face-preserves-family-without-mutating-source ()
  "Tinting an icon should preserve its family and leave the source string untouched."
  (let ((source (propertize (copy-sequence "[icon]")
                            'face '(:family "Symbols Nerd Font Mono"))))
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (_spec &rest _fallback)
                 source)))
      (let* ((icon (clutch--icon-with-face '(codicon . "nf-cod-files")
                                           "⊞"
                                           'font-lock-keyword-face))
             (source-face (get-text-property 0 'face source))
             (icon-face (get-text-property 0 'face icon)))
        (should (equal source-face '(:family "Symbols Nerd Font Mono")))
        (should (equal icon-face
                       '((:family "Symbols Nerd Font Mono")
                         font-lock-keyword-face)))))))

(ert-deftest clutch-test-fixed-width-icon-preserves-icon-family-when-faced ()
  "Fixed-width icons should append FACE without dropping the icon family."
  (cl-letf (((symbol-function 'clutch--icon)
             (lambda (_spec &rest _fallback)
               (propertize "[sort]"
                           'face '(:family "Symbols Nerd Font Mono")))))
    (let* ((icon (clutch--fixed-width-icon '(codicon . "nf-cod-arrow_up")
                                           "▲"
                                           'header-line))
           (face (get-text-property 0 'face icon)))
      (should (equal face
                     '((:family "Symbols Nerd Font Mono")
                       header-line))))))

(ert-deftest clutch-test-icon-supports-any-family ()
  "Icon helper should dispatch any nerd-icons family via nerd-icons--function-name."
  (cl-letf (((symbol-function 'require) (lambda (&rest _) t))
            ((symbol-function 'nerd-icons--function-name)
             (lambda (family) (intern (concat "mock-icon-" (symbol-name family)))))
            ((symbol-function 'mock-icon-octicon)
             (lambda (name) (concat "oct:" name)))
            ((symbol-function 'mock-icon-devicon)
             (lambda (name) (concat "dev:" name))))
    (should (equal (clutch--icon '(octicon . "nf-oct-sort_desc") "fallback")
                   "oct:nf-oct-sort_desc"))
    (should (equal (clutch--icon '(devicon . "nf-dev-mysql") "fallback")
                   "dev:nf-dev-mysql"))))

(ert-deftest clutch-test-connected-header-line-uses-backend-direction-separator ()
  "Connected header line should group backend and connection identity."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-backend-segment)
               (lambda (&rest _args) "[db]"))
              ((symbol-function 'clutch--connection-state-icon)
               (lambda (_connected) "[ok]"))
              ((symbol-function 'clutch--connection-display-key)
               (lambda (_conn) "user@host"))
              ((symbol-function 'clutch--current-schema-header-line-segment)
               (lambda (_conn) "[schema] app"))
              ((symbol-function 'clutch--schema-status-header-line-segment)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment)
               (lambda (_conn) "Tx: Auto"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (substring-no-properties
                   (clutch--build-connection-header-line))))
        (should (equal line
                       "[db]  ›  [ok] user@host  •  [schema] app  •  Tx: Auto"))))))

(ert-deftest clutch-test-header-line-shows-schema-even-when-redundant ()
  "Connection header line should show the effective schema whenever available."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "MySQL"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "user@host"))
              ((symbol-function 'clutch-db-user) (lambda (_conn) "user"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "zj_test"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) "zj_test"))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "user@host" line))
        (should (string-match-p "\\[schema\\] zj_test" line))
        (should-not (string-match-p "Schema:" line))))))

(ert-deftest clutch-test-header-line-shows-clickhouse-database ()
  "Connection header line should show the current ClickHouse database."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-clickhouse-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "ClickHouse"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "default@127.0.0.1"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "demo"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) nil))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "default@127.0.0.1" line))
        (should (string-match-p "\\[schema\\] demo" line))))))

(ert-deftest clutch-test-disconnected-header-line-shows-backend-and-warning-state ()
  "Disconnected header line should keep backend context and warn loudly."
  (with-temp-buffer
    (setq-local clutch-connection nil
                clutch--connection-params '(:backend jdbc :driver oracle))
    ;; With nerd-icons: show icon only, no name.
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) "[db]"))
              ((symbol-function 'clutch--nerd-icons-available-p) (lambda () t))
              ((symbol-function 'clutch--backend-display-name-from-params) (lambda (_params) "Oracle"))
              ((symbol-function 'clutch--connection-state-icon) (lambda (_connected) "[disc]"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let* ((line (clutch--build-connection-header-line))
             (start (string-match "Disconnect" line)))
        (should (string-match-p "\\[db\\]" line))
        (should-not (string-match-p "Oracle" line))
        (should start)
        (should (eq (get-text-property start 'face line) 'warning))))
    ;; Without nerd-icons: show name, no icon.
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) nil))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) ""))
              ((symbol-function 'clutch--nerd-icons-available-p) (lambda () nil))
              ((symbol-function 'clutch--backend-display-name-from-params) (lambda (_params) "Oracle"))
              ((symbol-function 'clutch--connection-state-icon) (lambda (_connected) "[disc]"))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let* ((line (clutch--build-connection-header-line))
             (start (string-match "Disconnect" line)))
        (should (string-match-p "Oracle" line))
        (should start)
        (should (eq (get-text-property start 'face line) 'warning))))))

(ert-deftest clutch-test-connection-display-key-hides-default-port ()
  "Connection display key should omit the backend default port."
  (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "scott"))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "dbhost"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 1521))
            ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "scott@dbhost"))))

(ert-deftest clutch-test-connection-display-key-keeps-nondefault-port ()
  "Connection display key should keep non-default ports."
  (cl-letf (((symbol-function 'clutch-db-user) (lambda (_conn) "scott"))
            ((symbol-function 'clutch-db-host) (lambda (_conn) "dbhost"))
            ((symbol-function 'clutch-db-port) (lambda (_conn) 1522))
            ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle")))
    (should (equal (clutch--connection-display-key 'fake-conn)
                   "scott@dbhost:1522"))))

(ert-deftest clutch-test-jdbc-backend-icon-spec-uses-database-cog-outline ()
  "Generic JDBC should use the requested database-cog-outline icon."
  (should (equal (car (car (alist-get 'jdbc clutch--db-icon-specs))) 'mdicon))
  (should (equal (cdr (car (alist-get 'jdbc clutch--db-icon-specs)))
                 "nf-md-database_cog_outline")))

(ert-deftest clutch-test-connection-state-icon-uses-database-check-outline ()
  "Connected state icon should use the requested database-check-outline glyph."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--icon)
               (lambda (spec fallback)
                 (setq captured (list spec fallback))
                 "[ok]")))
      (should (equal (clutch--connection-state-icon t) "[ok]"))
      (should (equal captured '((mdicon . "nf-md-database_check_outline") "⬢"))))))

(ert-deftest clutch-test-connection-display-key-derives-generic-jdbc-endpoint ()
  "Generic JDBC connections should show endpoint details derived from :url."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn
               :params '(:driver jdbc
                         :url "jdbc:kingbase8://127.0.0.1:54321/test"
                         :display-name "KingbaseES"
                         :user "system"))))
    (should (equal (clutch--connection-display-key conn)
                   "system@127.0.0.1:54321"))))

;;;; Connection — reconnect and disconnect

(ert-deftest clutch-test-disconnect-blocks-dirty-manual-commit-connection ()
  "Disconnect should warn before dropping uncommitted manual-commit work."
  (let ((clutch--tx-dirty-cache (make-hash-table :test 'eq))
        (clutch-connection 'fake-conn)
        disconnected)
    (puthash clutch-connection t clutch--tx-dirty-cache)
    (cl-letf (((symbol-function 'clutch-db-manual-commit-p) (lambda (_conn) t))
              ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'yes-or-no-p) (lambda (_prompt) nil))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t))))
      (should-error (clutch-disconnect) :type 'user-error)
      (should-not disconnected)
      (should clutch-connection)
      (should (clutch--tx-dirty-p clutch-connection)))))

(ert-deftest clutch-test-do-disconnect-stops-ssh-tunnel ()
  "Disconnect should stop any SSH tunnel associated with the connection."
  (let ((clutch--connection-remote-params-cache (make-hash-table :test 'eq))
        (clutch--connection-ssh-tunnel-cache (make-hash-table :test 'eq))
        disconnected
        stopped)
    (puthash 'fake-conn '(:process fake-proc) clutch--connection-ssh-tunnel-cache)
    (cl-letf (((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
              ((symbol-function 'clutch--invalidate-derived-buffers) #'ignore)
              ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
              ((symbol-function 'clutch--forget-problem-record) #'ignore)
              ((symbol-function 'clutch-db-disconnect)
               (lambda (_conn) (setq disconnected t)))
              ((symbol-function 'process-live-p)
               (lambda (proc) (eq proc 'fake-proc)))
              ((symbol-function 'delete-process)
               (lambda (proc) (setq stopped proc))))
      (clutch--do-disconnect 'fake-conn)
      (should disconnected)
      (should (eq stopped 'fake-proc))
      (should-not (gethash 'fake-conn clutch--connection-ssh-tunnel-cache)))))

(ert-deftest clutch-test-kill-console-disconnects-and-invalidates ()
  "Killing a console buffer disconnects and invalidates derived buffers."
  (let ((disconnected nil)
        (console (generate-new-buffer " *clutch-console*"))
        (result (generate-new-buffer " *clutch-result*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer result
            (setq-local clutch-connection 'fake-conn))
          (with-current-buffer console
            (clutch-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer console)
          (should disconnected)
          ;; Derived buffer's connection should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p console) (kill-buffer console))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-kill-indirect-buffer-does-not-disconnect ()
  "Killing an indirect SQL buffer must NOT disconnect the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-indirect*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer buf
            (clutch-mode)
            (clutch--indirect-mode 1)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should-not disconnected)))

(ert-deftest clutch-test-kill-plain-sql-buffer-disconnects ()
  "Killing a plain `clutch-mode' buffer with no console-name should still disconnect.
This applies when the buffer owns the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-plain-sql*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--save-console) #'ignore))
          (with-current-buffer buf
            (clutch-mode)
            ;; No clutch--console-name, no clutch--indirect-mode.
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should disconnected)))

(ert-deftest clutch-test-kill-repl-buffer-disconnects ()
  "Killing a REPL buffer that owns a connection should disconnect."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-repl-kill*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_c) t))
                  ((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore))
          (with-current-buffer buf
            (clutch-repl-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should disconnected)))

(ert-deftest clutch-test-reconnect-invalidates-derived-buffers ()
  "Reconnecting in a console should invalidate derived buffers holding the old connection."
  (let ((old-conn (list 'old))
        (new-conn (list 'new))
        (result (generate-new-buffer " *clutch-result-reconnect*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (c) (eq c old-conn)))
                  ((symbol-function 'clutch-db-disconnect) #'ignore)
                  ((symbol-function 'clutch--confirm-disconnect-transaction-loss) #'ignore)
                  ((symbol-function 'clutch--mark-dml-results-connection-closed) #'ignore)
                  ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                  ((symbol-function 'clutch--read-connection-params)
                   (lambda () '(:backend mysql :host "localhost")))
                  ((symbol-function 'clutch--effective-sql-product) (lambda (_p) 'mysql))
                  ((symbol-function 'clutch--build-conn) (lambda (_p) new-conn))
                  ((symbol-function 'clutch--bind-connection-context) #'ignore)
                  ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                  ((symbol-function 'clutch--update-mode-line) #'ignore)
                  ((symbol-function 'clutch--connection-key) (lambda (_c) "test")))
          (with-current-buffer result
            (setq-local clutch-connection old-conn))
          (let ((clutch-connection old-conn))
            (clutch-connect))
          ;; After reconnect, derived buffer should be invalidated.
          (should-not (buffer-local-value 'clutch-connection result)))
      (when (buffer-live-p result) (kill-buffer result)))))

(ert-deftest clutch-test-kill-result-buffer-does-not-disconnect ()
  "Killing a derived result buffer must NOT disconnect the connection."
  (let ((disconnected nil)
        (buf (generate-new-buffer " *clutch-result-kill*")))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch-db-disconnect)
                   (lambda (_c) (setq disconnected t)))
                  ((symbol-function 'clutch--result-buffer-cleanup) #'ignore))
          (with-current-buffer buf
            (clutch-result-mode)
            (setq clutch-connection 'fake-conn))
          (kill-buffer buf))
      (when (buffer-live-p buf) (kill-buffer buf)))
    (should-not disconnected)))

(ert-deftest clutch-test-reconnect-preserves-pending ()
  "Reconnect preserves staged changes in result buffers."
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
                      ((symbol-function 'clutch--update-mode-line) #'ignore))
              (setq-local clutch--connection-params '(:backend mysql))
              (clutch--try-reconnect)))
          (with-current-buffer result-buf
            (should (equal clutch--pending-deletes (list (vector 1))))))
      (kill-buffer result-buf)
      (kill-buffer clutch-buf))))

(ert-deftest clutch-test-try-reconnect-releases-old-ssh-transport ()
  "Reconnect should stop the old SSH tunnel after the new connection is ready."
  (let ((released nil))
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn
                  clutch--connection-params '(:backend pg
                                              :host "db.internal"
                                              :port 5432
                                              :ssh-host "bastion-prod")
                  clutch--conn-sql-product 'pg)
      (cl-letf (((symbol-function 'clutch--build-conn)
                 (lambda (_params) 'new-conn))
                ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                ((symbol-function 'clutch--release-connection-transport)
                 (lambda (conn) (setq released conn)))
                ((symbol-function 'clutch--rebind-connection-buffers) #'ignore)
                ((symbol-function 'clutch--finalize-rebound-connection)
                 (lambda (_conn) 'done))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "alice@db.internal:5432/appdb"))
                ((symbol-function 'message) #'ignore))
        (should (clutch--try-reconnect))
        (should (eq released 'old-conn))))))

(ert-deftest clutch-test-result-buffer-reconnects-using-inherited-context ()
  "Result buffers should inherit reconnect params from their source buffer."
  (let (result-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer))
                (result (make-clutch-db-result
                         :connection 'old-conn
                         :columns nil
                         :rows nil)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-live-p)
                       (lambda (_conn) t))
                      ((symbol-function 'clutch--connection-key)
                       (lambda (conn) (symbol-name conn)))
                      ((symbol-function 'clutch--display-dml-result) #'ignore)
                      ((symbol-function 'clutch--show-result-buffer)
                       (lambda (buf) (setq result-buf buf) buf)))
              (clutch--display-result result "UPDATE demo SET x = 1" 0.1))
            (with-current-buffer result-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-result-status-line) #'ignore)
                        ((symbol-function 'clutch--update-position-indicator) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p result-buf)
        (kill-buffer result-buf)))))

(ert-deftest clutch-test-object-buffer-reconnects-using-inherited-context ()
  "Object definition buffers should inherit reconnect params from their source buffer."
  (let (object-buf built)
    (unwind-protect
        (with-temp-buffer
          (let ((source-buf (current-buffer)))
            (setq-local clutch-connection 'old-conn
                        clutch--connection-params '(:backend mysql :database "db")
                        clutch--conn-sql-product 'mysql)
            (cl-letf (((symbol-function 'clutch-db-show-create-table)
                       (lambda (_conn _table) "CREATE TABLE demo (id INT)"))
                      ((symbol-function 'sql-mode) #'ignore)
                      ((symbol-function 'sql-set-product) #'ignore)
                      ((symbol-function 'font-lock-ensure) #'ignore)
                      ((symbol-function 'clutch--connection-key)
                       (lambda (conn) (symbol-name conn)))
                      ((symbol-function 'pop-to-buffer)
                       (lambda (buf &rest _args)
                         (setq object-buf buf)
                         buf)))
              (clutch-object-show-ddl-or-source '(:name "demo" :type "TABLE")))
            (with-current-buffer object-buf
              (should (equal clutch--connection-params
                             '(:backend mysql :database "db")))
              (should (eq clutch--conn-sql-product 'mysql))
              (cl-letf (((symbol-function 'clutch-db-live-p)
                         (lambda (conn) (eq conn 'new-conn)))
                        ((symbol-function 'clutch--build-conn)
                         (lambda (params)
                           (setq built params)
                           'new-conn))
                        ((symbol-function 'clutch--clear-tx-dirty) #'ignore)
                        ((symbol-function 'clutch--prime-schema-cache) #'ignore)
                        ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                        ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                        ((symbol-function 'clutch--connection-key)
                         (lambda (conn) (symbol-name conn)))
                        ((symbol-function 'message) #'ignore))
                (clutch--ensure-connection)
                (should (eq clutch-connection 'new-conn))
                (should (equal built '(:backend mysql :database "db")))))
            (with-current-buffer source-buf
              (should (eq clutch-connection 'new-conn)))))
      (when (buffer-live-p object-buf)
        (kill-buffer object-buf)))))

(ert-deftest clutch-test-connect-failure-preserves-old-live-connection ()
  "Interactive connect should not drop the old live session on failure."
  (let ((clutch-connection 'old-conn)
        disconnected)
    (with-temp-buffer
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "newdb")))
                ((symbol-function 'clutch--build-conn)
                 (lambda (_params)
                   (signal 'clutch-db-error '("connect failed"))))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch-connect) :type 'clutch-db-error)
        (should (eq clutch-connection 'old-conn))
        (should-not disconnected)))))

(ert-deftest clutch-test-connect-rebuilds-conn-when-agent-dies-during-disconnect ()
  "Reconnect should rebuild a dead new connection after old disconnect."
  (let ((built nil)
        (activated nil))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch-connection 'old-conn)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (conn)
                   (pcase conn
                     ('old-conn t)
                     ('new-conn-1 nil)
                     (_ t))))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 #'ignore)
                ((symbol-function 'clutch--connect-params-for-current-buffer)
                 (lambda () '(:backend jdbc :database "newdb")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'jdbc))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (push params built)
                   (pcase (length built)
                     (1 'new-conn-1)
                     (2 'new-conn-2)
                     (_ 'unexpected-conn))))
                ((symbol-function 'clutch--do-disconnect)
                 #'ignore)
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (conn params product)
                   (setq activated (list conn params product))))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (= (length built) 2))
        (should (equal (nreverse built)
                       '((:backend jdbc :database "newdb")
                         (:backend jdbc :database "newdb"))))
        (should (equal activated
                       (list 'new-conn-2
                             (clutch--materialize-connection-params
                              '(:backend jdbc :database "newdb"))
                             'jdbc)))))))

(ert-deftest clutch-test-replace-connection-rebuilds-dead-conn-after-disconnect ()
  "Replacing a connection should rebuild if the first new conn dies during disconnect."
  (let (built rebound finalized disconnected cleared)
    (cl-letf (((symbol-function 'clutch--effective-sql-product)
               (lambda (_params) 'clickhouse))
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "default-key"))
              ((symbol-function 'clutch--build-conn)
               (lambda (params)
                 (push params built)
                 (pcase (length built)
                   (1 'new-conn-1)
                   (2 'new-conn-2)
                   (_ 'unexpected-conn))))
              ((symbol-function 'clutch--clear-tx-dirty)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (conn)
                 (pcase conn
                   ('old-conn t)
                   ('new-conn-1 nil)
                   ('new-conn-2 t)
                   (_ nil))))
              ((symbol-function 'clutch-db-disconnect)
               (lambda (conn) (setq disconnected conn)))
              ((symbol-function 'clutch--rebind-connection-buffers)
               (lambda (_old new params product)
                 (setq rebound (list new params product))))
              ((symbol-function 'clutch--clear-connection-metadata-caches)
               (lambda (conn &optional key)
                 (push (list conn key) cleared)))
              ((symbol-function 'clutch--finalize-rebound-connection)
               (lambda (conn) (setq finalized conn) conn)))
      (clutch--replace-connection 'old-conn '(:backend clickhouse :database "demo") 'clickhouse)
      (should (equal (nreverse built)
                     '((:backend clickhouse :database "demo")
                       (:backend clickhouse :database "demo"))))
      (should (eq disconnected 'old-conn))
      (should (equal rebound '(new-conn-2 (:backend clickhouse :database "demo") clickhouse)))
      (should (eq finalized 'new-conn-2))
      (should (equal (nreverse cleared)
                     '((old-conn "default-key")
                       (new-conn-2 nil)))))))

(ert-deftest clutch-test-connect-in-query-console-reuses-saved-connection ()
  "Query console reconnect should reuse that console's saved connection."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))
             ("beta" . (:backend mysql :database "app_b"))))
          built
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "should-not-be-used")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should-not read-called)
        (should (equal built '(:backend mysql :database "app_a" :pass-entry "alpha")))))))

(ert-deftest clutch-test-connect-stores-resolved-password-in-connection-context ()
  "Connect should retain resolved credentials for reconnects."
  (with-temp-buffer
    (let ((clutch-connection-alist
           '(("alpha" . (:backend mysql :database "app_a"))))
          built
          activated)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--resolve-password)
                 (lambda (_params) "secret"))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn params _product)
                   (setq activated params)
                   nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (equal built
                       '(:backend mysql :database "app_a"
                         :pass-entry "alpha")))
        (should (equal activated
                       (clutch--materialize-connection-params
                        '(:backend mysql :database "app_a"
                          :pass-entry "alpha"))))
        ))))

(ert-deftest clutch-test-connect-in-query-console-errors-when-saved-connection-missing ()
  "Query console reconnect should error when its saved connection no longer exists."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("beta" . (:backend mysql :database "app_b"))))
          read-called)
      (setq-local clutch--console-name "alpha")
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda ()
                   (setq read-called t)
                   '(:backend mysql :database "fallback"))))
        (should-error (clutch-connect) :type 'user-error)
        (should-not read-called)))))

(ert-deftest clutch-test-query-console-does-not-create-buffer-on-connect-failure ()
  "Query console should not create a visible buffer before connect succeeds."
  (let* ((name "alpha")
         (buffer-name (clutch--console-buffer-base-name name))
         (clutch-connection-alist
          '(("alpha" . (:backend mysql :database "app_a")))))
    (unwind-protect
        (cl-letf (((symbol-function 'clutch--effective-sql-product)
                   (lambda (_params) 'mysql))
                  ((symbol-function 'clutch--build-conn)
                   (lambda (_params)
                     (user-error "Connection refused"))))
          (should-error (clutch-query-console name) :type 'user-error)
          (should-not (get-buffer buffer-name)))
      (when-let* ((buf (get-buffer buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-query-console-switches-to-existing-connected-buffer ()
  "Query console should reuse an existing connected console buffer."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-existing*"))
         built)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'live-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (_name) existing))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (conn) (eq conn 'live-conn)))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (_params)
                       (setq built t)
                       'unexpected-conn)))
            (clutch-query-console name)
            (should-not built)
            (should (eq (current-buffer) existing))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-query-console-reconnects-dead-existing-buffer ()
  "Query console should reconnect an existing dead console before switching."
  (let* ((name "alpha")
         (existing (get-buffer-create " *clutch-query-console-dead*"))
         (clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
         built
         activated)
    (unwind-protect
        (with-current-buffer existing
          (clutch-mode)
          (setq-local clutch--console-name name)
          (setq-local clutch-connection 'dead-conn)
          (cl-letf (((symbol-function 'clutch--find-console-buffer)
                     (lambda (_name) existing))
                    ((symbol-function 'clutch--connection-alive-p)
                     (lambda (_conn) nil))
                    ((symbol-function 'clutch--effective-sql-product)
                     (lambda (_params) 'mysql))
                    ((symbol-function 'clutch--build-conn)
                     (lambda (params)
                       (setq built params)
                       'new-conn))
                    ((symbol-function 'clutch--update-console-buffer-name)
                     (lambda () nil))
                    ((symbol-function 'clutch--activate-current-buffer-connection)
                     (lambda (conn params product)
                       (setq-local clutch-connection conn)
                       (setq activated (list conn params product)))))
            (clutch-query-console name)
            (should (equal built '(:backend mysql :database "app_a" :pass-entry "alpha")))
            (should (equal activated
                           '(new-conn (:backend mysql :database "app_a" :pass-entry "alpha") mysql)))
            (should (eq (current-buffer) existing))))
      (when (buffer-live-p existing)
        (kill-buffer existing)))))

(ert-deftest clutch-test-connect-outside-console-still-uses-generic-read-flow ()
  "Non-console buffers should keep the generic interactive connect flow."
  (with-temp-buffer
    (let ((clutch-connection-alist '(("alpha" . (:backend mysql :database "app_a"))))
          built)
      (cl-letf (((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) nil))
                ((symbol-function 'clutch--read-connection-params)
                 (lambda () '(:backend mysql :database "manual_db")))
                ((symbol-function 'clutch--effective-sql-product)
                 (lambda (_params) 'mysql))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built params)
                   'new-conn))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "test-conn"))
                ((symbol-function 'clutch--activate-current-buffer-connection)
                 (lambda (_conn _params _product) nil))
                ((symbol-function 'message) #'ignore))
        (clutch-connect)
        (should (equal built '(:backend mysql :database "manual_db")))))))

;;;; Debug — error humanization

(ert-deftest clutch-test-humanize-db-error-clickhouse-update ()
  "ClickHouse NOT_IMPLEMENTED error should get cleaned and hinted."
  (let ((msg (concat "Code: 48. DB::Exception: Lightweight updates are not supported. "
                     "Lightweight updates are supported only for tables with materialized "
                     "_block_number column. (NOT_IMPLEMENTED) "
                     "(version 26.2.5.45 (official build))  "
                     "(queryId= 92961bbf-e430-4c89-95c3-0deb471daec6)")))
    (let ((result (clutch--humanize-db-error msg)))
      (should (string-match-p "enable lightweight update" result))
      (should-not (string-match-p "queryId" result))
      (should-not (string-match-p "version 26" result)))))

(ert-deftest clutch-test-humanize-db-error-clickhouse-delete ()
  "ClickHouse lightweight delete error should match singular and plural forms."
  (dolist (msg '("Lightweight delete is not supported"
                 "Lightweight deletes are not supported"))
    (let ((result (clutch--humanize-db-error msg)))
      (should (string-match-p "enable lightweight delete" result)))))

(ert-deftest clutch-test-humanize-db-error-oracle-ora00942 ()
  "ORA-00942 should get a friendly hint appended."
  (let ((result (clutch--humanize-db-error "ORA-00942: table or view does not exist")))
    (should (string-match-p "ORA-00942" result))
    (should (string-match-p "table or view does not exist" result))))

(ert-deftest clutch-test-humanize-db-error-connection-refused ()
  "Connection refused should hint to check host and port."
  (let ((result (clutch--humanize-db-error "Connection refused (host=db.example.com, port=3306)")))
    (should (string-match-p "check host and port" result))))

(ert-deftest clutch-test-humanize-db-error-jdbc-driver-missing ()
  "Missing JDBC driver should point to the concrete install command."
  (let ((result (clutch--humanize-db-error
                 "SQLException [SQLState=08001]: No suitable driver found for jdbc:oracle:thin:@//db:1521/ORCL")))
    (should (string-match-p "No suitable driver found" result))
    (should (string-match-p "clutch-jdbc-install-driver RET oracle" result))))

(ert-deftest clutch-test-humanize-db-error-unknown-passes-through ()
  "Unknown errors should pass through with noise stripped but no hint."
  (let ((result (clutch--humanize-db-error "Something totally unexpected happened")))
    (should (equal result "Something totally unexpected happened"))
    (should-not (string-match-p "\\[" result))))

(ert-deftest clutch-test-humanize-db-error-strips-queryid ()
  "ClickHouse queryId and version suffixes should be removed."
  (let ((result (clutch--humanize-db-error
                 "Some error (version 24.1.1.1 (official build)) (queryId= abc-123)")))
    (should-not (string-match-p "queryId" result))
    (should-not (string-match-p "version 24" result))
    (should (string-match-p "Some error" result))))

(ert-deftest clutch-test-humanize-db-error-strips-stack-trace ()
  "Java stack traces should be truncated from the first 'at' frame."
  (let ((result (clutch--humanize-db-error
                 (concat "Connection failed: timeout\n"
                         "\tat java.base/java.net.Socket.connect(Socket.java:633)\n"
                         "\tat com.clickhouse.client.Http.open(Http.java:42)"))))
    (should (string-match-p "Connection failed" result))
    (should-not (string-match-p "java\\.base" result))
    (should-not (string-match-p "Socket" result))))

(ert-deftest clutch-test-humanize-db-error-strips-database-error-prefix ()
  "The redundant 'Database error: ' prefix should be removed."
  (let ((result (clutch--humanize-db-error "Database error: ORA-00942: table does not exist")))
    (should-not (string-prefix-p "Database error:" result))
    (should (string-match-p "ORA-00942" result))))

;;;; Debug — problem records and buffer

(ert-deftest clutch-test-debug-buffer-removes-old-details-commands ()
  "The old error-details workflow should be deleted outright."
  (should-not (fboundp 'clutch-show-last-error-details))
  (should-not (fboundp 'clutch-show-error-details))
  (should-not (fboundp 'clutch-debug-buffer))
  (should-not (fboundp 'clutch-error-details-refresh))
  (should-not (fboundp 'clutch-error-details-copy-message))
  (should-not (fboundp 'clutch-error-details-copy-all))
  (should-not (boundp 'clutch-error-details-mode-map)))

(ert-deftest clutch-test-debug-mode-creates-dedicated-buffer ()
  "Enabling debug mode should create the dedicated debug buffer immediately."
  (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
    (kill-buffer buf))
  (let ((clutch-debug-mode nil))
    (unwind-protect
        (progn
          (clutch-debug-mode 1)
          (let ((buf (get-buffer clutch-debug-buffer-name)))
            (should (buffer-live-p buf))
            (with-current-buffer buf
              (should (derived-mode-p 'clutch--debug-buffer-mode))
              (should (string-match-p "Clutch Debug"
                                      (buffer-string))))))
      (clutch-debug-mode -1)
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-buffer-appends-current-buffer-failure ()
  "Problem records should be appended to the dedicated debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Connection failed [check host and port]"
                   :diag (:category "connect"
                          :op "connect"
                          :request-id 71
                          :exception-class "java.sql.SQLNonTransientConnectionException"
                          :sql-state "08071"
                          :context (:redacted-url "jdbc:clickhouse://127.0.0.1:8123/testdb?password=<redacted>"
                                    :generated-sql "ALTER SESSION SET CURRENT_SCHEMA = \"REPORTING\""
                                    :property-keys ("http_header_COOKIE" "socket_timeout"))
                          :cause-chain ((:exception-class "java.sql.SQLNonTransientConnectionException"
                                         :message "reason-71")
                                        (:exception-class "java.net.ConnectException"
                                         :message "root-71")))
                   :stderr-tail "Connect request 71 failed")))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (clutch--remember-problem-record
         :buffer (current-buffer)
         :problem details)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Connection failed" text))
          (should (string-match-p "08071" text))
          (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" text))
          (should (string-match-p "Connect request 71 failed" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-connection-problem-record ()
  "Connection-scoped problem records should also be appended to the debug buffer."
  (let ((problem '(:backend oracle
                   :summary "Metadata failed"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 88
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (clutch--remember-problem-record
         :connection 'fake-conn
         :problem problem)
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Metadata failed" text))
          (should (string-match-p "get-columns" text)))))))

(ert-deftest clutch-test-debug-buffer-appends-debug-trace-when-enabled ()
  "Debug mode should append recent captured events to the debug buffer."
  (let ((details '(:backend jdbc
                   :summary "Query failed"
                   :diag (:category "query"
                          :op "execute"
                          :raw-message "ORA-00942"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch--buffer-error-details details)
        (clutch--remember-debug-event
         :op "execute"
         :phase "error"
         :backend 'oracle
         :summary "Query failed"
         :sql "SELECT * FROM missing_table")
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Trace Event" text))
          (should (string-match-p "Operation: execute" text))
          (should (string-match-p "Phase: error" text))
          (should (string-match-p "Query failed" text))
          (should (string-match-p "SELECT \\* FROM missing_table" text)))))))

(ert-deftest clutch-test-run-db-query-success-clears-problems-across-connection-buffers ()
  "Successful queries should clear stale failure state for the whole connection."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-source*"))
        (peer (generate-new-buffer " *clutch-debug-peer*"))
        cleared)
    (unwind-protect
        (progn
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "old")))
          (with-current-buffer peer
            (setq-local clutch-connection conn)
            (cl-letf (((symbol-function 'clutch-db-query)
                       (lambda (_conn _sql) 'ok))
                      ((symbol-function 'clutch-db-manual-commit-p)
                       (lambda (_conn) nil))
                      ((symbol-function 'clutch-db-clear-error-details)
                       (lambda (clear-conn)
                         (setq cleared clear-conn))))
              (should (eq (clutch--run-db-query conn "SELECT 1") 'ok))))
          (with-current-buffer source
            (should-not clutch--buffer-error-details)
            (should-not (clutch--problem-record-for-connection conn)))
          (should-not (gethash conn clutch--problem-records-by-conn))
          (should (eq cleared conn)))
      (kill-buffer source)
      (kill-buffer peer))))

(ert-deftest clutch-test-debug-mode-enable-replays-stored-problem-records ()
  "Enabling debug mode should replay already-stored problems as history."
  (let ((source (generate-new-buffer " *clutch-debug-replay*"))
        (summary "pre-debug failure"))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem `(:backend oracle :summary ,summary)))
          (clutch-debug-mode 1)
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Historical Problems" text))
            (should (string-match-p summary text))))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-does-not-clear-problem-records ()
  "Enabling debug mode should keep already-stored problem records."
  (let ((source (generate-new-buffer " *clutch-debug-problem*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch--clear-problem-capture)
          (with-current-buffer source
            (clutch--remember-problem-record
             :buffer source
             :problem '(:summary "pre-debug")))
          (clutch-debug-mode 1)
          (with-current-buffer source
            (should clutch--buffer-error-details)))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-debug-mode-enable-clears-debug-events-but-keeps-problems ()
  "Starting a new debug capture should clear events but keep problems."
  (let ((conn 'fake-conn)
        (source (generate-new-buffer " *clutch-debug-capture*")))
    (unwind-protect
        (progn
          (when clutch-debug-mode
            (clutch-debug-mode -1))
          (clutch--clear-problem-capture)
          (with-current-buffer source
            (setq-local clutch-connection conn)
            (clutch-debug-mode 1)
            (clutch--remember-problem-record
             :buffer source
             :connection conn
             :problem '(:summary "pre-debug"))
            (clutch--remember-debug-event
             :buffer source
             :connection conn
             :op "execute"
             :phase "error"
             :summary "boom")
            (should clutch--debug-events)
            (should clutch--buffer-error-details)
            (clutch-debug-mode -1)
            (clutch-debug-mode 1)
            (should-not clutch--debug-events)
            (should clutch--buffer-error-details)))
      (when clutch-debug-mode
        (clutch-debug-mode -1))
      (when (buffer-live-p source)
        (kill-buffer source))
      (when-let* ((buf (get-buffer clutch-debug-buffer-name)))
        (kill-buffer buf)))))

(ert-deftest clutch-test-execute-error-populates-problem-record ()
  "SQL execution failures should populate the current buffer problem record."
  (let ((source (generate-new-buffer " *clutch-error-source*")))
    (unwind-protect
        (with-current-buffer source
          (set-window-buffer (selected-window) source)
          (setq-local clutch-connection 'fake-conn
                      clutch--source-window (selected-window))
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn sql _page-num _page-size) sql))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn _sql)
                       (signal 'clutch-db-error
                               (list "ORA-00942: table or view does not exist"))))
                    ((symbol-function 'clutch-db-error-details)
                     (lambda (_conn) nil))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (catch 'clutch--execution-aborted
              (clutch--execute-select "SELECT * FROM missing_table" 'fake-conn))
            (let* ((details clutch--buffer-error-details)
                   (diag (plist-get details :diag)))
              (should details)
              (should (equal (plist-get details :summary)
                             (clutch--humanize-db-error
                              "ORA-00942: table or view does not exist")))
              (should (equal (plist-get diag :raw-message)
                             "ORA-00942: table or view does not exist"))
              (should (equal (plist-get (plist-get diag :context) :sql)
                             "SELECT * FROM missing_table")))))
      (kill-buffer source))))

(ert-deftest clutch-test-execute-error-points-to-debug-workflow-when-disabled ()
  "Query failures should point users at the single debug workflow."
  (let ((source (generate-new-buffer " *clutch-execute-hint-source*"))
        message-text)
    (unwind-protect
        (with-current-buffer source
          (set-window-buffer (selected-window) source)
          (setq-local clutch-connection 'fake-conn
                      clutch--source-window (selected-window))
          (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
                     (lambda (_conn sql _page-num _page-size) sql))
                    ((symbol-function 'clutch--run-db-query)
                     (lambda (_conn _sql)
                       (signal 'clutch-db-error
                               (list "ORA-00942: table or view does not exist"))))
                    ((symbol-function 'clutch-db-error-details)
                     (lambda (_conn) nil))
                    ((symbol-function 'message)
                     (lambda (fmt &rest args)
                       (setq message-text (apply #'format fmt args))))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (catch 'clutch--execution-aborted
              (clutch--execute-select "SELECT * FROM missing_table" 'fake-conn))
            (should (string-match-p "clutch-debug-mode" message-text))
            (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                    message-text))))
      (kill-buffer source))))

;;;; Execute — query execution and error handling

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
                   (error "Should not paginate")))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (cl-incf calls)
                   (make-clutch-db-result :rows '((1) (2))))))
        (should (equal (clutch-result--collect-all-export-rows) '((1) (2))))
        (should (= calls 1))))))

(ert-deftest clutch-test-collect-all-export-rows-ensures-connection ()
  "Export-all should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT id FROM t LIMIT 1"
                  clutch--last-query "SELECT id FROM t LIMIT 1")
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :rows '((1))))))
        (should (equal (clutch-result--collect-all-export-rows) '((1))))
        (should ensured)
        (should (eq captured-conn 'new-conn))))))

(ert-deftest clutch-test-execute-select-detects-primary-key-before-first-render ()
  "Primary key cache should be ready before the first result render."
  (let ((clutch--source-window (selected-window))
        (captured-pk :unset))
    (cl-letf (((symbol-function 'clutch-db-build-paged-sql)
               (lambda (_conn sql _page-num _page-size &optional _order-by) sql))
              ((symbol-function 'clutch-db-row-identity-candidates)
               (lambda (_conn _table)
                 (list (list :kind 'primary-key
                             :name "PRIMARY"
                             :columns '("id")))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn id) (format "\"%s\"" id)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn _sql)
                 (make-clutch-db-result
                  :columns '((:name "id") (:name "name") (:name "clutch__rid_0"))
                  :rows '((1 "a" 1)))))
              ((symbol-function 'clutch--result-buffer-name)
               (lambda () "*clutch-test-result*"))
              ((symbol-function 'clutch--show-result-buffer) #'ignore)
              ((symbol-function 'clutch--load-fk-info) #'ignore)
              ((symbol-function 'clutch--display-select-result)
               (lambda (&rest _args)
                 (setq captured-pk clutch--cached-pk-indices))))
      (unwind-protect
          (progn
            (clutch--execute-select "SELECT * FROM users" 'fake-conn)
            (should (equal captured-pk '(0))))
        (when-let* ((buf (get-buffer "*clutch-test-result*")))
          (kill-buffer buf))))))

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

(ert-deftest clutch-test-preview-execution-sql-prefers-pending-edits-in-result-mode ()
  "Preview in result mode should show generated UPDATE SQL when edits exist."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch-connection 'fake-conn
                  clutch--pending-edits '(((0 . 1) . "v")))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name=? WHERE id=1" . ("v")))))
                ((symbol-function 'clutch-db-escape-literal)
                 (lambda (_conn value) (format "'%s'" value)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (string-match-p "UPDATE t SET name='v' WHERE id=1;" captured))))))

(ert-deftest clutch-test-preview-execution-sql-uses-pending-batch-in-result-mode ()
  "Preview in result mode should mirror the staged commit batch."
  (with-temp-buffer
    (let (captured)
      (setq-local clutch--pending-inserts '(a)
                  clutch--pending-edits '(b)
                  clutch--pending-deletes '(c))
      (cl-letf (((symbol-function 'derived-mode-p) (lambda (&rest _modes) t))
                ((symbol-function 'clutch-result--build-pending-insert-statements)
                 (lambda () '(("INSERT INTO t VALUES (1)" . nil))))
                ((symbol-function 'clutch-result--build-update-statements)
                 (lambda () '(("UPDATE t SET name='' WHERE id=1" . nil))))
                ((symbol-function 'clutch-result--build-pending-delete-statements)
                 (lambda () '(("DELETE FROM t WHERE id=1" . nil))))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       (mapconcat #'identity
                                  '("INSERT INTO t VALUES (1);"
                                    "UPDATE t SET name='' WHERE id=1;"
                                    "DELETE FROM t WHERE id=1;")
                                  "\n")))))))

(ert-deftest clutch-test-result-effective-query-applies-where-filter ()
  "Result workflows should reuse the filtered SQL, not just display the filter."
  (with-temp-buffer
    (setq-local clutch--base-query "SELECT * FROM t"
                clutch--last-query "SELECT * FROM t WHERE id > 0"
                clutch--where-filter "id = 1")
    (cl-letf (((symbol-function 'clutch--apply-where)
               (lambda (sql filter)
                 (format "FILTER[%s]{%s}" filter sql))))
      (should (equal (clutch-result--effective-query)
                     "FILTER[id = 1]{SELECT * FROM t}")))))

(ert-deftest clutch-test-execute-page-uses-effective-filtered-query ()
  "Paging should continue using the active WHERE filter."
  (with-temp-buffer
    (let (captured-base)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1"
                  clutch-result-max-rows 500)
      (cl-letf (((symbol-function 'clutch--apply-where)
                 (lambda (sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch--build-paged-sql)
                 (lambda (sql _page-num _page-size &optional _order-by)
                   (setq captured-base sql)
                   "SELECT * FROM paged"))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :columns nil :rows nil)))
                ((symbol-function 'clutch--update-page-state) #'ignore)
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch--execute-page 0)
        (should (equal captured-base "FILTER[id = 1]{SELECT * FROM t}"))))))

(ert-deftest clutch-test-count-total-uses-effective-filtered-query ()
  "COUNT should run against the filtered SQL when a WHERE filter is active."
  (with-temp-buffer
    (let (captured-base)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1")
      (cl-letf (((symbol-function 'clutch--apply-where)
                 (lambda (sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch--build-count-sql)
                 (lambda (sql)
                   (setq captured-base sql)
                   "SELECT COUNT(*)"))
                ((symbol-function 'clutch-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :rows '((7)))))
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-count-total)
        (should (equal captured-base "FILTER[id = 1]{SELECT * FROM t}"))
        (should (= clutch--page-total-rows 7))))))

(ert-deftest clutch-test-execute-page-ensures-connection-before-query ()
  "Paging should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch--build-paged-sql)
                 (lambda (_sql _page-num _page-size &optional _order-by)
                   "SELECT * FROM paged"))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :columns nil :rows nil)))
                ((symbol-function 'clutch--update-page-state) #'ignore)
                ((symbol-function 'clutch--refresh-display) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch--execute-page 0)
        (should ensured)
        (should (eq captured-conn 'new-conn))))))

(ert-deftest clutch-test-execute-page-remembers-error-details-and-debug-event ()
  "Paging failures should populate `current-buffer' error details and trace."
  (with-temp-buffer
    (let ((clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)"))
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch-result-max-rows 100)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--build-paged-sql)
                 (lambda (_sql _page-num _page-size &optional _order-by)
                   "SELECT * FROM t LIMIT 100 OFFSET 0"))
                ((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (signal 'clutch-db-error (list raw-message)))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err))))))
          (should-error (clutch--execute-page 0) :type 'user-error)
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (event (car clutch--debug-events)))
            (should details)
            (should (eq (plist-get details :backend) 'pg))
            (should (equal (plist-get details :summary)
                           (clutch--humanize-db-error raw-message)))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "SELECT * FROM t"))
            (should event)
            (should (equal (plist-get event :phase) "error"))
            (should (equal (plist-get event :summary)
                           display-summary))))))))

(ert-deftest clutch-test-execute-dml-skips-debug-backend-lookup-when-disabled ()
  "DML execution should not consult debug-only backend state when debug is off."
  (with-temp-buffer
    (let ((clutch-debug-mode nil)
          rendered)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn)
                   (error "Debug-disabled path should not resolve backend key")))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn _sql)
                   (make-clutch-db-result :affected-rows 1)))
                ((symbol-function 'clutch--schema-affecting-query-p)
                 (lambda (_sql) nil))
                ((symbol-function 'clutch--display-result)
                 (lambda (result sql _elapsed)
                   (setq rendered (list result sql)))))
        (should (clutch--execute-dml "UPDATE demo SET enabled = 1" 'fake-conn))
        (should rendered)))))

(ert-deftest clutch-test-count-total-ensures-connection-before-query ()
  "COUNT should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured captured-conn)
      (setq-local clutch-connection 'stale-conn
                  clutch--base-query "SELECT * FROM t")
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch--build-count-sql)
                 (lambda (_sql) "SELECT COUNT(*)"))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :rows '((3)))))
                ((symbol-function 'clutch--refresh-footer-line) #'ignore)
                ((symbol-function 'message) #'ignore))
        (clutch-result-count-total)
        (should ensured)
        (should (eq captured-conn 'new-conn))
        (should (= clutch--page-total-rows 3))))))

(ert-deftest clutch-test-result-rerun-uses-effective-filtered-query ()
  "Rerun should preserve the current WHERE filter."
  (with-temp-buffer
    (let (executed)
      (setq-local clutch-connection 'fake-conn
                  clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1")
      (cl-letf (((symbol-function 'clutch--apply-where)
                 (lambda (sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--execute)
                 (lambda (sql &optional conn)
                   (setq executed (list sql conn)))))
        (clutch-result-rerun)
        (should (equal executed
                       '("FILTER[id = 1]{SELECT * FROM t}" nil)))))))

(ert-deftest clutch-test-preview-execution-sql-uses-effective-filtered-query ()
  "Preview should show the filtered SQL in result mode."
  (with-temp-buffer
    (clutch-result-mode)
    (let (captured)
      (setq-local clutch--base-query "SELECT * FROM t"
                  clutch--where-filter "id = 1")
      (cl-letf (((symbol-function 'clutch--apply-where)
                 (lambda (sql filter)
                   (format "FILTER[%s]{%s}" filter sql)))
                ((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql)
                   (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured "FILTER[id = 1]{SELECT * FROM t}"))))))

(ert-deftest clutch-test-preview-execution-sql-prefers-semicolon-statement-bounds-in-sql-buffer ()
  "Preview should mirror DWIM statement bounds for semicolon-delimited SQL buffers."
  (with-temp-buffer
    (insert "INSERT INTO demo(note) VALUES (E'first line\n\nthird line');\n\nSELECT 2")
    (goto-char (point-min))
    (search-forward "third")
    (let (captured)
      (cl-letf (((symbol-function 'clutch--preview-sql-buffer)
                 (lambda (sql) (setq captured sql))))
        (clutch-preview-execution-sql)
        (should (equal captured
                       "INSERT INTO demo(note) VALUES (E'first line\n\nthird line')"))))))

(ert-deftest clutch-test-execute-params-fallback-renders-sql-before-query ()
  "Fallback parameter execution should render SQL via escape helpers."
  (let (captured-sql)
    (cl-letf (((symbol-function 'clutch-db-escape-literal)
               (lambda (_conn value)
                 (format "'%s'" value)))
              ((symbol-function 'clutch-db-query)
               (lambda (_conn sql)
                 (setq captured-sql sql)
                 'ok)))
      (should (eq (clutch-db-execute-params
                   'fake-conn
                   "UPDATE demo SET name = ?, age = ? WHERE note IS ?"
                   '("alice" 7 nil))
                  'ok))
      (should (equal captured-sql
                     "UPDATE demo SET name = 'alice', age = 7 WHERE note IS NULL")))))

(ert-deftest clutch-test-execute-params-fallback-json-error-surfaces ()
  "Fallback parameter execution should signal when JSON parameter serialization fails."
  (let ((payload (make-hash-table :test 'equal))
        query-called)
    (puthash "key" "value" payload)
    (cl-letf (((symbol-function 'json-serialize)
               (lambda (_value)
                 (signal 'wrong-type-argument '("json serialization failed"))))
              ((symbol-function 'clutch-db-query)
               (lambda (&rest _args)
                 (setq query-called t)
                 'unexpected)))
      (let ((err (should-error
                  (clutch-db-execute-params
                   'fake-conn
                   "INSERT INTO demo(payload) VALUES (?)"
                   (list payload))
                  :type 'clutch-db-error)))
        (should-not query-called)
        (should (string-match-p
                 "Cannot serialize parameter value as JSON"
                 (cadr err)))))))

(ert-deftest clutch-test-execute-statements-remembers-error-details-before-last ()
  "Earlier failing statements should store details and humanized messages."
  (with-temp-buffer
    (let ((clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)"))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'mysql))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (if (equal sql "UPDATE first SET x = 1")
                       (signal 'clutch-db-error (list raw-message))
                     'ok))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err)))))
              signaled)
          (condition-case err
              (clutch--execute-statements
               '("UPDATE first SET x = 1"
                 "UPDATE second SET y = 2"))
            (user-error
             (setq signaled err)))
          (should signaled)
          (should (equal (cadr signaled)
                         (format "Statement 1 failed: %s"
                                 (clutch--debug-workflow-message display-summary))))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (event (car clutch--debug-events)))
            (should details)
            (should (eq (plist-get details :backend) 'mysql))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "UPDATE first SET x = 1"))
            (should event)
            (should (equal (plist-get event :phase) "error"))
            (should (equal (plist-get event :summary)
                           display-summary))))))))

(ert-deftest clutch-test-execute-statements-remembers-error-details-on-last-statement ()
  "Final DML failures should store details and humanized messages."
  (with-temp-buffer
    (let ((clutch-debug-mode t)
          (raw-message "Connection refused (host=db.example.com, port=3306)"))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'mysql))
                ((symbol-function 'clutch--run-db-query)
                 (lambda (_conn sql)
                   (if (equal sql "DELETE FROM broken_rows")
                       (signal 'clutch-db-error (list raw-message))
                     'ok))))
        (let ((display-summary
               (condition-case err
                   (signal 'clutch-db-error (list raw-message))
                 (clutch-db-error
                  (clutch--humanize-db-error (error-message-string err)))))
              signaled)
          (condition-case err
              (clutch--execute-statements
               '("UPDATE ok_rows SET enabled = 1"
                 "DELETE FROM broken_rows"))
            (user-error
             (setq signaled err)))
          (should signaled)
          (should (equal (cadr signaled)
                         (format "Statement 2 failed: %s"
                                 (clutch--debug-workflow-message display-summary))))
          (let* ((details clutch--buffer-error-details)
                 (diag (plist-get details :diag))
                 (event (car clutch--debug-events)))
            (should details)
            (should (eq (plist-get details :backend) 'mysql))
            (should (equal (plist-get diag :raw-message) raw-message))
            (should (equal (plist-get (plist-get diag :context) :sql)
                           "DELETE FROM broken_rows"))
            (should event)
            (should (equal (plist-get event :phase) "error"))
            (should (equal (plist-get event :summary)
                           display-summary))))))))

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
               "ORA-00904"
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

(ert-deftest clutch-test-executed-sql-overlay-marks-statement-start-line ()
  "Executed SQL should use a start-line marker instead of a body highlight."
  (with-temp-buffer
    (insert "  SELECT 1;\n  SELECT 2;")
    (clutch--mark-executed-sql-region (point-min) (point-max))
    (should (overlayp clutch--executed-sql-overlay))
    (should (= (overlay-start clutch--executed-sql-overlay) (point-min)))
    (should (overlay-get clutch--executed-sql-overlay 'before-string))
    (should-not (overlay-get clutch--executed-sql-overlay 'modification-hooks))))

(ert-deftest clutch-test-execute-quit-disconnects-and-clears-connection ()
  "Quit should abandon the connection when no backend interrupt is available."
  (with-temp-buffer
    (let ((buf (current-buffer))
          (disconnected nil)
          (clutch--tx-dirty-cache (make-hash-table :test 'eq))
          (clutch-connection 'fake-conn)
          (clutch--executing-p nil))
      (puthash clutch-connection t clutch--tx-dirty-cache)
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
        (with-current-buffer buf
          (should-not clutch-connection))
        (should-not (gethash 'fake-conn clutch--tx-dirty-cache))
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-execute-starts-spinner-when-query-begins ()
  "Executing a query should start the global spinner."
  (with-temp-buffer
    (let ((clutch-connection 'fake-conn)
          (clutch--executing-p nil)
          spinner-started
          execute-saw-spinner)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--clear-error-position-overlay) #'ignore)
                ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--require-risky-dml-confirmation) #'ignore)
                ((symbol-function 'clutch--spinner-start)
                 (lambda () (setq spinner-started t)))
                ((symbol-function 'clutch--update-mode-line) #'ignore)
                ((symbol-function 'redisplay) #'ignore)
                ((symbol-function 'clutch--select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select)
                 (lambda (&rest _args)
                   (setq execute-saw-spinner spinner-started))))
        (clutch--execute "SELECT 1" clutch-connection)
        (should spinner-started)
        (should execute-saw-spinner)
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-execute-in-result-buffer-keeps-table-header-line ()
  "Executing from a result buffer should not replace the table header line."
  (with-temp-buffer
    (clutch-result-mode)
    (let ((clutch-connection 'fake-conn)
          (clutch--executing-p nil)
          (header-line-format " result header")
          seen-header)
      (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
                ((symbol-function 'clutch--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--clear-error-position-overlay) #'ignore)
                ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--require-risky-dml-confirmation) #'ignore)
                ((symbol-function 'clutch--spinner-start) #'ignore)
                ((symbol-function 'redisplay) #'ignore)
                ((symbol-function 'clutch--select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select)
                 (lambda (&rest _args)
                   (setq seen-header header-line-format))))
        (clutch--execute "SELECT 1" clutch-connection)
        (should (equal seen-header " result header"))
        (should (equal header-line-format " result header"))))))

(ert-deftest clutch-test-execute-quit-prefers-backend-interrupt-over-disconnect ()
  "Quit should keep the session when a backend interrupt succeeds."
  (with-temp-buffer
    (let* ((buf (current-buffer))
           (conn 'fake-conn)
           (interrupted nil)
           (disconnected nil)
           (clutch--tx-dirty-cache (make-hash-table :test 'eq))
           (clutch-connection conn)
           (clutch--executing-p nil))
      (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
                ((symbol-function 'clutch--check-pending-changes) #'ignore)
                ((symbol-function 'clutch--clear-error-position-overlay) #'ignore)
                ((symbol-function 'clutch--destructive-query-p) (lambda (_sql) nil))
                ((symbol-function 'clutch--update-mode-line) (lambda () nil))
                ((symbol-function 'clutch--select-query-p) (lambda (_sql) t))
                ((symbol-function 'clutch--execute-select) (lambda (&rest _args) (signal 'quit nil)))
                ((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (setq interrupted t)
                   t))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn) (setq disconnected t))))
        (should-error (clutch--execute "SELECT pg_sleep(10)" clutch-connection)
                      :type 'user-error)
        (should interrupted)
        (should-not disconnected)
        (with-current-buffer buf
          (should (eq clutch-connection conn)))
        (should-not clutch--executing-p)))))

(ert-deftest clutch-test-handle-query-quit-remembers-interrupt-error-details-and-debug-event ()
  "Interrupt RPC failures should store details and a cancel debug event."
  (with-temp-buffer
    (let* ((clutch-debug-mode t)
           (clutch-connection 'fake-conn)
           (raw-message "Connection refused (host=db.example.com, port=3306)")
           (captured-message nil)
           (disconnected nil))
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'pg))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch-db-interrupt-query)
                 (lambda (_conn)
                   (signal 'clutch-db-error (list raw-message))))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (_conn)
                   (setq disconnected t)))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq captured-message (apply #'format fmt args)))))
        (should-error (clutch--handle-query-quit clutch-connection)
                      :type 'clutch-query-interrupted)
        (let* ((summary (clutch--humanize-db-error raw-message))
               (message-summary
                (condition-case err
                    (signal 'clutch-db-error (list raw-message))
                  (clutch-db-error
                   (clutch--humanize-db-error (error-message-string err)))))
               (details clutch--buffer-error-details)
               (diag (plist-get details :diag))
               (context (plist-get diag :context))
               (cancel-event
                (cl-find-if
                 (lambda (event)
                   (and (equal (plist-get event :op) "cancel")
                        (equal (plist-get event :phase) "error")))
                 clutch--debug-events))
               (interrupt-event
                (cl-find-if
                 (lambda (event)
                   (and (equal (plist-get event :op) "interrupt")
                        (equal (plist-get event :phase) "disconnect")))
                 clutch--debug-events)))
          (should disconnected)
          (should details)
          (should (eq (plist-get details :backend) 'pg))
          (should (equal (plist-get details :summary) summary))
          (should (equal (plist-get diag :raw-message) raw-message))
          (should (plist-member context :sql))
          (should-not (plist-get context :sql))
          (should (equal captured-message
                         (format "Interrupt failed: %s"
                                 (clutch--debug-workflow-message message-summary))))
          (should cancel-event)
          (should (equal (plist-get cancel-event :summary) message-summary))
          (should interrupt-event))))))

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

;;;; Console — yank cleanup and save

(ert-deftest clutch-test-console-yank-cleanup-strips-trailing-whitespace ()
  "Yank into a query console should clean trailing whitespace in pasted region."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (insert "existing SQL;\n")
      (let ((start (point)))
        (insert "SELECT *   \nFROM t   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-substring start (point-max))
                       "SELECT *\nFROM t\n"))))))

(ert-deftest clutch-test-console-yank-cleanup-does-not-touch-existing-text ()
  "Yank cleanup should not modify text outside the pasted region."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (insert "SELECT 1;   \n")
      (let ((pre-text (buffer-string))
            (start (point)))
        (insert "SELECT 2;\n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-substring 1 (1+ (length pre-text)))
                       pre-text))))))

(ert-deftest clutch-test-console-yank-cleanup-respects-defcustom ()
  "Yank cleanup should be a no-op when `clutch-console-yank-cleanup' is nil."
  (let ((clutch-console-yank-cleanup nil))
    (with-temp-buffer
      (clutch-mode)
      (setq-local clutch--console-name "test")
      (add-hook 'post-command-hook #'clutch--console-yank-cleanup nil t)
      (let ((start (point)))
        (insert "SELECT *   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-string) "SELECT *   \n"))))))

(ert-deftest clutch-test-console-yank-cleanup-skips-non-console-buffers ()
  "Yank cleanup should not run in non-console clutch buffers."
  (let ((clutch-console-yank-cleanup t))
    (with-temp-buffer
      (clutch-mode)
      ;; clutch--console-name is nil — not a console
      (let ((start (point)))
        (insert "SELECT *   \n")
        (set-mark start)
        (let ((this-command 'yank))
          (clutch--console-yank-cleanup))
        (should (equal (buffer-string) "SELECT *   \n"))))))

(ert-deftest clutch-test-save-console-reports-write-error ()
  "Console persistence failures should surface a minibuffer warning."
  (let (reported)
    (with-temp-buffer
      (setq-local clutch--console-name "demo")
      (cl-letf (((symbol-function 'make-directory) #'ignore)
                ((symbol-function 'write-region)
                 (lambda (&rest _args)
                   (error "Disk full")))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq reported (apply #'format fmt args)))))
        (clutch--save-console)
        (should (string-match-p
                 "Failed to save console demo: disk full"
                 reported))))))

(ert-deftest clutch-test-save-console-writes-buffer-to-file ()
  "Saving a named console should write its current contents to disk."
  (let ((dir (make-temp-file "clutch-console-" t)))
    (unwind-protect
        (with-temp-buffer
          (let ((clutch-console-directory dir))
            (setq-local clutch--console-name "test-db")
            (insert "SELECT 1;\nSELECT 2;")
            (clutch--save-console)
            (let ((path (expand-file-name "test-db.sql" dir)))
              (should (file-exists-p path))
              (with-temp-buffer
                (insert-file-contents path)
                (should (equal (buffer-string) "SELECT 1;\nSELECT 2;"))))))
      (delete-directory dir t))))

(ert-deftest clutch-test-save-console-skips-unnamed-buffer ()
  "Saving should be a no-op when the console buffer has no name."
  (let* ((parent (make-temp-file "clutch-console-" t))
         (dir (expand-file-name "missing" parent)))
    (unwind-protect
        (with-temp-buffer
          (let ((clutch-console-directory dir))
            (insert "SELECT 1;")
            (clutch--save-console)
            (should-not (file-exists-p dir))))
      (delete-directory parent t))))

(ert-deftest clutch-test-save-console-creates-directory-if-missing ()
  "Saving a named console should create its directory when needed."
  (let* ((parent (make-temp-file "clutch-console-" t))
         (dir (expand-file-name "nested" parent)))
    (unwind-protect
        (with-temp-buffer
          (let ((clutch-console-directory dir))
            (setq-local clutch--console-name "test")
            (insert "SELECT 42;")
            (clutch--save-console)
            (should (file-directory-p dir))
            (should (file-exists-p (expand-file-name "test.sql" dir)))))
      (delete-directory parent t))))

;;;; REPL

(ert-deftest clutch-test-repl-input-sender-accumulates-until-semicolon ()
  "REPL input sender should accumulate partial SQL and show continuation prompt."
  (with-temp-buffer
    (let ((clutch-repl--pending-input "")
          output)
      (cl-letf (((symbol-function 'clutch-repl--output)
                 (lambda (text) (push text output)))
                ((symbol-function 'clutch-repl--execute-and-print)
                 (lambda (_sql) (error "Should not execute"))))
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
                 (lambda (_text) (error "Should not output continuation"))))
        (clutch-repl--input-sender nil " 1;")
        (should (equal sent "SELECT\n 1;"))
        (should (equal clutch-repl--pending-input ""))))))

(ert-deftest clutch-test-repl-execute-and-print-not-connected ()
  "REPL should print not-connected message when no live connection."
  (with-temp-buffer
    (let (captured)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (user-error "Not connected.  Use C-c C-e to connect")))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq captured text))))
        (clutch-repl--execute-and-print "SELECT 1")
        (should (string-match-p "Not connected" captured))
        (should (string-match-p "db> $" captured))))))

(ert-deftest clutch-test-repl-execute-and-print-ensures-connection ()
  "REPL should use the shared reconnect path before querying."
  (with-temp-buffer
    (let (ensured
          captured-conn
          output)
      (setq-local clutch-connection 'stale-conn)
      (cl-letf (((symbol-function 'clutch--ensure-connection)
                 (lambda ()
                   (setq ensured t)
                   (setq-local clutch-connection 'new-conn)))
                ((symbol-function 'clutch-db-query)
                 (lambda (conn _sql)
                   (setq captured-conn conn)
                   (make-clutch-db-result :rows '((1)))))
                ((symbol-function 'clutch-repl--output)
                 (lambda (text) (setq output text))))
        (clutch-repl--execute-and-print "UPDATE t SET x = 1;")
        (should ensured)
        (should (eq captured-conn 'new-conn))
        (should (string-match-p "Affected rows" output))))))

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

;;;; Object — browse and actions

(ert-deftest clutch-test-object-browse-errors-without-console ()
  "Object browse should error when no matching query console is open."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--find-console-for-conn)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) name)))
      (should-error (clutch-object-browse '(:name "users" :type "TABLE"))
                    :type 'user-error))))

(ert-deftest clutch-test-object-browse-errors-on-non-table-like-type ()
  "Browse should reject object types that do not expose rows."
  (should-error
   (clutch-object-browse '(:name "order_idx" :type "INDEX"))
   :type 'user-error
   :exclude-subtypes nil))

(ert-deftest clutch-test-object-browse-inserts-sql-into-console ()
  "Object browse should insert escaped SELECT in the target console buffer."
  (let ((console (generate-new-buffer " *clutch-test-console*")))
    (unwind-protect
        (with-current-buffer console
          (insert "SELECT 1;")
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'derived-mode-p)
                     (lambda (&rest modes) (memq 'clutch-mode modes)))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn name) (format "\"%s\"" name)))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (set-buffer buf)
                       buf)))
            (clutch-object-browse '(:name "order-items" :type "TABLE")))
          (should (string-suffix-p
                   "\n\nSELECT * FROM \"order-items\";"
                   (buffer-string))))
      (kill-buffer console))))

(ert-deftest clutch-test-object-browse-preserves-schema-qualification ()
  "Object browse should keep schema-qualified object names."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'derived-mode-p)
               (lambda (&rest modes) (memq 'clutch-mode modes)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "\"%s\"" name)))
              ((symbol-function 'pop-to-buffer)
               (lambda (buf &rest _args)
                 (set-buffer buf)
                 buf)))
      (clutch-object-browse
       '(:name "background_schedule_pool_log"
         :type "TABLE"
         :source-schema "system")))
    (should (equal (buffer-string)
                   "SELECT * FROM \"system\".\"background_schedule_pool_log\";"))))

(ert-deftest clutch-test-object-browse-uses-object-schema-before-source-schema ()
  "Object browse should prefer the object's real schema over discovery schema."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'derived-mode-p)
               (lambda (&rest modes) (memq 'clutch-mode modes)))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn name) (format "\"%s\"" name)))
              ((symbol-function 'pop-to-buffer)
               (lambda (buf &rest _args)
                 (set-buffer buf)
                 buf)))
      (clutch-object-browse
       '(:name "ORDERS"
         :type "SYNONYM"
         :schema "DATA_OWNER"
         :source-schema "APP")))
    (should (equal (buffer-string)
                   "SELECT * FROM \"DATA_OWNER\".\"ORDERS\";"))))

(ert-deftest clutch-test-object-browse-clickhouse-uses-unquoted-identifiers ()
  "ClickHouse browse SQL should avoid double quotes for simple identifiers."
  (require 'clutch-db-jdbc)
  (let ((conn (make-clutch-jdbc-conn :params '(:driver clickhouse))))
    (with-temp-buffer
      (setq-local clutch-connection conn)
      (cl-letf (((symbol-function 'derived-mode-p)
                 (lambda (&rest modes) (memq 'clutch-mode modes)))
                ((symbol-function 'pop-to-buffer)
                 (lambda (buf &rest _args)
                   (set-buffer buf)
                   buf)))
        (clutch-object-browse
         '(:name "background_schedule_pool_log"
           :type "TABLE"
           :source-schema "system")))
      (should (equal (buffer-string)
                     "SELECT * FROM system.background_schedule_pool_log;")))))

(ert-deftest clutch-test-object-browse-inserts-at-first-line-in-empty-console ()
  "Object browse should insert at line 1 when the console has no content."
  (let ((console (generate-new-buffer " *clutch-test-console-empty*")))
    (unwind-protect
        (with-current-buffer console
          (insert "\n\n")
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'derived-mode-p)
                     (lambda (&rest modes) (memq 'clutch-mode modes)))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn name) (format "\"%s\"" name)))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (set-buffer buf)
                       buf)))
            (clutch-object-browse '(:name "order-items" :type "TABLE")))
          (should (equal (buffer-string)
                         "SELECT * FROM \"order-items\";")))
      (kill-buffer console))))

(ert-deftest clutch-test-object-browse-inserts-around-current-point-in-console ()
  "Object browse should insert around point with one blank line on each side."
  (let ((console (generate-new-buffer " *clutch-test-console-middle*")))
    (unwind-protect
        (with-current-buffer console
          (insert "SELECT 1;\nSELECT 2;")
          (goto-char (point-min))
          (forward-line 1)
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'derived-mode-p)
                     (lambda (&rest modes) (memq 'clutch-mode modes)))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn name) (format "\"%s\"" name)))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (set-buffer buf)
                       buf)))
            (clutch-object-browse '(:name "order-items" :type "TABLE")))
          (should (equal (buffer-string)
                         (concat "SELECT 1;\n\n"
                                 "SELECT * FROM \"order-items\";\n\n"
                                 "SELECT 2;"))))
      (kill-buffer console))))

(ert-deftest clutch-test-object-browse-does-not-add-extra-blank-lines ()
  "Object browse should reuse an existing blank-line separator."
  (let ((console (generate-new-buffer " *clutch-test-console-spacing*")))
    (unwind-protect
        (with-current-buffer console
          (insert "SELECT 1;\n\n")
          (setq-local clutch-connection 'fake-conn)
          (cl-letf (((symbol-function 'derived-mode-p)
                     (lambda (&rest modes) (memq 'clutch-mode modes)))
                    ((symbol-function 'clutch-db-escape-identifier)
                     (lambda (_conn name) (format "\"%s\"" name)))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args)
                       (set-buffer buf)
                       buf)))
            (clutch-object-browse '(:name "order-items" :type "TABLE")))
          (should (equal (buffer-string)
                         "SELECT 1;\n\nSELECT * FROM \"order-items\";")))
      (kill-buffer console))))

(ert-deftest clutch-test-object-default-action-routes-table-like-types-to-browse ()
  "Default action should browse rows for table-like objects."
  (let (browse-entry show-entry)
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry) (setq browse-entry entry)))
              ((symbol-function 'clutch-object-show-ddl-or-source)
               (lambda (entry) (setq show-entry entry))))
      (clutch-object-default-action '(:name "orders" :type "TABLE"))
      (should (equal browse-entry '(:name "orders" :type "TABLE")))
      (should-not show-entry))))

(ert-deftest clutch-test-object-default-action-routes-non-table-types-to-definition ()
  "Default action should show DDL/source for non-table objects."
  (let (browse-entry show-entry)
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry) (setq browse-entry entry)))
              ((symbol-function 'clutch-object-show-ddl-or-source)
               (lambda (entry) (setq show-entry entry))))
      (clutch-object-default-action '(:name "process_order" :type "PROCEDURE"))
      (should-not browse-entry)
      (should (equal show-entry '(:name "process_order" :type "PROCEDURE"))))))

(ert-deftest clutch-test-object-jump-target-resolves-index-table ()
  "Jump target should follow index target-table metadata."
  (let (described)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--object-matches-by-name)
                 (lambda (_conn name &optional table-like-only)
                   (should table-like-only)
                   (should (equal name "ORDERS"))
                   '((:name "ORDERS" :type "TABLE" :schema "APP" :source-schema "APP"))))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry) (setq described entry))))
        (clutch-object-jump-target
         '(:name "ORDER_IDX" :type "INDEX"
           :schema "APP" :source-schema "APP"
           :target-table "ORDERS"))
        (should (equal described
                       '(:name "ORDERS" :type "TABLE"
                         :schema "APP" :source-schema "APP")))))))

(ert-deftest clutch-test-object-jump-target-prefers-synonym-target-schema ()
  "Jump target should prefer synonym matches in the declared target schema."
  (let (described)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--object-matches-by-name)
                 (lambda (_conn name &optional _table-like-only)
                   (should (equal name "ORDERS"))
                   '((:name "ORDERS" :type "TABLE"
                      :schema "ARCHIVE" :source-schema "ARCHIVE")
                     (:name "ORDERS" :type "TABLE"
                      :schema "APP" :source-schema "APP"))))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry) (setq described entry))))
        (clutch-object-jump-target
         '(:name "ORDERS_SYM" :type "SYNONYM"
           :target-name "ORDERS" :target-schema "APP"))
        (should (equal described
                       '(:name "ORDERS" :type "TABLE"
                         :schema "APP" :source-schema "APP")))))))

(ert-deftest clutch-test-object-jump-target-errors-on-unsupported-type ()
  "Forward jump should reject object types without target semantics."
  (should-error
   (clutch-object-jump-target '(:name "ORDER_SEQ" :type "SEQUENCE"))
   :type 'user-error
   :exclude-subtypes nil))

(ert-deftest clutch-test-act-dwim-opens-transient-with-current-entry ()
  "Act-dwim should resolve the current object and open the shared action UI."
  (let (setup-command)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (&rest _)
                   '(:name "PROCESS_ORDER" :type "PROCEDURE")))
                ((symbol-function 'transient-setup)
                 (lambda (command &rest _args)
                   (setq setup-command command))))
        (let ((clutch--object-action-entry nil))
          (clutch-act-dwim)
          (should (eq setup-command 'clutch-object-actions-menu))
          (should (equal clutch--object-action-entry
                         '(:name "PROCESS_ORDER" :type "PROCEDURE"))))))))

(ert-deftest clutch-test-act-dwim-errors-without-action-ui ()
  "Act-dwim should not silently run the default action when no UI exists."
  (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
             (lambda (&rest _)
               '(:name "ORDERS" :type "TABLE")))
            ((symbol-function 'clutch--present-object-actions-natively)
             (lambda (_entry) nil)))
    (should-error (clutch-act-dwim) :type 'user-error)))

(ert-deftest clutch-test-act-dwim-with-explicit-entry-opens-transient ()
  "Act-dwim should still work when called programmatically with ENTRY."
  (let (setup-command)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'transient-setup)
                 (lambda (command &rest _args)
                   (setq setup-command command))))
        (let ((clutch--object-action-entry nil))
          (clutch-act-dwim '(:name "ORDERS" :type "TABLE"))
          (should (eq setup-command 'clutch-object-actions-menu))
          (should (equal clutch--object-action-entry
                         '(:name "ORDERS" :type "TABLE"))))))))

(ert-deftest clutch-test-object-action-inapt-flags-reflect-target-type ()
  "Object action transient flags should reflect the current target type."
  (let ((clutch--object-action-entry '(:name "ORDERS" :type "TABLE")))
    (should (clutch--object-act-jump-target-inapt-p)))
  (let ((clutch--object-action-entry '(:name "ORDER_IDX" :type "INDEX")))
    (should-not (clutch--object-act-jump-target-inapt-p))))

(ert-deftest clutch-test-copy-object-fqname-prompts-for-fqname ()
  "Copy-fqname should use an fqname-specific prompt."
  (let (prompt)
    (cl-letf (((symbol-function 'clutch--resolve-object-entry)
               (lambda (arg &rest _)
                 (setq prompt arg)
                 '(:name "ORDERS" :type "TABLE" :schema "APP")))
              ((symbol-function 'kill-new) #'ignore)
              ((symbol-function 'message) #'ignore))
      (clutch-copy-object-fqname)
      (should (equal prompt "Copy object fqname: ")))))

(ert-deftest clutch-test-browse-table-inserts-escaped-select ()
  "Browse-table compatibility wrapper should still escape identifiers."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (insert "-- tail")
    (cl-letf (((symbol-function 'clutch-object-browse)
               (lambda (entry)
                 (let ((sql (format "SELECT * FROM %s;"
                                    (clutch-db-escape-identifier
                                     clutch-connection
                                     (plist-get entry :name)))))
                   (goto-char (point-max))
                   (unless (bolp) (insert "\n"))
                   (insert "\n" sql))))
              ((symbol-function 'clutch-db-escape-identifier)
               (lambda (_conn tbl) (format "\"%s\"" tbl))))
      (clutch-browse-table "order-items"))
    (should (string-suffix-p
             "\n\nSELECT * FROM \"order-items\";"
             (buffer-string)))))

(ert-deftest clutch-test-browse-table-interactive-preserves-entry-schema ()
  "Interactive browse-table should preserve schema-qualified entries."
  (let (captured-entry)
    (cl-letf (((symbol-function 'clutch-object-read)
               (lambda (&rest _args)
                 '(:name "background_schedule_pool_log"
                   :type "TABLE"
                   :source-schema "system")))
              ((symbol-function 'clutch-object-browse)
               (lambda (entry)
                 (setq captured-entry entry))))
      (call-interactively #'clutch-browse-table))
    (should (equal captured-entry
                   '(:name "background_schedule_pool_log"
                     :type "TABLE"
                     :source-schema "system")))))

;;;; Object — describe and DDL

(ert-deftest clutch-test-describe-dwim-prompts-when-point-has-no-object ()
  "Describe-dwim should resolve an object and then open its describe view."
  (let (resolved-prompt captured)
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'thing-at-point) (lambda (&rest _) nil))
                ((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (prompt &rest _args)
                   (setq resolved-prompt prompt)
                   '(:name "IDX_A" :type "INDEX")))
                ((symbol-function 'clutch-object-describe)
                 (lambda (entry)
                   (setq captured entry))))
        (clutch-describe-dwim)
        (should (equal resolved-prompt "Describe object: "))
        (should (equal captured '(:name "IDX_A" :type "INDEX")))))))

(ert-deftest clutch-test-describe-buffer-exposes-shared-dwim-bindings ()
  "Describe buffers should reuse the shared action commands."
  (let ((entry '(:name "PROCESS_ORDER" :type "PROCEDURE")))
    (with-temp-buffer
      (clutch--render-object-describe 'fake-conn entry nil nil)
      (should (eq major-mode 'clutch-describe-mode))
      (should (equal clutch-browser-current-object entry))
      (should (equal clutch--describe-object-entry entry))
      (should (eq (lookup-key clutch-describe-mode-map (kbd "C-c C-d"))
                  #'clutch-describe-dwim))
      (should (eq (lookup-key clutch-describe-mode-map (kbd "C-c C-o"))
                  #'clutch-act-dwim)))))

(ert-deftest clutch-test-render-object-describe-keeps-fqname-in-body-not-header ()
  "Describe buffers should keep the fqname in the body title, not duplicate it in the header-line."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch--bind-connection-context) #'ignore)
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[desc]"))
              ((symbol-function 'clutch--object-describe-text)
               (lambda (_conn _entry)
                 "PUBLIC.orders (TABLE)\n\nSummary\n  Name  orders")))
      (clutch--render-object-describe
       'fake-conn
       '(:name "orders" :type "TABLE" :source-schema "PUBLIC"))
      (should (string-match-p "PUBLIC.orders (TABLE)" (buffer-string)))
      (should (string-match-p "\\[desc\\]" clutch-describe--header-base))
      (should (string-match-p "show definition" clutch-describe--header-base))
      (should-not (string-match-p "PUBLIC.orders" clutch-describe--header-base)))))

(ert-deftest clutch-test-render-object-describe-fontifies-short-standalone-index-name ()
  "Short standalone index names in describe buffers should be highlighted."
  (with-temp-buffer
    (cl-letf (((symbol-function 'clutch--bind-connection-context) #'ignore)
              ((symbol-function 'clutch--icon-with-face) (lambda (&rest _) "[desc]"))
              ((symbol-function 'clutch--ensure-table-comment) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-column-details) (lambda (&rest _) nil))
              ((symbol-function 'clutch-db-list-columns) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (_conn _entry type)
                 (pcase type
                   ("INDEX"
                    (list (list :name "IDX_FFP_RECEIPT_CODE" :unique nil)
                          (list :name "idx_rcc" :unique nil)))
                   (_ nil)))))
      (clutch--render-object-describe
       'fake-conn
       '(:name "ffp_order_consign" :type "TABLE" :source-schema "zj_test"))
      (goto-char (point-min))
      (re-search-forward "^  \\(idx_rcc\\)")
      (should (eq (get-text-property (match-beginning 1) 'face)
                  'font-lock-variable-name-face)))))

(ert-deftest clutch-test-object-entry-label-uses-lowercase-type ()
  "Object-entry labels should keep schema uppercase and type lowercase."
  (should (equal (clutch--object-entry-label
                  '(:name "USER_TABLES" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC"))
                 "PUBLIC/synonym"))
  (should (equal (clutch--object-entry-label
                  '(:name "MONTHLY_REPORT" :type "VIEW"
                    :schema "DATA_OWNER" :source-schema "DATA_OWNER"))
                 "DATA_OWNER/view")))

(ert-deftest clutch-test-object-sql-name-rejects-current-schema-placeholder ()
  "Browse SQL must never contain literal \"current_schema\" as schema qualifier.
Reproduces the bug where jumping to a PG table generated
SELECT * FROM \"current_schema\".\"orders_large\" which PostgreSQL rejects.
The fix is in the PG backend: entries carry the real schema (e.g. \"public\"),
so `clutch--object-sql-name' produces \"public\".\"orders_large\"."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    ;; An entry with a real schema produces correct qualified SQL.
    (let* ((entry '(:name "orders_large" :type "TABLE"
                    :schema "public" :source-schema "public"))
           (sql-name (clutch--object-sql-name 'fake entry)))
      (should (equal sql-name "\"public\".\"orders_large\""))
      (should-not (string-match-p "current_schema" sql-name)))))

(ert-deftest clutch-test-object-sql-name-never-produces-current-schema-literal ()
  "Ensure `clutch--object-sql-name' never emits \"current_schema\" as a schema qualifier."
  (cl-letf (((symbol-function 'clutch-db-escape-identifier)
             (lambda (_conn id) (format "\"%s\"" id))))
    ;; With a real schema name, should produce "public"."orders"
    (should (equal (clutch--object-sql-name 'fake '(:name "orders" :type "TABLE"
                                                    :schema "public"))
                   "\"public\".\"orders\""))
    ;; Without schema, should produce bare "orders"
    (should (equal (clutch--object-sql-name 'fake '(:name "orders" :type "TABLE"))
                   "\"orders\""))))

(ert-deftest clutch-test-object-show-ddl-or-source-derives-oracle-sql-product-from-backend ()
  "Object definition buffers should use Oracle font-lock when backend is oracle."
  (let (captured-product)
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch-db-show-create-table)
               (lambda (_conn _table) "CREATE TABLE demo_tasks (id NUMBER)"))
              ((symbol-function 'sql-mode) #'ignore)
              ((symbol-function 'sql-set-product)
               (lambda (product) (setq captured-product product)))
              ((symbol-function 'font-lock-ensure) #'ignore)
              ((symbol-function 'pop-to-buffer) (lambda (&rest _args) nil)))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (setq-local clutch--connection-params '(:backend oracle))
        (setq-local clutch--conn-sql-product nil)
        (clutch-object-show-ddl-or-source '(:name "DEMO_TASKS" :type "TABLE"))))
    (should (eq captured-product 'oracle))))

(ert-deftest clutch-test-object-show-ddl-or-source-populates-problem-record-and-debug-trace ()
  "Definition/source failures should feed the shared problem/debug workflow."
  (let ((details '(:backend jdbc
                   :summary "ORA-04043: object does not exist"
                   :diag (:category "metadata"
                          :op "get-object-ddl"
                          :conn-id 7
                          :raw-message "ORA-04043: object does not exist"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch-db-show-create-table)
                   (lambda (&rest _args)
                     (signal 'clutch-db-error
                             (list "ORA-04043: object does not exist" details))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (should-error
           (clutch-object-show-ddl-or-source '(:name "DEMO_TASKS" :type "TABLE"))
           :type 'clutch-db-error)
          (should clutch--buffer-error-details)
          (should (string-match-p "ORA-04043"
                                  (plist-get clutch--buffer-error-details :summary)))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: show-definition" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "DEMO_TASKS" text))))))))

(ert-deftest clutch-test-object-describe-propagates-detail-errors ()
  "Describe rendering should not silently swallow object detail failures."
  (cl-letf (((symbol-function 'clutch-db-object-details)
             (lambda (_conn _entry)
               (signal 'clutch-db-error '("detail boom")))))
    (should-error
     (clutch--object-describe-text 'fake-conn '(:name "IDX_USERS" :type "INDEX"))
     :type 'clutch-db-error)))

(ert-deftest clutch-test-object-describe-table-propagates-column-detail-errors ()
  "Table describe should not hide column-detail failures behind list-columns."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (detail-calls 0)
        (list-columns-calls 0))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "dev-key"))
              ((symbol-function 'clutch-db-column-details)
               (lambda (_conn _table)
                 (cl-incf detail-calls)
                 (signal 'clutch-db-error '("column detail boom"))))
              ((symbol-function 'clutch-db-list-columns)
               (lambda (&rest _args)
                 (cl-incf list-columns-calls)
                 '("id" "name")))
              ((symbol-function 'clutch--ensure-table-comment)
               (lambda (&rest _args) nil))
              ((symbol-function 'clutch--object-related-entries)
               (lambda (&rest _args) nil)))
      (should-error
       (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
       :type 'clutch-db-error)
      (should-error
       (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
       :type 'clutch-db-error)
      (should (= detail-calls 1))
      (should (= list-columns-calls 0)))))

(ert-deftest clutch-test-object-describe-populates-problem-record-in-source-buffer ()
  "Describe failures should populate a problem record in the invoking buffer."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (details '(:backend jdbc
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "dev-key"))
                ((symbol-function 'clutch-db-error-details)
                 (lambda (_conn) details))
                ((symbol-function 'clutch-db-column-details)
                 (lambda (_conn _table)
                   (signal 'clutch-db-error
                           (list "ORA-12592: TNS:bad packet" details))))
                ((symbol-function 'clutch--ensure-table-comment)
                 (lambda (&rest _args) nil))
                ((symbol-function 'clutch--object-related-entries)
                 (lambda (&rest _args) nil))
                ((symbol-function 'pop-to-buffer)
                 (lambda (buf &rest _args) buf)))
        (should-error
         (clutch-object-describe '(:name "USERS" :type "TABLE"))
         :type 'clutch-db-error)
        (let* ((problem clutch--buffer-error-details)
               (diag (plist-get problem :diag)))
          (should problem)
          (should (string-match-p "ORA-12592" (plist-get problem :summary)))
          (should (equal (plist-get diag :op) "get-columns")))))))

(ert-deftest clutch-test-object-describe-populates-debug-trace-when-enabled ()
  "Describe failures should contribute debug trace entries when debug mode is on."
  (let ((clutch--column-details-cache (make-hash-table :test 'equal))
        (clutch--column-details-status-cache (make-hash-table :test 'equal))
        (details '(:backend jdbc
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "get-columns"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "dev-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch-db-error-details)
                   (lambda (_conn) details))
                  ((symbol-function 'clutch-db-column-details)
                   (lambda (_conn _table)
                     (signal 'clutch-db-error
                             (list "ORA-12592: TNS:bad packet" details))))
                  ((symbol-function 'clutch--ensure-table-comment)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'clutch--object-related-entries)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (should-error
           (clutch-object-describe '(:name "USERS" :type "TABLE"))
           :type 'clutch-db-error)
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: describe" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "USERS" text))))))))

(ert-deftest clutch-test-object-describe-success-clears-stale-problem-records ()
  "Successful describe should clear older failure state for the same connection."
  (let ((source (generate-new-buffer " *clutch-describe-source*")))
    (unwind-protect
        (with-current-buffer source
          (setq-local clutch-connection 'fake-conn
                      clutch--buffer-error-details '(:summary "old"))
          (puthash 'fake-conn '(:summary "old") clutch--problem-records-by-conn)
          (cl-letf (((symbol-function 'clutch--remember-current-object) #'ignore)
                    ((symbol-function 'clutch--object-fqname)
                     (lambda (_entry) "USERS"))
                    ((symbol-function 'clutch--object-describe-text)
                     (lambda (_conn _entry) "ok"))
                    ((symbol-function 'pop-to-buffer)
                     (lambda (buf &rest _args) buf)))
            (clutch-object-describe '(:name "USERS" :type "TABLE"))
            (should-not clutch--buffer-error-details)
            (should-not (gethash 'fake-conn clutch--problem-records-by-conn))))
      (kill-buffer source))))

(ert-deftest clutch-test-describe-refresh-success-clears-stale-problem-records ()
  "Refreshing a describe buffer should clear stale failure state on success."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch--describe-object-entry '(:name "USERS" :type "TABLE")
                clutch--buffer-error-details '(:summary "old"))
    (puthash 'fake-conn '(:summary "old") clutch--problem-records-by-conn)
    (cl-letf (((symbol-function 'clutch--refresh-current-schema) #'ignore)
              ((symbol-function 'clutch--render-object-describe) #'ignore))
      (clutch-describe-refresh)
      (should-not clutch--buffer-error-details)
      (should-not (gethash 'fake-conn clutch--problem-records-by-conn)))))

(ert-deftest clutch-test-object-describe-propagates-related-object-errors ()
  "Describe rendering should not silently swallow related-object lookup failures."
  (cl-letf (((symbol-function 'clutch--ensure-table-comment) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-column-details) (lambda (&rest _) nil))
            ((symbol-function 'clutch-db-list-columns) (lambda (&rest _) '("id")))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn)
               (signal 'clutch-db-error '("related boom")))))
    (should-error
     (clutch--object-describe-text 'fake-conn '(:name "USERS" :type "TABLE"))
     :type 'clutch-db-error)))

;;;; Object — jump, resolve, and warmup

(ert-deftest clutch-test-object-read-annotates-mixed-object-types ()
  "Object reader should expose warmed non-table metadata in the flat picker."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        captured)
    (let ((by-type (make-hash-table :test 'equal)))
      (puthash "INDEX"
               '((:name "ORDER_IDX" :type "INDEX"
                  :schema "APP" :source-schema "APP"))
               by-type)
      (puthash "PROCEDURE"
               '((:name "PROCESS_ORDER" :type "PROCEDURE"
                  :schema "APP" :source-schema "APP"
                  :status "VALID"))
               by-type)
      (puthash "fake-key"
               (list :entries '((:name "ORDER_IDX" :type "INDEX"
                                 :schema "APP" :source-schema "APP")
                                (:name "PROCESS_ORDER" :type "PROCEDURE"
                                 :schema "APP" :source-schema "APP"
                                 :status "VALID"))
                     :by-type by-type
                     :loaded-categories '(indexes procedures))
               clutch--object-cache))
    (cl-letf (((symbol-function 'clutch--ensure-connection) #'ignore)
              ((symbol-function 'clutch--warn-schema-cache-state) #'ignore)
              ((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-db-list-table-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE" :schema "APP" :source-schema "APP")
                   (:name "V_ORDERS" :type "VIEW" :schema "APP" :source-schema "APP"))))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "USER_TABLES" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC"))))
              ((symbol-function 'clutch-db-list-objects)
               (lambda (&rest _args)
                 (ert-fail "default flat picker should not synchronously fetch uncached object categories")))
              ((symbol-function 'run-with-idle-timer)
               (lambda (&rest _args) 'fake-timer))
              ((symbol-function 'completing-read)
               (lambda (_prompt collection &rest _args)
                 (let* ((metadata (funcall collection "" nil 'metadata))
                        (meta-alist (cdr metadata))
                        (annotation-fn (alist-get 'annotation-function meta-alist))
                        (group-fn (alist-get 'group-function meta-alist))
                        (base (all-completions "" collection nil)))
                   (setq captured
                         (list :category (alist-get 'category meta-alist)
                               :base base
                               :orders-group (funcall group-fn "ORDERS" nil)
                               :proc-group (funcall group-fn "PROCESS_ORDER" nil)
                               :proc-ann (funcall annotation-fn "PROCESS_ORDER")))
                   "PROCESS_ORDER"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (equal (clutch-object-read "Object: ")
                       '(:name "PROCESS_ORDER" :type "PROCEDURE"
                         :schema "APP" :source-schema "APP" :status "VALID"))))
      (should (eq (plist-get captured :category) 'clutch-object))
      (should (equal (plist-get captured :base)
                     '("ORDERS" "V_ORDERS" "USER_TABLES" "ORDER_IDX" "PROCESS_ORDER")))
      (should (equal (plist-get captured :orders-group) "Tables"))
      (should (equal (plist-get captured :proc-group) "Procedures"))
      (should (equal (plist-get captured :proc-ann) "  APP/procedure")))))

(ert-deftest clutch-test-jump-resolves-then-runs-default-action ()
  "Jump should resolve the object once and run the shared default action."
  (let (resolved-prompt resolved-category resolved-types action-call presented-entry)
    (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
               (lambda (prompt &optional _table-like-only category allowed-types)
                 (setq resolved-prompt prompt)
                 (setq resolved-category category)
                 (setq resolved-types allowed-types)
                 '(:name "ORDERS" :type "TABLE")))
              ((symbol-function 'clutch--run-object-action)
               (lambda (entry action-id)
                 (setq action-call (list entry action-id))))
              ((symbol-function 'clutch--present-object-actions-natively)
               (lambda (entry)
                 (setq presented-entry entry)
                 t)))
      (clutch-jump)
      (should (equal resolved-prompt "Jump to object: "))
      (should-not resolved-category)
      (should (equal resolved-types clutch-primary-object-types))
      (should (equal action-call
                     '((:name "ORDERS" :type "TABLE") browse)))
      (should-not presented-entry))))

(ert-deftest clutch-test-jump-falls-back-to-default-action-when-ui-is-unavailable ()
  "Jump should still run the default action when no action UI is available."
  (let (resolved-prompt action-call)
    (cl-letf (((symbol-function 'clutch--resolve-object-dwim)
               (lambda (prompt &optional _table-like-only _category _allowed-types)
                 (setq resolved-prompt prompt)
                 '(:name "PROCESS_ORDER" :type "PROCEDURE")))
              ((symbol-function 'clutch--run-object-action)
                 (lambda (entry action-id)
                   (setq action-call (list entry action-id)))))
      (clutch-jump)
      (should (equal resolved-prompt "Jump to object: "))
      (should (equal action-call
                     '((:name "PROCESS_ORDER" :type "PROCEDURE")
                       show-definition))))))

(ert-deftest clutch-test-jump-on-table-at-point-in-console-prompts-before-browse ()
  "Jump should prompt before browsing when point is on a table-like name."
  (with-temp-buffer
    (clutch-mode)
    (insert "users")
    (goto-char (point-min))
    (setq-local clutch-connection 'fake-conn)
    (let (read-args resolved-called action-call)
      (cl-letf (((symbol-function 'clutch-object-at-point)
                 (lambda () '(:name "users" :type "TABLE")))
                ((symbol-function 'thing-at-point)
                 (lambda (&rest _args) "users"))
                ((symbol-function 'clutch-object-read)
                 (lambda (prompt &optional table-like-only initial-input category allowed-types)
                   (setq read-args (list prompt table-like-only initial-input category allowed-types))
                   '(:name "users" :type "TABLE")))
                ((symbol-function 'clutch--resolve-object-dwim)
                 (lambda (&rest _args)
                   (setq resolved-called t)
                   '(:name "users" :type "TABLE")))
                ((symbol-function 'clutch--run-object-action)
                 (lambda (entry action-id)
                   (setq action-call (list entry action-id)))))
        (clutch-jump)
        (should read-args)
        (should (equal (nth 0 read-args) "Jump to object: "))
        (should-not (nth 1 read-args))
        (should (equal (nth 2 read-args) "users"))
        (should-not (nth 3 read-args))
        (should (equal (nth 4 read-args) clutch-primary-object-types))
        (should-not resolved-called)
        (should (equal action-call
                       '((:name "users" :type "TABLE") browse)))))))

(ert-deftest clutch-test-object-read-filters-by-allowed-types ()
  "Object reader should honor an explicit allowed type set."
  (let (captured)
    (cl-letf (((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_IDX" :type "INDEX")
                   (:name "PROCESS_ORDER" :type "PROCEDURE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _category)
                 (setq captured entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (should (equal (clutch-object-read "Object: " nil nil nil '("TABLE" "VIEW"))
                       '(:name "ORDERS" :type "TABLE"))))
      (should (equal captured '((:name "ORDERS" :type "TABLE")))))))

(ert-deftest clutch-test-symbol-has-local-completions-p ()
  "Prefix helper should match case-insensitively."
  (let ((entries '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_ITEMS" :type "TABLE"))))
    (should (clutch--symbol-has-local-completions-p "ord" entries))
    (should (clutch--symbol-has-local-completions-p "ORD" entries))
    (should-not (clutch--symbol-has-local-completions-p "ZZZ" entries))))

(ert-deftest clutch-test-resolve-entry-prefills-when-symbol-has-local-completions ()
  "When symbol at point prefix-matches local entries, pre-fill the picker."
  (let (captured-initial)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "ORD"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "ORDERS" :type "TABLE")
                   (:name "ORDER_ITEMS" :type "TABLE"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "ORDERS" :type "TABLE"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (equal captured-initial "ORD"))))))

(ert-deftest clutch-test-resolve-entry-single-remote-hit-returns-directly ()
  "When on-demand search finds exactly one hit, return it directly."
  (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-connection) (lambda () t))
            ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
            ((symbol-function 'thing-at-point) (lambda (&rest _) "RARE_TABLE"))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn &optional _refresh) nil))
            ((symbol-function 'clutch-db-search-table-entries)
             (lambda (_conn _prefix)
               '((:name "RARE_TABLE" :type "TABLE"))))
            ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t)))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--resolve-object-entry "Describe: ")
                     '(:name "RARE_TABLE" :type "TABLE"))))))

(ert-deftest clutch-test-resolve-entry-multiple-remote-hits-opens-picker ()
  "When on-demand search finds multiple hits, open picker with those hits."
  (let (captured-entries)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "AUDIT"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "AUDIT_LOG" :type "TABLE")
                   (:name "AUDIT_TRAIL" :type "TABLE"))))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _cat)
                 (setq captured-entries entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (= (length captured-entries) 2))))))

(ert-deftest clutch-test-resolve-entry-no-match-anywhere-opens-full-picker ()
  "When nothing matches, show message and open picker with no pre-fill."
  (let (captured-initial messages)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "preferential_price"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "ORDERS" :type "TABLE"))))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix) nil))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) t))
              ((symbol-function 'message)
               (lambda (fmt &rest args)
                 (push (apply #'format fmt args) messages)))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "ORDERS" :type "TABLE"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (null captured-initial))
        (should (cl-some (lambda (m) (string-match-p "preferential_price" m))
                         messages))))))

(ert-deftest clutch-test-resolve-entry-sync-refresh-finds-non-table-object ()
  "Sync refresh should discover non-table objects when cache is incomplete."
  (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
            ((symbol-function 'clutch--ensure-connection) (lambda () t))
            ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
            ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
            ((symbol-function 'thing-at-point) (lambda (&rest _) "PROCESS_ORDER"))
            ((symbol-function 'clutch--object-entries)
             (lambda (_conn &optional refresh)
               (if refresh
                   '((:name "PROCESS_ORDER" :type "PROCEDURE"))
                 nil)))
            ((symbol-function 'clutch-db-search-table-entries)
             (lambda (_conn _prefix) nil))
            ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil)))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--resolve-object-entry "Describe: ")
                     '(:name "PROCESS_ORDER" :type "PROCEDURE"))))))

(ert-deftest clutch-test-resolve-entry-merges-table-and-non-table-hits ()
  "On-demand search should merge table search and sync-refresh results."
  (let (captured-entries)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "ORDER"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional refresh)
                 (if refresh
                     '((:name "ORDERS" :type "TABLE")
                       (:name "ORDER_PROC" :type "PROCEDURE"))
                   nil)))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix)
                 '((:name "ORDERS" :type "TABLE"))))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt entries &optional _initial _cat)
                 (setq captured-entries entries)
                 (car entries))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        ;; Should have both TABLE and PROCEDURE hits
        (should (>= (length captured-entries) 2))
        (should (cl-some (lambda (e) (equal (plist-get e :type) "PROCEDURE"))
                         captured-entries))))))

(ert-deftest clutch-test-resolve-entry-prefix-matches-non-table-objects ()
  "Local prefix match should work for non-table objects too."
  (let (captured-initial)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "GET_"))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional _refresh)
                 '((:name "GET_ORDER" :type "FUNCTION")
                   (:name "GET_CUSTOMER" :type "FUNCTION"))))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional initial _cat)
                 (setq captured-initial initial)
                 '(:name "GET_ORDER" :type "FUNCTION"))))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Describe: ")
        (should (equal captured-initial "GET_"))))))

(ert-deftest clutch-test-resolve-entry-table-like-only-skips-non-table-refresh ()
  "When table-like-only is set, on-demand search should not sync-refresh."
  (let (refresh-called)
    (cl-letf (((symbol-function 'clutch--buffer-current-object) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-matches-at-point) (lambda (&rest _) nil))
              ((symbol-function 'clutch--ensure-connection) (lambda () t))
              ((symbol-function 'clutch--warn-schema-cache-state) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-connection-alive-p) (lambda (_) t))
              ((symbol-function 'thing-at-point) (lambda (&rest _) "UNKNOWN_TBL"))
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-search-table-entries)
               (lambda (_conn _prefix) nil))
              ((symbol-function 'clutch--object-entries)
               (lambda (_conn &optional refresh)
                 (when refresh (setq refresh-called t))
                 nil))
              ((symbol-function 'clutch--object-cache-complete-p) (lambda (_) nil))
              ((symbol-function 'message) (lambda (&rest _) nil))
              ((symbol-function 'clutch--object-entry-reader)
               (lambda (_conn _prompt _entries &optional _initial _cat) nil)))
      (with-temp-buffer
        (setq-local clutch-connection 'fake-conn)
        (clutch--resolve-object-entry "Table: " t)
        (should-not refresh-called)))))

(ert-deftest clutch-test-object-at-point-prefers-table-over-public-synonym ()
  "Symbol resolution should prefer concrete schema objects over PUBLIC synonyms."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'thing-at-point)
               (lambda (&rest _) "BQS_BERTH_QUEUE"))
              ((symbol-function 'clutch--object-matches-by-name)
               (lambda (_conn name &optional _table-like-only _allowed-types)
                 (should (equal name "BQS_BERTH_QUEUE"))
                 '((:name "BQS_BERTH_QUEUE" :type "PUBLIC SYNONYM"
                    :schema "SYS" :source-schema "PUBLIC")
                   (:name "BQS_BERTH_QUEUE" :type "TABLE"
                    :schema "zj_test" :source-schema "zj_test")))))
      (should (equal (clutch-object-at-point)
                     '(:name "BQS_BERTH_QUEUE" :type "TABLE"
                       :schema "zj_test" :source-schema "zj_test"))))))

(ert-deftest clutch-test-object-entries-use-fast-partial-cache-on-first-open ()
  "Unified object entries should avoid synchronous full metadata scans."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        scheduled)
    (let ((by-type (make-hash-table :test 'equal)))
      (puthash "PROCEDURE"
               '((:name "PROCESS_ORDER" :type "PROCEDURE"
                  :schema "APP" :source-schema "APP"))
               by-type)
      (puthash "fake-key"
               (list :entries '((:name "PROCESS_ORDER" :type "PROCEDURE"
                                 :schema "APP" :source-schema "APP"))
                     :by-type by-type
                     :loaded-categories '(procedures))
               clutch--object-cache))
    (cl-letf (((symbol-function 'clutch--connection-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--browseable-object-entries)
               (lambda (_conn)
                 '((:name "ORDERS" :type "TABLE"
                    :schema "APP" :source-schema "APP"))))
              ((symbol-function 'clutch-db-list-objects)
               (lambda (&rest _args)
                 (ert-fail "should not synchronously load uncached object categories")))
              ((symbol-function 'run-with-idle-timer)
               (lambda (secs repeat fn)
                 (setq scheduled (list secs repeat fn))
                 'fake-timer)))
      (should (equal (clutch--object-entries 'fake-conn)
                     '((:name "ORDERS" :type "TABLE"
                        :schema "APP" :source-schema "APP")
                       (:name "PROCESS_ORDER" :type "PROCEDURE"
                        :schema "APP" :source-schema "APP"))))
      (when scheduled
        (should (equal (car scheduled) clutch-object-warmup-idle-delay-seconds))))))

(ert-deftest clutch-test-object-warmup-prefers-async-object-loading-when-available ()
  "Warmup should use backend async object loading when supported."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        async-call sync-called timer-fn)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (_conn category callback &optional _errback)
                 (setq async-call category)
                 (funcall callback
                          '((:name "ORDER_IDX" :type "INDEX"
                             :schema "APP" :source-schema "APP")))
                 t))
              ((symbol-function 'clutch--store-object-cache-type-entries)
               (lambda (_conn type entries)
                 (setq async-call (list async-call type entries))))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (setq sync-called t)
                 nil)))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (funcall timer-fn)
      (should (equal async-call
                     '(indexes "INDEX" ((:name "ORDER_IDX" :type "INDEX"
                                 :schema "APP" :source-schema "APP")))))
      (should-not sync-called))))

(ert-deftest clutch-test-object-warmup-reschedules-when-connection-is-busy ()
  "Warmup should defer background object loading while the connection is busy."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fns async-called sync-called)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) t))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (push fn timer-fns)
                 (intern (format "fake-timer-%d" (length timer-fns)))))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (&rest _args)
                 (setq async-called t)
                 t))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (setq sync-called t)
                 nil)))
      (clutch--schedule-object-warmup 'fake-conn)
      (should (= (length timer-fns) 1))
      (funcall (car timer-fns))
      (should (= (length timer-fns) 2))
      (should-not async-called)
      (should-not sync-called))))

(ert-deftest clutch-test-object-warmup-reschedules-on-recoverable-db-errors ()
  "Warmup should reschedule when background metadata fetch hits `clutch-db-error'."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fns
        warned)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--warn-completion-metadata-error-once)
               (lambda (message-text)
                 (setq warned message-text)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (push fn timer-fns)
                 (intern (format "fake-timer-%d" (length timer-fns)))))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (&rest _args)
                 nil))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (signal 'clutch-db-error '("warmup boom")))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should (= (length timer-fns) 1))
      (funcall (car timer-fns))
      (should (= (length timer-fns) 2))
      (should (string-match-p "warmup boom" warned)))))

(ert-deftest clutch-test-object-warmup-records-debug-event-on-recoverable-db-errors ()
  "Warmup metadata failures should feed the shared debug trace when enabled."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fns)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--object-cache-key)
                   (lambda (_conn) "fake-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--object-cache-loaded-categories)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch--warn-completion-metadata-error-once)
                   #'ignore)
                  ((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn)
                     (push fn timer-fns)
                     (intern (format "fake-timer-%d" (length timer-fns)))))
                  ((symbol-function 'clutch-db-list-objects-async)
                   (lambda (&rest _args)
                     nil))
                  ((symbol-function 'clutch--object-type-entries)
                   (lambda (&rest _args)
                     (signal 'clutch-db-error '("warmup boom"))))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (clutch--schedule-object-warmup 'fake-conn)
          (should (= (length timer-fns) 1))
          (funcall (car timer-fns))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: object-warmup" text))
            (should (string-match-p "Phase: warning" text))
            (should (string-match-p "warmup boom" text))))))))

(ert-deftest clutch-test-object-warmup-warns-on-connection-liveness-errors ()
  "Warmup should warn instead of silently hiding recoverable liveness failures."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        warned
        scheduled)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn)
                 (signal 'clutch-db-error '("alive boom"))))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch--warn-completion-metadata-error-once)
               (lambda (message-text)
                 (setq warned message-text)))
              ((symbol-function 'run-with-idle-timer)
               (lambda (&rest _args)
                 (setq scheduled t)
                 'fake-timer)))
      (clutch--schedule-object-warmup 'fake-conn)
      (should (string-match-p "alive boom" warned))
      (should-not scheduled))))

(ert-deftest clutch-test-safe-completion-call-records-debug-event-on-db-errors ()
  "Recoverable completion metadata errors should surface in the debug buffer."
  (with-temp-buffer
    (let ((clutch-debug-mode t))
      (setq-local clutch-connection 'fake-conn)
      (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                 (lambda (_conn) 'oracle))
                ((symbol-function 'clutch--warn-completion-metadata-error-once)
                 #'ignore)
                ((symbol-function 'pop-to-buffer)
                 (lambda (buf &rest _args) buf)))
        (clutch--clear-debug-capture)
        (should-not
         (clutch--safe-completion-call
          (lambda ()
            (signal 'clutch-db-error '("completion boom")))))
        (let ((text (clutch-test--debug-buffer-string)))
          (should (string-match-p "Operation: completion" text))
          (should (string-match-p "Phase: warning" text))
          (should (string-match-p "completion boom" text)))))))

(ert-deftest clutch-test-object-warmup-propagates-non-db-errors ()
  "Warmup should still expose programming errors that are not db-runtime races."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (&rest _args)
                 nil))
              ((symbol-function 'clutch--object-type-entries)
               (lambda (&rest _args)
                 (error "Warmup boom"))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (should-error (funcall timer-fn) :type 'error))))

(ert-deftest clutch-test-object-warmup-ignores-stale-async-callbacks ()
  "Warmup should ignore async object results after invalidation."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn
        async-callback
        stored)
    (cl-letf (((symbol-function 'clutch--object-cache-key)
               (lambda (_conn) "fake-key"))
              ((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch--object-cache-loaded-categories)
               (lambda (_conn) nil))
              ((symbol-function 'clutch-db-busy-p)
               (lambda (_conn) nil))
              ((symbol-function 'run-with-idle-timer)
               (lambda (_secs _repeat fn)
                 (setq timer-fn fn)
                 'fake-timer))
              ((symbol-function 'clutch-db-list-objects-async)
               (lambda (_conn _category callback &optional _errback)
                 (setq async-callback callback)
                 t))
              ((symbol-function 'clutch--store-object-cache-type-entries)
               (lambda (_conn _type entries)
                 (setq stored entries))))
      (clutch--schedule-object-warmup 'fake-conn)
      (should timer-fn)
      (funcall timer-fn)
      (should async-callback)
      (clutch--invalidate-object-warmup 'fake-conn)
      (funcall async-callback
               '((:name "ORDER_IDX" :type "INDEX"
                  :schema "APP" :source-schema "APP")))
      (should-not stored))))

(ert-deftest clutch-test-object-warmup-records-submit-success-and-stale-debug-events ()
  "Async object warmup should record submit, success, and stale-drop phases."
  (let ((clutch--object-cache (make-hash-table :test 'equal))
        (clutch--object-warmup-timers (make-hash-table :test 'equal))
        (clutch--object-warmup-generations (make-hash-table :test 'equal))
        timer-fn
        async-callback)
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (setq-local clutch-connection 'fake-conn)
        (cl-letf (((symbol-function 'clutch--object-cache-key)
                   (lambda (_conn) "fake-key"))
                  ((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'mysql))
                  ((symbol-function 'clutch--connection-alive-p)
                   (lambda (_conn) t))
                  ((symbol-function 'clutch--object-cache-loaded-categories)
                   (lambda (_conn) nil))
                  ((symbol-function 'clutch-db-busy-p)
                   (lambda (_conn) nil))
                  ((symbol-function 'run-with-idle-timer)
                   (lambda (_secs _repeat fn)
                     (setq timer-fn fn)
                     'fake-timer))
                  ((symbol-function 'clutch-db-list-objects-async)
                   (lambda (_conn _category callback &optional _errback)
                     (setq async-callback callback)
                     t))
                  ((symbol-function 'clutch--store-object-cache-type-entries)
                   (lambda (&rest _args) nil))
                  ((symbol-function 'pop-to-buffer)
                   (lambda (buf &rest _args) buf)))
          (clutch--clear-debug-capture)
          (clutch--schedule-object-warmup 'fake-conn)
          (should timer-fn)
          (funcall timer-fn)
          (should async-callback)
          (funcall async-callback
                   '((:name "ORDER_IDX" :type "INDEX"
                      :schema "APP" :source-schema "APP")))
          (clutch--schedule-object-warmup 'fake-conn)
          (funcall timer-fn)
          (clutch--invalidate-object-warmup 'fake-conn)
          (funcall async-callback
                   '((:name "STALE_IDX" :type "INDEX"
                      :schema "APP" :source-schema "APP")))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "Operation: object-warmup" text))
            (should (string-match-p "Phase: submit" text))
            (should (string-match-p "Phase: success" text))
            (should (string-match-p "Phase: stale-drop" text))))))))

(ert-deftest clutch-test-object-cache-key-propagates-connection-key-errors ()
  "Object cache key should not silently hide connection-key failures."
  (cl-letf (((symbol-function 'clutch--connection-key)
             (lambda (_conn)
               (signal 'error '("cache-key boom")))))
    (should-error (clutch--object-cache-key 'fake-conn) :type 'error)))

(ert-deftest clutch-test-object-cache-key-falls-back-on-nil-connection-key ()
  "Object cache key should still fall back when connection-key returns nil."
  (cl-letf (((symbol-function 'clutch--connection-key)
             (lambda (_conn) nil)))
    (should (equal (clutch--object-cache-key 'fake-conn)
                   "fake-conn"))))

(ert-deftest clutch-test-browseable-object-entries-skip-empty-search-for-oracle-jdbc ()
  "Oracle JDBC browseable entries should not issue an extra empty-prefix search."
  (cl-letf (((symbol-function 'clutch-db-browseable-object-entries)
             (lambda (_conn)
               '((:name "ORDERS" :type "TABLE")))))
    (should (equal (clutch--browseable-object-entries 'fake-conn)
                   '((:name "ORDERS" :type "TABLE"))))))

(ert-deftest clutch-test-object-entry-reader-keeps-overloaded-object-names ()
  "Object reader should keep duplicate names distinct when identity differs."
  (let (captured)
    (cl-letf (((symbol-function 'completing-read)
               (lambda (_prompt collection &rest _args)
                 (setq captured (funcall collection "" nil t))
                 (car captured))))
      (should (equal (clutch--object-entry-reader
                      'fake-conn
                      "Object: "
                      '((:name "process_order" :type "FUNCTION"
                         :schema "public" :source-schema "public"
                         :identity "OID:1")
                        (:name "process_order" :type "FUNCTION"
                         :schema "public" :source-schema "public"
                         :identity "OID:2")))
                     '(:name "process_order" :type "FUNCTION"
                       :schema "public" :source-schema "public"
                       :identity "OID:1")))
      (should (= (length captured) 2))
      (should-not (equal (car captured) (cadr captured))))))

(ert-deftest clutch-test-object-entry-annotation-shows-detail-only-for-duplicates ()
  "Object entry detail should only appear when duplicate names need disambiguation."
  (let ((duplicate-counts (make-hash-table :test 'equal)))
    (puthash "ORDERS" 1 duplicate-counts)
    (should (equal (substring-no-properties
                    (clutch--object-entry-annotation
                     '(:name "ORDERS" :type "SYNONYM"
                       :schema "DATA_OWNER" :source-schema "APP")
                     duplicate-counts))
                   "  APP/synonym"))
    (puthash "ORDERS" 2 duplicate-counts)
    (should (equal (substring-no-properties
                    (clutch--object-entry-annotation
                     '(:name "ORDERS" :type "SYNONYM"
                       :schema "DATA_OWNER" :source-schema "APP")
                     duplicate-counts))
                   "  APP/synonym  DATA_OWNER"))))

(ert-deftest clutch-test-object-resolve-prefers-definition-buffer-object-over-symbol-at-point ()
  "Definition buffers should use the displayed object before `symbol-at-point'."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn
                clutch-browser-current-object '(:name "BQS_BERTH" :type "TABLE"))
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'thing-at-point)
               (lambda (&rest _) "BQS_BERTH_0807"))
              ((symbol-function 'clutch--object-matches-by-name)
               (lambda (_conn name &optional _table-like-only _allowed-types)
                 (should (equal name "BQS_BERTH_0807"))
                           '((:name "BQS_BERTH_0807" :type "TABLE")))))
      (should (equal (clutch--resolve-object-entry "Referenced by: ")
                     '(:name "BQS_BERTH" :type "TABLE"))))))

;;;; Object — Embark integration

(ert-deftest clutch-test-embark-object-target-uses-object-at-point ()
  "Embark target finder should expose the object at point."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local major-mode 'clutch-mode)
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-object-at-point)
               (lambda () '(:name "orders" :type "TABLE")))
              ((symbol-function 'bounds-of-thing-at-point)
               (lambda (_thing) '(1 . 7))))
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "orders" :type "TABLE") 1 7))))))

(ert-deftest clutch-test-embark-object-target-prefers-definition-buffer-object ()
  "Embark target finder should use the buffer's current object in definition buffers."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (setq-local major-mode 'sql-mode)
    (setq-local clutch-browser-current-object '(:name "PROCESS_ORDER" :type "PROCEDURE"))
    (cl-letf (((symbol-function 'clutch--connection-alive-p)
               (lambda (_conn) t))
              ((symbol-function 'clutch-object-at-point)
               (lambda () nil)))
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "PROCESS_ORDER" :type "PROCEDURE")))))))

(ert-deftest clutch-test-embark-object-target-prefers-dispatch-entry ()
  "Embark target finder should use the explicit dispatch entry when present."
  (let ((clutch--object-dispatch-entry '(:name "ORDERS" :type "TABLE")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-object (:name "ORDERS" :type "TABLE")))))))

(ert-deftest clutch-test-embark-object-target-uses-target-capable-type ()
  "Embark target finder should expose a distinct target type for target-capable objects."
  (let ((clutch--object-dispatch-entry
         '(:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      (should (equal (clutch--embark-object-target)
                     '(clutch-target-object
                       (:name "ORDER_IDX" :type "INDEX" :target-table "ORDERS")))))))

(ert-deftest clutch-test-embark-around-hook-resolves-candidate ()
  "Around-action hook should resolve target string to entry via completion map."
  (let* ((entry '(:name "xxx_detail" :type "TABLE" :schema "public"))
         (clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx_detail" entry clutch--object-completion-entry-map)
    ;; Simulate what Embark passes to around hooks.
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "xxx_detail"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (equal resolved entry)))))

(ert-deftest clutch-test-embark-around-hook-clears-map ()
  "Around-action hook should clear the completion map after resolution."
  (let ((clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx" '(:name "xxx" :type "TABLE") clutch--object-completion-entry-map)
    (clutch--embark-with-resolved-entry
     :target "xxx"
     :run (lambda (&rest _) nil))
    ;; The global should be nil after the hook returns.
    (should (null (symbol-value 'clutch--object-completion-entry-map)))))

(ert-deftest clutch-test-embark-around-hook-nil-for-unknown-target ()
  "Around-action hook should leave dispatch entry nil for unknown targets."
  (let* ((clutch--object-completion-entry-map (make-hash-table :test 'equal)))
    (puthash "xxx" '(:name "xxx" :type "TABLE") clutch--object-completion-entry-map)
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "yyy"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (null resolved)))))

(ert-deftest clutch-test-embark-around-hook-nil-without-map ()
  "Around-action hook should be a no-op when no completion map exists."
  (let ((clutch--object-completion-entry-map nil))
    (let (resolved)
      (clutch--embark-with-resolved-entry
       :target "xxx"
       :run (lambda (&rest _)
              (setq resolved clutch--object-dispatch-entry)))
      (should (null resolved)))))

(ert-deftest clutch-test-resolve-object-entry-prefers-dispatch-entry ()
  "Resolution should prefer dispatch entry over point matches."
  (let ((clutch--object-dispatch-entry '(:name "xxx_detail" :type "TABLE")))
    (with-temp-buffer
      (setq-local clutch-connection 'fake-conn)
      ;; Insert a different object name at point to prove dispatch wins.
      (insert "xxx")
      (goto-char (point-min))
      (should (equal (clutch--resolve-object-entry "Test: ")
                     '(:name "xxx_detail" :type "TABLE"))))))

(ert-deftest clutch-test-embark-action-specs-hide-default-actions ()
  "Embark menus should omit actions that duplicate `RET'."
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs
                          (lambda (spec)
                            (not (eq (plist-get spec :id) 'jump-target)))))
                 '(describe show-definition copy-name copy-fqname))))

(ert-deftest clutch-test-embark-target-action-specs-keep-jump-target ()
  "Target-capable Embark menus should keep jump-target."
  (should (equal (mapcar (lambda (spec) (plist-get spec :id))
                         (clutch--embark-action-specs))
                 '(describe show-definition jump-target copy-name copy-fqname))))

(ert-deftest clutch-test-embark-command-label-uses-shared-label ()
  "Embark command labels should reuse the shared object action wording."
  (should (equal (clutch--embark-command-label 'clutch-object-show-ddl-or-source)
                 "Show definition"))
  (should (equal (clutch--embark-command-label 'clutch-object-default-action)
                 "Default action")))

;;;; Object — schema switching

(ert-deftest clutch-test-switch-schema-uses-selected-schema-and-refreshes ()
  "Schema switching should update params, clear caches, and refresh metadata."
  (let ((conn 'fake-conn)
        switched refresh-called message-text)
    (with-temp-buffer
      (setq-local clutch-connection conn
                  clutch--connection-params '(:driver oracle :schema "ZJ_TEST"))
      (cl-letf (((symbol-function 'clutch-db-list-schemas)
                 (lambda (_conn) '("ZJ_TEST" "CJH_TEST")))
                ((symbol-function 'clutch--connection-key)
                 (lambda (_conn) "user@host:1521/ORCL"))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "ZJ_TEST"))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "CJH_TEST"))
                ((symbol-function 'clutch-db-set-current-schema)
                 (lambda (_conn schema) (setq switched schema) schema))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional _key) (setq refresh-called 'cleared)))
                ((symbol-function 'clutch--refresh-current-schema)
                 (lambda (&optional quiet)
                   (setq refresh-called (list refresh-called quiet))
                   t))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq message-text (apply #'format fmt args)))))
        (clutch-switch-schema)
        (should (equal switched "CJH_TEST"))
        (should (equal clutch--connection-params '(:driver oracle :schema "CJH_TEST")))
        (should (equal refresh-called '(cleared t)))
        (should (equal message-text "Current schema: CJH_TEST"))))))

(ert-deftest clutch-test-switch-schema-failure-populates-problem-record-and-debug-trace ()
  "Schema-switch failures should feed the shared problem/debug workflow."
  (let ((details '(:backend oracle
                   :summary "ORA-12592: TNS:bad packet"
                   :diag (:category "metadata"
                          :op "set-current-schema"
                          :conn-id 7
                          :raw-message "ORA-12592: TNS:bad packet"
                          :context (:generated-sql
                                    "ALTER SESSION SET CURRENT_SCHEMA = \"CJH_TEST\"")))))
    (with-temp-buffer
      (let ((clutch-debug-mode t))
        (clutch--clear-debug-capture)
        (setq-local clutch-connection 'fake-conn
                    clutch--connection-params '(:driver oracle :schema "ZJ_TEST"))
        (cl-letf (((symbol-function 'clutch--backend-key-from-conn)
                   (lambda (_conn) 'oracle))
                  ((symbol-function 'clutch--connection-key)
                   (lambda (_conn) "user@host:1521/ORCL"))
                  ((symbol-function 'clutch-db-list-schemas)
                   (lambda (_conn) '("ZJ_TEST" "CJH_TEST")))
                  ((symbol-function 'clutch-db-current-schema)
                   (lambda (_conn) "ZJ_TEST"))
                  ((symbol-function 'completing-read)
                   (lambda (&rest _args) "CJH_TEST"))
                  ((symbol-function 'clutch-db-set-current-schema)
                   (lambda (_conn _schema)
                     (signal 'clutch-db-error
                             (list "ORA-12592: TNS:bad packet" details)))))
          (condition-case err
              (progn
                (clutch-switch-schema)
                (should nil))
            (user-error
             (should (string-match-p "ORA-12592" (cadr err)))
             (should (string-match-p (regexp-quote clutch-debug-buffer-name)
                                     (cadr err)))))
          (let ((problem clutch--buffer-error-details))
            (should problem)
            (should (equal (plist-get problem :summary) "ORA-12592: TNS:bad packet"))
            (should (eq (plist-get problem :backend) 'oracle)))
          (let ((text (clutch-test--debug-buffer-string)))
            (should (string-match-p "set-current-schema" text))
            (should (string-match-p "Operation: schema-switch" text))
            (should (string-match-p "Phase: error" text))
            (should (string-match-p "ALTER SESSION SET CURRENT_SCHEMA" text))))))))

(ert-deftest clutch-test-switch-schema-updates-mysql-database-param ()
  "MySQL schema switching should persist the selected database in buffer params."
  (let ((conn 'fake-conn)
        switched cleared-keys)
    (with-temp-buffer
      (setq-local clutch-connection conn
                  clutch--connection-params '(:backend mysql :database "zj_test"))
      (cl-letf (((symbol-function 'clutch--connection-key)
                 (lambda (_conn)
                   (if (equal clutch--connection-params '(:backend mysql :database "zj_test"))
                       "user@host:3306/zj_test"
                     "user@host:3306/cjh_test")))
                ((symbol-function 'clutch-db-list-schemas)
                 (lambda (_conn) '("zj_test" "cjh_test")))
                ((symbol-function 'clutch-db-current-schema)
                 (lambda (_conn) "zj_test"))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "cjh_test"))
                ((symbol-function 'clutch-db-set-current-schema)
                 (lambda (_conn schema) (setq switched schema) schema))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional key)
                   (push (or key "current") cleared-keys)))
                ((symbol-function 'clutch--refresh-current-schema)
                 (lambda (&optional _quiet) t))
                ((symbol-function 'message) (lambda (&rest _args) nil)))
        (clutch-switch-schema)
        (should (equal switched "cjh_test"))
        (should (equal clutch--connection-params '(:backend mysql :database "cjh_test")))
        (should (equal cleared-keys '("current" "user@host:3306/zj_test")))))))

(ert-deftest clutch-test-switch-schema-clickhouse-reconnects-to-selected-database ()
  "ClickHouse switching should reconnect with the selected database."
  (let ((old-conn 'old-conn)
        (new-conn 'new-conn)
        built-params disconnected primed tx-cleared cleared-keys message-text)
    (with-temp-buffer
      (setq-local clutch-connection old-conn
                  clutch--connection-params '(:backend clickhouse
                                              :host "127.0.0.1"
                                              :port 8123
                                              :database "default"
                                              :user "default"))
      (cl-letf (((symbol-function 'clutch--list-clickhouse-databases)
                 (lambda (_conn) '("default" "demo")))
                ((symbol-function 'clutch--connection-alive-p)
                 (lambda (_conn) t))
                ((symbol-function 'clutch--confirm-disconnect-transaction-loss)
                 (lambda (&rest _) t))
                ((symbol-function 'completing-read)
                 (lambda (&rest _args) "demo"))
                ((symbol-function 'clutch--build-conn)
                 (lambda (params)
                   (setq built-params params)
                   new-conn))
                ((symbol-function 'clutch-db-disconnect)
                 (lambda (conn) (setq disconnected conn)))
                ((symbol-function 'clutch--prime-schema-cache)
                 (lambda (conn) (setq primed conn)))
                ((symbol-function 'clutch--refresh-schema-status-ui) #'ignore)
                ((symbol-function 'clutch--refresh-transaction-ui) #'ignore)
                ((symbol-function 'clutch--clear-tx-dirty)
                 (lambda (conn) (setq tx-cleared conn)))
                ((symbol-function 'clutch--clear-connection-metadata-caches)
                 (lambda (_conn &optional key)
                   (push (or key "current") cleared-keys)))
                ((symbol-function 'clutch--connection-key)
                 (lambda (conn)
                   (if (eq conn old-conn)
                       "default-key"
                     "demo-key")))
                ((symbol-function 'message)
                 (lambda (fmt &rest args)
                   (setq message-text (apply #'format fmt args)))))
        (clutch-switch-schema)
        (should (eq clutch-connection new-conn))
        (should (equal built-params '(:backend clickhouse
                                      :host "127.0.0.1"
                                      :port 8123
                                      :database "demo"
                                      :user "default")))
        (should (equal clutch--connection-params '(:backend clickhouse
                                                   :host "127.0.0.1"
                                                   :port 8123
                                                   :database "demo"
                                                   :user "default")))
        (should (eq disconnected old-conn))
        (should (eq primed new-conn))
        (should (eq tx-cleared old-conn))
        (should (equal cleared-keys '("current" "default-key")))
        (should (equal message-text "Current database: demo"))))))

(ert-deftest clutch-test-switch-schema-header-line-shows-current-schema ()
  "Connection header line should display a non-default effective schema."
  (with-temp-buffer
    (setq-local clutch-connection 'fake-conn)
    (cl-letf (((symbol-function 'clutch--connection-alive-p) (lambda (_conn) t))
              ((symbol-function 'clutch--icon) (lambda (&rest _) "[schema]"))
              ((symbol-function 'clutch--db-backend-icon-for-key) (lambda (_key) nil))
              ((symbol-function 'clutch-db-display-name) (lambda (_conn) "Oracle"))
              ((symbol-function 'clutch--connection-display-key) (lambda (_conn) "scott@dbhost"))
              ((symbol-function 'clutch-db-user) (lambda (_conn) "SCOTT"))
              ((symbol-function 'clutch-db-database) (lambda (_conn) "ORCL"))
              ((symbol-function 'clutch-db-current-schema) (lambda (_conn) "ZJ_TEST"))
              ((symbol-function 'clutch--schema-status-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--tx-header-line-segment) (lambda (_conn) nil))
              ((symbol-function 'clutch--header-line-indent) (lambda () "")))
      (let ((line (clutch--build-connection-header-line)))
        (should (string-match-p "scott@dbhost" line))
        (should (string-match-p "\\[schema\\] ZJ_TEST" line))
        (should-not (string-match-p "Schema:" line))))))

;;;; Temporal formatting

(ert-deftest clutch-test-pg-bool-type-category ()
  "Pg-oid-bool should map to text, not numeric."
  (require 'clutch-db-pg)
  (should (eq (clutch-db-pg--type-category 16) 'text)))

(ert-deftest clutch-test-format-temporal-datetime ()
  "Clutch-db-format-temporal should format datetime plists."
  (should (equal (clutch-db-format-temporal
                  '(:year 2024 :month 1 :day 15 :hours 13 :minutes 45 :seconds 30))
                 "2024-01-15 13:45:30")))

(ert-deftest clutch-test-format-temporal-date-only ()
  "Clutch-db-format-temporal should format date-only plists."
  (should (equal (clutch-db-format-temporal '(:year 2024 :month 6 :day 1))
                 "2024-06-01")))

(ert-deftest clutch-test-format-temporal-time-only ()
  "Clutch-db-format-temporal should format time-only plists."
  (should (equal (clutch-db-format-temporal
                  '(:hours 13 :minutes 5 :seconds 0 :negative nil))
                 "13:05:00")))

(ert-deftest clutch-test-format-temporal-time-negative ()
  "Clutch-db-format-temporal should format negative time plists."
  (should (equal (clutch-db-format-temporal
                  '(:hours 1 :minutes 0 :seconds 0 :negative t))
                 "-01:00:00")))

(ert-deftest clutch-test-format-temporal-non-temporal ()
  "Clutch-db-format-temporal should return nil for non-temporal plists."
  (should (null (clutch-db-format-temporal '(:foo 1 :bar 2)))))

;;;; Live integration tests

(defmacro clutch-test--with-conn (var &rest body)
  "Execute BODY with VAR bound to a live connection.
Skips if `clutch-test-password' is nil."
  (declare (indent 1))
  `(if (null clutch-test-password)
       (ert-skip "Set clutch-test-password to enable live tests")
     (let ((mysql-tls-verify-server nil))
       (let ((,var (clutch-db-connect
                    clutch-test-backend
                    (list :host clutch-test-host
                          :port clutch-test-port
                          :user clutch-test-user
                          :password clutch-test-password
                          :database clutch-test-database))))
         (unwind-protect
             (progn ,@body)
           (clutch-db-disconnect ,var))))))

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
        (let ((widths (clutch--compute-column-widths col-names rows columns)))
          (setq-local clutch--column-widths widths))
        ;; Render
        (clutch--render-result)
        ;; Verify buffer has content
        (should (> (buffer-size) 0))
        ;; Column names are in header-line/mode-line, not buffer text.
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
              (let ((row-identity (car (clutch-db-row-identity-candidates
                                         conn table))))
                (should row-identity)
                (setq-local clutch--row-identity
                            (append (list :table table
                                          :indices '(0)
                                          :source-indices '(0))
                                    row-identity)))
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

;;; clutch-test.el ends here
