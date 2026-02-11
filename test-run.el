;;; test-run.el --- Test the column paging + record buffer -*- lexical-binding: t; -*-
(require 'mysql)
(require 'data-lens)

(defun test-mysql ()
  "Run all tests."
  (message "=== Loading packages ===")
  (message "mysql.el: %s  mysql-interactive.el: %s"
           (featurep 'mysql) (featurep 'mysql-interactive))

  (message "\n=== Connecting ===")
  (let ((conn (mysql-connect :host "127.0.0.1" :port 3307
                             :user "testuser" :password "testpass"
                             :database "testdb")))
    (mysql-query conn "SET NAMES utf8mb4")
    (message "Connected: %s" (data-lens--connection-key conn))

    ;; Test 1: basic query
    (message "\n=== Test 1: SELECT * FROM customers ===")
    (let* ((result (mysql-query conn "SELECT * FROM customers"))
           (columns (mysql-result-columns result))
           (rows (mysql-result-rows result))
           (col-names (data-lens--column-names columns)))
      (message "Columns (%d): %s"
               (length col-names) (mapconcat #'identity col-names ", "))
      (message "Rows: %d" (length rows))

      ;; Test 2: column widths
      (message "\n=== Test 2: compute-column-widths ===")
      (let ((widths (data-lens--compute-column-widths col-names rows columns)))
        (message "Widths: %s" widths)

        ;; Test 3: column pages at width=80
        (message "\n=== Test 3: column pages (w=80) ===")
        (let ((pages (data-lens--compute-column-pages widths nil 80)))
          (message "Pages: %d" (length pages))
          (dotimes (i (length pages))
            (message "  Page %d: %s" (1+ i) (aref pages i))))

        ;; Test 4: column pages at width=120
        (message "\n=== Test 4: column pages (w=120) ===")
        (let ((pages (data-lens--compute-column-pages widths nil 120)))
          (message "Pages: %d" (length pages))
          (dotimes (i (length pages))
            (message "  Page %d: %s" (1+ i) (aref pages i))))

        ;; Test 5: long field detection
        (message "\n=== Test 5: long field detection ===")
        (dolist (col columns)
          (when (data-lens--long-field-type-p col)
            (message "  Long: %s (type=%d)" (plist-get col :name) (plist-get col :type))))

        ;; Test 6: render single row
        (message "\n=== Test 6: render-row ===")
        (let* ((data-lens--result-column-defs columns)
               (data-lens--result-columns col-names)
               (data-lens--pending-edits nil)
               (data-lens--fk-info nil)
               (pages (data-lens--compute-column-pages widths nil 80))
               (visible (append (aref pages 0) nil))
               (rendered (data-lens--render-row (car rows) 0 visible widths)))
          (message "Row: %s" (substring-no-properties rendered)))

        ;; Test 7: full render page 1
        (message "\n=== Test 7: render page 1 (w=80) ===")
        (with-temp-buffer
          (setq-local data-lens--result-columns col-names)
          (setq-local data-lens--result-column-defs columns)
          (setq-local data-lens--result-rows rows)
          (setq-local data-lens--display-offset (length rows))
          (setq-local data-lens--pending-edits nil)
          (setq-local data-lens--fk-info nil)
          (setq-local data-lens--where-filter nil)
          (setq-local data-lens--current-col-page 0)
          (setq-local data-lens--pinned-columns nil)
          (setq-local data-lens--column-widths widths)
          (setq-local data-lens--column-pages
                      (data-lens--compute-column-pages widths nil 80))
          (data-lens--render-result)
          (message "%s" (buffer-string)))

        ;; Test 8: render page 2
        (message "\n=== Test 8: render page 2 ===")
        (with-temp-buffer
          (setq-local data-lens--result-columns col-names)
          (setq-local data-lens--result-column-defs columns)
          (setq-local data-lens--result-rows rows)
          (setq-local data-lens--display-offset (length rows))
          (setq-local data-lens--pending-edits nil)
          (setq-local data-lens--fk-info nil)
          (setq-local data-lens--where-filter nil)
          (setq-local data-lens--current-col-page 1)
          (setq-local data-lens--pinned-columns nil)
          (setq-local data-lens--column-widths widths)
          (setq-local data-lens--column-pages
                      (data-lens--compute-column-pages widths nil 80))
          (data-lens--render-result)
          (message "%s" (buffer-string)))

        ;; Test 9: pinned column
        (message "\n=== Test 9: pin id (col 0), show page 2 ===")
        (with-temp-buffer
          (setq-local data-lens--result-columns col-names)
          (setq-local data-lens--result-column-defs columns)
          (setq-local data-lens--result-rows rows)
          (setq-local data-lens--display-offset (length rows))
          (setq-local data-lens--pending-edits nil)
          (setq-local data-lens--fk-info nil)
          (setq-local data-lens--where-filter nil)
          (setq-local data-lens--current-col-page 1)
          (setq-local data-lens--pinned-columns (list 0))
          (setq-local data-lens--column-widths widths)
          (setq-local data-lens--column-pages
                      (data-lens--compute-column-pages widths (list 0) 80))
          (data-lens--render-result)
          (message "%s" (buffer-string)))))

    ;; Test 10: orders
    (message "\n=== Test 10: orders (16 cols) ===")
    (let* ((result (mysql-query conn "SELECT * FROM orders"))
           (columns (mysql-result-columns result))
           (rows (mysql-result-rows result))
           (col-names (data-lens--column-names columns))
           (widths (data-lens--compute-column-widths col-names rows columns))
           (pages (data-lens--compute-column-pages widths nil 80)))
      (message "Columns: %d, Pages: %d" (length col-names) (length pages))
      (with-temp-buffer
        (setq-local data-lens--result-columns col-names)
        (setq-local data-lens--result-column-defs columns)
        (setq-local data-lens--result-rows rows)
        (setq-local data-lens--display-offset (length rows))
        (setq-local data-lens--pending-edits nil)
        (setq-local data-lens--fk-info nil)
        (setq-local data-lens--where-filter nil)
        (setq-local data-lens--current-col-page 0)
        (setq-local data-lens--pinned-columns nil)
        (setq-local data-lens--column-widths widths)
        (setq-local data-lens--column-pages pages)
        (data-lens--render-result)
        (message "%s" (buffer-string))))

    ;; Test 11: CSV export
    (message "\n=== Test 11: CSV export ===")
    (let* ((result (mysql-query conn "SELECT id, first_name, last_name, email FROM customers LIMIT 3"))
           (col-names (data-lens--column-names (mysql-result-columns result)))
           (rows (mysql-result-rows result))
           (csv-escape (lambda (val)
                         (let ((s (data-lens--format-value val)))
                           (if (string-match-p "[,\"\n]" s)
                               (format "\"%s\"" (replace-regexp-in-string "\"" "\"\"" s))
                             s)))))
      (message "%s" (mapconcat #'identity col-names ","))
      (dolist (row rows)
        (message "%s" (mapconcat csv-escape row ","))))

    ;; Test 12: order_items with FK
    (message "\n=== Test 12: order_items FK columns ===")
    (let* ((result (mysql-query conn "SELECT * FROM order_items"))
           (columns (mysql-result-columns result))
           (col-names (data-lens--column-names columns))
           (rows (mysql-result-rows result)))
      (with-temp-buffer
        (setq-local data-lens-connection conn)
        (setq-local data-lens--result-columns col-names)
        (setq-local data-lens--result-column-defs columns)
        (setq-local data-lens--result-rows rows)
        (setq-local data-lens--fk-info nil)
        (data-lens--load-fk-info)
        (message "FK info: %s" data-lens--fk-info)))

    (mysql-disconnect conn)
    (message "\n=== ALL TESTS PASSED ===")))

(condition-case err
    (test-mysql)
  (error (message "FAILED: %s" (error-message-string err))))
