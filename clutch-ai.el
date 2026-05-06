;;; clutch-ai.el --- Optional AI SQL advisory workflow -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Optional AI-assisted SQL review and optimization helpers for clutch.
;; This module deliberately does not require `llm'.  Users may install llm and
;; set `clutch-ai-provider' to an llm provider object, or set it to a function.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'clutch-connection)
(require 'diff-mode)
(require 'pp)
(require 'seq)
(require 'subr-x)

;;;###autoload
(defgroup clutch-ai nil
  "Optional AI-assisted SQL advice."
  :group 'clutch
  :prefix "clutch-ai-")

;;;###autoload
(defcustom clutch-ai-provider nil
  "Provider used by clutch AI commands.
When nil, AI commands signal a `user-error'.
When a function, it is called with two arguments: PROMPT and CONTEXT.
It may return a string, or a plist with :sql, :notes, and :risk.
Otherwise, when the optional llm package is installed, this value is passed to
`llm-chat' with the generated prompt."
  :type '(choice (const :tag "Disabled" nil)
                 (function :tag "Function")
                 (sexp :tag "llm provider object"))
  :group 'clutch-ai)

;;;###autoload
(defcustom clutch-ai-include-explain nil
  "When non-nil, include a non-ANALYZE EXPLAIN plan in AI SQL context.
This runs an EXPLAIN query against the current connection.  Clutch never runs
EXPLAIN ANALYZE from AI commands."
  :type 'boolean
  :group 'clutch-ai)

;;;###autoload
(defcustom clutch-ai-max-context-chars 24000
  "Maximum characters of schema and plan context sent to the AI provider."
  :type 'integer
  :group 'clutch-ai)

;;;###autoload
(defcustom clutch-ai-schema-hints-file
  (expand-file-name "clutch/schema-hints.el" user-emacs-directory)
  "File storing clutch schema hints used by optional AI workflows.
Hints are keyed by `:clutch-schema-id' when present in connection params,
falling back to the saved console name and then the connection key."
  :type 'file
  :group 'clutch-ai)

(defvar-local clutch-ai--review-source-buffer nil)
(defvar-local clutch-ai--review-source-beg nil)
(defvar-local clutch-ai--review-source-end nil)
(defvar-local clutch-ai--review-original-sql nil)
(defvar-local clutch-ai--review-candidate-sql nil)

(defvar clutch-connection)
(defvar clutch--connection-params)
(defvar clutch--conn-sql-product)
(defvar clutch--console-name)
(defvar clutch--last-query)

(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--dwim-bounds-at-point "clutch-query" ())
(declare-function clutch--ensure-connection "clutch-connection" ())
(declare-function clutch--run-db-query "clutch-connection" (conn sql))
(declare-function clutch--statement-table-identifiers "clutch" ())
(declare-function clutch-result--effective-query "clutch-query" ())

(defconst clutch-ai--schema-hints-template
  ";; -*- mode: emacs-lisp; lexical-binding: t; -*-
;; Clutch schema hints.
;;
;; This file is data, not code.  Entries are keyed by :clutch-schema-id when
;; configured on a saved connection, falling back to the console name or
;; connection key.
;;
;; Example:
;; '((\"app-prod\"
;;    :relationships
;;    (((:table \"orders\" :column \"user_id\")
;;      (:table \"users\" :column \"id\")
;;      :confidence user-confirmed
;;      :note \"Logical FK; database has no declared constraint\"))))

nil
")

(defun clutch-ai--load-schema-hints ()
  "Return schema hints from `clutch-ai-schema-hints-file', or nil."
  (if (not (file-readable-p clutch-ai-schema-hints-file))
      nil
    (condition-case err
        (with-temp-buffer
          (insert-file-contents clutch-ai-schema-hints-file)
          (goto-char (point-min))
          (let ((value (read (current-buffer))))
            (unless (or (null value) (listp value))
              (user-error "Invalid schema hints file: expected a list"))
            value))
      (end-of-file nil)
      (error
       (user-error "Failed to read schema hints from %s: %s"
                   clutch-ai-schema-hints-file
                   (error-message-string err))))))

(defun clutch-ai--save-schema-hints (hints)
  "Persist schema HINTS to `clutch-ai-schema-hints-file'."
  (make-directory (file-name-directory clutch-ai-schema-hints-file) t)
  (with-temp-file clutch-ai-schema-hints-file
    (insert ";; -*- mode: emacs-lisp; lexical-binding: t; -*-\n")
    (insert ";; Clutch schema hints.  This file is data, not code.\n\n")
    (pp hints (current-buffer))))

(defun clutch-ai--schema-hints-key (&optional conn)
  "Return the stable schema-hints key for CONN or current buffer."
  (or (plist-get clutch--connection-params :clutch-schema-id)
      clutch--console-name
      (and (or conn clutch-connection)
           (clutch--connection-key (or conn clutch-connection)))
      (user-error "No connection available for schema hints")))

(defun clutch-ai--schema-hints-for-key (key &optional hints)
  "Return schema hints plist for KEY from HINTS."
  (cdr (assoc key (or hints (clutch-ai--load-schema-hints)))))

(defun clutch-ai--schema-hints-for-connection (&optional conn)
  "Return schema hints plist for CONN or current buffer."
  (clutch-ai--schema-hints-for-key (clutch-ai--schema-hints-key conn)))

(defun clutch-ai--logical-relationships-for-connection (&optional conn)
  "Return logical relationships for CONN or current buffer."
  (plist-get (clutch-ai--schema-hints-for-connection conn) :relationships))

(defun clutch-ai--relationship-endpoint-equal-p (a b)
  "Return non-nil when relationship endpoints A and B match."
  (and (string= (downcase (or (plist-get a :table) ""))
                (downcase (or (plist-get b :table) "")))
       (string= (downcase (or (plist-get a :column) ""))
                (downcase (or (plist-get b :column) "")))))

(defun clutch-ai--relationship-same-p (a b)
  "Return non-nil when relationships A and B identify the same columns."
  (and (clutch-ai--relationship-endpoint-equal-p (nth 0 a) (nth 0 b))
       (clutch-ai--relationship-endpoint-equal-p (nth 1 a) (nth 1 b))))

(defun clutch-ai--put-logical-relationship (key relationship)
  "Store RELATIONSHIP under schema hints KEY.
Updates an existing matching relationship instead of appending a duplicate.
Return non-nil when an existing relationship was updated."
  (let* ((hints (clutch-ai--load-schema-hints))
         (entry (assoc key hints))
         (plist (copy-sequence (cdr entry)))
         (relationships (copy-sequence (plist-get plist :relationships)))
         (updated nil))
    (setq relationships
          (mapcar (lambda (existing)
                    (if (clutch-ai--relationship-same-p existing relationship)
                        (progn
                          (setq updated t)
                          relationship)
                      existing))
                  relationships))
    (unless updated
      (setq relationships (append relationships (list relationship))))
    (setq plist (plist-put plist :relationships relationships))
    (if entry
        (setcdr entry plist)
      (setq hints (append hints (list (cons key plist)))))
    (clutch-ai--save-schema-hints hints)
    updated))

(defun clutch-ai--relationship-confirmed-p (relationship)
  "Return non-nil when RELATIONSHIP is user-confirmed."
  (eq (plist-get (nthcdr 2 relationship) :confidence) 'user-confirmed))

(defun clutch-ai--relationship-relevant-p (relationship tables)
  "Return non-nil when RELATIONSHIP involves one of TABLES."
  (or (null tables)
      (let ((from-table (plist-get (nth 0 relationship) :table))
            (to-table (plist-get (nth 1 relationship) :table)))
        (or (member-ignore-case from-table tables)
            (member-ignore-case to-table tables)))))

(defun clutch-ai--format-logical-relationship (relationship)
  "Return advisory context text for RELATIONSHIP."
  (let ((from (nth 0 relationship))
        (to (nth 1 relationship))
        (props (nthcdr 2 relationship)))
    (string-trim
     (format "- %s.%s -> %s.%s [%s]%s"
             (plist-get from :table)
             (plist-get from :column)
             (plist-get to :table)
             (plist-get to :column)
             (or (plist-get props :confidence) "unknown")
             (if-let* ((note (plist-get props :note))
                       ((not (string-empty-p note))))
                 (format " %s" note)
               "")))))

(defun clutch-ai--logical-relationships-context (conn tables)
  "Return logical relationship context for CONN filtered by TABLES."
  (let ((relationships
         (seq-filter
          (lambda (relationship)
            (and (clutch-ai--relationship-confirmed-p relationship)
                 (clutch-ai--relationship-relevant-p relationship tables)))
          (clutch-ai--logical-relationships-for-connection conn))))
    (when relationships
      (concat "## Logical relationships from clutch schema hints\n"
              "These are user-confirmed relationships not necessarily declared in the database.\n"
              (mapconcat #'clutch-ai--format-logical-relationship
                         relationships
                         "\n")))))

(defun clutch-ai--source-sql ()
  "Return current SQL advisory source as a plist."
  (cond
   ((derived-mode-p 'clutch-result-mode)
    (let ((sql (clutch-result--effective-query)))
      (unless (and sql (not (string-empty-p (string-trim sql))))
        (user-error "No result query to optimize"))
      (list :sql (string-trim sql))))
   ((use-region-p)
    (let ((sql (string-trim (buffer-substring-no-properties
                             (region-beginning) (region-end)))))
      (when (string-empty-p sql)
        (user-error "No SQL in region"))
      (list :sql sql
            :source-buffer (current-buffer)
            :beg (region-beginning)
            :end (region-end))))
   (t
    (pcase-let* ((`(,beg . ,end) (clutch--dwim-bounds-at-point))
                 (sql (string-trim (buffer-substring-no-properties beg end))))
      (when (string-empty-p sql)
        (user-error "No SQL at point"))
      (list :sql sql :source-buffer (current-buffer) :beg beg :end end)))))

(defun clutch-ai--safe-string (fn &rest args)
  "Call FN with ARGS and return its string result, or nil on failure."
  (condition-case nil
      (let ((value (apply fn args)))
        (and (stringp value) (not (string-empty-p value)) value))
    (error nil)))

(defun clutch-ai--format-columns (conn table)
  "Return column metadata text for TABLE on CONN."
  (when-let* ((columns (condition-case nil
                          (clutch-db-list-columns conn table)
                        (error nil))))
    (mapconcat
     (lambda (col)
       (if (plist-get col :name)
           (format "- %s %s%s%s%s"
                   (plist-get col :name)
                   (or (plist-get col :type) "")
                   (if (eq (plist-get col :nullable) nil) " NOT NULL" "")
                   (if (plist-get col :primary-key) " PRIMARY KEY" "")
                   (if-let* ((default (plist-get col :default)))
                       (format " DEFAULT %s" default)
                     ""))
         (format "- %s" col)))
     columns
     "\n")))

(defun clutch-ai--index-entries-for-table (conn table)
  "Return index entries for TABLE on CONN."
  (seq-filter
   (lambda (entry)
     (string= (downcase (or (plist-get entry :target-table) ""))
              (downcase table)))
   (condition-case nil
       (clutch-db-list-objects conn 'indexes)
     (error nil))))

(defun clutch-ai--format-index (conn entry)
  "Return advisory text for index ENTRY on CONN."
  (or (clutch-ai--safe-string #'clutch-db-show-create-object conn entry)
      (let* ((details (condition-case nil
                          (clutch-db-object-details conn entry)
                        (error nil)))
             (columns (and details
                           (mapconcat
                            (lambda (col)
                              (format "%s %s"
                                      (or (plist-get col :name) "")
                                      (or (plist-get col :descend) "ASC")))
                            details
                            ", "))))
        (string-trim
         (format "%s%s%s"
                 (or (plist-get entry :name) "")
                 (if (eq (plist-get entry :unique) t) " UNIQUE" "")
                 (if columns (format " (%s)" columns) ""))))))

(defun clutch-ai--table-context (conn table)
  "Return schema and index context for TABLE on CONN."
  (string-join
   (delq nil
         (list
          (format "## Table: %s" table)
          (or (clutch-ai--safe-string #'clutch-db-show-create-table conn table)
              (when-let* ((columns (clutch-ai--format-columns conn table)))
                (format "Columns:\n%s" columns)))
          (when-let* ((indexes (clutch-ai--index-entries-for-table conn table)))
            (concat "Indexes:\n"
                    (mapconcat (lambda (entry)
                                 (concat "- " (clutch-ai--format-index conn entry)))
                               indexes
                               "\n")))))
   "\n\n"))

(defun clutch-ai--select-like-p (sql)
  "Return non-nil when SQL looks safe for plain EXPLAIN."
  (string-match-p "\\`[ \t\n\r]*(?\\s-*\\(select\\|with\\)\\b" sql))

(defun clutch-ai--format-result (result)
  "Return a compact text rendering of clutch-db RESULT."
  (let* ((columns (mapcar (lambda (col)
                            (or (plist-get col :name) (format "%s" col)))
                          (clutch-db-result-columns result)))
         (rows (clutch-db-result-rows result)))
    (when rows
      (string-join
       (cons (string-join columns "\t")
             (mapcar (lambda (row)
                       (mapconcat (lambda (cell)
                                    (if cell (format "%s" cell) "NULL"))
                                  row
                                  "\t"))
                     rows))
       "\n"))))

(defun clutch-ai--explain-context (conn sql)
  "Return EXPLAIN context for SQL on CONN when available."
  (when (and clutch-ai-include-explain
             (clutch-ai--select-like-p sql))
    (condition-case err
        (when-let* ((text (clutch-ai--format-result
                           (clutch--run-db-query conn (concat "EXPLAIN " sql)))))
          (concat "## EXPLAIN\n" text))
      (error (format "## EXPLAIN\nUnavailable: %s" (error-message-string err))))))

(defun clutch-ai--sql-tables (source-buffer)
  "Return table identifiers from SOURCE-BUFFER."
  (when source-buffer
    (with-current-buffer source-buffer
      (condition-case nil
          (clutch--statement-table-identifiers)
        (error nil)))))

(defun clutch-ai--tables-from-sql (sql)
  "Return table identifiers parsed from SQL text."
  (with-temp-buffer
    (insert sql)
    (goto-char (point-min))
    (condition-case nil
        (clutch--statement-table-identifiers)
      (error nil))))

(defun clutch-ai--context (source)
  "Build AI context for SOURCE plist."
  (clutch--ensure-connection)
  (let* ((conn clutch-connection)
         (sql (plist-get source :sql))
         (tables (delete-dups
                  (copy-sequence
                   (or (clutch-ai--sql-tables (plist-get source :source-buffer))
                       (clutch-ai--tables-from-sql sql)
                       '()))))
         (parts
          (delq nil
                (append
                 (list
                  (format "Connection: %s" (clutch--connection-key conn))
                 (format "Backend: %s" (or (clutch--backend-key-from-conn conn) "unknown"))
                  (format "SQL product: %s" (or clutch--conn-sql-product "unknown"))
                  (format "Schema hints key: %s" (clutch-ai--schema-hints-key conn))
                  "No row data or sample values are included by default."
                  "## SQL"
                  sql)
                 (list (clutch-ai--logical-relationships-context conn tables))
                 (mapcar (lambda (table)
                           (clutch-ai--table-context conn table))
                         tables)
                 (list (clutch-ai--explain-context conn sql))))))
    (let ((context (string-join parts "\n\n")))
      (if (> (length context) clutch-ai-max-context-chars)
          (concat (substring context 0 clutch-ai-max-context-chars)
                  "\n\n[Context truncated by clutch-ai-max-context-chars]")
        context))))

(defun clutch-ai--prompt (context)
  "Return the SQL optimization prompt for CONTEXT."
  (concat
   "You are helping optimize a database SQL query from Emacs clutch.\n"
   "Use only the provided SQL, schema, indexes, and optional EXPLAIN plan.\n"
   "Do not invent columns, tables, constraints, indexes, or data distributions.\n"
   "Do not request or assume row samples unless the context explicitly includes them.\n"
   "Return exactly these sections:\n"
   "Optimized SQL:\n```sql\n...\n```\n"
   "Notes:\n- ...\n"
   "Risks:\n- ...\n\n"
   context))

(defun clutch-ai--call-provider (prompt context)
  "Call `clutch-ai-provider' with PROMPT and CONTEXT."
  (cond
   ((null clutch-ai-provider)
    (user-error "Set `clutch-ai-provider' to a function or llm provider first"))
   ((functionp clutch-ai-provider)
    (funcall clutch-ai-provider prompt context))
   ((and (require 'llm nil t)
         (fboundp 'llm-chat))
    (llm-chat clutch-ai-provider prompt))
   (t
    (user-error "Optional llm package is not available"))))

(defun clutch-ai--extract-fenced-sql (text)
  "Extract first fenced SQL block from TEXT, or nil."
  (when (string-match "```\\(?:sql\\|SQL\\)?[ \t\n\r]*\\(\\(?:.\\|\n\\)*?\\)[ \t\n\r]*```" text)
    (string-trim (match-string 1 text))))

(defun clutch-ai--response-sql (response)
  "Return candidate SQL from provider RESPONSE."
  (cond
   ((and (listp response) (plist-get response :sql))
    (string-trim (plist-get response :sql)))
   ((stringp response)
    (or (clutch-ai--extract-fenced-sql response)
        (string-trim response)))
   (t
    (user-error "AI provider returned unsupported response: %S" response))))

(defun clutch-ai--response-notes (response)
  "Return explanatory text from provider RESPONSE."
  (cond
   ((and (listp response)
         (or (plist-get response :notes) (plist-get response :risk)))
    (string-join
     (delq nil
           (list (when-let* ((notes (plist-get response :notes)))
                   (format "Notes:\n%s" notes))
                 (when-let* ((risk (plist-get response :risk)))
                   (format "Risks:\n%s" risk))))
     "\n\n"))
   ((stringp response)
    response)
   (t "")))

(defun clutch-ai--line-list (text)
  "Return TEXT as a list of lines, preserving a final empty line when present."
  (split-string text "\n"))

(defun clutch-ai--simple-unified-diff (old new)
  "Return a small unified diff between OLD and NEW.
This fallback is intentionally conservative; it gives review visibility without
depending on an external diff executable."
  (let ((old-lines (clutch-ai--line-list old))
        (new-lines (clutch-ai--line-list new)))
    (concat "--- original.sql\n"
            "+++ optimized.sql\n"
            "@@\n"
            (mapconcat (lambda (line) (concat "-" line)) old-lines "\n")
            "\n"
            (mapconcat (lambda (line) (concat "+" line)) new-lines "\n")
            "\n")))

(defun clutch-ai--unified-diff (old new)
  "Return a unified diff between OLD and NEW."
  (condition-case nil
      (let ((old-file (make-temp-file "clutch-ai-old-" nil ".sql"))
            (new-file (make-temp-file "clutch-ai-new-" nil ".sql")))
        (unwind-protect
            (progn
              (write-region old nil old-file nil 'silent)
              (write-region new nil new-file nil 'silent)
              (with-current-buffer
                  (diff-no-select old-file new-file "-u" t)
                (prog1 (buffer-string)
                  (kill-buffer (current-buffer)))))
          (when (file-exists-p old-file)
            (delete-file old-file))
          (when (file-exists-p new-file)
            (delete-file new-file))))
    (error (clutch-ai--simple-unified-diff old new))))

(defvar clutch-ai-review-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map diff-mode-map)
    (define-key map (kbd "a") #'clutch-ai-review-accept)
    (define-key map (kbd "C-c C-c") #'clutch-ai-review-accept)
    (define-key map (kbd "q") #'quit-window)
    map)
  "Keymap for `clutch-ai-review-mode'.")

(define-derived-mode clutch-ai-review-mode diff-mode "clutch-ai-review"
  "Major mode for reviewing AI SQL optimization diffs.")

(defun clutch-ai--show-review (source candidate notes)
  "Show review buffer for SOURCE, CANDIDATE SQL, and NOTES."
  (let ((buf (get-buffer-create "*clutch-ai-sql-review*"))
        (original (plist-get source :sql)))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert "AI SQL Review\n")
        (insert "=============\n\n")
        (insert "Press a or C-c C-c to accept.  Press q to quit without changes.\n\n")
        (when (and notes (not (string-empty-p (string-trim notes))))
          (insert (string-trim notes) "\n\n"))
        (insert (clutch-ai--unified-diff original candidate))
        (goto-char (point-min))
        (clutch-ai-review-mode)
        (setq-local clutch-ai--review-source-buffer (plist-get source :source-buffer)
                    clutch-ai--review-source-beg (plist-get source :beg)
                    clutch-ai--review-source-end (plist-get source :end)
                    clutch-ai--review-original-sql original
                    clutch-ai--review-candidate-sql candidate)
        (setq buffer-read-only t)))
    (pop-to-buffer buf)))

;;;###autoload
(defun clutch-ai-add-logical-relationship (from-table from-column to-table to-column note)
  "Add a user-confirmed logical relationship for the current connection."
  (interactive
   (progn
     (clutch--ensure-connection)
     (list (read-string "From table: ")
           (read-string "From column: ")
           (read-string "To table: ")
           (read-string "To column: ")
           (read-string "Note (optional): "))))
  (clutch--ensure-connection)
  (let* ((key (clutch-ai--schema-hints-key clutch-connection))
         (relationship
          (list (list :table from-table :column from-column)
                (list :table to-table :column to-column)
                :confidence 'user-confirmed
                :note note))
         (updated (clutch-ai--put-logical-relationship key relationship)))
    (message "%s logical relationship for %s"
             (if updated "Updated" "Added")
             key)))

;;;###autoload
(defun clutch-ai-edit-schema-hints ()
  "Open `clutch-ai-schema-hints-file' for manual editing."
  (interactive)
  (unless (file-exists-p clutch-ai-schema-hints-file)
    (make-directory (file-name-directory clutch-ai-schema-hints-file) t)
    (with-temp-file clutch-ai-schema-hints-file
      (insert clutch-ai--schema-hints-template)))
  (find-file clutch-ai-schema-hints-file))

;;;###autoload
(defun clutch-ai-review-accept ()
  "Accept the SQL candidate in the current AI review buffer."
  (interactive)
  (unless (and clutch-ai--review-source-buffer
               (buffer-live-p clutch-ai--review-source-buffer)
               clutch-ai--review-source-beg
               clutch-ai--review-source-end)
    (user-error "This review has no editable source SQL"))
  (let ((candidate clutch-ai--review-candidate-sql)
        (source-buffer clutch-ai--review-source-buffer)
        (beg clutch-ai--review-source-beg)
        (end clutch-ai--review-source-end))
    (with-current-buffer source-buffer
      (save-excursion
        (goto-char beg)
        (delete-region beg end)
        (insert candidate)))
    (message "Applied optimized SQL")
    (quit-window)))

;;;###autoload
(defun clutch-ai-optimize-sql ()
  "Ask an optional AI provider to optimize current SQL and review the diff.
The command sends SQL plus relevant table schema/index metadata.  It does not
send row data by default and never applies AI output without explicit review
acceptance."
  (interactive)
  (let* ((source (clutch-ai--source-sql))
         (context (clutch-ai--context source))
         (prompt (clutch-ai--prompt context))
         (response (clutch-ai--call-provider prompt context))
         (candidate (clutch-ai--response-sql response)))
    (when (string-empty-p candidate)
      (user-error "AI provider returned empty SQL"))
    (clutch-ai--show-review source candidate (clutch-ai--response-notes response))))

(provide 'clutch-ai)
;;; clutch-ai.el ends here
