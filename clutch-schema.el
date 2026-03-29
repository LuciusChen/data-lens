;;; clutch-schema.el --- Schema and metadata cache state for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Internal schema refresh and metadata cache state loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'subr-x)

(defvar clutch--schema-cache (make-hash-table :test 'equal)
  "Global schema cache keyed by connection key string.")

(defvar clutch--column-details-cache (make-hash-table :test 'equal)
  "Cache for full column details keyed by connection key string.")

(defvar clutch--column-details-status-cache (make-hash-table :test 'equal)
  "Per-connection status for column-detail fetches.")

(defvar clutch--column-details-queue-cache (make-hash-table :test 'equal)
  "Per-connection queue of tables waiting for async column-detail fetch.")

(defvar clutch--column-details-active-cache (make-hash-table :test 'equal)
  "Per-connection active async column-detail fetch as (TABLE . TICKET).")

(defvar clutch--column-details-ticket-counter 0
  "Monotonic counter used to reject stale async column-detail callbacks.")

(defvar clutch--columns-status-cache (make-hash-table :test 'equal)
  "Per-connection status for synchronous column-name loads.")

(defvar clutch--table-comment-cache (make-hash-table :test 'equal)
  "Cache for table comments keyed by connection key string.")

(defvar clutch--help-doc-cache (make-hash-table :test 'equal)
  "Cache for live function docs fetched from the database server.")

(defvar clutch--schema-install-timers (make-hash-table :test 'equal)
  "Idle timers finishing large schema installs keyed by connection key string.")

(defvar clutch--schema-status-cache (make-hash-table :test 'equal)
  "Schema refresh status cache keyed by connection key string.")

(defvar clutch--schema-refresh-ticket-counter 0
  "Monotonic counter used to reject stale async schema refreshes.")

(defvar clutch--schema-refresh-tickets (make-hash-table :test 'equal)
  "Latest schema refresh ticket keyed by connection key string.")

(defvar clutch--object-cache)
(defvar clutch-connection)
(defvar clutch-schema-cache-install-batch-size)

(declare-function clutch--connection-alive-p "clutch" (conn))
(declare-function clutch--connection-key "clutch" (conn))
(declare-function clutch--cancel-object-warmup "clutch-object" (conn &optional key))
(declare-function clutch--schedule-object-warmup "clutch-object" (conn))
(declare-function clutch--refresh-schema-status-ui "clutch" (conn))
(declare-function clutch--run-db-query "clutch" (conn sql))
(declare-function clutch--set-schema-status "clutch" (conn state &optional table-count error-message))

(defun clutch--schema-status-entry (conn)
  "Return schema status plist for CONN, or nil."
  (and conn
       (gethash (clutch--connection-key conn) clutch--schema-status-cache)))

(defun clutch--begin-schema-refresh-ticket (conn)
  "Issue and record a new schema refresh ticket for CONN."
  (let ((ticket (cl-incf clutch--schema-refresh-ticket-counter)))
    (puthash (clutch--connection-key conn) ticket clutch--schema-refresh-tickets)
    ticket))

(defun clutch--schema-refresh-ticket-current-p (conn ticket)
  "Return non-nil when TICKET is still current for CONN."
  (and conn
       (clutch--connection-alive-p conn)
       (eql (gethash (clutch--connection-key conn) clutch--schema-refresh-tickets)
            ticket)))

(defun clutch--columns-status (conn table)
  "Return column-name load status plist for TABLE on CONN, or nil."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--columns-status-cache)))
    (and cache (gethash table cache))))

(defun clutch--set-columns-status (conn table state &optional error-message)
  "Record synchronous column-name load STATE for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--columns-status-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--columns-status-cache)
                      h))))
    (puthash table (list :state state :error error-message) cache)))

(defun clutch--clear-columns-status (conn table)
  "Clear any recorded column-name load status for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--columns-status-cache)))
    (when cache
      (remhash table cache))))

(defun clutch--column-details-status (conn table)
  "Return async column-detail status plist for TABLE on CONN, or nil."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--column-details-status-cache)))
    (and cache (gethash table cache))))

(defun clutch--cached-column-details (conn table)
  "Return cached column details for TABLE on CONN, or nil if not loaded."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--column-details-cache))
         (details (and cache (gethash table cache 'missing))))
    (unless (eq details 'missing) details)))

(defun clutch--set-column-details-status (conn table state
                                              &optional error-message ticket)
  "Record async column-detail STATE for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--column-details-status-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--column-details-status-cache)
                      h))))
    (puthash table (list :state state :error error-message :ticket ticket) cache)))

(defun clutch--clear-column-details-status (conn table)
  "Clear any recorded column-detail status for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--column-details-status-cache)))
    (when cache
      (remhash table cache))))

(defun clutch--begin-column-details-ticket ()
  "Issue a new column-detail ticket."
  (cl-incf clutch--column-details-ticket-counter))

(defun clutch--column-details-ticket-current-p (conn table ticket)
  "Return non-nil when TICKET is still current for TABLE on CONN."
  (and conn
       (clutch--connection-alive-p conn)
       (eql (plist-get (clutch--column-details-status conn table) :ticket)
            ticket)))

(defun clutch--column-details-queue (conn)
  "Return the async column-details queue for CONN."
  (gethash (clutch--connection-key conn) clutch--column-details-queue-cache))

(defun clutch--set-column-details-queue (conn queue)
  "Store async column-details QUEUE for CONN."
  (puthash (clutch--connection-key conn) queue clutch--column-details-queue-cache))

(defun clutch--column-details-active (conn)
  "Return the active async column-details fetch for CONN, or nil."
  (gethash (clutch--connection-key conn) clutch--column-details-active-cache))

(defun clutch--set-column-details-active (conn table ticket)
  "Record TABLE/TICKET as the active async column-details fetch for CONN."
  (puthash (clutch--connection-key conn) (cons table ticket)
           clutch--column-details-active-cache))

(defun clutch--clear-column-details-active (conn)
  "Clear the active async column-details fetch for CONN."
  (remhash (clutch--connection-key conn) clutch--column-details-active-cache))

(defun clutch--cancel-schema-install (conn &optional key)
  "Cancel any pending schema-install timer for CONN or explicit KEY."
  (let ((key (or key (clutch--connection-key conn))))
    (when-let* ((timer (gethash key clutch--schema-install-timers)))
      (cancel-timer timer)
      (remhash key clutch--schema-install-timers))))

(defun clutch--finish-install-schema-cache (conn key schema)
  "Publish installed SCHEMA cache for CONN under KEY."
  (puthash key schema clutch--schema-cache)
  (remhash key clutch--columns-status-cache)
  (remhash key clutch--column-details-cache)
  (remhash key clutch--column-details-status-cache)
  (remhash key clutch--column-details-queue-cache)
  (remhash key clutch--column-details-active-cache)
  (remhash key clutch--table-comment-cache)
  (remhash key clutch--help-doc-cache)
  (clutch--cancel-object-warmup conn)
  (remhash key clutch--object-cache)
  (clutch--set-schema-status conn 'ready (hash-table-count schema))
  (clutch--schedule-object-warmup conn)
  t)

(defun clutch--install-schema-cache-batched (conn table-names key ticket)
  "Install TABLE-NAMES for CONN incrementally using idle timers."
  (let ((schema (make-hash-table :test 'equal))
        (remaining table-names)
        (batch-size (max 1 clutch-schema-cache-install-batch-size)))
    (cl-labels ((step ()
                  (remhash key clutch--schema-install-timers)
                  (when (and conn
                             (clutch--connection-alive-p conn)
                             (or (null ticket)
                                 (clutch--schema-refresh-ticket-current-p conn ticket)))
                    (let ((count 0))
                      (while (and remaining (< count batch-size))
                        (puthash (car remaining) nil schema)
                        (setq remaining (cdr remaining))
                        (cl-incf count))
                      (if remaining
                          (puthash key
                                   (run-with-idle-timer 0 nil #'step)
                                   clutch--schema-install-timers)
                        (clutch--finish-install-schema-cache conn key schema))))))
      (puthash key
               (run-with-idle-timer 0 nil #'step)
               clutch--schema-install-timers))
    t))

(defun clutch--install-schema-cache (conn table-names &optional ticket)
  "Install TABLE-NAMES as the schema cache for CONN.
When TICKET is non-nil, ignore the update unless it is still current."
  (when (and conn
             (clutch--connection-alive-p conn)
             (or (null ticket)
                 (clutch--schema-refresh-ticket-current-p conn ticket)))
    (let* ((key (clutch--connection-key conn))
           (small-p (<= (length table-names) clutch-schema-cache-install-batch-size))
           (schema (and small-p (make-hash-table :test 'equal))))
      (clutch--cancel-schema-install conn)
      (clutch--cancel-object-warmup conn)
      (remhash key clutch--object-cache)
      (if small-p
          (progn
            (dolist (tbl table-names)
              (puthash tbl nil schema))
            (clutch--finish-install-schema-cache conn key schema))
        (clutch--install-schema-cache-batched conn table-names key ticket)))))

(defun clutch--clear-connection-metadata-caches (conn &optional key)
  "Clear schema-scoped metadata caches for CONN.
When KEY is non-nil, clear that cache namespace instead of CONN's current key."
  (let ((key (or key (clutch--connection-key conn))))
    (remhash key clutch--schema-cache)
    (remhash key clutch--columns-status-cache)
    (remhash key clutch--column-details-cache)
    (remhash key clutch--column-details-status-cache)
    (remhash key clutch--column-details-queue-cache)
    (remhash key clutch--column-details-active-cache)
    (remhash key clutch--table-comment-cache)
    (remhash key clutch--help-doc-cache)
    (clutch--cancel-schema-install conn key)
    (clutch--cancel-object-warmup conn key)
    (remhash key clutch--object-cache)
    (remhash key clutch--schema-status-cache)
    (remhash key clutch--schema-refresh-tickets)))

(defun clutch--refresh-schema-cache-async (conn)
  "Refresh schema cache for CONN asynchronously when supported.
Return non-nil when an asynchronous refresh was started."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn)))
    (clutch--set-schema-status conn 'refreshing)
    (clutch-db-refresh-schema-async
     conn
     (lambda (table-names)
       (clutch--install-schema-cache conn table-names ticket))
     (lambda (message)
       (when (clutch--schema-refresh-ticket-current-p conn ticket)
         (clutch--set-schema-status conn 'failed nil message))))))

(defun clutch--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast). Column info is loaded lazily."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn)))
    (clutch--set-schema-status conn 'refreshing)
    (condition-case err
        (let ((table-names (clutch-db-list-tables conn)))
          (clutch--install-schema-cache conn table-names ticket))
      (clutch-db-error
       (clutch--set-schema-status conn 'failed nil (error-message-string err))
       nil)
      (error
      (clutch--set-schema-status conn 'failed nil (error-message-string err))
       nil))))

(defun clutch--prime-schema-cache (conn)
  "Kick off the appropriate schema refresh strategy for CONN."
  (if (clutch-db-eager-schema-refresh-p conn)
      (clutch--refresh-schema-cache conn)
    (unless (clutch--refresh-schema-cache-async conn)
      (clutch--set-schema-status conn 'stale))))

(defun clutch--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA.
Fetches from the backend if not yet cached.  Returns column list."
  (let ((cols (gethash table schema 'missing))
        (status (clutch--columns-status conn table)))
    (unless (eq cols 'missing)
      (or cols
          (unless (eq (plist-get status :state) 'failed)
            (condition-case err
                (let ((col-names (clutch-db-list-columns conn table)))
                  (puthash table col-names schema)
                  (clutch--clear-columns-status conn table)
                  col-names)
              (clutch-db-error
               (clutch--set-columns-status conn table 'failed
                                           (error-message-string err))
               nil)))))))

(defun clutch--ensure-column-details (conn table &optional strict)
  "Return column details for TABLE on CONN, loading lazily if needed.
Returns a list of plists with :name :type :nullable :primary-key :foreign-key,
or nil on error.  When STRICT is non-nil, signal `clutch-db-error' instead of
silently returning nil."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--column-details-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--column-details-cache)
                      h)))
         (cached (gethash table cache 'missing))
         (status (clutch--column-details-status conn table)))
    (if (not (eq cached 'missing))
        cached
      (if (eq (plist-get status :state) 'failed)
          (when strict
            (signal 'clutch-db-error
                    (list (or (plist-get status :error)
                              (format "Failed to load column details for %s"
                                      table)))))
        (condition-case err
            (let ((details (clutch-db-column-details conn table)))
              (puthash table details cache)
              (clutch--clear-column-details-status conn table)
              details)
          (clutch-db-error
           (let ((message (error-message-string err)))
             (clutch--set-column-details-status conn table 'failed message)
             (when strict
               (signal 'clutch-db-error (list message)))
             nil)))))))

(defun clutch--drain-column-details-async (conn)
  "Start the next queued async column-details fetch for CONN."
  (unless (or (clutch--column-details-active conn)
              (not (clutch--connection-alive-p conn)))
    (when-let* ((queue (clutch--column-details-queue conn))
                (table (car queue))
                (ticket (plist-get (clutch--column-details-status conn table) :ticket)))
      (clutch--set-column-details-queue conn (cdr queue))
      (clutch--set-column-details-active conn table ticket)
      (clutch--set-column-details-status conn table 'loading nil ticket)
      (unless (clutch-db-column-details-async
               conn table
               (lambda (details)
                 (when (clutch--column-details-ticket-current-p conn table ticket)
                   (let* ((key (clutch--connection-key conn))
                          (cache (or (gethash key clutch--column-details-cache)
                                     (let ((h (make-hash-table :test 'equal)))
                                       (puthash key h clutch--column-details-cache)
                                       h))))
                     (puthash table details cache)
                     (clutch--clear-column-details-status conn table)
                     (clutch--clear-column-details-active conn)
                     (clutch--refresh-schema-status-ui conn)
                     (clutch--drain-column-details-async conn))))
               (lambda (message)
                 (when (clutch--column-details-ticket-current-p conn table ticket)
                   (clutch--set-column-details-status conn table 'failed
                                                      message ticket)
                   (clutch--clear-column-details-active conn)
                   (clutch--refresh-schema-status-ui conn)
                   (clutch--drain-column-details-async conn))))
        (clutch--set-column-details-status conn table 'failed
                                           "Column detail fetch is unavailable"
                                           ticket)
        (clutch--clear-column-details-active conn)
        (clutch--refresh-schema-status-ui conn)
        (clutch--drain-column-details-async conn)))))

(defun clutch--ensure-column-details-async (conn table)
  "Queue an async column-detail fetch for TABLE on CONN when needed."
  (let ((state (plist-get (clutch--column-details-status conn table) :state))
        (ticket (clutch--begin-column-details-ticket)))
    (unless (or (clutch--cached-column-details conn table)
                (memq state '(queued loading)))
      (clutch--set-column-details-status conn table 'queued nil ticket)
      (clutch--set-column-details-queue
       conn
       (append (clutch--column-details-queue conn) (list table)))
      (clutch--drain-column-details-async conn)
      t)))

(defun clutch--ensure-table-comment (conn table)
  "Return the comment for TABLE on CONN, loading lazily if needed.
Returns a string or nil."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--table-comment-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--table-comment-cache)
                      h)))
         (cached (gethash table cache 'missing)))
    (if (not (eq cached 'missing))
        cached
      (condition-case nil
          (let ((comment (clutch-db-table-comment conn table)))
            (puthash table comment cache)
            comment)
        (clutch-db-error nil)))))

(defun clutch--parse-mysql-help-text (text)
  "Parse a MySQL HELP description TEXT into a (:sig SIG :desc DESC) plist.
Returns nil if TEXT cannot be parsed (no Syntax: section)."
  (let* ((lines (split-string text "\n"))
         (pos   (cl-position-if (lambda (l) (string-match-p "\\`Syntax:" l))
                                lines)))
    (when pos
      (let ((i (1+ pos)) sig desc)
        (while (and (< i (length lines)) (string-empty-p (nth i lines)))
          (cl-incf i))
        (let (sig-lines)
          (while (and (< i (length lines)) (not (string-empty-p (nth i lines))))
            (push (string-trim (nth i lines)) sig-lines)
            (cl-incf i))
          (setq sig (string-join (nreverse sig-lines) " / ")))
        (while (and (< i (length lines)) (string-empty-p (nth i lines)))
          (cl-incf i))
        (let (desc-lines)
          (while (and (< i (length lines))
                      (not (string-empty-p (nth i lines)))
                      (not (string-prefix-p "URL:" (nth i lines))))
            (push (nth i lines) desc-lines)
            (cl-incf i))
          (setq desc (string-join (nreverse desc-lines) " ")))
        (when (and sig (not (string-empty-p sig)))
          (list :sig sig :desc (or desc "")))))))

(defun clutch--mysql-help-query (conn sym)
  "Query MySQL HELP for SYM on CONN and return a doc plist or nil.
Returns nil when the symbol is unrecognised."
  (let* ((result  (clutch--run-db-query conn (format "HELP '%s'" (upcase sym))))
         (columns (clutch-db-result-columns result))
         (rows    (clutch-db-result-rows result)))
    (when (and rows (>= (length columns) 3))
      (pcase-let ((`(,_name ,desc ,_example) (car rows)))
        (when (stringp desc)
          (clutch--parse-mysql-help-text desc))))))

(defun clutch--format-help-doc (doc)
  "Format a DOC plist (:sig SIG :desc DESC) as a propertized eldoc string."
  (let ((sig  (plist-get doc :sig))
        (desc (plist-get doc :desc)))
    (concat (propertize sig 'face 'font-lock-function-name-face)
            (when (and desc (not (string-empty-p desc)))
              (propertize (concat "  — " desc) 'face 'shadow)))))

(defun clutch--ensure-help-doc (conn sym)
  "Return a live HELP eldoc string for SYM from CONN, with caching.
Queries the server on first access; subsequent calls read from cache.
Returns nil when SYM is not a known built-in on this server."
  (let* ((key   (clutch--connection-key conn))
         (cache (or (gethash key clutch--help-doc-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--help-doc-cache)
                      h)))
         (uname (upcase sym))
         (entry (gethash uname cache 'missing)))
    (cond
     ((eq entry 'missing)
      (condition-case nil
          (let ((doc (clutch--mysql-help-query conn sym)))
            (puthash uname (or doc 'not-found) cache)
            (when doc (clutch--format-help-doc doc)))
        (clutch-db-error nil)))
     ((eq entry 'not-found) nil)
     (t (clutch--format-help-doc entry)))))

(defun clutch--schema-for-connection (&optional conn)
  "Return the schema hash-table for CONN, or nil.
When CONN is nil, use `clutch-connection'."
  (let ((conn (or conn clutch-connection)))
    (when (clutch--connection-alive-p conn)
      (gethash (clutch--connection-key conn) clutch--schema-cache))))

(provide 'clutch-schema)

;;; clutch-schema.el ends here
