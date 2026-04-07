;;; clutch-schema.el --- Schema refresh and metadata caches -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

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

(defvar clutch--columns-ticket-counter 0
  "Monotonic counter used to reject stale async column-name callbacks.")

(defvar clutch--columns-status-cache (make-hash-table :test 'equal)
  "Per-connection status for synchronous column-name loads.")

(defvar clutch--table-comment-cache (make-hash-table :test 'equal)
  "Cache for table comments keyed by connection key string.")

(defvar clutch--table-comment-status-cache (make-hash-table :test 'equal)
  "Per-connection status for async table-comment fetches.")

(defvar clutch--table-comment-ticket-counter 0
  "Monotonic counter used to reject stale async table-comment callbacks.")

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
(defvar clutch-debug-mode nil)
(defvar clutch-schema-cache-install-batch-size)

(declare-function clutch--connection-alive-p "clutch-connection" (conn))
(declare-function clutch--backend-key-from-conn "clutch-connection" (conn))
(declare-function clutch--connection-key "clutch-connection" (conn))
(declare-function clutch--humanize-db-error "clutch-query" (msg))
(declare-function clutch--invalidate-object-warmup "clutch-object" (conn &optional key))
(declare-function clutch--remember-debug-event "clutch" (&rest event))
(declare-function clutch--remember-problem-record "clutch" (&rest args))
(declare-function clutch--cached-columns "clutch" (schema table))
(declare-function clutch--refresh-result-metadata-buffers "clutch-ui" (conn table))
(declare-function clutch--schedule-object-warmup "clutch-object" (conn))
(declare-function clutch--refresh-schema-status-ui "clutch" (conn))
(declare-function clutch--run-db-query "clutch-connection" (conn sql))
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

(defun clutch--set-columns-status (conn table state &optional error-message ticket)
  "Record synchronous column-name load STATE for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--columns-status-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--columns-status-cache)
                      h))))
    (puthash table (list :state state :error error-message :ticket ticket) cache)))

(defun clutch--clear-columns-status (conn table)
  "Clear any recorded column-name load status for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--columns-status-cache)))
    (when cache
      (remhash table cache))))

(defun clutch--begin-columns-ticket ()
  "Issue a new column-name ticket."
  (cl-incf clutch--columns-ticket-counter))

(defun clutch--columns-ticket-current-p (conn table ticket)
  "Return non-nil when TICKET is still current for TABLE on CONN."
  (and conn
       (clutch--connection-alive-p conn)
       (eql (plist-get (clutch--columns-status conn table) :ticket)
            ticket)))

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

(defun clutch--table-comment-status (conn table)
  "Return async table-comment status plist for TABLE on CONN, or nil."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--table-comment-status-cache)))
    (and cache (gethash table cache))))

(defun clutch--cached-table-comment (conn table)
  "Return cached table comment for TABLE on CONN, or nil if not loaded."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--table-comment-cache))
         (comment (and cache (gethash table cache 'missing))))
    (unless (eq comment 'missing) comment)))

(defun clutch--table-comment-cached-p (conn table)
  "Return non-nil when TABLE has a cached comment entry on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--table-comment-cache)))
    (and cache
         (not (eq (gethash table cache 'missing) 'missing)))))

(defun clutch--set-table-comment-status (conn table state
                                              &optional error-message ticket)
  "Record async table-comment STATE for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (or (gethash key clutch--table-comment-status-cache)
                    (let ((h (make-hash-table :test 'equal)))
                      (puthash key h clutch--table-comment-status-cache)
                      h))))
    (puthash table (list :state state :error error-message :ticket ticket) cache)))

(defun clutch--clear-table-comment-status (conn table)
  "Clear any recorded table-comment status for TABLE on CONN."
  (let* ((key (clutch--connection-key conn))
         (cache (gethash key clutch--table-comment-status-cache)))
    (when cache
      (remhash table cache))))

(defun clutch--begin-table-comment-ticket ()
  "Issue a new table-comment ticket."
  (cl-incf clutch--table-comment-ticket-counter))

(defun clutch--table-comment-ticket-current-p (conn table ticket)
  "Return non-nil when TICKET is still current for TABLE on CONN."
  (and conn
       (clutch--connection-alive-p conn)
       (eql (plist-get (clutch--table-comment-status conn table) :ticket)
            ticket)))

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
  (remhash key clutch--table-comment-status-cache)
  (remhash key clutch--help-doc-cache)
  (clutch--invalidate-object-warmup conn)
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
      (clutch--invalidate-object-warmup conn)
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
    (remhash key clutch--table-comment-status-cache)
    (remhash key clutch--help-doc-cache)
    (clutch--cancel-schema-install conn key)
    (clutch--invalidate-object-warmup conn key)
    (remhash key clutch--object-cache)
    (remhash key clutch--schema-status-cache)
    (remhash key clutch--schema-refresh-tickets)))

(defun clutch--refresh-schema-cache-async (conn)
  "Refresh schema cache for CONN asynchronously when supported.
Return non-nil when an asynchronous refresh was started."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn))
        (backend (when clutch-debug-mode
                   (condition-case nil
                       (clutch--backend-key-from-conn conn)
                     (error nil)))))
    (clutch--set-schema-status conn 'refreshing)
    (let ((started
           (clutch-db-refresh-schema-async
            conn
            (lambda (table-names)
              (if (clutch--schema-refresh-ticket-current-p conn ticket)
                  (progn
                    (when clutch-debug-mode
                      (clutch--remember-debug-event
                       :connection conn
                       :op "schema-refresh"
                       :phase "success"
                       :backend backend
                       :summary (format "Loaded %d tables" (length table-names))))
                    (clutch--install-schema-cache conn table-names ticket))
                (when clutch-debug-mode
                  (clutch--remember-debug-event
                   :connection conn
                   :op "schema-refresh"
                   :phase "stale-drop"
                   :backend backend
                   :summary "Ignored stale schema refresh result"))))
            (lambda (message)
              (when (clutch--schema-refresh-ticket-current-p conn ticket)
                (clutch--set-schema-status conn 'failed nil message)
                (clutch--remember-problem-record
                 :connection conn
                 :problem (list :backend (clutch--backend-key-from-conn conn)
                                :summary (clutch--humanize-db-error message)
                                :diag (list :category "metadata"
                                            :op "schema-refresh"
                                            :raw-message message)))
                (when clutch-debug-mode
                  (clutch--remember-debug-event
                   :connection conn
                   :op "schema-refresh"
                   :phase "error"
                   :backend backend
                   :summary message)))
              (unless (clutch--schema-refresh-ticket-current-p conn ticket)
                (when clutch-debug-mode
                  (clutch--remember-debug-event
                   :connection conn
                   :op "schema-refresh"
                   :phase "stale-drop"
                   :backend backend
                   :summary "Ignored stale schema refresh error")))))))
      (when (and started clutch-debug-mode)
        (clutch--remember-debug-event
         :connection conn
         :op "schema-refresh"
         :phase "submit"
         :backend backend
         :summary "Queued background schema refresh"))
      started)))

(defun clutch--refresh-schema-cache (conn)
  "Refresh schema cache for CONN.
Only loads table names (fast).  Column info is loaded lazily."
  (let ((ticket (clutch--begin-schema-refresh-ticket conn)))
    (clutch--set-schema-status conn 'refreshing)
    (condition-case err
        (let ((table-names (clutch-db-list-tables conn)))
          (prog1
              (clutch--install-schema-cache conn table-names ticket)
            (when clutch-debug-mode
              (clutch--remember-debug-event
               :connection conn
               :op "schema-refresh"
               :phase "success"
               :backend (clutch--backend-key-from-conn conn)
               :summary (format "Loaded %d tables" (length table-names))))))
      (clutch-db-error
       (clutch--set-schema-status conn 'failed nil (error-message-string err))
       (clutch--remember-problem-record
        :connection conn
        :problem (list :backend (clutch--backend-key-from-conn conn)
                       :summary (clutch--humanize-db-error
                                 (error-message-string err))
                       :diag (list :category "metadata"
                                   :op "schema-refresh"
                                   :raw-message (error-message-string err))))
       (when clutch-debug-mode
         (clutch--remember-debug-event
          :connection conn
          :op "schema-refresh"
          :phase "error"
          :backend (clutch--backend-key-from-conn conn)
          :summary (error-message-string err)))
       nil)
      (error
       (clutch--set-schema-status conn 'failed nil (error-message-string err))
       (clutch--remember-problem-record
        :connection conn
        :problem (list :backend (clutch--backend-key-from-conn conn)
                       :summary (clutch--humanize-db-error
                                 (error-message-string err))
                       :diag (list :category "metadata"
                                   :op "schema-refresh"
                                   :raw-message (error-message-string err))))
       (when clutch-debug-mode
         (clutch--remember-debug-event
          :connection conn
          :op "schema-refresh"
          :phase "error"
          :backend (clutch--backend-key-from-conn conn)
          :summary (error-message-string err)))
       nil))))

(defun clutch--prime-schema-cache (conn)
  "Kick off the appropriate schema refresh strategy for CONN."
  (if (clutch-db-eager-schema-refresh-p conn)
      (clutch--refresh-schema-cache conn)
    (unless (clutch--refresh-schema-cache-async conn)
      (clutch--refresh-schema-cache conn))))

(defun clutch--ensure-columns (conn schema table)
  "Ensure column info for TABLE is loaded in SCHEMA for CONN.
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

(defun clutch--ensure-columns-async (conn schema table)
  "Queue an async column-name fetch for TABLE in SCHEMA on CONN when needed."
  (let ((state (plist-get (clutch--columns-status conn table) :state))
        (ticket (clutch--begin-columns-ticket))
        (backend (when clutch-debug-mode
                   (condition-case nil
                       (clutch--backend-key-from-conn conn)
                     (error nil)))))
      (unless (or (clutch--cached-columns schema table)
                  (memq state '(queued loading)))
      (clutch--set-columns-status conn table 'loading nil ticket)
      (let ((started
             (clutch-db-list-columns-async
              conn table
              (lambda (columns)
                (if (clutch--columns-ticket-current-p conn table ticket)
                    (progn
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "list-columns"
                         :phase "success"
                         :backend backend
                         :summary (format "Loaded %d column names for %s"
                                          (length columns) table)
                         :context (list :table table)))
                      (when-let* ((live-schema (gethash (clutch--connection-key conn)
                                                       clutch--schema-cache)))
                        (puthash table columns live-schema))
                      (clutch--clear-columns-status conn table)
                      (clutch--refresh-schema-status-ui conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "list-columns"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale column-name result for %s" table)
                     :context (list :table table)))))
              (lambda (message)
                (if (clutch--columns-ticket-current-p conn table ticket)
                    (progn
                      (clutch--set-columns-status conn table 'failed message ticket)
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "list-columns"
                         :phase "error"
                         :backend backend
                         :summary message
                         :context (list :table table)))
                      (clutch--refresh-schema-status-ui conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "list-columns"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale column-name error for %s" table)
                     :context (list :table table))))))))
        (when (and started clutch-debug-mode)
          (clutch--remember-debug-event
           :connection conn
           :op "list-columns"
           :phase "submit"
           :backend backend
           :summary (format "Queued background column-name preheat for %s" table)
           :context (list :table table)))
        (unless started
        (clutch--ensure-columns conn schema table)
          (clutch--refresh-schema-status-ui conn)))
      t)))

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
            (let* ((message (or (plist-get status :error)
                                (format "Failed to load column details for %s"
                                        table)))
                   (details (clutch-db-error-details conn)))
              (signal 'clutch-db-error
                      (if details
                          (list message (copy-tree details))
                        (list message)))))
        (condition-case err
            (let ((details (clutch-db-column-details conn table)))
              (puthash table details cache)
              (clutch--clear-column-details-status conn table)
              details)
          (clutch-db-error
           (let* ((message (error-message-string err))
                  (details (or (nth 2 err)
                               (clutch-db-error-details conn))))
             (clutch--set-column-details-status conn table 'failed message)
             (when strict
               (signal 'clutch-db-error
                       (if details
                           (list message (copy-tree details))
                         (list message))))
             nil)))))))

(defun clutch--drain-column-details-async (conn)
  "Start the next queued async column-details fetch for CONN."
  (unless (or (clutch--column-details-active conn)
              (not (clutch--connection-alive-p conn)))
    (let ((backend (when clutch-debug-mode
                     (condition-case nil
                         (clutch--backend-key-from-conn conn)
                       (error nil)))))
      (when-let* ((queue (clutch--column-details-queue conn))
                  (table (car queue))
                  (ticket (plist-get (clutch--column-details-status conn table) :ticket)))
      (clutch--set-column-details-queue conn (cdr queue))
      (clutch--set-column-details-active conn table ticket)
      (clutch--set-column-details-status conn table 'loading nil ticket)
      (let ((started
             (clutch-db-column-details-async
              conn table
              (lambda (details)
                (if (clutch--column-details-ticket-current-p conn table ticket)
                    (let* ((key (clutch--connection-key conn))
                           (cache (or (gethash key clutch--column-details-cache)
                                      (let ((h (make-hash-table :test 'equal)))
                                        (puthash key h clutch--column-details-cache)
                                        h))))
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "column-details"
                         :phase "success"
                         :backend backend
                         :summary (format "Loaded %d column details for %s"
                                          (length details) table)
                         :context (list :table table)))
                      (puthash table details cache)
                      (clutch--clear-column-details-status conn table)
                      (clutch--clear-column-details-active conn)
                      (clutch--refresh-result-metadata-buffers conn table)
                      (clutch--refresh-schema-status-ui conn)
                      (clutch--drain-column-details-async conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "column-details"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale column-detail result for %s" table)
                     :context (list :table table)))))
              (lambda (message)
                (if (clutch--column-details-ticket-current-p conn table ticket)
                    (progn
                      (clutch--set-column-details-status conn table 'failed
                                                         message ticket)
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "column-details"
                         :phase "error"
                         :backend backend
                         :summary message
                         :context (list :table table)))
                      (clutch--clear-column-details-active conn)
                      (clutch--refresh-schema-status-ui conn)
                      (clutch--drain-column-details-async conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "column-details"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale column-detail error for %s" table)
                     :context (list :table table))))))))
        (when (and started clutch-debug-mode)
          (clutch--remember-debug-event
           :connection conn
           :op "column-details"
           :phase "submit"
           :backend backend
           :summary (format "Queued background column-detail preheat for %s" table)
           :context (list :table table)))
        (unless started
        (clutch--ensure-column-details conn table)
          (clutch--clear-column-details-active conn)
          (when (clutch--cached-column-details conn table)
            (clutch--refresh-result-metadata-buffers conn table))
          (clutch--refresh-schema-status-ui conn)
          (clutch--drain-column-details-async conn)))))))

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

(defun clutch--ensure-table-comment-async (conn table)
  "Queue an async table-comment fetch for TABLE on CONN when needed."
  (let ((state (plist-get (clutch--table-comment-status conn table) :state))
        (ticket (clutch--begin-table-comment-ticket))
        (backend (when clutch-debug-mode
                   (condition-case nil
                       (clutch--backend-key-from-conn conn)
                     (error nil)))))
      (unless (or (not table)
                  (clutch--table-comment-cached-p conn table)
                  (memq state '(queued loading failed)))
      (clutch--set-table-comment-status conn table 'loading nil ticket)
      (let ((started
             (clutch-db-table-comment-async
              conn table
              (lambda (comment)
                (if (clutch--table-comment-ticket-current-p conn table ticket)
                    (progn
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "table-comment"
                         :phase "success"
                         :backend backend
                         :summary (format "Loaded table comment for %s" table)
                         :context (list :table table)))
                      (let* ((key (clutch--connection-key conn))
                             (cache (or (gethash key clutch--table-comment-cache)
                                        (let ((h (make-hash-table :test 'equal)))
                                          (puthash key h clutch--table-comment-cache)
                                          h))))
                        (puthash table comment cache))
                      (clutch--clear-table-comment-status conn table)
                      (clutch--refresh-schema-status-ui conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "table-comment"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale table-comment result for %s" table)
                     :context (list :table table)))))
              (lambda (message)
                (if (clutch--table-comment-ticket-current-p conn table ticket)
                    (progn
                      (clutch--set-table-comment-status conn table 'failed
                                                        message ticket)
                      (when clutch-debug-mode
                        (clutch--remember-debug-event
                         :connection conn
                         :op "table-comment"
                         :phase "error"
                         :backend backend
                         :summary message
                         :context (list :table table)))
                      (clutch--refresh-schema-status-ui conn))
                  (when clutch-debug-mode
                    (clutch--remember-debug-event
                     :connection conn
                     :op "table-comment"
                     :phase "stale-drop"
                     :backend backend
                     :summary (format "Ignored stale table-comment error for %s" table)
                     :context (list :table table))))))))
        (when (and started clutch-debug-mode)
          (clutch--remember-debug-event
           :connection conn
           :op "table-comment"
           :phase "submit"
           :backend backend
           :summary (format "Queued background table-comment preheat for %s" table)
           :context (list :table table)))
        (unless started
        (condition-case err
            (let* ((key (clutch--connection-key conn))
                   (cache (or (gethash key clutch--table-comment-cache)
                              (let ((h (make-hash-table :test 'equal)))
                                (puthash key h clutch--table-comment-cache)
                                h))))
              (puthash table (clutch-db-table-comment conn table) cache)
              (clutch--clear-table-comment-status conn table))
          (clutch-db-error
           (clutch--set-table-comment-status conn table 'failed
                                             (error-message-string err)
                                             ticket)))
          (clutch--refresh-schema-status-ui conn)))
      t)))

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
