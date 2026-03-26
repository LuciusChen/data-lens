;;; clutch-object.el --- Object workflow for clutch -*- lexical-binding: t; -*-

;;; Commentary:

;; Internal object discovery, describe, action, and Embark helpers loaded from `clutch.el'.

;;; Code:

(require 'cl-lib)
(require 'clutch-db)
(require 'sql)
(require 'subr-x)
(require 'transient)

(defvar clutch--conn-sql-product)
(defvar clutch--connection-params)
(defvar-local clutch--describe-object-entry nil
  "Object entry currently displayed in a clutch describe buffer.")
(defvar clutch--object-cache (make-hash-table :test 'equal)
  "Object discovery cache keyed by connection key string.
Each value is a plist with at least :entries and :fetched-at.")
(defvar clutch--object-warmup-timers (make-hash-table :test 'equal)
  "Idle timers warming object discovery caches keyed by connection key string.")
(defvar clutch-connection)
(defvar clutch-object-warmup-idle-delay-seconds)
(defvar clutch-primary-object-types)
(defvar clutch-sql-product)

(defvar embark-default-action-overrides)
(defvar embark-target-finders)
(defvar embark-keymap-alist)

(declare-function clutch--bind-connection-context "clutch" (conn &optional params product))
(declare-function clutch--header-with-disconnect-badge "clutch-ui" (base))
(declare-function clutch--connection-alive-p "clutch" (conn))
(declare-function clutch--effective-sql-product "clutch" (params))
(declare-function clutch--ensure-column-details "clutch-schema" (conn table))
(declare-function clutch--ensure-connection "clutch" ())
(declare-function clutch--ensure-table-comment "clutch-schema" (conn table))
(declare-function clutch--icon-with-face "clutch-ui"
                  (name fallback face &rest icon-args))
(declare-function clutch--connection-key "clutch" (conn))
(declare-function clutch--refresh-current-schema "clutch" (&optional silent))
(declare-function clutch--schema-for-connection "clutch-schema" (&optional conn))
(declare-function clutch--warn-completion-metadata-error-once "clutch" (message-text))
(declare-function clutch--warn-schema-cache-state "clutch" (&optional conn))

(defun clutch--object-type-allowed-p (entry allowed-types)
  "Return non-nil when ENTRY is permitted by ALLOWED-TYPES.
When ALLOWED-TYPES is nil, allow all entries."
  (or (null allowed-types)
      (member (upcase (or (plist-get entry :type) ""))
              allowed-types)))

(defun clutch--filter-object-entries-by-types (entries allowed-types)
  "Return ENTRIES filtered by ALLOWED-TYPES.
When ALLOWED-TYPES is nil, return ENTRIES unchanged."
  (if (null allowed-types)
      entries
    (seq-filter
     (lambda (entry)
       (clutch--object-type-allowed-p entry allowed-types))
     entries)))

(defun clutch--object-cache-key (conn)
  "Return the cache key for object discovery on CONN.
This currently matches the connection identity and is structured as a helper so
future schema switching can widen the key without touching all call sites."
  (or (clutch--connection-key conn)
      (format "%S" conn)))

(defun clutch--object-connection-alive-p (conn)
  "Return non-nil when CONN is usable for object metadata work.
Recoverable database liveness-check failures are warned once and treated as
temporarily unavailable."
  (condition-case err
      (clutch--connection-alive-p conn)
    (clutch-db-error
     (clutch--warn-completion-metadata-error-once (error-message-string err))
     nil)))

;;;; Object discovery

(defconst clutch--object-categories
  '(indexes sequences procedures functions triggers)
  "Metadata categories loaded into clutch object pickers.")

(defconst clutch--object-type-order
  '("TABLE" "VIEW" "SYNONYM" "INDEX" "SEQUENCE"
    "PROCEDURE" "FUNCTION" "TRIGGER")
  "Display order for grouped clutch object completion candidates.")

(defvar-local clutch-browser-current-object nil
  "Most recently selected database object for the current buffer.")

(defvar clutch--object-action-entry nil
  "Object entry currently targeted by `clutch-act-dwim'.")

(defvar clutch--object-dispatch-entry nil
  "Dynamic object entry currently being dispatched via Embark.")

(defun clutch--object-entry-key (entry)
  "Return a stable identity key for object ENTRY."
  (or (plist-get entry :identity)
      (list (or (plist-get entry :name) "")
            (or (plist-get entry :type) "")
            (or (plist-get entry :schema) "")
            (or (plist-get entry :source-schema) ""))))

(defun clutch--merge-object-entries (&rest entry-lists)
  "Merge ENTRY-LISTS by stable object identity."
  (let ((seen (make-hash-table :test 'equal))
        merged)
    (dolist (entries entry-lists)
      (dolist (entry entries)
        (let ((key (clutch--object-entry-key entry)))
          (unless (gethash key seen)
            (puthash key t seen)
            (push entry merged)))))
    (nreverse merged)))

(defun clutch--object-cache-entry (conn)
  "Return cached object discovery metadata for CONN, or nil."
  (and conn
       (gethash (clutch--object-cache-key conn)
                clutch--object-cache)))

(defun clutch--object-cache-entries (conn)
  "Return cached object entries for CONN, or nil."
  (plist-get (clutch--object-cache-entry conn) :entries))

(defun clutch--object-cache-type-table (conn)
  "Return cached per-type object entries for CONN, or nil."
  (plist-get (clutch--object-cache-entry conn) :by-type))

(defun clutch--object-cache-loaded-categories (conn)
  "Return loaded object category symbols for CONN, or nil."
  (plist-get (clutch--object-cache-entry conn) :loaded-categories))

(defun clutch--object-cache-complete-p (conn)
  "Return non-nil when CONN has a fully warmed object cache."
  (equal (sort (copy-sequence (clutch--object-cache-loaded-categories conn))
               (lambda (a b) (string< (symbol-name a) (symbol-name b))))
         (sort (copy-sequence clutch--object-categories)
               (lambda (a b) (string< (symbol-name a) (symbol-name b))))))

(defun clutch--make-object-type-cache (entries)
  "Return a hash table grouping ENTRIES by normalized object type."
  (let ((by-type (make-hash-table :test 'equal)))
    (dolist (entry entries)
      (let* ((type (clutch--normalize-object-type (plist-get entry :type)))
             (existing (gethash type by-type)))
        (puthash type (nconc existing (list entry)) by-type)))
    by-type))

(defun clutch--filter-object-entries-by-type (entries type)
  "Return ENTRIES filtered to normalized object TYPE."
  (seq-filter
   (lambda (entry)
     (equal (clutch--normalize-object-type (plist-get entry :type))
            (clutch--normalize-object-type type)))
   entries))

(defun clutch--object-cache-type-entries (conn type)
  "Return cached object entries for CONN filtered to TYPE, or nil."
  (let ((type (clutch--normalize-object-type type)))
    (or (when-let* ((entries (clutch--object-cache-entries conn)))
          (clutch--filter-object-entries-by-type entries type))
        (when-let* ((by-type (clutch--object-cache-type-table conn)))
          (gethash type by-type)))))

(defun clutch--store-object-cache (conn entries)
  "Store object ENTRIES for CONN and return ENTRIES."
  (puthash (clutch--object-cache-key conn)
           (list :entries entries
                 :by-type (clutch--make-object-type-cache entries)
                 :loaded-categories (copy-sequence clutch--object-categories)
                 :fetched-at (float-time))
           clutch--object-cache)
  entries)

(defun clutch--store-object-cache-type-entries (conn type entries)
  "Store per-type object ENTRIES for CONN and TYPE, returning ENTRIES."
  (let* ((key (clutch--object-cache-key conn))
         (cache (or (gethash key clutch--object-cache) (list)))
         (by-type (or (plist-get cache :by-type)
                      (make-hash-table :test 'equal)))
         (loaded (copy-sequence (plist-get cache :loaded-categories)))
         (type (clutch--normalize-object-type type))
         (category (pcase type
                     ("INDEX" 'indexes)
                     ("SEQUENCE" 'sequences)
                     ("PROCEDURE" 'procedures)
                     ("FUNCTION" 'functions)
                     ("TRIGGER" 'triggers)
                     (_ nil))))
    (puthash type entries by-type)
    (when category
      (cl-pushnew category loaded))
    (puthash key
             (list :entries (clutch--merge-object-entries
                             (clutch--browseable-object-entries conn)
                             (apply #'append
                                    (delq nil
                                          (mapcar (lambda (object-type)
                                                    (gethash object-type by-type))
                                                  clutch--object-type-order))))
                   :by-type by-type
                   :loaded-categories loaded
                   :fetched-at (float-time))
             clutch--object-cache)
    entries))

(defun clutch--table-like-entry-p (entry)
  "Return non-nil when ENTRY can be browsed as rows."
  (member (upcase (or (plist-get entry :type) ""))
          '("TABLE" "VIEW" "SYNONYM")))

(defun clutch--normalize-object-type (type)
  "Return a normalized object TYPE string."
  (let ((type (upcase (or type ""))))
    (pcase type
      ("PUBLIC SYNONYM" "SYNONYM")
      (_ type))))

(defun clutch--object-type-title (type)
  "Return a plural display title for object TYPE."
  (pcase (clutch--normalize-object-type type)
    ("TABLE" "Tables")
    ("VIEW" "Views")
    ("SYNONYM" "Synonyms")
    ("INDEX" "Indexes")
    ("SEQUENCE" "Sequences")
    ("PROCEDURE" "Procedures")
    ("FUNCTION" "Functions")
    ("TRIGGER" "Triggers")
    (_ "Objects")))

(defun clutch--object-entry-group-title (entry)
  "Return the grouped completion title for ENTRY."
  (clutch--object-type-title (plist-get entry :type)))

(defun clutch--object-type-rank (type)
  "Return the display rank for object TYPE."
  (or (seq-position clutch--object-type-order
                    (clutch--normalize-object-type type)
                    #'equal)
      most-positive-fixnum))

(defun clutch--object-category-type (category)
  "Return the normalized object TYPE string for CATEGORY."
  (pcase category
    ('indexes "INDEX")
    ('sequences "SEQUENCE")
    ('procedures "PROCEDURE")
    ('functions "FUNCTION")
    ('triggers "TRIGGER")
    (_ nil)))

(defun clutch--cancel-object-warmup (conn &optional key)
  "Cancel any pending object warmup timer for CONN or explicit KEY."
  (let ((key (or key (clutch--object-cache-key conn))))
    (when-let* ((timer (gethash key clutch--object-warmup-timers)))
      (cancel-timer timer)
      (remhash key clutch--object-warmup-timers))))

(defun clutch--schedule-object-warmup (conn)
  "Warm non-table object categories for CONN during idle time."
  (let* ((key (clutch--object-cache-key conn))
         (loaded (clutch--object-cache-loaded-categories conn))
         (next (seq-find (lambda (category)
                           (not (memq category loaded)))
                         clutch--object-categories)))
    (cond
     ((or (not conn)
          (not (clutch--object-connection-alive-p conn))
          (null next))
      (clutch--cancel-object-warmup conn))
     ((gethash key clutch--object-warmup-timers)
      nil)
     (t
      (puthash
       key
       (run-with-idle-timer
        clutch-object-warmup-idle-delay-seconds nil
        (lambda ()
          (remhash key clutch--object-warmup-timers)
          (when (and conn (clutch--object-connection-alive-p conn))
            (condition-case err
                (if (clutch-db-busy-p conn)
                    (clutch--schedule-object-warmup conn)
                  (let ((type (clutch--object-category-type next)))
                    (unless
                        (and type
                             (clutch-db-list-objects-async
                              conn next
                              (lambda (entries)
                                (when (and conn
                                           (clutch--object-connection-alive-p conn))
                                  (clutch--store-object-cache-type-entries conn type entries)
                                  (clutch--schedule-object-warmup conn)))
                              (lambda (&rest _)
                                (when (and conn
                                           (clutch--object-connection-alive-p conn))
                                  (clutch--schedule-object-warmup conn)))))
                      (progn
                        (when type
                          (clutch--object-type-entries conn type))
                        (clutch--schedule-object-warmup conn)))))
              (clutch-db-error err
               (clutch--warn-completion-metadata-error-once
                (error-message-string err))
               (when (and conn
                          (clutch--object-connection-alive-p conn))
                 (clutch--schedule-object-warmup conn)))))))
       clutch--object-warmup-timers)))))

(defun clutch--partial-object-entries (conn)
  "Return a fast object snapshot for CONN.
Includes browseable objects immediately and any already-warmed categories."
  (let ((entries
         (clutch--merge-object-entries
          (clutch--browseable-object-entries conn)
          (clutch--object-cache-entries conn))))
    (unless (clutch--object-cache-complete-p conn)
      (clutch--schedule-object-warmup conn))
    entries))

(defun clutch--object-entries (conn &optional refresh)
  "Return cached object entries for CONN.
When REFRESH is non-nil, bypass any cached discovery snapshot."
  (if refresh
      (clutch--store-object-cache
       conn
       (apply
        #'clutch--merge-object-entries
        (append
         (list (clutch-db-list-table-entries conn)
               (clutch-db-search-table-entries conn ""))
         (mapcar (lambda (category)
                   (clutch-db-list-objects conn category))
                 clutch--object-categories))))
    (clutch--partial-object-entries conn)))

(defun clutch--object-type-entries (conn type &optional refresh)
  "Return object entries for CONN filtered to TYPE.
When REFRESH is non-nil, bypass any cached per-type entries."
  (let ((type (clutch--normalize-object-type type)))
    (or (and (not refresh)
             (clutch--object-cache-type-entries conn type))
        (clutch--store-object-cache-type-entries
         conn type
         (pcase type
           ((or "TABLE" "VIEW" "SYNONYM")
            (clutch--filter-object-entries-by-type
             (clutch--browseable-object-entries conn)
             type))
           ("INDEX" (clutch-db-list-objects conn 'indexes))
           ("SEQUENCE" (clutch-db-list-objects conn 'sequences))
           ("PROCEDURE" (clutch-db-list-objects conn 'procedures))
           ("FUNCTION" (clutch-db-list-objects conn 'functions))
           ("TRIGGER" (clutch-db-list-objects conn 'triggers))
           (_ nil))))))

(defun clutch--object-entry-display-detail (entry duplicate-counts)
  "Return optional disambiguation detail for ENTRY.
DUPLICATE-COUNTS maps object names to the number of visible entries."
  (let* ((name (or (plist-get entry :name) ""))
         (target (clutch--object-entry-target entry))
         (schema (or (plist-get entry :schema) ""))
         (source (or (plist-get entry :source-schema) ""))
         (identity (plist-get entry :identity)))
    (when (> (gethash name duplicate-counts 0) 1)
      (cond
       ((and target (not (string-empty-p target))) target)
       ((and schema source (not (string-empty-p schema)) (not (string= schema source)))
        schema)
       ((and identity (not (string-empty-p identity))) identity)
       ((not (string-empty-p schema)) schema)
       (t nil)))))

(defun clutch--object-entry-candidate (entry duplicate-counts)
  "Return a completion candidate string for ENTRY.
DUPLICATE-COUNTS maps object names to the number of visible entries."
  (let* ((name (or (plist-get entry :name) ""))
         (count (gethash name duplicate-counts 0)))
    (if (> count 1)
        (concat name
                (propertize
                 (format "\t%s\t%s\t%s\t%s"
                         (clutch--object-entry-label entry)
                         (or (plist-get entry :schema) "")
                         (or (plist-get entry :source-schema) "")
                         (or (plist-get entry :identity) ""))
                 'invisible t))
      name)))

(defun clutch--object-entry-annotation (entry duplicate-counts)
  "Return a propertized minibuffer annotation string for object ENTRY."
  (let* ((label (clutch--object-entry-label entry))
         (detail (clutch--object-entry-display-detail entry duplicate-counts)))
    (concat
     (propertize "  " 'face 'shadow)
     (propertize label
                 'face (if (equal (plist-get entry :source-schema) "PUBLIC")
                           'clutch-object-public-source-face
                         'clutch-object-source-face))
     (when detail
       (concat
        (propertize "  " 'face 'shadow)
        (propertize detail 'face 'clutch-object-type-face))))))

(defun clutch--object-entry-affixation (cands entry-map duplicate-counts)
  "Return affixated completion tuples for object CANDS."
  (let* ((labels
          (mapcar (lambda (cand)
                    (clutch--object-entry-label (gethash cand entry-map)))
                  cands))
         (label-width (if labels
                          (apply #'max (mapcar #'string-width labels))
                        0)))
    (mapcar
     (lambda (cand)
       (let* ((entry (gethash cand entry-map))
              (label (clutch--object-entry-label entry))
              (detail (clutch--object-entry-display-detail entry duplicate-counts))
              (label-face (if (equal (plist-get entry :source-schema) "PUBLIC")
                              'clutch-object-public-source-face
                            'clutch-object-source-face))
              (suffix
               (concat
                (propertize "  " 'face 'shadow)
                (propertize
                 (format (format "%%-%ds" label-width) label)
                 'face label-face)
                (if detail
                    (concat
                     (propertize "  " 'face 'shadow)
                     (propertize detail 'face 'clutch-object-type-face))
                  ""))))
         (list cand "" suffix)))
     cands)))

(defun clutch--object-entry-reader (_conn prompt entries &optional initial-input category)
  "Read an object entry from ENTRIES on CONN using PROMPT."
  (let* ((sorted
          (sort (copy-sequence entries)
                (lambda (a b)
                  (clutch--object-entry-key<
                   (clutch--object-entry-sort-key a)
                   (clutch--object-entry-sort-key b)))))
         (entry-map (make-hash-table :test 'equal))
         (duplicate-counts (make-hash-table :test 'equal))
         candidates)
    (dolist (entry sorted)
      (puthash (plist-get entry :name)
               (1+ (gethash (plist-get entry :name) duplicate-counts 0))
               duplicate-counts))
    (dolist (entry sorted)
      (let ((candidate
             (clutch--object-entry-candidate entry duplicate-counts)))
        (puthash candidate entry entry-map)
        (push candidate candidates)))
    (setq candidates (nreverse candidates))
    (cl-labels ((candidate-list () candidates)
                (annotation (candidate)
                  (when-let* ((entry (gethash candidate entry-map)))
                    (clutch--object-entry-annotation entry duplicate-counts)))
                (group (candidate transform)
                  (if transform
                      candidate
                    (when-let* ((entry (gethash candidate entry-map)))
                      (clutch--object-entry-group-title entry))))
                (affixate (cands)
                  (clutch--object-entry-affixation cands entry-map duplicate-counts))
                (complete (str pred action)
                  (if (eq action 'metadata)
                      `(metadata
                        ,@(when category `((category . ,category)))
                        (annotation-function . ,#'annotation)
                        (group-function . ,#'group)
                        (affixation-function . ,#'affixate)
                        (display-sort-function . identity)
                        (cycle-sort-function . identity))
                    (complete-with-action action (candidate-list) str pred))))
      (let ((choice
             (completing-read
              prompt
              #'complete
              nil t initial-input)))
        (or (gethash choice entry-map)
            (user-error "Unknown clutch object: %s" choice))))))

(defun clutch--synonym-entry-p (entry)
  "Return non-nil when ENTRY is a synonym-like object."
  (string-match-p "SYNONYM" (upcase (or (plist-get entry :type) ""))))

(defun clutch--public-source-entry-p (entry)
  "Return non-nil when ENTRY comes from PUBLIC source schema."
  (string-equal-ignore-case "PUBLIC" (or (plist-get entry :source-schema) "")))

(defun clutch--preferred-object-match (matches &optional table-like-only)
  "Return the preferred entry from MATCHES, or nil when still ambiguous.
When TABLE-LIKE-ONLY is non-nil, only consider table-like matches."
  (let* ((candidates (if table-like-only
                         (seq-filter #'clutch--table-like-entry-p matches)
                       matches))
         (non-public (seq-remove #'clutch--public-source-entry-p candidates))
         (preferred (seq-remove #'clutch--synonym-entry-p non-public)))
    (cond
     ((= (length preferred) 1) (car preferred))
     ((= (length non-public) 1) (car non-public))
     ((= (length candidates) 1) (car candidates))
     (t nil))))

(defun clutch--object-matches-by-name (conn name &optional table-like-only allowed-types)
  "Return object entries on CONN whose name matches NAME."
  (let ((entries
         (clutch--filter-object-entries-by-types
          (if table-like-only
              (clutch--browseable-object-entries conn)
            (clutch--object-entries conn))
          allowed-types)))
    (seq-filter
     (lambda (entry)
       (and (or (not table-like-only)
                (clutch--table-like-entry-p entry))
            (string-equal-ignore-case
             name
             (or (plist-get entry :name) ""))))
     entries)))

(defun clutch--object-matches-at-point (&optional table-like-only allowed-types)
  "Return matching object entries for the symbol at point."
  (when-let* ((conn clutch-connection)
              ((clutch--object-connection-alive-p conn))
              (sym (thing-at-point 'symbol t)))
    (clutch--object-matches-by-name conn sym table-like-only allowed-types)))

(defun clutch--remember-current-object (entry)
  "Store ENTRY as the current object for the active buffer."
  (setq clutch-browser-current-object entry)
  entry)

(defun clutch--command-context-buffer ()
  "Return the active clutch buffer for the current command."
  (if (and (minibufferp)
           (window-live-p (minibuffer-selected-window)))
      (window-buffer (minibuffer-selected-window))
    (current-buffer)))

(defun clutch--command-connection-context ()
  "Return connection context for the current command."
  (let ((buf (clutch--command-context-buffer)))
    (list :buffer buf
          :connection (buffer-local-value 'clutch-connection buf)
          :params (buffer-local-value 'clutch--connection-params buf)
          :product (buffer-local-value 'clutch--conn-sql-product buf))))

(defun clutch-object-at-point ()
  "Return the uniquely identified object entry at point, or nil."
  (clutch--preferred-object-match (clutch--object-matches-at-point)))

(defun clutch-object-read (&optional prompt table-like-only initial-input category allowed-types)
  "Read and return a database object entry for the current connection.
When TABLE-LIKE-ONLY is non-nil, only include table-like objects."
  (clutch--ensure-connection)
  (clutch--warn-schema-cache-state clutch-connection)
  (let* ((entries
          (clutch--filter-object-entries-by-types
           (if table-like-only
               (clutch--browseable-object-entries clutch-connection)
             (clutch--object-entries clutch-connection))
           allowed-types)))
    (clutch--remember-current-object
     (clutch--object-entry-reader
      clutch-connection
      (or prompt "Object: ")
      entries
      initial-input
      (or category 'clutch-object)))))

(defun clutch--buffer-current-object (&optional table-like-only allowed-types)
  "Return the current object associated with the command context buffer.
When TABLE-LIKE-ONLY is non-nil, only return table-like objects."
  (let* ((buf (clutch--command-context-buffer))
         (entry (and (buffer-live-p buf)
                     (buffer-local-value 'clutch-browser-current-object buf))))
    (when (and entry
               (or (not table-like-only)
                   (clutch--table-like-entry-p entry))
               (clutch--object-type-allowed-p entry allowed-types)
               (not (with-current-buffer buf
                      (derived-mode-p 'clutch-mode 'clutch-repl-mode))))
      entry)))

(defun clutch--symbol-has-local-completions-p (symbol entries)
  "Return non-nil when SYMBOL prefix-matches any entry name in ENTRIES."
  (let ((downcased (downcase symbol)))
    (cl-loop for entry in entries
             thereis (string-prefix-p downcased
                                      (downcase (or (plist-get entry :name) ""))))))

(defun clutch--on-demand-object-search (conn sym table-like-only allowed-types)
  "Search for objects matching SYM on CONN beyond the local cache.
Returns a list of matching entries from table search and, when cache is
incomplete and TABLE-LIKE-ONLY is nil, from a sync-refreshed full snapshot.
Results are filtered by ALLOWED-TYPES and deduplicated."
  (let* ((table-hits
          (condition-case nil
              (clutch-db-search-table-entries conn sym)
            (clutch-db-error nil)))
         (full-entries
          (when (and (not table-like-only)
                     (not (clutch--object-cache-complete-p conn)))
            (clutch--filter-object-entries-by-types
             (clutch--object-entries conn t)
             allowed-types)))
         (name-from-full
          (when full-entries
            (let ((downcased (downcase sym)))
              (seq-filter
               (lambda (e)
                 (string-prefix-p downcased
                                  (downcase (or (plist-get e :name) ""))))
               full-entries)))))
    (list :hits (clutch--filter-object-entries-by-types
                  (clutch--merge-object-entries-by-name
                   (append table-hits name-from-full))
                  allowed-types)
          :full-entries full-entries)))

(defun clutch--resolve-object-entry (prompt &optional table-like-only category allowed-types)
  "Return the object entry for PROMPT in the current buffer context.
Uses a layered resolution strategy:
1. Buffer-local current object or exact match at point
2. Multiple matches → picker with pre-fill
3. Local prefix match → pre-fill picker
4. On-demand remote search → direct return or picker with results
5. No match → picker with full candidate list, no pre-fill"
  (let ((current-object (clutch--buffer-current-object table-like-only allowed-types))
        (matches (clutch--object-matches-at-point table-like-only allowed-types)))
    (clutch--remember-current-object
     (cond
      (current-object
       current-object)
      ((clutch--preferred-object-match matches table-like-only))
      ((> (length matches) 1)
       (clutch--object-entry-reader clutch-connection
                                    prompt
                                    matches
                                    (thing-at-point 'symbol t)
                                    category))
      (t
       (clutch--ensure-connection)
       (clutch--warn-schema-cache-state clutch-connection)
       (let* ((sym (thing-at-point 'symbol t))
              (entries
               (clutch--filter-object-entries-by-types
                (if table-like-only
                    (clutch--browseable-object-entries clutch-connection)
                  (clutch--object-entries clutch-connection))
                allowed-types))
              (cat (or category 'clutch-object)))
         (cond
          ((null sym)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries nil cat))
          ((clutch--symbol-has-local-completions-p sym entries)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries sym cat))
          ((clutch--object-connection-alive-p clutch-connection)
           (let* ((result (clutch--on-demand-object-search
                           clutch-connection sym table-like-only allowed-types))
                  (hits (plist-get result :hits))
                  (full (plist-get result :full-entries)))
             (cond
              ((= (length hits) 1) (car hits))
              ((> (length hits) 1)
               (clutch--object-entry-reader clutch-connection prompt
                                             hits sym cat))
              (t
               (message "No matching object found for: %s" sym)
               (clutch--object-entry-reader clutch-connection
                                             (or prompt "Object: ")
                                             (or full entries) nil cat)))))
          (t
           (message "No matching object found for: %s" sym)
           (clutch--object-entry-reader clutch-connection
                                         (or prompt "Object: ") entries nil cat)))))))))

(defun clutch--read-table-name (prompt tables)
  "Read a table name from TABLES with PROMPT.
Annotates the collection with `clutch-object' category so Embark
can offer object actions on the completion candidates."
  (completing-read prompt
                   (lambda (str pred action)
                     (if (eq action 'metadata)
                         '(metadata (category . clutch-object))
                       (complete-with-action action tables str pred)))
                   nil t))

(defun clutch--object-entry-label (entry)
  "Return a compact source/type label for object ENTRY."
  (let* ((source (plist-get entry :source-schema))
         (type (clutch--object-entry-type-display entry)))
    (cond
     ((and source (not (string-empty-p type)))
      (format "%s/%s" source type))
     (source source)
     ((not (string-empty-p type)) type)
     (t ""))))

(defun clutch--object-entry-type-display (entry)
  "Return the compact type display for object ENTRY."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (cond
     ((equal type "PUBLIC SYNONYM") "synonym")
     ((string-empty-p type) "")
     (t (downcase type)))))

(defun clutch--object-entry-identity-key (entry)
  "Return a stable identity key for object ENTRY."
  (list (or (plist-get entry :name) "")
        (or (plist-get entry :type) "")
        (or (plist-get entry :schema) "")
        (or (plist-get entry :source-schema) "")))

(defun clutch--object-entry-sort-key (entry)
  "Return a stable sort key for ENTRY."
  (list (clutch--object-type-rank (plist-get entry :type))
        (if (equal (plist-get entry :source-schema) "PUBLIC") 1 0)
        (or (plist-get entry :source-schema) "")
        (or (plist-get entry :name) "")))

(defun clutch--object-entry-key< (left right)
  "Return non-nil when sort key LEFT should sort before RIGHT."
  (or (< (nth 0 left) (nth 0 right))
      (and (= (nth 0 left) (nth 0 right))
           (or (< (nth 1 left) (nth 1 right))
               (and (= (nth 1 left) (nth 1 right))
                    (or (string< (nth 2 left) (nth 2 right))
                        (and (string= (nth 2 left) (nth 2 right))
                             (string< (nth 3 left) (nth 3 right)))))))))

(defun clutch--merge-object-entries-by-name (&rest entry-lists)
  "Merge ENTRY-LISTS by object identity, preserving the first occurrence."
  (let ((seen (make-hash-table :test 'equal))
        merged)
    (dolist (entries entry-lists)
      (dolist (entry entries)
        (let ((key (clutch--object-entry-identity-key entry)))
          (unless (gethash key seen)
            (puthash key t seen)
            (push entry merged)))))
    (nreverse merged)))

(defun clutch--object-entry-target (entry)
  "Return the target schema display for object ENTRY, or nil."
  (let ((schema (or (plist-get entry :schema) ""))
        (source (or (plist-get entry :source-schema) "")))
    (unless (or (string-empty-p schema)
                (string= schema source))
      schema)))

(defun clutch--browseable-object-entries (conn)
  "Return the base browseable object entry list for CONN.
Includes both schema/browser entries and search-discovered entries so object
selection can surface objects from different Oracle sources and types."
  (clutch--merge-object-entries-by-name
   (clutch-db-browseable-object-entries conn)))

(defun clutch--find-console-for-conn (conn)
  "Return the clutch-mode buffer that owns CONN, or nil."
  (cl-loop for buf in (buffer-list)
           when (and (eq (buffer-local-value 'major-mode buf) 'clutch-mode)
                     (eq (buffer-local-value 'clutch-connection buf) conn))
           return buf))

(defun clutch--console-has-nonblank-content-p ()
  "Return non-nil when the current console buffer has nonblank content."
  (save-excursion
    (goto-char (point-min))
    (re-search-forward "[^[:space:]\n\r\t]" nil t)))

(defun clutch--count-adjacent-newlines-before (pos)
  "Return the number of contiguous whitespace newlines immediately before POS."
  (save-excursion
    (goto-char pos)
    (let ((end (point)))
      (skip-chars-backward " \t\n")
      (cl-loop for ch across (buffer-substring-no-properties (point) end)
               count (eq ch ?\n)))))

(defun clutch--count-adjacent-newlines-after (pos)
  "Return the number of contiguous whitespace newlines immediately after POS."
  (save-excursion
    (goto-char pos)
    (let ((start (point)))
      (skip-chars-forward " \t\n")
      (cl-loop for ch across (buffer-substring-no-properties start (point))
               count (eq ch ?\n)))))

(defun clutch--console-nonblank-before-p (pos)
  "Return non-nil when POS has nonblank console content before it."
  (save-excursion
    (goto-char pos)
    (re-search-backward "[^[:space:]\n\r\t]" nil t)))

(defun clutch--console-nonblank-after-p (pos)
  "Return non-nil when POS has nonblank console content after it."
  (save-excursion
    (goto-char pos)
    (re-search-forward "[^[:space:]\n\r\t]" nil t)))

(defun clutch--insert-console-sql-block (sql)
  "Insert SQL into the current console with normalized blank-line spacing."
  (if (not (clutch--console-has-nonblank-content-p))
      (progn
        (erase-buffer)
        (insert sql))
    (let* ((pos (point))
           (before-p (clutch--console-nonblank-before-p pos))
           (after-p (clutch--console-nonblank-after-p pos))
           (before-nl (and before-p (clutch--count-adjacent-newlines-before pos)))
           (after-nl (and after-p (clutch--count-adjacent-newlines-after pos)))
           (prefix (if before-p (make-string (max 0 (- 2 before-nl)) ?\n) ""))
           (suffix (if after-p (make-string (max 0 (- 2 after-nl)) ?\n) "")))
      (insert prefix sql suffix))))

(defun clutch--object-fqname (entry)
  "Return a display FQNAME for object ENTRY."
  (let* ((schema (or (plist-get entry :source-schema)
                     (plist-get entry :schema)))
         (name (or (plist-get entry :name) "")))
    (if (and schema (not (string-empty-p schema)))
        (format "%s.%s" schema name)
      name)))

(defun clutch--show-object-text-buffer (conn entry text &optional params product)
  "Display TEXT for ENTRY using CONN's SQL product."
  (let* ((product (or product
                      (and params
                           (clutch--effective-sql-product params))
                      clutch-sql-product))
         (title (clutch--object-fqname entry))
         (buf (get-buffer-create (format "*clutch: %s*" title))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (sql-mode)
        (sql-set-product product)
        (clutch--bind-connection-context conn params product)
        (setq-local clutch-browser-current-object entry)
        (local-set-key (kbd "C-c C-o") #'clutch-act-dwim)
        (local-set-key (kbd "C-c C-d") #'clutch-describe-dwim)
        (erase-buffer)
        (insert text)
        (insert "\n")
        (font-lock-ensure)
        (setq buffer-read-only t)
        (goto-char (point-min))))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch--object-definition-text (conn entry)
  "Return the definition text for ENTRY on CONN."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (pcase type
      ((or "PROCEDURE" "FUNCTION" "TRIGGER")
       (clutch-db-object-source conn entry))
      ("TABLE"
       (clutch-db-show-create-table conn (plist-get entry :name)))
      (_
       (clutch-db-show-create-object conn entry)))))

(defun clutch--object-type-string (entry)
  "Return ENTRY's object type string, defaulting to OBJECT."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (if (string-empty-p type) "OBJECT" type)))

(defun clutch--object-display-name (entry)
  "Return a display name for ENTRY."
  (or (plist-get entry :name) ""))

(defun clutch--object-supports-definition-p (entry)
  "Return non-nil when ENTRY supports DDL/source display."
  (member (clutch--object-type-string entry)
          '("TABLE" "VIEW" "SYNONYM" "INDEX" "SEQUENCE"
            "PROCEDURE" "FUNCTION" "TRIGGER")))

(defun clutch--object-supports-jump-target-p (entry)
  "Return non-nil when ENTRY supports forward relationship jumps."
  (member (clutch--object-type-string entry)
          '("SYNONYM" "INDEX" "TRIGGER")))

(defun clutch--object-summary-pairs (entry)
  "Return summary label/value pairs for object ENTRY."
  (let* ((type (clutch--object-type-string entry))
         (target-schema (plist-get entry :target-schema))
         (target-name (plist-get entry :target-name))
         (target (cond
                  ((and target-schema target-name)
                   (format "%s.%s" target-schema target-name))
                  ((plist-get entry :target-table)
                   (plist-get entry :target-table))
                  (target-name target-name)
                  (t nil))))
    (delq nil
          (list
           (cons "Name" (or (plist-get entry :name) ""))
           (cons "Type" type)
           (and (plist-get entry :source-schema)
                (cons "Source schema" (plist-get entry :source-schema)))
           (and (plist-get entry :schema)
                (not (equal (plist-get entry :schema)
                            (plist-get entry :source-schema)))
                (cons "Schema" (plist-get entry :schema)))
           (and target
                (cons (if (string= type "SYNONYM") "Target object" "Target")
                      target))
           (and (plist-get entry :status)
                (cons "Status" (plist-get entry :status)))
           (and (plist-get entry :timing)
                (cons "Timing" (plist-get entry :timing)))
           (and (plist-get entry :event)
                (cons "Event" (plist-get entry :event)))
           (and (eq (plist-get entry :unique) t)
                (cons "Unique" "yes"))
           (and (string= type "SEQUENCE")
                (cons "Range"
                      (format "%s -> %s"
                              (or (plist-get entry :min) "?")
                              (or (plist-get entry :max) "?"))))
           (and (string= type "SEQUENCE")
                (cons "Increment"
                      (format "%s" (or (plist-get entry :increment) "?"))))
           (and (string= type "SEQUENCE")
                (cons "Last value"
                      (format "%s" (or (plist-get entry :last) "?"))))
           (and (plist-get entry :identity)
                (cons "Identity" (plist-get entry :identity)))))))

(defun clutch--object-summary-lines (entry)
  "Return formatted summary lines for object ENTRY."
  (let* ((pairs (clutch--object-summary-pairs entry))
         (width (if pairs
                    (apply #'max (mapcar (lambda (pair) (length (car pair))) pairs))
                  0)))
    (mapcar (lambda (pair)
              (format (format "  %%-%ds  %%s" width)
                      (car pair)
                      (cdr pair)))
            pairs)))

(defun clutch--object-tag-string (parts)
  "Return a compact display string for non-empty PARTS."
  (string-join (seq-remove #'string-empty-p (delq nil parts)) ", "))

(defun clutch--object-format-column (column)
  "Return a display line for COLUMN."
  (let* ((name (or (plist-get column :name) ""))
         (type (or (plist-get column :type) ""))
         (fk (plist-get column :foreign-key))
         (fk-target (when fk
                      (format "FK -> %s.%s"
                              (plist-get fk :ref-table)
                              (plist-get fk :ref-column))))
         (tags (clutch--object-tag-string
                (list (unless (plist-get column :nullable) "NOT NULL")
                      (and (plist-get column :primary-key) "PK")
                      fk-target
                      (and (plist-get column :generated) "generated")
                      (and (plist-get column :default)
                           (format "default %s" (plist-get column :default)))
                      (plist-get column :comment)))))
    (string-trim-right
     (format "  %-18s  %-18s%s"
             name
             type
             (if (string-empty-p tags)
                 ""
               (format "  %s" tags))))))

(defun clutch--object-format-index-column (column)
  "Return a display line for index COLUMN."
  (string-trim-right
   (format "  %-18s  #%s  %s"
           (or (plist-get column :name) "")
           (or (plist-get column :position) "?")
           (or (plist-get column :descend) "ASC"))))

(defun clutch--object-format-routine-param (param)
  "Return a display line for routine PARAM."
  (string-trim-right
   (format "  %-18s  %-18s%s"
           (or (plist-get param :name)
               (plist-get param :mode)
               "")
           (or (plist-get param :type) "")
           (if-let* ((mode (plist-get param :mode))
                     ((not (string-empty-p mode))))
               (format "  %s" mode)
             ""))))

(defun clutch--object-related-entries (conn entry type)
  "Return related TYPE entries for table-like ENTRY on CONN."
  (when-let* ((name (plist-get entry :name))
              (objects (clutch--object-entries conn)))
    (seq-filter
     (lambda (candidate)
       (and (equal (clutch--normalize-object-type (plist-get candidate :type))
                   (clutch--normalize-object-type type))
            (string-equal-ignore-case
             (or (plist-get candidate :target-table) "")
             name)))
     objects)))

(defun clutch--object-describe-sections (conn entry)
  "Return describe sections for ENTRY on CONN."
  (pcase (clutch--object-type-string entry)
    ((or "TABLE" "VIEW")
     (let ((name (plist-get entry :name)))
       (delq nil
             (list
             (when-let* ((comment (and (string= (clutch--object-type-string entry) "TABLE")
                                        (clutch--ensure-table-comment conn name))))
                (cons "Comment" (list (format "  %s" comment))))
              (when-let* ((details (or (clutch--ensure-column-details conn name)
                                       (clutch-db-list-columns conn name))))
                (cons "Columns"
                      (mapcar (if (and details (plist-get (car-safe details) :name))
                                  #'clutch--object-format-column
                                (lambda (column) (format "  %s" column)))
                              details)))
              (when-let* ((indexes (clutch--object-related-entries conn entry "INDEX")))
                (cons "Indexes"
                      (mapcar (lambda (index)
                                (format "  %-18s%s"
                                        (or (plist-get index :name) "")
                                        (if (eq (plist-get index :unique) t)
                                            "  UNIQUE"
                                          "")))
                              indexes)))
              (when-let* ((triggers (clutch--object-related-entries conn entry "TRIGGER")))
                (cons "Triggers"
                      (mapcar (lambda (trigger)
                                (format "  %-18s  %s"
                                        (or (plist-get trigger :name) "")
                                        (clutch--object-tag-string
                                         (list (plist-get trigger :timing)
                                               (plist-get trigger :event)
                                               (plist-get trigger :status)))))
                              triggers)))))))
    ("INDEX"
     (when-let* ((details (clutch-db-object-details conn entry)))
       (list (cons "Columns" (mapcar #'clutch--object-format-index-column details)))))
    ((or "PROCEDURE" "FUNCTION")
     (when-let* ((details (clutch-db-object-details conn entry)))
       (list (cons "Parameters" (mapcar #'clutch--object-format-routine-param details)))))
    (_ nil)))

(defun clutch--object-describe-text (conn entry)
  "Return describe text for ENTRY on CONN."
  (let* ((header (format "%s (%s)"
                         (clutch--object-fqname entry)
                         (clutch--object-type-string entry)))
         (sections (delq nil
                         (append
                          (list (cons "Summary"
                                      (clutch--object-summary-lines entry)))
                          (clutch--object-describe-sections conn entry)))))
    (string-join
     (cons
      header
      (mapcar
       (lambda (section)
         (let* ((title (car section))
                (lines (cdr section))
                (count (length lines))
                (display-title (if (member title '("Summary" "Comment"))
                                   title
                                 (format "%s (%d)" title count))))
           (format "%s\n%s" display-title (string-join lines "\n"))))
       sections))
     "\n\n")))

(defun clutch--fontify-object-describe ()
  "Apply lightweight highlighting to the current describe buffer."
  (let ((inhibit-read-only t))
    (save-excursion
      (goto-char (point-min))
      (when (re-search-forward "^\\(.+\\) (\\([A-Z ]+\\))$" (line-end-position) t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-function-name-face t)
        (add-face-text-property (match-beginning 2) (match-end 2)
                                'font-lock-type-face t))
      (goto-char (point-min))
      (while (re-search-forward "^\\([A-Z][A-Za-z ]+\\(?: ([0-9]+)\\)?\\)$" nil t)
        (unless (= (match-beginning 1) (point-min))
          (add-face-text-property (match-beginning 1) (match-end 1)
                                  'font-lock-keyword-face t)))
      (goto-char (point-min))
      (while (re-search-forward
              "^  \\([A-Za-z][A-Za-z0-9/_-]*\\(?: [A-Za-z0-9/_-]+\\)*\\)\\(?:[[:blank:]]\\{2,\\}\\|:\\|$\\)"
              nil t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-variable-name-face t))
      (goto-char (point-min))
      (while (re-search-forward
              "\\_<\\(PK\\|FK\\|UNIQUE\\|VALID\\|INVALID\\|ENABLED\\|DISABLED\\|ASC\\|DESC\\|RETURN\\)\\_>"
              nil t)
        (add-face-text-property (match-beginning 1) (match-end 1)
                                'font-lock-constant-face t)))))

(defun clutch--render-object-describe (conn entry &optional params product)
  "Render describe content for ENTRY using CONN."
  (let ((inhibit-read-only t))
    (clutch-describe-mode)
    (clutch--bind-connection-context conn params product)
    (setq-local clutch-browser-current-object entry
                clutch--describe-object-entry entry
                clutch-describe--header-base
                (let ((icon (clutch--icon-with-face '(mdicon . "nf-md-table")
                                                    "▦" 'header-line)))
                  (if (string-empty-p icon)
                      " [s: show definition  C-c C-o: object actions  g: refresh]"
                    (format " %s  [s: show definition  C-c C-o: object actions  g: refresh]"
                            icon)))
                header-line-format
                '(:eval (clutch--header-with-disconnect-badge
                         clutch-describe--header-base))
                revert-buffer-function #'clutch-describe-refresh)
    (erase-buffer)
    (insert (clutch--object-describe-text conn entry))
    (insert "\n")
    (clutch--fontify-object-describe)
    (goto-char (point-min))))

(defun clutch--show-object-describe-buffer (conn entry &optional params product)
  "Display a describe buffer for ENTRY using CONN."
  (let ((buf (get-buffer-create
              (format "*clutch describe: %s*" (clutch--object-fqname entry)))))
    (with-current-buffer buf
      (clutch--render-object-describe conn entry params product))
    (pop-to-buffer buf '((display-buffer-at-bottom)))))

(defun clutch-describe-refresh (&optional _ignore-auto _noconfirm)
  "Refresh the current describe buffer."
  (interactive)
  (unless clutch--describe-object-entry
    (user-error "No object is associated with this buffer"))
  (clutch--refresh-current-schema (not (called-interactively-p 'interactive)))
  (clutch--render-object-describe clutch-connection
                                  clutch--describe-object-entry
                                  clutch--connection-params
                                  clutch--conn-sql-product))

(defvar clutch-describe-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "s") #'clutch-object-show-ddl-or-source)
    (define-key map (kbd "C-c C-d") #'clutch-describe-dwim)
    (define-key map (kbd "C-c C-o") #'clutch-act-dwim)
    (define-key map (kbd "g") #'clutch-describe-refresh)
    map)
  "Keymap for `clutch-describe-mode'.")

(defvar-local clutch-describe--header-base nil
  "Cached describe header string, set during render.")

(define-derived-mode clutch-describe-mode special-mode "clutch-describe"
  "Major mode for clutch object describe buffers.")

(defun clutch-object-show-ddl-or-source (&optional entry)
  "Show DDL or source for ENTRY."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Show definition: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (name (clutch--object-display-name entry))
         (type (clutch--object-type-string entry))
         (text (clutch--object-definition-text conn entry)))
    (unless (clutch--object-supports-definition-p entry)
      (user-error "%s %s does not expose a definition"
                  type name))
    (unless text
      (user-error "DDL/source unavailable for %s %s" type name))
    (clutch--remember-current-object entry)
    (clutch--show-object-text-buffer conn entry text
                                     (plist-get context :params)
                                     (plist-get context :product))))

(defun clutch-object-describe (&optional entry)
  "Show a describe buffer for ENTRY."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Describe object: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection"))))
    (clutch--remember-current-object entry)
    (clutch--show-object-describe-buffer conn entry
                                         (plist-get context :params)
                                         (plist-get context :product))))

(defun clutch-object-browse (&optional entry)
  "Insert SELECT * FROM the current table-like object into a query console."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Browse object: " t)))
         (type (clutch--object-type-string entry)))
    (unless (clutch--table-like-entry-p entry)
      (user-error "%s %s does not support row browsing"
                  type (clutch--object-display-name entry)))
    (clutch--remember-current-object entry)
    (let* ((conn (or clutch-connection
                     (plist-get context :connection)
                     (user-error "No active connection")))
           (sql (format "SELECT * FROM %s;"
                        (clutch-db-escape-identifier conn
                                                     (plist-get entry :name))))
           (console (or (and (derived-mode-p 'clutch-mode)
                             (eq clutch-connection conn)
                             (current-buffer))
                        (clutch--find-console-for-conn conn)
                        (user-error "No query console open for this connection"))))
      (pop-to-buffer console)
      (unless (eq (current-buffer) console)
        (goto-char (point-max)))
      (clutch--insert-console-sql-block sql))))

(defun clutch--filter-object-target-matches (entries &optional schema)
  "Return target ENTRIES, optionally preferring SCHEMA matches."
  (let ((entries (clutch--merge-object-entries entries)))
    (if (and schema (not (string-empty-p schema)))
        (or (seq-filter
             (lambda (entry)
               (or (string-equal-ignore-case schema (or (plist-get entry :schema) ""))
                   (string-equal-ignore-case schema (or (plist-get entry :source-schema) ""))))
             entries)
            entries)
      entries)))

(defun clutch--resolve-object-targets (conn entry)
  "Return target object entries for ENTRY on CONN."
  (let ((type (upcase (or (plist-get entry :type) ""))))
    (pcase type
      ((or "INDEX" "TRIGGER")
       (let* ((target-table (plist-get entry :target-table))
              (matches (and target-table
                            (clutch--object-matches-by-name conn target-table t))))
         (or matches
             (when target-table
               (list (list :name target-table
                           :type "TABLE"
                           :schema (plist-get entry :schema)
                           :source-schema (plist-get entry :source-schema)))))))
      ("SYNONYM"
       (when-let* ((target-name (plist-get entry :target-name)))
         (clutch--filter-object-target-matches
          (clutch--object-matches-by-name conn target-name)
          (plist-get entry :target-schema))))
      (_ nil))))

(defun clutch--read-object-target (conn entry)
  "Read a target object for ENTRY on CONN."
  (unless (clutch--object-supports-jump-target-p entry)
    (user-error "%s %s has no target object"
                (clutch--object-type-string entry)
                (clutch--object-display-name entry)))
  (let ((targets (clutch--resolve-object-targets conn entry)))
    (pcase (length targets)
      (0 (user-error "No target object for %s %s"
                     (clutch--object-type-string entry)
                     (clutch--object-display-name entry)))
      (1 (car targets))
      (_ (clutch--object-entry-reader conn "Jump to target: " targets)))))

(defun clutch-object-jump-target (&optional entry)
  "Jump from ENTRY to its target object."
  (interactive)
  (let* ((context (clutch--command-connection-context))
         (entry (or entry
                    (clutch--resolve-object-entry "Jump from object: ")))
         (conn (or clutch-connection
                   (plist-get context :connection)
                   (user-error "No active connection")))
         (target (clutch--read-object-target conn entry)))
    (clutch--remember-current-object target)
    (clutch-object-describe target)))

(defun clutch-object-default-action (&optional entry)
  "Run the default action for ENTRY."
  (interactive)
  (let ((entry (or entry
                   (clutch--resolve-object-entry "Object: "))))
    (clutch--run-object-action entry (clutch--object-default-action-id entry))))

(defun clutch--resolve-object-dwim (&optional prompt table-like-only category allowed-types)
  "Resolve a clutch object from point or prompt.
PROMPT is passed to the fallback reader.  When TABLE-LIKE-ONLY is non-nil,
only resolve table-like objects."
  (clutch--resolve-object-entry (or prompt "Object: ")
                                table-like-only
                                category
                                allowed-types))

(defun clutch-copy-object-name (&optional entry)
  "Copy the object name from ENTRY to the kill ring."
  (interactive)
  (let* ((entry (or entry
                    (clutch--resolve-object-entry "Copy object name: ")))
         (name (plist-get entry :name)))
    (kill-new name)
    (clutch--remember-current-object entry)
    (message "Copied object name: %s" name)))

(defun clutch-copy-object-fqname (&optional entry)
  "Copy the fully qualified object name from ENTRY to the kill ring."
  (interactive)
  (let* ((entry (or entry
                    (clutch--resolve-object-entry "Copy object fqname: ")))
         (fqname (clutch--object-fqname entry)))
    (kill-new fqname)
    (clutch--remember-current-object entry)
    (message "Copied object fqname: %s" fqname)))

(defun clutch-describe-table (table)
  "Describe TABLE using the unified object workflow."
  (interactive
   (list (if-let* ((schema (clutch--schema-for-connection)))
             (progn
               (clutch--warn-schema-cache-state)
               (clutch--read-table-name "Table: " (hash-table-keys schema)))
           (read-string "Table: "))))
  (clutch-object-describe (list :name table :type "TABLE")))

(defun clutch-describe-table-at-point ()
  "Describe the object at point, or prompt when none is resolved."
  (interactive)
  (clutch-describe-dwim))

(defun clutch-browse-table (table)
  "Insert SELECT * FROM TABLE at the end of a query console."
  (interactive
   (list (plist-get (clutch-object-read "Browse object: " t) :name)))
  (clutch-object-browse (list :name table :type "TABLE")))


(defconst clutch--object-action-registry
  '((:id describe
     :key "d"
     :label "Describe object"
     :command clutch-object-describe)
    (:id show-definition
     :key "s"
     :label "Show definition"
     :command clutch-object-show-ddl-or-source
     :predicate clutch--object-supports-definition-p)
    (:id browse
     :key "b"
     :label "Browse rows"
     :command clutch-object-browse
     :predicate clutch--table-like-entry-p)
    (:id jump-target
     :key "j"
     :label "Jump target"
     :command clutch-object-jump-target
     :predicate clutch--object-supports-jump-target-p)
    (:id copy-name
     :key "n"
     :label "Copy name"
     :command clutch-copy-object-name)
    (:id copy-fqname
     :key "f"
     :label "Copy fqname"
     :command clutch-copy-object-fqname))
  "Shared object action registry for clutch object workflows.")

(defun clutch--object-action-spec (action-id)
  "Return the action spec for ACTION-ID."
  (or (seq-find (lambda (spec)
                  (eq (plist-get spec :id) action-id))
                clutch--object-action-registry)
      (error "Unknown clutch object action: %s" action-id)))

(defun clutch--object-action-label (action-id)
  "Return the display label for ACTION-ID."
  (plist-get (clutch--object-action-spec action-id) :label))

(defun clutch--object-action-command (action-id)
  "Return the command implementing ACTION-ID."
  (plist-get (clutch--object-action-spec action-id) :command))

(defun clutch--object-action-available-p (entry action-id)
  "Return non-nil when ACTION-ID is applicable to ENTRY."
  (if-let* ((predicate (plist-get (clutch--object-action-spec action-id) :predicate)))
      (funcall predicate entry)
    t))

(defun clutch--object-default-action-id (entry)
  "Return the default action id for ENTRY."
  (if (clutch--table-like-entry-p entry)
      'browse
    'show-definition))

(defun clutch--run-object-action (entry action-id)
  "Run ACTION-ID for ENTRY."
  (unless (clutch--object-action-available-p entry action-id)
    (user-error "%s does not support %s"
                (clutch--object-fqname entry)
                (downcase (clutch--object-action-label action-id))))
  (clutch--remember-current-object entry)
  (funcall (clutch--object-action-command action-id) entry))

(defun clutch--object-action-target ()
  "Return the current object action target, prompting when needed."
  (or clutch--object-action-entry
      (setq clutch--object-action-entry
            (clutch--resolve-object-dwim "Object actions for: "))))

(defun clutch--object-act-jump-target-inapt-p ()
  "Return non-nil when forward jumps are unavailable for the action target."
  (let ((entry clutch--object-action-entry))
    (or (null entry)
        (not (clutch--object-supports-jump-target-p entry)))))

(defun clutch--object-act-describe ()
  "Describe the current object action target."
  (interactive)
  (clutch--run-object-action (clutch--object-action-target) 'describe))

(defun clutch--object-act-show-definition ()
  "Show DDL or source for the current object action target."
  (interactive)
  (clutch--run-object-action (clutch--object-action-target) 'show-definition))

(transient-define-suffix clutch--object-act-jump-target ()
  "Jump from the current object action target to its target object."
  :inapt-if #'clutch--object-act-jump-target-inapt-p
  (interactive)
  (clutch--run-object-action (clutch--object-action-target) 'jump-target))

(defun clutch--object-act-copy-name ()
  "Copy the name of the current object action target."
  (interactive)
  (clutch--run-object-action (clutch--object-action-target) 'copy-name))

(defun clutch--object-act-copy-fqname ()
  "Copy the fully qualified name of the current object action target."
  (interactive)
  (clutch--run-object-action (clutch--object-action-target) 'copy-fqname))

(transient-define-prefix clutch-object-actions-menu ()
  "Transient fallback for clutch object actions."
  [["Open"
    ("d" (lambda () (clutch--object-action-label 'describe))
     clutch--object-act-describe)
    ("s" (lambda () (clutch--object-action-label 'show-definition))
     clutch--object-act-show-definition)]
  ["Navigate"
    ("j" (lambda () (clutch--object-action-label 'jump-target))
     clutch--object-act-jump-target)]
   ["Copy"
    ("n" (lambda () (clutch--object-action-label 'copy-name))
     clutch--object-act-copy-name)
    ("f" (lambda () (clutch--object-action-label 'copy-fqname))
     clutch--object-act-copy-fqname)]
   ["Cache"
    ("g" "Refresh schema" clutch-refresh-schema)]])

(defun clutch--present-object-actions-natively (entry)
  "Present actions for ENTRY via clutch's native action UI."
  (setq clutch--object-action-entry entry)
  (clutch--remember-current-object entry)
  (cond
   ((fboundp 'transient-setup)
    (transient-setup 'clutch-object-actions-menu)
    t)
   (t nil)))

(defun clutch-act-dwim (&optional entry)
  "Resolve an object and present its action UI."
  (interactive)
  (let ((entry (or entry
                   (clutch--resolve-object-dwim "Object actions for: "))))
    (unless (clutch--present-object-actions-natively entry)
      (user-error "No object action UI is available"))))

(defun clutch-jump (&optional entry)
  "Resolve an object and run its default action."
  (interactive)
  (let ((entry (or entry
                   (clutch--resolve-object-dwim "Jump to object: "
                                                nil nil
                                                clutch-primary-object-types))))
    (clutch--run-object-action entry (clutch--object-default-action-id entry))))

(defun clutch-describe-dwim (&optional entry)
  "Resolve an object and open its describe view."
  (interactive)
  (clutch--run-object-action
   (or entry
       (clutch--resolve-object-dwim "Describe object: "))
   'describe))

(defun clutch--embark-action-specs (&optional predicate)
  "Return object actions that should appear in Embark menus.
When PREDICATE is non-nil, keep only action specs matching it."
  (seq-filter
   (lambda (spec)
     (and (not (eq (plist-get spec :id) 'browse))
          (or (null predicate)
              (funcall predicate spec))))
   clutch--object-action-registry))

(defun clutch--embark-target-type (entry)
  "Return the Embark target type for ENTRY."
  (if (clutch--object-supports-jump-target-p entry)
      'clutch-target-object
    'clutch-object))

(defconst clutch--embark-command-labels
  '((clutch-object-default-action . "Default action")
    (clutch-object-describe . "Describe object")
    (clutch-object-show-ddl-or-source . "Show definition")
    (clutch-object-jump-target . "Jump target")
    (clutch-copy-object-name . "Copy name")
    (clutch-copy-object-fqname . "Copy fqname"))
  "Display labels for clutch commands shown through Embark.")

(defun clutch--embark-command-label (cmd)
  "Return Embark display label for clutch command CMD, or nil."
  (alist-get cmd clutch--embark-command-labels))

(defun clutch--embark-command-name-advice (orig cmd)
  "Return a clutch-specific display name for CMD, delegating to ORIG otherwise."
  (or (clutch--embark-command-label cmd)
      (funcall orig cmd)))

;;;; Embark integration (optional)

(defun clutch--embark-object-target ()
  "Return an Embark target for the clutch object at point."
  (when (or clutch--object-dispatch-entry
            (clutch--connection-alive-p clutch-connection))
    (if clutch--object-dispatch-entry
        `(,(clutch--embark-target-type clutch--object-dispatch-entry)
          ,clutch--object-dispatch-entry)
      (pcase major-mode
        ((or 'clutch-mode 'clutch-repl-mode)
         (when-let* ((entry (clutch-object-at-point))
                     (bounds (bounds-of-thing-at-point 'symbol)))
           `(,(clutch--embark-target-type entry)
             ,entry ,(car bounds) ,(cdr bounds))))
        (_
         (when clutch-browser-current-object
           `(,(clutch--embark-target-type clutch-browser-current-object)
             ,clutch-browser-current-object)))))))

(with-eval-after-load 'embark
  (add-to-list 'embark-default-action-overrides
               '(clutch-object . clutch-object-default-action))
  (add-to-list 'embark-default-action-overrides
               '(clutch-target-object . clutch-object-default-action))
  (advice-add 'embark--command-name :around #'clutch--embark-command-name-advice)

  (defvar clutch-embark-object-actions
    (let ((map (make-sparse-keymap)))
      (dolist (spec (clutch--embark-action-specs
                     (lambda (spec)
                       (not (eq (plist-get spec :id) 'jump-target)))))
        (define-key map
                    (kbd (plist-get spec :key))
                    (plist-get spec :command)))
      map)
    "Embark actions for clutch objects.")

  (defvar clutch-embark-target-object-actions
    (let ((map (make-sparse-keymap)))
      (dolist (spec (clutch--embark-action-specs))
        (define-key map
                    (kbd (plist-get spec :key))
                    (plist-get spec :command)))
      map)
    "Embark actions for clutch objects with explicit targets.")

  (add-to-list 'embark-target-finders #'clutch--embark-object-target)
  (add-to-list 'embark-keymap-alist
               '(clutch-object . clutch-embark-object-actions))
  (add-to-list 'embark-keymap-alist
               '(clutch-target-object . clutch-embark-target-object-actions)))

(provide 'clutch-object)
;;; clutch-object.el ends here
