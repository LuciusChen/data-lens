;;; mysql.el --- Pure Elisp MySQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; Author: Lucius Chen
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: databases, mysql, comm
;; URL: https://github.com/LuciusChen/mysql.el

;; This file is part of mysql.el.

;; mysql.el is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; mysql.el is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with mysql.el.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; A pure Emacs Lisp MySQL client that implements the MySQL wire protocol
;; directly, without depending on the external `mysql' CLI.
;;
;; Usage:
;;
;;   (require 'mysql)
;;
;;   (let ((conn (mysql-connect :host "127.0.0.1"
;;                              :port 3306
;;                              :user "root"
;;                              :password "secret"
;;                              :database "mydb")))
;;     (let ((result (mysql-query conn "SELECT * FROM users LIMIT 10")))
;;       (mysql-result-rows result))
;;     (mysql-disconnect conn))

;;; Code:

(require 'cl-lib)

;;;; Error types

(define-error 'mysql-error "MySQL error")
(define-error 'mysql-connection-error "MySQL connection error" 'mysql-error)
(define-error 'mysql-protocol-error "MySQL protocol error" 'mysql-error)
(define-error 'mysql-auth-error "MySQL authentication error" 'mysql-error)
(define-error 'mysql-query-error "MySQL query error" 'mysql-error)
(define-error 'mysql-timeout "MySQL timeout" 'mysql-error)
(define-error 'mysql-stmt-error "MySQL prepared statement error" 'mysql-error)

;;;; Capability flags (MySQL protocol)

(defconst mysql--cap-long-password        #x00000001)
(defconst mysql--cap-found-rows           #x00000002)
(defconst mysql--cap-long-flag            #x00000004)
(defconst mysql--cap-connect-with-db      #x00000008)
(defconst mysql--cap-protocol-41          #x00000200)
(defconst mysql--cap-transactions         #x00002000)
(defconst mysql--cap-secure-connection    #x00008000)
(defconst mysql--cap-ssl                  #x00000800)
(defconst mysql--cap-plugin-auth          #x00080000)
(defconst mysql--cap-plugin-auth-lenenc   #x00200000)
(defconst mysql--cap-deprecate-eof        #x01000000)

;;;; TLS configuration

(defgroup mysql nil
  "Pure Elisp MySQL client."
  :group 'comm
  :prefix "mysql-")

(defcustom mysql-tls-trustfiles nil
  "List of CA certificate file paths for TLS verification."
  :type '(repeat file)
  :group 'mysql)

(defcustom mysql-tls-keylist nil
  "List of (CERT-FILE KEY-FILE) pairs for client certificates."
  :type '(repeat (list file file))
  :group 'mysql)

(defcustom mysql-tls-verify-server t
  "Whether to verify the server certificate during TLS handshake."
  :type 'boolean
  :group 'mysql)

;;;; Column type constants

(defconst mysql-type-decimal     0)
(defconst mysql-type-tiny        1)
(defconst mysql-type-short       2)
(defconst mysql-type-long        3)
(defconst mysql-type-float       4)
(defconst mysql-type-double      5)
(defconst mysql-type-null        6)
(defconst mysql-type-timestamp   7)
(defconst mysql-type-longlong    8)
(defconst mysql-type-int24       9)
(defconst mysql-type-date       10)
(defconst mysql-type-time       11)
(defconst mysql-type-datetime   12)
(defconst mysql-type-year       13)
(defconst mysql-type-varchar    15)
(defconst mysql-type-bit        16)
(defconst mysql-type-json      245)
(defconst mysql-type-newdecimal 246)
(defconst mysql-type-enum      247)
(defconst mysql-type-set       248)
(defconst mysql-type-tiny-blob  249)
(defconst mysql-type-medium-blob 250)
(defconst mysql-type-long-blob  251)
(defconst mysql-type-blob       252)
(defconst mysql-type-var-string 253)
(defconst mysql-type-string     254)
(defconst mysql-type-geometry   255)

;;;; Data structures

(cl-defstruct mysql-conn
  "A MySQL connection object."
  process
  (buf nil)
  host port user database
  server-version
  connection-id
  capability-flags
  character-set
  status-flags
  (read-timeout 10)
  (sequence-id 0)
  tls
  (busy nil))

(cl-defstruct mysql-result
  "A MySQL query result."
  connection
  status
  columns
  rows
  affected-rows
  last-insert-id
  warnings)

;;;; Low-level I/O primitives

(defun mysql--ensure-data (conn n)
  "Ensure at least N bytes are available in CONN's input buffer.
Waits for data from the process, signaling `mysql-timeout' if
no new data arrives within `mysql-conn-read-timeout' seconds.
The timeout resets each time data is received, so large result
sets that stream continuously will not time out."
  (let* ((proc (mysql-conn-process conn))
         (buf (mysql-conn-buf conn))
         (timeout (mysql-conn-read-timeout conn))
         (deadline (+ (float-time) timeout)))
    (with-current-buffer buf
      (while (< (- (point-max) (point-min)) n)
        (let ((remaining (- deadline (float-time)))
              (prev-size (- (point-max) (point-min))))
          (when (<= remaining 0)
            (signal 'mysql-timeout
                    (list (format "Timed out waiting for %d bytes" n))))
          (if (accept-process-output proc remaining nil t)
              ;; Data arrived — reset the idle deadline.
              (when (> (- (point-max) (point-min)) prev-size)
                (setq deadline (+ (float-time) timeout)))
            ;; No new output from proc.  If the process exited, flush
            ;; any remaining queued output from the OS before giving up.
            (when (not (process-live-p proc))
              (accept-process-output nil 0.05 nil t)
              (when (< (- (point-max) (point-min)) n)
                (signal 'mysql-connection-error
                        (list "Connection closed by server"))))))))))

(defun mysql--read-bytes (conn n)
  "Read N bytes from CONN's input buffer as a unibyte string."
  (mysql--ensure-data conn n)
  (with-current-buffer (mysql-conn-buf conn)
    (goto-char (point-min))
    (let ((start (point)))
      (forward-char n)
      (let ((str (buffer-substring-no-properties start (point))))
        (delete-region start (point))
        str))))

(defun mysql--read-byte (conn)
  "Read a single byte from CONN, returning it as an integer."
  (aref (mysql--read-bytes conn 1) 0))

(defun mysql--read-int-le (conn n)
  "Read an N-byte little-endian integer from CONN."
  (let ((bytes (mysql--read-bytes conn n))
        (val 0))
    (dotimes (i n val)
      (setq val (logior val (ash (aref bytes i) (* i 8)))))))

(defun mysql--read-lenenc-int (conn)
  "Read a length-encoded integer from CONN."
  (let ((first (mysql--read-byte conn)))
    (cond
     ((< first #xfb) first)
     ((= first #xfc) (mysql--read-int-le conn 2))
     ((= first #xfd) (mysql--read-int-le conn 3))
     ((= first #xfe) (mysql--read-int-le conn 8))
     (t (signal 'mysql-protocol-error
                (list (format "Invalid lenenc-int prefix: 0x%02x" first)))))))

(defun mysql--read-string-nul (conn)
  "Read a NUL-terminated string from CONN."
  (let ((chars nil))
    (cl-loop for b = (mysql--read-byte conn)
             until (= b 0)
             do (push b chars))
    (apply #'unibyte-string (nreverse chars))))

(defun mysql--read-string-lenenc (conn)
  "Read a length-encoded string from CONN."
  (let ((len (mysql--read-lenenc-int conn)))
    (if (zerop len) ""
      (mysql--read-bytes conn len))))

;;;; Packet I/O

(defun mysql--read-packet (conn)
  "Read one MySQL packet from CONN.
Returns the payload as a unibyte string.  Handles packets split across
multiple 16 MB fragments."
  (let ((payload nil)
        (more t))
    (while more
      (let* ((len (mysql--read-int-le conn 3))
             (seq (mysql--read-byte conn)))
        (setf (mysql-conn-sequence-id conn) (logand (1+ seq) #xff))
        (let ((data (mysql--read-bytes conn len)))
          (push data payload))
        (setq more (= len #xffffff))))
    (apply #'concat (nreverse payload))))

(defun mysql--int-le-bytes (value n)
  "Encode VALUE as an N-byte little-endian unibyte string."
  (let ((bytes (make-string n 0)))
    (dotimes (i n bytes)
      (aset bytes i (logand (ash value (* i -8)) #xff)))))

(defun mysql--lenenc-int-bytes (value)
  "Encode VALUE as a length-encoded integer unibyte string."
  (cond
   ((< value #xfb)
    (unibyte-string value))
   ((<= value #xffff)
    (concat (unibyte-string #xfc) (mysql--int-le-bytes value 2)))
   ((<= value #xffffff)
    (concat (unibyte-string #xfd) (mysql--int-le-bytes value 3)))
   (t
    (concat (unibyte-string #xfe) (mysql--int-le-bytes value 8)))))

(defun mysql--send-packet (conn payload)
  "Send PAYLOAD as a MySQL packet on CONN.
Automatically prepends the 4-byte header (3-byte length + 1-byte sequence id).
Handles splitting payloads larger than 0xFFFFFF."
  (let ((proc (mysql-conn-process conn))
        (offset 0)
        (total (length payload)))
    (while (>= (- total offset) #xffffff)
      (let ((header (concat (mysql--int-le-bytes #xffffff 3)
                            (unibyte-string (mysql-conn-sequence-id conn)))))
        (setf (mysql-conn-sequence-id conn)
              (logand (1+ (mysql-conn-sequence-id conn)) #xff))
        (process-send-string proc header)
        (process-send-string proc (substring payload offset (+ offset #xffffff)))
        (cl-incf offset #xffffff)))
    (let* ((remaining (- total offset))
           (header (concat (mysql--int-le-bytes remaining 3)
                           (unibyte-string (mysql-conn-sequence-id conn)))))
      (setf (mysql-conn-sequence-id conn)
            (logand (1+ (mysql-conn-sequence-id conn)) #xff))
      (process-send-string proc header)
      (when (> remaining 0)
        (process-send-string proc (substring payload offset))))))

;;;; Authentication helpers

(defun mysql--sha1 (data)
  "Return the SHA-1 hash of DATA (a unibyte string) as a unibyte string."
  (let ((hex (secure-hash 'sha1 data nil nil t)))
    hex))

(defun mysql--xor-strings (a b)
  "XOR two equal-length unibyte strings A and B."
  (let* ((len (length a))
         (result (make-string len 0)))
    (dotimes (i len result)
      (aset result i (logxor (aref a i) (aref b i))))))

(defun mysql--auth-mysql-native-password (password salt)
  "Compute mysql_native_password auth response.
PASSWORD is a plain-text string.  SALT is the 20-byte auth data from
the server handshake.  Returns a 20-byte unibyte string.

Algorithm: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))"
  (if (or (null password) (string-empty-p password))
      ""
    (let* ((pass-bytes (encode-coding-string password 'utf-8))
           (sha1-pass (mysql--sha1 pass-bytes))
           (sha1-sha1-pass (mysql--sha1 sha1-pass))
           (concat-salt (concat salt sha1-sha1-pass))
           (sha1-concat (mysql--sha1 concat-salt)))
      (mysql--xor-strings sha1-pass sha1-concat))))

(defun mysql--auth-caching-sha2-password (password salt)
  "Compute caching_sha2_password auth response.
PASSWORD is a plain-text string.  SALT is the 20-byte nonce.
Returns a 32-byte unibyte string.

Algorithm: SHA256(password) XOR SHA256(SHA256(SHA256(password)) + salt)"
  (if (or (null password) (string-empty-p password))
      ""
    (let* ((pass-bytes (encode-coding-string password 'utf-8))
           (sha256-pass (secure-hash 'sha256 pass-bytes nil nil t))
           (sha256-sha256-pass (secure-hash 'sha256 sha256-pass nil nil t))
           (concat-data (concat sha256-sha256-pass salt))
           (sha256-concat (secure-hash 'sha256 concat-data nil nil t)))
      (mysql--xor-strings sha256-pass sha256-concat))))

;;;; Handshake

(defun mysql--read-le-uint (packet pos n)
  "Read an N-byte little-endian unsigned integer from PACKET at POS."
  (let ((val 0))
    (dotimes (i n val)
      (setq val (logior val (ash (aref packet (+ pos i)) (* i 8)))))))

(defun mysql--parse-handshake-salt (conn packet pos salt-part1 auth-data-len)
  "Parse the second salt part and auth plugin from handshake PACKET at POS.
CONN, SALT-PART1, and AUTH-DATA-LEN provide context.
Returns a plist with :salt and :auth-plugin."
  (let ((salt-part2 ""))
    (when (not (zerop (logand (mysql-conn-capability-flags conn)
                              mysql--cap-secure-connection)))
      (let ((part2-len (max 13 (- auth-data-len 8))))
        (setq salt-part2 (substring packet pos (+ pos part2-len)))
        (cl-incf pos part2-len)
        ;; Remove trailing NUL if present
        (when (and (> (length salt-part2) 0)
                   (= (aref salt-part2 (1- (length salt-part2))) 0))
          (setq salt-part2 (substring salt-part2 0 -1)))))
    (let ((auth-plugin nil))
      (when (not (zerop (logand (mysql-conn-capability-flags conn)
                                mysql--cap-plugin-auth)))
        (when-let* ((nul-pos (cl-position 0 packet :start pos)))
          (setq auth-plugin (substring packet pos nul-pos))))
      (list :salt (concat salt-part1 salt-part2)
            :auth-plugin (or auth-plugin "mysql_native_password")))))

(defun mysql--parse-handshake (conn packet)
  "Parse a HandshakeV10 PACKET and update CONN with server info.
Returns a plist with :salt and :auth-plugin."
  (let ((pos 0))
    (unless (= (aref packet 0) 10)
      (signal 'mysql-protocol-error
              (list (format "Unsupported protocol version: %d" (aref packet 0)))))
    (cl-incf pos)
    ;; server_version: NUL-terminated string
    (let ((nul-pos (cl-position 0 packet :start pos)))
      (setf (mysql-conn-server-version conn)
            (substring packet pos nul-pos))
      (setq pos (1+ nul-pos)))
    (setf (mysql-conn-connection-id conn) (mysql--read-le-uint packet pos 4))
    (cl-incf pos 4)
    ;; auth_plugin_data_part_1: 8 bytes
    (let ((salt-part1 (substring packet pos (+ pos 8))))
      (cl-incf pos 8)
      (cl-incf pos 1) ;; filler
      (let ((cap-low (mysql--read-le-uint packet pos 2)))
        (cl-incf pos 2)
        (setf (mysql-conn-character-set conn) (aref packet pos))
        (cl-incf pos 1)
        (setf (mysql-conn-status-flags conn) (mysql--read-le-uint packet pos 2))
        (cl-incf pos 2)
        (let ((cap-high (mysql--read-le-uint packet pos 2)))
          (cl-incf pos 2)
          (setf (mysql-conn-capability-flags conn)
                (logior cap-low (ash cap-high 16)))
          (let ((auth-data-len (aref packet pos)))
            (cl-incf pos 1)
            (cl-incf pos 10) ;; reserved
            (mysql--parse-handshake-salt
             conn packet pos salt-part1 auth-data-len)))))))

(defun mysql--client-capabilities (conn)
  "Compute the client capability flags for CONN."
  (logior mysql--cap-long-password
          mysql--cap-found-rows
          mysql--cap-long-flag
          mysql--cap-protocol-41
          mysql--cap-transactions
          mysql--cap-secure-connection
          mysql--cap-plugin-auth
          (if (mysql-conn-tls conn) mysql--cap-ssl 0)
          (if (mysql-conn-database conn) mysql--cap-connect-with-db 0)))

(defun mysql--build-handshake-response (conn password salt auth-plugin)
  "Build a HandshakeResponse41 packet for CONN.
PASSWORD is the user password, SALT is the server nonce, AUTH-PLUGIN
is the authentication plugin name."
  (let* ((client-flags (mysql--client-capabilities conn))
         (auth-response (mysql--compute-auth-response password salt auth-plugin))
         (parts nil))
    (setf (mysql-conn-capability-flags conn)
          (logand client-flags (mysql-conn-capability-flags conn)))
    (push (mysql--int-le-bytes client-flags 4) parts)
    (push (mysql--int-le-bytes #x00ffffff 4) parts)  ;; max_packet_size
    (push (unibyte-string 45) parts)                  ;; charset: utf8mb4
    (push (make-string 23 0) parts)                   ;; filler
    (push (concat (encode-coding-string (mysql-conn-user conn) 'utf-8)
                  (unibyte-string 0))
          parts)
    (push (unibyte-string (length auth-response)) parts)
    (push auth-response parts)
    (when (mysql-conn-database conn)
      (push (concat (encode-coding-string (mysql-conn-database conn) 'utf-8)
                    (unibyte-string 0))
            parts))
    (push (concat (encode-coding-string auth-plugin 'utf-8)
                  (unibyte-string 0))
          parts)
    (apply #'concat (nreverse parts))))

(defun mysql--compute-auth-response (password salt auth-plugin)
  "Compute auth response for the given AUTH-PLUGIN.
PASSWORD is the plaintext password, SALT is the server nonce."
  (pcase auth-plugin
    ("mysql_native_password"
     (mysql--auth-mysql-native-password password salt))
    ("caching_sha2_password"
     (mysql--auth-caching-sha2-password password salt))
    (_
     (signal 'mysql-auth-error
             (list (format "Unsupported auth plugin: %s" auth-plugin))))))

;;;; Response parsing helpers

(defun mysql--parse-ok-packet (packet)
  "Parse an OK_Packet from PACKET (first byte 0x00 already verified).
Returns a plist with :affected-rows, :last-insert-id, :status-flags, :warnings."
  (let* ((r1 (mysql--read-lenenc-int-from-string packet 1))
         (affected-rows (car r1))
         (r2 (mysql--read-lenenc-int-from-string packet (cdr r1)))
         (last-insert-id (car r2))
         (pos (cdr r2))
         (status-flags (when (< (1+ pos) (length packet))
                         (prog1 (logior (aref packet pos)
                                        (ash (aref packet (+ pos 1)) 8))
                           (cl-incf pos 2))))
         (warnings (when (< (1+ pos) (length packet))
                     (logior (aref packet pos)
                             (ash (aref packet (+ pos 1)) 8)))))
    (list :affected-rows affected-rows
          :last-insert-id last-insert-id
          :status-flags status-flags
          :warnings warnings)))

(defun mysql--parse-err-packet (packet)
  "Parse an ERR_Packet from PACKET (first byte 0xFF already verified).
Returns a plist with :code, :state, :message."
  (let* ((code (logior (aref packet 1) (ash (aref packet 2) 8)))
         (pos 3)
         state message)
    ;; SQL state marker '#' + 5-byte state (if CLIENT_PROTOCOL_41)
    (when (and (< pos (length packet))
               (= (aref packet pos) ?#))
      (cl-incf pos 1)
      (setq state (substring packet pos (min (+ pos 5) (length packet))))
      (cl-incf pos 5))
    (setq message (decode-coding-string (substring packet pos) 'utf-8))
    (list :code code :state state :message message)))

(defun mysql--parse-eof-packet (_packet)
  "Parse an EOF_Packet.  Returns t."
  t)

(defun mysql--packet-type (packet)
  "Determine the type of PACKET from its first byte.
Returns one of: ok, err, eof, local-infile, or data."
  (let ((first (aref packet 0)))
    (cond
     ((= first #x00) 'ok)
     ((= first #xff) 'err)
     ((and (= first #xfe) (<= (length packet) 9)) 'eof)
     ((= first #xfb) 'local-infile)
     (t 'data))))

;;;; Column definition parsing

(defun mysql--read-lenenc-int-from-string (str pos)
  "Read a length-encoded integer from STR at POS.
Returns (value . new-pos)."
  (let ((first (aref str pos)))
    (cond
     ((< first #xfb)
      (cons first (1+ pos)))
     ((= first #xfc)
      (cons (logior (aref str (+ pos 1))
                    (ash (aref str (+ pos 2)) 8))
            (+ pos 3)))
     ((= first #xfd)
      (cons (logior (aref str (+ pos 1))
                    (ash (aref str (+ pos 2)) 8)
                    (ash (aref str (+ pos 3)) 16))
            (+ pos 4)))
     ((= first #xfe)
      (let ((val 0))
        (dotimes (i 8)
          (setq val (logior val (ash (aref str (+ pos 1 i)) (* i 8)))))
        (cons val (+ pos 9)))))))

(defun mysql--read-lenenc-string-from-string (str pos)
  "Read a length-encoded string from STR at POS.
Returns (string . new-pos)."
  (let* ((r (mysql--read-lenenc-int-from-string str pos))
         (len (car r))
         (p (cdr r)))
    (cons (substring str p (+ p len)) (+ p len))))

(defun mysql--parse-column-definition (packet)
  "Parse a Column Definition packet.
Returns a plist with column metadata."
  ;; Read the 6 lenenc-string fields: catalog, schema, table, org_table, name, org_name
  (let ((pos 0)
        (strings nil))
    (dotimes (_ 6)
      (let ((r (mysql--read-lenenc-string-from-string packet pos)))
        (push (decode-coding-string (car r) 'utf-8) strings)
        (setq pos (cdr r))))
    (setq strings (nreverse strings))
    ;; Skip fixed-length fields marker (0x0c)
    (cl-incf pos 1)
    (let ((character-set (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8))))
      (cl-incf pos 2)
      (let ((column-length (logior (aref packet pos)
                                   (ash (aref packet (+ pos 1)) 8)
                                   (ash (aref packet (+ pos 2)) 16)
                                   (ash (aref packet (+ pos 3)) 24))))
        (cl-incf pos 4)
        (let ((column-type (aref packet pos)))
          (cl-incf pos 1)
          (let ((flags (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8))))
            (cl-incf pos 2)
            (list :catalog (nth 0 strings) :schema (nth 1 strings)
                  :table (nth 2 strings) :org-table (nth 3 strings)
                  :name (nth 4 strings) :org-name (nth 5 strings)
                  :character-set character-set
                  :column-length column-length :type column-type :flags flags
                  :decimals (aref packet pos))))))))

;;;; Row parsing

(defun mysql--parse-result-row (packet column-count)
  "Parse a result row from PACKET with COLUMN-COUNT columns.
Each column value is either NULL (0xFB prefix) or a lenenc-string."
  (let ((pos 0)
        (row nil))
    (dotimes (_ column-count)
      (if (= (aref packet pos) #xfb)
          (progn
            (push nil row)
            (cl-incf pos 1))
        (let ((r (mysql--read-lenenc-string-from-string packet pos)))
          (push (car r) row)
          (setq pos (cdr r)))))
    (nreverse row)))

;;;; Type conversion

(defvar mysql-type-parsers nil
  "Alist of (TYPE-CODE . PARSER-FN) for custom type parsing.
Each PARSER-FN takes a single string argument and returns the
converted Elisp value.  Entries here override built-in parsers.")

(defun mysql--parse-date (value)
  "Parse a MySQL DATE string \"YYYY-MM-DD\" into a plist.
Returns (:year Y :month M :day D), or nil for zero dates."
  (if (or (string= value "0000-00-00") (string-empty-p value))
      nil
    (let ((parts (split-string value "-")))
      (list :year (string-to-number (nth 0 parts))
            :month (string-to-number (nth 1 parts))
            :day (string-to-number (nth 2 parts))))))

(defun mysql--parse-time (value)
  "Parse a MySQL TIME string \"[-]HH:MM:SS[.ffffff]\" into a plist.
Returns (:hours H :minutes M :seconds S :negative BOOL)."
  (if (string-empty-p value)
      nil
    (let* ((negative (string-prefix-p "-" value))
           (s (if negative (substring value 1) value))
           (dot-pos (string-search "." s))
           (time-part (if dot-pos (substring s 0 dot-pos) s))
           (parts (split-string time-part ":")))
      (list :hours (string-to-number (nth 0 parts))
            :minutes (string-to-number (nth 1 parts))
            :seconds (string-to-number (nth 2 parts))
            :negative negative))))

(defun mysql--parse-datetime (value)
  "Parse a MySQL DATETIME/TIMESTAMP string into a plist.
Input: \"YYYY-MM-DD HH:MM:SS[.ffffff]\".
Returns (:year Y :month M :day D :hours H :minutes M :seconds S),
or nil for zero datetimes."
  (if (or (string-prefix-p "0000-00-00" value) (string-empty-p value))
      nil
    (let* ((space-pos (string-search " " value))
           (date-part (substring value 0 space-pos))
           (time-part (if space-pos (substring value (1+ space-pos)) "00:00:00"))
           (dot-pos (string-search "." time-part))
           (time-base (if dot-pos (substring time-part 0 dot-pos) time-part))
           (date-parts (split-string date-part "-"))
           (time-parts (split-string time-base ":")))
      (list :year (string-to-number (nth 0 date-parts))
            :month (string-to-number (nth 1 date-parts))
            :day (string-to-number (nth 2 date-parts))
            :hours (string-to-number (nth 0 time-parts))
            :minutes (string-to-number (nth 1 time-parts))
            :seconds (string-to-number (nth 2 time-parts))))))

(defun mysql--parse-bit (value)
  "Parse a MySQL BIT binary string into an integer."
  (let ((result 0))
    (dotimes (i (length value))
      (setq result (logior (ash result 8) (aref value i))))
    result))

(defun mysql--parse-value (value type)
  "Parse string VALUE according to MySQL column TYPE code.
Returns the converted Elisp value."
  (if (null value)
      nil
    (if-let* ((custom (alist-get type mysql-type-parsers)))
        (funcall custom value)
      (pcase type
        ;; Integer types
        ((or 1 2 3 8 9)
         (string-to-number value))
        ;; Float/Double
        ((or 4 5)
         (string-to-number value))
        ;; Decimal / NewDecimal
        ((or 0 246)
         (string-to-number value))
        ;; Year
        (13 (string-to-number value))
        ;; Timestamp / Datetime
        ((or 7 12) (mysql--parse-datetime value))
        ;; Date
        (10 (mysql--parse-date value))
        ;; Time
        (11 (mysql--parse-time value))
        ;; Bit
        (16 (mysql--parse-bit value))
        ;; JSON
        (245
         (let ((s (decode-coding-string value 'utf-8)))
           (if (fboundp 'json-parse-string)
               (json-parse-string s)
             s)))
        ;; Everything else: decode as UTF-8 string
        (_ (decode-coding-string value 'utf-8))))))

(defun mysql--convert-row (row columns)
  "Convert ROW values according to COLUMNS type information."
  (cl-mapcar (lambda (val col)
               (mysql--parse-value val (plist-get col :type)))
             row columns))

;;;; TLS support

(defun mysql--tls-available-p ()
  "Return non-nil if GnuTLS support is available in this Emacs."
  (and (fboundp 'gnutls-available-p) (gnutls-available-p)))

(defun mysql--build-ssl-request (conn)
  "Build a 32-byte SSL_REQUEST packet for CONN."
  (let* ((client-flags (logior mysql--cap-long-password
                               mysql--cap-found-rows
                               mysql--cap-long-flag
                               mysql--cap-protocol-41
                               mysql--cap-ssl
                               mysql--cap-transactions
                               mysql--cap-secure-connection
                               mysql--cap-plugin-auth
                               (if (mysql-conn-database conn)
                                   mysql--cap-connect-with-db
                                 0))))
    (concat (mysql--int-le-bytes client-flags 4)
            (mysql--int-le-bytes #x00ffffff 4)
            (unibyte-string 45)
            (make-string 23 0))))

(defun mysql--upgrade-to-tls (conn)
  "Upgrade CONN's network connection to TLS using GnuTLS."
  (let ((proc (mysql-conn-process conn)))
    (gnutls-negotiate
     :process proc
     :hostname (mysql-conn-host conn)
     :trustfiles mysql-tls-trustfiles
     :keylist mysql-tls-keylist
     :verify-hostname-error mysql-tls-verify-server
     :verify-error mysql-tls-verify-server)
    (setf (mysql-conn-tls conn) t)))

;;;; Connection

(defun mysql--authenticate (conn password tls)
  "Perform the MySQL handshake and authentication sequence on CONN.
PASSWORD is the plaintext password; TLS non-nil means upgrade to TLS first."
  (let* ((handshake-packet (mysql--read-packet conn))
         (handshake-info (mysql--parse-handshake conn handshake-packet))
         (salt (plist-get handshake-info :salt))
         (auth-plugin (plist-get handshake-info :auth-plugin)))
    (when tls
      (when (zerop (logand (mysql-conn-capability-flags conn)
                          mysql--cap-ssl))
        (signal 'mysql-connection-error
                (list "Server does not support SSL")))
      (setf (mysql-conn-sequence-id conn) 1)
      (mysql--send-packet conn (mysql--build-ssl-request conn))
      (mysql--upgrade-to-tls conn))
    (setf (mysql-conn-sequence-id conn) (if tls 2 1))
    (mysql--send-packet conn
                        (mysql--build-handshake-response conn password salt auth-plugin))
    (mysql--handle-auth-response conn password salt auth-plugin)))

(cl-defun mysql-connect (&key (host "127.0.0.1") (port 3306) user password database tls)
  "Connect to a MySQL server and authenticate.
Returns a `mysql-conn' struct on success.

HOST defaults to \"127.0.0.1\", PORT defaults to 3306.
USER, PASSWORD, and DATABASE are strings (DATABASE is optional).
When TLS is non-nil, upgrade the connection to TLS before authenticating."
  (unless user
    (signal 'mysql-connection-error (list "No user specified")))
  (when (and tls (not (mysql--tls-available-p)))
    (signal 'mysql-connection-error (list "TLS requested but GnuTLS is not available")))
  (let* ((buf (generate-new-buffer " *mysql-input*")))
    (with-current-buffer buf
      (set-buffer-multibyte nil))
    (let* ((proc (open-network-stream "mysql" buf host port
                                      :type 'plain
                                      :coding 'binary))
           (conn (make-mysql-conn :process proc
                                  :buf buf
                                  :host host
                                  :port port
                                  :user user
                                  :database database)))
      (set-process-coding-system proc 'binary 'binary)
      (set-process-filter proc
                          (lambda (_proc data)
                            (with-current-buffer buf
                              (goto-char (point-max))
                              (insert data))))
      (condition-case err
          (progn
            (mysql--authenticate conn password tls)
            conn)
        (error
         (when (process-live-p proc)
           (delete-process proc))
         (when (buffer-live-p buf)
           (kill-buffer buf))
         (signal (car err) (cdr err)))))))

(defun mysql--handle-auth-switch (conn password packet)
  "Handle an AUTH_SWITCH_REQUEST in PACKET.
Resend credentials with the new plugin and continue authentication."
  (let* ((pos 1)
         (nul-pos (cl-position 0 packet :start pos))
         (new-plugin (substring packet pos nul-pos))
         (new-salt (substring packet (1+ nul-pos)
                              (if (= (aref packet (1- (length packet))) 0)
                                  (1- (length packet))
                                (length packet))))
         (new-auth (mysql--compute-auth-response password new-salt new-plugin)))
    (mysql--send-packet conn new-auth)
    (mysql--handle-auth-response conn password new-salt new-plugin)))

(defun mysql--handle-auth-more-data (conn password salt auth-plugin packet)
  "Handle caching_sha2_password AuthMoreData in PACKET.
Dispatches on fast-auth success vs full-auth requirement."
  (pcase (aref packet 1)
    (#x03
     ;; Fast auth success -- read the OK packet that follows
     (let ((ok-packet (mysql--read-packet conn)))
       (pcase (mysql--packet-type ok-packet)
         ('ok
          (let ((ok-info (mysql--parse-ok-packet ok-packet)))
            (setf (mysql-conn-status-flags conn) (plist-get ok-info :status-flags))))
         ('err
          (let ((err-info (mysql--parse-err-packet ok-packet)))
            (signal 'mysql-auth-error
                    (list (format "Auth failed after fast-auth: [%d] %s"
                                  (plist-get err-info :code)
                                  (plist-get err-info :message)))))))))
    (#x04
     ;; Full authentication required
     (if (mysql-conn-tls conn)
         (let ((cleartext (concat (encode-coding-string (or password "") 'utf-8)
                                  (unibyte-string 0))))
           (mysql--send-packet conn cleartext)
           (mysql--handle-auth-response conn password salt auth-plugin))
       (signal 'mysql-auth-error
               (list "caching_sha2_password full authentication requires TLS"))))))

(defun mysql--handle-auth-response (conn password salt auth-plugin)
  "Handle the authentication response from the server.
CONN is the connection, PASSWORD is the plaintext password,
SALT is the nonce, AUTH-PLUGIN is the current auth plugin name."
  (let ((packet (condition-case _err
                    (mysql--read-packet conn)
                  (mysql-connection-error
                   (signal 'mysql-auth-error
                           (list "Connection closed during authentication"))))))
    ;; Dispatch on first byte directly (not mysql--packet-type, because
    ;; AUTH_SWITCH_REQUEST 0xFE can exceed 9 bytes).
    (pcase (aref packet 0)
      (#x00
       (let ((ok-info (mysql--parse-ok-packet packet)))
         (setf (mysql-conn-status-flags conn) (plist-get ok-info :status-flags))))
      (#xff
       (let ((err-info (mysql--parse-err-packet packet)))
         (signal 'mysql-auth-error
                 (list (format "Authentication failed: [%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (#xfe
       (mysql--handle-auth-switch conn password packet))
      (#x01
       (when (> (length packet) 1)
         (mysql--handle-auth-more-data conn password salt auth-plugin packet))))))

;;;; Query execution

(defun mysql-query (conn sql)
  "Execute SQL query on CONN and return a `mysql-result'.
SQL is a string containing the query to execute.
Signals `mysql-error' if the connection is busy (re-entrant call)."
  (when (mysql-conn-busy conn)
    (signal 'mysql-error
            (list "Connection busy — cannot send query while another is in progress")))
  ;; Flush any stale data left from previously interrupted queries.
  (with-current-buffer (mysql-conn-buf conn)
    (erase-buffer))
  (setf (mysql-conn-busy conn) t)
  (unwind-protect
      ;; Bind throw-on-input to nil so that `while-no-input' (used by
      ;; completion frameworks like corfu/company) cannot abort us
      ;; mid-response, which would leave partial data in the buffer and
      ;; corrupt subsequent queries.
      (let ((throw-on-input nil))
        (setf (mysql-conn-sequence-id conn) 0)
        (mysql--send-packet conn (concat (unibyte-string #x03)
                                         (encode-coding-string sql 'utf-8)))
        (let ((packet (mysql--read-packet conn)))
          (pcase (mysql--packet-type packet)
            ('ok
             (let ((ok-info (mysql--parse-ok-packet packet)))
               (setf (mysql-conn-status-flags conn)
                     (plist-get ok-info :status-flags))
               (make-mysql-result
                :connection conn
                :status "OK"
                :affected-rows (plist-get ok-info :affected-rows)
                :last-insert-id (plist-get ok-info :last-insert-id)
                :warnings (plist-get ok-info :warnings))))
            ('err
             (let ((err-info (mysql--parse-err-packet packet)))
               (signal 'mysql-query-error
                       (list (format "[%d] %s%s"
                                     (plist-get err-info :code)
                                     (if (plist-get err-info :state)
                                         (format "(%s) " (plist-get err-info :state))
                                       "")
                                     (plist-get err-info :message))))))
            (_
             ;; Result set: first byte is column_count (lenenc int)
             (mysql--read-result-set conn packet)))))
    (setf (mysql-conn-busy conn) nil)))

(defun mysql--read-column-definitions (conn col-count)
  "Read COL-COUNT column definition packets from CONN.
Returns a list of column plists.  Also consumes the EOF packet."
  (let ((columns nil))
    (dotimes (_ col-count)
      (push (mysql--parse-column-definition (mysql--read-packet conn)) columns))
    (setq columns (nreverse columns))
    ;; Read EOF after columns (unless CLIENT_DEPRECATE_EOF)
    (when (zerop (logand (mysql-conn-capability-flags conn)
                        mysql--cap-deprecate-eof))
      (let ((eof-packet (mysql--read-packet conn)))
        (unless (eq (mysql--packet-type eof-packet) 'eof)
          (signal 'mysql-protocol-error
                  (list "Expected EOF packet after column definitions")))))
    columns))

(defun mysql--read-text-rows (conn col-count columns)
  "Read text protocol rows from CONN until EOF.
COL-COUNT and COLUMNS guide parsing.  Returns rows in order."
  (let ((rows nil))
    (cl-loop
     (let ((row-packet (mysql--read-packet conn)))
       (pcase (mysql--packet-type row-packet)
         ((or 'eof 'ok) (cl-return nil))
         ('err
          (let ((err-info (mysql--parse-err-packet row-packet)))
            (signal 'mysql-query-error
                    (list (format "[%d] %s"
                                  (plist-get err-info :code)
                                  (plist-get err-info :message))))))
         (_ (push (mysql--convert-row
                   (mysql--parse-result-row row-packet col-count) columns)
                  rows)))))
    (nreverse rows)))

(defun mysql--read-result-set (conn first-packet)
  "Read a full result set from CONN.
FIRST-PACKET contains the column-count.  Returns a `mysql-result'."
  (let* ((col-count (let ((b (aref first-packet 0)))
                      (if (< b #xfb) b
                        (car (mysql--read-lenenc-int-from-string first-packet 0)))))
         (columns (mysql--read-column-definitions conn col-count))
         (rows (mysql--read-text-rows conn col-count columns)))
    (make-mysql-result
     :connection conn
     :status "OK"
     :columns columns
     :rows rows)))

;;;; Disconnect

(defun mysql-disconnect (conn)
  "Disconnect from MySQL server, sending COM_QUIT.
CONN is a `mysql-conn' returned by `mysql-connect'."
  (when conn
    (condition-case nil
        (when (process-live-p (mysql-conn-process conn))
          ;; Send COM_QUIT
          (setf (mysql-conn-sequence-id conn) 0)
          (mysql--send-packet conn (unibyte-string #x01)))
      (error nil))
    (when (process-live-p (mysql-conn-process conn))
      (delete-process (mysql-conn-process conn)))
    (when (buffer-live-p (mysql-conn-buf conn))
      (kill-buffer (mysql-conn-buf conn)))))

;;;; Prepared statements

(cl-defstruct mysql-stmt
  "A MySQL prepared statement."
  conn id param-count column-count param-definitions column-definitions)

(defun mysql--parse-prepare-ok (conn packet)
  "Parse a COM_STMT_PREPARE_OK response from PACKET.
Reads param and column definition packets from CONN.
Returns a `mysql-stmt'."
  (unless (= (aref packet 0) #x00)
    (signal 'mysql-stmt-error (list "Expected OK status in PREPARE response")))
  (let* ((stmt-id (logior (aref packet 1)
                          (ash (aref packet 2) 8)
                          (ash (aref packet 3) 16)
                          (ash (aref packet 4) 24)))
         (num-columns (logior (aref packet 5) (ash (aref packet 6) 8)))
         (num-params (logior (aref packet 7) (ash (aref packet 8) 8)))
         ;; byte 9 is filler (0x00)
         ;; bytes 10-11 are warning_count
         (param-defs nil)
         (col-defs nil))
    ;; Read param definitions
    (when (> num-params 0)
      (dotimes (_ num-params)
        (push (mysql--parse-column-definition (mysql--read-packet conn)) param-defs))
      (setq param-defs (nreverse param-defs))
      ;; Read EOF after params
      (mysql--read-packet conn))
    ;; Read column definitions
    (when (> num-columns 0)
      (dotimes (_ num-columns)
        (push (mysql--parse-column-definition (mysql--read-packet conn)) col-defs))
      (setq col-defs (nreverse col-defs))
      ;; Read EOF after columns
      (mysql--read-packet conn))
    (make-mysql-stmt :conn conn
                     :id stmt-id
                     :param-count num-params
                     :column-count num-columns
                     :param-definitions param-defs
                     :column-definitions col-defs)))

(defun mysql--elisp-to-mysql-type (value)
  "Map Elisp VALUE to a 2-byte MySQL type code (little-endian).
Returns a cons (TYPE-CODE . UNSIGNED-FLAG)."
  (cond
   ((null value) (cons mysql-type-null 0))
   ((integerp value) (cons mysql-type-longlong 0))
   ((floatp value) (cons mysql-type-var-string 0))
   ((stringp value) (cons mysql-type-var-string 0))
   (t (cons mysql-type-var-string 0))))

(defun mysql--encode-binary-value (value)
  "Encode VALUE for a binary protocol parameter.
Integers are encoded as 8-byte LE; others as lenenc strings."
  (cond
   ((null value) "")
   ((integerp value)
    (let ((bytes (make-string 8 0))
          (v (if (< value 0) (+ (ash 1 64) value) value)))
      (dotimes (i 8)
        (aset bytes i (logand (ash v (* i -8)) #xff)))
      bytes))
   ((floatp value)
    (let ((s (number-to-string value)))
      (concat (mysql--lenenc-int-bytes (length s)) s)))
   ((stringp value)
    (let ((encoded (encode-coding-string value 'utf-8)))
      (concat (mysql--lenenc-int-bytes (length encoded)) encoded)))
   (t
    (let ((s (format "%s" value)))
      (concat (mysql--lenenc-int-bytes (length s)) s)))))

(defun mysql--build-execute-packet (stmt params)
  "Build a COM_STMT_EXECUTE packet for STMT with PARAMS."
  (let* ((stmt-id (mysql-stmt-id stmt))
         (param-count (mysql-stmt-param-count stmt))
         (parts nil))
    ;; Command byte
    (push (unibyte-string #x17) parts)
    ;; stmt_id: 4 bytes LE
    (push (mysql--int-le-bytes stmt-id 4) parts)
    ;; flags: 1 byte (0x00 = CURSOR_TYPE_NO_CURSOR)
    (push (unibyte-string #x00) parts)
    ;; iteration_count: 4 bytes LE (always 1)
    (push (mysql--int-le-bytes 1 4) parts)
    (when (> param-count 0)
      ;; NULL bitmap: (param-count + 7) / 8 bytes
      (let* ((bitmap-len (/ (+ param-count 7) 8))
             (bitmap (make-string bitmap-len 0)))
        (dotimes (i param-count)
          (when (null (nth i params))
            (let ((byte-idx (/ i 8))
                  (bit-idx (% i 8)))
              (aset bitmap byte-idx
                    (logior (aref bitmap byte-idx) (ash 1 bit-idx))))))
        (push bitmap parts))
      ;; new_params_bound_flag: 1 byte (always 1 = send types)
      (push (unibyte-string #x01) parts)
      ;; Type array: 2 bytes per param (type code + unsigned flag)
      (dotimes (i param-count)
        (let ((type-info (mysql--elisp-to-mysql-type (nth i params))))
          (push (unibyte-string (car type-info) (cdr type-info)) parts)))
      ;; Values (non-NULL only)
      (dotimes (i param-count)
        (unless (null (nth i params))
          (push (mysql--encode-binary-value (nth i params)) parts))))
    (apply #'concat (nreverse parts))))

(defun mysql-prepare (conn sql)
  "Prepare SQL statement on CONN.  Returns a `mysql-stmt'."
  (setf (mysql-conn-sequence-id conn) 0)
  (mysql--send-packet conn (concat (unibyte-string #x16)
                                   (encode-coding-string sql 'utf-8)))
  (let ((packet (mysql--read-packet conn)))
    (pcase (mysql--packet-type packet)
      ('err
       (let ((err-info (mysql--parse-err-packet packet)))
         (signal 'mysql-stmt-error
                 (list (format "[%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (_ (mysql--parse-prepare-ok conn packet)))))

(defun mysql-execute (stmt &rest params)
  "Execute prepared STMT with PARAMS.  Returns a `mysql-result'."
  (let ((conn (mysql-stmt-conn stmt)))
    (unless (= (length params) (mysql-stmt-param-count stmt))
      (signal 'mysql-stmt-error
              (list (format "Expected %d params, got %d"
                            (mysql-stmt-param-count stmt) (length params)))))
    (setf (mysql-conn-sequence-id conn) 0)
    (mysql--send-packet conn (mysql--build-execute-packet stmt params))
    (let ((packet (mysql--read-packet conn)))
      (pcase (mysql--packet-type packet)
        ('ok
         (let ((ok-info (mysql--parse-ok-packet packet)))
           (setf (mysql-conn-status-flags conn) (plist-get ok-info :status-flags))
           (make-mysql-result
            :connection conn
            :status "OK"
            :affected-rows (plist-get ok-info :affected-rows)
            :last-insert-id (plist-get ok-info :last-insert-id)
            :warnings (plist-get ok-info :warnings))))
        ('err
         (let ((err-info (mysql--parse-err-packet packet)))
           (signal 'mysql-stmt-error
                   (list (format "[%d] %s"
                                 (plist-get err-info :code)
                                 (plist-get err-info :message))))))
        (_
         ;; Binary result set
         (mysql--read-binary-result-set conn packet))))))

(defun mysql-stmt-close (stmt)
  "Close prepared STMT.  No server response is expected."
  (let ((conn (mysql-stmt-conn stmt)))
    (setf (mysql-conn-sequence-id conn) 0)
    (mysql--send-packet conn (concat (unibyte-string #x19)
                                     (mysql--int-le-bytes (mysql-stmt-id stmt) 4)))))

;; Binary result set reading

(defun mysql--read-binary-result-set (conn first-packet)
  "Read a binary protocol result set from CONN.
FIRST-PACKET contains the column count.  Returns a `mysql-result'."
  (let* ((col-count (aref first-packet 0))
         (columns nil)
         (rows nil))
    ;; Read column definitions
    (dotimes (_ col-count)
      (push (mysql--parse-column-definition (mysql--read-packet conn)) columns))
    (setq columns (nreverse columns))
    ;; Read EOF after columns
    (mysql--read-packet conn)
    ;; Read binary rows until EOF.
    ;; Binary rows start with 0x00 (same as OK packets), so we must NOT
    ;; use mysql--packet-type here.  Since CLIENT_DEPRECATE_EOF is not
    ;; set, the result set always ends with an EOF packet (0xFE, <=9 bytes).
    (cl-loop
     (let ((row-packet (mysql--read-packet conn)))
       (cond
        ((and (= (aref row-packet 0) #xfe) (<= (length row-packet) 9))
         (cl-return nil))
        ((= (aref row-packet 0) #xff)
         (let ((err-info (mysql--parse-err-packet row-packet)))
           (signal 'mysql-stmt-error
                   (list (format "[%d] %s"
                                 (plist-get err-info :code)
                                 (plist-get err-info :message))))))
        (t
         (push (mysql--parse-binary-row row-packet columns) rows)))))
    (make-mysql-result
     :connection conn
     :status "OK"
     :columns columns
     :rows (nreverse rows))))

(defun mysql--binary-null-p (null-bitmap col-index)
  "Check if column COL-INDEX is NULL in NULL-BITMAP.
Binary row NULL bitmap has a 2-bit offset."
  (let* ((offset (+ col-index 2))
         (byte-idx (/ offset 8))
         (bit-idx (% offset 8)))
    (not (zerop (logand (aref null-bitmap byte-idx) (ash 1 bit-idx))))))

(defun mysql--parse-binary-row (packet columns)
  "Parse a binary protocol row from PACKET using COLUMNS metadata."
  ;; First byte is 0x00 (packet header for binary rows)
  (let* ((col-count (length columns))
         (bitmap-len (/ (+ col-count 2 7) 8))
         (null-bitmap (substring packet 1 (+ 1 bitmap-len)))
         (pos (+ 1 bitmap-len))
         (row nil))
    (dotimes (i col-count)
      (if (mysql--binary-null-p null-bitmap i)
          (push nil row)
        (let ((result (mysql--decode-binary-value packet pos
                                                   (plist-get (nth i columns) :type))))
          (push (car result) row)
          (setq pos (cdr result)))))
    (nreverse row)))

(defun mysql--decode-binary-value (packet pos type)
  "Decode a binary value from PACKET at POS for the given TYPE.
Returns (value . new-pos)."
  (pcase type
    ;; Integer types
    ((pred (= mysql-type-tiny))
     (cons (aref packet pos) (1+ pos)))
    ((or (pred (= mysql-type-short)) (pred (= mysql-type-year)))
     (cons (mysql--read-le-uint packet pos 2) (+ pos 2)))
    ((or (pred (= mysql-type-long)) (pred (= mysql-type-int24)))
     (cons (mysql--read-le-uint packet pos 4) (+ pos 4)))
    ((pred (= mysql-type-longlong))
     (cons (mysql--read-le-uint packet pos 8) (+ pos 8)))
    ;; Floating point
    ((pred (= mysql-type-float))
     (cons (mysql--ieee754-single-to-float packet pos) (+ pos 4)))
    ((pred (= mysql-type-double))
     (cons (mysql--ieee754-double-to-float packet pos) (+ pos 8)))
    ;; Temporal types
    ((or (pred (= mysql-type-date))
         (pred (= mysql-type-datetime))
         (pred (= mysql-type-timestamp)))
     (mysql--decode-binary-datetime packet pos type))
    ((pred (= mysql-type-time))
     (mysql--decode-binary-time packet pos))
    ((pred (= mysql-type-null))
     (cons nil pos))
    ;; All string-like types: lenenc string
    (_
     (pcase-let ((`(,len . ,start) (mysql--read-lenenc-int-from-string packet pos)))
       (cons (substring packet start (+ start len)) (+ start len))))))

(defun mysql--ieee754-single-to-float (data offset)
  "Decode a 4-byte IEEE 754 single-precision float from DATA at OFFSET."
  (let* ((b0 (aref data offset))
         (b1 (aref data (+ offset 1)))
         (b2 (aref data (+ offset 2)))
         (b3 (aref data (+ offset 3)))
         (bits (logior b0 (ash b1 8) (ash b2 16) (ash b3 24)))
         (sign (if (zerop (logand bits #x80000000)) 1.0 -1.0))
         (exponent (logand (ash bits -23) #xff))
         (mantissa (logand bits #x7fffff)))
    (cond
     ((= exponent 0)
      (if (= mantissa 0) (* sign 0.0)
        (* sign (ldexp (/ (float mantissa) #x800000) -126))))
     ((= exponent #xff)
      (if (= mantissa 0) (* sign 1.0e+INF) 0.0e+NaN))
     (t
      (* sign (ldexp (+ 1.0 (/ (float mantissa) #x800000)) (- exponent 127)))))))

(defun mysql--ieee754-double-to-float (data offset)
  "Decode an 8-byte IEEE 754 double-precision float from DATA at OFFSET."
  (let* ((b0 (aref data offset))
         (b1 (aref data (+ offset 1)))
         (b2 (aref data (+ offset 2)))
         (b3 (aref data (+ offset 3)))
         (b4 (aref data (+ offset 4)))
         (b5 (aref data (+ offset 5)))
         (b6 (aref data (+ offset 6)))
         (b7 (aref data (+ offset 7)))
         (sign (if (zerop (logand b7 #x80)) 1.0 -1.0))
         (exponent (logior (ash (logand b7 #x7f) 4)
                           (ash b6 -4)))
         ;; 52-bit mantissa: b6[3:0] b5 b4 b3 b2 b1 b0
         (mantissa (+ (* (float (logand b6 #x0f)) (expt 2.0 48))
                      (* (float b5) (expt 2.0 40))
                      (* (float b4) (expt 2.0 32))
                      (* (float b3) (expt 2.0 24))
                      (* (float b2) (expt 2.0 16))
                      (* (float b1) (expt 2.0 8))
                      (float b0))))
    (cond
     ((= exponent 0)
      (if (= mantissa 0.0) (* sign 0.0)
        (* sign (ldexp (/ mantissa (expt 2.0 52)) -1022))))
     ((= exponent #x7ff)
      (if (= mantissa 0.0) (* sign 1.0e+INF) 0.0e+NaN))
     (t
      (* sign (ldexp (+ 1.0 (/ mantissa (expt 2.0 52))) (- exponent 1023)))))))

(defun mysql--decode-binary-datetime (packet pos type)
  "Decode a binary DATE/DATETIME/TIMESTAMP from PACKET at POS.
Returns (value . new-pos)."
  (let ((len (aref packet pos)))
    (cl-incf pos)
    (pcase len
      (0
       (cons nil pos))
      (4
       (let ((year (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
             (month (aref packet (+ pos 2)))
             (day (aref packet (+ pos 3))))
         (cons (if (= type mysql-type-date)
                   (list :year year :month month :day day)
                 (list :year year :month month :day day
                       :hours 0 :minutes 0 :seconds 0))
               (+ pos 4))))
      (7
       (let ((year (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
             (month (aref packet (+ pos 2)))
             (day (aref packet (+ pos 3)))
             (hours (aref packet (+ pos 4)))
             (minutes (aref packet (+ pos 5)))
             (seconds (aref packet (+ pos 6))))
         (cons (list :year year :month month :day day
                     :hours hours :minutes minutes :seconds seconds)
               (+ pos 7))))
      (11
       (let ((year (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
             (month (aref packet (+ pos 2)))
             (day (aref packet (+ pos 3)))
             (hours (aref packet (+ pos 4)))
             (minutes (aref packet (+ pos 5)))
             (seconds (aref packet (+ pos 6))))
         ;; microseconds: 4 bytes (ignored for now, but skip them)
         (cons (list :year year :month month :day day
                     :hours hours :minutes minutes :seconds seconds)
               (+ pos 11))))
      (_
       (cons nil (+ pos len))))))

(defun mysql--decode-binary-time (packet pos)
  "Decode a binary TIME value from PACKET at POS.
Returns (value . new-pos)."
  (let ((len (aref packet pos)))
    (cl-incf pos)
    (pcase len
      (0
       (cons (list :hours 0 :minutes 0 :seconds 0 :negative nil) pos))
      (8
       (let ((negative (not (zerop (aref packet pos))))
             ;; days: 4 bytes LE (convert to hours)
             (days (logior (aref packet (+ pos 1))
                           (ash (aref packet (+ pos 2)) 8)
                           (ash (aref packet (+ pos 3)) 16)
                           (ash (aref packet (+ pos 4)) 24)))
             (hours (aref packet (+ pos 5)))
             (minutes (aref packet (+ pos 6)))
             (seconds (aref packet (+ pos 7))))
         (cons (list :hours (+ (* days 24) hours)
                     :minutes minutes :seconds seconds
                     :negative negative)
               (+ pos 8))))
      (12
       (let ((negative (not (zerop (aref packet pos))))
             (days (logior (aref packet (+ pos 1))
                           (ash (aref packet (+ pos 2)) 8)
                           (ash (aref packet (+ pos 3)) 16)
                           (ash (aref packet (+ pos 4)) 24)))
             (hours (aref packet (+ pos 5)))
             (minutes (aref packet (+ pos 6)))
             (seconds (aref packet (+ pos 7))))
         ;; microseconds: 4 bytes (skip)
         (cons (list :hours (+ (* days 24) hours)
                     :minutes minutes :seconds seconds
                     :negative negative)
               (+ pos 12))))
      (_
       (cons nil (+ pos len))))))

;;;; Convenience APIs

(defmacro with-mysql-connection (var connect-args &rest body)
  "Execute BODY with VAR bound to a MySQL connection.
CONNECT-ARGS is a plist passed to `mysql-connect'.
The connection is automatically closed when BODY exits."
  (declare (indent 2))
  `(let ((,var (mysql-connect ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (mysql-disconnect ,var))))

(defmacro with-mysql-transaction (conn &rest body)
  "Execute BODY inside a SQL transaction on CONN.
Issues BEGIN before BODY.  If BODY completes normally, issues COMMIT.
If BODY signals an error, issues ROLLBACK before re-raising."
  (declare (indent 1))
  (let ((c (make-symbol "conn")))
    `(let ((,c ,conn))
       (mysql-query ,c "BEGIN")
       (condition-case err
           (prog1 (progn ,@body)
             (mysql-query ,c "COMMIT"))
         (error
          (mysql-query ,c "ROLLBACK")
          (signal (car err) (cdr err)))))))

(defun mysql-ping (conn)
  "Send COM_PING to the MySQL server via CONN.
Returns t if the server is alive, or signals an error."
  (setf (mysql-conn-sequence-id conn) 0)
  (mysql--send-packet conn (unibyte-string #x0e))
  (let ((packet (mysql--read-packet conn)))
    (pcase (mysql--packet-type packet)
      ('ok t)
      ('err
       (let ((err-info (mysql--parse-err-packet packet)))
         (signal 'mysql-error
                 (list (format "Ping failed: [%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (_ (signal 'mysql-protocol-error (list "Unexpected response to COM_PING"))))))

(defun mysql-escape-identifier (name)
  "Escape NAME for use as a MySQL identifier.
Wraps in backticks and doubles any embedded backticks."
  (concat "`" (replace-regexp-in-string "`" "``" name) "`"))

(defun mysql-escape-literal (value)
  "Escape VALUE for use as a MySQL string literal.
Wraps in single quotes and escapes special characters."
  (concat "'"
          (replace-regexp-in-string
           "[\0\n\r\\\\'\"\\x1a]"
           (lambda (ch)
             (pcase ch
               ("\0"   "\\0")
               ("\n"   "\\n")
               ("\r"   "\\r")
               ("\\"   "\\\\")
               ("'"    "\\'")
               ("\""   "\\\"")
               ("\x1a" "\\Z")
               (_ ch)))
           value nil t)
          "'"))

(defun mysql-connect/uri (uri)
  "Connect to MySQL using a URI string.
URI format: mysql://user:password@host:port/database"
  (unless (string-match
           "\\`mysql://\\([^:@]*\\)\\(?::\\([^@]*\\)\\)?@\\([^:/]*\\)\\(?::\\([0-9]+\\)\\)?\\(?:/\\(.*\\)\\)?\\'"
           uri)
    (signal 'mysql-connection-error (list (format "Invalid MySQL URI: %s" uri))))
  (let ((user (match-string 1 uri))
        (password (match-string 2 uri))
        (host (match-string 3 uri))
        (port (if-let* ((p (match-string 4 uri))) (string-to-number p) 3306))
        (database (match-string 5 uri)))
    (mysql-connect :host host :port port :user user
                   :password password
                   :database (if (or (null database) (string-empty-p database))
                                 nil database))))

(provide 'mysql)
;;; mysql.el ends here
