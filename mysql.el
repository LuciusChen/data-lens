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
(require 'mysql-error)

;;;; Capability flags (MySQL protocol)

(defconst mysql--cap-long-password        #x00000001)
(defconst mysql--cap-found-rows           #x00000002)
(defconst mysql--cap-long-flag            #x00000004)
(defconst mysql--cap-connect-with-db      #x00000008)
(defconst mysql--cap-protocol-41          #x00000200)
(defconst mysql--cap-transactions         #x00002000)
(defconst mysql--cap-secure-connection    #x00008000)
(defconst mysql--cap-plugin-auth          #x00080000)
(defconst mysql--cap-plugin-auth-lenenc   #x00200000)
(defconst mysql--cap-deprecate-eof        #x01000000)

;;;; Column type constants

(defconst mysql--type-decimal     0)
(defconst mysql--type-tiny        1)
(defconst mysql--type-short       2)
(defconst mysql--type-long        3)
(defconst mysql--type-float       4)
(defconst mysql--type-double      5)
(defconst mysql--type-null        6)
(defconst mysql--type-timestamp   7)
(defconst mysql--type-longlong    8)
(defconst mysql--type-int24       9)
(defconst mysql--type-date       10)
(defconst mysql--type-time       11)
(defconst mysql--type-datetime   12)
(defconst mysql--type-year       13)
(defconst mysql--type-varchar    15)
(defconst mysql--type-bit        16)
(defconst mysql--type-json      245)
(defconst mysql--type-newdecimal 246)
(defconst mysql--type-enum      247)
(defconst mysql--type-set       248)
(defconst mysql--type-tiny-blob  249)
(defconst mysql--type-medium-blob 250)
(defconst mysql--type-long-blob  251)
(defconst mysql--type-blob       252)
(defconst mysql--type-var-string 253)
(defconst mysql--type-string     254)
(defconst mysql--type-geometry   255)

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
  (sequence-id 0))

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
`mysql-conn-read-timeout' seconds elapse."
  (let ((proc (mysql-conn-process conn))
        (buf (mysql-conn-buf conn))
        (deadline (+ (float-time) (mysql-conn-read-timeout conn))))
    (with-current-buffer buf
      (while (< (- (point-max) (point-min)) n)
        (let ((remaining (- deadline (float-time))))
          (when (<= remaining 0)
            (signal 'mysql-timeout
                    (list (format "Timed out waiting for %d bytes" n))))
          (unless (accept-process-output proc remaining nil t)
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

(defun mysql--parse-handshake (conn packet)
  "Parse a HandshakeV10 PACKET and update CONN with server info.
Returns a plist with :salt and :auth-plugin."
  (let* ((pos 0)
         (protocol-version (aref packet pos)))
    (unless (= protocol-version 10)
      (signal 'mysql-protocol-error
              (list (format "Unsupported protocol version: %d" protocol-version))))
    (cl-incf pos)
    ;; server_version: NUL-terminated string
    (let ((nul-pos (cl-position 0 packet :start pos)))
      (setf (mysql-conn-server-version conn)
            (substring packet pos nul-pos))
      (setq pos (1+ nul-pos)))
    ;; connection_id: 4 bytes LE
    (setf (mysql-conn-connection-id conn)
          (logior (aref packet pos)
                  (ash (aref packet (+ pos 1)) 8)
                  (ash (aref packet (+ pos 2)) 16)
                  (ash (aref packet (+ pos 3)) 24)))
    (cl-incf pos 4)
    ;; auth_plugin_data_part_1: 8 bytes
    (let ((salt-part1 (substring packet pos (+ pos 8))))
      (cl-incf pos 8)
      ;; filler: 1 byte (0x00)
      (cl-incf pos 1)
      ;; capability_flags lower 2 bytes
      (let ((cap-low (logior (aref packet pos)
                             (ash (aref packet (+ pos 1)) 8))))
        (cl-incf pos 2)
        ;; character_set: 1 byte
        (setf (mysql-conn-character-set conn) (aref packet pos))
        (cl-incf pos 1)
        ;; status_flags: 2 bytes
        (setf (mysql-conn-status-flags conn)
              (logior (aref packet pos)
                      (ash (aref packet (+ pos 1)) 8)))
        (cl-incf pos 2)
        ;; capability_flags upper 2 bytes
        (let ((cap-high (logior (aref packet pos)
                                (ash (aref packet (+ pos 1)) 8))))
          (cl-incf pos 2)
          (let ((server-caps (logior cap-low (ash cap-high 16))))
            (setf (mysql-conn-capability-flags conn) server-caps))
          ;; auth_plugin_data_len or 0
          (let ((auth-data-len (aref packet pos)))
            (cl-incf pos 1)
            ;; reserved: 10 bytes (all zeros)
            (cl-incf pos 10)
            ;; auth_plugin_data_part_2 (if cap SECURE_CONNECTION)
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
              ;; auth_plugin_name (NUL-terminated)
              (let ((auth-plugin nil))
                (when (not (zerop (logand (mysql-conn-capability-flags conn)
                                         mysql--cap-plugin-auth)))
                  (let ((nul-pos2 (cl-position 0 packet :start pos)))
                    (when nul-pos2
                      (setq auth-plugin (substring packet pos nul-pos2)))))
                (list :salt (concat salt-part1 salt-part2)
                      :auth-plugin (or auth-plugin "mysql_native_password"))))))))))

(defun mysql--build-handshake-response (conn password salt auth-plugin)
  "Build a HandshakeResponse41 packet for CONN.
PASSWORD is the user password, SALT is the server nonce, AUTH-PLUGIN
is the authentication plugin name."
  (let* ((client-flags (logior mysql--cap-long-password
                               mysql--cap-found-rows
                               mysql--cap-long-flag
                               mysql--cap-protocol-41
                               mysql--cap-transactions
                               mysql--cap-secure-connection
                               mysql--cap-plugin-auth
                               (if (mysql-conn-database conn)
                                   mysql--cap-connect-with-db
                                 0)))
         (auth-response (mysql--compute-auth-response password salt auth-plugin))
         (parts nil))
    (setf (mysql-conn-capability-flags conn)
          (logand client-flags (mysql-conn-capability-flags conn)))
    ;; client_flag: 4 bytes
    (push (mysql--int-le-bytes client-flags 4) parts)
    ;; max_packet_size: 4 bytes
    (push (mysql--int-le-bytes #x00ffffff 4) parts)
    ;; character_set: 1 byte (utf8mb4 = 45)
    (push (unibyte-string 45) parts)
    ;; filler: 23 bytes of 0x00
    (push (make-string 23 0) parts)
    ;; username: NUL-terminated
    (push (concat (encode-coding-string (mysql-conn-user conn) 'utf-8)
                  (unibyte-string 0))
          parts)
    ;; auth_response: length-prefixed
    (push (unibyte-string (length auth-response)) parts)
    (push auth-response parts)
    ;; database: NUL-terminated (if connect-with-db)
    (when (mysql-conn-database conn)
      (push (concat (encode-coding-string (mysql-conn-database conn) 'utf-8)
                    (unibyte-string 0))
            parts))
    ;; auth_plugin_name: NUL-terminated
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
  (let ((pos 1)
        affected-rows last-insert-id status-flags warnings)
    ;; affected_rows: lenenc int
    (pcase (aref packet pos)
      ((and b (guard (< b #xfb)))
       (setq affected-rows b)
       (cl-incf pos 1))
      (#xfc
       (cl-incf pos 1)
       (setq affected-rows (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
       (cl-incf pos 2))
      (#xfd
       (cl-incf pos 1)
       (setq affected-rows (logior (aref packet pos)
                                   (ash (aref packet (+ pos 1)) 8)
                                   (ash (aref packet (+ pos 2)) 16)))
       (cl-incf pos 3))
      (#xfe
       (cl-incf pos 1)
       (setq affected-rows 0)
       (dotimes (i 8)
         (setq affected-rows (logior affected-rows (ash (aref packet (+ pos i)) (* i 8)))))
       (cl-incf pos 8)))
    ;; last_insert_id: lenenc int
    (pcase (aref packet pos)
      ((and b (guard (< b #xfb)))
       (setq last-insert-id b)
       (cl-incf pos 1))
      (#xfc
       (cl-incf pos 1)
       (setq last-insert-id (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
       (cl-incf pos 2))
      (#xfd
       (cl-incf pos 1)
       (setq last-insert-id (logior (aref packet pos)
                                    (ash (aref packet (+ pos 1)) 8)
                                    (ash (aref packet (+ pos 2)) 16)))
       (cl-incf pos 3))
      (#xfe
       (cl-incf pos 1)
       (setq last-insert-id 0)
       (dotimes (i 8)
         (setq last-insert-id (logior last-insert-id (ash (aref packet (+ pos i)) (* i 8)))))
       (cl-incf pos 8)))
    ;; status_flags: 2 bytes LE
    (when (< (+ pos 1) (length packet))
      (setq status-flags (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
      (cl-incf pos 2))
    ;; warnings: 2 bytes LE
    (when (< (+ pos 1) (length packet))
      (setq warnings (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8))))
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
    (setq message (substring packet pos))
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
  (let ((pos 0) catalog schema table org-table name org-name
        character-set column-length column-type flags decimals)
    ;; catalog (always "def")
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq catalog (car r) pos (cdr r)))
    ;; schema
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq schema (car r) pos (cdr r)))
    ;; table (virtual)
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq table (car r) pos (cdr r)))
    ;; org_table (physical)
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq org-table (car r) pos (cdr r)))
    ;; name (virtual)
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq name (car r) pos (cdr r)))
    ;; org_name (physical)
    (let ((r (mysql--read-lenenc-string-from-string packet pos)))
      (setq org-name (car r) pos (cdr r)))
    ;; fixed-length fields marker (0x0c)
    (cl-incf pos 1)
    ;; character_set: 2 bytes LE
    (setq character-set (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
    (cl-incf pos 2)
    ;; column_length: 4 bytes LE
    (setq column-length (logior (aref packet pos)
                                (ash (aref packet (+ pos 1)) 8)
                                (ash (aref packet (+ pos 2)) 16)
                                (ash (aref packet (+ pos 3)) 24)))
    (cl-incf pos 4)
    ;; column_type: 1 byte
    (setq column-type (aref packet pos))
    (cl-incf pos 1)
    ;; flags: 2 bytes LE
    (setq flags (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
    (cl-incf pos 2)
    ;; decimals: 1 byte
    (setq decimals (aref packet pos))
    (list :catalog catalog :schema schema :table table :org-table org-table
          :name name :org-name org-name :character-set character-set
          :column-length column-length :type column-type :flags flags
          :decimals decimals)))

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

(defun mysql--parse-value (value type)
  "Parse string VALUE according to MySQL column TYPE code.
Returns the converted Elisp value."
  (if (null value)
      nil
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
      ;; JSON
      (245
       (if (fboundp 'json-parse-string)
           (json-parse-string value)
         value))
      ;; Everything else: return as string
      (_ value))))

(defun mysql--convert-row (row columns)
  "Convert ROW values according to COLUMNS type information."
  (cl-mapcar (lambda (val col)
               (mysql--parse-value val (plist-get col :type)))
             row columns))

;;;; Connection

(cl-defun mysql-connect (&key (host "127.0.0.1") (port 3306) user password database)
  "Connect to a MySQL server and authenticate.
Returns a `mysql-conn' struct on success.

HOST defaults to \"127.0.0.1\", PORT defaults to 3306.
USER, PASSWORD, and DATABASE are strings (DATABASE is optional)."
  (unless user
    (signal 'mysql-connection-error (list "USER is required")))
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
            ;; Read server handshake
            (let* ((handshake-packet (mysql--read-packet conn))
                   (handshake-info (mysql--parse-handshake conn handshake-packet))
                   (salt (plist-get handshake-info :salt))
                   (auth-plugin (plist-get handshake-info :auth-plugin)))
              ;; Send handshake response
              (setf (mysql-conn-sequence-id conn) 1)
              (let ((response (mysql--build-handshake-response conn password salt auth-plugin)))
                (mysql--send-packet conn response))
              ;; Read auth result
              (mysql--handle-auth-response conn password salt auth-plugin))
            conn)
        (error
         ;; Clean up on failure
         (when (process-live-p proc)
           (delete-process proc))
         (when (buffer-live-p buf)
           (kill-buffer buf))
         (signal (car err) (cdr err)))))))

(defun mysql--handle-auth-response (conn password salt auth-plugin)
  "Handle the authentication response from the server.
CONN is the connection, PASSWORD is the plaintext password,
SALT is the nonce, AUTH-PLUGIN is the current auth plugin name."
  (let ((packet (condition-case _err
                    (mysql--read-packet conn)
                  (mysql-connection-error
                   (signal 'mysql-auth-error
                           (list "Connection closed during authentication"))))))
    (pcase (mysql--packet-type packet)
      ('ok
       ;; Authentication successful
       (let ((ok-info (mysql--parse-ok-packet packet)))
         (setf (mysql-conn-status-flags conn) (plist-get ok-info :status-flags))))
      ('err
       (let ((err-info (mysql--parse-err-packet packet)))
         (signal 'mysql-auth-error
                 (list (format "Authentication failed: [%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      ('eof
       ;; AUTH_SWITCH_REQUEST (0xFE)
       ;; New auth plugin name + auth data follow
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
      (_
       ;; Could be caching_sha2_password fast-auth success (0x01 0x03)
       ;; or request for full authentication (0x01 0x04)
       (when (and (> (length packet) 1)
                  (= (aref packet 0) #x01))
         (pcase (aref packet 1)
           (#x03
            ;; Fast auth success — read the OK packet that follows
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
            ;; Full authentication required — need RSA encryption over secure channel
            ;; For now, only support this over TLS or Unix socket
            (signal 'mysql-auth-error
                    (list "caching_sha2_password full authentication requires TLS (not yet supported)")))))))))

;;;; Query execution

(defun mysql-query (conn sql)
  "Execute SQL query on CONN and return a `mysql-result'.
SQL is a string containing the query to execute."
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

(defun mysql--read-result-set (conn first-packet)
  "Read a full result set from CONN.
FIRST-PACKET contains the column-count.  Returns a `mysql-result'."
  (let* ((column-count (aref first-packet 0))
         ;; For lenenc, handle multi-byte
         (col-count
          (cond
           ((< column-count #xfb) column-count)
           (t (car (mysql--read-lenenc-int-from-string first-packet 0)))))
         (columns nil)
         (rows nil))
    ;; Read column definitions
    (dotimes (_ col-count)
      (let ((col-packet (mysql--read-packet conn)))
        (push (mysql--parse-column-definition col-packet) columns)))
    (setq columns (nreverse columns))
    ;; Read EOF after columns (unless CLIENT_DEPRECATE_EOF)
    (unless (not (zerop (logand (mysql-conn-capability-flags conn)
                                mysql--cap-deprecate-eof)))
      (let ((eof-packet (mysql--read-packet conn)))
        (unless (eq (mysql--packet-type eof-packet) 'eof)
          (signal 'mysql-protocol-error
                  (list "Expected EOF packet after column definitions")))))
    ;; Read rows
    (cl-loop
     (let ((row-packet (mysql--read-packet conn)))
       (pcase (mysql--packet-type row-packet)
         ((or 'eof 'ok)
          (cl-return nil))
         ('err
          (let ((err-info (mysql--parse-err-packet row-packet)))
            (signal 'mysql-query-error
                    (list (format "[%d] %s"
                                  (plist-get err-info :code)
                                  (plist-get err-info :message))))))
         (_
          (let* ((raw-row (mysql--parse-result-row row-packet col-count))
                 (typed-row (mysql--convert-row raw-row columns)))
            (push typed-row rows))))))
    (make-mysql-result
     :connection conn
     :status "OK"
     :columns columns
     :rows (nreverse rows))))

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

(provide 'mysql)
;;; mysql.el ends here
