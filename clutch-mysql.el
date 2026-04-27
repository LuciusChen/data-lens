;;; clutch-mysql.el --- Internal MySQL wire protocol client -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: comm, data
;; URL: https://github.com/LuciusChen/clutch
;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is part of clutch.

;; clutch is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; clutch is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with clutch.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; A pure Emacs Lisp MySQL client that implements the MySQL wire protocol
;; directly for clutch, without depending on the external `mysql' CLI.
;;
;; Usage:
;;
;;   (require 'clutch-mysql)
;;
;;   (let ((conn (clutch-mysql-connect :host "127.0.0.1"
;;                              :port 3306
;;                              :user "root"
;;                              :password "secret"
;;                              :database "mydb")))
;;     (let ((result (clutch-mysql-query conn "SELECT * FROM users LIMIT 10")))
;;       (clutch-mysql-result-rows result))
;;     (clutch-mysql-disconnect conn))

;;; Code:

(require 'cl-lib)

(declare-function gnutls-negotiate "gnutls" (&rest _spec))

;;;; Error types

(define-error 'clutch-mysql-error "MySQL error")
(define-error 'clutch-mysql-connection-error "MySQL connection error" 'clutch-mysql-error)
(define-error 'clutch-mysql-protocol-error "MySQL protocol error" 'clutch-mysql-error)
(define-error 'clutch-mysql-auth-error "MySQL authentication error" 'clutch-mysql-error)
(define-error 'clutch-mysql-query-error "MySQL query error" 'clutch-mysql-error)
(define-error 'clutch-mysql-timeout "MySQL timeout" 'clutch-mysql-error)
(define-error 'clutch-mysql-stmt-error "MySQL prepared statement error" 'clutch-mysql-error)

;;;; Capability flags (MySQL protocol)

(defconst clutch-mysql--cap-long-password        #x00000001)
(defconst clutch-mysql--cap-found-rows           #x00000002)
(defconst clutch-mysql--cap-long-flag            #x00000004)
(defconst clutch-mysql--cap-connect-with-db      #x00000008)
(defconst clutch-mysql--cap-protocol-41          #x00000200)
(defconst clutch-mysql--cap-transactions         #x00002000)
(defconst clutch-mysql--cap-secure-connection    #x00008000)
(defconst clutch-mysql--cap-ssl                  #x00000800)
(defconst clutch-mysql--cap-plugin-auth          #x00080000)
(defconst clutch-mysql--cap-plugin-auth-lenenc   #x00200000)
(defconst clutch-mysql--cap-deprecate-eof        #x01000000)

;;;; Server status flags

(defconst clutch-mysql--server-status-in-transaction #x0001
  "MySQL server status flag: a transaction is active.")

(defconst clutch-mysql--server-status-autocommit #x0002
  "MySQL server status flag: autocommit mode is enabled.")

;;;; TLS configuration

(defgroup clutch-mysql nil
  "Internal pure Elisp MySQL client for clutch."
  :group 'clutch
  :prefix "clutch-mysql-")

(defcustom clutch-mysql-tls-trustfiles nil
  "List of CA certificate file paths for TLS verification."
  :type '(repeat file)
  :group 'clutch-mysql)

(defcustom clutch-mysql-tls-keylist nil
  "List of (CERT-FILE KEY-FILE) pairs for client certificates."
  :type '(repeat (list file file))
  :group 'clutch-mysql)

(defcustom clutch-mysql-tls-verify-server t
  "Whether to verify the server certificate during TLS handshake."
  :type 'boolean
  :group 'clutch-mysql)

(defun clutch-mysql--normalize-ssl-mode (ssl-mode)
  "Return the canonical TLS-disabling SSL-MODE, or nil when absent.
MySQL's official spelling is `disabled'.  The compatibility alias `off' is
also accepted."
  (cond
   ((null ssl-mode) nil)
   ((memq ssl-mode '(disabled off)) 'disabled)
   ((and (stringp ssl-mode)
         (member (downcase ssl-mode) '("disabled" "off")))
    'disabled)
   (t
    (signal 'clutch-mysql-connection-error
            (list (format "Unsupported ssl-mode %S (supported: disabled)" ssl-mode))))))

;;;; Column type constants

(defconst clutch-mysql-type-decimal     0 "MySQL DECIMAL column type.")
(defconst clutch-mysql-type-tiny        1 "MySQL TINYINT column type.")
(defconst clutch-mysql-type-short       2 "MySQL SMALLINT column type.")
(defconst clutch-mysql-type-long        3 "MySQL INT column type.")
(defconst clutch-mysql-type-float       4 "MySQL FLOAT column type.")
(defconst clutch-mysql-type-double      5 "MySQL DOUBLE column type.")
(defconst clutch-mysql-type-null        6 "MySQL NULL column type.")
(defconst clutch-mysql-type-timestamp   7 "MySQL TIMESTAMP column type.")
(defconst clutch-mysql-type-longlong    8 "MySQL BIGINT column type.")
(defconst clutch-mysql-type-int24       9 "MySQL MEDIUMINT column type.")
(defconst clutch-mysql-type-date       10 "MySQL DATE column type.")
(defconst clutch-mysql-type-time       11 "MySQL TIME column type.")
(defconst clutch-mysql-type-datetime   12 "MySQL DATETIME column type.")
(defconst clutch-mysql-type-year       13 "MySQL YEAR column type.")
(defconst clutch-mysql-type-varchar    15 "MySQL VARCHAR column type.")
(defconst clutch-mysql-type-bit        16 "MySQL BIT column type.")
(defconst clutch-mysql-type-json      245 "MySQL JSON column type.")
(defconst clutch-mysql-type-newdecimal 246 "MySQL DECIMAL (new) column type.")
(defconst clutch-mysql-type-enum      247 "MySQL ENUM column type.")
(defconst clutch-mysql-type-set       248 "MySQL SET column type.")
(defconst clutch-mysql-type-tiny-blob  249 "MySQL TINYBLOB column type.")
(defconst clutch-mysql-type-medium-blob 250 "MySQL MEDIUMBLOB column type.")
(defconst clutch-mysql-type-long-blob  251 "MySQL LONGBLOB column type.")
(defconst clutch-mysql-type-blob       252 "MySQL BLOB column type.")
(defconst clutch-mysql-type-var-string 253 "MySQL VAR_STRING column type.")
(defconst clutch-mysql-type-string     254 "MySQL STRING column type.")
(defconst clutch-mysql-type-geometry   255 "MySQL GEOMETRY column type.")

;;;; Data structures

(cl-defstruct clutch-mysql-conn
  "A MySQL connection object."
  process
  (buf nil)
  host port user database
  server-version
  connection-id
  capability-flags
  character-set
  status-flags
  (read-idle-timeout 30)
  (sequence-id 0)
  (read-offset 0)
  tls
  (busy nil))

(cl-defstruct clutch-mysql-result
  "A MySQL query result."
  connection
  status
  columns
  rows
  affected-rows
  last-insert-id
  warnings)

;;;; Low-level I/O primitives

(defun clutch-mysql--ensure-data (conn n)
  "Ensure at least N bytes beyond read-offset are available in CONN's buffer.
Waits for data, resetting the idle deadline on each arrival.
Signals `clutch-mysql-timeout' or `clutch-mysql-connection-error' on failure."
  (let* ((proc (clutch-mysql-conn-process conn))
         (buf (clutch-mysql-conn-buf conn))
         (timeout (clutch-mysql-conn-read-idle-timeout conn))
         (deadline (+ (float-time) timeout)))
    (with-current-buffer buf
      (while (< (- (point-max) (+ (point-min) (clutch-mysql-conn-read-offset conn))) n)
        (let ((remaining (- deadline (float-time)))
              (prev-size (- (point-max) (point-min))))
          (when (<= remaining 0)
            (signal 'clutch-mysql-timeout
                    (list (format "Timed out waiting for %d bytes" n))))
          (if (accept-process-output proc remaining nil t)
              ;; Data arrived — reset the idle deadline.
              (when (> (- (point-max) (point-min)) prev-size)
                (setq deadline (+ (float-time) timeout)))
            ;; No new output from proc.  If the process exited, flush
            ;; any remaining queued output from the OS before giving up.
            (when (not (process-live-p proc))
              (accept-process-output nil 0.05 nil t)
              (when (< (- (point-max) (+ (point-min) (clutch-mysql-conn-read-offset conn))) n)
                (signal 'clutch-mysql-connection-error
                        (list "Connection closed by server"))))))))))

(defun clutch-mysql--read-bytes (conn n)
  "Read N bytes from CONN's input buffer as a unibyte string.
Advances read-offset without deleting buffer content."
  (clutch-mysql--ensure-data conn n)
  (with-current-buffer (clutch-mysql-conn-buf conn)
    (let* ((off (clutch-mysql-conn-read-offset conn))
           (start (+ (point-min) off))
           (str (buffer-substring-no-properties start (+ start n))))
      (setf (clutch-mysql-conn-read-offset conn) (+ off n))
      str)))

(defun clutch-mysql--read-byte (conn)
  "Read a single byte from CONN, returning it as an integer."
  (aref (clutch-mysql--read-bytes conn 1) 0))

(defun clutch-mysql--read-int-le (conn n)
  "Read a little-endian integer of N bytes from CONN."
  (let ((bytes (clutch-mysql--read-bytes conn n))
        (val 0)
        (i 0))
    (while (< i n)
      (setq val (logior val (ash (aref bytes i) (* i 8))))
      (setq i (1+ i)))
    val))

(defun clutch-mysql--read-lenenc-int (conn)
  "Read a length-encoded integer from CONN."
  (let ((first (clutch-mysql--read-byte conn)))
    (cond
     ((< first #xfb) first)
     ((= first #xfc) (clutch-mysql--read-int-le conn 2))
     ((= first #xfd) (clutch-mysql--read-int-le conn 3))
     ((= first #xfe) (clutch-mysql--read-int-le conn 8))
     (t (signal 'clutch-mysql-protocol-error
                (list (format "Invalid lenenc-int prefix: 0x%02x" first)))))))

;;;; Packet I/O

(defun clutch-mysql--read-packet (conn)
  "Read one MySQL packet from CONN.
Returns the payload as a unibyte string.  Handles packets split across
multiple 16 MB fragments."
  (let ((payload nil)
        (more t))
    (while more
      (let* ((len (clutch-mysql--read-int-le conn 3))
             (seq (clutch-mysql--read-byte conn)))
        (setf (clutch-mysql-conn-sequence-id conn) (logand (1+ seq) #xff))
        (let ((data (clutch-mysql--read-bytes conn len)))
          (push data payload))
        (setq more (= len #xffffff))))
    ;; Bulk-flush all bytes consumed by this packet; one delete-region per packet.
    (with-current-buffer (clutch-mysql-conn-buf conn)
      (let ((off (clutch-mysql-conn-read-offset conn)))
        (when (> off 0)
          (delete-region (point-min) (+ (point-min) off))
          (setf (clutch-mysql-conn-read-offset conn) 0))))
    (apply #'concat (nreverse payload))))

(defun clutch-mysql--int-le-bytes (value n)
  "Encode VALUE as a little-endian unibyte string of N bytes."
  (let ((bytes (make-string n 0))
        (i 0))
    (while (< i n)
      (aset bytes i (logand (ash value (* i -8)) #xff))
      (setq i (1+ i)))
    bytes))

(defun clutch-mysql--lenenc-int-bytes (value)
  "Encode VALUE as a length-encoded integer unibyte string."
  (cond
   ((< value #xfb)
    (unibyte-string value))
   ((<= value #xffff)
    (concat (unibyte-string #xfc) (clutch-mysql--int-le-bytes value 2)))
   ((<= value #xffffff)
    (concat (unibyte-string #xfd) (clutch-mysql--int-le-bytes value 3)))
   (t
    (concat (unibyte-string #xfe) (clutch-mysql--int-le-bytes value 8)))))

(defun clutch-mysql--send-packet (conn payload)
  "Send PAYLOAD as a MySQL packet on CONN.
Automatically prepends the 4-byte header (3-byte length + 1-byte sequence id).
Handles splitting payloads larger than 0xFFFFFF."
  (let ((proc (clutch-mysql-conn-process conn))
        (offset 0)
        (total (length payload)))
    (while (>= (- total offset) #xffffff)
      (let ((header (concat (clutch-mysql--int-le-bytes #xffffff 3)
                            (unibyte-string (clutch-mysql-conn-sequence-id conn)))))
        (setf (clutch-mysql-conn-sequence-id conn)
              (logand (1+ (clutch-mysql-conn-sequence-id conn)) #xff))
        (process-send-string proc header)
        (process-send-string proc (substring payload offset (+ offset #xffffff)))
        (cl-incf offset #xffffff)))
    (let* ((remaining (- total offset))
           (header (concat (clutch-mysql--int-le-bytes remaining 3)
                           (unibyte-string (clutch-mysql-conn-sequence-id conn)))))
      (setf (clutch-mysql-conn-sequence-id conn)
            (logand (1+ (clutch-mysql-conn-sequence-id conn)) #xff))
      (process-send-string proc header)
      (when (> remaining 0)
        (process-send-string proc (substring payload offset))))))

;;;; Authentication helpers

(defun clutch-mysql--sha1 (data)
  "Return the SHA-1 hash of DATA (a unibyte string) as a unibyte string."
  (let ((hex (secure-hash 'sha1 data nil nil t)))
    hex))

(defun clutch-mysql--xor-strings (a b)
  "XOR two equal-length unibyte strings A and B."
  (let* ((len (length a))
         (result (make-string len 0))
         (i 0))
    (while (< i len)
      (aset result i (logxor (aref a i) (aref b i)))
      (setq i (1+ i)))
    result))

(defun clutch-mysql--auth-native-password (password salt)
  "Compute mysql_native_password auth response.
PASSWORD is a plain-text string.  SALT is the 20-byte auth data from
the server handshake.  Returns a 20-byte unibyte string.

Algorithm: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))"
  (if (or (null password) (string-empty-p password))
      ""
    (let* ((pass-bytes (encode-coding-string password 'utf-8))
           (sha1-pass (clutch-mysql--sha1 pass-bytes))
           (sha1-sha1-pass (clutch-mysql--sha1 sha1-pass))
           (concat-salt (concat salt sha1-sha1-pass))
           (sha1-concat (clutch-mysql--sha1 concat-salt)))
      (clutch-mysql--xor-strings sha1-pass sha1-concat))))

(defun clutch-mysql--auth-caching-sha2-password (password salt)
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
      (clutch-mysql--xor-strings sha256-pass sha256-concat))))

;;;; Handshake

(defun clutch-mysql--read-le-uint (packet pos n)
  "Read an unsigned little-endian integer of N bytes from PACKET at POS."
  (let ((val 0)
        (i 0))
    (while (< i n)
      (setq val (logior val (ash (aref packet (+ pos i)) (* i 8))))
      (setq i (1+ i)))
    val))

(defun clutch-mysql--parse-handshake-salt (conn packet pos salt-part1 auth-data-len)
  "Parse the second salt part and auth plugin from handshake PACKET at POS.
CONN, SALT-PART1, and AUTH-DATA-LEN provide context.
Returns a plist with :salt and :auth-plugin."
  (let ((salt-part2 ""))
    (when (not (zerop (logand (clutch-mysql-conn-capability-flags conn)
                              clutch-mysql--cap-secure-connection)))
      (let ((part2-len (max 13 (- auth-data-len 8))))
        (setq salt-part2 (substring packet pos (+ pos part2-len)))
        (cl-incf pos part2-len)
        ;; Remove trailing NUL if present
        (when (and (> (length salt-part2) 0)
                   (= (aref salt-part2 (1- (length salt-part2))) 0))
          (setq salt-part2 (substring salt-part2 0 -1)))))
    (let ((auth-plugin nil))
      (when (not (zerop (logand (clutch-mysql-conn-capability-flags conn)
                                clutch-mysql--cap-plugin-auth)))
        (when-let* ((nul-pos (cl-position 0 packet :start pos)))
          (setq auth-plugin (substring packet pos nul-pos))))
      (list :salt (concat salt-part1 salt-part2)
            :auth-plugin (or auth-plugin "mysql_native_password")))))

(defun clutch-mysql--parse-handshake-body (conn packet pos salt-part1)
  "Parse capability flags and auth data from PACKET at POS into CONN.
SALT-PART1 is the first 8 bytes of the auth plugin data.
Returns the result of `clutch-mysql--parse-handshake-salt'."
  (cl-incf pos 9) ;; skip 8-byte salt-part1 + 1 filler byte
  (setf (clutch-mysql-conn-capability-flags conn) (clutch-mysql--read-le-uint packet pos 2))
  (cl-incf pos 2)
  (setf (clutch-mysql-conn-character-set conn) (aref packet pos))
  (cl-incf pos 1)
  (setf (clutch-mysql-conn-status-flags conn) (clutch-mysql--read-le-uint packet pos 2))
  (cl-incf pos 2)
  ;; capability_flags high 2 bytes — merge with low
  (setf (clutch-mysql-conn-capability-flags conn)
        (logior (clutch-mysql-conn-capability-flags conn)
                (ash (clutch-mysql--read-le-uint packet pos 2) 16)))
  (cl-incf pos 2)
  (let ((auth-data-len (aref packet pos)))
    (cl-incf pos 11) ;; 1 auth_plugin_data_len + 10 reserved
    (clutch-mysql--parse-handshake-salt conn packet pos salt-part1 auth-data-len)))

(defun clutch-mysql--parse-handshake (conn packet)
  "Parse a HandshakeV10 PACKET and update CONN with server info.
Returns a plist with :salt and :auth-plugin."
  (let ((pos 0))
    (unless (= (aref packet 0) 10)
      (signal 'clutch-mysql-protocol-error
              (list (format "Unsupported protocol version: %d" (aref packet 0)))))
    (cl-incf pos)
    ;; server_version: NUL-terminated string
    (let ((nul-pos (cl-position 0 packet :start pos)))
      (setf (clutch-mysql-conn-server-version conn) (substring packet pos nul-pos))
      (setq pos (1+ nul-pos)))
    (setf (clutch-mysql-conn-connection-id conn) (clutch-mysql--read-le-uint packet pos 4))
    (cl-incf pos 4)
    ;; auth_plugin_data_part_1 (8 bytes) + capability flags + auth data
    (let ((salt-part1 (substring packet pos (+ pos 8))))
      (clutch-mysql--parse-handshake-body conn packet pos salt-part1))))

(defun clutch-mysql--client-capabilities (conn)
  "Compute the client capability flags for CONN."
  (logior clutch-mysql--cap-long-password
          clutch-mysql--cap-found-rows
          clutch-mysql--cap-long-flag
          clutch-mysql--cap-protocol-41
          clutch-mysql--cap-transactions
          clutch-mysql--cap-secure-connection
          clutch-mysql--cap-plugin-auth
          (if (clutch-mysql-conn-tls conn) clutch-mysql--cap-ssl 0)
          (if (clutch-mysql-conn-database conn) clutch-mysql--cap-connect-with-db 0)))

(defun clutch-mysql--build-handshake-response (conn password salt auth-plugin)
  "Build a HandshakeResponse41 packet for CONN.
PASSWORD is the user password, SALT is the server nonce, AUTH-PLUGIN
is the authentication plugin name."
  (let* ((client-flags (clutch-mysql--client-capabilities conn))
         (auth-response (clutch-mysql--compute-auth-response password salt auth-plugin))
         (parts nil))
    (setf (clutch-mysql-conn-capability-flags conn)
          (logand client-flags (clutch-mysql-conn-capability-flags conn)))
    (push (clutch-mysql--int-le-bytes client-flags 4) parts)
    (push (clutch-mysql--int-le-bytes #x00ffffff 4) parts)  ;; max_packet_size
    (push (unibyte-string 45) parts)                  ;; charset: utf8mb4
    (push (make-string 23 0) parts)                   ;; filler
    (push (concat (encode-coding-string (clutch-mysql-conn-user conn) 'utf-8)
                  (unibyte-string 0))
          parts)
    (push (unibyte-string (length auth-response)) parts)
    (push auth-response parts)
    (when (clutch-mysql-conn-database conn)
      (push (concat (encode-coding-string (clutch-mysql-conn-database conn) 'utf-8)
                    (unibyte-string 0))
            parts))
    (push (concat (encode-coding-string auth-plugin 'utf-8)
                  (unibyte-string 0))
          parts)
    (apply #'concat (nreverse parts))))

(defun clutch-mysql--compute-auth-response (password salt auth-plugin)
  "Compute auth response for the given AUTH-PLUGIN.
PASSWORD is the plaintext password, SALT is the server nonce."
  (pcase auth-plugin
    ("mysql_native_password"
     (clutch-mysql--auth-native-password password salt))
    ("caching_sha2_password"
     (clutch-mysql--auth-caching-sha2-password password salt))
    (_
     (signal 'clutch-mysql-auth-error
             (list (format "Unsupported auth plugin: %s" auth-plugin))))))

;;;; Response parsing helpers

(defun clutch-mysql--parse-ok-packet (packet)
  "Parse an OK_Packet from PACKET (first byte 0x00 already verified).
Returns a plist with :affected-rows, :last-insert-id, :status-flags, :warnings."
  (pcase-let* ((`(,affected-rows . ,pos1) (clutch-mysql--read-lenenc-int-from-string packet 1))
               (`(,last-insert-id . ,pos) (clutch-mysql--read-lenenc-int-from-string packet pos1))
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

(defun clutch-mysql--parse-err-packet (packet)
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

(defun clutch-mysql--packet-type (packet)
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

(defun clutch-mysql--read-lenenc-int-from-string (str pos)
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

(defun clutch-mysql--read-lenenc-string-from-string (str pos)
  "Read a length-encoded string from STR at POS.
Returns (string . new-pos)."
  (pcase-let* ((`(,len . ,p) (clutch-mysql--read-lenenc-int-from-string str pos)))
    (cons (substring str p (+ p len)) (+ p len))))

(defun clutch-mysql--parse-column-definition (packet)
  "Parse a Column Definition PACKET.
Returns a plist with column metadata."
  ;; Read the 6 lenenc-string fields: catalog, schema, table, org_table, name, org_name
  (let* ((pos 0)
         (strings (cl-loop repeat 6
                           collect (pcase-let ((`(,str . ,new-pos)
                                               (clutch-mysql--read-lenenc-string-from-string packet pos)))
                                     (setq pos new-pos)
                                     (decode-coding-string str 'utf-8)))))
    ;; Fixed-length fields after 0x0c marker: offsets relative to pos+1
    (cl-incf pos 1)
    (let ((character-set (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
          (column-length (logior (aref packet (+ pos 2))
                                 (ash (aref packet (+ pos 3)) 8)
                                 (ash (aref packet (+ pos 4)) 16)
                                 (ash (aref packet (+ pos 5)) 24)))
          (column-type (aref packet (+ pos 6)))
          (flags (logior (aref packet (+ pos 7)) (ash (aref packet (+ pos 8)) 8)))
          (decimals (aref packet (+ pos 9))))
      (pcase-let ((`(,catalog ,schema ,table ,org-table ,name ,org-name) strings))
        (list :catalog catalog :schema schema
              :table table :org-table org-table
              :name name :org-name org-name
              :character-set character-set
              :column-length column-length :type column-type :flags flags
              :decimals decimals)))))

;;;; Row parsing

(defun clutch-mysql--parse-result-row (packet column-count)
  "Parse a result row from PACKET with COLUMN-COUNT columns.
Each column value is either NULL (0xFB prefix) or a lenenc-string."
  (let ((pos 0)
        (row nil))
    (dotimes (_ column-count)
      (if (= (aref packet pos) #xfb)
          (progn
            (push nil row)
            (cl-incf pos 1))
        (pcase-let ((`(,val . ,new-pos) (clutch-mysql--read-lenenc-string-from-string packet pos)))
          (push val row)
          (setq pos new-pos))))
    (nreverse row)))

;;;; Type conversion

(defvar clutch-mysql-type-parsers nil
  "Alist of (TYPE-CODE . PARSER-FN) for custom type parsing.
Each PARSER-FN takes a single string argument and returns the
converted Elisp value.  Entries here override built-in parsers.")

(defun clutch-mysql--parse-date (value)
  "Parse MySQL DATE VALUE \"YYYY-MM-DD\" into a plist.
Returns (:year Y :month M :day D), or nil for zero dates."
  (if (or (string= value "0000-00-00") (string-empty-p value))
      nil
    (pcase-let ((`(,y ,m ,d) (split-string value "-")))
      (list :year (string-to-number y)
            :month (string-to-number m)
            :day (string-to-number d)))))

(defun clutch-mysql--parse-time (value)
  "Parse MySQL TIME VALUE \"[-]HH:MM:SS[.ffffff]\" into a plist.
Returns (:hours H :minutes M :seconds S :negative BOOL)."
  (if (string-empty-p value)
      nil
    (let* ((negative (string-prefix-p "-" value))
           (s (if negative (substring value 1) value))
           (dot-pos (string-search "." s))
           (time-part (if dot-pos (substring s 0 dot-pos) s)))
      (pcase-let ((`(,h ,m ,sec) (split-string time-part ":")))
        (list :hours (string-to-number h)
              :minutes (string-to-number m)
              :seconds (string-to-number sec)
              :negative negative)))))

(defun clutch-mysql--parse-datetime (value)
  "Parse MySQL DATETIME/TIMESTAMP VALUE into a plist.
Input: \"YYYY-MM-DD HH:MM:SS[.ffffff]\".
Returns (:year Y :month M :day D :hours H :minutes M :seconds S),
or nil for zero datetimes."
  (if (or (string-prefix-p "0000-00-00" value) (string-empty-p value))
      nil
    (let* ((space-pos (string-search " " value))
           (date-part (substring value 0 space-pos))
           (time-part (if space-pos (substring value (1+ space-pos)) "00:00:00"))
           (dot-pos (string-search "." time-part))
           (time-base (if dot-pos (substring time-part 0 dot-pos) time-part)))
      (pcase-let ((`(,y ,mo ,d) (split-string date-part "-"))
                  (`(,h ,mi ,s) (split-string time-base ":")))
        (list :year (string-to-number y)
              :month (string-to-number mo)
              :day (string-to-number d)
              :hours (string-to-number h)
              :minutes (string-to-number mi)
              :seconds (string-to-number s))))))

(defun clutch-mysql--parse-bit (value)
  "Parse MySQL BIT binary VALUE into an integer."
  (let ((result 0))
    (dotimes (i (length value))
      (setq result (logior (ash result 8) (aref value i))))
    result))

(defun clutch-mysql--parse-typed-value (value type)
  "Parse non-null string VALUE according to MySQL column TYPE code."
  (if-let* ((custom (alist-get type clutch-mysql-type-parsers)))
      (funcall custom value)
    (pcase type
      ((or 1 2 3 8 9)   (string-to-number value))  ;; integers
      ((or 4 5)         (string-to-number value))  ;; float/double
      ((or 0 246)       (string-to-number value))  ;; decimal/newdecimal
      (13               (string-to-number value))  ;; year
      ((or 7 12)        (clutch-mysql--parse-datetime value))
      (10               (clutch-mysql--parse-date value))
      (11               (clutch-mysql--parse-time value))
      (16               (clutch-mysql--parse-bit value))
      (245
       (let ((s (decode-coding-string value 'utf-8)))
         (if (fboundp 'json-parse-string) (json-parse-string s) s)))
      (_                (decode-coding-string value 'utf-8)))))

(defun clutch-mysql--parse-value (value type)
  "Parse string VALUE according to MySQL column TYPE code.
Returns the converted Elisp value, or nil for SQL NULL."
  (when value (clutch-mysql--parse-typed-value value type)))

(defun clutch-mysql--convert-row (row columns)
  "Convert ROW values according to COLUMNS type information."
  (cl-mapcar (lambda (val col)
               (clutch-mysql--parse-value val (plist-get col :type)))
             row columns))

;;;; TLS support

(defun clutch-mysql--tls-available-p ()
  "Return non-nil if GnuTLS support is available in this Emacs."
  (and (fboundp 'gnutls-available-p) (gnutls-available-p)))

(defun clutch-mysql--build-ssl-request (conn)
  "Build a 32-byte SSL_REQUEST packet for CONN."
  (let* ((client-flags (logior clutch-mysql--cap-long-password
                               clutch-mysql--cap-found-rows
                               clutch-mysql--cap-long-flag
                               clutch-mysql--cap-protocol-41
                               clutch-mysql--cap-ssl
                               clutch-mysql--cap-transactions
                               clutch-mysql--cap-secure-connection
                               clutch-mysql--cap-plugin-auth
                               (if (clutch-mysql-conn-database conn)
                                   clutch-mysql--cap-connect-with-db
                                 0))))
    (concat (clutch-mysql--int-le-bytes client-flags 4)
            (clutch-mysql--int-le-bytes #x00ffffff 4)
            (unibyte-string 45)
            (make-string 23 0))))

(defun clutch-mysql--upgrade-to-tls (conn)
  "Upgrade CONN's network connection to TLS using GnuTLS."
  (let ((proc (clutch-mysql-conn-process conn)))
    (require 'gnutls)
    (gnutls-negotiate
     :process proc
     :hostname (clutch-mysql-conn-host conn)
     :trustfiles clutch-mysql-tls-trustfiles
     :keylist clutch-mysql-tls-keylist
     :verify-hostname-error clutch-mysql-tls-verify-server
     :verify-error clutch-mysql-tls-verify-server)
    (setf (clutch-mysql-conn-tls conn) t)))

(defun clutch-mysql--cleanup-connection-resources (proc buf)
  "Dispose of partially opened MySQL transport resources PROC and BUF."
  (when (process-live-p proc) (delete-process proc))
  (when (buffer-live-p buf) (kill-buffer buf)))

(defun clutch-mysql--caching-sha2-full-auth-requires-tls-p (err)
  "Return non-nil when ERR is the caching_sha2 full-auth TLS requirement."
  (pcase err
    (`(clutch-mysql-auth-error ,message)
     (equal message "caching_sha2_password full authentication requires TLS"))
    (_ nil)))

(defun clutch-mysql--requested-tls-mode (tls tls-specified-p ssl-mode)
  "Return the requested TLS mode for TLS, TLS-SPECIFIED-P, and SSL-MODE.
The return value is one of the symbols `default', `required', or `disabled'."
  (let ((normalized-ssl-mode (clutch-mysql--normalize-ssl-mode ssl-mode)))
    (when (and normalized-ssl-mode tls-specified-p tls)
      (signal 'clutch-mysql-connection-error
              (list "Conflicting MySQL TLS options: :tls t cannot be combined with :ssl-mode disabled")))
    (cond
     (normalized-ssl-mode 'disabled)
     ((and tls-specified-p (null tls)) 'disabled)
     (tls 'required)
     (t 'default))))

(defun clutch-mysql--retry-auth-with-tls-p (err tls-mode)
  "Return non-nil when ERR should trigger a TLS reconnect retry.
ERR is the condition data raised during authentication and TLS-MODE is the
original connection mode."
  (and (eq tls-mode 'default)
       (clutch-mysql--tls-available-p)
       (clutch-mysql--caching-sha2-full-auth-requires-tls-p err)))

;;;; Connection

(defun clutch-mysql--authenticate (conn password tls)
  "Perform the MySQL handshake and authentication sequence on CONN.
PASSWORD is the plaintext password; TLS non-nil means upgrade to TLS first."
  (let* ((handshake-packet (clutch-mysql--read-packet conn))
         (handshake-info (clutch-mysql--parse-handshake conn handshake-packet))
         (salt (plist-get handshake-info :salt))
         (auth-plugin (plist-get handshake-info :auth-plugin)))
    (when tls
      (when (zerop (logand (clutch-mysql-conn-capability-flags conn)
                          clutch-mysql--cap-ssl))
        (signal 'clutch-mysql-connection-error
                (list "Server does not support SSL")))
      (setf (clutch-mysql-conn-sequence-id conn) 1)
      (clutch-mysql--send-packet conn (clutch-mysql--build-ssl-request conn))
      (clutch-mysql--upgrade-to-tls conn))
    (setf (clutch-mysql-conn-sequence-id conn) (if tls 2 1))
    (clutch-mysql--send-packet conn
                        (clutch-mysql--build-handshake-response conn password salt auth-plugin))
    (clutch-mysql--handle-auth-response conn password salt auth-plugin)))

(defun clutch-mysql--wait-for-connect (proc host port connect-timeout)
  "Wait for PROC to connect to HOST:PORT within CONNECT-TIMEOUT seconds."
  (let ((deadline (and connect-timeout
                       (+ (float-time) connect-timeout))))
    (while (eq (process-status proc) 'connect)
      (let ((remaining (if deadline
                           (- deadline (float-time))
                         0.05)))
        (when (and deadline (<= remaining 0))
          (delete-process proc)
          (signal 'clutch-mysql-connection-error
                  (list (format "Timed out connecting to %s:%s" host port))))
        ;; Poll in short slices.  Some Emacs/network stacks do not wake
        ;; `accept-process-output' promptly on connect state transitions,
        ;; which otherwise stretches a fast localhost connect to the full
        ;; timeout window.
        (accept-process-output proc (if deadline
                                        (min 0.05 (max 0.0 remaining))
                                      0.05))))
    (unless (memq (process-status proc) '(open run))
      (signal 'clutch-mysql-connection-error
              (list (format "Failed to connect to %s:%s" host port))))))

(defun clutch-mysql--open-connection (host port &optional connect-timeout)
  "Open a raw TCP connection to HOST:PORT for MySQL.
CONNECT-TIMEOUT, when non-nil, limits the initial socket connect.
Returns (PROCESS . BUFFER)."
  (let ((buf (generate-new-buffer " *clutch-mysql-input*")))
    (with-current-buffer buf
      (set-buffer-multibyte nil))
    (let ((proc (make-network-process :name "mysql"
                                      :buffer buf
                                      :host host
                                      :service port
                                      :nowait t
                                      :coding 'binary)))
      (set-process-coding-system proc 'binary 'binary)
      (set-process-filter proc
                          (lambda (_proc data)
                            (with-current-buffer buf
                              (goto-char (point-max))
                              (insert data))))
      (clutch-mysql--wait-for-connect proc host port connect-timeout)
      (cons proc buf))))

(cl-defun clutch-mysql-connect (&key (host "127.0.0.1") (port 3306) user password
                                database (tls nil tls-specified-p) ssl-mode
                                (read-idle-timeout 30) (connect-timeout 10))
  "Connect to a MySQL server and authenticate.
Returns a `clutch-mysql-conn' struct on success.

HOST defaults to \"127.0.0.1\", PORT defaults to 3306.
USER, PASSWORD, and DATABASE are strings (DATABASE is optional).
When TLS is non-nil, upgrade the connection to TLS before authenticating.
When TLS is explicitly nil, keep the connection on plaintext and suppress the
automatic MySQL 8 TLS retry path.  When SSL-MODE is `disabled', also force
plaintext; the compatibility alias `off' is accepted.  Combining `:tls t`
with `:ssl-mode disabled' signals an error.
READ-IDLE-TIMEOUT limits query I/O stalls.  CONNECT-TIMEOUT limits the initial
TCP connection wait."
  (unless user
    (signal 'clutch-mysql-connection-error (list "No user specified")))
  (let* ((tls-mode (clutch-mysql--requested-tls-mode tls tls-specified-p ssl-mode))
         (tls (eq tls-mode 'required))
         (ssl-mode (clutch-mysql--normalize-ssl-mode ssl-mode)))
    (when (and tls (not (clutch-mysql--tls-available-p)))
    (signal 'clutch-mysql-connection-error (list "TLS requested but GnuTLS is not available")))
    (pcase-let ((`(,proc . ,buf) (clutch-mysql--open-connection host port connect-timeout)))
      (let ((conn (make-clutch-mysql-conn :process proc :buf buf
                                   :host host :port port
                                   :user user :database database
                                   :read-idle-timeout read-idle-timeout)))
        (condition-case err
            (progn
              (clutch-mysql--authenticate conn password tls)
              conn)
          (clutch-mysql-auth-error
           (clutch-mysql--cleanup-connection-resources proc buf)
           (if (clutch-mysql--retry-auth-with-tls-p err tls-mode)
               (clutch-mysql-connect :host host :port port
                              :user user :password password
                              :database database :tls t :ssl-mode ssl-mode
                              :read-idle-timeout read-idle-timeout
                              :connect-timeout connect-timeout)
             (signal (car err) (cdr err))))
          (error
           (clutch-mysql--cleanup-connection-resources proc buf)
           (signal (car err) (cdr err))))))))

(defun clutch-mysql--handle-auth-switch (conn password packet)
  "Handle an AUTH_SWITCH_REQUEST in PACKET for CONN.
Resend PASSWORD with the new plugin and continue authentication."
  (let* ((pos 1)
         (nul-pos (cl-position 0 packet :start pos))
         (new-plugin (substring packet pos nul-pos))
         (new-salt (substring packet (1+ nul-pos)
                              (if (= (aref packet (1- (length packet))) 0)
                                  (1- (length packet))
                                (length packet))))
         (new-auth (clutch-mysql--compute-auth-response password new-salt new-plugin)))
    (clutch-mysql--send-packet conn new-auth)
    (clutch-mysql--handle-auth-response conn password new-salt new-plugin)))

(defun clutch-mysql--handle-auth-more-data (conn password salt auth-plugin packet)
  "Handle caching_sha2_password AuthMoreData in PACKET for CONN.
Dispatches on fast-auth success vs full-auth requirement for PASSWORD
using SALT and AUTH-PLUGIN."
  (pcase (aref packet 1)
    (#x03
     ;; Fast auth success -- read the OK packet that follows
     (let ((ok-packet (clutch-mysql--read-packet conn)))
       (pcase (clutch-mysql--packet-type ok-packet)
         ('ok
          (let ((ok-info (clutch-mysql--parse-ok-packet ok-packet)))
            (setf (clutch-mysql-conn-status-flags conn) (plist-get ok-info :status-flags))))
         ('err
          (let ((err-info (clutch-mysql--parse-err-packet ok-packet)))
            (signal 'clutch-mysql-auth-error
                    (list (format "Auth failed after fast-auth: [%d] %s"
                                  (plist-get err-info :code)
                                  (plist-get err-info :message)))))))))
    (#x04
     ;; Full authentication required
     (if (clutch-mysql-conn-tls conn)
         (let ((cleartext (concat (encode-coding-string (or password "") 'utf-8)
                                  (unibyte-string 0))))
           (clutch-mysql--send-packet conn cleartext)
           (clutch-mysql--handle-auth-response conn password salt auth-plugin))
       (signal 'clutch-mysql-auth-error
               (list "caching_sha2_password full authentication requires TLS"))))))

(defun clutch-mysql--handle-auth-response (conn password salt auth-plugin)
  "Handle the authentication response from the server.
CONN is the connection, PASSWORD is the plaintext password,
SALT is the nonce, AUTH-PLUGIN is the current auth plugin name."
  (let ((packet (condition-case _err
                    (clutch-mysql--read-packet conn)
                  (clutch-mysql-connection-error
                   (signal 'clutch-mysql-auth-error
                           (list "Connection closed during authentication"))))))
    ;; Dispatch on first byte directly (not clutch-mysql--packet-type, because
    ;; AUTH_SWITCH_REQUEST 0xFE can exceed 9 bytes).
    (pcase (aref packet 0)
      (#x00
       (let ((ok-info (clutch-mysql--parse-ok-packet packet)))
         (setf (clutch-mysql-conn-status-flags conn) (plist-get ok-info :status-flags))))
      (#xff
       (let ((err-info (clutch-mysql--parse-err-packet packet)))
         (signal 'clutch-mysql-auth-error
                 (list (format "Authentication failed: [%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (#xfe
       (clutch-mysql--handle-auth-switch conn password packet))
      (#x01
       (when (> (length packet) 1)
         (clutch-mysql--handle-auth-more-data conn password salt auth-plugin packet))))))

;;;; Query execution

(defun clutch-mysql--handle-query-response (conn packet)
  "Dispatch on PACKET type and return a `clutch-mysql-result' for CONN."
  (pcase (clutch-mysql--packet-type packet)
    ('ok
     (let ((ok-info (clutch-mysql--parse-ok-packet packet)))
       (setf (clutch-mysql-conn-status-flags conn)
             (plist-get ok-info :status-flags))
       (make-clutch-mysql-result
        :connection conn
        :status "OK"
        :affected-rows (plist-get ok-info :affected-rows)
        :last-insert-id (plist-get ok-info :last-insert-id)
        :warnings (plist-get ok-info :warnings))))
    ('err
     (let ((err-info (clutch-mysql--parse-err-packet packet)))
       (signal 'clutch-mysql-query-error
               (list (format "[%d] %s%s"
                             (plist-get err-info :code)
                             (if (plist-get err-info :state)
                                 (format "(%s) " (plist-get err-info :state))
                               "")
                             (plist-get err-info :message))))))
    (_
     ;; Result set: first byte is column_count (lenenc int)
     (clutch-mysql--read-result-set conn packet))))

(defun clutch-mysql-query (conn sql)
  "Execute SQL query on CONN and return a `clutch-mysql-result'.
SQL is a string containing the query to execute.
Signals `clutch-mysql-error' if the connection is busy (re-entrant call)."
  (when (clutch-mysql-conn-busy conn)
    (signal 'clutch-mysql-error
            (list "Connection busy — cannot send query while another is in progress")))
  ;; Flush any stale data left from previously interrupted queries.
  (with-current-buffer (clutch-mysql-conn-buf conn)
    (erase-buffer)
    (setf (clutch-mysql-conn-read-offset conn) 0))
  (setf (clutch-mysql-conn-busy conn) t)
  (unwind-protect
      ;; Bind throw-on-input to nil so that `while-no-input' (used by
      ;; completion frameworks like corfu/company) cannot abort us
      ;; mid-response, which would leave partial data in the buffer and
      ;; corrupt subsequent queries.
      (let ((throw-on-input nil))
        (setf (clutch-mysql-conn-sequence-id conn) 0)
        (clutch-mysql--send-packet conn (concat (unibyte-string #x03)
                                         (encode-coding-string sql 'utf-8)))
        (clutch-mysql--handle-query-response conn (clutch-mysql--read-packet conn)))
    (setf (clutch-mysql-conn-busy conn) nil)))

(defun clutch-mysql--read-column-definitions (conn col-count)
  "Read COL-COUNT column definition packets from CONN.
Returns a list of column plists.  Also consumes the EOF packet."
  (let ((columns (cl-loop repeat col-count
                          collect (clutch-mysql--parse-column-definition
                                   (clutch-mysql--read-packet conn)))))
    ;; Read EOF after columns (unless CLIENT_DEPRECATE_EOF)
    (when (zerop (logand (clutch-mysql-conn-capability-flags conn)
                        clutch-mysql--cap-deprecate-eof))
      (let ((eof-packet (clutch-mysql--read-packet conn)))
        (unless (eq (clutch-mysql--packet-type eof-packet) 'eof)
          (signal 'clutch-mysql-protocol-error
                  (list "Missing EOF packet after column definitions")))))
    columns))

(defun clutch-mysql--read-text-rows (conn col-count columns)
  "Read text protocol rows from CONN until EOF.
COL-COUNT and COLUMNS guide parsing.  Returns rows in order."
  (let ((rows nil))
    (cl-loop
     (let ((row-packet (clutch-mysql--read-packet conn)))
       (pcase (clutch-mysql--packet-type row-packet)
         ((or 'eof 'ok) (cl-return nil))
         ('err
          (let ((err-info (clutch-mysql--parse-err-packet row-packet)))
            (signal 'clutch-mysql-query-error
                    (list (format "[%d] %s"
                                  (plist-get err-info :code)
                                  (plist-get err-info :message))))))
         (_ (push (clutch-mysql--convert-row
                   (clutch-mysql--parse-result-row row-packet col-count) columns)
                  rows)))))
    (nreverse rows)))

(defun clutch-mysql--read-result-set (conn first-packet)
  "Read a full result set from CONN.
FIRST-PACKET contains the column-count.  Returns a `clutch-mysql-result'."
  (let* ((col-count (let ((b (aref first-packet 0)))
                      (if (< b #xfb) b
                        (car (clutch-mysql--read-lenenc-int-from-string first-packet 0)))))
         (columns (clutch-mysql--read-column-definitions conn col-count))
         (rows (clutch-mysql--read-text-rows conn col-count columns)))
    (make-clutch-mysql-result
     :connection conn
     :status "OK"
     :columns columns
     :rows rows)))

;;;; Disconnect

(defun clutch-mysql-disconnect (conn)
  "Disconnect from MySQL server, sending COM_QUIT.
CONN is a `clutch-mysql-conn' returned by `clutch-mysql-connect'."
  (when conn
    (condition-case nil
        (when (process-live-p (clutch-mysql-conn-process conn))
          ;; Send COM_QUIT
          (setf (clutch-mysql-conn-sequence-id conn) 0)
          (clutch-mysql--send-packet conn (unibyte-string #x01)))
      (error nil))
    (when (process-live-p (clutch-mysql-conn-process conn))
      (delete-process (clutch-mysql-conn-process conn)))
    (when (buffer-live-p (clutch-mysql-conn-buf conn))
      (kill-buffer (clutch-mysql-conn-buf conn)))))

;;;; Prepared statements

(cl-defstruct clutch-mysql-stmt
  "A MySQL prepared statement."
  conn id param-count column-count param-definitions column-definitions)

(defun clutch-mysql--read-definition-packets (conn count)
  "Read COUNT column-definition packets from CONN, then consume the EOF.
Returns a list of parsed definitions, or nil when COUNT is 0."
  (when (> count 0)
    (prog1 (cl-loop repeat count
                    collect (clutch-mysql--parse-column-definition
                             (clutch-mysql--read-packet conn)))
      (clutch-mysql--read-packet conn)))) ;; EOF after definitions

(defun clutch-mysql--parse-prepare-ok (conn packet)
  "Parse a COM_STMT_PREPARE_OK response from PACKET.
Reads param and column definition packets from CONN.
Returns a `clutch-mysql-stmt'."
  (unless (= (aref packet 0) #x00)
    (signal 'clutch-mysql-stmt-error (list "Non-OK status in PREPARE response")))
  (let* ((stmt-id     (logior (aref packet 1) (ash (aref packet 2) 8)
                              (ash (aref packet 3) 16) (ash (aref packet 4) 24)))
         (num-columns (logior (aref packet 5) (ash (aref packet 6) 8)))
         (num-params  (logior (aref packet 7) (ash (aref packet 8) 8)))
         (param-defs  (clutch-mysql--read-definition-packets conn num-params))
         (col-defs    (clutch-mysql--read-definition-packets conn num-columns)))
    (make-clutch-mysql-stmt :conn conn
                     :id stmt-id
                     :param-count num-params
                     :column-count num-columns
                     :param-definitions param-defs
                     :column-definitions col-defs)))

(defun clutch-mysql--elisp-to-wire-type (value)
  "Map Elisp VALUE to a 2-byte MySQL type code (little-endian).
Returns a cons (TYPE-CODE . UNSIGNED-FLAG)."
  (cond
   ((null value) (cons clutch-mysql-type-null 0))
   ((integerp value) (cons clutch-mysql-type-longlong 0))
   ((floatp value) (cons clutch-mysql-type-var-string 0))
   ((stringp value) (cons clutch-mysql-type-var-string 0))
   (t (cons clutch-mysql-type-var-string 0))))

(defun clutch-mysql--encode-binary-value (value)
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
      (concat (clutch-mysql--lenenc-int-bytes (length s)) s)))
   ((stringp value)
    (let ((encoded (encode-coding-string value 'utf-8)))
      (concat (clutch-mysql--lenenc-int-bytes (length encoded)) encoded)))
   (t
    (let ((s (format "%s" value)))
      (concat (clutch-mysql--lenenc-int-bytes (length s)) s)))))

(defun clutch-mysql--build-null-bitmap (params param-count)
  "Return a NULL-bitmap string for PARAMS (length PARAM-COUNT).
Each bit is set for a NULL parameter."
  (let* ((bitmap-len (/ (+ param-count 7) 8))
         (bitmap (make-string bitmap-len 0)))
    (dotimes (i param-count)
      (when (null (nth i params))
        (let ((byte-idx (/ i 8))
              (bit-idx  (% i 8)))
          (aset bitmap byte-idx
                (logior (aref bitmap byte-idx) (ash 1 bit-idx))))))
    bitmap))

(defun clutch-mysql--build-execute-packet (stmt params)
  "Build a COM_STMT_EXECUTE packet for STMT with PARAMS."
  (let* ((stmt-id     (clutch-mysql-stmt-id stmt))
         (param-count (clutch-mysql-stmt-param-count stmt))
         (parts nil))
    (push (unibyte-string #x17) parts)            ;; command byte
    (push (clutch-mysql--int-le-bytes stmt-id 4) parts)  ;; stmt_id: 4 bytes LE
    (push (unibyte-string #x00) parts)            ;; flags: no cursor
    (push (clutch-mysql--int-le-bytes 1 4) parts)        ;; iteration_count: always 1
    (when (> param-count 0)
      (push (clutch-mysql--build-null-bitmap params param-count) parts)
      (push (unibyte-string #x01) parts) ;; new_params_bound_flag
      (dotimes (i param-count) ;; type array: 2 bytes per param
        (let ((type-info (clutch-mysql--elisp-to-wire-type (nth i params))))
          (push (unibyte-string (car type-info) (cdr type-info)) parts)))
      (dotimes (i param-count) ;; values (non-NULL only)
        (unless (null (nth i params))
          (push (clutch-mysql--encode-binary-value (nth i params)) parts))))
    (apply #'concat (nreverse parts))))

(defun clutch-mysql-prepare (conn sql)
  "Prepare SQL statement on CONN.  Returns a `clutch-mysql-stmt'."
  (setf (clutch-mysql-conn-sequence-id conn) 0)
  (clutch-mysql--send-packet conn (concat (unibyte-string #x16)
                                   (encode-coding-string sql 'utf-8)))
  (let ((packet (clutch-mysql--read-packet conn)))
    (pcase (clutch-mysql--packet-type packet)
      ('err
       (let ((err-info (clutch-mysql--parse-err-packet packet)))
         (signal 'clutch-mysql-stmt-error
                 (list (format "[%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (_ (clutch-mysql--parse-prepare-ok conn packet)))))

(defun clutch-mysql-execute (stmt &rest params)
  "Execute prepared STMT with PARAMS.  Returns a `clutch-mysql-result'."
  (let ((conn (clutch-mysql-stmt-conn stmt)))
    (unless (= (length params) (clutch-mysql-stmt-param-count stmt))
      (signal 'clutch-mysql-stmt-error
              (list (format "Expected %d params, got %d"
                            (clutch-mysql-stmt-param-count stmt) (length params)))))
    (setf (clutch-mysql-conn-sequence-id conn) 0)
    (clutch-mysql--send-packet conn (clutch-mysql--build-execute-packet stmt params))
    (let ((packet (clutch-mysql--read-packet conn)))
      (pcase (clutch-mysql--packet-type packet)
        ('ok
         (let ((ok-info (clutch-mysql--parse-ok-packet packet)))
           (setf (clutch-mysql-conn-status-flags conn) (plist-get ok-info :status-flags))
           (make-clutch-mysql-result
            :connection conn
            :status "OK"
            :affected-rows (plist-get ok-info :affected-rows)
            :last-insert-id (plist-get ok-info :last-insert-id)
            :warnings (plist-get ok-info :warnings))))
        ('err
         (let ((err-info (clutch-mysql--parse-err-packet packet)))
           (signal 'clutch-mysql-stmt-error
                   (list (format "[%d] %s"
                                 (plist-get err-info :code)
                                 (plist-get err-info :message))))))
        (_
         ;; Binary result set
         (clutch-mysql--read-binary-result-set conn packet))))))

(defun clutch-mysql-stmt-close (stmt)
  "Close prepared STMT.  No server response is expected."
  (let ((conn (clutch-mysql-stmt-conn stmt)))
    (setf (clutch-mysql-conn-sequence-id conn) 0)
    (clutch-mysql--send-packet conn (concat (unibyte-string #x19)
                                     (clutch-mysql--int-le-bytes (clutch-mysql-stmt-id stmt) 4)))))

;; Binary result set reading

(defun clutch-mysql--read-binary-rows (conn columns)
  "Read binary row packets from CONN until EOF, returning rows in order.
COLUMNS is the column-definition list.
Binary rows start with 0x00 so we cannot use `clutch-mysql--packet-type';
the result set ends with an EOF packet (0xFE, ≤9 bytes)."
  (let (rows)
    (cl-loop
     (let ((row-packet (clutch-mysql--read-packet conn)))
       (cond
        ((and (= (aref row-packet 0) #xfe) (<= (length row-packet) 9))
         (cl-return (nreverse rows)))
        ((= (aref row-packet 0) #xff)
         (let ((err-info (clutch-mysql--parse-err-packet row-packet)))
           (signal 'clutch-mysql-stmt-error
                   (list (format "[%d] %s"
                                 (plist-get err-info :code)
                                 (plist-get err-info :message))))))
        (t
         (push (clutch-mysql--parse-binary-row row-packet columns) rows)))))))

(defun clutch-mysql--read-binary-result-set (conn first-packet)
  "Read a binary protocol result set from CONN.
FIRST-PACKET contains the column count.  Returns a `clutch-mysql-result'."
  (let* ((col-count (aref first-packet 0))
         (columns   (cl-loop repeat col-count
                             collect (clutch-mysql--parse-column-definition
                                      (clutch-mysql--read-packet conn)))))
    (clutch-mysql--read-packet conn) ;; EOF after column definitions
    (make-clutch-mysql-result
     :connection conn
     :status "OK"
     :columns columns
     :rows (clutch-mysql--read-binary-rows conn columns))))

(defun clutch-mysql--binary-null-p (null-bitmap col-index)
  "Check if column COL-INDEX is NULL in NULL-BITMAP.
Binary row NULL bitmap has a 2-bit offset."
  (let* ((offset (+ col-index 2))
         (byte-idx (/ offset 8))
         (bit-idx (% offset 8)))
    (not (zerop (logand (aref null-bitmap byte-idx) (ash 1 bit-idx))))))

(defun clutch-mysql--parse-binary-row (packet columns)
  "Parse a binary protocol row from PACKET using COLUMNS metadata."
  ;; First byte is 0x00 (packet header for binary rows)
  (let* ((col-count (length columns))
         (bitmap-len (/ (+ col-count 2 7) 8))
         (null-bitmap (substring packet 1 (+ 1 bitmap-len)))
         (pos (+ 1 bitmap-len))
         (row nil))
    (dotimes (i col-count)
      (if (clutch-mysql--binary-null-p null-bitmap i)
          (push nil row)
        (pcase-let ((`(,val . ,new-pos) (clutch-mysql--decode-binary-value packet pos
                                                                    (plist-get (nth i columns) :type))))
          (push val row)
          (setq pos new-pos))))
    (nreverse row)))

(defun clutch-mysql--decode-binary-lenenc-string (packet pos)
  "Decode a length-encoded string from PACKET at POS.
Returns (string . new-pos)."
  (pcase-let ((`(,len . ,start) (clutch-mysql--read-lenenc-int-from-string packet pos)))
    (cons (substring packet start (+ start len)) (+ start len))))

(defun clutch-mysql--decode-binary-value (packet pos type)
  "Decode a binary value from PACKET at POS for the given TYPE.
Returns (value . new-pos)."
  (pcase type
    ((pred (= clutch-mysql-type-tiny))
     (cons (aref packet pos) (1+ pos)))
    ((or (pred (= clutch-mysql-type-short)) (pred (= clutch-mysql-type-year)))
     (cons (clutch-mysql--read-le-uint packet pos 2) (+ pos 2)))
    ((or (pred (= clutch-mysql-type-long)) (pred (= clutch-mysql-type-int24)))
     (cons (clutch-mysql--read-le-uint packet pos 4) (+ pos 4)))
    ((pred (= clutch-mysql-type-longlong))
     (cons (clutch-mysql--read-le-uint packet pos 8) (+ pos 8)))
    ((pred (= clutch-mysql-type-float))
     (cons (clutch-mysql--ieee754-single-to-float packet pos) (+ pos 4)))
    ((pred (= clutch-mysql-type-double))
     (cons (clutch-mysql--ieee754-double-to-float packet pos) (+ pos 8)))
    ((or (pred (= clutch-mysql-type-date))
         (pred (= clutch-mysql-type-datetime))
         (pred (= clutch-mysql-type-timestamp)))
     (clutch-mysql--decode-binary-datetime packet pos type))
    ((pred (= clutch-mysql-type-time))
     (clutch-mysql--decode-binary-time packet pos))
    ((pred (= clutch-mysql-type-null))
     (cons nil pos))
    (_
     (clutch-mysql--decode-binary-lenenc-string packet pos))))

(defun clutch-mysql--ieee754-single-to-float (data offset)
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

(defun clutch-mysql--ieee754-double-to-float (data offset)
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
         ;; Multipliers are (expt 2.0 N) pre-computed as literals:
         ;;   48->281474976710656.0  40->1099511627776.0  32->4294967296.0
         ;;   24->16777216.0         16->65536.0           8->256.0
         (mantissa (+ (* (float (logand b6 #x0f)) 281474976710656.0)
                      (* (float b5) 1099511627776.0)
                      (* (float b4) 4294967296.0)
                      (* (float b3) 16777216.0)
                      (* (float b2) 65536.0)
                      (* (float b1) 256.0)
                      (float b0))))
    ;; 2^52 = 4503599627370496.0
    (cond
     ((= exponent 0)
      (if (= mantissa 0.0) (* sign 0.0)
        (* sign (ldexp (/ mantissa 4503599627370496.0) -1022))))
     ((= exponent #x7ff)
      (if (= mantissa 0.0) (* sign 1.0e+INF) 0.0e+NaN))
     (t
      (* sign (ldexp (+ 1.0 (/ mantissa 4503599627370496.0)) (- exponent 1023)))))))

(defun clutch-mysql--decode-binary-datetime (packet pos type)
  "Decode a binary DATE/DATETIME/TIMESTAMP of TYPE from PACKET at POS.
Returns (value . new-pos)."
  (let ((len (aref packet pos)))
    (cl-incf pos)
    (pcase len
      (0 (cons nil pos))
      (4
       (let ((year (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
             (month (aref packet (+ pos 2)))
             (day (aref packet (+ pos 3))))
         (cons (if (= type clutch-mysql-type-date)
                   (list :year year :month month :day day)
                 (list :year year :month month :day day
                       :hours 0 :minutes 0 :seconds 0))
               (+ pos 4))))
      ((or 7 11)
       (let ((year (logior (aref packet pos) (ash (aref packet (+ pos 1)) 8)))
             (month (aref packet (+ pos 2)))
             (day (aref packet (+ pos 3)))
             (hours (aref packet (+ pos 4)))
             (minutes (aref packet (+ pos 5)))
             (seconds (aref packet (+ pos 6))))
         (cons (list :year year :month month :day day
                     :hours hours :minutes minutes :seconds seconds)
               (+ pos len))))
      (_ (cons nil (+ pos len))))))

(defun clutch-mysql--decode-binary-time (packet pos)
  "Decode a binary TIME value from PACKET at POS.
Returns (value . new-pos)."
  (let ((len (aref packet pos)))
    (cl-incf pos)
    (pcase len
      (0 (cons (list :hours 0 :minutes 0 :seconds 0 :negative nil) pos))
      ((or 8 12)
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
               (+ pos len))))
      (_ (cons nil (+ pos len))))))

;;;; Convenience APIs

(defmacro with-clutch-mysql-connection (var connect-args &rest body)
  "Execute BODY with VAR bound to a MySQL connection.
CONNECT-ARGS is a plist passed to `clutch-mysql-connect'.
The connection is automatically closed when BODY exits."
  (declare (indent 2))
  `(let ((,var (clutch-mysql-connect ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (clutch-mysql-disconnect ,var))))

(defmacro with-clutch-mysql-transaction (conn &rest body)
  "Execute BODY inside a SQL transaction on CONN.
Issues BEGIN before BODY.  If BODY completes normally, issues COMMIT.
If BODY signals an error, issues ROLLBACK before re-raising."
  (declare (indent 1))
  (let ((c (make-symbol "conn")))
    `(let ((,c ,conn))
       (clutch-mysql-query ,c "BEGIN")
       (condition-case err
           (prog1 (progn ,@body)
             (clutch-mysql-query ,c "COMMIT"))
         (error
         (clutch-mysql-query ,c "ROLLBACK")
          (signal (car err) (cdr err)))))))

(defun clutch-mysql-autocommit-p (conn)
  "Return non-nil when MySQL CONN has autocommit enabled.
When CONN has not yet observed any server status flags, default to
assuming autocommit mode."
  (let ((flags (clutch-mysql-conn-status-flags conn)))
    (if (integerp flags)
        (not (zerop (logand flags clutch-mysql--server-status-autocommit)))
      t)))

(defun clutch-mysql-in-transaction-p (conn)
  "Return non-nil when MySQL CONN is inside a transaction."
  (let ((flags (clutch-mysql-conn-status-flags conn)))
    (and (integerp flags)
         (not (zerop (logand flags clutch-mysql--server-status-in-transaction))))))

(defun clutch-mysql-set-autocommit (conn enabled)
  "Set MySQL CONN autocommit mode to ENABLED.
ENABLED non-nil turns autocommit on; nil turns it off."
  (clutch-mysql-query conn
               (format "SET autocommit = %d"
                       (if enabled 1 0))))

(defun clutch-mysql-commit (conn)
  "Commit the current transaction on MySQL CONN."
  (clutch-mysql-query conn "COMMIT"))

(defun clutch-mysql-rollback (conn)
  "Roll back the current transaction on MySQL CONN."
  (clutch-mysql-query conn "ROLLBACK"))

(defun clutch-mysql-ping (conn)
  "Send COM_PING to the MySQL server via CONN.
Returns t if the server is alive, or signals an error."
  (setf (clutch-mysql-conn-sequence-id conn) 0)
  (clutch-mysql--send-packet conn (unibyte-string #x0e))
  (let ((packet (clutch-mysql--read-packet conn)))
    (pcase (clutch-mysql--packet-type packet)
      ('ok t)
      ('err
       (let ((err-info (clutch-mysql--parse-err-packet packet)))
         (signal 'clutch-mysql-error
                 (list (format "Ping failed: [%d] %s"
                               (plist-get err-info :code)
                               (plist-get err-info :message))))))
      (_ (signal 'clutch-mysql-protocol-error (list "Unexpected response to COM_PING"))))))

(defun clutch-mysql-escape-identifier (name)
  "Escape NAME for use as a MySQL identifier.
Wraps in backticks and doubles any embedded backticks."
  (concat "`" (replace-regexp-in-string "`" "``" name) "`"))

(defun clutch-mysql-escape-literal (value)
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

(defun clutch-mysql-connect-uri (uri)
  "Connect to MySQL using a URI string.
URI format: mysql://user:password@host:port/database"
  (unless (string-match
           "\\`mysql://\\([^:@]*\\)\\(?::\\([^@]*\\)\\)?@\\([^:/]*\\)\\(?::\\([0-9]+\\)\\)?\\(?:/\\(.*\\)\\)?\\'"
           uri)
    (signal 'clutch-mysql-connection-error (list (format "Invalid MySQL URI: %s" uri))))
  (let ((user (match-string 1 uri))
        (password (match-string 2 uri))
        (host (match-string 3 uri))
        (port (if-let* ((p (match-string 4 uri))) (string-to-number p) 3306))
        (database (match-string 5 uri)))
    (clutch-mysql-connect :host host :port port :user user
                   :password password
                   :database (if (or (null database) (string-empty-p database))
                                 nil database))))

(provide 'clutch-mysql)
;;; clutch-mysql.el ends here
