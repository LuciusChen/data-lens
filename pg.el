;;; pg.el --- Pure Elisp PostgreSQL client -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

;; Author: Lucius Chen
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: databases, postgresql, comm
;; URL: https://github.com/LuciusChen/mysql.el

;; This file is part of data-lens.

;; data-lens is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; data-lens is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with data-lens.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; A pure Emacs Lisp PostgreSQL client that implements the PostgreSQL
;; wire protocol v3 directly, without depending on external tools.
;;
;; Usage:
;;
;;   (require 'pg)
;;
;;   (let ((conn (pg-connect :host "127.0.0.1"
;;                           :port 5432
;;                           :user "postgres"
;;                           :password "secret"
;;                           :database "mydb")))
;;     (let ((result (pg-query conn "SELECT * FROM users LIMIT 10")))
;;       (pg-result-rows result))
;;     (pg-disconnect conn))

;;; Code:

(require 'cl-lib)

(declare-function gnutls-negotiate "gnutls" (&rest _spec))

;;;; Error types

(define-error 'pg-error "PostgreSQL error")
(define-error 'pg-connection-error "PostgreSQL connection error" 'pg-error)
(define-error 'pg-protocol-error "PostgreSQL protocol error" 'pg-error)
(define-error 'pg-auth-error "PostgreSQL authentication error" 'pg-error)
(define-error 'pg-query-error "PostgreSQL query error" 'pg-error)
(define-error 'pg-timeout "PostgreSQL timeout" 'pg-error)

;;;; TLS configuration

(defgroup pg nil
  "Pure Elisp PostgreSQL client."
  :group 'comm
  :prefix "pg-")

(defcustom pg-tls-trustfiles nil
  "List of CA certificate file paths for TLS verification."
  :type '(repeat file)
  :group 'pg)

(defcustom pg-tls-keylist nil
  "List of (CERT-FILE KEY-FILE) pairs for client certificates."
  :type '(repeat (list file file))
  :group 'pg)

(defcustom pg-tls-verify-server t
  "Whether to verify the server certificate during TLS handshake."
  :type 'boolean
  :group 'pg)

;;;; Common OIDs for type parsing

(defconst pg-oid-bool     16)
(defconst pg-oid-bytea    17)
(defconst pg-oid-int8     20)
(defconst pg-oid-int2     21)
(defconst pg-oid-int4     23)
(defconst pg-oid-text     25)
(defconst pg-oid-json    114)
(defconst pg-oid-float4  700)
(defconst pg-oid-float8  701)
(defconst pg-oid-varchar 1043)
(defconst pg-oid-date    1082)
(defconst pg-oid-time    1083)
(defconst pg-oid-timestamp 1114)
(defconst pg-oid-timestamptz 1184)
(defconst pg-oid-numeric 1700)
(defconst pg-oid-jsonb   3802)

;;;; Data structures

(cl-defstruct pg-conn
  "A PostgreSQL connection object."
  process
  (buf nil)
  host port user database
  server-version
  pid secret-key
  (parameters nil)
  (read-timeout 10)
  tls
  (busy nil))

(cl-defstruct pg-result
  "A PostgreSQL query result."
  connection
  status
  columns
  rows
  affected-rows)

;;;; Low-level I/O primitives

(defun pg--ensure-data (conn n)
  "Ensure at least N bytes are available in CONN's input buffer."
  (let* ((proc (pg-conn-process conn))
         (buf (pg-conn-buf conn))
         (timeout (pg-conn-read-timeout conn))
         (deadline (+ (float-time) timeout)))
    (with-current-buffer buf
      (while (< (- (point-max) (point-min)) n)
        (let ((remaining (- deadline (float-time)))
              (prev-size (- (point-max) (point-min))))
          (when (<= remaining 0)
            (signal 'pg-timeout
                    (list (format "Timed out waiting for %d bytes" n))))
          (if (accept-process-output proc remaining nil t)
              (when (> (- (point-max) (point-min)) prev-size)
                (setq deadline (+ (float-time) timeout)))
            (when (not (process-live-p proc))
              (accept-process-output nil 0.05 nil t)
              (when (< (- (point-max) (point-min)) n)
                (signal 'pg-connection-error
                        (list "Connection closed by server"))))))))))

(defun pg--read-bytes (conn n)
  "Read N bytes from CONN's input buffer as a unibyte string."
  (pg--ensure-data conn n)
  (with-current-buffer (pg-conn-buf conn)
    (goto-char (point-min))
    (let ((start (point)))
      (forward-char n)
      (let ((str (buffer-substring-no-properties start (point))))
        (delete-region start (point))
        str))))

(defun pg--read-byte (conn)
  "Read a single byte from CONN, returning it as an integer."
  (aref (pg--read-bytes conn 1) 0))

(defun pg--read-int32-be (conn)
  "Read a 4-byte big-endian integer from CONN."
  (let ((bytes (pg--read-bytes conn 4)))
    (logior (ash (aref bytes 0) 24)
            (ash (aref bytes 1) 16)
            (ash (aref bytes 2) 8)
            (aref bytes 3))))

(defun pg--read-int16-be (conn)
  "Read a 2-byte big-endian integer from CONN."
  (let ((bytes (pg--read-bytes conn 2)))
    (logior (ash (aref bytes 0) 8)
            (aref bytes 1))))

(defun pg--read-string-nul (conn)
  "Read a NUL-terminated string from CONN."
  (let ((chars nil))
    (cl-loop for b = (pg--read-byte conn)
             until (= b 0)
             do (push b chars))
    (decode-coding-string
     (apply #'unibyte-string (nreverse chars))
     'utf-8)))

;;;; Wire encoding helpers

(defun pg--int32-be-bytes (value)
  "Encode VALUE as a 4-byte big-endian unibyte string."
  (unibyte-string (logand (ash value -24) #xff)
                  (logand (ash value -16) #xff)
                  (logand (ash value -8) #xff)
                  (logand value #xff)))

(defun pg--int16-be-bytes (value)
  "Encode VALUE as a 2-byte big-endian unibyte string."
  (unibyte-string (logand (ash value -8) #xff)
                  (logand value #xff)))

(defun pg--encode-string (str)
  "Encode STR as a NUL-terminated unibyte string."
  (concat (encode-coding-string str 'utf-8) (unibyte-string 0)))

;;;; Message I/O

(defun pg--send-message (conn type payload)
  "Send a message of TYPE with PAYLOAD on CONN.
TYPE is a character (e.g., ?Q for Query) or nil for startup messages.
PAYLOAD is a unibyte string."
  (let ((proc (pg-conn-process conn))
        (len (+ 4 (length payload))))
    (when type
      (process-send-string proc (unibyte-string type)))
    (process-send-string proc (pg--int32-be-bytes len))
    (when (> (length payload) 0)
      (process-send-string proc payload))))

(defun pg--send-startup-message (conn)
  "Send a StartupMessage on CONN (no type byte, just length + data)."
  (let* ((user (pg-conn-user conn))
         (database (or (pg-conn-database conn) user))
         (payload (concat (pg--int32-be-bytes #x00030000)  ; protocol version 3.0
                          (pg--encode-string "user")
                          (pg--encode-string user)
                          (pg--encode-string "database")
                          (pg--encode-string database)
                          (pg--encode-string "client_encoding")
                          (pg--encode-string "UTF8")
                          (unibyte-string 0)))  ; terminator
         (proc (pg-conn-process conn))
         (len (+ 4 (length payload))))
    (process-send-string proc (pg--int32-be-bytes len))
    (process-send-string proc payload)))

(defun pg--read-message (conn)
  "Read one message from CONN.
Returns a cons (TYPE . PAYLOAD) where TYPE is a character and
PAYLOAD is a unibyte string."
  (let* ((type (pg--read-byte conn))
         (len (pg--read-int32-be conn))
         (payload-len (- len 4))
         (payload (if (> payload-len 0)
                      (pg--read-bytes conn payload-len)
                    "")))
    (cons type payload)))

;;;; Message parsing helpers

(defun pg--read-be-int32-from-string (str pos)
  "Read a 4-byte big-endian signed integer from STR at POS."
  (let ((unsigned (logior (ash (aref str pos) 24)
                          (ash (aref str (+ pos 1)) 16)
                          (ash (aref str (+ pos 2)) 8)
                          (aref str (+ pos 3)))))
    (if (> unsigned #x7fffffff)
        (- unsigned #x100000000)
      unsigned)))

(defun pg--read-be-int16-from-string (str pos)
  "Read a 2-byte big-endian integer from STR at POS."
  (logior (ash (aref str pos) 8)
          (aref str (+ pos 1))))

(defun pg--read-nul-string-from-string (str pos)
  "Read a NUL-terminated string from STR at POS.
Returns (STRING . NEW-POS)."
  (let ((nul-pos (cl-position 0 str :start pos)))
    (if nul-pos
        (cons (decode-coding-string (substring str pos nul-pos) 'utf-8)
              (1+ nul-pos))
      (cons (decode-coding-string (substring str pos) 'utf-8)
            (length str)))))

;;;; Error/Notice parsing

(defun pg--parse-error-fields (payload)
  "Parse an ErrorResponse or NoticeResponse PAYLOAD.
Returns an alist of (field-code . value) pairs."
  (let ((pos 0))
    (cl-loop while (and (< pos (length payload))
                        (not (zerop (aref payload pos))))
             for code = (aref payload pos)
             do (cl-incf pos)
             for (str . new-pos) = (pg--read-nul-string-from-string payload pos)
             do (setq pos new-pos)
             collect (cons code str))))

(defun pg--error-fields-message (fields)
  "Format a human-readable error message from FIELDS alist."
  (let ((severity (or (alist-get ?S fields) "ERROR"))
        (code (or (alist-get ?C fields) "?????"))
        (message (or (alist-get ?M fields) "Unknown error")))
    (format "%s [%s]: %s" severity code message)))

;;;; Authentication

(defun pg--md5-hex (data)
  "Return the MD5 hash of DATA as a lowercase hex string."
  (secure-hash 'md5 data))

(defun pg--md5-password (user password salt)
  "Compute MD5 password hash for PostgreSQL.
Returns the string \"md5\" + md5(md5(password + user) + salt)."
  (let* ((inner (pg--md5-hex (concat password user)))
         (outer (pg--md5-hex (concat inner salt))))
    (concat "md5" outer)))

(defun pg--handle-auth-md5 (conn password payload)
  "Handle AuthenticationMD5Password.
PAYLOAD contains the 4-byte salt at offset 4."
  (let* ((salt (substring payload 4 8))
         (hash (pg--md5-password (pg-conn-user conn) password salt))
         (msg (pg--encode-string hash)))
    (pg--send-message conn ?p msg)))

(defun pg--handle-auth-cleartext (conn password)
  "Handle AuthenticationCleartextPassword."
  (pg--send-message conn ?p (pg--encode-string (or password ""))))

;; SCRAM-SHA-256 (SASL) authentication

(defun pg--hmac-sha256 (key data)
  "Compute HMAC-SHA-256 of DATA with KEY.
Both KEY and DATA should be unibyte strings.
Returns a 32-byte unibyte string."
  (let* ((block-size 64)
         (key-len (length key))
         (actual-key (if (> key-len block-size)
                         (secure-hash 'sha256 key nil nil t)
                       key))
         (padded-key (concat actual-key
                             (make-string (- block-size (length actual-key)) 0)))
         (o-pad (make-string block-size 0))
         (i-pad (make-string block-size 0)))
    (dotimes (i block-size)
      (aset o-pad i (logxor (aref padded-key i) #x5c))
      (aset i-pad i (logxor (aref padded-key i) #x36)))
    (secure-hash 'sha256 (concat o-pad (secure-hash 'sha256 (concat i-pad data) nil nil t))
                 nil nil t)))

(defun pg--pbkdf2-sha256 (password salt iterations key-length)
  "Derive KEY-LENGTH bytes using PBKDF2 with HMAC-SHA-256.
PASSWORD and SALT are unibyte strings, ITERATIONS is an integer."
  (let ((dk nil)
        (block 1)
        (hlen 32))
    (while (< (length (apply #'concat (nreverse (copy-sequence dk)))) key-length)
      (let* ((u1 (pg--hmac-sha256 password
                                   (concat salt (pg--int32-be-bytes block))))
             (u-prev u1)
             (result (copy-sequence u1)))
        (dotimes (_ (1- iterations))
          (let ((u-next (pg--hmac-sha256 password u-prev)))
            (dotimes (j hlen)
              (aset result j (logxor (aref result j) (aref u-next j))))
            (setq u-prev u-next)))
        (push result dk))
      (cl-incf block))
    (substring (apply #'concat (nreverse dk)) 0 key-length)))

(defun pg--xor-strings (a b)
  "XOR two equal-length unibyte strings A and B."
  (let* ((len (length a))
         (result (make-string len 0)))
    (dotimes (idx len)
      (aset result idx (logxor (aref a idx) (aref b idx))))
    result))

(defun pg--scram-client-first (_user)
  "Generate SCRAM-SHA-256 client-first-message.
Returns (CLIENT-FIRST-MESSAGE CLIENT-NONCE . CLIENT-FIRST-BARE)."
  (let ((nonce-bytes (make-string 18 0)))
    (dotimes (i 18)
      (aset nonce-bytes i (+ 33 (random 94))))
    (let* ((client-nonce (base64-encode-string nonce-bytes t))
           (client-first-bare (format "n=,r=%s" client-nonce))
           (client-first (concat "n,," client-first-bare)))
      (cons client-first (cons client-nonce client-first-bare)))))

(defun pg--scram-parse-server-first (server-first)
  "Parse server-first-message.
Returns (:nonce NONCE :salt SALT :iterations ITER)."
  (let ((nonce nil) (salt nil) (iterations nil))
    (dolist (part (split-string server-first ","))
      (cond
       ((string-prefix-p "r=" part)
        (setq nonce (substring part 2)))
       ((string-prefix-p "s=" part)
        (setq salt (base64-decode-string (substring part 2))))
       ((string-prefix-p "i=" part)
        (setq iterations (string-to-number (substring part 2))))))
    (list :nonce nonce :salt salt :iterations iterations)))

(defun pg--scram-client-final (password client-nonce client-first-bare
                                        server-first)
  "Generate SCRAM-SHA-256 client-final-message and verify server proof.
Returns (CLIENT-FINAL-MESSAGE . SERVER-SIGNATURE)."
  (let* ((parsed (pg--scram-parse-server-first server-first))
         (server-nonce (plist-get parsed :nonce))
         (salt (plist-get parsed :salt))
         (iterations (plist-get parsed :iterations)))
    (unless (string-prefix-p client-nonce server-nonce)
      (signal 'pg-auth-error (list "Server nonce does not start with client nonce")))
    (let* ((salted-password (pg--pbkdf2-sha256
                             (encode-coding-string password 'utf-8)
                             salt iterations 32))
           (client-key (pg--hmac-sha256 salted-password
                                         (encode-coding-string "Client Key" 'utf-8)))
           (stored-key (secure-hash 'sha256 client-key nil nil t))
           (channel-binding (base64-encode-string
                             (encode-coding-string "n,," 'utf-8) t))
           (client-final-without-proof
            (format "c=%s,r=%s" channel-binding server-nonce))
           (auth-message (concat client-first-bare ","
                                 server-first ","
                                 client-final-without-proof))
           (client-signature (pg--hmac-sha256
                              stored-key
                              (encode-coding-string auth-message 'utf-8)))
           (client-proof (pg--xor-strings client-key client-signature))
           (server-key (pg--hmac-sha256 salted-password
                                         (encode-coding-string "Server Key" 'utf-8)))
           (server-signature (pg--hmac-sha256
                              server-key
                              (encode-coding-string auth-message 'utf-8)))
           (proof-b64 (base64-encode-string client-proof t))
           (client-final (format "%s,p=%s"
                                 client-final-without-proof proof-b64)))
      (cons client-final server-signature))))

(defun pg--parse-sasl-mechanisms (payload)
  "Parse SASL mechanism names from an AuthenticationSASL PAYLOAD.
Skips the 4-byte auth-type header.  Returns a list of mechanism name strings."
  (let ((pos 4))
    (cl-loop while (and (< pos (length payload))
                        (not (zerop (aref payload pos))))
             for (name . new-pos) = (pg--read-nul-string-from-string payload pos)
             do (setq pos new-pos)
             collect name)))

(defun pg--sasl-read-auth-message (conn expected-type label)
  "Read an Authentication message from CONN and verify its type.
EXPECTED-TYPE is the int32 auth subtype.  LABEL is for error messages.
Returns the message payload."
  (pcase-let ((`(,msg-type . ,msg-payload) (pg--read-message conn)))
    (unless (= msg-type ?R)
      (signal 'pg-auth-error (list (format "Expected %s" label))))
    (let ((auth-type (pg--read-be-int32-from-string msg-payload 0)))
      (unless (= auth-type expected-type)
        (signal 'pg-auth-error
                (list (format "Expected %s (%d), got %d"
                              label expected-type auth-type)))))
    msg-payload))

(defun pg--sasl-verify-server-signature (payload expected-sig)
  "Verify the server signature in SASL final PAYLOAD against EXPECTED-SIG."
  (let* ((server-final (decode-coding-string (substring payload 4) 'utf-8))
         (v-part (when (string-prefix-p "v=" server-final)
                   (base64-decode-string (substring server-final 2)))))
    (unless (equal v-part expected-sig)
      (signal 'pg-auth-error (list "Server signature verification failed")))))

(defun pg--handle-auth-sasl (conn password payload)
  "Handle SASL authentication (SCRAM-SHA-256).
PAYLOAD contains the list of SASL mechanism names."
  (let ((mechanisms (pg--parse-sasl-mechanisms payload)))
    (unless (member "SCRAM-SHA-256" mechanisms)
      (signal 'pg-auth-error
              (list (format "Server requires unsupported SASL mechanism(s): %s"
                            (mapconcat #'identity mechanisms ", "))))))
  ;; Step 1: SASLInitialResponse
  (let* ((scram-data (pg--scram-client-first (pg-conn-user conn)))
         (client-first (car scram-data))
         (client-nonce (cadr scram-data))
         (client-first-bare (cddr scram-data))
         (msg-data (encode-coding-string client-first 'utf-8))
         (sasl-payload (concat (pg--encode-string "SCRAM-SHA-256")
                               (pg--int32-be-bytes (length msg-data))
                               msg-data)))
    (pg--send-message conn ?p sasl-payload)
    ;; Step 2: Read SASLContinue, compute client-final
    (let* ((cont-payload (pg--sasl-read-auth-message conn 11 "SASLContinue"))
           (server-first (decode-coding-string (substring cont-payload 4) 'utf-8))
           (final-data (pg--scram-client-final
                        password client-nonce client-first-bare server-first)))
      ;; Step 3: Send client-final
      (pg--send-message conn ?p (encode-coding-string (car final-data) 'utf-8))
      ;; Step 4: Verify server signature
      (let ((final-payload (pg--sasl-read-auth-message conn 12 "SASLFinal")))
        (pg--sasl-verify-server-signature final-payload (cdr final-data))))))

(defun pg--handle-authentication (conn password)
  "Handle the authentication phase after sending StartupMessage.
Reads auth messages and responds appropriately."
  (let ((done nil))
    (while (not done)
      (let* ((msg (pg--read-message conn))
             (msg-type (car msg))
             (payload (cdr msg)))
        (pcase msg-type
          (?R  ;; Authentication*
           (let ((auth-type (pg--read-be-int32-from-string payload 0)))
             (pcase auth-type
               (0  ;; AuthenticationOk
                (setq done t))
               (3  ;; AuthenticationCleartextPassword
                (pg--handle-auth-cleartext conn password))
               (5  ;; AuthenticationMD5Password
                (pg--handle-auth-md5 conn password payload))
               (10 ;; AuthenticationSASL
                (pg--handle-auth-sasl conn password payload))
               (_
                (signal 'pg-auth-error
                        (list (format "Unsupported auth type: %d" auth-type)))))))
          (?E  ;; ErrorResponse
           (let* ((fields (pg--parse-error-fields payload))
                  (msg-str (pg--error-fields-message fields)))
             (signal 'pg-auth-error (list msg-str))))
          (_ (signal 'pg-protocol-error
                     (list (format "Unexpected message type during auth: %c"
                                   msg-type)))))))))

;;;; Post-auth: read until ReadyForQuery

(defun pg--read-startup-messages (conn)
  "Read ParameterStatus, BackendKeyData, and ReadyForQuery after auth."
  (let ((done nil))
    (while (not done)
      (let* ((msg (pg--read-message conn))
             (msg-type (car msg))
             (payload (cdr msg)))
        (pcase msg-type
          (?S  ;; ParameterStatus
           (pcase-let ((`(,name . ,pos1)
                        (pg--read-nul-string-from-string payload 0)))
             (pcase-let ((`(,value . ,_pos2)
                          (pg--read-nul-string-from-string payload pos1)))
               (push (cons name value) (pg-conn-parameters conn))
               (when (string= name "server_version")
                 (setf (pg-conn-server-version conn) value)))))
          (?K  ;; BackendKeyData
           (setf (pg-conn-pid conn) (pg--read-be-int32-from-string payload 0))
           (setf (pg-conn-secret-key conn)
                 (pg--read-be-int32-from-string payload 4)))
          (?Z  ;; ReadyForQuery
           (setq done t))
          (?E  ;; ErrorResponse
           (let* ((fields (pg--parse-error-fields payload))
                  (msg-str (pg--error-fields-message fields)))
             (signal 'pg-connection-error (list msg-str))))
          (?N  ;; NoticeResponse — ignore
           nil)
          (_ nil))))))

;;;; TLS support

(defun pg--tls-available-p ()
  "Return non-nil if GnuTLS support is available."
  (and (fboundp 'gnutls-available-p) (gnutls-available-p)))

(defun pg--send-ssl-request (conn)
  "Send an SSLRequest to the server.
Returns the server's response character (?S or ?N)."
  (let ((proc (pg-conn-process conn)))
    ;; SSLRequest: length=8, code=80877103
    (process-send-string proc (pg--int32-be-bytes 8))
    (process-send-string proc (pg--int32-be-bytes 80877103))
    (pg--read-byte conn)))

(defun pg--upgrade-to-tls (conn)
  "Upgrade CONN's network connection to TLS."
  (let ((proc (pg-conn-process conn)))
    (gnutls-negotiate
     :process proc
     :hostname (pg-conn-host conn)
     :trustfiles pg-tls-trustfiles
     :keylist pg-tls-keylist
     :verify-hostname-error pg-tls-verify-server
     :verify-error pg-tls-verify-server)
    (setf (pg-conn-tls conn) t)))

;;;; Connection

(defun pg--open-connection (host port)
  "Open a raw TCP connection to HOST:PORT for PostgreSQL.
Returns (PROCESS . BUFFER)."
  (let ((buf (generate-new-buffer " *pg-input*")))
    (with-current-buffer buf
      (set-buffer-multibyte nil))
    (let ((proc (open-network-stream "pg" buf host port
                                     :type 'plain
                                     :coding 'binary)))
      (set-process-coding-system proc 'binary 'binary)
      (set-process-filter proc
                          (lambda (_proc data)
                            (with-current-buffer buf
                              (goto-char (point-max))
                              (insert data))))
      (cons proc buf))))

(defun pg--negotiate-tls (conn)
  "Negotiate TLS on CONN.  Signals an error if server refuses."
  (let ((response (pg--send-ssl-request conn)))
    (pcase response
      (?S (pg--upgrade-to-tls conn))
      (?N (signal 'pg-connection-error
                  (list "Server does not support SSL")))
      (_ (signal 'pg-connection-error
                 (list (format "Unexpected SSL response: %c" response)))))))

(cl-defun pg-connect (&key (host "127.0.0.1") (port 5432) user password database tls)
  "Connect to a PostgreSQL server and authenticate.
Returns a `pg-conn' struct on success.

HOST defaults to \"127.0.0.1\", PORT defaults to 5432.
USER, PASSWORD, and DATABASE are strings.
When TLS is non-nil, attempt TLS upgrade before authenticating."
  (unless user
    (signal 'pg-connection-error (list "No user specified")))
  (when (and tls (not (pg--tls-available-p)))
    (signal 'pg-connection-error (list "TLS requested but GnuTLS is not available")))
  (pcase-let ((`(,proc . ,buf) (pg--open-connection host port)))
    (let ((conn (make-pg-conn :process proc :buf buf
                              :host host :port port
                              :user user :database database)))
      (condition-case err
          (progn
            (when tls (pg--negotiate-tls conn))
            (pg--send-startup-message conn)
            (pg--handle-authentication conn password)
            (pg--read-startup-messages conn)
            conn)
        (error
         (when (process-live-p proc) (delete-process proc))
         (when (buffer-live-p buf) (kill-buffer buf))
         (signal (car err) (cdr err)))))))

(defun pg-disconnect (conn)
  "Disconnect from PostgreSQL server, sending Terminate.
CONN is a `pg-conn' returned by `pg-connect'."
  (when conn
    (condition-case nil
        (when (process-live-p (pg-conn-process conn))
          ;; Send Terminate message
          (pg--send-message conn ?X ""))
      (error nil))
    (when (process-live-p (pg-conn-process conn))
      (delete-process (pg-conn-process conn)))
    (when (buffer-live-p (pg-conn-buf conn))
      (kill-buffer (pg-conn-buf conn)))))

;;;; Type parsing

(defvar pg-type-parsers nil
  "Alist of (OID . PARSER-FN) for custom type parsing.
Each PARSER-FN takes a single string argument and returns the
converted Elisp value.  Entries here override built-in parsers.")

(defun pg--parse-date (value)
  "Parse a PostgreSQL date string VALUE to a plist."
  (let ((parts (split-string value "-")))
    (list :year (string-to-number (nth 0 parts))
          :month (string-to-number (nth 1 parts))
          :day (string-to-number (nth 2 parts)))))

(defun pg--parse-time (value)
  "Parse a PostgreSQL time string VALUE to a plist."
  (let* ((dot-pos (string-search "." value))
         (time-part (if dot-pos (substring value 0 dot-pos) value))
         (parts (split-string time-part ":")))
    (list :hours (string-to-number (nth 0 parts))
          :minutes (string-to-number (nth 1 parts))
          :seconds (string-to-number (nth 2 parts))
          :negative nil)))

(defun pg--parse-timestamp (value)
  "Parse a PostgreSQL timestamp/timestamptz string VALUE to a plist."
  (let* ((space-pos (string-search " " value))
         (date-part (substring value 0 space-pos))
         (rest (if space-pos (substring value (1+ space-pos)) "00:00:00"))
         (time-str (replace-regexp-in-string "[+-][0-9:]+\\'" "" rest))
         (dot-pos (string-search "." time-str))
         (time-base (if dot-pos (substring time-str 0 dot-pos) time-str))
         (date-parts (split-string date-part "-"))
         (time-parts (split-string time-base ":")))
    (list :year (string-to-number (nth 0 date-parts))
          :month (string-to-number (nth 1 date-parts))
          :day (string-to-number (nth 2 date-parts))
          :hours (string-to-number (nth 0 time-parts))
          :minutes (string-to-number (nth 1 time-parts))
          :seconds (string-to-number (nth 2 time-parts)))))

(defun pg--parse-value (value oid)
  "Parse string VALUE according to PostgreSQL column OID.
Returns the converted Elisp value."
  (if (null value)
      nil
    (if-let* ((custom (alist-get oid pg-type-parsers)))
        (funcall custom value)
      (pcase oid
        ((pred (= pg-oid-bool))
         (pcase value ("t" t) ("f" nil) (_ value)))
        ((or (pred (= pg-oid-int2))
             (pred (= pg-oid-int4))
             (pred (= pg-oid-int8))
             (pred (= pg-oid-float4))
             (pred (= pg-oid-float8))
             (pred (= pg-oid-numeric)))
         (string-to-number value))
        ((or (pred (= pg-oid-json))
             (pred (= pg-oid-jsonb)))
         (if (fboundp 'json-parse-string)
             (json-parse-string value)
           value))
        ((pred (= pg-oid-date))      (pg--parse-date value))
        ((pred (= pg-oid-time))      (pg--parse-time value))
        ((or (pred (= pg-oid-timestamp))
             (pred (= pg-oid-timestamptz)))
         (pg--parse-timestamp value))
        (_ value)))))

;;;; Query execution

(defun pg--parse-row-description (payload)
  "Parse a RowDescription payload.
Returns a list of column plists (:name STR :oid INT ...)."
  (let ((num-fields (pg--read-be-int16-from-string payload 0))
        (pos 2)
        (columns nil))
    (dotimes (_ num-fields)
      (pcase-let ((`(,name . ,new-pos)
                   (pg--read-nul-string-from-string payload pos)))
        (setq pos new-pos)
        (let ((table-oid (pg--read-be-int32-from-string payload pos))
              (col-attr (pg--read-be-int16-from-string payload (+ pos 4)))
              (type-oid (pg--read-be-int32-from-string payload (+ pos 6)))
              (type-size (pg--read-be-int16-from-string payload (+ pos 10)))
              (type-mod (pg--read-be-int32-from-string payload (+ pos 12)))
              (format-code (pg--read-be-int16-from-string payload (+ pos 16))))
          (push (list :name name
                      :table-oid table-oid
                      :col-attr col-attr
                      :type-oid type-oid
                      :type-size type-size
                      :type-mod type-mod
                      :format-code format-code)
                columns)
          (setq pos (+ pos 18)))))
    (nreverse columns)))

(defun pg--parse-data-row (payload columns)
  "Parse a DataRow PAYLOAD using COLUMNS metadata.
Returns a list of values."
  (let ((num-fields (pg--read-be-int16-from-string payload 0))
        (pos 2)
        (row nil))
    (dotimes (i num-fields)
      (let ((len (pg--read-be-int32-from-string payload pos)))
        (setq pos (+ pos 4))
        (if (= len -1)
            ;; NULL
            (push nil row)
          (let* ((raw (substring payload pos (+ pos len)))
                 (str (decode-coding-string raw 'utf-8))
                 (col (nth i columns))
                 (oid (plist-get col :type-oid)))
            (push (pg--parse-value str oid) row)
            (setq pos (+ pos len))))))
    (nreverse row)))

(defun pg--parse-command-complete (payload)
  "Parse a CommandComplete PAYLOAD.
Returns (TAG . AFFECTED-ROWS) where AFFECTED-ROWS may be nil."
  (pcase-let ((`(,tag . ,_)
               (pg--read-nul-string-from-string payload 0)))
    (let ((parts (split-string tag " ")))
      (cons tag
            (when (> (length parts) 1)
              (let ((last (car (last parts))))
                (when (string-match-p "\\`[0-9]+\\'" last)
                  (string-to-number last))))))))

(defun pg--drain-until-ready (conn)
  "Read and discard messages from CONN until ReadyForQuery."
  (cl-loop for (msg-type . _payload) = (pg--read-message conn)
           until (= msg-type ?Z)))

(defun pg--handle-query-error (conn payload)
  "Handle an ErrorResponse PAYLOAD during query, then drain to ReadyForQuery."
  (let ((fields (pg--parse-error-fields payload)))
    (pg--drain-until-ready conn)
    (signal 'pg-query-error (list (pg--error-fields-message fields)))))

(defun pg-query (conn sql)
  "Execute SQL query on CONN using the simple query protocol.
Returns a `pg-result'.
Signals `pg-error' if the connection is busy (re-entrant call)."
  (when (pg-conn-busy conn)
    (signal 'pg-error
            (list "Connection busy — cannot send query while another is in progress")))
  ;; Flush any stale data left from previously interrupted queries.
  (with-current-buffer (pg-conn-buf conn)
    (erase-buffer))
  (setf (pg-conn-busy conn) t)
  (unwind-protect
      ;; Bind throw-on-input to nil so that `while-no-input' (used by
      ;; completion frameworks like corfu/company) cannot abort us
      ;; mid-response, which would leave partial data in the buffer and
      ;; corrupt subsequent queries.
      (let ((throw-on-input nil))
        (pg--send-message conn ?Q (pg--encode-string sql))
        (let ((columns nil) (rows nil) (affected-rows nil) (status nil) (done nil))
          (while (not done)
            (pcase-let ((`(,msg-type . ,payload) (pg--read-message conn)))
              (pcase msg-type
                (?T (setq columns (pg--parse-row-description payload)))
                (?D (push (pg--parse-data-row payload columns) rows))
                (?C (pcase-let ((`(,tag . ,n) (pg--parse-command-complete payload)))
                      (setq status tag affected-rows n)))
                (?Z (setq done t))
                (?E (pg--handle-query-error conn payload))
                (_ nil))))
          (make-pg-result
           :connection conn
           :status (or status "OK")
           :columns columns
           :rows (nreverse rows)
           :affected-rows affected-rows)))
    (setf (pg-conn-busy conn) nil)))

;;;; Ping

(defun pg-ping (conn)
  "Send a simple query to test if CONN is alive.
Returns t if the server is alive, or signals an error."
  (pg-query conn "SELECT 1")
  t)

;;;; Utilities

(defun pg-escape-identifier (name)
  "Escape NAME for use as a PostgreSQL identifier.
Wraps in double quotes and doubles any embedded double quotes."
  (concat "\"" (replace-regexp-in-string "\"" "\"\"" name) "\""))

(defun pg-escape-literal (value)
  "Escape VALUE for use as a PostgreSQL string literal.
Wraps in single quotes and doubles any embedded single quotes."
  (concat "'" (replace-regexp-in-string "'" "''" value) "'"))

;;;; Convenience macros

(defmacro with-pg-connection (var connect-args &rest body)
  "Execute BODY with VAR bound to a PostgreSQL connection.
CONNECT-ARGS is a plist passed to `pg-connect'.
The connection is automatically closed when BODY exits."
  (declare (indent 2))
  `(let ((,var (pg-connect ,@connect-args)))
     (unwind-protect
         (progn ,@body)
       (pg-disconnect ,var))))

(defmacro with-pg-transaction (conn &rest body)
  "Execute BODY inside a SQL transaction on CONN.
Issues BEGIN before BODY.  If BODY completes normally, issues COMMIT.
If BODY signals an error, issues ROLLBACK before re-raising."
  (declare (indent 1))
  (let ((c (make-symbol "conn")))
    `(let ((,c ,conn))
       (pg-query ,c "BEGIN")
       (condition-case err
           (prog1 (progn ,@body)
             (pg-query ,c "COMMIT"))
         (error
          (pg-query ,c "ROLLBACK")
          (signal (car err) (cdr err)))))))

(provide 'pg)
;;; pg.el ends here
