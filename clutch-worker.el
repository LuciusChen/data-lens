;;; clutch-worker.el --- Background metadata worker -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

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

;; Internal background worker used for passive metadata refresh tasks.

;;; Code:

(require 'cl-lib)

(cl-defstruct clutch--worker
  "Background worker state for one logical connection."
  mutex
  condvar
  queue
  closing-p
  thread)

(defvar clutch--metadata-workers (make-hash-table :test 'eq)
  "Background metadata workers keyed by live connection object.")

(defun clutch--worker-supported-p ()
  "Return non-nil when Emacs supports the worker primitives clutch needs."
  (and (fboundp 'make-thread)
       (fboundp 'make-mutex)
       (fboundp 'make-condition-variable)
       (fboundp 'condition-wait)
       (fboundp 'condition-notify)))

(defun clutch--worker-loop (worker)
  "Run WORKER's task loop until shutdown."
  (catch 'done
    (while t
      (let (job)
        (let ((mutex (clutch--worker-mutex worker)))
          (mutex-lock mutex)
          (unwind-protect
              (progn
                (while (and (null (clutch--worker-queue worker))
                            (not (clutch--worker-closing-p worker)))
                  (condition-wait (clutch--worker-condvar worker)))
                (cond
                 ((clutch--worker-queue worker)
                  (setq job (car (clutch--worker-queue worker)))
                  (setf (clutch--worker-queue worker)
                        (cdr (clutch--worker-queue worker))))
                 ((clutch--worker-closing-p worker)
                  (throw 'done nil))))
            (mutex-unlock mutex)))
        (when job
          (funcall job))))))

(defun clutch--worker-create ()
  "Create and start a background metadata worker."
  (let* ((mutex (make-mutex "clutch-metadata-worker"))
         (worker (make-clutch--worker
                  :mutex mutex
                  :condvar (make-condition-variable mutex)
                  :queue nil
                  :closing-p nil)))
    (setf (clutch--worker-thread worker)
          (make-thread (lambda () (clutch--worker-loop worker))
                       "clutch-metadata-worker"))
    worker))

(defun clutch--worker-get (conn)
  "Return the live worker for CONN, creating one when needed."
  (let ((worker (gethash conn clutch--metadata-workers)))
    (if (and worker
             (thread-live-p (clutch--worker-thread worker)))
        worker
      (setq worker (clutch--worker-create))
      (puthash conn worker clutch--metadata-workers)
      worker)))

(defun clutch--worker-submit (conn work-fn callback &optional errback)
  "Run WORK-FN for CONN on a background worker.
CALLBACK receives the result on the main thread.  ERRBACK receives an
error-message string on the main thread.  Return non-nil when the task
was queued."
  (when (clutch--worker-supported-p)
    (let* ((worker (clutch--worker-get conn))
           (job
            (lambda ()
              (condition-case err
                  (let ((result (funcall work-fn)))
                    (when callback
                      (run-at-time 0 nil callback result)))
                (error
                 (when errback
                   (run-at-time 0 nil errback (error-message-string err))))))))
      (let ((mutex (clutch--worker-mutex worker)))
        (mutex-lock mutex)
        (unwind-protect
            (progn
              (setf (clutch--worker-queue worker)
                    (nconc (clutch--worker-queue worker) (list job)))
              (condition-notify (clutch--worker-condvar worker)))
          (mutex-unlock mutex)))
      t)))

(defun clutch--worker-shutdown (conn)
  "Stop and forget any background worker associated with CONN."
  (when-let* ((worker (gethash conn clutch--metadata-workers)))
    (remhash conn clutch--metadata-workers)
    (let ((mutex (clutch--worker-mutex worker)))
      (mutex-lock mutex)
      (unwind-protect
          (progn
            (setf (clutch--worker-closing-p worker) t
                  (clutch--worker-queue worker) nil)
            (condition-notify (clutch--worker-condvar worker)))
        (mutex-unlock mutex)))
    t))

(provide 'clutch-worker)
;;; clutch-worker.el ends here
