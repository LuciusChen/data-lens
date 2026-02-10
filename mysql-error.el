;;; mysql-error.el --- Error types for mysql.el -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Free Software Foundation, Inc.

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

;; Error type hierarchy for the MySQL client.

;;; Code:

(define-error 'mysql-error "MySQL error")
(define-error 'mysql-connection-error "MySQL connection error" 'mysql-error)
(define-error 'mysql-protocol-error "MySQL protocol error" 'mysql-error)
(define-error 'mysql-auth-error "MySQL authentication error" 'mysql-error)
(define-error 'mysql-query-error "MySQL query error" 'mysql-error)
(define-error 'mysql-timeout "MySQL timeout" 'mysql-error)

(provide 'mysql-error)
;;; mysql-error.el ends here
