;;; ob-postgresql.el --- Org-Babel PostgreSQL compatibility shim -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;;; Commentary:

;; Compatibility entry for Org-Babel postgresql language.
;; Loads `ob-clutch', which defines `org-babel-execute:postgresql'.

;;; Code:

(require 'ob-clutch)

(provide 'ob-postgresql)
;;; ob-postgresql.el ends here
