;;; ob-mysql.el --- Org-Babel MySQL compatibility shim -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;;; Commentary:

;; Compatibility entry for Org-Babel mysql language.
;; Loads `ob-clutch', which defines `org-babel-execute:mysql'.

;;; Code:

(require 'ob-clutch)

(provide 'ob-mysql)
;;; ob-mysql.el ends here
