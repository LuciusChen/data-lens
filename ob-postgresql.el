;;; ob-postgresql.el --- Org-Babel postgresql entry via ob-clutch -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;;; Commentary:

;; Compatibility entry for Org-Babel postgresql language.
;; Loads `ob-clutch', which defines `org-babel-execute:postgresql'.

;;; Code:

(require 'ob-clutch)

(provide 'ob-postgresql)
;;; ob-postgresql.el ends here
