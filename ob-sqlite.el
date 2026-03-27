;;; ob-sqlite.el --- Org-Babel sqlite entry via ob-clutch -*- lexical-binding: t; -*-

;; Copyright (C) 2025-2026 Lucius Chen

;;; Commentary:

;; Compatibility entry for Org-Babel sqlite language.
;; Loads `ob-clutch', which defines `org-babel-execute:sqlite'.

;;; Code:

(require 'ob-clutch)

(provide 'ob-sqlite)
;;; ob-sqlite.el ends here
