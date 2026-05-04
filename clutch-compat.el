;;; clutch-compat.el --- Compatibility helpers for clutch -*- lexical-binding: t; -*-

;; Author: Lucius Chen <chenyh572@gmail.com>
;; Maintainer: Lucius Chen <chenyh572@gmail.com>
;; Version: 0.1.0
;; Package-Requires: ((emacs "28.1"))
;; Keywords: data, tools
;; URL: https://github.com/LuciusChen/clutch

;;; Commentary:

;; Small compatibility shims for Emacs versions supported by clutch.

;;; Code:

(require 'cl-lib)

(unless (fboundp 'prop-match-beginning)
  (cl-defstruct (prop-match
                 (:constructor clutch--prop-match-create
                               (beginning end value)))
    beginning
    end
    value))

(defun clutch--text-property-search-match-p (value current predicate)
  "Return non-nil when CURRENT matches VALUE according to PREDICATE."
  (cond
   ((functionp predicate) (funcall predicate value current))
   (predicate (equal value current))
   (t (not (equal value current)))))

(defun clutch--text-property-search-run-beginning (position property)
  "Return beginning of PROPERTY run containing POSITION."
  (or (previous-single-property-change (1+ position) property nil (point-min))
      (point-min)))

(unless (fboundp 'text-property-search-forward)
  (defun text-property-search-forward (property &optional value predicate not-current)
    "Search forward for the next PROPERTY match.
This compatibility implementation is used on Emacs versions that do
not provide `text-property-search-forward'."
    (let ((origin (point))
          (position (point))
          (limit (point-max))
          match)
      (while (and (not match) (< position limit))
        (let* ((current (get-text-property position property))
               (run-beginning (clutch--text-property-search-run-beginning
                               position property))
               (end (or (next-single-property-change position property nil limit)
                        limit))
               (beginning (max run-beginning origin)))
          (if (and (clutch--text-property-search-match-p value current predicate)
                   (not (and not-current
                             (<= run-beginning origin)
                             (< origin end))))
              (setq match (clutch--prop-match-create beginning end current))
            (setq position (max (1+ position) end)))))
      (when match
        (goto-char (prop-match-end match))
        match))))

(unless (fboundp 'text-property-search-backward)
  (defun text-property-search-backward (property &optional value predicate not-current)
    "Search backward for the previous PROPERTY match.
This compatibility implementation is used on Emacs versions that do
not provide `text-property-search-backward'."
    (let ((origin (point))
          (position (1- (point)))
          (limit (point-min))
          match)
      (while (and (not match) (>= position limit))
        (let* ((current (get-text-property position property))
               (beginning (clutch--text-property-search-run-beginning
                           position property))
               (run-end (or (next-single-property-change position property nil
                                                         (point-max))
                            (point-max)))
               (end (min origin run-end)))
          (if (and (clutch--text-property-search-match-p value current predicate)
                   (not (and not-current
                             (<= beginning origin)
                             (< origin run-end))))
              (setq match (clutch--prop-match-create beginning end current))
            (setq position (1- beginning)))))
      (when match
        (goto-char (prop-match-beginning match))
        match))))

(provide 'clutch-compat)

;;; clutch-compat.el ends here
