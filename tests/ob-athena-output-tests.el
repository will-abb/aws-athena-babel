;;; ob-athena-output-tests.el --- Tests for ob-athena output processing -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

;; TODO: Add output-related tests here

(provide 'ob-athena-output-tests)
;;; ob-athena-output-tests.el ends here
