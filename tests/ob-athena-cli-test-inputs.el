;;; ob-athena-query-start-tests.el --- Tests for query submission logic -*- lexical-binding: t; -*-

(require 'ert)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defmacro with-shell-output (output &rest body)
  "Temporarily override `shell-command-to-string` to return OUTPUT during BODY."
  `(let ((real-shell (symbol-function 'shell-command-to-string)))
     (unwind-protect
         (progn
           (fset 'shell-command-to-string (lambda (&rest _) ,output))
           ,@body)
       (fset 'shell-command-to-string real-shell))))

(ert-deftest ob-athena--start-query-execution-empty-output ()
  "Raise error on empty CLI output."
  (with-shell-output ""
                     (should-error (ob-athena--start-query-execution '()) :type 'error)))

(ert-deftest ob-athena--start-query-execution-credentials-error ()
  "Raise error when CLI reports credential issue."
  (with-shell-output "Unable to locate credentials"
                     (should-error (ob-athena--start-query-execution '()) :type 'error)))

(ert-deftest ob-athena--start-query-execution-invalid-output ()
  "Raise error on malformed CLI output."
  (with-shell-output "Error: invalid"
                     (should-error (ob-athena--start-query-execution '()) :type 'error)))

(provide 'ob-athena-query-start-tests)
;;; ob-athena-query-start-tests.el ends here
