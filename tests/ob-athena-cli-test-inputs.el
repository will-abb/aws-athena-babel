;;; ob-athena-query-start-tests.el --- Tests for query submission logic -*- lexical-binding: t; -*-

(require 'ert)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defmacro with-shell-output (output &rest body)
  "Mock `shell-command-to-string` to return OUTPUT during BODY."
  `(cl-letf (((symbol-function 'shell-command-to-string) (lambda (&rest _) ,output)))
     ,@body))

(ert-deftest ob-athena--start-query-execution-valid-id ()
  "Return valid QueryExecutionId from well-formed CLI output."
  (with-shell-output "abc123-def456"
                     (let ((ctx '((output-location . "s3://test/")
                                  (workgroup . "test-wg")
                                  (database . "test-db")
                                  (profile . "test-profile"))))
                       (should (equal (ob-athena--start-query-execution ctx) "abc123-def456")))))

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
