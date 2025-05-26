;;; ob-athena-submit-query-test.el --- Test for ob-athena-submit-query -*- lexical-binding: t; -*-

(require 'ert)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-submit-query-generates-correct-cli-command ()
  "Test that ob-athena-submit-query generates the correct AWS CLI call."
  (let* ((mock-output "{\"QueryExecutionId\": \"1234-mock-query-id\"}")
         (called-command nil)
         (ctx '((database . "test_db")
                (query-string . "SELECT * FROM my_table")
                (s3-output-location . "s3://my-bucket/")
                (workgroup . "primary"))))
    (cl-letf (((symbol-function 'shell-command-to-string)
               (lambda (cmd)
                 (setq called-command cmd)
                 mock-output)))
      (let ((result (ob-athena-submit-query ctx)))
        (should (string= result "1234-mock-query-id"))
        (should (string-match-p "aws athena start-query-execution" called-command))
        (should (string-match-p "--query-string 'SELECT \\* FROM my_table'" called-command))
        (should (string-match-p "--query-execution-context Database=test_db" called-command))
        (should (string-match-p "--result-configuration OutputLocation=s3://my-bucket/" called-command))
        (should (string-match-p "--work-group primary" called-command))))))
