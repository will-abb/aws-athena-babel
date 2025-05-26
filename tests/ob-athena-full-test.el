;;; ob-athena-full-integrations.el --- Integration tests for ob-athena -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(defun ob-athena--full-test-buffer ()
  "Return an Org buffer with an Athena source block for integration testing."
  (let ((buf (generate-new-buffer "*ob-athena-full-test*")))
    (with-current-buffer buf
      (insert
       "#+begin_src athena :aws-profile \"personal-athena-admin-005343251202\""
       " :database \"blogdb\""
       " :s3-output-location \"s3://athena-query-results-005343251202/\""
       " :workgroup \"primary\""
       " :poll-interval 3"
       " :fullscreen t"
       " :result-reuse-enabled t"
       " :result-reuse-max-age 10080"
       " :console-region \"us-east-1\""
       " :var select_clause=\"SELECT id, element, datavalue\""
       " :var table=\"original_csv\""
       " :var limit=10\n"
       "${select_clause}\n"
       "FROM ${table}\n"
       "LIMIT ${limit};\n"
       "#+end_src\n")
      (org-mode))
    buf))

(ert-deftest ob-athena--full-execution-test ()
  "Run a full end-to-end test of an Athena query block."
  (skip-unless (executable-find "aws"))
  (skip-unless (executable-find "mlr"))
  (let ((buf (ob-athena--full-test-buffer))
        result)
    (with-current-buffer buf
      (goto-char (point-min))
      (search-forward "#+begin_src")
      (setq result (org-babel-execute-src-block)))
    (should (and (listp result)
                 (seq-some (lambda (line) (string-match-p "Query submitted" line)) result)))
    (kill-buffer buf)))
