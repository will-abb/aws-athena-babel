;;; ob-athena-integration-tests.el --- Real integration tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-real-query-executes ()
  "Run a real Athena query via Org Babel."
  (let ((org-src-lang-modes '(("athena" . sql)))
        ;; Required to enable :var substitution
        (org-babel-load-languages '((athena . t)))
        ;; This buffer simulates an Org user
        (org-code-block
         "#+begin_src athena :aws-profile \"personal-athena-admin-005343251202\" :database \"blogdb\" :s3-output-location \"s3://athena-query-results-005343251202/\" :workgroup \"primary\" :poll-interval 3 :fullscreen t :result-reuse-enabled t :result-reuse-max-age 10080 :console-region \"us-east-1\" :var select_clause=\"SELECT id, element, datavalue\" :var table=\"original_csv\" :var limit=10
${select_clause}
FROM ${table}
LIMIT ${limit};
#+end_src"))

    (with-temp-buffer
      (insert org-code-block)
      (goto-char (point-min))
      (org-babel-execute-src-block)) ; actually runs the Athena query
    (should t))) ; If we didn’t crash, consider it passed for now

(provide 'ob-athena-integration-tests)
;;; ob-athena-integration-tests.el ends here
