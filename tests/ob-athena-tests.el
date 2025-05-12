;;; ob-athena-tests.el --- Description -*- lexical-binding: t; -*-
(require 'ert)
(require 'org)
(require 'ob-athena)

(ert-deftest ob-athena--basic-cloudtrail-test ()
  "Execute an Athena query block and ensure CSV output is created."
  (let* ((org-src-block
          "#+begin_src athena :var test-var=\"element\" :aws-profile \"personal-athena-admin-005343251202\" :database \"blogdb\" :s3-output-location \"s3://athena-query-results-005343251202/\" :workgroup \"primary\" :poll-interval 3 :fullscreen t :result-reuse-enabled t :result-reuse-max-age 10080 :console-region \"us-east-1\"\nSELECT '${test-var}' AS value;\n#+end_src")
         (buffer (get-buffer-create "*ob-athena-test*")))
    (with-current-buffer buffer
      (erase-buffer)
      (org-mode)
      (insert org-src-block)
      (goto-char (point-min))
      (re-search-forward "#\\+begin_src")
      (let ((result (org-babel-execute-src-block)))
        ;; Should return a list of links
        (should (listp result))
        (let ((csv-link (nth 2 result)))
          ;; Link should look like file:/tmp/XXXX.csv
          (should (string-match "^file:.+\\.csv$" csv-link))
          (should (file-exists-p (substring csv-link 5))))))))


(provide 'ob-athena-tests)
;;; ob-athena-tests.el ends here
