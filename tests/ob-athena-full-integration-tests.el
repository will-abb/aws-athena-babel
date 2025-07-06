;;; ob-athena-full-integration-tests.el --- Full integration tests for ob-athena -*- lexical-binding: t; -*-
;;; run each function in its separate test, don't run entire file as each query can only run in its own
;;; separate emacs environment

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)
(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--extract-csv-path (result)
  "Extract local file path from Org link RESULT."
  (when (stringp result)
    (if (string-match "\\[\\[file:\\(.*?\\)\\]\\[" result)
        (match-string 1 result)
      result)))

(ert-deftest ob-athena-user-profiles-return-all-records ()
  "Ensure all records from test_user_profiles are returned."
  (let ((org-src
         "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"s3://athen-query-test-queries-005343251202/test-data/all-records-test/\" :csv-output-dir \"/tmp/all-records-test\" :query-file \"/tmp/all-records-test.sql\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\"
SELECT * FROM test_user_profiles;
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (let* ((result (org-babel-execute-src-block))
             (csv-path (ob-athena--extract-csv-path (car (last result)))))
        (should
         (and
          (file-exists-p csv-path)
          (with-temp-buffer
            (insert-file-contents csv-path)
            (let ((content (buffer-string))
                  (lines (split-string (buffer-string) "\n" t)))
              (message "Line count: %d" (length lines))
              (message "Character count: %d" (length content))
              (and (= (length lines) 21)
                   (= (length content) 2138))))))))))

(ert-deftest ob-athena-user-profiles-filter-by-name-score-signup ()
  "Query test_user_profiles using :var name, score, and signup_date, filtering by inequality. Validate output content and character length."
  (let ((org-src
         "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"s3://athen-query-test-queries-005343251202/test-data/filters-test/\" :workgroup \"primary\" :poll-interval 3 :fullscreen t :result-reuse-enabled nil :result-reuse-max-age 0 :console-region \"us-east-1\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"
SELECT id, name, score, signup_date FROM test_user_profiles
WHERE name != '${name}'
AND score >= ${score}
AND signup_date != '${signup_date}';
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (let* ((result (org-babel-execute-src-block))
             (csv-path (ob-athena--extract-csv-path (car (last result))))
             (expected-csv "\"id\",\"name\",\"score\",\"signup_date\"\n\
\"3\",\"Charlie\",\"95.9\",\"2024-01-03\"\n\
\"8\",\"Hank\",\"91.4\",\"2024-01-08\"\n\
\"16\",\"Paul\",\"93.4\",\"2024-01-16\"\n\
\"18\",\"Ruby\",\"92.0\",\"2024-01-18\"\n"))
        (should
         (and
          (file-exists-p csv-path)
          (with-temp-buffer
            (insert-file-contents csv-path)
            (let ((actual (buffer-string)))
              (and
               (equal actual expected-csv)
               (= (length actual) 163))))))))))

(ert-deftest ob-athena-user-profiles-simple-select-all ()
  "Run a simple SELECT * query on test_user_profiles without any variables or interpolation."
  (let ((org-src
         "#+begin_src athena :s3-output-location \"s3://athena-query-results-005343251202/\"
SELECT * FROM test_user_profiles;
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (let* ((result (org-babel-execute-src-block))
             (csv-path (ob-athena--extract-csv-path (car (last result)))))
        (should
         (and
          (file-exists-p csv-path)
          (with-temp-buffer
            (insert-file-contents csv-path)
            (let ((lines (split-string (buffer-string) "\n" t)))
              (message "Simple SELECT line count: %d" (length lines))
              (> (length lines) 1)))))))))

;;; ob-athena-full-integration-tests.el ends here
