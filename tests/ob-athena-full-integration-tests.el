;;; ob-athena-full-integration-tests.el --- Description -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--run-user-profiles-query ()
  "Run a test Athena query against the user profiles table."
  (let ((org-src-lang-modes '(("athena" . sql)))
        (org-babel-load-languages '((athena . t))))
    (with-temp-buffer
      (insert
       "#+begin_src athena :aws-profile \"williseed-athena\" :database \"blogdb\" :s3-output-location \"s3://athen-query-test-queries-005343251202/test-data/\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\"\nSELECT id, name, email, country FROM test_user_profiles WHERE is_active = true LIMIT 5;\n#+end_src")
      (goto-char (point-min))
      (org-babel-execute-src-block))))

(defun ob-athena--extract-query-id (result)
  "Extract query ID from RESULT if it contains a CSV S3 path."
  (when (and (listp result)
             (string-match "/\\([a-f0-9-]+\\)\\.csv" (car (last result))))
    (match-string 1 (car (last result)))))

(defun ob-athena--extract-csv-path (result)
  "Extract local file path from Org link RESULT."
  (when (stringp result)
    (if (string-match "\\[\\[file:\\(.*?\\)\\]\\[" result)
        (match-string 1 result)
      result)))

(defun ob-athena--csv-has-header-and-data-p (csv-path)
  "Return non-nil if CSV at CSV-PATH has expected header and at least one data row."
  (when (file-exists-p csv-path)
    (let* ((lines (with-temp-buffer
                    (insert-file-contents csv-path)
                    (split-string (buffer-string) "\n" t))))
      (message "CSV content:\n%s" (mapconcat #'identity lines "\n"))
      (and (string-match-p "id.*name.*email.*country" (car lines))
           (> (length lines) 1)))))

(ert-deftest ob-athena-user-profiles-query-returns-valid-id ()
  "Ensure a valid query ID is returned from user profiles test table."
  (let* ((result (ob-athena--run-user-profiles-query))
         (query-id (ob-athena--extract-query-id result)))
    (should (and query-id (string-match-p "^[a-f0-9-]+$" query-id)))))

(ert-deftest ob-athena-user-profiles-csv-valid ()
  "Verify CSV output from test_user_profiles contains expected headers and data."
  (let* ((result (ob-athena--run-user-profiles-query))
         (csv-path (ob-athena--extract-csv-path (car (last result)))))
    (should (ob-athena--csv-has-header-and-data-p csv-path))))

(provide 'ob-athena-full-integration-tests)
;;; ob-athena-full-integration-tests.el ends here
