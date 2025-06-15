;;; ob-athena-full-integration-tests.el --- Full integration tests for ob-athena -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--run-query (sql)
  "Run raw Athena SQL query wrapped in an Org Babel block for testing."
  (let ((org-src-lang-modes '(("athena" . sql)))
        (org-babel-load-languages '((athena . t))))
    (with-temp-buffer
      (insert (format "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"s3://athen-query-test-queries-005343251202/test-data/\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"\n%s\n#+end_src" sql))
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

(ert-deftest ob-athena-user-profiles-return-all-records ()
  "Ensure all records from test_user_profiles are returned in CSV and match expected character count."
  (let* ((sql "SELECT * FROM test_user_profiles;")
         (result (ob-athena--run-query sql))
         (csv-path (ob-athena--extract-csv-path (car (last result)))))
    (should (and (file-exists-p csv-path)
                 (with-temp-buffer
                   (insert-file-contents csv-path)
                   (let ((content (buffer-string))
                         (lines (split-string (buffer-string) "\n" t)))
                     (message "Line count: %d" (length lines))
                     (message "Character count: %d" (length content))
                     (and (= (length lines) 21)
                          (> (length content) 1700))))))))


;;; ob-athena-full-integration-tests.el ends here
