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
  (ob-athena--reset-test-environment)
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

(ert-deftest ob-athena-user-profiles-filter-by-name-score-signup ()
  "Query test_user_profiles using :var name, score, and signup_date, filtering by inequality. Validate output content and character length."
  (ob-athena--reset-test-environment)
  (let* ((sql "SELECT id, name, score, signup_date
               FROM test_user_profiles
               WHERE name != '${name}'
                 AND score >= ${score}
                 AND signup_date != '${signup_date}';")
         (result (ob-athena--run-query sql))
         (csv-path (ob-athena--extract-csv-path (car (last result))))
         (expected-csv "\"id\",\"name\",\"score\",\"signup_date\"\n\
\"3\",\"Charlie\",\"95.9\",\"2024-01-03\"\n\
\"8\",\"Hank\",\"91.4\",\"2024-01-08\"\n\
\"16\",\"Paul\",\"93.4\",\"2024-01-16\"\n\
\"18\",\"Ruby\",\"92.0\",\"2024-01-18\"\n"))
    (should (and (file-exists-p csv-path)
                 (with-temp-buffer
                   (insert-file-contents csv-path)
                   (let ((actual (buffer-string)))
                     (and
                      (equal actual expected-csv)
                      (= (length actual) 163))))))))
(defun ob-athena--reset-test-environment ()
  "Clean up test state before each test to avoid cross-test interference."
  ;; Kill Athena-related buffers
  (dolist (buf (buffer-list))
    (when (string-match-p "^\\*ob-athena.*\\*$" (buffer-name buf))
      (kill-buffer buf)))
  ;; Sleep briefly to let any S3 I/O settle (especially on CI or fast fs)
  (sleep-for 1)
  ;; Clear Org Babel context
  (setq-local ob-athena--context nil)
  ;; Ensure no residual temp output files interfere
  (let ((temp-dir "/tmp/user/1000/")) ; adjust if needed
    (when (file-exists-p temp-dir)
      (dolist (file (directory-files temp-dir t "\\.csv$"))
        (delete-file file))))
  ;; Could optionally clear result CSVs from your known output bucket
  ;; but that's only necessary if they persist and cause collisions
  )

;;; ob-athena-full-integration-tests.el ends here
