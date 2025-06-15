;;; ob-athena-full-integration-tests.el --- Full integration tests for ob-athena -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defvar ob-athena-csv-output-dir
  (expand-file-name (format "athena-test-%d" (floor (float-time)))
                    (temporary-file-directory))
  "Directory where Athena test CSV result files are saved.")

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
  (let* ((timestamp (floor (float-time)))
         (s3-path (format "s3://athen-query-test-queries-005343251202/test-data/%d/1/" timestamp))
         (org-src (format "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"%s\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"\nSELECT * FROM test_user_profiles;\n#+end_src" s3-path)))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (let* ((result (org-babel-execute-src-block))
             (csv-path (ob-athena--extract-csv-path (car (last result)))))
        (should (and (file-exists-p csv-path)
                     (with-temp-buffer
                       (insert-file-contents csv-path)
                       (let ((content (buffer-string))
                             (lines (split-string (buffer-string) "\n" t)))
                         (message "Line count: %d" (length lines))
                         (message "Character count: %d" (length content))
                         (and (= (length lines) 21)
                              (> (length content) 1700))))))))))

;;; ob-athena-full-integration-tests.el ends here
