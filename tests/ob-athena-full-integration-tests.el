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
         (s3-path (format "s3://athen-query-test-queries-005343251202/test-data/%d/" timestamp))
         (csv-dir (expand-file-name (number-to-string timestamp) temporary-file-directory))
         (org-src (format "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"%s\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\" :csv-output-dir \"%s\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"\nSELECT * FROM test_user_profiles;\n#+end_src"
                          s3-path csv-dir)))
    (make-directory csv-dir t)
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
                              (> (length content) 1700))))))))
    (ob-athena--cleanup-buffers)))

;; (ert-deftest ob-athena-user-profiles-filter-by-name-score-signup ()
;;   "Query test_user_profiles using :var name, score, and signup_date, filtering by inequality. Validate output content and character length."
;;   (let* ((timestamp (floor (float-time)))
;;          (csv-dir (expand-file-name (number-to-string timestamp) temporary-file-directory))
;;          (s3-path (format "s3://athen-query-test-queries-005343251202/test-data/%d/" timestamp))
;;          (query-file (expand-file-name (format "athena-query-%d.sql" timestamp) csv-dir))
;;          (org-src (format
;;                    "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"%s\" :csv-output-dir \"%s\" :query-file \"%s\" :workgroup \"primary\" :poll-interval 3 :fullscreen nil :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"\nSELECT id, name, score, signup_date FROM test_user_profiles\nWHERE name != '${name}'\nAND score >= ${score}\nAND signup_date != '${signup_date}';\n#+end_src"
;;                    s3-path csv-dir query-file)))
;;     (make-directory csv-dir t)
;;     (with-temp-buffer
;;       (insert org-src)
;;       (goto-char (point-min))
;;       (let* ((result (org-babel-execute-src-block))
;;              (csv-path (ob-athena--extract-csv-path (car (last result))))
;;              (expected-csv "\"id\",\"name\",\"score\",\"signup_date\"\n\
;; \"3\",\"Charlie\",\"95.9\",\"2024-01-03\"\n\
;; \"8\",\"Hank\",\"91.4\",\"2024-01-08\"\n\
;; \"16\",\"Paul\",\"93.4\",\"2024-01-16\"\n\
;; \"18\",\"Ruby\",\"92.0\",\"2024-01-18\"\n"))
;;         (should
;;          (and
;;           (file-exists-p csv-path)
;;           (with-temp-buffer
;;             (insert-file-contents csv-path)
;;             (let ((actual (buffer-string)))
;;               (and
;;                (equal actual expected-csv)
;;                (= (length actual) 163))))))))
;;     (ob-athena--cleanup-buffers)))



(defun ob-athena--cleanup-buffers ()
  "Kill common Athena-related result buffers if they exist."
  (dolist (buf-name '("*Athena Monitor*" "*Athena Raw Results*" "*Athena JSON Results*"))
    (when (get-buffer buf-name)
      (kill-buffer buf-name))))

;;; ob-athena-full-integration-tests.el ends here
