;;; ob-athena-minimal-integration-tests.el --- Minimal integration tests -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--run-sample-query ()
  "Run a real Athena query and return the Org result."
  (let ((org-src-lang-modes '(("athena" . sql)))
        (org-babel-load-languages '((athena . t))))
    (with-temp-buffer
      (insert "#+begin_src athena :aws-profile \"williseed-athena\" :database \"blogdb\" :s3-output-location \"s3://athena-query-results-005343251202/\" :workgroup \"primary\" :poll-interval 3 :fullscreen t :result-reuse-enabled nil :result-reuse-max-age 10080 :console-region \"us-east-1\" :var select_clause=\"SELECT id, element, datavalue\" :var table=\"original_csv\" :var limit=10\n${select_clause}\nFROM ${table}\nLIMIT ${limit};\n#+end_src")
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
      (and (string= (car lines) "\"id\",\"element\",\"datavalue\"")
           (> (length lines) 1)))))

(ert-deftest ob-athena-query-returns-valid-id ()
  "Ensure a valid query ID is returned."
  (let* ((result (ob-athena--run-sample-query))
         (query-id (ob-athena--extract-query-id result)))
    (should (and query-id (string-match-p "^[a-f0-9-]+$" query-id)))))

(ert-deftest ob-athena-csv-has-correct-header-and-rows ()
  "Verify the downloaded Athena CSV has correct header and at least one data row."
  (let* ((result (ob-athena--run-sample-query))
         (csv-path (ob-athena--extract-csv-path (car (last result)))))
    (should (ob-athena--csv-has-header-and-data-p csv-path))))
