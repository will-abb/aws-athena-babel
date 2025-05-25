;;; ob-athena-output-tests.el --- Tests for ob-athena query output handling -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--load-sample-output (relative-path)
  "Load sample query output from a file as a string."
  (let* ((base default-directory)
         (path (expand-file-name relative-path base)))
    (with-temp-buffer
      (insert-file-contents path)
      (buffer-string))))

(defconst test-query-id "0f7bf33b-3eb1-4e6c-a0c3-d5316894b062")
(defconst test-csv-path (expand-file-name
                         (concat test-query-id "-csv-resultsfile.csv")
                         "fixtures"))
(defconst test-json-path (expand-file-name
                          (concat test-query-id ".json")
                          temporary-file-directory))

(ert-deftest ob-athena--format-csv-table-produces-org ()
  "Test that ob-athena--format-csv-table returns a valid Org-mode table string."
  (let ((table (ob-athena--format-csv-table test-csv-path)))
    (should (stringp table))
    (should (string-match-p "^| id" table))
    (should (string-match-p "| US1TXGV0021" table))))

(ert-deftest ob-athena--insert-console-style-results-inserts-content ()
  "Ensure ob-athena--insert-console-style-results inserts an Org table."
  (with-temp-buffer
    (ob-athena--insert-console-style-results (current-buffer) test-csv-path)
    (goto-char (point-min))
    (should (search-forward "--- Athena Console-style Results ---" nil t))
    (should (search-forward "| ASN00041495" nil t))))

(ert-deftest ob-athena-show-csv-results-works ()
  "Simulate CSV result display in user buffer."
  (let ((ob-athena-csv-output-dir "fixtures"))
    (with-current-buffer (get-buffer-create "*Athena Monitor*")
      (setq-local ob-athena-query-id test-query-id)
      (ob-athena-show-csv-results)
      (with-current-buffer "*Athena Raw Results*"
        (goto-char (point-min))
        (should (search-forward "US1TXGV0021" nil t))))))

(ert-deftest ob-athena-show-json-results-generates-buffer ()
  "Test JSON conversion from CSV using mlr."
  (let ((ob-athena-csv-output-dir "fixtures"))
    (when (executable-find "mlr")
      (with-current-buffer (get-buffer-create "*Athena Monitor*")
        (setq-local ob-athena-query-id test-query-id)
        (ob-athena-show-json-results)
        (with-current-buffer "*Athena JSON Results*"
          (goto-char (point-min))
          (should (looking-at-p "\\["))
          (should (re-search-forward "\"element\"" nil t)))))))

(ert-deftest ob-athena--render-org-table-aligns-columns ()
  "Ensure rendered Org table has padded cells per column width."
  (let* ((rows '(("a" "longtext") ("bb" "short")))
         (widths (ob-athena--calculate-column-widths rows))
         (table (ob-athena--render-org-table rows widths)))
    (should (string-match-p "| a  | longtext |" table))
    (should (string-match-p "| bb | short    |" table))))

(provide 'ob-athena-output-tests)
;;; ob-athena-output-tests.el ends here
