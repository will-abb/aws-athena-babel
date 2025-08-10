;;; ob-athena-output-conversion-unit-tests.el --- Tests for ob-athena query output handling -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

;; Minimal stub to prevent `void-function json-mode` in batch runs without it loaded
(unless (fboundp 'json-mode)
  (defun json-mode ()
    "Stub for json-mode in test batch runs."
    (fundamental-mode)))

(defvar auto-save-list-file-prefix nil)
(setq org-confirm-babel-evaluate nil)

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
  (let ((table (ob-athena--format-csv-table test-csv-path))
        (expected
         "| id          | element | datavalue |
| US1TXGV0021 | PRCP    | 0         |
| US1TXGV0021 | SNOW    | 0         |
| US1KSSG0036 | PRCP    | 0         |
| GME00126430 | TMAX    | -22       |
| GME00126430 | TMIN    | -124      |
| GME00126430 | PRCP    | 0         |
| GME00126430 | SNWD    | 30        |
| ASN00041495 | PRCP    | 70        |
| ASN00099002 | PRCP    | 0         |
| US1KSMG0005 | PRCP    | 10        |"))
    (should (stringp table))
    (should (equal table expected))))

(ert-deftest ob-athena--insert-console-style-results-inserts-content ()
  "Ensure ob-athena--insert-console-style-results inserts an Org table."
  (with-temp-buffer
    (ob-athena--insert-console-style-results (current-buffer) test-csv-path)
    (let ((actual (string-trim-right (substring-no-properties (buffer-string))))
          (expected (string-trim-right "\n\n--- Athena Console-style Results ---\n\n\
| id          | element | datavalue |\n\
| US1TXGV0021 | PRCP    | 0         |\n\
| US1TXGV0021 | SNOW    | 0         |\n\
| US1KSSG0036 | PRCP    | 0         |\n\
| GME00126430 | TMAX    | -22       |\n\
| GME00126430 | TMIN    | -124      |\n\
| GME00126430 | PRCP    | 0         |\n\
| GME00126430 | SNWD    | 30        |\n\
| ASN00041495 | PRCP    | 70        |\n\
| ASN00099002 | PRCP    | 0         |\n\
| US1KSMG0005 | PRCP    | 10        |\n")))
      (should (equal actual expected)))))

(ert-deftest ob-athena-show-csv-results-works ()
  "Simulate CSV result display in user buffer and check for full expected content."
  (let ((ob-athena-csv-output-dir "fixtures"))
    (copy-file
     (expand-file-name (concat test-query-id "-csv-resultsfile.csv") "fixtures")
     (expand-file-name (concat test-query-id ".csv") "fixtures") t)
    (with-current-buffer (get-buffer-create "*Athena Monitor*")
      (setq-local ob-athena-query-id test-query-id)
      (ob-athena-show-csv-results)
      (with-current-buffer "*Athena Raw Results*"
        (goto-char (point-min))
        (let ((expected (string-trim-right
                         "\"id\",\"element\",\"datavalue\"
\"US1TXGV0021\",\"PRCP\",\"0\"
\"US1TXGV0021\",\"SNOW\",\"0\"
\"US1KSSG0036\",\"PRCP\",\"0\"
\"GME00126430\",\"TMAX\",\"-22\"
\"GME00126430\",\"TMIN\",\"-124\"
\"GME00126430\",\"PRCP\",\"0\"
\"GME00126430\",\"SNWD\",\"30\"
\"ASN00041495\",\"PRCP\",\"70\"
\"ASN00099002\",\"PRCP\",\"0\"
\"US1KSMG0005\",\"PRCP\",\"10\""))
              (actual (string-trim-right
                       (substring-no-properties (buffer-string)))))
          (should (equal actual expected)))))))

(ert-deftest ob-athena-show-json-results-generates-buffer ()
  "Ensure JSON output buffer contains expected content from known output file."
  (let ((ob-athena-csv-output-dir "fixtures"))
    (when (executable-find "mlr")
      (copy-file
       (expand-file-name (concat test-query-id "-json-output.json") "fixtures")
       (expand-file-name (concat test-query-id ".json") "fixtures") t)
      (with-current-buffer (get-buffer-create "*Athena Monitor*")
        (setq-local ob-athena-query-id test-query-id)
        (ob-athena-show-json-results)
        (with-current-buffer "*Athena JSON Results*"
          (goto-char (point-min))
          (let ((expected (string-trim-right
                           (with-temp-buffer
                             (insert-file-contents
                              (expand-file-name
                               (concat test-query-id "-json-output.json")
                               "fixtures"))
                             (buffer-string))))
                (actual (string-trim-right
                         (substring-no-properties (buffer-string)))))
            (should (equal actual expected))))))))

(ert-deftest ob-athena--render-org-table-aligns-columns ()
  "Ensure rendered Org table has padded cells per column width."
  (let* ((rows '(("a" "longtext") ("bb" "short")))
         (widths (ob-athena--calculate-column-widths rows))
         (table (ob-athena--render-org-table rows widths)))
    (should (string-match-p "| a  | longtext |" table))
    (should (string-match-p "| bb | short    |" table))))

(provide 'ob-athena-output-conversion-unit-tests)
;;; ob-athena-output-conversion-unit-tests.el ends here
