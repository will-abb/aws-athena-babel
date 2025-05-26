;;; ob-athena-cli-parsing-tests.el --- Tests for ob-athena JSON parsing -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--load-sample-json (relative-path)
  "Load a JSON test fixture file from RELATIVE-PATH under tests/fixtures."
  (let* ((source-path (or load-file-name buffer-file-name default-directory))
         (base (if (stringp source-path)
                   (file-name-directory source-path)
                 default-directory)))
    (with-temp-buffer
      (insert-file-contents (expand-file-name relative-path base))
      (buffer-string))))

(ert-deftest ob-athena-parse-state ()
  "Test extraction of query execution state from real CLI JSON."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "State") "SUCCEEDED"))))

(ert-deftest ob-athena-parse-bytes-scanned ()
  "Test extraction of DataScannedInBytes field."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (= (ob-athena--extract-json-number json "DataScannedInBytes") 4746704))))

(ert-deftest ob-athena-parse-query-id ()
  "Test extraction of QueryExecutionId."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "QueryExecutionId")
                     "43977ec3-ba3e-4874-912a-73f426532ffb"))))

(ert-deftest ob-athena-parse-output-location ()
  "Test parsing of the S3 output location."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "OutputLocation")
                     "s3://athena-query-results-005343251202/43977ec3-ba3e-4874-912a-73f426532ffb.csv"))))

(ert-deftest ob-athena-calculate-cost-10mb ()
  "Ensure cost is correct within margin for 10MB query."
  (should (< (abs (- (ob-athena--calculate-query-cost 10485760)
                     4.76837158203125e-05))
             1e-10)))

(ert-deftest ob-athena-calculate-cost-1tb ()
  "Ensure cost is correctly calculated for 1TB of data."
  (should (= (ob-athena--calculate-query-cost 1099511627776) 5.0)))

(ert-deftest ob-athena-extract-json-field-missing-key ()
  "Return nil when the key is missing from the JSON string."
  (should (null (ob-athena--extract-json-field "{\"OtherKey\": \"value\"}" "State"))))

(ert-deftest ob-athena-extract-json-field-non-json ()
  "Gracefully return nil when input is not valid JSON."
  (should (null (ob-athena--extract-json-field "not a json" "State"))))

(ert-deftest ob-athena-extract-query-result-path ()
  "Test extraction of OutputLocation from Athena query JSON."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--query-result-path json)
                     "s3://athena-query-results-005343251202/43977ec3-ba3e-4874-912a-73f426532ffb.csv"))))

(ert-deftest ob-athena-build-timing-section-content ()
  "Ensure timing section is rendered with expected substrings."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json"))
        (output nil))
    (setq output (ob-athena--build-timing-section json))
    (should (stringp output))
    (should (string-match "Execution Time: 3\\.45 sec" output))
    (should (string-match "Total Time: 3\\.67 sec" output))
    (should (string-match "Queue Time: 0\\.11 sec" output))))

(ert-deftest ob-athena--calculate-column-widths-works ()
  (let* ((rows '(("a" "12345") ("bbb" "678")))
         (widths (ob-athena--calculate-column-widths rows)))
    (should (equal widths '(3 5)))))

(ert-deftest ob-athena--extract-json-field-valid-key ()
  (let ((json "{\"foo\": \"bar\"}"))
    (should (equal (ob-athena--extract-json-field json "foo") "bar"))))

(provide 'ob-athena-cli-parsing-tests)
;;; ob-athena-cli-parsing-tests.el ends here
