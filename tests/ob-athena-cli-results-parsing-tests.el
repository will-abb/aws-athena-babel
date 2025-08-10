;;; ob-athena-cli-results-parsing-tests.el --- Tests for ob-athena JSON parsing -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)
(setq org-confirm-babel-evaluate nil)

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
  (let ((json (ob-athena--load-sample-json
               "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "State") "SUCCEEDED")))
  (let ((json (ob-athena--load-sample-json
               "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json")))
    (should (string= (ob-athena--extract-json-field json "State") "FAILED"))))

(ert-deftest ob-athena-parse-bytes-scanned ()
  "Test extraction of DataScannedInBytes field."
  (let ((json (ob-athena--load-sample-json
               "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (= (ob-athena--extract-json-number json "DataScannedInBytes") 4746704)))
  (let ((json (ob-athena--load-sample-json
               "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json")))
    (should (= (ob-athena--extract-json-number json "DataScannedInBytes") 0))))

(ert-deftest ob-athena-parse-query-id ()
  "Test extraction of QueryExecutionId."
  (let ((json (ob-athena--load-sample-json
               "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "QueryExecutionId")
                     "43977ec3-ba3e-4874-912a-73f426532ffb")))
  (let ((json (ob-athena--load-sample-json
               "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json")))
    (should (string= (ob-athena--extract-json-field json "QueryExecutionId")
                     "4bf8a6ca-0880-4383-bc76-7a3baeb8b749"))))

(ert-deftest ob-athena-parse-output-location ()
  "Test parsing of the S3 output location."
  (let ((json (ob-athena--load-sample-json "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json")))
    (should (string= (ob-athena--extract-json-field json "OutputLocation")
                     "s3://athena-query-results-005343251202/43977ec3-ba3e-4874-912a-73f426532ffb.csv"))))

(ert-deftest ob-athena-calculate-cost-10mb ()
  "Ensure cost is correct within margin for 10MB query."
  (should (< (abs (- (ob-athena--calculate-query-cost 10485760)
                     4.76837158203125e-05))
             1e-9)))

(ert-deftest ob-athena-calculate-cost-1tb ()
  "Ensure cost is correctly calculated for 1TB of data."
  (should (= (ob-athena--calculate-query-cost 1099511627776) 5.0)))

(ert-deftest ob-athena-calculate-cost-from-real-files ()
  "Verify cost calculation from real Athena fixture JSON files."
  (let* ((json-success (ob-athena--load-sample-json
                        "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json"))
         (bytes-success (ob-athena--extract-json-number json-success "DataScannedInBytes"))
         (rounded-up-mb (ceiling (/ bytes-success 1048576.0)))
         (billable-mb (max rounded-up-mb 10))
         (expected-cost-success (/ (* billable-mb 1048576.0 5.0) 1099511627776.0))
         (actual-cost-success (ob-athena--calculate-query-cost bytes-success)))
    (should (floatp actual-cost-success))
    (should (= bytes-success 4746704))
    (should (< (abs (- actual-cost-success expected-cost-success)) 1e-9)))

  (let* ((json-failed (ob-athena--load-sample-json
                       "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json"))
         (bytes-failed (ob-athena--extract-json-number json-failed "DataScannedInBytes"))
         (rounded-up-mb-failed (ceiling (/ bytes-failed 1048576.0)))
         (billable-mb-failed (max rounded-up-mb-failed 10))
         (expected-cost-failed (/ (* billable-mb-failed 1048576.0 5.0) 1099511627776.0))
         (cost-failed (ob-athena--calculate-query-cost bytes-failed)))
    (should (= bytes-failed 0))
    (should (< (abs (- cost-failed expected-cost-failed)) 1e-9))))


(ert-deftest ob-athena-extract-json-field-missing-key ()
  "Return nil when the key is missing from the JSON string."
  ;; Completely missing
  (should (null (ob-athena--extract-json-field "{\"OtherKey\": \"value\"}" "State")))
  ;; Key name is similar but different
  (should (null (ob-athena--extract-json-field "{\"States\": \"RUNNING\"}" "State")))
  ;; Empty object
  (should (null (ob-athena--extract-json-field "{}" "State"))))

(ert-deftest ob-athena-extract-json-field-non-json ()
  "Gracefully return nil when input is not valid JSON."
  ;; Fully invalid JSON
  (should (null (ob-athena--extract-json-field "not a json" "State")))
  ;; Empty string
  (should (null (ob-athena--extract-json-field "" "State")))
  ;; Null input
  (should (null (ob-athena--extract-json-field nil "State"))))

(ert-deftest ob-athena-extract-query-result-path ()
  "Test extraction of OutputLocation from Athena query JSON."
  (let ((json-success (ob-athena--load-sample-json
                       "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json"))
        (json-failed (ob-athena--load-sample-json
                      "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json")))
    (should (string= (ob-athena--query-result-path json-success)
                     "s3://athena-query-results-005343251202/43977ec3-ba3e-4874-912a-73f426532ffb.csv"))
    (should (string= (ob-athena--query-result-path json-failed)
                     "s3://athena-query-results-005343251202/4bf8a6ca-0880-4383-bc76-7a3baeb8b749.csv"))))

(ert-deftest ob-athena-build-timing-section-content ()
  "Ensure timing section is rendered with expected substrings for both success and failure cases."
  (let ((json-success (ob-athena--load-sample-json
                       "fixtures/43977ec3-ba3e-4874-912a-73f426532ffb-query-success-select-id-element-datavalue.json"))
        (json-failed (ob-athena--load-sample-json
                      "fixtures/4bf8a6ca-0880-4383-bc76-7a3baeb8b749-query-failed-no-table-exists.json")))
    (let ((output (ob-athena--build-timing-section json-success)))
      (should (stringp output))
      (should (string-match "Execution Time: 3\\.4480 sec" output))
      (should (string-match "Total Time: 3\\.6730 sec" output))
      (should (string-match "Queue Time: 0\\.1100 sec" output)))

    (let ((output (ob-athena--build-timing-section json-failed)))
      (should (stringp output))
      (should (string-match "Execution Time:" output)))))

(ert-deftest ob-athena--calculate-column-widths-works ()
  (let* ((rows '(("a" "12345") ("bbb" "678")))
         (widths (ob-athena--calculate-column-widths rows)))
    (should (equal widths '(3 5))))
  ;; Empty table
  (let ((widths (ob-athena--calculate-column-widths nil)))
    (should (equal widths nil)))
  ;; Single row
  (let ((widths (ob-athena--calculate-column-widths '(("one" "two")))))
    (should (equal widths '(3 3))))
  ;; Varying column widths
  (let ((widths (ob-athena--calculate-column-widths
                 '(("short" "tiny")
                   ("longertext" "medium-length")
                   ("m" "xxxxxxxxxxxxxxxx")))))
    (should (equal widths '(10 16))))
  ;; Includes empty strings
  (let ((widths (ob-athena--calculate-column-widths
                 '(("" "") ("1234" "56") ("7" "")))))
    (should (equal widths '(4 2))))
  ;; Mixed types: numbers, symbols (should coerce to string)
  (let ((widths (ob-athena--calculate-column-widths
                 `((,(number-to-string 123) ,(symbol-name 'foo))
                   ("abc" "defghij")))))
    (should (equal widths '(3 7)))))

(ert-deftest ob-athena--extract-json-field-valid-key ()
  (let ((json "{\"foo\": \"bar\"}"))
    (should (equal (ob-athena--extract-json-field json "foo") "bar"))))

(ert-deftest ob-athena--extract-json-field-multiple-keys ()
  "Return correct value when multiple keys are present."
  (let ((json "{\"foo\": \"bar\", \"baz\": \"qux\"}"))
    (should (equal (ob-athena--extract-json-field json "baz") "qux"))))

(ert-deftest ob-athena--extract-json-field-empty-value ()
  "Return empty string when key exists but value is empty."
  (let ((json "{\"foo\": \"\"}"))
    (should (equal (ob-athena--extract-json-field json "foo") ""))))

(ert-deftest ob-athena--extract-json-field-key-with-spaces ()
  "Extract value from a key that includes spaces."
  (let ((json "{\"key with space\": \"value\"}"))
    (should (equal (ob-athena--extract-json-field json "key with space") "value"))))

(ert-deftest ob-athena--extract-json-field-unquoted-numeric-value ()
  "Return nil if the value is numeric and not in quotes (not handled by regex)."
  (let ((json "{\"foo\": 123}"))
    (should (null (ob-athena--extract-json-field json "foo")))))

(ert-deftest ob-athena--extract-json-field-nested-object ()
  "Return nil if the value is a nested object (not handled by string regex)."
  (let ((json "{\"foo\": {\"bar\": \"baz\"}}"))
    (should (null (ob-athena--extract-json-field json "foo")))))

(provide 'ob-athena-cli-parsing-tests)
;;; ob-athena-cli-results-parsing-tests.el ends here
