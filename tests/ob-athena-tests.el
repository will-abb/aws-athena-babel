;;; ob-athena-tests.el --- Description -*- lexical-binding: t; -*-
(require 'ert)
(require 'org)
(require 'ob-athena)

(defun ob-athena--load-sample-json (file)
  "Load a JSON test file from disk as a string."
  (with-temp-buffer
    (insert-file-contents file)
    (buffer-string)))

(ert-deftest ob-athena-parse-state ()
  "Test extraction of query execution state from CLI JSON."
  (let ((json (ob-athena--load-sample-json "test/data/sample-query-output.json")))
    (should (string= (ob-athena--extract-json-field json "State") "SUCCEEDED"))))

(ert-deftest ob-athena-parse-bytes-scanned ()
  "Test extraction of bytes scanned from CLI JSON."
  (let ((json (ob-athena--load-sample-json "test/data/sample-query-output.json")))
    (should (= (ob-athena--extract-json-number json "DataScannedInBytes") 10485760))))

(provide 'ob-athena-tests)
;;; ob-athena-tests.el ends here
