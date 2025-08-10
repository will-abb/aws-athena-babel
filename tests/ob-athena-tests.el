;;; ob-athena-tests.el --- Load all ERT test files for ob-athena -*- lexical-binding: t; -*-

(setq org-confirm-babel-evaluate nil)
(defconst ob-athena-test-dir
  (file-name-directory (or load-file-name buffer-file-name)))

(load (expand-file-name "ob-athena-babel-parsing-unit-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-results-parsing-unit-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-query-execution-unit-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-output-conversion-unit-tests.el" ob-athena-test-dir))

(load (expand-file-name "ob-athena-minimal-integration-and-smoke-test.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-all-records-integration-test.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-filter-by-name-score-signup-integration-test.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-simple-select-all-integration-test.el" ob-athena-test-dir))

(load (expand-file-name "ob-athena-no-babel-headers-functional-test.el" ob-athena-test-dir))

(provide 'ob-athena-tests)
;;; ob-athena-tests.el ends here
