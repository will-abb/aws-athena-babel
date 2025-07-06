;;; ob-athena-tests.el --- Load all ERT test files for ob-athena -*- lexical-binding: t; -*-

(setq org-confirm-babel-evaluate nil)
(defconst ob-athena-test-dir
  (file-name-directory (or load-file-name buffer-file-name)))

(load (expand-file-name "ob-athena-babel-parsing-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-results-parsing-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-query-execution-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-minimal-integration-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-output-converstion-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-full-integration-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-no-babel-headers.el" ob-athena-test-dir))

(provide 'ob-athena-tests)

;;; ob-athena-tests.el ends here

