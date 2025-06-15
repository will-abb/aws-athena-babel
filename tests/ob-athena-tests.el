 ;;; ob-athena-tests.el --- Load all ERT test files for ob-athena -*- lexical-binding: t; -*-

(defconst ob-athena-test-dir
  (file-name-directory (or load-file-name buffer-file-name)))

(load (expand-file-name "ob-athena-babel-parsing-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-parsing-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-cli-test-inputs.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-integration-tests.el" ob-athena-test-dir))
(load (expand-file-name "ob-athena-output-tests.el" ob-athena-test-dir))

(provide 'ob-athena-tests)

;;; ob-athena-tests.el ends here
