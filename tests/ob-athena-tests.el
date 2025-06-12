;;; ob-athena-tests.el --- Load all ERT test files for ob-athena -*- lexical-binding: t; -*-

(load-file "ob-athena-babel-parsing-tests.el")
(load-file "ob-athena-cli-parsing-tests.el")
(load-file "ob-athena-cli-test-inputs.el")
(load-file "ob-athena-integration-tests.el")
(load-file "ob-athena-live-integration-tests.el")
(load-file "ob-athena-output-tests.el")

(provide 'ob-athena-tests)

;;; ob-athena-tests.el ends here
