;;; ob-athena-babel-parsing-tests.el --- Tests for ob-athena Babel header processing -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)
(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-build-context-header-overrides-and-defaults ()
  "Ensure all context keys are present and user overrides take effect."
  (let* ((params '((:output-location . "s3://custom/")
                   (:workgroup . "custom-wg")
                   (:database . "custom-db")
                   (:poll-interval . 1)))
         (ctx (ob-athena--build-context params))
         (expected-keys
          '(output-location workgroup profile database poll-interval
            console-region csv-output-dir result-reuse-enabled
            result-reuse-max-age fullscreen-monitor-buffer)))
    ;; Check all expected keys are present
    (dolist (key expected-keys)
      (should (alist-get key ctx)))
    ;; Check override values
    (should (equal (alist-get 'output-location ctx) "s3://custom/"))
    (should (equal (alist-get 'workgroup ctx) "custom-wg"))
    (should (equal (alist-get 'database ctx) "custom-db"))
    (should (= (alist-get 'poll-interval ctx) 1))
    ;; Check fallback value from defaults
    (should (equal (alist-get 'console-region ctx) ob-athena-console-region))))

(ert-deftest ob-athena-expand-body-with-vars ()
  "Ensure :var values are correctly substituted into query body."
  (let* ((params '((:var . (user . "bob")) (:var . (limit . 10))))
         (body "SELECT * FROM logs WHERE user = '${user}' LIMIT ${limit}")
         (result (org-babel-expand-body:athena body params)))
    (should (string-match "user = 'bob'" result))
    (should (string-match "LIMIT 10" result))))

(ert-deftest ob-athena-parsed-query-matches-expected ()
  "Check that parsed query from ob-athena contains expected substituted values."
  (let* ((fixture "fixtures/0f7bf33b-3eb1-4e6c-a0c3-d5316894b062-parsed-ob-query.sql")
         (query (with-temp-buffer
                  (insert-file-contents fixture)
                  (buffer-string))))
    (should (string-match "SELECT id, element, datavalue" query))
    (should (string-match "FROM original_csv" query))
    (should (string-match "LIMIT 10" query))))

(ert-deftest ob-athena-build-context-without-headers ()
  "Ensure default context keys are populated even when no headers are provided."
  (let* ((params nil)
         (ctx (ob-athena--build-context params))
         (expected-keys
          '(output-location workgroup profile database poll-interval
            console-region csv-output-dir result-reuse-enabled
            result-reuse-max-age fullscreen-monitor-buffer)))
    (dolist (key expected-keys)
      (should (alist-get key ctx)))
    (should (numberp (alist-get 'poll-interval ctx)))
    (should (memq (alist-get 'result-reuse-enabled ctx) '(t nil)))))

(provide 'ob-athena-babel-parsing-tests)
;;; ob-athena-babel-parsing-tests.el ends here
