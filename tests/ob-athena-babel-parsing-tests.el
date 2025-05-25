;;; ob-athena-babel-tests.el --- Tests for ob-athena Babel header processing -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-build-context-header-overrides ()
  "Ensure Babel header args override default context keys."
  (let* ((params '((:output-location . "s3://custom/")
                   (:workgroup . "custom-wg")
                   (:database . "custom-db")
                   (:poll-interval . 1)))
         (ctx (ob-athena--build-context params)))
    (should (equal (alist-get 'output-location ctx) "s3://custom/"))
    (should (equal (alist-get 'workgroup ctx) "custom-wg"))
    (should (equal (alist-get 'database ctx) "custom-db"))
    (should (= (alist-get 'poll-interval ctx) 1))))

(ert-deftest ob-athena-expand-body-with-vars ()
  "Ensure :var values are correctly substituted into query body."
  (let* ((params '((:var . (user . "bob")) (:var . (limit . 10))))
         (body "SELECT * FROM logs WHERE user = '${user}' LIMIT ${limit}")
         (result (org-babel-expand-body:athena body params)))
    (should (string-match "user = 'bob'" result))
    (should (string-match "LIMIT 10" result))))

(defun ob-athena--write-query-to-file (query)
  "Write Athena QUERY string to file and archive it for testing."
  (with-temp-file ob-athena-query-file
    (insert query))
  (when-let ((id (bound-and-true-p ob-athena-query-id)))
    (let ((archive-path (expand-file-name (format "tests/fixtures/%s-expanded-query.sql" id)
                                          user-emacs-directory)))
      (with-temp-file archive-path
        (insert query)))))

(ert-deftest ob-athena-parsed-query-matches-expected ()
  "Check that parsed query from ob-athena contains expected substituted values."
  (let* ((fixture "tests/fixtures/0f7bf33b-3eb1-4e6c-a0c3-d5316894b062-parsed-ob-query.sql")
         (query (with-temp-buffer
                  (insert-file-contents fixture)
                  (buffer-string))))
    (should (string-match "SELECT id, element, datavalue" query))
    (should (string-match "FROM original_csv" query))
    (should (string-match "LIMIT 10" query))))
