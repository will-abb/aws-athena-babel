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
